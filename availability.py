import datetime
import json
import os
import smtplib
import ssl
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import List, Optional

import cachetools.func
import pandas as pd
import requests
from retry import retry
import pgeocode
import random

headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'}

headers = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9", 
    "Accept-Encoding": "gzip, deflate", 
    "Accept-Language": "en-GB,en-US;q=0.9,en;q=0.8", 
    "Dnt": "1", 
    "Host": "httpbin.org", 
    "Upgrade-Insecure-Requests": "1", 
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.97 Safari/537.36", 
  }

user_agent_list = [
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_5) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/13.1.1 Safari/605.1.15',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:77.0) Gecko/20100101 Firefox/77.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.97 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:77.0) Gecko/20100101 Firefox/77.0',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.97 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36',
    "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/31.0.1623.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 6.2; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/30.0.1599.17 Safari/537.36",
    "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/29.0.1547.62 Safari/537.36",
    "Mozilla/5.0 (X11; CrOS i686 4319.74.0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/29.0.1547.57 Safari/537.36",
    "Mozilla/5.0 (Windows NT 6.2; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/29.0.1547.2 Safari/537.36",
    "Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/28.0.1468.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 6.2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/28.0.1467.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 6.2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/28.0.1464.0 Safari/537.36",
]

def get_random_header():
    user_agent = random.choice(user_agent_list)
    _header = headers.copy()
    _header['User-Agent'] = user_agent
    return _header

def get_all_district_ids():
    district_df_all = None
    for state_code in range(1, 40):
        response = requests.get("https://cdn-api.co-vin.in/api/v2/admin/location/districts/{}".format(state_code), timeout=3, headers=get_random_header())
        district_df = pd.DataFrame(json.loads(response.text))
        district_df = pd.json_normalize(district_df['districts'])
        if district_df_all is None:
            district_df_all = district_df
        else:
            district_df_all = pd.concat([district_df_all, district_df])

        district_df_all.district_id = district_df_all.district_id.astype(int)

    district_df_all = district_df_all[["district_name", "district_id"]].sort_values("district_name")
    return district_df_all

@cachetools.func.ttl_cache(maxsize=100, ttl=30 * 60)
@retry(KeyError, tries=5, delay=2)
def get_data(URL):
    response = requests.get(URL, timeout=3, headers=get_random_header())
    data = json.loads(response.text)['centers']
    return data


def get_availability(district_ids: List[int], min_age_limit: int, pincode_search: Optional[str] = None, show_empty_slots: bool= False):
    INP_DATE = datetime.datetime.today().strftime("%d-%m-%Y")
    all_date_df = []
    for district_id in district_ids:
        print(f"checking for INP_DATE:{INP_DATE} & DIST_ID:{district_id}")
        URL = "https://cdn-api.co-vin.in/api/v2/appointment/sessions/public/calendarByDistrict?district_id={}&date={}".format(district_id, INP_DATE)
        data = get_data(URL)
        df = pd.DataFrame(data)
        if len(df):
            df = df.explode("sessions")
            df['min_age_limit'] = df.sessions.apply(lambda x: x['min_age_limit'])
            df['available_capacity'] = df.sessions.apply(lambda x: x['available_capacity']).astype(int)
            df['date'] = df.sessions.apply(lambda x: x['date'])
            df['vaccine'] = df.sessions.apply(lambda x: x['vaccine'])
            df = df[["date", "min_age_limit", "available_capacity", "pincode", "name", "state_name", "district_name", "block_name", "fee_type", "vaccine"]]
            all_date_df.append(df)
            # if all_date_df is not None:
            #     all_date_df = pd.concat([all_date_df, df])
            # else:
            #     all_date_df = df
    if len(all_date_df)>0:
        all_date_df = pd.concat(all_date_df)
        all_date_df = all_date_df.drop(["block_name"], axis=1)
        if pincode_search is not None and pincode_search!="":
            dist = pgeocode.GeoDistance('in')
            all_date_df['distance'] = df.pincode.apply(lambda x: dist.query_postal_code(str(pincode_search), x)).fillna(9999).round(0)
            all_date_df.sort_values(["distance", "available_capacity"], ascending=[True, False], inplace=True)
        else:
            all_date_df.sort_values(["available_capacity"], ascending=[False], inplace=True)
        all_date_df = all_date_df[all_date_df.min_age_limit <= min_age_limit]
        if not show_empty_slots:
            all_date_df = all_date_df[all_date_df.available_capacity > 0]
        # Human Readable Column names
        all_date_df.rename(columns={
            "name": "Center",
            "district_name": "District",
            "fee_type": "Free/Paid",
            "min_age_limit": "Min Eligible Age",
            "pincode": "Pin Code",
            "distance": "Distance from you(km)",
            "available_capacity": "Available Slots"
        }, inplace=True)

        return all_date_df
    return pd.DataFrame()


def send_email(data_frame, age, send_empty_email=False):
    # Used most of code from https://realpython.com/python-send-email/ and modified

    sender_email = os.environ['SENDER_EMAIL']
    receiver_email = os.environ['RECEIVER_EMAIL']
    message = MIMEMultipart("alternative")
    message["From"] = sender_email
    message["To"] = receiver_email
    to_send_email = True
    if data_frame is None or len(data_frame.index) == 0:
        print("Empty Data")
        message["Subject"] = "Availability for Max Age {} is 0 <EOM>".format(age, len(data_frame.index))
        text = ""
        part1 = MIMEText(text, "plain")
        message.attach(part1)
        to_send_email = send_empty_email

    else:

        message["Subject"] = "Availability for Max Age {} Count {}".format(age, len(data_frame.index))
        text = """\
        Hi,
        Please refer vaccine availability"""

        html_header = """\
        <html>
        <body>
            <p>

        """

        html_footer = """\
        
            </p>
        </body>
        </html>
        """

        html = "{}{}{}".format(html_header, data_frame.to_html(), html_footer)

        part1 = MIMEText(text, "plain")
        part2 = MIMEText(html, "html")

        message.attach(part1)
        message.attach(part2)
    if to_send_email:
        context = ssl.create_default_context()
        with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=context) as server:
            server.login(sender_email, os.environ['SENDER_PASSWORD'])
            server.sendmail(
                sender_email, receiver_email, message.as_string()
            )


if __name__ == "__main__":
    tvm = 296
    kannur = 297
    dist_ids = [tvm]
    # next_n_days = 1
    min_age_limit = 40
    send_empty_email = False
    pincode = 695024
    show_empty_slots = False
    availability_data = get_availability(dist_ids, min_age_limit, pincode, show_empty_slots)
    # print(availability_data)
    send_email(availability_data, min_age_limit, send_empty_email = False)
    if not show_empty_slots and len(availability_data)==0:
        print("No Slots available")
