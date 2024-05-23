import time
import datetime
import uuid
import hmac
import hashlib
import requests
import json
from dotenv import load_dotenv
import os
from supabase import create_client, Client


def get_keys_and_template():
    response = supabase.table('MORNING_FINANCIAL').select("*").execute()
    return response.data[0]


def unique_id():
    return str(uuid.uuid1().hex)


def get_iso_datetime():
    utc_offset_sec = time.altzone if time.localtime().tm_isdst else time.timezone
    utc_offset = datetime.timedelta(seconds=-utc_offset_sec)
    return datetime.datetime.now().replace(tzinfo=datetime.timezone(offset=utc_offset)).isoformat()


def get_signature(key, msg):
    return hmac.new(key.encode(), msg.encode(), hashlib.sha256).hexdigest()


def get_headers(apiKey, apiSecret):
    date = get_iso_datetime()
    salt = unique_id()
    data = date + salt
    return {
        'Content-Type': "application/json",
        'Authorization': 'HMAC-SHA256 ApiKey=' + apiKey + ', Date=' + date + ', salt=' + salt +
                         ', signature=' + get_signature(apiSecret, data)
    }


def get_kakao_financial_posts(id, last_send_no):
    res = []
    financial_posts = requests.get(os.environ.get("KAKAO_BANK_POSTS_API"))\
        .json()["data"]["list"]

    for i, post in enumerate(financial_posts):
        no = post["no"]

        if i == 0:
            supabase.table(os.environ.get("TABLE_NAME_1"))\
                .update({"kakao_last_send_no": no}).eq("id", id).execute()

        if no == last_send_no:
            break

        res.append(f'https://brunch.co.kr/@kakaobank/{no}')

    return res


def get_toss_financial_posts(id, last_send_key):
    res = []
    financial_posts = requests.get(os.environ.get("TOSS_BANK_POSTS_API"))\
        .json()["success"]["results"]

    for i, post in enumerate(financial_posts):
        key = post["key"]

        if i == 0:
            supabase.table(os.environ.get("TABLE_NAME_2"))\
                .update({"toss_last_send_key": key}).eq("id", id).execute()

        if key == last_send_key:
            break

        res.append(f'https://blog.toss.im/article/{key}')

    return res


load_dotenv()

url: str = os.environ.get("SUPABASE_URL")
key: str = os.environ.get("SUPABASE_KEY")

supabase: Client = create_client(url, key)

supabase.auth.sign_in_with_password({
    "email": os.environ.get("SUPABASE_USER_EMAIL"),
    "password": os.environ.get("SUPABASE_USER_PASSWORD")
})

subscriber_phone_numbers = supabase.table(os.environ.get("TABLE_NAME_3"))\
    .select("*").execute().data

# 마지막으로 보낸 포스트들 정보와 SMS 템플릿 불러오기
keys_and_template = get_keys_and_template()

id, message_template = keys_and_template["id"], keys_and_template["message_template"]
kakao_last_send_no, toss_last_send_key = keys_and_template["kakao_last_send_no"], \
    keys_and_template["toss_last_send_key"]

kakao_financial_posts = get_kakao_financial_posts(id, kakao_last_send_no)
toss_financial_posts = get_toss_financial_posts(id, toss_last_send_key)
post_links = '\n'.join(kakao_financial_posts + toss_financial_posts)

request_header = get_headers(os.environ.get("SMS_API_KEY"),
                             os.environ.get("SMS_API_SECRET"))
request_data = json.dumps({
    "messages": [
        {
            "to": subscriber_phone_numbers[i]["phone_number"],
            "from": os.environ.get("SENDER_PHONE_NUMBER"),
            "text": message_template.format(post_links)
        } for i in range(len(subscriber_phone_numbers))
    ]
}, indent=4)

res = requests.post(os.environ.get("SMS_API"),
              headers=request_header,
              data=request_data)

supabase.auth.sign_out()

