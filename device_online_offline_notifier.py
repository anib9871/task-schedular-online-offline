import mysql.connector
from datetime import datetime, timedelta
import time as t
import requests
import smtplib
from email.mime.text import MIMEText
import pytz

# ================== CONFIG ==================
db_config = {
    "host": "switchback.proxy.rlwy.net",
    "user": "root",
    "port": 44750,
    "password": "qYxlhEiaEvtiRvKaFyigDPtXSSCpddMv",
    "database": "railway",
}

SMS_API_URL = "http://www.universalsmsadvertising.com/universalsmsapi.php"
SMS_USER = "8960853914"
SMS_PASS = "8960853914"
SENDER_ID = "FRTLLP"

SMTP_SERVER = "smtp.gmail.com"
SMTP_PORT = 587
EMAIL_USER = "testwebservice71@gmail.com"
EMAIL_PASS = "akuu vulg ejlg ysbt"

# TZ = pytz.timezone("Asia/Singapore")

# ===================================================
def build_message(ntf_typ, devnm):
    messages = {
        3: f"WARNING!! The {devnm} is offline. Please take necessary action - Regards Fertisense LLP",
        5: f"INFO!! The device {devnm} is back online. No action is required - Regards Fertisense LLP",
    }
    return messages.get(ntf_typ, f"Alert for {devnm} - Regards Fertisense LLP")

def send_sms(phone, message):
    print("🔹 Sending SMS...")
    try:
        params = {
            "user_name": SMS_USER,
            "user_password": SMS_PASS,
            "mobile": phone,
            "sender_id": SENDER_ID,
            "type": "F",
            "text": message
        }
        response = requests.get(SMS_API_URL, params=params)
        print("✅ SMS sent! Response:", response.text)
    except Exception as e:
        print("❌ SMS failed:", e)

def send_email(subject, message, email_ids):
    if not email_ids:
        print("❌ No email recipients. Skipping.")
        return
    print("🔹 Sending Email...")
    try:
        msg = MIMEText(message)
        msg["Subject"] = subject
        msg["From"] = EMAIL_USER
        msg["To"] = ", ".join(email_ids)
        server = smtplib.SMTP(SMTP_SERVER, SMTP_PORT)
        server.starttls()
        server.login(EMAIL_USER, EMAIL_PASS)
        server.sendmail(EMAIL_USER, email_ids, msg.as_string())
        server.quit()
        print("✅ Email sent successfully!")
    except Exception as e:
        print("❌ Email failed:", e)

def get_contact_info(device_id):
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor(dictionary=True)
        cursor.execute("""
            SELECT ORGANIZATION_ID, CENTRE_ID
            FROM master_device
            WHERE DEVICE_ID = %s
        """, (device_id,))
        device = cursor.fetchone()
        if not device:
            return [], []

        org_id = device["ORGANIZATION_ID"]
        centre_id = device["CENTRE_ID"]

        cursor.execute("""
            SELECT USER_ID_id
            FROM userorganizationcentrelink
            WHERE ORGANIZATION_ID_id = %s
              AND CENTRE_ID_id = %s
        """, (org_id, centre_id))
        user_links = cursor.fetchall()
        user_ids = [u["USER_ID_id"] for u in user_links]
        if not user_ids:
            return [], []

        format_strings = ','.join(['%s'] * len(user_ids))
        query = f"""
            SELECT USER_ID, PHONE, EMAIL, SEND_SMS, SEND_EMAIL
            FROM master_user
            WHERE USER_ID IN ({format_strings})
              AND (SEND_SMS = 1 OR SEND_EMAIL = 1)
        """
        cursor.execute(query, tuple(user_ids))
        users = cursor.fetchall()

        phones = [u["PHONE"] for u in users if u["SEND_SMS"] == 1]
        emails = [u["EMAIL"] for u in users if u["SEND_EMAIL"] == 1]

        return phones, emails
    except Exception as e:
        print("❌ Error in get_contact_info:", e)
        return [], []
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals() and conn.is_connected():
            conn.close()

# ===================================================
def check_device_online_status():
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor(dictionary=True)

        offline_threshold = 5 # minutes
        now = datetime.now()

        cursor.execute("""
            SELECT DEVICE_ID, DEVICE_NAME
            FROM master_device
        """)
        devices = cursor.fetchall()

        for device in devices:
            devid = device["DEVICE_ID"]
            devnm = device["DEVICE_NAME"]

            # latest reading from device_reading_log
            cursor.execute("""
                SELECT READING_DATE, READING_TIME
                FROM device_reading_log
                WHERE DEVICE_ID = %s
                ORDER BY READING_DATE DESC, READING_TIME DESC
                LIMIT 1
            """, (devid,))
            last_read = cursor.fetchone()
            if not last_read:
                continue

            reading_time = (datetime.min + last_read["READING_TIME"]).time()
            last_update = datetime.combine(last_read["READING_DATE"], reading_time)
            last_update = (last_update)
            diff_minutes = (now - last_update).total_seconds() / 60

            if diff_minutes > offline_threshold:
                print(f"🚨 {devnm} is OFFLINE! Last data {round(diff_minutes, 2)} mins ago")
                message = build_message(3, devnm)
                phones, emails = get_contact_info(devid)
                for phone in phones:
                    send_sms(phone, message)
                send_email("Device Offline Alert", message, emails)

                cursor.execute("""
                    INSERT INTO iot_api_devicealarmlog (DEVICE_ID, ALARM_DATE, ALARM_TIME, IS_ACTIVE)
                    VALUES (%s, %s, %s, 1)
                """, (devid, now.date(), now.time()))
                conn.commit()

            else:
                print(f"✅ {devnm} is ONLINE. Last update {round(diff_minutes, 2)} mins ago")

        cursor.close()
        conn.close()

    except Exception as e:
        print("❌ Error in check_device_online_status:", e)

# ===================================================
if __name__ == "__main__":
    while True:
        check_device_online_status()
        print("⏳ Waiting 1 minute for next check...")
        t.sleep(60)
