import mysql.connector
import time as t
from datetime import datetime, time as dt_time, timedelta , date
import requests
import smtplib
from email.mime.text import MIMEText
import json, os

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

OFFLINE_THRESHOLD = 2
SECOND_NOTIFICATION_HOURS = 6

STATE_FILE = "notification_state.json"

# ================== STATE FILE ==================
def load_state():
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, "r") as f:
                return json.load(f)
        except:
            return {}
    return {}

def save_state(state):
    with open(STATE_FILE, "w") as f:
        json.dump(state, f, indent=2)

# ================== HELPERS ==================
def build_message(ntf_typ, devnm):
    msgs = {
        3: f"WARNING!! The {devnm} is offline. Please take necessary action - Regards Fertisense LLP",
        5: f"INFO!! The device {devnm} is back online. No action is required - Regards Fertisense LLP",
    }
    return msgs.get(ntf_typ)

def send_sms(phone, msg):
    try:
        params = {
            "user_name": SMS_USER,
            "user_password": SMS_PASS,
            "mobile": phone,
            "sender_id": SENDER_ID,
            "type": "F",
            "text": msg
        }
        requests.get(SMS_API_URL, params=params, timeout=10)
        return True
    except:
        return False

def send_email(subject, message, email_ids):
    if not email_ids:
        return False
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
        return True
    except:
        return False

# ================== GET CONTACTS ==================
def get_contact_info(device_id):
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor(dictionary=True)

        today = date.today()

        cursor.execute("""
            SELECT sh.*, msi.Package_Name
            FROM Subcription_History sh
            JOIN Master_Subscription_Info msi
              ON sh.Subscription_ID = msi.Subscription_ID
            WHERE sh.Device_ID=%s
              AND sh.Subscription_ID=8
              AND sh.Subcription_End_date >= %s
        """, (device_id, today))
        subscription = cursor.fetchone()

        if not subscription:
            return [], [], 1, 1

        cursor.execute("SELECT ORGANIZATION_ID, CENTRE_ID FROM master_device WHERE DEVICE_ID=%s", (device_id,))
        device = cursor.fetchone()
        if not device:
            return [], [], 1, 1

        org = device["ORGANIZATION_ID"] or 1
        centre = device["CENTRE_ID"] or 1

        cursor.execute("""
            SELECT USER_ID_id FROM userorganizationcentrelink 
            WHERE ORGANIZATION_ID_id=%s AND CENTRE_ID_id=%s
        """, (org, centre))
        user_ids = [u["USER_ID_id"] for u in cursor.fetchall()]
        if not user_ids:
            return [], [], org, centre

        fmt = ','.join(['%s']*len(user_ids))
        cursor.execute(f"""
            SELECT PHONE, EMAIL, SEND_SMS, SEND_EMAIL
            FROM master_user 
            WHERE USER_ID IN ({fmt})
              AND (SEND_SMS=1 OR SEND_EMAIL=1)
        """, tuple(user_ids))
        users = cursor.fetchall()

        phones = [u["PHONE"] for u in users if u["SEND_SMS"] == 1]
        emails = [u["EMAIL"] for u in users if u["SEND_EMAIL"] == 1]

        return phones, emails, org, centre

    except:
        return [], [], 1, 1
    finally:
        if 'cursor' in locals(): cursor.close()
        if 'conn' in locals() and conn.is_connected(): conn.close()

# ================== MAIN LOGIC ==================
def check_device_online_status():
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor(dictionary=True)
        now = datetime.now()

        # only active (not condemned)
        cursor.execute("SELECT DEVICE_ID, DEVICE_NAME, DEVICE_STATUS FROM master_device")
        devices = cursor.fetchall()

        state = load_state()

        for device in devices:
            devid = str(device["DEVICE_ID"])
            devnm = device["DEVICE_NAME"]

            # 🛑 skip condemned devices
            if device["DEVICE_STATUS"] != 1:
                continue

            phones, emails, org, centre = get_contact_info(devid)
            if not phones and not emails:
                continue

            cursor.execute("""
                SELECT READING_DATE, READING_TIME 
                FROM device_reading_log 
                WHERE DEVICE_ID=%s 
                ORDER BY READING_DATE DESC, READING_TIME DESC LIMIT 1
            """, (devid,))
            last = cursor.fetchone()
            cursor.fetchall()

            # calculate online/offline
            if last:
                rtime = last["READING_TIME"]
                if isinstance(rtime, timedelta):
                    ts = rtime.total_seconds()
                    rtime = dt_time(int(ts//3600), int((ts%3600)//60), int(ts%60))

                last_dt = datetime.combine(last["READING_DATE"], rtime)
                diff = (now - last_dt).total_seconds()/60
                current_state = 1 if diff <= OFFLINE_THRESHOLD else 0
            else:
                current_state = 0

            # no verify loop (removed)

            # NOTIFICATION
            record = state.get(devid, {})
            last_state = record.get("last_state")
            last_time = record.get("last_notif_time")

            notify = False

            if last_state != current_state:
                notify = True
            elif last_time:
                if (now - datetime.fromisoformat(last_time)) >= timedelta(hours=SECOND_NOTIFICATION_HOURS):
                    notify = True

            if notify:
                sms_sent = False
                email_sent = False

                if current_state == 0:
                    msg = build_message(3, devnm)
                else:
                    msg = build_message(5, devnm)

                for p in phones:
                    if send_sms(p, msg): sms_sent = True

                email_sent = send_email(f"{devnm} Status Update", msg, emails)

                # UPDATE OR INSERT LOG → now using DEVICE_ONLINE_STATUS
                cursor.execute("SELECT id FROM iot_api_devicealarmlog WHERE DEVICE_ID=%s AND ALARM_DATE=%s",
                               (devid, now.date()))
                exist = cursor.fetchone()
                cursor.fetchall()

                if exist:
                    cursor.execute("""
                        UPDATE iot_api_devicealarmlog
                        SET DEVICE_ONLINE_STATUS=%s,
                            DEVICE_STATUS_DATE=%s,
                            DEVICE_STATUS_TIME=%s,
                            DEVICE_STATUS_SMS_DATE=%s,
                            DEVICE_STATUS_SMS_TIME=%s,
                            DEVICE_STATUS_EMAIL_DATE=%s,
                            DEVICE_STATUS_EMAIL_TIME=%s,
                            ORGANIZATION_ID=%s,
                            CENTRE_ID=%s
                        WHERE id=%s
                    """, (
                        current_state,
                        now.date(),
                        now.time(),
                        now.date() if sms_sent else None,
                        now.time() if sms_sent else None,
                        now.date() if email_sent else None,
                        now.time() if email_sent else None,
                        org, centre,
                        exist["id"]
                    ))
                else:
                    cursor.execute("""
                        INSERT INTO iot_api_devicealarmlog
                        (DEVICE_ID, SENSOR_ID, PARAMETER_ID, ALARM_DATE, ALARM_TIME,
                         DEVICE_ONLINE_STATUS, DEVICE_STATUS_DATE, DEVICE_STATUS_TIME,
                         DEVICE_STATUS_SMS_DATE, DEVICE_STATUS_SMS_TIME,
                         DEVICE_STATUS_EMAIL_DATE, DEVICE_STATUS_EMAIL_TIME,
                         ORGANIZATION_ID, CENTRE_ID)
                         VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    """, (
                        devid, 0, 0,
                        now.date(), now.time(),
                        current_state,
                        now.date(), now.time(),
                        now.date() if sms_sent else None, now.time() if sms_sent else None,
                        now.date() if email_sent else None, now.time() if email_sent else None,
                        org, centre
                    ))

                conn.commit()

                # update JSON
                state[devid] = {
                    "last_state": current_state,
                    "last_notif_time": now.isoformat()
                }
                save_state(state)

        cursor.close()
        conn.close()
    except Exception as e:
        print("ERROR:", e)

# ================== RUN ==================
if __name__ == "__main__":
    check_device_online_status()
