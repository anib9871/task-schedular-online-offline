#!/usr/bin/env python3
import mysql.connector
from datetime import datetime, time as dt_time, timedelta, date
import requests
import smtplib
from email.mime.text import MIMEText
import os
import sys
import traceback

# ================== CONFIG ==================
db_config = {
    "host": "switchback.proxy.rlwy.net",
    "user": "root",
    "port": 44750,
    "password": "qYxlhEiaEvtiRvKaFyigDPtXSSCpddMv",
    "database": "railway",
    "raise_on_warnings": True,
}

SMS_API_URL = "https://www.universalsmsadvertising.com/universalsmsapi.php"
SMS_USER = "8960853914"
SMS_PASS = "8960853914"
SENDER_ID = "FRTLLP"

SMTP_SERVER = "smtp.gmail.com"
SMTP_PORT = 587
EMAIL_USER = "testwebservice71@gmail.com"
EMAIL_PASS = "akuu vulg ejlg ysbt"

OFFLINE_THRESHOLD = 10         # minutes
SECOND_NOTIFICATION_HOURS = 6  # hours

# ================== HELPERS ==================
def log(msg):
    print(f"[{datetime.now().isoformat(sep=' ', timespec='seconds')}] {msg}")

def build_message(ntf_typ, devnm):
    messages = {
        3: f"WARNING!! The {devnm} is offline. Please take necessary action - Regards Fertisense LLP",
        5: f"INFO!! The device {devnm} is back online. No action is required - Regards Fertisense LLP",
    }
    return messages.get(ntf_typ, f"Alert for {devnm} - Regards Fertisense LLP")

def send_sms(phone, message):
    if not phone:
        return False
    try:
        params = {
            "user_name": SMS_USER,
            "user_password": SMS_PASS,
            "mobile": phone,
            "sender_id": SENDER_ID,
            "type": "F",
            "text": message
        }
        r = requests.get(SMS_API_URL, params=params, timeout=30)
        log(f"SMS API -> phone={phone} status_code={r.status_code} text={r.text[:200]}")
        return r.status_code == 200
    except Exception as e:
        log(f"❌ SMS failed for {phone}: {e}")
        return False

def send_email(subject, message, email_ids):
    if not email_ids:
        return False
    try:
        msg = MIMEText(message)
        msg["Subject"] = subject
        msg["From"] = EMAIL_USER
        msg["To"] = ", ".join(email_ids)
        server = smtplib.SMTP(SMTP_SERVER, SMTP_PORT, timeout=20)
        server.starttls()
        server.login(EMAIL_USER, EMAIL_PASS)
        server.sendmail(EMAIL_USER, email_ids, msg.as_string())
        server.quit()
        log("✅ Email sent")
        return True
    except Exception as e:
        log(f"❌ Email failed: {e}")
        return False

# Subscription/contact fetch
def get_contact_info(device_id):
    conn = None
    cursor = None
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
            ORDER BY sh.Subcription_End_date DESC
            LIMIT 1
        """, (device_id, today))
        subscription = cursor.fetchone()

        if not subscription:
            return [], [], 1, 1

        cursor.execute("SELECT ORGANIZATION_ID, CENTRE_ID FROM iot_api_masterdevice WHERE DEVICE_ID=%s", (device_id,))
        device = cursor.fetchone()
        if not device:
            return [], [], 1, 1

        org_id = device["ORGANIZATION_ID"]
        centre_id = device["CENTRE_ID"]

        cursor.execute("""
            SELECT USER_ID_id FROM userorganizationcentrelink
            WHERE ORGANIZATION_ID_id=%s AND CENTRE_ID_id=%s
        """, (org_id, centre_id))
        rows = cursor.fetchall()
        user_ids = [r["USER_ID_id"] for r in rows]

        if not user_ids:
            return [], [], org_id, centre_id

        format_str = ",".join(["%s"] * len(user_ids))
        cursor.execute(f"""
            SELECT USER_ID, PHONE, EMAIL, SEND_SMS, SEND_EMAIL
            FROM master_user
            WHERE USER_ID IN ({format_str})
        """, tuple(user_ids))
        users = cursor.fetchall()

        phones = []
        emails = []

        for u in users:
            if u["SEND_SMS"] == 1 and u["PHONE"]:
                phones.append(str(u["PHONE"]))
            if u["SEND_EMAIL"] == 1 and u["EMAIL"]:
                emails.append(u["EMAIL"])

        phones = list(dict.fromkeys(phones))
        emails = list(dict.fromkeys(emails))

        return phones, emails, org_id, centre_id

    except:
        return [], [], 1, 1
    finally:
        if cursor: cursor.close()
        if conn and conn.is_connected(): conn.close()

# Parse reading time
def parse_reading_time(val):
    if val is None:
        return None
    if isinstance(val, timedelta):
        sec = int(val.total_seconds())
        return dt_time(sec // 3600, (sec % 3600) // 60, sec % 60)
    try:
        if hasattr(val, "hour"):
            return val
    except:
        pass
    if isinstance(val, str):
        p = val.split(":")
        if len(p) == 3:
            return dt_time(int(p[0]), int(p[1]), int(p[2]))
    return None

# ================== MAIN ==================
def check_device_online_status():
    conn = None
    cursor = None
    try:
        log("🚀 Starting device online/offline check")
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor(dictionary=True)

        now = datetime.now()

        cursor.execute("SELECT DEVICE_ID, DEVICE_NAME FROM iot_api_masterdevice WHERE DEVICE_STATUS=1")
        devices = cursor.fetchall()

        for d in devices:
            devid = d["DEVICE_ID"]
            devnm = d["DEVICE_NAME"]

            phones, emails, org, centre = get_contact_info(devid)

            # get last reading
            cursor.execute("""
                SELECT READING_DATE, READING_TIME
                FROM device_reading_log
                WHERE DEVICE_ID=%s
                ORDER BY READING_DATE DESC, READING_TIME DESC
                LIMIT 1
            """, (devid,))
            last = cursor.fetchone()

            diff = None
            if last:
                rd = last["READING_DATE"]
                rt = parse_reading_time(last["READING_TIME"])
                if rd and rt:
                    lastdt = datetime.combine(rd, rt)
                    diff = (now - lastdt).total_seconds() / 60
                    if diff < 0:
                        diff = OFFLINE_THRESHOLD + 5
                else:
                    diff = OFFLINE_THRESHOLD + 5
            else:
                diff = OFFLINE_THRESHOLD + 5

            # online/offline
            is_online = diff <= OFFLINE_THRESHOLD

            # get existing alarm
            cursor.execute("""
                SELECT *
                FROM device_status_alarm_log
                WHERE DEVICE_ID=%s AND IS_ACTIVE=1
                ORDER BY DEVICE_STATUS_ALARM_ID DESC LIMIT 1
            """, (devid,))
            existing = cursor.fetchone()

            # ---------------- ONLINE ----------------
            if is_online:
                if existing:
                    msg = build_message(5, devnm)
                    sms_ok = False
                    email_ok = False

                    for ph in phones:
                        if send_sms(ph, msg):
                            sms_ok = True

                    if emails:
                        email_ok = send_email(f"{devnm} Status Update", msg, emails)

                    cursor.execute("""
                        UPDATE device_status_alarm_log
                        SET IS_ACTIVE=0,
                            UPDATED_ON_DATE=%s,
                            UPDATED_ON_TIME=%s
                        WHERE DEVICE_STATUS_ALARM_ID=%s
                    """, (now.date(), now.time(), existing["DEVICE_STATUS_ALARM_ID"]))
                    conn.commit()

                continue

            # ---------------- OFFLINE ----------------
            if not existing:
                log("➡ Creating new offline alarm (first notification is automatic).")

                sms_msg = build_message(3, devnm)
                for ph in phones:
                    send_sms(ph, sms_msg)

                email_sent = False
                if emails:
                    email_sent = send_email(f"{devnm} Status Update", sms_msg, emails)

                cursor.execute("""
                    INSERT INTO device_status_alarm_log
                    (DEVICE_ID, DEVICE_STATUS, IS_ACTIVE,
                     CREATED_ON_DATE, CREATED_ON_TIME,
                     SMS_DATE, SMS_TIME,
                     EMAIL_DATE, EMAIL_TIME)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """, (
                    devid, 1, 1,
                    now.date(), now.time(),
                    now.date(), now.time(),           # ALWAYS record first alert
                    now.date() if email_sent else None,
                    now.time() if email_sent else None
                ))
                conn.commit()
                continue

            # existing offline alarm → check repeat SMS logic
            sms_date = existing["SMS_DATE"]
            sms_time = existing["SMS_TIME"]

            if not sms_date or not sms_time:
                # Should NEVER happen now because INSERT logs first SMS time
                continue

            last_sms_dt = datetime.combine(sms_date, sms_time)

            # 6 hour rule
            if now >= last_sms_dt + timedelta(hours=SECOND_NOTIFICATION_HOURS):
                log("➡ 6+ hours passed → sending repeat SMS")
                sms_msg = build_message(3, devnm)

                sms_ok = False
                for ph in phones:
                    if send_sms(ph, sms_msg):
                        sms_ok = True

                if sms_ok:
                    cursor.execute("""
                        UPDATE device_status_alarm_log
                        SET SMS_DATE=%s, SMS_TIME=%s
                        WHERE DEVICE_STATUS_ALARM_ID=%s
                    """, (now.date(), now.time(), existing["DEVICE_STATUS_ALARM_ID"]))
                    conn.commit()
            else:
                log("➡ SMS sent recently (<6 hrs), not sending again.")

        log("✅ Processing complete.")

    except Exception as e:
        log(f"❌ Error: {e}")
        traceback.print_exc()

    finally:
        if cursor: 
            try: cursor.close()
            except: pass
        if conn and conn.is_connected():
            conn.close()


if __name__ == "__main__":
    check_device_online_status()
