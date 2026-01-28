import os
import mysql.connector
import requests
# import smtplib
import pytz
import traceback
from datetime import datetime, date, time as dt_time, timedelta, timezone
import sib_api_v3_sdk
from sib_api_v3_sdk.rest import ApiException


BREVO_API_KEY = os.getenv("BREVO_API_KEY")

# ===== GLOBAL CONTACT CACHE =====
CONTACT_CACHE = {}


# ================== CONFIG ==================
db_config = {
    "host": "switchyard.proxy.rlwy.net",
    "user": "root",
    "port": 28085,
    "password": "NOtYUNawwodSrBfGubHhwKaFtWyGXQct",
    "database": "railway",
}

SMS_API_URL = "https://www.universalsmsadvertising.com/universalsmsapi.php"
SMS_USER = "8960853914"
SMS_PASS = "8960853914"
SENDER_ID = "FRTLLP"

OFFLINE_THRESHOLD = 5
SECOND_NOTIFICATION_HOURS = 6

BREVO_API_KEY = os.getenv("BREVO_API_KEY")
EMAIL_USER = "fertisenseiot@gmail.com"

IST_PYTZ = pytz.timezone("Asia/Kolkata")
UTC = timezone.utc

# ================== HELPERS ==================
def log(msg):
    print(f"[{datetime.now().isoformat(sep=' ', timespec='seconds')}] {msg}")


# ================== DLT SAFE MESSAGE BUILDER ==================
def build_message(ntf_typ, devnm):
    messages = {
        3: f"WARNING!! The {devnm} is offline. Please take necessary action- Regards Fertisense LLP",
        5: f"INFO!! The {devnm} is back online. No action is required- Regards Fertisense LLP",
    }
    return messages.get(ntf_typ, f"Alert for {devnm}- Regards Fertisense LLP")


# ================== DLT REJECTION DETECTOR ==================
def detect_dlt_issue(response_text: str) -> str:
    if not response_text:
        return "UNKNOWN_RESPONSE"

    txt = response_text.lower()

    if "template" in txt:
        return "DLT_TEMPLATE_MISMATCH"
    if "sender" in txt:
        return "SENDER_ID_NOT_APPROVED"
    if "dlt" in txt:
        return "DLT_VALIDATION_FAILED"
    if "route" in txt:
        return "ROUTE_NOT_ALLOWED"
    if "pending" in txt:
        return "DLT_PENDING_OPERATOR_APPROVAL"
    if "invalid" in txt:
        return "INVALID_NUMBER_OR_CONTENT"

    return "UNKNOWN_GATEWAY_RESPONSE"


# ================== SMS SENDING ==================
def send_sms_single(phone, message):
    if not phone:
        return False

    try:
        params = {
            "user_name": SMS_USER,
            "user_password": SMS_PASS,
            "mobile": str(phone).strip(),
            "sender_id": SENDER_ID,
            "type": "F",
            "text": message
        }

        r = requests.get(SMS_API_URL, params=params, timeout=30)
        reason = detect_dlt_issue(r.text)

        log(
            f"SMS -> {phone} | HTTP={r.status_code} | "
            f"DLT_REASON={reason} | RAW='{r.text.strip()[:120]}'"
        )

        return r.status_code == 200

    except Exception as e:
        log(f"❌ SMS failed for {phone}: {e}")
        return False


def send_sms(phone, message):
    """
    Supports:
      - single phone number (str/int)
      - multiple phone numbers (list/tuple/set)
    Sends ONE API request per number (DLT-safe).
    """
    if not phone:
        return False

    if isinstance(phone, (str, int)):
        phones = [str(phone)]
    elif isinstance(phone, (list, tuple, set)):
        phones = [str(p).strip() for p in phone if p]
    else:
        return False

    success_any = False

    for ph in phones:
        if send_sms_single(ph, message):
            success_any = True

    return success_any


# ================== EMAIL TEMPLATES ==================
def offline_html(dev_name, diff_minutes=None):
    uptime_text = ""
    if diff_minutes is not None:
        uptime_text = f"<p><strong>Last seen:</strong> {diff_minutes:.1f} minutes ago</p>"
    return f"""
    <html><body>
    <h2 style="color:#b02a2a">⚠ {dev_name} is OFFLINE</h2>
    {uptime_text}
    <p>Please take necessary action.</p>
    <hr><small>Regards,<br/>Fertisense LLP</small>
    </body></html>
    """


def online_html(dev_name):
    return f"""
    <html><body>
    <h2 style="color:#2a9d2a">✔ {dev_name} is BACK ONLINE</h2>
    <p>No action required.</p>
    <hr><small>Regards,<br/>Fertisense LLP</small>
    </body></html>
    """


# ================== EMAIL ==================
def send_email(subject, html_content, email_ids):
    if not email_ids:
        return False

    try:
        configuration = sib_api_v3_sdk.Configuration()
        configuration.api_key['api-key'] = BREVO_API_KEY

        api_instance = sib_api_v3_sdk.TransactionalEmailsApi(
            sib_api_v3_sdk.ApiClient(configuration)
        )

        email = sib_api_v3_sdk.SendSmtpEmail(
            subject=subject,
            html_content=html_content,
            sender={
                "name": "Fertisense_Email",
                "email": "fertisenseiot@gmail.com"  # Brevo me verified hona chahiye
            },
            to=[{"email": e} for e in email_ids]
        )

        api_instance.send_transac_email(email)
        log(f"✅ Brevo email sent to {len(email_ids)} users")
        return True

    except ApiException as e:
        log(f"❌ Brevo email failed: {e}")
        return False



# ================== CONTACT FETCH ==================
# def get_contact_info( device_id):
#     conn = cursor = None
#     try:
#         conn = mysql.connector.connect(**db_config)
#         cursor = conn.cursor(dictionary=True)

#         cursor.execute(
#             "SELECT ORGANIZATION_ID, CENTRE_ID FROM iot_api_masterdevice WHERE DEVICE_ID=%s",
#             (device_id,)
#         )
#         device = cursor.fetchone()
#         if not device:
#             return [], [], 1, 1

#         org_id = device.get("ORGANIZATION_ID") or 1
#         centre_id = device.get("CENTRE_ID") or 1

#         cursor.execute("""
#             SELECT u.PHONE, u.EMAIL, u.SEND_SMS, u.SEND_EMAIL
#             FROM userorganizationcentrelink l
#             JOIN master_user u ON l.USER_ID_id = u.USER_ID
#             WHERE l.ORGANIZATION_ID_id=%s AND l.CENTRE_ID_id=%s
#         """, (org_id, centre_id))

#         users = cursor.fetchall()
#         phones, emails = [], []

#         for u in users:

#              # ---------- SMS PHONES ----------
#             if u.get("SEND_SMS") == 1 and u.get("PHONE"):
#                raw_phone = str(u["PHONE"])
#                for part in raw_phone.replace("/", ",").split(","):
#                    num = part.strip()
#                    if num:
#                         phones.append(num)

#                    # ---------- EMAILS ---------- 

#             if u.get("SEND_EMAIL") == 1 and u.get("EMAIL"):
#                 emails.append(u["EMAIL"].strip())

#         # duplicates hatao, order safe
#         phones = list(dict.fromkeys(phones))
#         emails = list(dict.fromkeys(emails))
#         log(f"📞 Final Phones: {phones}")
#         log(f"📧 Final Emails: {emails}")


#         return list(set(phones)), list(set(emails)), org_id, centre_id
    
#         # log(f"📞 Phones resolved: {phones}")


#     except Exception as e:
#         log(f"❌ Contact fetch error: {e}")
#         return [], [], 1, 1
#     finally:
#         if cursor:
#             cursor.close()
#         if conn:
#             conn.close()

def get_contact_info(cursor, device_id):
    """
    Railway optimized:
    - No new DB connection
    - Uses shared cursor
    - Cached per device
    """

    # ---------- CACHE CHECK ----------
    if device_id in CONTACT_CACHE:
        return CONTACT_CACHE[device_id]

    # ---------- DEVICE ORG / CENTRE ----------
    cursor.execute(
        "SELECT ORGANIZATION_ID, CENTRE_ID FROM iot_api_masterdevice WHERE DEVICE_ID=%s",
        (device_id,)
    )
    device = cursor.fetchone()
    if not device:
        CONTACT_CACHE[device_id] = ([], [], 1, 1)
        return CONTACT_CACHE[device_id]

    org_id = device.get("ORGANIZATION_ID") or 1
    centre_id = device.get("CENTRE_ID") or 1

    # ---------- USERS FETCH ----------
    cursor.execute("""
        SELECT u.PHONE, u.EMAIL, u.SEND_SMS, u.SEND_EMAIL
        FROM userorganizationcentrelink l
        JOIN master_user u ON l.USER_ID_id = u.USER_ID
        WHERE l.ORGANIZATION_ID_id=%s
          AND l.CENTRE_ID_id=%s
    """, (org_id, centre_id))

    users = cursor.fetchall()

    phones = []
    emails = []

    for u in users:

        # ---------- SMS PHONES ----------
        if u.get("SEND_SMS") == 1 and u.get("PHONE"):
            raw_phone = str(u["PHONE"])
            for part in raw_phone.replace("/", ",").split(","):
                num = part.strip()
                if num:
                    phones.append(num)

        # ---------- EMAILS ----------
        if u.get("SEND_EMAIL") == 1 and u.get("EMAIL"):
            emails.append(u["EMAIL"].strip())

    # ---------- DEDUPE (order safe) ----------
    phones = list(dict.fromkeys(phones))
    emails = list(dict.fromkeys(emails))

    # ---------- SAVE TO CACHE ----------
    CONTACT_CACHE[device_id] = (phones, emails, org_id, centre_id)

    return CONTACT_CACHE[device_id]


# ================== TIME PARSER ==================
def parse_reading_time(val):
    if isinstance(val, timedelta):
        s = int(val.total_seconds())
        return dt_time(s // 3600, (s % 3600) // 60, s % 60)
    if isinstance(val, dt_time):
        return val
    if isinstance(val, str):
        try:
            h, m, *s = map(int, val.split(":"))
            return dt_time(h, m, s[0] if s else 0)
        except Exception:
            return None
    return None


# ================== MAIN LOGIC ==================
def check_device_online_status():
    conn = cursor = None
    try:
        log("🚀 Device online/offline check started")
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor(dictionary=True)

        now = datetime.now(IST_PYTZ)

        cursor.execute(
            "SELECT DEVICE_ID, DEVICE_NAME FROM iot_api_masterdevice"
        )
        devices = cursor.fetchall()

        log(f"📟 Active devices found: {len(devices)}")


        for d in devices:

            devid = d["DEVICE_ID"]
            devnm = d.get("DEVICE_NAME") or f"Device-{devid}"
   
            log(f"➡️ Checking device: {devnm} ({devid})")

            

            phones, emails, _, _ = get_contact_info(cursor, devid)

            cursor.execute("""
                SELECT READING_DATE, READING_TIME
                FROM device_reading_log
                WHERE DEVICE_ID=%s
                ORDER BY READING_DATE DESC, READING_TIME DESC
                LIMIT 1
            """, (devid,))
            last_read = cursor.fetchone()

            if last_read:
                rd = last_read["READING_DATE"]
                rt = parse_reading_time(last_read["READING_TIME"])
                last_update = (
                    datetime.combine(rd, rt)
                    .replace(tzinfo=UTC)
                    .astimezone(IST_PYTZ)
                    if rd and rt else None
                )
            else:
                last_update = None

            diff_minutes = (
                (now - last_update).total_seconds() / 60
                if last_update else OFFLINE_THRESHOLD + 1
            )
            current_state = 1 if diff_minutes <= OFFLINE_THRESHOLD else 0

            log(f"🧠 Last update: {last_update}")
            log(f"🧠 Diff minutes: {diff_minutes}")
            log(f"🧠 Current state (1=online,0=offline): {current_state}")


            cursor.execute("""
                SELECT * FROM device_status_alarm_log
                WHERE DEVICE_ID=%s AND IS_ACTIVE=1
                ORDER BY DEVICE_STATUS_ALARM_ID DESC
                LIMIT 1
            """, (devid,))
            alarm = cursor.fetchone()

            log(f"🧠 Existing alarm: {alarm}")
            log(f"📞 Phones: {phones}")
            log(f"📧 Emails: {emails}")


            # ---------- ONLINE ----------
            if current_state == 1 and alarm and alarm["IS_ACTIVE"] == 1:
                message = build_message(5, devnm)
                sms_ok = send_sms(phones, message)
                email_ok = send_email(f"{devnm} Back Online", online_html(devnm), emails)

                cursor.execute("""
                    UPDATE device_status_alarm_log
                    SET IS_ACTIVE=0,
                        UPDATED_ON_DATE=%s,
                        UPDATED_ON_TIME=%s,
                        SMS_DATE=%s,
                        SMS_TIME=%s,
                        EMAIL_DATE=%s,
                        EMAIL_TIME=%s
                    WHERE DEVICE_STATUS_ALARM_ID=%s
                """, (
                    now.date(), now.time(),
                    now.date() if sms_ok else alarm["SMS_DATE"],
                    now.time() if sms_ok else alarm["SMS_TIME"],
                    now.date() if email_ok else alarm["EMAIL_DATE"],
                    now.time() if email_ok else alarm["EMAIL_TIME"],
                    alarm["DEVICE_STATUS_ALARM_ID"]
                ))
                conn.commit()
                continue

            # ---------- OFFLINE ----------
            if current_state == 0 and (not alarm or alarm["IS_ACTIVE"] ==0):
                message = build_message(3, devnm)
                sms_ok = send_sms(phones, message)
                email_ok = send_email(
                    f"{devnm} Offline",
                    offline_html(devnm, diff_minutes),
                    emails
                )

                cursor.execute("""
                    INSERT INTO device_status_alarm_log
                    (DEVICE_ID, DEVICE_STATUS, IS_ACTIVE,
                     CREATED_ON_DATE, CREATED_ON_TIME,
                     SMS_DATE, SMS_TIME, EMAIL_DATE, EMAIL_TIME)
                    VALUES (%s,1,1,%s,%s,%s,%s,%s,%s)
                """, (
                    devid,
                    now.date(), now.time(),
                    now.date() if sms_ok else None,
                    now.time() if sms_ok else None,
                    now.date() if email_ok else None,
                    now.time() if email_ok else None
                ))
                conn.commit()

        log("✅ Device check completed")

    except Exception as e:
        log(f"❌ Fatal error: {e}")
        traceback.print_exc()
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


# ================== RUN ==================
if __name__ == "__main__":
    check_device_online_status()
