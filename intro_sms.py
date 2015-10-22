import psycopg2
import psycopg2.extras
import json
import requests
import re
import sys
# XXX
sys.exit()

from settings import CONFIG, TREATMENT_MESSAGES, CONTROL_MESSAGES, SMS_OFFSET_TIME
from datetime import datetime, timedelta

import logging
logging.basicConfig()

user = CONFIG['dhis2_user']
passwd = CONFIG['dhis2_passwd']
# SMS_OFFSET_TIME = 1  # time in between SMS in minutes.

reporting_period = datetime.now().strftime('%Y-%m')

# To handle Json in DB well
psycopg2.extras.register_default_json(loads=lambda x: x)
psycopg2.extensions.register_adapter(dict, psycopg2.extras.Json)

msgs = TREATMENT_MESSAGES
# msgs is of the form [{"has_params": False, "has_variations": False, "text": ""}, {"has_params": ....}, ]


def queue_facility_sms(conn, cur, params, run_time):  # params has the facility uuid and other params
    cur.execute(
        "INSERT INTO schedules (params, run_time) VALUES(%s, %s)", [params, run_time])
    conn.commit()
    return None


conn2 = psycopg2.connect(
    "dbname=" + CONFIG["mtrack_dbname"] + " host= " + CONFIG["dbhost"] + " port=" + CONFIG["dbport"] +
    " user=" + CONFIG["dbuser"] + " password=" + CONFIG["dbpasswd"])
cur2 = conn2.cursor(cursor_factory=psycopg2.extras.DictCursor)


def get_recipients(cur, fuuid):
    cur.execute(
        "SELECT facility_reporters('%s') AS recipients;" % fuuid)
    r = cur2.fetchone()
    if r:
        return r["recipients"]
    return ""


def send_facility_sms(params):  # params has the facility uuid and other params
    print "sending_with_params:", params
    res = requests.get(CONFIG["smsurl"], params=params)
    print res.text
    return res.text


def float_values(m):
    for k, v in m.iteritems():
        if k in ('level', 'facility_count', 'rank'):
            continue
        try:
            m[k] = float(v)
        except:
            pass
    return m


def add_variations(msg):
    """
        adds variations to msg. - becomes decreased, while + becomes increased
        e.g. 'ANC1 visits -80%, and ANC4 visits +20% and PCV3 -12.23'
        become 'ANC1 visits decreased 80%, and ANC4 visits increased 20% and PCV3 decreased 12.23'
    """
    y = re.sub('-(\d{,2})', 'decreased \\1', msg)
    y = re.sub('\+(\d{,2})', 'increased \\1', y)
    return y

if __name__ == '__main__':
    conn = psycopg2.connect(
        "dbname=" + CONFIG["dbname"] + " host= " + CONFIG["dbhost"] + " port=" + CONFIG["dbport"] +
        " user=" + CONFIG["dbuser"] + " password=" + CONFIG["dbpasswd"])
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    # Treatment facilities in Treatment/Control districts
    # cur.execute(
    #     "SELECT uuid, is_treatment_district, facility_values::text FROM facilities_view "
    #     "WHERE is_treatment_facility = 't'")
    # cur.execute("SELECT uuid, is_treatment_district, facility_values::text FROM facilities_view WHERE dhis2id='E1TxjBqAvWB'")
    cur.execute("SELECT uuid, is_treatment_district, facility_values::text FROM facilities_view WHERE dhis2id='H6KWp7l4pOe'")
    res = cur.fetchall()
    print "Total T_T and T_C =>", len(res)
    for r in res:
        stats = json.loads(r["facility_values"])
        stats = stats.get(reporting_period, {})
        stats = float_values(stats)

        if stats:  # only bother if there is a saved record
            current_time = datetime.now()
            for i, m in enumerate(msgs):
                if i == 1 and not r["is_treatment_district"]:  # second message changes based on facility category
                    t = (
                        "These SMS updates will compare your performance over the past "
                        "3 months to your performance at the same time last year.")
                else:
                    if m["has_params"]:
                        t = m["text"] % stats
                    else:
                        t = m["text"]
                    if m["has_variations"]:
                        t = add_variations(t)
                params = {
                    'text': t,
                    'sender': get_recipients(cur2, r["uuid"]),
                    'username': CONFIG['smsuser'],
                    'password': CONFIG['smspasswd'],
                    'fuuid': r["uuid"],  # just to track the facility
                }
                # print params
                sched_time = current_time + timedelta(minutes=SMS_OFFSET_TIME)
                queue_facility_sms(conn, cur, params, sched_time)
                current_time = sched_time

    # Control facilities in treatment districts
    cur.execute(
        "SELECT uuid, facility_values::text FROM facilities_view WHERE is_treatment_facility = 'f' AND "
        "is_treatment_district = 't' LIMIT 1")
    res = cur.fetchall()
    print "Total C_T => ", len(res)
    messages = CONTROL_MESSAGES
    for r in res:
        current_time = datetime.now()
        for m in messages:
            params = {
                'text': m,
                'sender': get_recipients(cur2, r["uuid"]),
                'username': CONFIG['smsuser'],
                'password': CONFIG['smspasswd'],
                'fuuid': r["uuid"],  # just to track the facility
            }
            sched_time = current_time + timedelta(minutes=SMS_OFFSET_TIME)
            queue_facility_sms(conn, cur, params, sched_time)
            current_time = sched_time

    conn.close()
