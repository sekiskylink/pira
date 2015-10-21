#!/usr/bin/env python
import psycopg2
import psycopg2.extras
import json
import requests
import logging
from settings import CONFIG

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s [%(process)d] %(levelname)-4s:  %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    # filename='/var/log/mtsms/mtsms.log',
    filename='/var/log/pira/pira_sched.log',
    filemode='a'
)

# To handle Json in DB well
psycopg2.extras.register_default_json(loads=lambda x: x)
psycopg2.extensions.register_adapter(dict, psycopg2.extras.Json)

conn = psycopg2.connect(
    "dbname=" + CONFIG["dbname"] + " host= " + CONFIG["dbhost"] + " port=" + CONFIG["dbport"] +
    " user=" + CONFIG["dbuser"] + " password=" + CONFIG["dbpasswd"])
cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

conn2 = psycopg2.connect(
    "dbname=" + CONFIG["mtrack_dbname"] + " host= " + CONFIG["dbhost"] + " port=" + CONFIG["dbport"] +
    " user=" + CONFIG["dbuser"] + " password=" + CONFIG["dbpasswd"])
cur2 = conn2.cursor(cursor_factory=psycopg2.extras.DictCursor)


def send_facility_sms(params):  # params has the facility uuid and other sms params
    res = requests.get(CONFIG["smsurl"], params=params)
    return res.text

cur.execute(
    "SELECT id, params::text FROM schedules WHERE to_char(run_time, 'yyyy-mm-dd HH:MI')"
    " = to_char(now(), 'yyyy-mm-dd HH:MI') "
    " AND status = 'ready' FOR UPDATE NOWAIT")
res = cur.fetchall()
sched_len = len(res)
logging.info("Scheduler got %s SMS to send out" % sched_len)
for r in res:
    # cur.execute("SELECT id FROM schedules WHERE id = %s FOR UPDATE NOWAIT", [r["id"]])
    params = json.loads(r["params"])
    try:
        response = send_facility_sms(params)
        status = 'completed' if response == 'Accepted' else 'failed'
        cur.execute("UPDATE schedules SET status = %s WHERE id = %s", [status, r["id"]])
        conn.commit()
        logging.info(
            "Scheduler run: [schedid:%s] [status:%s] [msg:%s]" % (r["id"], status, params["text"]))
    except Exception as e:
        logging.error("Scheduler Failed on [schedid:%s], [reason:%s]" % (r["id"], str(e)))
conn.close()
