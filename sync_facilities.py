import psycopg2
import psycopg2.extras
import requests
import json
from settings import config

user = config["dhis2_user"]
passwd = config["dhis2_passwd"]

conn = psycopg2.connect(
    "dbname=" + config["dbname"] + " host= " + config["dbhost"] + " port=" + config["dbport"] +
    " user=" + config["dbuser"] + " password=" + config["dbpasswd"])

cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

cur.execute("SELECT dhis2id, name FROM districts WHERE eligible = %s", ('t'))

URL = "%s.json?level=5&fields=id,uuid,name" % config["orgunits_url"]

payload = {}


def get_url(url):
    res = requests.get(url, params=payload, auth=(user, passwd))
    return res.text

response = get_url(URL)
orgunits_dict = json.loads(response)
orgunits = orgunits_dict['organisationUnits']

for orgunit in orgunits:
    cur.execute("SELECT id FROM facilities WHERE dhis2id = %s", (orgunit["id"]))
    res = cur.fetchone()
    if not res:  # we don't have an entry already
        cur.execute(
            "INSERT INTO facilities(name, dhis2id, uuid) VALUES (%s, %s, %s)",
            (orgunit["name"], orgunit["id"], orgunit["uuid"]))
    else:  # we have the entry
        cur.execute(
            "UPDATE facilities SET name = %s, uuid = %s WHERE dhis2id = %s",
            (orgunit["name"], orgunit["uuid"], orgunit["id"]))
    conn.commit()
conn.close()
