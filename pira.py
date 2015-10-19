#!/usr/bin/env python
import os
import sys
import tempfile
import requests
import pandas
import numpy as np
import psycopg2
import psycopg2.extras
import json
import simplejson
import datetime
from settings import CONFIG, MONTHLY_MESSAGES, SMS_OFFSET_TIME
# import pprint
import re
import getopt
import time

TEST_MODE = False
SENDSMS = True
TEST_DISTRICT_ID = 'x75Yh65MaUa'

cmd = sys.argv[1:]
opts, args = getopt.getopt(
    cmd, 'tnd:',
    ['testing', 'nosms'])

for option, parameter in opts:
    if option in ('-t', '--testing'):
        TEST_MODE = True
    if option in ('-n', '--nosms'):
        SENDSMS = False
    if option == '-d':
        TEST_DISTRICT_ID = parameter


def get_indicator_values(data, indicator, offset, current=False):
    """
    Takes a row from the csv file, the type of indicator, offset for
    the corresponding values across the 3 months period.
    current is true if we're picking values for current year, else previous year
    It returns a list of 3 values for the indicator across the 3 months period
    """
    initial_index = first_field_index[indicator]
    if not current:
        return data[initial_index], data[initial_index + offset], data[initial_index + (offset * 2)]
    else:
        return (
            data[initial_index + DATALEN],
            data[initial_index + offset + DATALEN], data[initial_index + (offset * 2) + DATALEN])


def get_reporting_rate(data, current=False):
    """
    Reporting Rate = (No of expected reports - No of NaNs)/ (No of expected reports) * 100
    v in this function = expected reports
    Returns Reporting Rate
    """
    if not current:
        v = data[FIRST_DATAFIELD_INDEX: (FIRST_DATAFIELD_INDEX + DATALEN)]
        numerator = len([i for i in v if not np.isnan(i)])
        rr = (float(numerator) / DATALEN) * 100
        return float("%.2f" % rr)
    else:
        v = data[(FIRST_DATAFIELD_INDEX + DATALEN):]
        numerator = len([i for i in v if not np.isnan(i)])
        rr = (float(numerator) / DATALEN) * 100
        return float("%.2f" % rr)


def avg_of_list(l):
    if not l:
        return 0
    mean = np.mean(l)
    return float("%.2f" % mean)


def get_facility_level(cur, dhis2id):
    cur.execute("SELECT level FROM facilities WHERE dhis2id = %s", [dhis2id])
    res = cur.fetchone()
    if res:
        return res["level"]
    return None


def generate_period_string():
    """Return 3 months period string for both the current report
    and similar report a year ago
    e.g 201404;201405;201406;201504;201505;201506 for July 2015"""
    t = datetime.datetime.now()
    year = t.year
    month = t.month
    if month <= 4:
        third = (month - 4) % 12 if (month - 4) < 0 else 12 if (month - 4) == 0 else (month - 4)
        second = (month - 3) % 12 if (month - 3) < 0 else 12 if (month - 3) == 0 else (month - 3)
        first = (month - 2) % 12 if (month - 2) < 0 else 12 if (month - 2) == 0 else (month - 2)

        str1 = "%s%02d;%s%02d;%s%02d" % (year - 1, third, year - 1, second, year - 1, first)
        str2 = "%s%02d;%s%02d;%s%02d" % (year, third, year, second, year, first)
    else:
        str1 = "%s%02d;%s%02d;%s%02d" % (year - 1, month - 4, year - 1, month - 3, year - 1, month - 2)
        str2 = "%s%02d;%s%02d;%s%02d" % (year, month - 4, year, month - 3, year, month - 2)
    return "%s;%s" % (str1, str2)


def get_url(url):
    res = requests.get(url, params=payload, auth=(user, passwd))
    return res.text

usecols = range(0, 28)
# remove 3rd and 4th field
usecols.remove(2)
usecols.remove(3)


def read_csv_to_file(url):
    res = requests.get(url, params=payload, auth=(user, passwd))
    f = tempfile.NamedTemporaryFile(delete=False)  # create temporary file
    fname = f.name
    for chunck in res.iter_content(1024):
        f.write(chunck)
    f.close()
    pobj = pandas.read_csv(fname, usecols=usecols)
    os.unlink(fname)
    return pobj


def send_facility_sms(params):  # params has the facility uuid and other params
    res = requests.get(CONFIG["smsurl"], params=params)
    return res.text


def read_csv_to_file2(url):
    datafile = os.path.dirname(__file__) + os.path.sep + "datax1.csv"
    pobj = pandas.read_csv(datafile)
    return pobj


def ordinal(n):
    return str(int(n)) + (
        "th" if 4 <= n % 100 <= 20 else {1: "st", 2: "nd", 3: "rd"}.get(n % 10, "th"))


def save_facility_record(conn, cur, fuuid, record):
    if "comment" in record:
        record.pop("comment")
    if "month" in record:
        record.pop("month")
    cur.execute("SELECT previous_values::text FROM facilities WHERE uuid = %s", [fuuid])
    res = cur.fetchone()
    if res:
        prev_vals = json.loads(res['previous_values'])
        prev_vals['%s' % datetime.datetime.now().strftime('%Y-%m')] = record
        cur.execute(
            "UPDATE facilities SET previous_values = %s WHERE uuid = %s",
            [psycopg2.extras.Json(prev_vals, dumps=simplejson.dumps), fuuid])
        conn.commit()


def queue_facility_sms(conn, cur, params, run_time):  # params has the facility uuid and other params
    cur.execute(
        "INSERT INTO schedules (params, run_time) VALUES(%s, %s)",
        [psycopg2.extras.Json(params, dumps=simplejson.dumps), run_time])
    conn.commit()
    return None


def float_values(m):
    for k, v in m.iteritems():
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


def facility_sms_reports(facility_id, report):
    report = float_values(report)
    msgs = []
    for m in MONTHLY_MESSAGES:
        if not m["has_params"]:
            msgs.append(m["text"])
        else:  # has some parameters
            if m["has_variations"]:
                msgs.append(add_variations(m["text"] % report))
            else:
                msgs.append(m["text"] % report)
    return msgs

# BASE_URL will actuall point to the pivot table download we need
BASE_URL = CONFIG['base_url']
BASE_URL = BASE_URL + "dimension=pe:%s&" % generate_period_string()
BASE_URL = BASE_URL + "dimension=dx:yTtv6wuTWUN;eGSUL2aL0zW;OWJ3hkJ9VYA;iNVDqc0xKi0&dimension=ou:LEVEL-5;%s"
payload = {
    "filter": "u8EjsUj11nz:jTolsq2vJv8;GM7GlqjfGAW;luVzKLwlHJV",
    "tableLayout": "true",
    "columns": "pe;dx",
    "rows": "ou",
}

user = CONFIG['dhis2_user']
passwd = CONFIG['dhis2_passwd']

OFFSET = 4  # synonymous with number of indicators
DATALEN = 12  # 12 because for each year it is 4 indicators x 3 months
first_field_index = {
    'anc4': 2,
    'anc1': 3,
    'delivery': 4,
    'pcv': 5
}
FIRST_DATAFIELD_INDEX = 2

# connection to PIRA DB
conn = psycopg2.connect(
    "dbname=" + CONFIG["dbname"] + " host= " + CONFIG["dbhost"] + " port=" + CONFIG["dbport"] +
    " user=" + CONFIG["dbuser"] + " password=" + CONFIG["dbpasswd"])
cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

# connect to mTrac DB
conn2 = psycopg2.connect(
    "dbname=" + CONFIG["mtrack_dbname"] + " host= " + CONFIG["dbhost"] + " port=" + CONFIG["dbport"] +
    " user=" + CONFIG["dbuser"] + " password=" + CONFIG["dbpasswd"])
cur2 = conn2.cursor(cursor_factory=psycopg2.extras.DictCursor)

if TEST_MODE:
    cur.execute("SELECT dhis2id, name FROM districts WHERE dhis2id = %s", [TEST_DISTRICT_ID])
else:
    cur.execute("SELECT dhis2id, name, is_treatment FROM districts WHERE eligible = 't'")
res = cur.fetchall()

# START District Code
for r in res:
    print "#################>", r["dhis2id"]
    # data = pandas.read_csv('datax1.csv', usecols=usecols)
    try:
        data = read_csv_to_file(BASE_URL % r["dhis2id"])
    except Exception as e:
        print "failed for district", r["dhis2id"]
        print str(e)
        continue
    vals = (data.values)
    # This stats guy holds the data just as we need it.
    stats_dict = {}
    hcii_scores = {}  # to keep the ranks for HC IIs
    hciii_scores = {}  # to keep the rank scores for HC IIIs
    for v in vals:  # XXX
        print v[0]
        # first element is a facility id
        facility_id = v[0]
        # let's immediateltly skip those not in the survey
        cur.execute("SELECT eligible FROM facilities WHERE dhis2id = %s ", [facility_id])
        fres = cur.fetchone()
        if fres:
            if not fres["eligible"]:
                continue
        stats_dict[facility_id] = {}
        total_score = 0
        for k in first_field_index.keys():
            prev = [i for i in get_indicator_values(v, k, OFFSET) if not np.isnan(i)]
            current = [i for i in get_indicator_values(v, k, OFFSET, True) if not np.isnan(i)]
            prev_val = avg_of_list(prev)
            curr_val = avg_of_list(current)

            stats_dict[facility_id]['prev_%s' % k] = prev_val
            stats_dict[facility_id]['curr_%s' % k] = curr_val
            # comparison for this indicator
            if prev_val > 0:
                comp = (float(curr_val - prev_val) / prev_val) * 100
            elif curr_val > 0:
                comp = 100
            else:
                comp = 0
            stats_dict[facility_id]['comp_%s' % k] = ("%.2f" % comp)
            total_score += comp
            # print v[0], k, "prev=>", avg_of_list(prev), "current=>", avg_of_list(current)
        prev_rr = get_reporting_rate(v)
        curr_rr = get_reporting_rate(v, True)
        stats_dict[facility_id]['prev_rr'] = prev_rr
        stats_dict[facility_id]['curr_rr'] = curr_rr

        # comparison for reporting rate
        if prev_rr > 0:
            comp = (float(curr_rr - prev_rr) / prev_rr) * 100
        elif curr_rr > 0:
            comp = 100
        else:
            comp = 0
        stats_dict[facility_id]['comp_rr'] = float("%.2f" % comp)
        # total_score += comp  # we're ignoring rr as part of total score
        stats_dict[facility_id]['total_score'] = ("%.2f" % (float(total_score) / 4))
        facility_level = get_facility_level(cur, facility_id)
        stats_dict[facility_id]['rank'] = ''
        stats_dict[facility_id]['level'] = facility_level
        stats_dict[facility_id]['facility_count'] = 0
        # print facility_level
        if facility_level:
            if facility_level == 'HC II':
                hcii_scores[facility_id] = (float(total_score) / 4)
            elif facility_level == 'HC III':
                hciii_scores[facility_id] = (float(total_score) / 4)

    try:  # just in case we have no facilities eligible
        # Lets immediately Rank HC II - using pandas
        hcii_obj = pandas.DataFrame(hcii_scores.items(), columns=['Facility', 'Rank'])
        sorted_hcii = hcii_obj.sort(['Rank'], ascending=[False])  # HC II Ranks sorted in desc
        fcount = len(sorted_hcii.values)
        # Now update stats_dict with the proper ranks on the scores
        for idx, v in enumerate(sorted_hcii.values):  # for HC II
            position = idx + 1
            stats_dict[v[0]]['rank'] = ordinal(position)  # v[0] = facility_id
            stats_dict[v[0]]['facility_count'] = fcount
    except:
        pass

    try:  # just in case we have no HC III  facilities eligible -> next line dies
        # Lets immediately Rank HC III - using pandas
        hciii_obj = pandas.DataFrame(hciii_scores.items(), columns=['Facility', 'Rank'])
        sorted_hciii = hciii_obj.sort(['Rank'], ascending=[False])  # HC III Ranks sorted in desc
        fcount = len(sorted_hciii.values)

        # Now update stats_dict with the proper ranks on the scores
        # print sorted_hciii.values
        for idx, v in enumerate(sorted_hciii.values):  # for HC III
            position = idx + 1
            stats_dict[v[0]]['rank'] = ordinal(position)  # v[0] = facility_id
            stats_dict[v[0]]['facility_count'] = fcount
    except:
        pass

    # pprint.pprint(stats_dict)
    # pprint.pprint(hcii_scores)
    # pprint.pprint(hciii_scores)

    for facilityid, stats in stats_dict.iteritems():
        cur.execute(
            "SELECT id, uuid, is_treatment FROM facilities WHERE "
            "dhis2id = '%s' FOR UPDATE NOWAIT" % facilityid)
        result = cur.fetchone()
        if result:
            facility_uuid = result["uuid"]
            is_treatment = result["is_treatment"]  # to determine who to SMS
        else:  # fetch it from dhis2
            is_treatment = False
            facilityurl = CONFIG["orgunits_url"] + "/" + facilityid + ".json"
            payload = {'fields': 'id,uuid,name'}
            try:
                resp = requests.get(facilityurl, params=payload, auth=(CONFIG["dhis2_user"], CONFIG["dhis2_passwd"]))
                f = json.loads(resp.text)
                if 'uuid' in f:
                    facility_uuid = f["uuid"]
            except:
                facility_uuid = ''
        # only generate report if we have a valid facility uuid
        if facility_uuid:
            report = stats
            # message = CONFIG["facility_report_template"] % report

            if is_treatment:  # send only to treatment facilities
                msgs = facility_sms_reports(facilityid, stats)
                current_time = datetime.datetime.now()
                for m in msgs:
                    params = {
                        'fuuid': facility_uuid,
                        'text': m,
                        'username': CONFIG['smsuser'],
                        'password': CONFIG['smspasswd'],
                        'district': r["name"],
                    }
                    sched_time = current_time + datetime.timedelta(minutes=SMS_OFFSET_TIME)
                    if SENDSMS:
                        queue_facility_sms(conn, cur, params, sched_time)
                    current_time = sched_time
                    # send_facility_sms(params)
            # save report whether treatment or control
            save_facility_record(conn, cur, facility_uuid, report)

    # sleep in between districts
    time.sleep(5)

# END District Code
conn.close()
