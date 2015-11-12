import psycopg2
import psycopg2.extras
import re
import sys
import getopt
import smtplib

from settings import CONFIG, SMS_OFFSET_TIME, DISTRICT_FIRST_MSG, DISTRICT_INTRO
from datetime import datetime, timedelta

from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

TEST_MODE = False
TEST_DISTRICT = 'Amudat'
INTRO_MODE = False
SENDSMS = True
SENDEMAIL = True

cmd = sys.argv[1:]
opts, args = getopt.getopt(
    cmd, 'tinxd:',
    ['testing', 'intro', 'nosms', 'noemial', 'district'])

for option, parameter in opts:
    if option in ('-t', '--testing'):
        TEST_MODE = True
    if option in ('-i', '--intro'):
        INTRO_MODE = True
    if option in ('-d', '--district'):
        TEST_DISTRICT = parameter
    if option in ('-n', '--nosms'):
        SENDSMS = False
    if option in ('-x', '--noemail'):
        SENDEMAIL = False

import logging
logging.basicConfig()

user = CONFIG['dhis2_user']
passwd = CONFIG['dhis2_passwd']
# SMS_OFFSET_TIME = 1  # time in between SMS in minutes.

reporting_period = datetime.now().strftime('%Y-%m')

# To handle Json in DB well
psycopg2.extras.register_default_json(loads=lambda x: x)
psycopg2.extensions.register_adapter(dict, psycopg2.extras.Json)

# msgs is of the form [{"has_params": False, "has_variations": False, "text": ""}, {"has_params": ....}, ]


def checkaddress(email):
    email_regex = re.compile(r'^[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,4}$', re.I)
    if email_regex.match(email):
        return True
    return False


def get_district_emails(cur, district):
    emails = []
    cur.execute(
        "SELECT email FROM district_contacts WHERE district_id = "
        "(SELECT id FROM districts WHERE name = %s)", [district])
    res = cur.fetchall()
    for r in res:
        emails.append(r["email"])
    return filter(checkaddress, emails)


def queue_facility_sms(conn, cur, params, run_time):  # params has the facility uuid and other params
    cur.execute(
        "INSERT INTO schedules (params, run_time) VALUES(%s, %s)", [params, run_time])
    conn.commit()
    return None


def get_recipients(cur, fuuid):
    cur.execute(
        "SELECT get_dhts('%s') AS recipients;" % fuuid)
    r = cur2.fetchone()
    if r:
        return r["recipients"]
    return ""


def float_values(m):
    for k, v in m.iteritems():
        if k in ('level', 'facility_count', 'rank'):
            continue
        try:
            m[k] = float(v)
        except:
            pass
    return m

if __name__ == '__main__':
    email_Str = """
<table>
    <thead>
        <tr><th colspan="13" align="center">%(district)s Monthly Report: Perfomance of Individual Facilities</th></tr>
        <tr>
            <th>Position</th>
            <th align='left'>Facility</th>
            <th> Level</th>
            <th> ANC1 </th>
            <th> ANC4 </th>
            <th> Delivery </th>
            <th> PCV </th>
            <th>Reporting Rate</th>
            <th>Total Score(%%change)</th>
            <th>ANC1(%%change)</th>
            <th>ANC4(%%change)</th>
            <th>Delivery(%%change)</th>
            <th>PCV(%%change)</th>
        </tr>
    </thead>
    <tbody>
        %(body)s
    </tbody>
</table>
"""
    conn2 = psycopg2.connect(
        "dbname=" + CONFIG["mtrack_dbname"] + " host= " + CONFIG["dbhost"] + " port=" + CONFIG["dbport"] +
        " user=" + CONFIG["dbuser"] + " password=" + CONFIG["dbpasswd"])
    cur2 = conn2.cursor(cursor_factory=psycopg2.extras.DictCursor)

    conn = psycopg2.connect(
        "dbname=" + CONFIG["dbname"] + " host= " + CONFIG["dbhost"] + " port=" + CONFIG["dbport"] +
        " user=" + CONFIG["dbuser"] + " password=" + CONFIG["dbpasswd"])
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    if TEST_MODE:
        cur.execute(
            "SELECT id, name FROM districts WHERE is_treatment ='t' and name = %s", [TEST_DISTRICT])
    else:
        cur.execute("SELECT id, name FROM districts WHERE is_treatment ='t'")
    res = cur.fetchall()

    for r in res:
        cur.execute(
            "SELECT rank, facility, flevel, curr_anc1, curr_anc4, curr_delivery, curr_pcv, curr_rr, total_score, "
            "comp_anc1, comp_anc4, comp_delivery, comp_pcv"
            " FROM get_monthly_reports(%s)"
            "WHERE fdistrict = %s", [reporting_period, r["name"]])
        data = cur.fetchall()
        if not data:
            continue

        hcii_data = []
        hciii_data = []

        # build the email table body
        body = ""
        for d in data:
            if d["flevel"] == "HC II":
                hcii_data.append(d)
            else:
                hciii_data.append(d)
            body += (
                "<tr>"
                "<td>%s</td><td>%s</td><td align='right'>%s</td><td align='right'>%s</td>"
                "<td align='right'>%s</td><td align='right'>%s</td><td align='right'>%s</td>"
                "<td align='right'>%s</td><td align='right'>%s</td><td align='right'>%s</td>"
                "<td align='right'>%s</td><td align='right'>%s</td><td align='right'>%s</td>"
                "</tr>" % tuple(d))

        if TEST_MODE:
            email_recipients = CONFIG['email_test_recipients']
        else:
            email_recipients = get_district_emails(cur, r["name"])

        email_Str = email_Str % ({'district': r["name"], 'body': body})
        if INTRO_MODE:
            email_Str = DISTRICT_INTRO % email_Str

        if SENDEMAIL:
            msg = MIMEMultipart('alternative')
            msg['Subject'] = "Monthly Performance Indicator Report"
            msg['From'] = CONFIG["email_sender"]
            msg['To'] = ','.join(email_recipients)

            part1 = MIMEText(DISTRICT_INTRO, 'plain')
            part2 = MIMEText(email_Str, 'html')

            msg.attach(part1)
            msg.attach(part2)
            try:
                smtpserver = smtplib.SMTP("smtp.gmail.com")
                smtpserver.set_debuglevel(True)
                smtpserver.ehlo()
                smtpserver.starttls()
                smtpserver.ehlo()
                smtpserver.login(CONFIG["email_sender"], CONFIG["email_pass"])
                smtpserver.sendmail(CONFIG["email_sender"], ', '.join(email_recipients), msg.as_string())
                smtpserver.quit()
            except smtplib.SMTPAuthenticationError as e:
                print "Unable to send message: %s" % e

        # now build the SMS
        messages = [DISTRICT_FIRST_MSG]
        sms_vals = {'HCII': hcii_data, 'HCIII': hciii_data}
        for k, v in sms_vals.iteritems():
            if len(v) >= 4:
                top = v[:2]
                msg1 = (
                    "%s Monthly Top Performers Report: "
                    "%s is in %s place, %s is in %s place" %
                    (k, top[0]["facility"], top[0]["rank"], top[1]["facility"], top[1]["rank"]))
                messages.append(msg1)

                bottom = v[-2:]
                msg2 = (
                    "%s Monthly Bottom Performers Report: "
                    "%s is in %s place, %s is in %s place" %
                    (k, bottom[0]["facility"], bottom[0]["rank"], bottom[1]["facility"], bottom[1]["rank"]))
                messages.append(msg2)

            elif len(v) == 3 or len(v) == 2:
                msg1 = (
                    "%s Monthly Top Performers Report: "
                    "%s is in %s place." %
                    (k, v[0]["facility"], v[0]["rank"]))
                messages.append(msg1)

                msg2 = (
                    "%s Monthly Bottom Performers Report: "
                    "%s is in %s place." %
                    (k, v[-1]["facility"], v[-1]["rank"]))
                messages.append(msg2)
            else:
                # messages.append("%s is the only reporting %s" % (v[0]["facility"], k))
                pass

        # get message recipients
        recipients = get_recipients(cur2, r["name"])
        current_time = datetime.now()
        if len(messages) > 1:
            for m in messages:
                params = {
                    'text': m,
                    'sender': recipients,
                    'username': CONFIG['smsuser'],
                    'password': CONFIG['smspasswd'],
                    'fuuid': r["name"],  # just to track the facility/district
                }
                sched_time = current_time + timedelta(minutes=SMS_OFFSET_TIME)
                if SENDSMS:
                    queue_facility_sms(conn, cur, params, sched_time)
                current_time = sched_time

    conn2.close()
    conn.close()
