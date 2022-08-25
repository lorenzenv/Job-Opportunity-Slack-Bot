import requests
import logging
import time
import psycopg2
from datetime import datetime
import os

# wait for 10 seconds so the get_jobs.py has time to pull the job offers and etl.py has time to establish the postgres db
time.sleep(10)

# get global variables from .env
webhook_url = os.getenv('WEBHOOK')
conn_pw = os.getenv('PASSWORD')

# establish a psycopg2 connection to the postgres db
conn = psycopg2.connect(
   database="agentur_jobs", user='postgres', password=conn_pw, host='postgresdbyall', port='5432'
)

# update automatically
conn.autocommit = True

# create the curser object
cursor = conn.cursor()

# function to post job offers on Slack
def post_on_slack(result):

    title = result[1]
    company = result[7]
    plz = str(result[6])
    city = result[5]

    post_date = str(result[3])
    post_date_obj = datetime.strptime(post_date, '%Y-%m-%d %H:%M:%S')
    post_date_print = str(post_date_obj.strftime("%b %d, %Y"))

    start_date = str(result[4])
    start_date_obj = datetime.strptime(start_date, '%Y-%m-%d %H:%M:%S')
    start_date_print = str(start_date_obj.strftime("%b %d, %Y"))

    url = "https://www.arbeitsagentur.de/jobsuche/suche?angebotsart=1&was=" + str(result[2]) + "&id=12288-2548913470-S"

    data = {
                "blocks": [
                    {
                        "type": "section",
                        "text": {
                            "type": "mrkdwn",
                            "text": "*" + title + "* :ghost: \n*Company:* " + company + "\n*In:* " + plz + ", " + city + "\n*Posted on:* " + post_date_print + "\n*Start date:* " + start_date_print + "\n<" + url + "|Go to the Arbeitsagentur listing>"
                                }
                    }
                    
                ]
            }
    # push to Slack
    requests.post(url=webhook_url, json = data)

# continiuously running while loop that checks if there are new job offers and runs the post_on_slack function
while True:
    cursor.execute('''SELECT * from jobs''')
    result_new = cursor.fetchone();
    if result_new:
        post_on_slack(result_new)
        logging.warning("job posted on slack")
        last_refnr = result_new[2]
        cursor.execute('''DELETE FROM jobs''')
    else:
        logging.warning("no new job to post on slack")
    # sleep for one hour
    time.sleep(3600)