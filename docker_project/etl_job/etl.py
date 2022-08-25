import time
import pymongo
from sqlalchemy import create_engine
import logging
import os

# wait 5 seconds so the get_jobs.py has time to pull the job offers
time.sleep(5)  

# get password from .env file
pg_pw = os.getenv('PASSWORD')

# postgres engine
pg = create_engine(f"postgresql://postgres:{pg_pw}@postgresdbyall:5432/agentur_jobs", echo=True)

# create table in postgres
pg.execute('''
    CREATE TABLE IF NOT EXISTS jobs (
    beruf TEXT,
    titel TEXT,
    refnr TEXT,
    datum TIMESTAMP,
    eintrittsdatum TIMESTAMP,
    arbeitsort TEXT,
    plz INT,
    arbeitgeber TEXT,
    hash_id TEXT
);
''')

# establish a connection to the MongoDB server and 
client = pymongo.MongoClient(host="mongodb", port=27017)
db = client.agentur_jobs

# define last_refnr to store the refnr of the most recent job offer
last_refnr = 0

# continiuous while loop checking for new jobs and adding them to the postgres db
while True:
    logging.warning("checking for new jobs")
    # get the most recent job
    new_jobs = db.jobs.find(limit=1)
    for job in new_jobs:
        if job['refnr'] != last_refnr:
            beruf = job['beruf']
            titel = job['titel']
            refnr = job['refnr']
            datum = job['aktuelleVeroeffentlichungsdatum']
            eintrittsdatum = job['eintrittsdatum']
            arbeitsort = job['arbeitsort']['ort']
            plz = job['arbeitsort']['plz']
            arbeitgeber = job['arbeitgeber']
            hash_id = job['hashId']
            query = "INSERT INTO jobs VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);"
            pg.execute(query, (beruf, titel, refnr, datum, eintrittsdatum, arbeitsort, plz, arbeitgeber, hash_id))
            logging.warning("new job added")
            last_refnr = refnr
        else:
            logging.warning("no new jobs")
    # sleep for one hour
    time.sleep(3600)
    


