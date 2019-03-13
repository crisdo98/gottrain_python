import argparse
import gzip
import json

import pandas as pd
import requests

from dao.DatabaseConnector import DatabaseConnector
from utilities.FileUtils import FileUtils
from utilities.JobStatus import JobStatus
from utilities.Logger import Logger
from utilities.MiscUtils import MiscUtils
from utilities.PasswordUtils import PasswordUtils

# import logging

logger = Logger(level="INFO").get_logger()
# logger = logging.getLogger('gottrain_logger')

parser = argparse.ArgumentParser()
parser.add_argument("database_config_file",
                    help="Please provide a path to the database configuration file")
parser.add_argument("refdata_config_file",
                    help="Please supply a path to the reference data configuration file")
args = parser.parse_args()

conn_config = FileUtils.load_config(args.database_config_file)

refdata_config = FileUtils.load_config(args.refdata_config_file)

logger.info("database_config_file: " + args.database_config_file)
logger.info("refdata_config_file: " + args.refdata_config_file)

# connection parameters
username = conn_config['rest_trust']['username']
encoded_password = conn_config['rest_trust']['encoded_password']
database_user = conn_config['database']['username']
database_encoded_password = conn_config['database']['encoded_password']
database_host = conn_config['database']['host']
database_name = conn_config['database']['database_name']

# refdata parameters
database_tablename = refdata_config['tablename']
database_landing_schema = refdata_config['landing_schema']
refdata_url = refdata_config['refdata_url']
tablename = refdata_config['tablename']
output_path = refdata_config['output_path']

r = requests.get(refdata_url, auth=(username, PasswordUtils.decode_password(encoded_password)))

if r.status_code != requests.codes.ok:
    raise Exception("Failed to login: %s" % r.text)

filename = tablename + "_" + MiscUtils.get_datetime()
# FileUtils.write_to_file(output_path, filename, r.content, encoding_type=None, suffix='gzip', write_type='wb')
open(output_path + filename + ".gzip", 'wb').write(r.content)

with gzip.open(output_path + filename + ".gzip") as f:
    file_content = json.loads(f.read())
tiplocdata = file_content['TIPLOCDATA']

df = pd.DataFrame.from_dict(tiplocdata)

json_str = json.dumps(tiplocdata)

df = pd.read_json(json_str)
df = df.drop_duplicates(subset=['NLC'], keep=False)

df.apply(lambda x: x.astype(str).str.upper())

database_conn = DatabaseConnector(database_user, database_encoded_password, database_name, database_host)

job_status = JobStatus('LANDING', tablename, database_conn)

# truncate table
database_conn.truncate_table(database_landing_schema, database_tablename)
record_count = len(df)

csv_data = df.to_csv(encoding='UTF-8', sep='\t', header=False, index=False)

logger.info("Finished processing (%d) '%s' records", record_count, database_tablename)

logger.info("Starting the bulk load into %s.%s", database_landing_schema, database_tablename)

database_conn.bulk_load(database_landing_schema + '.' + database_tablename, csv_data, separator="\t")

if record_count >= 0:
    '''logger.info("Successfully bulk loaded (%d) '%s' records into %s.%s",
                record_count,
                database_tablename,
                database_landing_schema,
                database_tablename)'''
    job_status.end_job()
    job_status.insert_stats(record_count, record_count, 0, 0)
else:
    job_status.end_job(status='ERROR')
