import argparse
import logging

from dao.DatabaseConnector import DatabaseConnector
from utilities.FileUtils import FileUtils
from utilities.JobStatus import JobStatus
from utilities.RefdataLanding import get_refdata_from_source

logger = logging.getLogger('gottrain_logger')

parser = argparse.ArgumentParser()
parser.add_argument("database_config_file",
                    help="Please provide a path to the database configuration file")
parser.add_argument("refdata_config_file",
                    help="Please supply a path to the reference data configuration file")
args = parser.parse_args()
logger.info("database_config_file: " + args.database_config_file)
logger.info("refdata_config_file: " + args.refdata_config_file)

conn_config = FileUtils.load_config(args.database_config_file)

refdata_config = FileUtils.load_config(args.refdata_config_file)

logger.info("database_config_file: " + args.database_config_file)
logger.info("refdata_config_file: " + args.refdata_config_file)

# connection parameters
username = conn_config['rest_darwin']['username']
encoded_password = conn_config['rest_darwin']['encoded_password']
authenticate_url = conn_config['rest_darwin']['auth_url']
database_user = conn_config['database']['username']
database_encoded_password = conn_config['database']['encoded_password']
database_host = conn_config['database']['host']
database_name = conn_config['database']['database_name']

# refdata parameters
database_landing_schema = refdata_config['landing_schema']
tablename = refdata_config['tablename']
json_key = refdata_config['type_key']
refdata_url = refdata_config['refdata_url']
output_path = refdata_config['output_path']

database_conn = DatabaseConnector(database_user, database_encoded_password, database_name, database_host)

job_status = JobStatus('LANDING', tablename, database_conn)

# truncate table
database_conn.truncate_table(database_landing_schema, tablename)

record_count = get_refdata_from_source(username,
                                       encoded_password,
                                       authenticate_url,
                                       refdata_url,
                                       database_conn,
                                       database_landing_schema,
                                       tablename,
                                       output_path,
                                       json_key,
                                       writefile=True)

job_status.end_job()
job_status.insert_stats(record_count, record_count, 0, 0)
