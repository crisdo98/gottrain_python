import argparse

from psycopg2 import sql

from dao.DatabaseConnector import DatabaseConnector
from utilities.FileUtils import FileUtils
from utilities.JobStatus import JobStatus
from utilities.Logger import Logger

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
database_user = conn_config['database']['username']
database_encoded_password = conn_config['database']['encoded_password']
database_host = conn_config['database']['host']
database_name = conn_config['database']['database_name']

# refdata parameters
database_landing_schema = refdata_config['landing_schema']
database_staging_schema = refdata_config['staging_schema']
tablename = refdata_config['tablename']
refdata_url = refdata_config['refdata_url']

database_conn = DatabaseConnector(database_user, database_encoded_password, database_name, database_host)

job_status = JobStatus('STAGING', tablename, database_conn)

# truncate table
database_conn.truncate_table(database_staging_schema, tablename)

sql_query = sql.SQL("INSERT INTO {}.{} SELECT *, md5({}::text), now() FROM {}.{}").format(
    sql.Identifier(database_staging_schema),
    sql.Identifier(tablename),
    sql.Identifier(tablename),
    sql.Identifier(database_landing_schema),
    sql.Identifier(tablename))

rows = database_conn.execute_query(sql_query)

if rows != -1:
    job_status.end_job()
    job_status.insert_stats(rows, rows, 0, 0)
else:
    job_status.end_job('ERROR')
