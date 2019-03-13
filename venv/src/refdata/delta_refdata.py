import argparse

from psycopg2 import sql

from dao.DatabaseConnector import DatabaseConnector
from utilities.FileUtils import FileUtils
from utilities.JobStatus import JobStatus
from utilities.Logger import Logger

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
database_staging_schema = refdata_config['staging_schema']
database_ref_schema = refdata_config['refdata_schema']
tablename = refdata_config['tablename']
source_key = refdata_config['source_key']

database_conn = DatabaseConnector(database_user, database_encoded_password, database_name, database_host)

job_status = JobStatus('REFERENCE', tablename, database_conn)

# delete records from ref table where the data has changed ready to refresh
update_query = sql.SQL("DELETE FROM {}.{} a WHERE EXISTS (SELECT 1 FROM {}.{}"
                       " WHERE a.{} = {} and a.md5key <> md5key)").format(
    sql.Identifier(database_ref_schema),
    sql.Identifier(tablename),
    sql.Identifier(database_staging_schema),
    sql.Identifier(tablename),
    sql.Identifier(source_key),
    sql.Identifier(source_key))

updates = database_conn.execute_query(update_query)

# insert new and updated records as new rows
insert_query = sql.SQL("INSERT INTO {}.{} SELECT * FROM {}.{} a WHERE a.{} NOT IN (SELECT {} FROM {}.{})").format(
    sql.Identifier(database_ref_schema),
    sql.Identifier(tablename),
    sql.Identifier(database_staging_schema),
    sql.Identifier(tablename),
    sql.Identifier(source_key),
    sql.Identifier(source_key),
    sql.Identifier(database_ref_schema),
    sql.Identifier(tablename))

inserts = (database_conn.execute_query(insert_query)) - updates

delete_query = sql.SQL("DELETE FROM {}.{} a WHERE NOT EXISTS (SELECT 1 FROM {}.{}"
                       " WHERE a.{} = {})").format(
    sql.Identifier(database_ref_schema),
    sql.Identifier(tablename),
    sql.Identifier(database_staging_schema),
    sql.Identifier(tablename),
    sql.Identifier(source_key),
    sql.Identifier(source_key))

deletes = database_conn.execute_query(delete_query)

# logger.info("Updates: %d, Inserts %d, Deletes: %d", updates, inserts, deletes)

job_status.end_job()
job_status.insert_stats(0, inserts, updates, deletes)
