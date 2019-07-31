import io
import logging

import psycopg2
from psycopg2 import sql

from utilities.PasswordUtils import PasswordUtils

logger = logging.getLogger('gottrain_logger')


class DatabaseConnector:

    def __init__(self, user, encoded_password, database_name, host, port=5432, conn=None):
        self.user = user
        self.password = PasswordUtils.decode_password(encoded_password)
        self.database_name = database_name
        self.host = host
        self.port = port
        self.conn = conn
        self.logger = logging.getLogger('gottrain_logger')

    def set_connection(self):
        self.conn = psycopg2.connect(user=self.user,
                                     password=self.password,
                                     host=self.host,
                                     port=self.port,
                                     dbname=self.database_name)

    def get_connection(self):
        if self.conn is None:
            self.set_connection()
        return self.conn

    def execute_query(self, sql_query, *params):
        rowcount = -1
        try:
            self.set_connection()
            logger.info("Attempting to execute query: " + sql_query.as_string(self.conn))
            with self.conn.cursor() as cur:
                try:
                    cur.execute(sql_query, params)
                    rowcount = cur.rowcount
                    logger.info("Successfully executed query: " + sql_query.as_string(self.conn))
                except psycopg2.Error:
                    self.logger.error("Unable to insert statement")
                self.conn.commit()
        except (Exception, psycopg2.Error) as error:
            self.logger.error("Error while connecting to PostgreSQL", error)
        finally:
            # closing database connection.
            if self.conn:
                self.conn.close()
                self.logger.debug("PostgreSQL connection is closed")
            return rowcount

    def select_query(self, sql_query, *params):
        try:
            self.get_connection()
            logger.info("Attempting to execute query: " + sql_query.as_string(self.conn))
            with self.conn.cursor() as cur:
                try:
                    cur.execute(sql_query, params)
                    value = cur.fetchone()[0]
                except psycopg2.Error:
                    self.logger.error("Unable to execute select statement")
                self.conn.commit()
        except (ConnectionRefusedError, psycopg2.Error) as error:
            self.logger.error("Connection Refused to PostgreSQL")
            self.logger.error("Exiting application.")
            exit(1)
        except BaseException:
            self.logger.error("Error connecting to PostgreSQL database")
            self.logger.error("Exiting application")
            exit(2)
        finally:
            # closing database connection.
            if self.conn:
                self.conn.close()
                self.logger.debug("PostgreSQL connection is closed")
        return value

    def get_next_job_id(self):
        database_schema = "reference"
        table_name = "job_sequence"
        sql_query = sql.SQL("SELECT nextval('{}.{}')").format(
            sql.Identifier(database_schema),
            sql.Identifier(table_name))
        return self.select_query(sql_query)

    def start_job_status(self, job_id, layer, tname, start_time):
        database_schema = "reference"
        table_name = "job_status"
        sql_query = sql.SQL("INSERT INTO {}.{} (job_id, layer, tablename, start_time, status) "
                            "values ({}, {}, {}, {}, 'STARTED')").format(
            sql.Identifier(database_schema),
            sql.Identifier(table_name),
            sql.Literal(job_id),
            sql.Literal(layer),
            sql.Literal(tname),
            sql.Literal(start_time))
        return self.execute_query(sql_query)

    def update_job_status(self, end_time, job_id, status='FINISHED'):
        database_schema = "reference"
        tablename = "job_status"
        sql_query = sql.SQL("UPDATE {}.{} set end_time={}, status={} WHERE job_id={}").format(
            sql.Identifier(database_schema),
            sql.Identifier(tablename),
            sql.Literal(end_time),
            sql.Literal(status),
            sql.Literal(job_id)
        )
        return self.execute_query(sql_query)

    def insert_job_counts(self, job_id, layer, tname, start_time, end_time, source, inserts, updates, deletes):
        database_schema = "reference"
        table_name = "job_counts"
        sql_query = sql.SQL("INSERT INTO {}.{} values({}, {}, {}, {}, {}, {}, {}, {}, {})").format(
            sql.Identifier(database_schema),
            sql.Identifier(table_name),
            sql.Literal(job_id),
            sql.Literal(layer),
            sql.Literal(tname),
            sql.Literal(start_time),
            sql.Literal(end_time),
            sql.Literal(source),
            sql.Literal(inserts),
            sql.Literal(updates),
            sql.Literal(deletes)
        )
        return self.execute_query(sql_query)

    def truncate_table(self, schema_name, table_name):
        sql_query = sql.SQL("TRUNCATE TABLE {}.{}").format(
            sql.Identifier(schema_name),
            sql.Identifier(table_name))
        self.set_connection()
        self.execute_query(sql_query)

    def bulk_load(self, table_name, sql_data, separator='\t', expert=True):
        data = io.StringIO(sql_data)
        try:
            self.set_connection()
            with self.conn.cursor() as cur:
                try:
                    if expert:
                        cur.copy_expert("COPY "
                                        + table_name
                                        + " FROM STDIN WITH (FORMAT CSV, DELIMITER '"
                                        + separator
                                        + "')", data)
                    else:
                        cur.copy_from(data, table_name, sep=separator, null="")
                except psycopg2.Error as e:
                    self.logger.error("Unable to insert statement", e)

                self.conn.commit()
        except (Exception, psycopg2.Error) as error:
            self.logger.error("Error while connecting to PostgreSQL", error)
        finally:
            # closing database connection.
            if self.conn:
                self.conn.close()
                self.logger.debug("PostgreSQL connection is closed")
