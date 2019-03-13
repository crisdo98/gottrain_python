import pandas as pd
from pandas.io.json import json_normalize

from utilities.FileUtils import FileUtils
from utilities.Logger import Logger
from utilities.MiscUtils import MiscUtils
from utilities.PasswordUtils import PasswordUtils
from utilities.RestService import RestService

logger = Logger(level="INFO").get_logger()


def get_refdata_from_source(rest_username,
                            rest_encoded_password,
                            auth_url,
                            refdata_url,
                            database_conn,
                            database_schema,
                            database_tablename,
                            output_path,
                            json_key,
                            writefile=False):
    rest_password = PasswordUtils.decode_password(rest_encoded_password)
    rs = RestService(rest_username, rest_password, auth_url)
    headers = rs.get_token_header()

    logger.info(database_tablename + " url: " + refdata_url)

    content = rs.requests_get(refdata_url, headers)
    data = content.decode('windows-1252', errors='ignore')

    filename = database_tablename + "_" + MiscUtils.get_datetime()

    if writefile:
        # save as xml
        FileUtils.write_to_file(output_path, filename, data.encode(), write_type='w', compression=True)

    json_doc = FileUtils.convert_xml_to_json(output_path, filename, data)

    ''' json_normalize and json_doc appear in the eval function 
    below is a hack to keep the imports being automatically removed'''
    dummy = False
    if dummy is True:
        json_normalize(json_doc)

    df = pd.DataFrame.from_dict(eval('json_normalize(json_doc' + json_key + ')'), orient='columns')

    record_count = len(df)

    csv_data = df.to_csv(encoding='UTF-8', sep='\t', header=False, index=False)

    logger.info("Finished processing (%d) '%s' records", record_count, database_tablename)

    logger.info("Starting the bulk load into %s.%s", database_schema, database_tablename)

    database_conn.bulk_load(database_schema + '.' + database_tablename, csv_data, separator="\t")

    logger.info("Successfully bulk loaded (%d) '%s' records into %s.%s",
                record_count,
                database_tablename,
                database_schema,
                database_tablename)
    return record_count
