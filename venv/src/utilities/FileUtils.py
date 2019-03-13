import bz2
import json
import logging

import xmltodict

logger = logging.getLogger('gottrain_logger')


class FileUtils:

    @staticmethod
    def convert_xml_to_json(output_path, filename, xml_data, encoding='UTF-8', indent=6, write_file=False):
        jsoned_xml = json.dumps(xmltodict.parse(xml_data, encoding=encoding, xml_attribs=False), indent=indent)
        jsoned_xml_loads = json.loads(jsoned_xml)
        if write_file:
            FileUtils.write_to_file(output_path, filename, jsoned_xml, suffix='json', write_type='w')
        return jsoned_xml_loads

    @staticmethod
    def write_to_file(output_path, filename, data, suffix='xml', encoding_type='UTF-8', write_type='wb',
                      compression=False):
        if compression:
            suffix = 'bz2'
            with bz2.open(output_path + filename + '.' + suffix,
                          mode=write_type,
                          encoding=None) as f:
                f.write(data)
            logger.info("Successfully wrote data to file: %s%s.%s", output_path, filename, suffix)
        else:
            with open(output_path + filename + '.' + suffix,
                      mode=write_type,
                      encoding=encoding_type,
                      compresslevel=9,
                      compression='gzip') as f:
                f.write(data)
            logger.info("Successfully wrote data to file: %s%s.%s", output_path, filename, suffix)

    @staticmethod
    def load_config(filepath):
        with open(filepath, 'r') as f:
            config = json.loads(f.read())
        return config
