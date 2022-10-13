import os
import sys
from datetime import datetime
from unittest import TestCase
import pytz
import xml
import xmlschema
from lxml import etree

from asdu.data_load.message_parts import Message, RecordType

script_location = os.path.dirname(__file__)
sys.path.append(script_location)


class TestMessage(TestCase):
    def test_get_from_setup_file(self):
        message = Message.get_from_setup_file(
            f'{script_location}/setup/RV_data.xml')
        message.header.generated_at = datetime.now()
        message.write_to_xml_file(messages_root_folder='C:/Projects/Python/fast_tools_python/src/asdu/data_load/setup/',
                                  short_representation_mode=False)
        print('dd')


class TestRecordType(TestCase):
    def test_get_closest_ref_time(self):
        time_stamp = datetime.now()

        print(time_stamp.isoformat())
        mes_types = [RecordType.MIN_5, RecordType.DAY, RecordType.MONTH, RecordType.HOUR_2]

        for mes_type in mes_types:
            print(mes_type.value, ':', mes_type.get_ref_time_from_store_time(), '->', mes_type.get_next_store_time())



class TestMessage(TestCase):
    def test_xml_schema(self):
        xmlschema_doc = etree.parse('C:/Projects/Python/fast_tools_python/src/asdu/data_load/setup/schemas\PT2H.xsd')
        xmlschema = etree.XMLSchema(xmlschema_doc)

        xml_doc = etree.parse('C:\Projects\Python/fast_tools_python\src/asdu\data_load\data\P_BLG.PT5M.RT.V1_2022_01_10_12_55_00.xml')
        result = xmlschema.validate(xml_doc)

        print(result)



    def test_xml_schema_1(self):
        result = xmlschema.validate(
            'C:\Projects\Python/fast_tools_python\src/asdu\data_load\data\G_PRBB.PT5M.RT.V1_2022_01_10_12_50_00.xml'
            , 'C:/Projects/Python/fast_tools_python/src/asdu/data_load/setup/schemas\PT5M.xsd')
        print(result)


    def test_time_conversion(self):
        timestamp=datetime.now()


        iso_local =timestamp.astimezone()
        print(iso_local.isoformat('T', 'seconds'))
        iso_not_local = timestamp.astimezone(pytz.timezone("Asia/Yakutsk"))
        print(iso_not_local.isoformat('T', 'seconds'))

        print(iso_local.timestamp())
        print(iso_not_local.timestamp())


        #Europe/Moscow
        #Asia/Yakutsk


