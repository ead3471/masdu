from __future__ import annotations
import glob
import logging
import time
import xml
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from enum import Enum
from xml.dom import minidom
from xml.etree.ElementTree import Element, SubElement, ElementTree
import dss
import pytz

from ft_history import FastToolsItem
import dateutil.parser
import os

messages_timezone = pytz.timezone("Europe/Moscow")


class InputSource(Enum):
    """Признак источника значения параметра 0 – ИС (первичный),1 – Ручной ввод,2 – Вычисленное."""
    RAW = '0'
    MAN = '1'
    CALC = '2'


class InformationType(Enum):
    REGIME = 'RT'
    PLAN = 'PL'
    BALANCE = 'UB'
    PRODUCTION = 'PRO'

    @classmethod
    def get_from_file_name(cls, file_name):
        """
        get message information type from given file
        :param file_name: message setup file name
        :return: InformationType
        """
        for record in InformationType:
            if record.value in file_name:
                return record
        logging.warning(f"Not found info type for file{file_name} return REGIME")
        return InformationType.REGIME


class RecordType(Enum):
    """
    Record file type
    """
    REAL = 'RV'
    MIN_5 = 'PT5M'
    HOUR_1 = 'PT1H'
    HOUR_2 = 'PT2H'
    DAY = 'PT24H'
    MONTH = 'P1M'

    @classmethod
    def get_from_file_name(cls, file_name):
        for record_type in RecordType:
            if record_type.value in file_name:
                return record_type
        logging.warning(f"Not found record type for file{file_name}, return MIN_5")
        return RecordType.MIN_5

    def is_time_to_send_file(self, info_type: InformationType) -> bool:
        """
        Checks if it's time to send the file. All files except InformationType.UB are sent as soon as they are created.
        InformationType.UB is sent at 12:00
        :param info_type: message information type.
        :return: true if its time to send a file
        """
        if self is RecordType.DAY and info_type is InformationType.BALANCE:
            current_time = datetime.now().astimezone(messages_timezone)
            return current_time.hour >= 12

        if self is RecordType.MONTH and info_type is InformationType.PRODUCTION:
            current_time = datetime.now().astimezone(messages_timezone)
            return current_time.hour >= 10 and current_time.day >= 6
        return True

    def get_local_store_folder(self, info_type=InformationType.REGIME) -> str:
        """
        Returns each file information type store folder
        :param info_type:
        :return: path to store folder
        """
        if info_type is InformationType.REGIME:
            return self.name

        if info_type is InformationType.BALANCE:
            return f'{self.name}_BALANCE'

        if info_type is InformationType.PRODUCTION:
            return f'{self.name}_PRO'

        if info_type is InformationType.PLAN:
            return f'{self.name}_PLAN'

    def get_archive_retrieve_time(self, time_stamp: datetime,
                                  info_type: InformationType = InformationType.REGIME) -> datetime:
        """
        Returns history retrieve timestamp for given file information type and given timestamp.
        For example:
         - current time = 12:03
         - message type = PT5M
         - function will return 12:00
        :param time_stamp: timestamp for data retrieve
        :param info_type: file information type
        :return:
        """

        result_time_stamp = time_stamp.replace(microsecond=0)

        if self is RecordType.REAL:
            return time_stamp.astimezone()

        if self is RecordType.MIN_5:
            return time_stamp.astimezone()

        if self is RecordType.HOUR_1:
            return time_stamp.astimezone()

        if self is RecordType.HOUR_2:
            return time_stamp.astimezone()

        if self is RecordType.DAY:  # 24 hours report is closing at 9.
            return result_time_stamp.astimezone().replace(hour=9, minute=0, second=0)

        if self is RecordType.MONTH:
            if info_type is InformationType.PRODUCTION:
                return result_time_stamp.astimezone().replace(day=1, hour=9, minute=0, second=0)
            else:
                return result_time_stamp.astimezone().replace(day=25, hour=16, minute=00, second=0)

        return result_time_stamp

    def get_last_store_time(self, time_stamp: datetime,
                            info_type: InformationType = InformationType.REGIME) -> datetime:
        """
        Returns last time when message should have been created. E.g. for mode two hours: time_stamp=10:30 -> return = 10:00
        Returns last time wh
        :param time_stamp:
        :param info_type:
        :return: Last time when message should have been created
        """
        time_stamp = time_stamp.astimezone(messages_timezone).replace(microsecond=0)
        if self is RecordType.REAL:
            return time_stamp.replace(microsecond=0)

        if self is RecordType.MIN_5:
            new_minutes = time_stamp.minute - time_stamp.minute % 5
            return time_stamp.replace(minute=new_minutes, second=0)

        if self is RecordType.HOUR_2:
            return time_stamp.replace(hour=time_stamp.hour - time_stamp.hour % 2, minute=0, second=0)

        if self is RecordType.DAY:  # Сутки только Баланс и Режим
            today_store_time = time_stamp.replace(hour=10, minute=0, second=0)  # Режимные по умолчанию
            if info_type is InformationType.BALANCE:
                today_store_time = time_stamp.replace(hour=12, minute=0, second=0)

            if time_stamp >= today_store_time:
                return today_store_time
            else:
                return today_store_time - relativedelta(days=1)

        if self is RecordType.MONTH:  # Месяц  только Продукция и План
            this_month_store_time = time_stamp.replace(day=6, hour=10, minute=0, second=0)  # Продукция по умолчанию
            if info_type is InformationType.PLAN:
                this_month_store_time = time_stamp.replace(day=25, hour=10, minute=5, second=0)

            if time_stamp >= this_month_store_time:
                return this_month_store_time
            else:
                return this_month_store_time - relativedelta(months=1)

    def get_ref_time_from_store_time(self, message_store_time: datetime,
                                     info_type: InformationType = InformationType.REGIME) -> datetime:
        """
        :param info_type: message info type
        :param message_store_time: Время создания сообщения
        :return: Returns the reference time for the current file save time.
        The reference time from daily data is the beginning of the aggregation period.
        For less than daily data - at the time of closing the aggregation.
        For example time_stamp=15.01.22 10:00.
        For two-hourly data -> store_time=10:00, ref_time=10:00.
        For balance daily -> store_time=15.01.22 12:00, ref_time=14.01.22 10:00.
        For balance regime -> store_time = 15.01.22 10:00, ref_time=14.01.22 10:00.
        """

        if self is RecordType.REAL:
            return message_store_time

        if self is RecordType.MIN_5:
            return message_store_time

        if self is RecordType.HOUR_1:
            return message_store_time

        if self is RecordType.HOUR_2:
            return message_store_time

        if self is RecordType.DAY:
            return message_store_time.replace(hour=10) - relativedelta(days=1)

        if self is RecordType.MONTH:
            if info_type is InformationType.PLAN:
                return message_store_time.replace(day=1, hour=10, minute=0, second=0) + relativedelta(months=1)
            else:
                return message_store_time.replace(day=1, hour=10) - relativedelta(months=1)

    def get_next_store_time(self, time_stamp: datetime, info_type=InformationType.REGIME) -> datetime:
        """
        Returns next file creation time
        :param info_type:
        :param time_stamp:
        :return: Time then next archive will be created. Need for files creation
        """
        if self is RecordType.MIN_5 or self is RecordType.REAL:
            return self.get_last_store_time(time_stamp, info_type) + relativedelta(minutes=5)

        if self is RecordType.HOUR_1:
            return self.get_last_store_time(time_stamp, info_type) + relativedelta(hours=1)

        if self is RecordType.HOUR_2:
            return self.get_last_store_time(time_stamp, info_type) + relativedelta(hours=2)

        if self is RecordType.DAY:
            return self.get_last_store_time(time_stamp, info_type) + relativedelta(days=1)

        if self is RecordType.MONTH:
            return self.get_last_store_time(time_stamp, info_type) + relativedelta(months=1)

    def get_prev_store_time(self, time_stamp: datetime, info_type: InformationType) -> datetime:
        """
        Returns previous file store time for the given timestamp
        :param time_stamp:
        :return: previous store time for given timestamp
        """
        if self is RecordType.MIN_5 or self is RecordType.REAL:
            return self.get_last_store_time(time_stamp) - relativedelta(minutes=5)

        if self is RecordType.HOUR_2:
            return time_stamp - relativedelta(hours=2)

        if self is RecordType.DAY:
            return self.get_last_store_time(time_stamp, info_type) - relativedelta(days=1)

        if self is RecordType.MONTH:
            return self.get_last_store_time(time_stamp, info_type) - timedelta(month=1)

    def get_file_storage_deep(self) -> relativedelta:
        """:return: maximum storage deep for every archive type. Used for old files cleaning
                """
        if self is RecordType.MIN_5 or self is RecordType.REAL:
            return relativedelta(minutes=30)
        if self is RecordType.HOUR_2 or self is RecordType.HOUR_1:
            return relativedelta(days=1)
        if self is RecordType.DAY:
            return relativedelta(days=7)
        if self is RecordType.MONTH:
            return relativedelta(months=1)

    def get_archive_deep(self) -> relativedelta:
        """
        :return: maximum retrive from base deep for every archive type. Used for filling missed archives
        """
        if self is RecordType.MIN_5 or self is RecordType.REAL:
            return relativedelta(minutes=5)
        if self is RecordType.HOUR_2 or self is RecordType.HOUR_1:
            return relativedelta(hours=2)
        if self is RecordType.DAY:
            return relativedelta(days=1)
        if self is RecordType.MONTH:
            return relativedelta(months=1)

    def get_remote_storage_folder(self) -> str:
        """
        :return: maximum retrive from base deep for every archive type. Used for filling missed archives
        """
        if self is RecordType.MIN_5 or self is RecordType.REAL:
            return "RTD/IN"
        if self is RecordType.HOUR_2 or self is RecordType.HOUR_1:
            return "SD/IN"
        if self is RecordType.DAY:
            return "SD/IN"
        if self is RecordType.MONTH:
            return "SD/IN"


class DataRecord:
    """Concrete value record in report file"""
    time_format = '%Y-%m-%dT%H:%M%S%z'

    formats = {
        'кгс/см2': '{:.1f}',
        'МПа': '{:.1f}',
        'C': '{:.1f}',
        'мг/м3': '{:.2f}',
        'об/мин': '{:.0f}',
        'тыс. м3/час': '{:.3f}',
        'тыс. м3/сут': '{:.3f}',
        'тыс. м3': '{:.3f}',
        'тонн': '{:.3f}',
        'тонн/час': '{:.3f}',
        'тыс. тонн': '{:.3f}',
        'тыс. тонн/час': '{:.3f}',
        'тыс. тонн/сут': '{:.3f}',
        'тонн/сут': '{:.0f}',
        'шт.': '{:.0f}',
        'тыс. кВт': '{:.0f}',
        'ppm': '{:.2f}',
        'г/тонн': '{:.3f}',
        '%': '{:.3f}',
        'тыс. кВт*час': '{:.3f}',
        'МДж/м3': '{:.3f}',
        'кг/м3': '{:.3f}',
        '-': '{:.0f}',
        None: '{:.0f}'
    }

    def __init__(self, id_type: str = 'ASDU_ESG',
                 input_source: InputSource = InputSource.RAW
                 , id_value: str = 'def_value'
                 , time_stamp: datetime = None
                 , full_name: str = 'def_name'
                 , eng_units: str = 'def_eu'
                 , value_format: str = None
                 , fast_tools_item: str = 'def_item'
                 , his_group: str = 'AGGREGATION'):
        self.id_type = id_type
        self.id_hash_value = id_value

        self.input_source = input_source
        self.full_name = full_name
        self.eng_units = eng_units
        if value_format is None:
            self.value_format = DataRecord.formats.get(self.eng_units, '{:.0f}')
        else:
            self.value_format = value_format

        self.fast_tools_item = FastToolsItem(fast_tools_item, history_group=his_group)

    def read_current_value(self, ft_conn, value_dataset):
        """update current value with last value from FT"""
        try:
            self.fast_tools_item.read_current_value(ft_conn=ft_conn, value_dataset=value_dataset)
        except BaseException as ex:
            logging.exception(f'Error read current value for {self.fast_tools_item.item_name}')

    def read_aggr_history_value(self, ft_conn, time_stamp: datetime):
        """
        Updates self value with value from history
        :param ft_conn: dss connection
        :param time_stamp: retrieve timestamp
        :return: None
        """
        try:
            self.fast_tools_item.read_history_aggregated_value(ft_conn=ft_conn, time_stamp=time_stamp,
                                                               limit_in_seconds=60)
        except BaseException:
            logging.exception(
                f'Error read aggr history value for {self.fast_tools_item}  at time = {time_stamp.isoformat()}')

    def read_rv_history_value(self, ft_conn, time_stamp: datetime):
        """
        Update value with value from realtime history group
        :param ft_conn: dss connection
        :param time_stamp: retrieve timestamp
        :return: None
        """
        try:
            self.fast_tools_item.read_history_rv_value(ft_conn=ft_conn, time_stamp=time_stamp)
        except BaseException:
            logging.exception(
                f'Error read rv history value for {self.fast_tools_item} at time = {time_stamp.isoformat()}')

    def get_full_xml_representation(self, show_time_stamp=True) -> Element:
        """
        Returns the full xml representation of current object
        :param show_time_stamp: if true, the element with the time stamp will be added
        :return: instance xml representation
        """
        # < DataSection >
        # < Identifier type = "ASDU_ESG" > 8085DB120C891D3FE0530F93380A7A31 < / Identifier >
        # < ParameterFullName > ГП ПРБ Благовещенск.АмГПЗ.Выход на Своб.ТЭС(газ).Поставка продукта(тыс.м3) < / ParameterFullName >
        # < Value > 990 < / Value >
        # < Source > 0 < / Source >
        # < Dimension > тыс.м3 < / Dimension >
        # < / DataSection >

        root = self.get_short_xml_representation(show_time_stamp=show_time_stamp)

        if self.input_source is not None:
            source_element = SubElement(root, "Source")
            source_element.text = self.input_source.value

        return root

    def get_short_xml_representation(self, show_time_stamp=True) -> Element:
        """
           Returns the short xml representation of current object
           :param show_time_stamp: if true, the element with the time stamp will be added
           :return: instance xml representation
       """
        # < DataSection >
        # < Identifier type = "ASDU_ESG" > 8085DB120C891D3FE0530F93380A7A31 < / Identifier >
        # < Value > 990 < / Value >
        # < / DataSection >
        root = Element('DataSection')

        id_element = SubElement(root, 'Identifier')
        id_element.set("type", self.id_type)
        id_element.text = self.id_hash_value

        if self.full_name is not None:
            full_name_element = SubElement(root, 'ParameterFullName')
            full_name_element.text = self.full_name

        value_element = SubElement(root, "Value")
        value_element.text = self.value_format.format(self.fast_tools_item.value)

        if show_time_stamp is True and self.fast_tools_item.time_stamp is not None:
            time_stamp_element = SubElement(root, 'Time')
            time_stamp_element.text = self.fast_tools_item.time_stamp.astimezone(messages_timezone).isoformat('T',
                                                                                                              'seconds')

        # Some EU skipped, because of ASDU bug in schema
        if self.eng_units is not None:
            if self.eng_units != '-' \
                    and self.eng_units != 'МДж/м3' \
                    and self.eng_units != 'кг/м3' \
                    and self.eng_units != 'ppm':
                eu_element = SubElement(root, "Dimension")
                result_eu = self.eng_units
                if self.eng_units == 'кгс/см2':
                    result_eu = 'кг/см2'

                eu_element.text = result_eu

        return root

    @classmethod
    def get_from_element(cls, data_record_element: Element) -> DataRecord:
        """
        Creates instance from xml element
        :param data_record_element:
        :return: DataRecord
        """
        # < DataRecord >
        # < id > 89F0B0EB953E2AF2E0530F93380A919E < / id >
        # < Comment > Давление газа на входе < / Comment >
        # < FT_TAG > ASDU.KC_PIN.hour_1_aggr.finalValue < / FT_TAG >
        # < EU > кгc / см² < / EU >
        # < / DataRecord >

        id_type = data_record_element.findtext("IdType", 'ASDU_ESG')
        id_hash = data_record_element.findtext("id")
        source_type_element = data_record_element.findtext('Source')
        if source_type_element is not None:
            source_type = InputSource(data_record_element.findtext('Source'))
        else:
            source_type = None

        ft_tag = data_record_element.findtext('FastToolsTag')
        eu = data_record_element.findtext('EU')
        value_format = data_record_element.findtext('ValueFormat')
        full_name = data_record_element.findtext('ParameterFullName')
        aggregation_group = data_record_element.findtext('HistoryGroup')

        return DataRecord(id_type, input_source=source_type, id_value=id_hash, full_name=full_name, eng_units=eu,
                          value_format=value_format,
                          fast_tools_item=ft_tag, his_group=aggregation_group)


class Header:
    """
    Report file header object
    """
    time_format = '%Y-%m-%dT%H:%M:%S%z'
    """Заголовок сообщения"""

    offset = time.timezone

    def __init__(self, sender_id: str = 'ГП Переработка Благовещенск',
                 receiver_id: str = 'М АСДУ ЕСГ',
                 generated_at: datetime = None,
                 comment: str = 'def_comment',
                 ref_time: datetime = None,
                 scale: RecordType = RecordType.HOUR_2,
                 location_id: str = 'P_BLG',
                 full_name: str = 'def_full_name',
                 info_type: InformationType = InformationType.REGIME
                 ):
        self.sender_id = sender_id
        self.receiver_id = receiver_id
        self.generated_at = generated_at
        self.comment = comment
        self.ref_time = ref_time
        self.scale = scale
        self.template_id = location_id + '.' + scale.value + '.' + info_type.value + '.V1'
        self.full_name = full_name
        self.info_type = info_type

    def get_short_xml_representation(self) -> Element:
        """
        :return: Short xml representation of object instance
        """
        # <HeaderSection>
        #      <Sender id="ГП Переработка Благовещенск" />
        #       <Receiver id="М АСДУ ЕСГ" />
        #       <Generated at="2021-02-17T22:01:04+03:00" />
        #       <Comment>Двухчасовой отчет за 17.02.2021 16:00:00</Comment>
        #       <ReferenceTime time="2021-02-17T16:00:00+03:00" />
        #       <Scale>PT2H</Scale>
        #       <Template id="G_PRBB.PT2H.RT.V1" />
        #       <FullName>Старший диспетчер ГППБ</FullName>
        #    </HeaderSection>

        root = Element('HeaderSection')
        sender_element = SubElement(root, 'Sender')
        sender_element.set('id', self.sender_id)

        receiver_element = SubElement(root, 'Receiver')
        receiver_element.set('id', self.receiver_id)

        generated_element = SubElement(root, 'Generated')
        if self.generated_at is None:
            generated_element.set('at', datetime.now().astimezone(messages_timezone).isoformat('T', 'seconds'))
        else:
            generated_element.set('at', self.generated_at.astimezone(messages_timezone).isoformat('T', 'seconds'))

        reference_time_element = SubElement(root, 'ReferenceTime')
        if self.ref_time is None:
            reference_time_element.set('time', datetime.now().astimezone(messages_timezone).isoformat('T', 'seconds'))
        else:
            reference_time_element.set('time', self.ref_time.astimezone(messages_timezone).isoformat('T', 'seconds'))

        scale_element = SubElement(root, 'Scale')
        scale_element.text = self.scale.value

        template_element = SubElement(root, 'Template')
        template_element.set('id', self.template_id)

        # обязательный элемент только для 24час и мес. отчета
        if self.scale == RecordType.DAY or self.scale == RecordType.MONTH:
            full_name_element = SubElement(root, 'FullName')
            full_name_element.text = self.full_name

        return root

    def get_full_xml_representation(self):
        """
        :return: Full xml representation of object instance
        """
        root = self.get_short_xml_representation()
        comment_element = SubElement(root, 'Comment')
        comment_element.text = self.comment
        return root

    @classmethod
    def get_from_element(cls, header_element: Element) -> Header:
        """
        Creates Header object from xml element
        :param header_element:
        :return: Header
        """
        #   < HeaderSection >
        #     < Sender id = "ГП Переработка Благовещенск" / >
        #     < Receiver id = "М АСДУ ЕСГ" / >
        #     <Comment > Файл со значениями технологических параметров.Реальное время < / Comment >
        #     < Scale > PT5M < / Scale >
        #     < Location = "D_URG" / >
        # < / HeaderSection >
        sender_id = header_element.find("Sender").get("id")
        receiver_id = header_element.find("Receiver").get("id")
        comment = header_element.findtext("Comment")
        scale = RecordType(header_element.findtext("Scale"))
        location = header_element.findtext("Location")
        info_type = InformationType(header_element.findtext('InformationType', 'RT'))

        full_name = header_element.findtext("FullName", None)

        return Header(sender_id=sender_id, receiver_id=receiver_id, comment=comment, scale=scale, location_id=location,
                      info_type=info_type, full_name=full_name)

    @classmethod
    def get_from_message_file(cls, data_file_name: str) -> Header:
        """
        Creates Header instance from setup file
        :param data_file_name: setup message file
        :return: Header
        """
        message = xml.etree.ElementTree.parse(data_file_name)
        header_element = message.find('HeaderSection')
        header = Header.get_from_element(header_element)
        ref_time_element = Element(message.find('ReferenceTime'))
        if ref_time_element is not None:
            header.ref_time = dateutil.parser.isoparse(ref_time_element.get('at'))


class Message:
    """
    Whole report message object
    """

    def __init__(self, header: Header, data: list[DataRecord]):
        self.header = header
        self.data_records = data

    @classmethod
    def is_time_to_remove(cls, message_name: str) -> bool:
        message_ref_time = Message.get_store_time_from_file(message_name)
        message_storage_deep = RecordType.get_from_file_name(message_name).get_file_storage_deep()
        return datetime.now().astimezone() - message_storage_deep > message_ref_time.astimezone()

    @classmethod
    def get_from_setup_file(cls, file_name: str) -> Message:
        """
        Creates instance from given setup file
        :param file_name:
        :return: Message instance
        """
        setup_xml = xml.etree.ElementTree.parse(file_name)
        header_element = Header.get_from_element(setup_xml.find("Header"))
        data_records_setup = setup_xml.find('DataRecords').findall('DataRecord')
        data_records = []
        for record in data_records_setup:
            new_data_record = DataRecord.get_from_element(record)
            data_records.append(new_data_record)
        return Message(header_element, data_records)

    def get_store_time(self) -> datetime:
        return self.header.generated_at

    def __update_by_current_values(self, ft_connection):
        """
        Update all items in message by fast tools last value
        :param ft_connection: dss connection
        :return: None
        """

        data_set = None
        try:
            data_set = dss.openDataset(ft_connection, 'ITEM_VAL', ['NAME', 'ITEM_VALUE', 'UPDATE_TIME'], 'r')
            for data_record in self.data_records:
                data_record.read_current_value(ft_conn=ft_connection, value_dataset=data_set)
        except Exception:
            logging.exception('Update current values for ' + self.header.template_id + ' exception')
        finally:
            if data_set is not None:
                dss.closeDataset(ft_connection, data_set)

    def __update_by_rv_history(self, time_stamp: datetime, ft_connection):
        """
        Update all items from history at specified time_stamp
        :param time_stamp:
        :param ft_connection:
        :return: None
        """

        try:
            for data_record in self.data_records:
                data_record.read_rv_history_value(ft_conn=ft_connection, time_stamp=time_stamp)
        except BaseException:
            logging.exception(
                f'Read real values history exception for {self.header.template_id}')

    def __update_by_aggr_history(self, time_stamp: datetime, ft_connection):
        """
        Update all items from history at specified time_stamp
        :param time_stamp: retrieve data timestamp
        :param ft_connection: dss connection
        :return: None
        """
        try:
            for data_record in self.data_records:
                data_record.read_aggr_history_value(ft_conn=ft_connection, time_stamp=time_stamp)
        except BaseException:
            logging.exception(f'Read aggregation history exception for {self.header.template_id}')

    def prepare_message(self, message_store_time: datetime, ft_connection):
        archive_retrieve_time = self.header.scale.get_archive_retrieve_time(message_store_time, self.header.info_type)
        logging.info(f'Prepare message: {self.header.template_id} timestamp={message_store_time.isoformat()}')
        if self.header.scale == RecordType.REAL:
            time_stamp_now = datetime.now().astimezone()
            logging.info(f'Now and time_stamp delta = {(time_stamp_now - message_store_time).total_seconds()}')
            if time_stamp_now - archive_retrieve_time < timedelta(seconds=300):
                self.header.ref_time = self.header.scale.get_ref_time_from_store_time(message_store_time,
                                                                                      self.header.info_type)
                logging.info(f'Update by current values. Ref_time = {self.header.ref_time.isoformat()}')
                self.__update_by_current_values(ft_connection)
            else:
                self.header.ref_time = self.header.scale.get_ref_time_from_store_time(message_store_time,
                                                                                      self.header.info_type)
                logging.info(f'Update by history rv values. Ref_time = {self.header.ref_time.isoformat()}')
                self.__update_by_rv_history(archive_retrieve_time, ft_connection)
        else:
            retrieve_history_timestamp = self.header.scale.get_archive_retrieve_time(message_store_time,
                                                                                     self.header.info_type)
            self.header.ref_time = self.header.scale.get_ref_time_from_store_time(message_store_time,
                                                                                  self.header.info_type)
            logging.info(
                f'Update by history aggr values:{self.header.template_id} '
                f'\ntime_stamp = {message_store_time.isoformat()} '
                f'\nref_time = {self.header.ref_time.isoformat()} '
                f'\narchive_retr_time = {retrieve_history_timestamp.astimezone().isoformat()} ')
            self.__update_by_aggr_history(retrieve_history_timestamp, ft_connection)

    def get_file_name(self) -> str:
        """
        Creates store file name for current Message. Example: G_PRBB.PT2H.RT.V1_2021_02_17_22_01_04.xml
        :return:
        """
        data_format = '%Y_%m_%d_%H_%M_%S.xml'
        return self.header.template_id + '_' + self.header.generated_at.astimezone(messages_timezone).strftime(
            data_format)

    def write_to_xml_file(self, file_name: str = None, messages_root_folder: str = '',
                          generated_at: datetime = datetime.now().astimezone(), short_representation_mode=True):
        """
        Writes current message to the xml file
        :param file_name: store file name
        :param messages_root_folder: store root folder
        :param generated_at: generation timestamp
        :param short_representation_mode: if true short xml representation will be used
        :return: None
        """
        self.header.generated_at = generated_at

        if file_name is None:
            file_name = self.get_file_name()

        os.makedirs(self.get_local_storage_message_folder(messages_root_folder), exist_ok=True)
        file_name = os.path.join(self.get_local_storage_message_folder(messages_root_folder), file_name)

        logging.info(f'Write message {self.header.template_id} to file: {file_name}')
        root = Element('BusinessMessage')
        if short_representation_mode:
            root.append(self.header.get_short_xml_representation())
        else:
            root.append(self.header.get_full_xml_representation())

        for data_record in self.data_records:
            if short_representation_mode:
                root.append(
                    data_record.get_short_xml_representation(show_time_stamp=(self.header.scale == RecordType.MIN_5)))
            else:
                root.append(
                    data_record.get_full_xml_representation(show_time_stamp=(self.header.scale == RecordType.MIN_5)))
        xmlstr = minidom.parseString(xml.etree.ElementTree.tostring(root)).toprettyxml(indent="   ", encoding='UTF-8')
        with open(file_name, "wb") as f:
            f.write(xmlstr)

    @classmethod
    def get_store_time_from_file(cls, message_file) -> datetime:
        """
        Extracts store time from given message file
        :param message_file:
        :return: file store time
        """
        message = xml.etree.ElementTree.parse(message_file)
        header_element = message.find('HeaderSection')

        if header_element is not None:
            try:
                ref_time_element_value = header_element.find('Generated').get('at')
                return dateutil.parser.isoparse(ref_time_element_value)
            except Exception:
                logging.error(f'Error parse message ref_time:{xml.etree.ElementTree.tostring(header_element)}')
        else:
            return datetime.fromtimestamp(0)

    @classmethod
    def get_remote_storage_folder_from_file_name(cls, file_name):
        return RecordType.get_from_file_name(file_name).get_remote_storage_folder()

    @classmethod
    def is_time_to_send_message_file_to_ftp(cls, message_file_name: str):
        message_record_type = RecordType.get_from_file_name(message_file_name)
        message_info_type = InformationType.get_from_file_name(message_file_name)
        return message_record_type.is_time_to_send_file(message_info_type)

    def get_local_storage_message_folder(self, messages_location: str) -> str:
        return f'{messages_location}/{self.header.scale.get_local_store_folder(self.header.info_type)}'

    def get_last_stored_message_store_time(self, messages_root_folder='data') -> datetime:
        """
        Returns reftime of last message, limited by message history dee
        :param messages_root_folder:
        :return:
        """

        message_files = glob.glob(
            f'{self.get_local_storage_message_folder(messages_root_folder)}/{self.header.template_id}*.xml')

        last_message_store_time = self.header.scale.get_last_store_time(
            datetime.now().astimezone() - self.header.scale.get_archive_deep(), self.header.info_type)
        for message_file in message_files:
            message_ref_time = Message.get_store_time_from_file(message_file)
            if message_ref_time > last_message_store_time:
                last_message_store_time = message_ref_time

        return last_message_store_time

    def is_prev_message_exist(self, message_time_stamp: datetime, last_message_time_stamp: datetime) -> bool:
        return self.header.scale.get_prev_store_time(message_time_stamp) <= last_message_time_stamp
