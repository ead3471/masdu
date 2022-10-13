from __future__ import annotations
from datetime import datetime
from logging.handlers import TimedRotatingFileHandler
import sftp_sync

import dss
import os
import sys
from message_parts import Message, RecordType, messages_timezone
import logging

script_location = os.path.dirname(__file__)
sys.path.append(script_location)


def init_logger(logging_level=logging.DEBUG):
    file_handler = TimedRotatingFileHandler(filename='runtime.log', when='D', interval=1, backupCount=3,
                                            encoding='utf-8',
                                            delay=False)

    logging.basicConfig(filename=f"{script_location}/logs/" + datetime.today().strftime('%Y-%m-%d') + ".log"
                        , level=logging_level
                        , format='%(asctime)s.%(msecs)03d %(levelname)s: %(message)s'
                        , datefmt='%Y-%m-%d %H:%M:%S'
                        )

    logging.getLogger('asdu_handler').addHandler(file_handler)

    logging.getLogger("paramiko").setLevel(logging.ERROR)


def get_setup_messages() -> list[Message]:
    # message = Message.get_from_setup_file('C:/Projects/Python/fast_tools_python/src/asdu/data_load/setup/RV_data.xml')
    setup_files = [f'{script_location}/setup/RV_data.xml',
                   f'{script_location}/setup/2H_data.xml',
                   f'{script_location}/setup/24H_data.xml',
                   f'{script_location}/setup/24H_UB_data.xml',
                   f'{script_location}/setup/1M_PRO_data.xml'
                   f'{script_location}/setup/1M_PL_data.xml'
                   ]
    result_messages = []
    # find setup message files
    for file in setup_files:
        try:
            logging.debug('Get setup message from file:' + file)
            message = Message.get_from_setup_file(file)
            result_messages.append(message)
            logging.debug('Message created:' + message.header.template_id)
        except Exception:
            logging.exception('Exception at process file ' + file)

    return result_messages


def update_and_store_messages(messages_location: str = 'data'):
    ft_connection = dss.connect()
    for message in messages:
        logging.debug('========Process message with  template_id = ' + message.header.template_id + ' ============')

        message_record_type = message.header.scale
        message_info_type = message.header.info_type

        today_last_message_store_time = message_record_type.get_last_store_time(
            datetime.now().astimezone(messages_timezone),
            message_info_type)

        last_stored_message_store_time = message.get_last_stored_message_store_time()

        new_message_store_time = message_record_type.get_next_store_time(last_stored_message_store_time,
                                                                         message_info_type)
        logging.debug(
            'last stored msg time stamp = ' + last_stored_message_store_time.isoformat() + ' next msg timestamp: ' + new_message_store_time.isoformat())
        while new_message_store_time <= today_last_message_store_time:
            message.prepare_message(new_message_store_time, ft_connection=ft_connection)
            message.write_to_xml_file(messages_root_folder=messages_location, generated_at=new_message_store_time)
            new_message_store_time = message.header.scale.get_next_store_time(new_message_store_time, message_info_type)

    dss.close(ft_connection)


if __name__ == '__main__':
    init_logger(logging_level=logging.DEBUG)
    messages = get_setup_messages()
    update_and_store_messages(f'{script_location}/data')

    sftp_sync.perform_synchronization()
