from __future__ import annotations
from datetime import datetime
from logging.handlers import TimedRotatingFileHandler
import sftp_sync

import dss
import os
import sys
from message_parts import Message, messages_timezone
import logging

SCRIPT_LOCATION = os.path.dirname(__file__)
sys.path.append(SCRIPT_LOCATION)

LOG_FOLDER = os.path.join(SCRIPT_LOCATION, "..", "logs")
DATA_FOLDER = os.path.join(SCRIPT_LOCATION, "..", "data")
SETUP_FOLDER = os.path.join(SCRIPT_LOCATION, "..", "setup")


def init_logger(logging_level=logging.DEBUG):
    os.makedirs(LOG_FOLDER, exist_ok=True)
    handler = TimedRotatingFileHandler(
        filename=str(os.path.join(LOG_FOLDER, 'handler_log')),
        when="D",
        interval=1,
        backupCount=30,
        encoding="utf-8",
        delay=False,
    )
    formatter = logging.Formatter(
        fmt="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    handler.setFormatter(formatter)
    handler.suffix = datetime.now().strftime("%Y-%m-%d_%H-%M-%S.log")

    logging.getLogger().addHandler(handler)
    logging.getLogger().setLevel(logging_level)

    logging.getLogger("paramiko").setLevel(logging.ERROR)


def get_setup_messages() -> list[Message]:
    """
    reads setup files from SETUP_FOLDER
    :return: list of Messages with setup
    """
    setup_files = [
        os.path.join(SETUP_FOLDER, 'RV_data.xml'),
        os.path.join(SETUP_FOLDER, '2H_data.xml'),
        os.path.join(SETUP_FOLDER, '24H_data.xml'),
        os.path.join(SETUP_FOLDER, '24H_UB_data.xml'),
        os.path.join(SETUP_FOLDER, '1M_PRO_data.xml'),
        os.path.join(SETUP_FOLDER, '1M_PL_data.xml')
    ]
    result_messages = []
    for file in setup_files:
        try:
            logging.info('Get setup message from file:' + file)
            message = Message.get_from_setup_file(file)
            result_messages.append(message)
            logging.info('Message created:' + message.header.template_id)
        except BaseException:
            logging.exception('Exception at process file ' + file)

    return result_messages


def update_and_store_messages(messages: list[Message], messages_location: str = 'data'):
    """
    read data for each message setup in messages and store results in messages_location folder
    :param messages: list of messages setup
    :param messages_location: folder to store report file
    :return: None
    """

    ft_connection = dss.connect()
    for message in messages:
        logging.info('========Process message with  template_id = ' + message.header.template_id + ' ============')

        message_record_type = message.header.scale
        message_info_type = message.header.info_type

        today_last_message_store_time = message_record_type.get_last_store_time(
            datetime.now().astimezone(messages_timezone),
            message_info_type)

        last_stored_message_store_time = message.get_last_stored_message_store_time()

        new_message_store_time = message_record_type.get_next_store_time(last_stored_message_store_time,
                                                                         message_info_type)
        logging.info((f'Last stored msg time stamp = {last_stored_message_store_time.isoformat()}'
                      f' next msg timestamp:{new_message_store_time.isoformat()}'))
        while new_message_store_time <= today_last_message_store_time:
            message.prepare_message(new_message_store_time, ft_connection=ft_connection)
            message.write_to_xml_file(messages_root_folder=messages_location, generated_at=new_message_store_time)
            new_message_store_time = message.header.scale.get_next_store_time(new_message_store_time, message_info_type)

    dss.close(ft_connection)


if __name__ == '__main__':
    init_logger(logging_level=logging.DEBUG)
    messages = get_setup_messages()
    update_and_store_messages(messages, DATA_FOLDER)
    sftp_sync.perform_synchronization()
