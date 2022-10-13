from __future__ import annotations
import logging
import os

import pysftp
from configparser import ConfigParser, RawConfigParser
import glob
import configparser

from message_parts import Message, RecordType

script_location = os.path.dirname(__file__)
# script_location = ""

setup_file_name = f'{script_location}/setup/sync_setup.ini'
sync_info_file = f'{script_location}/setup/loaded_files_info.ini'


class SyncInfo:
    def __init__(self, host: str, port: int, user: str, password: str, local_path: str, remote_path: str):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.local_path = local_path
        self.remote_path = remote_path


def get_sync_setup() -> SyncInfo:
    # TODO: load from file setup/
    return SyncInfo('10.85.35.101', 22, 'do2asduesg', 'Ji61J7Yxpfk', 'data', '/RTD/IN/')


def get_local_files(messages_location: str) -> list[str]:
    message_files = glob.glob(messages_location + '/*/*.xml', recursive=True)
    return message_files


def get_not_synced_files(synced_files_info: ConfigParser, local_stored_files: list(str)) -> list(str):
    files_in_sync_info = dict(synced_files_info['LOAD_FILES_INFO'].items())

    result_list = [file for file in local_stored_files
                   if files_in_sync_info.get(file, 'no') == 'no']

    return result_list


def load_files_to_sftp(not_synced_files: list(str), synced_files_info: RawConfigParser,
                       sync_setup: SyncInfo) -> ConfigParser:
    """Load files to sftp and mark them as downloaded in config parser"""
    # sync files that not synced and save sync flag
    logging.debug('Start synchronization')
    cnopts = pysftp.CnOpts()
    cnopts.hostkeys = None
    logging.debug(str(not_synced_files))
    # cnopts.hostkeys = None
    with(pysftp.Connection(host=sync_setup.host, username=sync_setup.user, password=sync_setup.password,
                           cnopts=cnopts)) as sftp_connection:
        uploaded_files_count = 0
        for file in not_synced_files:
            if not Message.is_time_to_send_message_file_ftp(file):
                continue

            file_name = file.split('\\')[-1]
            remote_storage_path = Message.get_remote_storage_folder_from_file_name(file_name=file_name)
            try:
                ftp_server_path = remote_storage_path + "/" + file_name
                logging.debug('Put file:' + file + " to " + ftp_server_path)
                sftp_connection.put(localpath=file, remotepath=ftp_server_path)
                synced_files_info['LOAD_FILES_INFO'][file] = 'yes'
                uploaded_files_count += 1
            except Exception as ex:
                logging.exception(f'Error at load file {file}  to  {ftp_server_path}: {ex}')
                synced_files_info['LOAD_FILES_INFO'][file] = 'no'

        logging.debug('Uploaded ' + str(uploaded_files_count) + ' files')

    return synced_files_info


def perform_synchronization():
    sync_setup = get_sync_setup()

    local_files = get_local_files(f'{script_location}/{sync_setup.local_path}')
    logging.debug('Found ' + str(len(local_files)) + ' local files')

    synced_files_info = RawConfigParser(delimiters='=')
    synced_files_info.optionxform = str
    synced_files_info.read(sync_info_file)

    not_synced_files = get_not_synced_files(synced_files_info=synced_files_info, local_stored_files=local_files)
    logging.debug(str(len(not_synced_files)) + ' are marked for sync')

    if len(not_synced_files) > 0:
        sync_files_info_after_synchronisation = load_files_to_sftp(not_synced_files=not_synced_files,
                                                                   synced_files_info=synced_files_info,
                                                                   sync_setup=sync_setup)
    else:
        sync_files_info_after_synchronisation = synced_files_info
        logging.debug('All files already synced')

    synced_files_info_after_cleaning = remove_old_files(sync_files_info_after_synchronisation)
    with open(sync_info_file, 'w') as configfile:
        synced_files_info_after_cleaning.write(configfile)


def remove_old_files(stored_files_info: RawConfigParser) -> RawConfigParser:
    registered_files = dict(stored_files_info['LOAD_FILES_INFO'].items())

    for file, is_synced in registered_files.items():
        if os.path.exists(file):
            if is_synced == "yes" and Message.is_time_to_remove(file):
                try:
                    os.remove(file)
                    logging.debug("Remove file:" + file)
                    stored_files_info.remove_option('LOAD_FILES_INFO', file)

                except BaseException as ex:
                    logging.error(f"Error remove file {file}:{ex}")
        else:
            logging.debug(f"File marked for removing: {file} doesnt exist")
            stored_files_info.remove_option('LOAD_FILES_INFO', file)

    return stored_files_info
