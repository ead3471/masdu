from __future__ import annotations
import logging
import os

import pysftp
from configparser import ConfigParser, RawConfigParser
import glob

from message_parts import Message

SCRIPT_LOCATION = os.path.dirname(__file__)
SETUP_FILE_NAME = os.path.join(SCRIPT_LOCATION, "..", "setup", "sync_setup.ini")
SYNC_INFO_FILE = os.path.join(SCRIPT_LOCATION, "..", "setup", "loaded_files_info.ini")
DATA_FILE_LOCATION = os.path.join(SCRIPT_LOCATION, "..", "data")


class SyncInfo:
    def __init__(self, host: str, port: int, user: str, password: str, remote_path: str):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.remote_path = remote_path


def init_from_setup_file() -> SyncInfo:
    synced_files_info = RawConfigParser(delimiters='=')
    synced_files_info.optionxform = str
    synced_files_info.read(SETUP_FILE_NAME)

    ftp_setup = dict(synced_files_info['CONNECTION_INFO'].items())

    # return SyncInfo(host=ftp_setup['host'],
    #                 port=int(ftp_setup['port']),
    #                 user=ftp_setup['user'],
    #                 password=ftp_setup['password'],
    #                 remote_path=ftp_setup['remote_path'],)

    return SyncInfo(host='10.85.35.101',
                    port=22,
                    user='do2asduesg',
                    password='Ji61J7Yxpfk',
                    remote_path='/RTD/IN/')


def get_local_files(messages_location: str) -> list[str]:
    message_files = glob.glob(messages_location + '/*/*.xml', recursive=True)
    return message_files


def get_not_synced_files(synced_files_info: ConfigParser, local_stored_files: list(str)) -> list(str):
    """
    checks locally stored files for information about synchronization with remote server

    :param synced_files_info: ConfigParser instance with information about previously synchronized files
    :param local_stored_files: list of files in DATA_FILE_LOCATION folder

    :return: list of paths to not synced files
    """
    files_in_sync_info = dict(synced_files_info['LOAD_FILES_INFO'].items())

    result_list = [file for file in local_stored_files
                   if files_in_sync_info.get(file, 'no') == 'no']

    return result_list


def load_files_to_sftp(not_synced_files: list[str], synced_files_info: RawConfigParser,
                       sync_setup: SyncInfo) -> ConfigParser:
    """
    Load files to sftp and mark them as downloaded in config parse

    :param not_synced_files: list of files marked for download to remote server
    :param synced_files_info: ConfigParser instance. function will update him with new data about data synchronization
    :param sync_setup: FTP server connection setup
    :return: updated ConfigParser
    """
    logging.info('Start synchronization')
    cnopts = pysftp.CnOpts()
    cnopts.hostkeys = None
    logging.debug(f"Not synced files:{not_synced_files}")
    with(pysftp.Connection(host=sync_setup.host, username=sync_setup.user, password=sync_setup.password,
                           cnopts=cnopts)) as sftp_connection:
        uploaded_files_count = 0
        for file in not_synced_files:
            if not Message.is_time_to_send_message_file_to_ftp(file):
                continue

            file_name = file.split('\\')[-1]
            remote_storage_path = Message.get_remote_storage_folder_from_file_name(file_name=file_name)
            try:
                ftp_server_path = remote_storage_path + "/" + file_name
                logging.info(f'Put file:{file} to {ftp_server_path}')
                sftp_connection.put(localpath=file, remotepath=ftp_server_path)
                synced_files_info['LOAD_FILES_INFO'][file] = 'yes'
                uploaded_files_count += 1
            except Exception as ex:
                logging.exception(f'Error at load file {file}  to  {ftp_server_path}: {ex}')
                synced_files_info['LOAD_FILES_INFO'][file] = 'no'

        logging.info(f'Uploaded {uploaded_files_count} files')

    return synced_files_info


def perform_synchronization():
    """
    executes local report files upload to the FTP server
    :return: None
    """
    sync_setup = init_from_setup_file()

    local_files = get_local_files(DATA_FILE_LOCATION)
    logging.info(f'Found {len(local_files)} local files')

    synced_files_info = RawConfigParser(delimiters='=')
    synced_files_info.optionxform = str
    synced_files_info.read(SYNC_INFO_FILE)

    not_synced_files = get_not_synced_files(synced_files_info=synced_files_info, local_stored_files=local_files)
    logging.debug(f'{len(not_synced_files)} are marked for sync')

    if len(not_synced_files) > 0:
        sync_files_info_after_synchronisation = load_files_to_sftp(not_synced_files=not_synced_files,
                                                                   synced_files_info=synced_files_info,
                                                                   sync_setup=sync_setup)
    else:
        sync_files_info_after_synchronisation = synced_files_info
        logging.debug('All files already synced')

    synced_files_info_after_cleaning = remove_old_files(sync_files_info_after_synchronisation)
    with open(SYNC_INFO_FILE, 'w') as configfile:
        synced_files_info_after_cleaning.write(configfile)


def remove_old_files(stored_files_info: RawConfigParser) -> RawConfigParser:
    """
    removes old files from the local storage and cleanup the sync_info file
    :param stored_files_info: RawConfigParser instance with synchronization information
    :return: None
    """
    registered_files = dict(stored_files_info['LOAD_FILES_INFO'].items())

    for file, is_synced in registered_files.items():
        if os.path.exists(file):
            if is_synced == "yes" and Message.is_time_to_remove(file):
                try:
                    os.remove(file)
                    logging.info("Remove file:" + file)
                    stored_files_info.remove_option('LOAD_FILES_INFO', file)

                except BaseException as ex:
                    logging.error(f"Error remove file {file}:{ex}")
        else:
            logging.info(f"File marked for removing: {file} doesnt exist")
            stored_files_info.remove_option('LOAD_FILES_INFO', file)

    return stored_files_info
