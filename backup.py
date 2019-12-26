#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import logging
import datetime
import os
import subprocess
import shutil
from pymongo import MongoClient
import tarfile

MONGO_CONNECTOR = 'mongodb://vm.services:27017'
MONGO_DUMP = '/usr/local/bin/mongodump'
MONGO_DB_EXCLUDE = ['admin', 'config', 'local', 'vigiadepreco']
BACKUP_FOLDER = '/Users/fabricio/1/backup'
REMOVE_OLD_TIME = 1 * 60 * 30

logger = logging.getLogger("backup")
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
ch.setFormatter(formatter)
logger.addHandler(ch)

today = datetime.datetime.now()
today_timestamp = today.timestamp() * 1000
mongo_client = MongoClient(MONGO_CONNECTOR + '/admin')
dbs_name = mongo_client.list_database_names()
mongo_client.close()
dbs_name = list(filter(lambda x: x not in MONGO_DB_EXCLUDE, dbs_name))


def filter_path_for_remove(tarinfo):
    backup_folder_string = BACKUP_FOLDER[1:] + '/'
    backup_tar_path = tarinfo.name
    new_path = backup_tar_path.replace(backup_folder_string, '')
    tarinfo.name = new_path
    logger.info('Archiving %s file' % new_path)
    return tarinfo


if not os.path.exists(BACKUP_FOLDER):
    logger.info('Folder %s has been created' % BACKUP_FOLDER)
    os.makedirs(BACKUP_FOLDER, 0o755, True)


def start_remove_old_backups():
    for root, dirnames, filenames in os.walk(BACKUP_FOLDER, followlinks=False):
        full_filenames = list(map(lambda x: '%s/%s' % (BACKUP_FOLDER, x), filenames))
        for full_filename in full_filenames:
            file_stat = os.stat(full_filename)
            file_created_file = datetime.datetime.fromtimestamp(file_stat.st_ctime)
            created_duration = today - file_created_file
            if created_duration.total_seconds() > REMOVE_OLD_TIME:
                logger.info('Removing %s old backup' % full_filename)
                os.remove(full_filename)


def do_backup():
    for db_name in dbs_name:
        backup_folder = '%s/%s_%s' % (BACKUP_FOLDER, db_name, today_timestamp)
        tar_file = '%s.tar' % backup_folder
        logger.info("Start Backup for database: %s on folder: %s" % (db_name, backup_folder))
        with(subprocess.Popen([MONGO_DUMP, '--uri=%s/%s' % (MONGO_CONNECTOR, db_name), '--gzip', '-o', backup_folder],
                              stderr=subprocess.PIPE, stdout=subprocess.PIPE)) as process:
            process.communicate()
            process.terminate()
            logger.info('Backup for %s has been finished' % db_name)
            if os.path.exists(tar_file):
                logger.info('Deleting old tar file: %s' % tar_file)
                os.rmdir(tar_file)
            logger.info('Tar Archiving start from %s to %s' % (backup_folder, tar_file))
            with tarfile.open(tar_file, 'x:') as tar:
                tar.add(backup_folder, recursive=True, filter=filter_path_for_remove)
                tar.close()
            logger.info('Tar Archiving from %s to %s has been finished' % (backup_folder, tar_file))
            logger.info('Cleanup backup')
            shutil.rmtree(backup_folder)


if __name__ == '__main__':
    do_backup()
    start_remove_old_backups()