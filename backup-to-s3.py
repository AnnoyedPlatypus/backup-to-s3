import subprocess
import boto3
from datetime import datetime, timedelta
import random, string
import os
import re
import zipfile

#
# Connect to the S3 service
#

def connect_to_s3(aws_config, verbose = False):
    if verbose:
        print('Connecting to Amazon S3...')

    session = boto3.Session(
        aws_access_key_id=aws_config['AWS_ACCESS_KEY_ID'],
        aws_secret_access_key=aws_config['AWS_SECRET_ACCESS_KEY']
        )
    s3 = session.resource('s3')

    if verbose:
        print('+ Connected')
        for bucket in s3.buckets.all():
            if (bucket.name == aws_config['AWS_STORAGE_BUCKET_NAME']):
                print('+ Confirmed target S3 bucket {bucket_name} exists'.format(bucket_name=aws_config['AWS_STORAGE_BUCKET_NAME']))

	return (s3)

#
# Upload dump to Amazon S3
#

def upload_to_s3(filepath, ftype, filename, config, verbose = False):
    s3 = connect_to_s3(config['aws'], verbose)

    if (ftype == "db"):
        s3_key = config['aws']['BUCKET_KEY_DB'] + "/" + filename + ".zip"
    else:
        s3_key = config['aws']['BUCKET_KEY_FOLDER'] + "/" + filename + ".zip"

    if verbose:
        print('Uploading dump to Amazon S3 into key "{s3_key}"...'.format(s3_key=s3_key))

    s3.Object(config['aws']['AWS_STORAGE_BUCKET_NAME'], filepath).put(Key=s3_key, Body=open(filepath, 'rb'))

    if verbose:
        print('+ Upload finished')


#
# Dump the database
#

def create_dump(config, db, filepath, filename, verbose=False, upload_callback=None):
    sqldump_cmd = ['mysqldump', db['NAME'], '-h', db['HOST'], '-P', db['PORT'], '-u', db['USER'], '-p{password}'.format(password=db['PASSWORD'])]
    proc = subprocess.Popen(sqldump_cmd, stdout=subprocess.PIPE)

    if verbose:
        print('Dumping MySQL database: {database} to file {filepath}'.format(database=db['NAME'], filepath=filepath))

    with open(filepath, 'w+') as f:
        while True:
            buf = proc.stdout.read(4096*1024) # Read 4 MB
            if buf != '':
                f.write(buf)
                if verbose:
                    print('+ Written data chunk')
            else:
                break

        if verbose:
            print('+ Dump finished')

        if os.path.isfile(filepath):
            # Zip the dump file up ready for sending.
            zip_name = filepath + '.zip'
            print('Zipping up the dump file to {zip_filename}'.format(zip_filename=zip_name))
            zipfile.ZipFile(zip_name, 'w', zipfile.ZIP_DEFLATED).write(filepath)
            try:
                if os.path.isfile(zip_name):
                    print('+ Zip file created successfully')
                    os.remove(filepath)
                    try:
                        upload_to_s3(zip_name, "db", filename, config, verbose)
                        if config['delete_backup'] == "true" and os.path.isfile(zip_name):
                            os.remove(zip_name)
                    except:
                        print('- Failed to upload zip file to S3 bucket {s3_bucket}'.format(s3_bucket=config['aws']['AWS_STORAGE_BUCKET_NAME']))
                else:
                    print('- Unable to find or access the compressed SQL dump file {filepath}'.format(filepath=zip_name))
            except:
                print('- Failed to create zip file')
                exit(1)
            if config['delete_backup'] == "true" and os.path.isfile(filepath):
                os.remove(filepath)

#
# Compress the target web folders into an archive file.
#

def create_archive(config, dir, filepath, filename, verbose=False, upload_callback=None):

    if verbose:
        print('Archiving target folder(s): {dir} to file {filepath}.zip'.format(dir=dir['DIR_NAME'], filepath=filepath))

    if os.path.isdir(dir['DIR_NAME']):
        # Archive the directory and all contents ready for sending.
        zip_name = filepath + '.zip'

        if verbose:
            print('+ Confirmed {dir} exists'.format(dir=dir['DIR_NAME']))

        zipf = zipfile.ZipFile(zip_name, 'w', zipfile.ZIP_DEFLATED)
        for root, dirs, files in os.walk(folder['DIR_NAME']):
            for file in files:
                zipf.write(os.path.join(root, file))
        
        try:
            if os.path.isfile(zip_name):
                print('+ Zip file for directory created successfully')
                try:
                    upload_to_s3(zip_name, "directory", filename, config, verbose)
                except:
                    print('Failed to upload directory archive file to S3 bucket {s3_bucket}'.format(s3_bucket=config['aws']['AWS_STORAGE_BUCKET_NAME']))
            else:
                print('Unable to find or access the directory archive file {zip_file}'.format(zip_file=zip_name))
        except:
            print('Failed to create directory archive file')
            exit(1)
        #os.remove(zip_name)


#
# Do our main function stuff including mysqldump and folder zipping
#

if __name__ == '__main__':
    import argparse, json

    parser = argparse.ArgumentParser(description='Database and Web Data to Amazon S3 Backup Tool')
    parser.add_argument(
        dest='config_file',
        help='Backup JSON configuration file.',
        default='backup-to-s3.json'
    )
    parser.add_argument(
        '-db'
        '--db_dump',
        action='store_true',
        dest='create_dump',
        help='Creates a database dump and uploads it to Amazon S3.',
        default=True,
    )
    parser.add_argument(
        '-f'
        '--folders',
        action='store_true',
        dest='create_archive',
        help='Creates a compressed folder archive and uploads it to Amazon S3.',
        default=True,
    )
    parser.add_argument(
        '-v'
        '--verbose',
        action='store_true',
        dest='verbose',
        help='Enable verbose mode.',
        default=True,
    )
    args = parser.parse_args()

    # Loads configuration file from JSON
    try:
        with open(args.config_file, 'r') as f:
            try:
                config=json.loads(f.read())
            except ValueError as e:
                print('Cannot parse configuration file (must be JSON).')
                exit(1)
    except IOError as e:		
        print('Cannot open configuration file ({filepath}). Does it exist ?'.format(filepath=args.config_file))
        exit(1)

    # Create the names for the database backup files and dump the databases.

    for database in config['databases']:
        db_backup_prefix = 'mysqldump_{database}'.format(database=database['NAME'])
        db_filename = '{backup_prefix}-{datetime:%Y}{datetime:%m}{datetime:%d}{datetime:%H}{datetime:%M}{datetime:%S}.sql'.format(datetime=datetime.now(), backup_prefix=db_backup_prefix)
        db_filepath = os.path.join(config['backup_directory'], db_filename)

        if args.create_dump:
            create_dump(config, database, db_filepath, db_filename, verbose=args.verbose)

    # Create the name(s) for the web folder backups and compress and send them.

    for folder in config['webfolders']:
        folders_backup_prefix = folder['DIR_SHORT_NAME']
        folders_filename = '{backup_prefix}-{datetime:%Y}{datetime:%m}{datetime:%d}{datetime:%H}{datetime:%M}{datetime:%S}'.format(datetime=datetime.now(), backup_prefix=folders_backup_prefix)
        folders_filepath = os.path.join(config['backup_directory'], folders_filename)

        if args.create_archive:
            create_archive(config, folder, folders_filepath, folders_filename, verbose=args.verbose)
