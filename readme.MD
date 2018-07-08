# Backup to S3
This python script will prepare and then copy the selected
database dumps and file directories to an AWS S3 bucket of your
choice. The backup files will be compressed and deleted after the
files are copied to S3.

NOTE: The use of S3 incurs a cost. You are solely responsible for 
managing the use of that system and any costs incurred.

## Requirements
- Python and pip
- The python libs listed in requirements.txt
- Access to an Amazon Web Services account and a valid set of API keys.
- An AWS S3 bucket.
- The S3 bucket should also have retention policies set on it. You may want to 
expire content into Glacier after a certain time.
- The mysqldump utility to be available on the system.
- A database user account that can read the target databases. DO NOT use your root account.

## Installation
Copy the files in this repo or just "git clone" them to the machine that needs
backing up. 

Install the libraries listed in the requirements.txt file.
i.e. "pip install boto3 argparse --user"

Rename and change the config file to suit your own needs. Run the
script manually to make sure your config is working as expected.

If all is good then add it to your crontab to run as often as you like. Each backup file
is named with the current timestamp to the second so multiple backups
each day can be identified.

Run the backup as below. Full paths defined if you're putting it into crontab and
based on a Ubuntu machine layout. User home is ubuntu in this example.

"/usr/bin/python /home/ubuntu/backup-to-s3/backup-to-s3.py backup-to-s3.json"

### Configuration
All the configuration required should be done in your configuration file. A default
file called "backup-to-s3.json.default" should have been included. Just make a copy of this
and update the values in the file as needed.
Note that the BUCKET_KEY_* values can be defined as folders using forward slashes. If the
folder does not exist in the bucket, they will be created.
You can add as many extra JSON sections for databaes and directories as you like.

To create a database user with read access to all the databases you can run this
as the database root user. It'd be better though if you only allocated this to the
actual databases being backed up. This command in MySQL will create a user called
"s3backup".

GRANT LOCK TABLES, SELECT ON *.* TO 's3backup'@'%' IDENTIFIED BY 'password';

The user you run the script as will need read permission to the directories that you
want to be backed up.

## ToDo
- Allow a sub-folder in a target directory to be excluded from the backup.