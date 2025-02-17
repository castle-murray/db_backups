#! /usr/bin/imh-python3

from pathlib import Path
from datetime import datetime
import subprocess
import shutil
import hashlib
from bakauth impor BakAuth
import gzip


BACKUP_DIR = Path('/backups/db_backups')
AUTH_FILES = Path('/root/baksync_auth_files')
LIST_FILES = Path('/root/db_lists')
PRIMARY_AUTH_FILE = Path('/opt/backups/etc/auth.json')
RETENTION_DAYS = 14
TEST_MODE = True
LOG_FILE = Path('/var/log/baksynk_bakker.log')
if not LOG_FILE.exists():
    LOG_FILE.touch()


backed_up_dbs = []
failed_backups = []

if not BACKUP_DIR.exists():
    BACKUP_DIR.mkdir()

def gz

def compute_hash(file: Path):
    hash_func = hashlib.new('sha256')
    with file.open('rb') as f:
        while chunk := f.read(4096):
            hash_func.update(chunk)
    return hash_func.hexdigest()

def check_hash(file1: Path, file2: Path) -> bool:
    return compute_hash(file1) == compute_hash(file2)

def get_yesterdays_backup(current_dated_dir: Path,database: str) -> Path:
    dated_dirs = current_dated_dir.parent.glob('*')
    dated_dirs = sorted(dated_dirs)
    if len(dated_dirs) > 1:
        previous = dated_dirs[-2] / f'{database}.sql'
    else:
        raise FileNotFoundError('No previous backup found')
    if previous.exists():
        return previous
    else:
        raise FileNotFoundError('No previous backup found')
    

# List of servers
# at present only used for prefixes
servers = ['www2', 'www3', 'www6']

# Function to escape spaces in database names
def escape_space(string: str):
    return string.replace(' ', '\ ')

# pulls named database down from BUM
def backup_db(db: str, backup_dir: Path):
    backup_file= backup_dir / f'{db}.sql'
    command = f'baksync mysql root --latest --db {db} -f {backup_file}'
    subprocess.run(command, shell=True)
    if backup_file.exists():
        with LOG_FILE.open('a') as f:
            f.write(f'backed up {db} to {backup_file}\n')
            f.close()
        try:
            yesterday = get_yesterdays_backup(backup_dir, db)
        except FileNotFoundError:
            with LOG_FILE.open('a') as f:
                f.write(f'No previous backup found for {db}\n')
                f.close()
        else:
            if check_hash(backup_file, yesterday):
                with LOG_FILE.open('a') as f:
                    f.write(f'Backup for {db} matches previous backup\nhardlinking{yesterday} to {backup_file}\n')
                    f.close()
                backup_file.unlink()
                yesterday.link_to(backup_file)
    else:
        with LOG_FILE.open('a') as f:
            f.write(f'failed to backup {db} to {backup_file}\n')
            f.close()


# swaps the BUM auth file for the specified servers auth file
def swap_auth_file(server):
    auth_file = AUTH_FILES / f'{server}.auth.json'
    with open(PRIMARY_AUTH_FILE, 'w') as f:
        f.write(auth_file.read_text())
        f.close()

# gets the list of databases to backup from the specified server
# currently pulling from static files on the www1 server
# will soon be updated to pull from 
def get_db_list(server):
    list_file = LIST_FILES / f'{server}.list'
    with open(list_file, 'r') as f:
        db_list = f.read().splitlines()
    return list(map(escape_space, db_list))


# retain 14 days of backups
def backup_retention():
    for server in servers:
        print(server)
        server_backup_dir = BACKUP_DIR / server
        dated_dirs = server_backup_dir.glob('*')
        dated_dirs = sorted(dated_dirs)
        if len(dated_dirs) <= RETENTION_DAYS:
            with LOG_FILE.open('a') as f:
                f.write(f'only {len(dated_dirs)} items. Server is set to {RETENTION_DAYS} of retention.\n')
                f.close()
        else:
            for dir in dated_dirs[:-RETENTION_DAYS]:
                if not TEST_MODE:
                    shutil.rmtree(dir)
                else:
                    with LOG_FILE.open('a') as f:
                        f.write(f'would have deleted {dir}\n')
                        f.close()            
        

def main():
    for server in servers:
        backup_dir = BACKUP_DIR / server
        if not backup_dir.exists():
            backup_dir.mkdir()
        daily_dir = backup_dir / datetime.now().strftime('%Y-%m-%d')
        if not daily_dir.exists():
            daily_dir.mkdir()

        swap_auth_file(server)
        db_list = get_db_list(server)
        for db in db_list:
            backup_db(db, daily_dir)

    swap_auth_file('www1')
    backup_retention()

if __name__ == '__main__':
    main()

