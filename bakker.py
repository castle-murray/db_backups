#! /usr/bin/imh-python3

from pathlib import Path
from datetime import datetime
import subprocess
import shutil
import hashlib
import gzip
from bakauth import BakAuth

BACKUP_DIR = Path('/backups/db_backups')
LIST_FILES = Path('/root/db_lists')
PRIMARY_AUTH_FILE = Path('/opt/backups/etc/auth.json')
RETENTION_DAYS = 14
TEST_MODE = True
LOG_FILE = Path('/var/log/baksynk_bakker.log')

if not LOG_FILE.exists():
    LOG_FILE.touch()
if not BACKUP_DIR.exists():
    BACKUP_DIR.mkdir()


def gzip_file(file: Path):
    with file.open('rb') as f:
        with gzip.open(f'{file}.gz', 'wb') as g:
            g.writelines(f)
    file.unlink()


def compute_hash(file: Path):
    hash_func = hashlib.new('sha256')
    hash_dir = file.parent / '.hashes'
    
    if not hash_dir.exists():
        hash_dir.mkdir()
    with file.open('rb') as f:
        while chunk := f.read(4096):
            hash_func.update(chunk)
    hash_file = hash_dir / f'{file.name}.hash'
    with hash_file.open('w') as f:
        f.write(hash_func.hexdigest())
        f.close()
    return hash_func.hexdigest()


def read_hash(file: Path):
    hash_dir = file.parent / '.hashes'
    hash_file = hash_dir / f'{file.name.strip(".gz")}.hash'


    if not hash_file.exists():
        return None
    with hash_file.open('r') as f:
        return f.read()


def check_hash(file1: Path, file2: Path) -> bool:
    if not file2.exists():
        return False
    hash1 = compute_hash(file1)
    hash2 = read_hash(file2)
    return hash1 == hash2


def get_yesterdays_backup(current_dated_dir: Path,database: str) -> Path:
    dated_dirs = current_dated_dir.parent.glob('*')
    dated_dirs = sorted(dated_dirs)
    if len(dated_dirs) > 1:
        previous = dated_dirs[-2] / f'{database}.sql.gz'
    else:
        raise FileNotFoundError('No previous backup found')
    if previous.exists():
        return previous
    else:
        raise FileNotFoundError('No previous backup found')
    

# Function to escape spaces in database names
def escape_space(string: str):
    return string.replace(' ', '\ ')


def get_dbs():
    db_objects = []
    restic = BakAuth().get_restic()
    backups = restic.get_backups(serialize=False)
    for user in backups:
        if not 'mysql' in backups[user]:
            continue
        backups[user]['mysql'].sort(key=lambda x: x.time)
        latest = backups[user]['mysql'][-1]
        for db in latest.dbs:
            db_objects.append((escape_space(db), latest.dbs[db]))
    return db_objects        
    

# pulls named database down from BUM
def backup_dbs(backup_dir: Path):
#    db = dbt[0]
#    cpuser = dbt[1]
    db_objects = get_dbs()
    for db, dbo in db_objects:
        dump_cmd = dbo.dump()
        backup_file = backup_dir / f'{db}.sql'
        try:
            with backup_file.open('wb') as f:
                dump_cmd.run(check=True, stdout=f)
            compute_hash(backup_file)
        except subprocess.CalledProcessError:
            backup_file.unlink()

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
                    gzip_file(backup_file)
            else:
                if check_hash(backup_file, yesterday):
                    with LOG_FILE.open('a') as f:
                        f.write(f'Backup for {db} matches previous backup\nhardlinking{yesterday} to {backup_file}\n')
                        f.close()
                    backup_file.unlink()
                    #backup_file.link_to(yesterday)
                    yesterday.link_to(backup_file)
                else:
                    gzip_file(backup_file)
                    with LOG_FILE.open('a') as f:
                        f.write(f'Backup for {db} does not match previous backup\n')
                        f.close()
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
    dated_dirs = BACKUP_DIR.glob('*')
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
    
#args = ArgumentParser()
#args.add_argument("--host", help="The server to backup")
#args.add_argument("--user", help="The mysql user with access to all databases")
#args.add_argument("--password", help="The password for the mysql user")



def main():
    #backup_dir = BACKUP_DIR / server
    daily_dir = BACKUP_DIR / datetime.now().strftime('%Y-%m-%d')
    if not daily_dir.exists():
        daily_dir.mkdir()

    #swap_auth_file(server)
    #db_list = get_db_list(server)
    backup_dbs(daily_dir)
    backup_retention()

if __name__ == '__main__':
    main()

