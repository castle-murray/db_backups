#!/usr/bin/env python3

import argparse
import hashlib
import gzip
import shutil
import subprocess
from datetime import datetime
from pathlib import Path

CWD = Path(__file__).parent
BACKUP_DIR = CWD / ('backups')
RETENTION = 14
TEST_MODE = False  # set to True to test without deleting older backups
LOG_FILE = CWD / ('baksynk_bakker.log')
CURRENT_DATE = datetime.now().strftime('%Y-%m-%d')

if not LOG_FILE.exists():
    LOG_FILE.touch()
if not BACKUP_DIR.exists():
    BACKUP_DIR.mkdir()

# utils

def log(msg: str):
    """Simple log appender."""
    current_time = datetime.now().strftime('%H:%M:%S')
    with LOG_FILE.open('a') as f:
        f.write(f"{CURRENT_DATE}-{current_time}:{msg}\n")

def gzip_file(file: Path):
    """Gzip the given file and remove the original."""
    with file.open('rb') as f_in, gzip.open(f'{file}.gz', 'wb') as f_out:
        shutil.copyfileobj(f_in, f_out)
    file.unlink()

def compute_hash(file: Path) -> str:
    """
    creates a hash file prior to gzipping
    This has to be done before gzipping because gzip timestamps the file
    """
    hash_func = hashlib.new('sha256')
    hash_dir = file.parent / '.hashes'
    hash_dir.mkdir(exist_ok=True)

    with file.open('rb') as f:
        while True:
            chunk = f.read(4096)
            if not chunk:
                break
            hash_func.update(chunk)
    hash_hex = hash_func.hexdigest()
    #
    hash_file = hash_dir / f'{file.name}.hash'
    with hash_file.open('w') as hf:
        hf.write(hash_hex)

    return hash_hex

def read_hash(file: Path) -> str | None:
    """returns hash for a given backup file"""
    hash_dir = file.parent / '.hashes'
    #all previous file paths have a .gz extension but the 
    #hash file does not so we strip it before looking for the hash file
    base_name = file.name
    base_name = base_name.strip('.gz')

    hash_file = hash_dir / f'{base_name}.hash'
    
    if not hash_file.exists():
        return None
    return hash_file.read_text().strip()

def check_hash(new_file: Path, old_file: Path) -> bool:
    """
    compares current backups hash with previous backups hash
    """
    if not old_file.exists():
        return False
    new_hash = compute_hash(new_file)
    old_hash = read_hash(old_file)
    if old_hash is None:
        return False
    return new_hash == old_hash

def get_previous_backup(current_dated_dir: Path, database: str) -> Path:
    """
    Locates the most recent backup of the same file
    """
    dated_dirs = sorted(current_dated_dir.parent.glob('*'))
    if len(dated_dirs) > 1:
        previous_dir = dated_dirs[-2]
        candidate = previous_dir / f'{database}.sql.gz'
        if candidate.exists():
            return candidate
        else:
            raise FileNotFoundError("No previous backup .sql.gz found.")
    else:
        raise FileNotFoundError("No previous dated directory found.")

def backup_retention():
    """
    retention policy for backups
    it says days but it's really the number of backups to keep
    I'm not fixing it.
    """
    dated_dirs = sorted(BACKUP_DIR.glob('*'))
    num_dirs = len(dated_dirs)
    if num_dirs <= RETENTION:
        log(f"Currently have {num_dirs} backups. Retention set to {RETENTION_DAYS}. Nothing to remove.")
    else:
        to_remove = dated_dirs[:-RETENTION]
        for old_dir in to_remove:
            if TEST_MODE:
                log(f"[TEST_MODE] Would remove {old_dir}")
            else:
                shutil.rmtree(old_dir)
                log(f"Removed old backup directory: {old_dir}")

# ---------------------------------------------------------------------
# Database Functions (using mysqldump)
# ---------------------------------------------------------------------
def list_databases(host: str, port: int, user: str, password: str) -> list[str]:
    """Return a list of databases, filtering out MySQL's default schemas."""
    cmd = [
        "mysql",
        "--ssl=0",
        f"--host={host}",
        f"--port={port}",
        f"--user={user}",
        f"--password={password}",
        "-e",
        "SHOW DATABASES;"
    ]
    result = subprocess.run(cmd, capture_output=True, text=True, check=True)
    # The first line is typically "Database"
    all_dbs = result.stdout.splitlines()
    # Filter out system dbs that often aren't needed in backups
    skip_dbs = {"Database", "information_schema", "performance_schema", "mysql", "sys"}
    return [db for db in all_dbs if db not in skip_dbs]

def backup_databases(backup_dir: Path, host: str, port: int, user: str, password: str):
    """
    For each database on the server, run mysqldump, compute the hash, and
    check if we can deduplicate (hardlink) against previous's backup.
    """
    databases = list_databases(host, port, user, password)
    for db in databases:
        print(db)
        backup_file = backup_dir / f'{db}.sql'
        dump_cmd = [
            "mysqldump",
            "--skip-comments",
            "--ssl=0",
            f"--host={host}",
            f"--port={port}",
            f"--user={user}",
            f"--password={password}",
            db
        ]
        try:
            # Write the SQL dump
            with backup_file.open('wb') as out_sql:
                subprocess.run(dump_cmd, stdout=out_sql, stderr=subprocess.PIPE, check=True)
            compute_hash(backup_file)

            # Attempt to locate a previous backup
            try:
                previous_backup = get_previous_backup(backup_dir, db)
            except FileNotFoundError:
                # No previous backup -> just compress
                log(f"No previous backup for {db} found, gzipping new backup.")
                gzip_file(backup_file)
            else:
                # We found a previous .sql.gz file
                if check_hash(backup_file, previous_backup):
                    # If matches, remove the new backup_file and hardlink old to new
                    log(f"Backup for {db} matches previous backup. Hardlinking old -> new.")
                    backup_file.unlink()
                    gzip_filename = backup_dir / f'{db}.sql.gz'
                    #previous_backup.hardlink_to(gzip_filename)
                    gzip_filename.hardlink_to(previous_backup)

                else:
                    log(f"Backup for {db} differs from previous backup. Compressing new dump.")
                    gzip_file(backup_file)

        except subprocess.CalledProcessError as e:
            log(f"[ERROR] Failed to backup {db}: {e}")
            if backup_file.exists():
                backup_file.unlink()

# ---------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="Backup MySQL databases to local directory.")
    parser.add_argument("--host", default="127.0.0.1", help="MySQL host")
    parser.add_argument("--port", default=3306, type=int, help="MySQL port")
    parser.add_argument("--user", required=True, help="MySQL user with ALL privileges")
    parser.add_argument("--password", required=True, help="MySQL password for that user")

    args = parser.parse_args()

    daily_dir = BACKUP_DIR / CURRENT_DATE
    daily_dir.mkdir(exist_ok=True)

    backup_databases(
        backup_dir=daily_dir,
        host=args.host,
        port=args.port,
        user=args.user,
        password=args.password
    )

    backup_retention()

if __name__ == "__main__":
    main()

