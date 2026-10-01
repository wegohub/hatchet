#!/usr/bin/env python3
"""Run shared PG/TiDB contracts in disposable databases; credentials stay in child environments."""
import argparse
import json
import os
from pathlib import Path
import subprocess
import signal
import tempfile
import time
import urllib.parse
import uuid

ROOT = Path(__file__).resolve().parents[2]

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--race', action='store_true')
    parser.add_argument('--run', default='.')
    parser.add_argument('--max-tidb-memory-percent', type=float, default=90,
                        help='Stop owned tests before the shared TiDB container exhausts memory')
    args = parser.parse_args()
    raw = subprocess.check_output(['bash', '-c', 'set +x; set -a; source .env; env -0'], cwd=ROOT)
    env = dict(entry.decode().split('=', 1) for entry in raw.split(b'\0') if b'=' in entry)
    dbx = {}
    for line in Path('/Users/fatcat/workspace/docker/dbx/.env').read_text().splitlines():
        if line.strip() and not line.startswith('#') and '=' in line:
            key, value = line.split('=', 1)
            dbx[key] = value.strip().strip('\"\'')
    env['TIDB_TEST_DSN'] = f"{dbx.get('TIDB_USER','root')}:{dbx['TIDB_PASSWORD']}@tcp(127.0.0.1:{dbx.get('TIDB_PORT','4000')})/"
    suffix = uuid.uuid4().hex[:12]
    database = 'hatchet_contract_' + suffix
    prefix = 'hatchet_olap_test_' + suffix + '_'
    env.update(TIDB_TEST_REUSE_SCHEMA='1', TIDB_TEST_DATABASE_PREFIX=prefix)
    pg = urllib.parse.urlsplit(env['DATABASE_URL'])
    env.update(DATABASE_URL=urllib.parse.urlunsplit(pg._replace(path='/' + database)), DATABASE_POSTGRES_DB_NAME=database,
               DATABASE_OLAP_BACKEND='postgres', SERVER_SECURITY_CHECK_ENABLED='false',
               DATABASE_READ_REPLICA_ENABLED='false', READ_REPLICA_ENABLED='false', DATABASE_PGBOUNCER_URL='')
    env['TIDB_CONTRACT_POSTGRES_URL'] = env['DATABASE_URL']
    created = False
    child = None
    print('Owned contract resources: PG=' + database + ' TiDB prefix=' + prefix, flush=True)
    with tempfile.TemporaryDirectory(prefix='hatchet-tidb-contract-') as directory:
        work = Path(directory)
        try:
            subprocess.run(['docker','exec','dbx-postgres','psql','-U','root','-d','postgres','-v','ON_ERROR_STOP=1',
                            '-c','CREATE DATABASE ' + database,'-c','ALTER DATABASE ' + database + " SET TIMEZONE='UTC'"],check=True,stdout=subprocess.DEVNULL)
            created = True
            with (work/'migration.log').open('w') as log:
                for command in [['go','build','-o',str(work/'migrate'),'./cmd/hatchet-migrate'],[str(work/'migrate')]]:
                    subprocess.run(command,cwd=ROOT,env=env,stdout=log,stderr=log,check=True)
            command = ['go','test','./pkg/repository/tidb','-v','-count=1','-timeout=20m','-run',args.run]
            if args.race:
                command.append('-race')
            child = subprocess.Popen(command,cwd=ROOT,env=env,start_new_session=True)
            while child.poll() is None:
                time.sleep(1)
                value = subprocess.check_output(['docker','stats','--no-stream','--format','{{.MemPerc}}','dbx-tidb'],text=True,timeout=15)
                if float(value.strip().rstrip('%')) >= args.max_tidb_memory_percent and child.poll() is None:
                    raise RuntimeError('Contract tests stopped at the TiDB memory guard; dbx settings were not changed')
            raise SystemExit(child.returncode)
        finally:
            if child is not None and child.poll() is None:
                os.killpg(child.pid, signal.SIGTERM)
                try:
                    child.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    os.killpg(child.pid, signal.SIGKILL)
                    child.wait()
            errors = []
            mysql_env = dict(env, MYSQL_PWD=dbx['TIDB_PASSWORD'])
            mysql = ['mysql','--connect-timeout=3','-h','127.0.0.1','-P',dbx.get('TIDB_PORT','4000'),'-u',dbx.get('TIDB_USER','root')]
            try:
                inventory = subprocess.run(mysql + ['-N','-e',"SELECT SCHEMA_NAME FROM information_schema.SCHEMATA WHERE LEFT(SCHEMA_NAME," + str(len(prefix)) + ")='" + prefix + "'"], env=mysql_env, capture_output=True, text=True, timeout=30, check=True)
                for name in inventory.stdout.splitlines():
                    if name.startswith(prefix) and name.replace('_','').isalnum():
                        subprocess.run(mysql + ['-e','DROP DATABASE `' + name + '`'], env=mysql_env, check=True, stdout=subprocess.DEVNULL, timeout=60)
            except Exception:
                errors.append('TiDB prefix ' + prefix)
            if created:
                try:
                    subprocess.run(['docker','exec','dbx-postgres','psql','-U','root','-d','postgres','-v','ON_ERROR_STOP=1',
                                    '-c','DROP DATABASE ' + database + ' WITH (FORCE)'],check=True,stdout=subprocess.DEVNULL,timeout=60)
                except Exception:
                    errors.append('PG database ' + database)
            if errors:
                raise RuntimeError('Owned resources still need cleanup: ' + ', '.join(errors))
            print('Isolated PG database and TiDB test databases cleaned',flush=True)

if __name__ == '__main__':
    main()
