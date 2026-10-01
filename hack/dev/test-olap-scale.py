#!/usr/bin/env python3
"""Compare public repository methods on fixed fixtures in disposable databases."""
import argparse
import importlib.util
import json
import os
from pathlib import Path
import signal
import subprocess
import tempfile
import time
import urllib.parse
import uuid

ROOT=Path(__file__).resolve().parents[2]
spec=importlib.util.spec_from_file_location('visibility',ROOT/'hack/dev/test-olap-latency.py')
visibility=importlib.util.module_from_spec(spec)
spec.loader.exec_module(visibility)

def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output',type=Path,required=True)
    parser.add_argument('--binary-dir',type=Path,required=True)
    parser.add_argument('--tidb-v1-source',type=Path,required=True)
    parser.add_argument('--tidb-v1-binary-dir',type=Path,required=True)
    parser.add_argument('--backends',default='postgres,tidb-v1,tidb')
    parser.add_argument('--sizes',default='10000,100000,1000000')
    parser.add_argument('--samples',type=int,default=50)
    parser.add_argument('--max-memory-percent',type=float,default=90)
    args=parser.parse_args()
    backends=args.backends.split(',')
    if any(backend not in ('postgres','tidb-v1','tidb') for backend in backends):
        parser.error('invalid backend')
    args.output.mkdir(parents=True,exist_ok=True,mode=0o700)
    env_base=visibility.loaded_env()
    dbx={}
    for line in Path('/Users/fatcat/workspace/docker/dbx/.env').read_text().splitlines():
        if line.startswith('TIDB_') and '=' in line:
            key,value=line.split('=',1);dbx[key]=value.strip().strip('\"\'')
    mysql_env=dict(env_base,MYSQL_PWD=dbx['TIDB_PASSWORD'])
    mysql=['mysql','--connect-timeout=3','-h','127.0.0.1','-P',dbx.get('TIDB_PORT','4000'),'-u',dbx.get('TIDB_USER','root')]
    def sql(statement):
        return subprocess.run(mysql+['-N','-e',statement],env=mysql_env,check=True,capture_output=True,text=True,timeout=60).stdout
    def run(command,env,name):
        path=args.output/(name+'.log')
        with path.open('w') as log:
            path.chmod(0o600)
            subprocess.run(command,cwd=ROOT,env=env,stdout=log,stderr=log,check=True)
    def interrupted(_signum,_frame):
        raise KeyboardInterrupt
    signal.signal(signal.SIGTERM,interrupted)
    resources=[]
    child=None
    def cleanup_databases():
        errors=[]
        for kind,name in list(reversed(resources)):
            try:
                if kind=='tidb':
                    sql('DROP DATABASE IF EXISTS '+name)
                else:
                    subprocess.run(['docker','exec','dbx-postgres','psql','-U','root','-d','postgres','-v','ON_ERROR_STOP=1','-c','DROP DATABASE IF EXISTS '+name+' WITH (FORCE)'],check=True,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL,timeout=60)
                resources.remove((kind,name))
            except Exception:
                errors.append(kind+':'+name)
        if errors:
            raise RuntimeError('Could not clean owned databases: '+', '.join(errors))
    try:
        with tempfile.TemporaryDirectory(prefix='hatchet-scale-build-') as directory:
            work=Path(directory)
            overlay={}
            source=args.tidb_v1_source.resolve()
            for path in (ROOT/'pkg/repository/tidb').glob('*.go'):
                baseline=source/path.name
                overlay[str(path)]=str(baseline) if baseline.exists() else ''
            for path in source.glob('*.go'):
                overlay[str(ROOT/'pkg/repository/tidb'/path.name)]=str(path)
            # The loader passes two v2-only options; inert fields keep the v1 implementation buildable.
            compatibility=work/'config-v1.go'
            compatibility.write_text((source/'config.go').read_text().replace('type Config struct {','type Config struct {\n WriteConcurrency int\n TiFlashQueryTimeout time.Duration'))
            overlay[str(ROOT/'pkg/repository/tidb/config.go')]=str(compatibility)
            (work/'overlay.json').write_text(json.dumps({'Replace':overlay}))
            for backend in backends:
                command=['go','build','-o',str(work/backend)]
                if backend=='tidb-v1':
                    command+=['-overlay',str(work/'overlay.json')]
                command+=['hack/dev/benchmark-olap-scale.go']
                run(command,env_base,'build-'+backend)
            for backend in backends:
                database='olap_scale_'+uuid.uuid4().hex[:12]
                subprocess.run(['docker','exec','dbx-postgres','psql','-U','root','-d','postgres','-v','ON_ERROR_STOP=1','-c','CREATE DATABASE '+database,'-c','ALTER DATABASE '+database+" SET TIMEZONE='UTC'"],check=True,stdout=subprocess.DEVNULL)
                resources.append(('pg',database))
                pg_url=urllib.parse.urlsplit(env_base['DATABASE_URL'])
                env=dict(env_base,DATABASE_URL=urllib.parse.urlunsplit(pg_url._replace(path='/'+database)),DATABASE_POSTGRES_DB_NAME=database,DATABASE_OLAP_BACKEND='postgres',DATABASE_READ_REPLICA_ENABLED='false',READ_REPLICA_ENABLED='false',DATABASE_PGBOUNCER_URL='',SERVER_SECURITY_CHECK_ENABLED='false',SERVER_SAMPLING_ENABLED='false')
                run([str(args.binary_dir.resolve()/'migrate')],env,backend+'-migrate-pg')
                if backend!='postgres':
                    sql('CREATE DATABASE '+database);resources.append(('tidb',database))
                    env['DATABASE_TIDB_DSN']=f"{dbx.get('TIDB_USER','root')}:{dbx['TIDB_PASSWORD']}@tcp(127.0.0.1:{dbx.get('TIDB_PORT','4000')})/{database}"
                    migrator=args.tidb_v1_binary_dir if backend=='tidb-v1' else args.binary_dir
                    run([str(migrator.resolve()/'migrate-tidb'),'--tiflash'],env,backend+'-migrate-tidb')
                    env['DATABASE_OLAP_BACKEND']='tidb'
                print(backend+': fixed fixtures '+args.sizes,flush=True)
                path=args.output/(backend+'.log')
                with path.open('w') as log:
                    path.chmod(0o600)
                    child=subprocess.Popen([str(work/backend),'--backend',backend,'--sizes',args.sizes,'--samples',str(args.samples),'--output',str((args.output/(backend+'.json')).resolve())],cwd=ROOT,env=env,stdout=log,stderr=log,start_new_session=True)
                    while child.poll() is None:
                        time.sleep(5)
                        containers=['dbx-postgres'] if backend=='postgres' else ['dbx-tidb','dbx-tikv','dbx-tiflash']
                        stats=subprocess.check_output(['docker','stats','--no-stream','--format','{{json .}}']+containers,text=True,timeout=30)
                        with (args.output/'resources.jsonl').open('a') as resource_log:
                            for row in stats.splitlines():
                                value=json.loads(row);value.update(backend=backend,observed_at=time.time());resource_log.write(json.dumps(value)+'\n')
                                percent=float(value['MemPerc'].rstrip('%'))
                                if percent>=args.max_memory_percent and child.poll() is None:
                                    child.terminate()
                                    raise RuntimeError('Scale test stopped at the container memory guard; no dbx settings were changed')
                    if child.returncode:
                        raise RuntimeError(backend+' benchmark failed; inspect '+str(path))
                    child=None
                result=json.loads((args.output/(backend+'.json')).read_text())
                print(backend+': measured fixture sizes '+','.join(str(row['DatasetRows']) for row in result),flush=True)
                cleanup_databases()
    finally:
        try:
            if child is not None and child.poll() is None:
                child.terminate()
                try:child.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    child.kill();child.wait()
        finally:
            cleanup_databases()
        print('Owned scale-test processes and databases cleaned',flush=True)

if __name__=='__main__':main()
