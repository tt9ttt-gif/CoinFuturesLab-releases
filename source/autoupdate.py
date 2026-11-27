"""Signed local-drop/GitHub update client, release builder and detached installer."""
import argparse
import base64
from contextlib import contextmanager
import hashlib
import io
import json
import os
from pathlib import Path
import re
import shutil
import socket
import sqlite3
import subprocess
import sys
import tempfile
import threading
import time
from urllib.parse import urlparse
from urllib.request import HTTPRedirectHandler, build_opener, Request
from zipfile import ZipFile, ZIP_DEFLATED

ROOT=Path(__file__).resolve().parent
DATA=ROOT/'desktop-data'
CONF=DATA/'update-config.json'
STATE=DATA/'update-state.json'
VERSION=ROOT/'version.json'
ASSET='CoinFuturesLab-update.zip'
MAX=32*1024*1024
FILES=('desktop.py','message_format.py','v17/live.py','v17/growth.py','v17/policy.py','requirements.txt')
CONTRACT=1
DESTS={'github.com','release-assets.githubusercontent.com','objects.githubusercontent.com','github-releases.githubusercontent.com'}

def read(path,default=None):
    try: return json.loads(path.read_text(encoding='utf-8'))
    except FileNotFoundError: return default

def write(path,value):
    path.parent.mkdir(parents=True,exist_ok=True)
    with tempfile.NamedTemporaryFile(dir=path.parent,delete=False,mode='w',encoding='utf-8') as f:
        temp=Path(f.name)
        json.dump(value,f,ensure_ascii=False,indent=2)
    temp.replace(path)

def version(value):
    if not isinstance(value,str) or not re.fullmatch(r'[0-9]{1,5}(\.[0-9]{1,5}){1,2}',value): raise ValueError('invalid version')
    return tuple(map(int,(value+'.0' if value.count('.')==1 else value).split('.')))

def current():
    return read(VERSION)['version']

def config():
    return read(CONF,{})

def change_state(phase,**fields):
    s=read(STATE,{})
    s.update(phase=phase,updated=time.time(),**fields)
    write(STATE,s)

class Redirect(HTTPRedirectHandler):
    def redirect_request(self,req,fp,code,msg,headers,newurl):
        dest=urlparse(newurl)
        if dest.scheme!='https' or dest.hostname not in DESTS or dest.username or dest.password or dest.port not in (None,443):
            raise ValueError('release redirect not allowed')
        return super().redirect_request(req,fp,code,msg,headers,newurl)

def asset(cfg):
    if cfg.get('source')=='folder':
        with (Path(cfg['path'])/ASSET).open('rb') as f: return f.read(MAX+1)
    repo=cfg.get('repo','')
    if cfg.get('source')!='github' or not re.fullmatch(r'[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+',repo):
        raise ValueError('configure a release source')
    url=f'https://github.com/{repo}/releases/latest/download/{ASSET}'
    with build_opener(Redirect()).open(Request(url,headers={'User-Agent':'CoinFuturesLab-Updater'}),timeout=20) as r:
        return r.read(MAX+1)

def verify(blob,key):
    from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey
    if len(blob)>MAX: raise ValueError('package exceeds maximum size')
    with ZipFile(io.BytesIO(blob)) as z:
        expected={'manifest.json','manifest.sig',*FILES}
        info=z.infolist()
        if len(info)!=len(expected) or {i.filename for i in info}!=expected or sum(i.file_size for i in info)>MAX:
            raise ValueError('unexpected package contents')
        raw=z.read('manifest.json')
        Ed25519PublicKey.from_public_bytes(base64.b64decode(key,validate=True)).verify(z.read('manifest.sig'),raw)
        manifest=json.loads(raw)
        version(manifest['version']); version(manifest['minimum'])
        if manifest['contract']!=CONTRACT or manifest.get('db_schema')!='unchanged':
            raise ValueError('update requires manual compatibility review')
        if version(current())<version(manifest['minimum']): raise ValueError('installed version below minimum')
        if set(manifest['sha256'])!=set(FILES): raise ValueError('incomplete release')
        files={name:z.read(name) for name in FILES}
        if any(hashlib.sha256(b).hexdigest()!=manifest['sha256'][name] for name,b in files.items()):
            raise ValueError('release digest mismatch')
    return manifest,files

def get_release():
    cfg=config()
    blob=asset(cfg)
    manifest,_=verify(blob,cfg['key'])
    return manifest,blob

def guard(port):
    sock=socket.socket()
    try:
        if os.name=='nt': sock.setsockopt(socket.SOL_SOCKET,socket.SO_EXCLUSIVEADDRUSE,1)
        sock.bind(('127.0.0.1',port))
        return sock
    except OSError:
        sock.close()
        return None

def stop_market():
    for n in range(105):
        lock=guard(47818)
        if lock: return lock
        if n==0: (DATA/'stop').touch()
        time.sleep(1)
    raise RuntimeError('market monitor did not stop in time')

def atomic_bytes(path,data):
    with tempfile.NamedTemporaryFile(dir=path.parent,delete=False) as f:
        tmp=Path(f.name)
        f.write(data)
    tmp.replace(path)

def snapshot():
    path=ROOT/'upgrade-backups'/('auto-'+str(time.time_ns()))
    path.mkdir(parents=True)
    for name in (*FILES,'version.json'):
        dest=path/name
        dest.parent.mkdir(parents=True,exist_ok=True)
        shutil.copy2(ROOT/name,dest)
    for name in ('desktop-data/desktop.sqlite3','v17/results/paper.sqlite3'):
        src=ROOT/name
        if src.exists():
            dest=path/name
            dest.parent.mkdir(parents=True,exist_ok=True)
            with sqlite3.connect(src) as a,sqlite3.connect(dest) as b:
                a.backup(b)
                if b.execute('PRAGMA quick_check').fetchone()[0]!='ok': raise RuntimeError('database backup validation failed')
    return path

def restore(path):
    for name in (*FILES,'version.json'):
        atomic_bytes(ROOT/name,(path/name).read_bytes())

def notice(message):
    try:
        import desktop
        token,chat=desktop.credentials()
        if token and chat:
            desktop.tg(token,'sendMessage',dict(chat_id=chat,text=message[:3500]))
    except Exception:
        pass

def monitor(python=None):
    env=dict(os.environ,PYTHONUTF8='1')
    return subprocess.Popen([str(python or runtime()),str(ROOT/'desktop.py'),'run'],cwd=ROOT,env=env,
                            **({'creationflags':subprocess.CREATE_NEW_CONSOLE} if os.name=='nt' else {'start_new_session':True}))

def runtime():
    return read(VERSION,{}).get('runtime') or sys.executable

def check_runtime(python):
    proc=subprocess.run([str(python),str(ROOT/'autoupdate.py'),'probe'],cwd=ROOT,timeout=105,capture_output=True)
    if proc.returncode: raise RuntimeError('market or Telegram health check failed')

def ready(child):
    for _ in range(20):
        time.sleep(.5)
        if child.poll() is not None: raise RuntimeError('monitor exited on startup')
        if not (DATA/'stop').exists() and guard_probe(): return
    raise RuntimeError('monitor did not open its lock')

def guard_probe():
    probe=guard(47818)
    if probe is None: return True
    probe.close()
    return False

def install(action):
    serial=guard(47819)
    if serial is None: return
    lock=None; backup=None; child=None
    saved=read(STATE,{})
    original=current()
    attempted=None
    try:
        DATA.mkdir(exist_ok=True)
        change_state('preparing')
        if action=='rollback':
            raw=saved.get('previous')
            if not raw: raise ValueError('no previous verified version')
            previous=Path(raw).resolve()
            if previous.parent!=(ROOT/'upgrade-backups').resolve(): raise ValueError('invalid backup path')
            release=read(previous/'version.json')
            files={f:(previous/f).read_bytes() for f in FILES}
        else:
            release,files=verify((DATA/'update-download.zip').read_bytes(),config()['key'])
            attempted=release['version']
            if version(release['version'])<=version(original): raise ValueError('release is not newer')
        python=release.get('runtime') if action=='rollback' else runtime()
        python=python or sys.executable
        if action!='rollback' and files['requirements.txt']!=(ROOT/'requirements.txt').read_bytes():
            from pip._vendor.packaging.requirements import Requirement
            for line in files['requirements.txt'].decode().splitlines():
                line=line.strip()
                if line and not line.startswith('#') and Requirement(line).url:
                    raise ValueError('direct dependency URLs are not allowed')
            runtime_dir=DATA/('release-runtime-'+str(time.time_ns()))
            subprocess.run([sys.executable,'-m','venv',str(runtime_dir)],check=True,timeout=180,capture_output=True)
            python=runtime_dir/('Scripts/python.exe' if os.name=='nt' else 'bin/python')
            req=runtime_dir/'release-requirements.txt'
            req.write_bytes(files['requirements.txt']+b'\ncryptography>=44,<52\n')
            subprocess.run([str(python),'-m','pip','--isolated','install','--index-url','https://pypi.org/simple','-r',str(req)],check=True,timeout=900,capture_output=True)
        # Validate source before asking the running monitor to stop.
        for name,value in files.items():
            if name.endswith('.py'): compile(value,name,'exec')
        change_state('stopping')
        lock=stop_market()
        backup=snapshot()
        change_state('installing')
        for name,value in files.items(): atomic_bytes(ROOT/name,value)
        write(VERSION,dict(version=release['version'],runtime=str(python)))
        check_runtime(python)
        lock.close();lock=None
        child=monitor(python)
        ready(child)
        history=saved.get('history',[])+[dict(action=action,version=current(),at=time.time())]
        change_state('success',installed=current(),previous=str(backup),history=history[-30:],error='',rejected=None)
        notice(f"업데이트 완료 · {current()}\n시장 데이터 연결 확인 · 기존 모의 원장 유지")
    except Exception as exc:
        if child and child.poll() is None:
            (DATA/'stop').touch()
            try: child.wait(timeout=105)
            except subprocess.TimeoutExpired: child.terminate(); child.wait(timeout=10)
        recovered=False
        if backup:
            try:
                if lock is None: lock=stop_market()
                restore(backup)
                lock.close();lock=None
                ready(monitor())
                recovered=True
            except Exception:
                change_state('recovery_failed',error=type(exc).__name__)
        if not backup:
            if lock: lock.close(); lock=None
            if not guard_probe():
                try: ready(monitor()); recovered=True
                except Exception: pass
            else: recovered=True
        change_state('rolled_back' if recovered else 'recovery_failed',rejected=attempted,error=f'{type(exc).__name__}: {str(exc)[:240]}')
        notice(f"업데이트 실패 · {type(exc).__name__}\n"+(f"{original} 감시 유지 또는 복구 확인" if recovered else '자동 복구 미확인. 업데이트 상태를 확인하세요.'))
    finally:
        if lock: lock.close()
        serial.close()

REQUEST_LOCK=threading.Lock()
def detach(action):
    subprocess.Popen([sys.executable,str(ROOT/'autoupdate.py'),action],cwd=ROOT,stdin=subprocess.DEVNULL,
                     stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL,
                     **({'creationflags':subprocess.DETACHED_PROCESS|subprocess.CREATE_NEW_PROCESS_GROUP}
                        if os.name=='nt' else {'start_new_session':True}))

def check(apply=False,auto_check=False):
    if not REQUEST_LOCK.acquire(blocking=False): return '업데이트 확인이 이미 진행 중입니다.'
    try:
        cfg=config()
        probe=guard(47819)
        if probe is None: return '설치가 진행 중입니다.'
        probe.close()
        if not cfg.get('source'): return '배포 채널 미설정. UpdateSetup.bat을 한 번 실행하세요.'
        release,blob=get_release()
        new=release['version']
        change_state('available' if version(new)>version(current()) else 'current',latest=new,notes=release.get('notes',''))
        if version(new)<=version(current()): return f'최신 버전 {current()}'
        apply=apply or (auto_check and cfg.get('auto',False))
        state=read(STATE,{})
        description=f"신규 {new} · 현재 {current()}\n{release.get('notes','')}\n예상 설치 {release.get('eta','1~3분, 의존성 변경 시 추가')}\n자동 설치 {'ON' if cfg.get('auto') else 'OFF'}"
        if state.get('announced')!=new:
            notice(description)
            change_state('available',announced=new)
        if apply and auto_check and state.get('rejected')==new:
            return '이 배포본은 이전 설치 실패 후 자동 재시도를 보류합니다.'
        if apply:
            atomic_bytes(DATA/'update-download.zip',blob)
            detach('install')
            return f'{new} 설치를 요청했습니다.'
        return description
    except Exception as exc:
        change_state('check_failed',error=type(exc).__name__)
        return f'업데이트 확인 실패 ({type(exc).__name__}). 현재 감시는 계속됩니다.'
    finally:
        REQUEST_LOCK.release()

def watch(stop):
    while not stop.is_set():
        check(auto_check=True)
        if stop.wait(1800): return

def command(text):
    if text=='/version': return '설치 버전 '+current()+'\n설치 폴더 '+str(ROOT)
    parts=text.split()
    if parts==['/update']: return check(apply=True)
    if parts==['/update','check']: return check()
    if parts==['/update','status']:
        s=read(STATE,{})
        return f"현재 {current()} · 배포 {s.get('latest','미확인')}\n상태 {s.get('phase','대기')}\n{s.get('error','')}"
    if parts==['/update','history']:
        return '\n'.join(f"{time.strftime('%Y-%m-%d %H:%M',time.localtime(x['at']))} {x['action']} {x['version']}" for x in read(STATE,{}).get('history',[])) or '설치 기록 없음'
    if parts==['/update','rollback']:
        if not read(STATE,{}).get('previous'): return '복구 가능한 이전 버전 없음'
        cfg=config();cfg['auto']=False;write(CONF,cfg)
        detach('rollback')
        return '이전 버전 복구 요청. 자동 설치를 껐습니다.'
    if parts[1:] in (['auto','on'],['auto','off']):
        cfg=config();cfg['auto']=parts[-1]=='on';write(CONF,cfg)
        return '자동 설치 '+('ON' if cfg['auto'] else 'OFF')
    return '/version · /update check|status|history|rollback · /update auto on|off'

def setup():
    kind=input('Release source [github/folder]: ').strip()
    if kind not in ('github','folder'): raise SystemExit('Unknown source')
    value=input('GitHub owner/repo: ' if kind=='github' else 'Release folder: ').strip().strip('"')
    key=input('Trusted publisher Ed25519 public key: ').strip()
    if len(base64.b64decode(key,validate=True))!=32: raise ValueError('Invalid public key')
    if kind=='github' and not re.fullmatch(r'[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+',value): raise ValueError('Invalid GitHub repository')
    if kind=='folder' and not Path(value).is_dir(): raise ValueError('Release folder unavailable')
    write(CONF,dict(source=kind,key=key,auto=False,**({'repo':value} if kind=='github' else {'path':str(Path(value).resolve())})))

def publish(args):
    from cryptography.hazmat.primitives import serialization
    from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
    key=Path(args.key)
    if args.action=='keygen':
        private=Ed25519PrivateKey.generate()
        fd=os.open(key,os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600)
        with os.fdopen(fd,'wb') as f:
            f.write(private.private_bytes(serialization.Encoding.Raw,serialization.PrivateFormat.Raw,serialization.NoEncryption()))
        print(base64.b64encode(private.public_key().public_bytes(serialization.Encoding.Raw,serialization.PublicFormat.Raw)).decode())
        return
    private=Ed25519PrivateKey.from_private_bytes(key.read_bytes())
    version(args.version)
    files={f:(Path(args.source)/f).read_bytes() for f in FILES}
    for f,b in files.items():
        if f.endswith('.py'): compile(b,f,'exec')
    manifest=dict(version=args.version,minimum='18.3',contract=CONTRACT,db_schema='unchanged',notes=args.notes,
                  sha256={f:hashlib.sha256(b).hexdigest() for f,b in files.items()})
    raw=json.dumps(manifest,ensure_ascii=False,sort_keys=True).encode()
    folder=Path(args.output);folder.mkdir(parents=True,exist_ok=True)
    buff=io.BytesIO()
    with ZipFile(buff,'w',ZIP_DEFLATED) as z:
        for name,value in files.items(): z.writestr(name,value)
        z.writestr('manifest.json',raw); z.writestr('manifest.sig',private.sign(raw))
    atomic_bytes(folder/ASSET,buff.getvalue())

def main():
    p=argparse.ArgumentParser()
    p.add_argument('action',choices=['setup','install','rollback','launch','probe','keygen','publish'])
    p.add_argument('--key');p.add_argument('--version');p.add_argument('--source',default='.');p.add_argument('--output',default='release');p.add_argument('--notes',default='')
    a=p.parse_args()
    if a.action=='setup': setup()
    elif a.action in ('keygen','publish'): publish(a)
    elif a.action=='launch':
        sys.exit(subprocess.call([str(runtime()),str(ROOT/'desktop.py'),'run'],cwd=ROOT))
    elif a.action=='probe':
        import desktop
        report=desktop.live.tick(read_only=True,market_api=desktop.ORIGINAL_API)
        if len(report['market'])!=2: sys.exit(1)
        token,chat=desktop.credentials()
        if token and chat:
            desktop.tg(token,'getMe',{})
            desktop.tg(token,'getChat',{'chat_id':chat})
    else: install(a.action)

if __name__=='__main__':
    main()
