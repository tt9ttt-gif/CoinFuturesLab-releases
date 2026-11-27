"""Exercise local release publication and installer against an isolated monitor."""
import base64
import io
import json
import os
from pathlib import Path
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import time
import zipfile
sys.path.insert(0,str(Path('source').resolve()))
import autoupdate as a

FIXTURE="""
import socket,time,sys
from pathlib import Path
class Live:
    def tick(self,**kwargs): return {'market':[{},{}]}
live=Live()
ORIGINAL_API=None
def credentials(): return None,None
def tg(*args): raise AssertionError('fixture has no Telegram credentials')
if __name__=='__main__':
    if FAIL_START: sys.exit(17)
    p=Path('desktop-data/stop')
    with socket.socket() as s:
        s.bind(('127.0.0.1',47818))
        s.listen(1)
        p.unlink(missing_ok=True)
        while not p.exists(): time.sleep(.05)
"""
with tempfile.TemporaryDirectory() as d:
    root=Path(d)/'installed'
    root.mkdir()
    src=Path(d)/'release-source';src.mkdir()
    drop=Path(d)/'drop'
    key=Path(d)/'publisher.key'
    shutil.copy2(a.ROOT/'autoupdate.py',root/'autoupdate.py')
    a.ROOT=root;a.DATA=root/'desktop-data';a.DATA.mkdir()
    a.CONF=a.DATA/'update-config.json';a.STATE=a.DATA/'update-state.json';a.VERSION=root/'version.json'
    a.write(a.VERSION,{'version':'18.3'})
    for name in a.FILES:
        target=root/name
        target.parent.mkdir(parents=True,exist_ok=True)
        target.write_text(FIXTURE.replace('FAIL_START','False') if name=='desktop.py' else '')
        to=src/name
        to.parent.mkdir(parents=True,exist_ok=True)
        shutil.copy2(target,to)
    public=subprocess.check_output([sys.executable,str(root/'autoupdate.py'),'keygen','--key',str(key)],text=True).strip()
    a.write(a.CONF,dict(source='folder',path=str(drop),key=public,auto=False))
    database=a.DATA/'desktop.sqlite3'
    with sqlite3.connect(database) as db:
        db.execute('create table positions (id int)')
        db.execute('insert into positions values (42)')
    def publish(ver):
        subprocess.run([sys.executable,str(root/'autoupdate.py'),'publish','--key',str(key),'--source',str(src),'--output',str(drop),'--version',ver],check=True)
    def install():
        manifest,blob=a.get_release()
        (a.DATA/'update-download.zip').write_bytes(blob)
        subprocess.run([sys.executable,str(root/'autoupdate.py'),'install'],check=True)
        return manifest
    first=a.monitor()
    a.ready(first)
    try:
        publish('18.4')
        assert '18.4' in a.check() and a.current()=='18.3'
        pristine=(drop/a.ASSET).read_bytes()
        rebuilt=io.BytesIO()
        with zipfile.ZipFile(io.BytesIO(pristine)) as source,zipfile.ZipFile(rebuilt,'w') as result:
            for name in source.namelist():
                result.writestr(name,source.read(name)+(b'\n# altered' if name=='desktop.py' else b''))
        try: a.verify(rebuilt.getvalue(),public)
        except ValueError as e: assert 'digest' in str(e)
        else: raise AssertionError('tamper accepted')
        try: a.verify(pristine,base64.b64encode(bytes(32)).decode())
        except Exception as e: assert type(e).__name__=='InvalidSignature'
        else: raise AssertionError('wrong signing key accepted')
        assert install()['version']=='18.4'
        assert a.current()=='18.4' and a.guard_probe()
        assert a.read(a.STATE)['phase']=='success'
        assert (Path(a.read(a.STATE)['previous'])/'desktop-data/desktop.sqlite3').exists()
        # Probe succeeds, but the actual replacement monitor exits immediately.
        (src/'desktop.py').write_text(FIXTURE.replace('FAIL_START','True'))
        publish('18.5')
        install()
        assert a.current()=='18.4' and a.read(a.STATE)['phase']=='rolled_back' and a.guard_probe()
        assert a.read(a.STATE)['rejected']=='18.5'
        subprocess.run([sys.executable,str(root/'autoupdate.py'),'rollback'],check=True)
        assert a.current()=='18.3' and a.guard_probe()
        with sqlite3.connect(database) as db: assert db.execute('select id from positions').fetchone()==(42,)
    finally:
        lock=a.stop_market()
        lock.close()
        first.wait(timeout=10)
print('Signed detection/download, tamper rejection, backup, separate install, monitor restart, startup rollback and DB preservation passed.')
