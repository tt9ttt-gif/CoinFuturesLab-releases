"""Validate a release plan and publish only after a round-trip check."""
import argparse
import base64
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import tempfile

sys.path.insert(0, str(Path("source").resolve()))
import autoupdate as updater
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

ROOT = Path.cwd()

def gh(*args):
    return subprocess.check_output(["gh", *args], text=True).strip()

def version_for(plan):
    base = updater.version(plan["base"])
    if "version" in plan:
        requested = updater.version(plan["version"])
        if plan["version"] != ".".join(map(str, requested)) or requested <= base:
            raise ValueError("explicit release version must be canonical and newer than the base")
        return plan["version"]
    index = {"major":0, "minor":1, "patch":2}[plan["bump"]]
    return ".".join(str(x+1 if i==index else 0 if i>index else x) for i,x in enumerate(base))

def build(work, key, plan, version):
    path=work/"key"
    path.write_bytes(key.private_bytes(serialization.Encoding.Raw, serialization.PrivateFormat.Raw,
                                      serialization.NoEncryption()))
    path.chmod(0o600)
    try:
        subprocess.run([sys.executable,"source/autoupdate.py","publish","--key",str(path),"--source","source",
                        "--output",str(work),"--version",version,"--notes",plan["notes"]], check=True)
    finally:
        path.unlink(missing_ok=True)
    public=base64.b64encode(key.public_key().public_bytes(serialization.Encoding.Raw,
                                                        serialization.PublicFormat.Raw)).decode()
    updater.VERSION=work/"installed-version.json"
    updater.VERSION.write_text(json.dumps({"version":"18.3"}), encoding="utf-8")
    blob=(work/updater.ASSET).read_bytes()
    manifest,files=updater.verify(blob,public)
    if manifest["version"]!=version or manifest["db_schema"]!="unchanged": raise ValueError("incompatible manifest")
    for name,data in files.items():
        if data!=(ROOT/"source"/name).read_bytes(): raise ValueError("package source mismatch")
    return blob,public

def validate_tree():
    tracked=subprocess.check_output(["git","ls-files","-z"]).decode().split("\0")
    for path in filter(None,tracked):
        name=Path(path).name.lower()
        if (name.endswith((".sqlite",".sqlite3",".db",".key",".pem",".pfx")) or name.startswith(".env")
            or re.search(r"(^|/)(desktop-data|results|upgrade-backups)(/|$)",path) or "telegram.local" in name):
            raise ValueError("private data path in repository: "+path)
    for name in updater.FILES:
        p=ROOT/"source"/name
        if p.is_symlink() or not p.is_file(): raise ValueError("invalid source path: "+name)
        if name.endswith(".py"): compile(p.read_bytes(),name,"exec")
        if re.search(rb'\b[0-9]{6,12}:[A-Za-z0-9_-]{32,}\b',p.read_bytes()):
            raise ValueError("possible Telegram credential in "+name)

def main():
    parser=argparse.ArgumentParser()
    parser.add_argument("--publish",action="store_true")
    args=parser.parse_args()
    validate_tree()
    plan=json.loads(Path("release-plan.json").read_text(encoding="utf-8"))
    version=version_for(plan)
    if plan["channel"] not in ("rehearsal","production"): raise ValueError("invalid channel")
    if not isinstance(plan["notes"],str) or not 0<len(plan["notes"])<=2000: raise ValueError("invalid notes")
    with tempfile.TemporaryDirectory() as temp:
        work=Path(temp)
        raw=os.environ.get("PRIVATE_KEY_B64") if args.publish else None
        key=(Ed25519PrivateKey.from_private_bytes(base64.b64decode(raw,validate=True))
             if args.publish else Ed25519PrivateKey.generate())
        blob,public=build(work,key,plan,version)
        if not args.publish:
            try: updater.verify(blob,base64.b64encode(bytes(32)).decode())
            except Exception as exc:
                if type(exc).__name__!="InvalidSignature": raise
            else: raise ValueError("wrong key was accepted")
            print("Validation passed for",version,plan["channel"])
            return
        repo=os.environ["GITHUB_REPOSITORY"]
        commit=os.environ["GITHUB_SHA"]
        latest=gh("api",f"repos/{repo}/releases/latest","--jq",".tag_name")
        if latest!=f"v{plan['base']}": raise ValueError("release base changed")
        rehearsal=plan["channel"]=="rehearsal"
        tag=f"v{version}-rehearsal" if rehearsal else f"v{version}"
        code=subprocess.run(["gh","release","view",tag,"--repo",repo],capture_output=True)
        if code.returncode==0:
            raise ValueError("release tag already exists")
        advertised=gh("api",f"repos/{repo}/releases/latest","--jq",".tag_name")
        if advertised!=latest: raise ValueError("release changed during validation")
        # Verify the operating signer against the previous public asset before publishing.
        previous=work/"previous"; previous.mkdir()
        gh("release","download",latest,"--repo",repo,"--pattern",updater.ASSET,"--dir",str(previous))
        updater.verify((previous/updater.ASSET).read_bytes(),public)
        gh("release","create",tag,str(work/updater.ASSET),"--repo",repo,"--target",commit,
           "--draft","--title",f"CoinFuturesLab {version}"+(" rehearsal" if rehearsal else ""),"--notes",plan["notes"],
           "--latest=false",*(["--prerelease"] if rehearsal else []))
        fetched=work/"fetched"; fetched.mkdir()
        gh("release","download",tag,"--repo",repo,"--pattern",updater.ASSET,"--dir",str(fetched))
        downloaded=(fetched/updater.ASSET).read_bytes()
        updater.verify(downloaded,public)
        if hashlib.sha256(downloaded).digest()!=hashlib.sha256(blob).digest(): raise ValueError("uploaded bytes mismatch")
        gh("release","edit",tag,"--repo",repo,"--draft=false","--latest="+("false" if rehearsal else "true"))
        print("Published verified asset",tag,hashlib.sha256(blob).hexdigest())

if __name__=="__main__":
    main()
