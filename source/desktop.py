"""Windows monitor. Exchange traffic is exclusively public market data."""
import argparse
import asyncio
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone, timedelta
import getpass
import json
import logging
import math
from logging.handlers import RotatingFileHandler
import os
from pathlib import Path
from queue import Queue, Full, Empty
import re
import secrets
import sqlite3
import sys
import threading
import time
from urllib.request import Request, urlopen
from urllib.error import HTTPError

import autoupdate
from message_format import RULE, timestamp, payload, reason, notice

ROOT=Path(__file__).resolve().parent
sys.path.insert(0,str(ROOT/'v17'))
import live
from growth import profiles, ACCOUNTS
from websockets.asyncio.client import connect

DATA=ROOT/'desktop-data'
DATA.mkdir(exist_ok=True)
DB=DATA/'desktop.sqlite3'
STOP=DATA/'stop'
LIVE=live.OUT/'latest.json'
LOG=logging.getLogger('v18')
BASE='wss://fstream.binance.com'
STREAMS=[f'{s.lower()}@{stream}' for s in live.SYMS for stream in ('aggTrade','markPrice@1s','kline_1m','kline_5m','kline_15m')]
KST=timezone(timedelta(hours=9))

def db():
    conn=sqlite3.connect(DB,timeout=15)
    conn.execute('CREATE TABLE IF NOT EXISTS kv (key TEXT PRIMARY KEY,value TEXT)')
    conn.execute('CREATE TABLE IF NOT EXISTS alerts (key TEXT PRIMARY KEY, signature TEXT, queued REAL)')
    conn.execute('CREATE TABLE IF NOT EXISTS outbox (id INTEGER PRIMARY KEY, body TEXT, detected REAL, analyzed REAL, sent REAL)')
    if 'category' not in {r[1] for r in conn.execute('PRAGMA table_info(outbox)')}:
        try: conn.execute("ALTER TABLE outbox ADD COLUMN category TEXT NOT NULL DEFAULT 'legacy'")
        except sqlite3.OperationalError:
            if 'category' not in {r[1] for r in conn.execute('PRAGMA table_info(outbox)')}: raise
    if 'suppressed' not in {r[1] for r in conn.execute('PRAGMA table_info(outbox)')}:
        try: conn.execute("ALTER TABLE outbox ADD COLUMN suppressed INTEGER NOT NULL DEFAULT 0")
        except sqlite3.OperationalError:
            if 'suppressed' not in {r[1] for r in conn.execute('PRAGMA table_info(outbox)')}: raise
    return conn

def get(key,default=None):
    with db() as c:
        row=c.execute('SELECT value FROM kv WHERE key=?',(key,)).fetchone()
        return json.loads(row[0]) if row else default

def put(key,value):
    with db() as c:
        c.execute('INSERT OR REPLACE INTO kv VALUES (?,?)',(key,json.dumps(value,ensure_ascii=False)))

def tg(token,method,params):
    req=Request('https://api.telegram.org/bot'+token+'/'+method,data=json.dumps(params).encode(),headers={'Content-Type':'application/json'})
    with urlopen(req,timeout=38) as r:
        x=json.load(r)
    if not x.get('ok'): raise RuntimeError('Telegram rejected request')
    return x['result']

def credentials():
    token=os.getenv('TELEGRAM_BOT_TOKEN')
    if not token:
        try:
            import keyring
            token=keyring.get_password('CoinFuturesLab','Telegram')
        except Exception:
            pass
    return token,os.getenv('TELEGRAM_CHAT_ID') or get('chat')

def alert_settings():
    return get('alert_settings',dict(mode='important' if get('alerts',True) else 'off',paper=False,summary=False))

def category(key):
    if key.startswith('user/') or key.startswith('health/'): return 'important'
    if key.startswith('paper/manage/'): return 'paper_manage'
    if key.startswith('paper/'): return 'paper'
    if key.startswith('summary/'): return 'summary'
    if key.startswith('idea/'): return 'important'
    return 'market'

def permitted(kind):
    cfg=alert_settings()
    return cfg['mode']!='off' and (kind=='important' or
        (kind=='market' and cfg['mode']=='all') or
        (kind in ('paper','paper_manage','summary') and cfg.get(kind,False)))

def notify(key,signature,text,detected=None,cooldown=0,force=False):
    kind=category(key)
    if not permitted(kind):
        return
    now=time.time()
    with db() as c:
        c.execute('BEGIN IMMEDIATE')
        row=c.execute('SELECT signature,queued FROM alerts WHERE key=?',(key,)).fetchone()
        if row and (row[0]==signature or (not force and now-row[1]<cooldown)):
            return
        c.execute('INSERT OR REPLACE INTO alerts VALUES (?,?,?)',(key,signature,now))
        c.execute('INSERT INTO outbox(body,detected,analyzed,category) VALUES (?,?,?,?)',(text,detected or now,now,kind))
    print(text,flush=True)

def health(channel,problem,now=None):
    now=time.time() if now is None else now
    key='health-state/'+channel
    old=get(key,{})
    if problem:
        LOG.warning('function=health channel=%s detail=%s',channel,problem)
        since=old.get('since',now)
        sent=old.get('sent')
        if now-since>=300 and (sent is None or now-sent>=1800) and permitted('important'):
            notify('health/'+channel,str(now),f'{channel} 분석 장애 지속 · {int((now-since)//60)}분',since)
            sent=now
        put(key,dict(since=since,sent=sent))
    elif old:
        if old.get('sent') is not None: notify('health/'+channel,'recovered/'+str(old['since']),f'{channel} 정상 복구')
        put(key,{})

def alert_command(command):
    cfg=alert_settings()
    args=command.split()[1:]
    if args in (['important'],['on'],['all'],['off']):
        cfg['mode']='important' if args==['on'] else args[0]
    elif len(args)==2 and args[0] in ('paper','paper_manage','summary') and args[1] in ('on','off'):
        cfg[args[0]]=args[1]=='on'
    elif args not in ([],['status']):
        return '/alerts important|all|off|on|status · /alerts paper on|off · /alerts paper_manage on|off · /alerts summary on|off'
    put('alert_settings',cfg)
    return f"알림 {cfg['mode']} · 모의 {'ON' if cfg['paper'] else 'OFF'} · 모의 관리 {'ON' if cfg.get('paper_manage',False) else 'OFF'} · 요약 {'ON' if cfg['summary'] else 'OFF'}"

def sender(stop,token,chat):
    delay=2
    while not stop.wait(1):
        with db() as c:
            row=c.execute('SELECT id,body,detected,analyzed,category FROM outbox WHERE sent IS NULL AND suppressed=0 ORDER BY id LIMIT 1').fetchone()
        if not row: continue
        ident,body,detected,analyzed,kind=row
        if kind=='legacy' or not permitted(kind) or (kind=='market' and time.time()-detected>120):
            with db() as c: c.execute('UPDATE outbox SET suppressed=1 WHERE id=?',(ident,))
            continue
        try:
            tg(token,'sendMessage',payload(chat,notice(body)))
            sent=time.time()
            with db() as c: c.execute('UPDATE outbox SET sent=? WHERE id=?',(sent,ident))
            LOG.info('alert=%s detected=%.3f analyzed=%.3f sent=%.3f latency=%.3f',ident,detected,analyzed,sent,sent-detected)
            delay=2
        except Exception as ex:
            LOG.warning('Telegram send failed: %s',type(ex).__name__)
            retry=int(ex.headers.get('Retry-After',delay)) if isinstance(ex,HTTPError) else delay
            stop.wait(max(retry,delay))
            delay=min(delay*2,120)

def parse(text,prior):
    msg=text.lower()
    out=dict(prior)
    targets=re.findall(r'btc(?:usdt)?|비트코인|eth(?:usdt)?|이더리움',msg)
    if targets:
        sym='BTCUSDT' if targets[0].startswith(('btc','비트')) else 'ETHUSDT'
        if sym!=out.get('symbol'): out={}
        out['symbol']=sym
    if ('롱' in msg or 'long' in msg) != ('숏' in msg or 'short' in msg):
        out['side']=1 if '롱' in msg or 'long' in msg else -1
    for key,pattern in [('leverage',r'([0-9]+)\s*배'),('entry',r'(?:진입가\s*([\d,.]+)|([\d,.]+)\s*에\s*진입)'),
                        ('target',r'익절가\s*([\d,.]+)'),('stop',r'손절가\s*([\d,.]+)')]:
        matched=re.search(pattern,msg)
        if matched: out[key]=float(next(x for x in matched.groups() if x is not None).replace(',',''))
    m=re.search(r'증거금\s*([\d,.]+)\s*(만)?',msg)
    if m: out['margin']=float(m[1].replace(',',''))*(10000 if m[2] else 1)
    return out

def fmt(value,d=2):
    return '자료 없음' if value is None else f'{value:,.{d}f}'

def margin_choices(plan,seed,positions,funding=None,requested_leverage=None):
    """Existing standard caps, with an additional funding and exposure reserve."""
    risk=0.; reserved=0.
    for held in positions:
        margin=held['margin']; lev=held['leverage']; entry=held['entry']
        reserved+=margin
        stop=held.get('stop')
        distance=max(0.,held['side']*(entry-stop)/entry) if stop is not None else 1/lev
        risk+=margin*lev*(distance+2*live.FEE+live.SLIP+.001)
    charge=max(.0001,abs(funding or 0.))
    adjusted=dict(plan,stop_loss_notional=plan['stop_loss_notional']+charge)
    candidates=profiles(adjusted,seed,max(0.,seed*.01-risk),max(0.,seed*.45-reserved),.01,'standard')['alternatives']
    if requested_leverage is not None:
        candidates=[r for r in candidates if r['leverage']==requested_leverage]
    choices=[]
    for row in candidates:
        lev=row['leverage']
        stress=row['stress_loss_ratio']+(.0005 if lev>=20 else 0.)
        cap=min(row['notional_krw'],max(0.,seed*.01-risk)/stress)/lev
        cap=math.floor(cap/100000)*100000
        if row['stop_to_liquidation_ratio'] < (3 if lev>=20 else 2)*max(plan['atr_ratio'],.001):
            cap=0
        choices.append(dict(row,cap=cap,loss_ratio=adjusted['stop_loss_notional']))
    viable=[r for r in choices if r['cap']>=100000]
    picked=max(viable,key=lambda r:(r['cap']>=1000000,r['cap']*r['leverage'],-r['leverage'])) if viable else None
    return picked,choices


def signal_context(m,c):
    direction=m.get('market_direction','불확실')
    f=m['indicators']
    side=c['side']
    evidence=set()
    if side*(f['f15m']['flow']+f['f5m']['flow'])>.10: evidence.add('flow')
    if side*f['f1h']['ema']>.001: evidence.add('structure')
    if f['f15m']['volume']>=1.25 and side*f['f15m']['r6']>0: evidence.add('volume')
    if m.get('oi_change') is not None and m['oi_change']>.002 and side*f['f15m']['r6']>0: evidence.add('oi')
    aligned=direction==('상승' if side==1 else '하락')
    neutral=direction=='횡보' and c['confirmed'] and len(evidence)>=2
    return (aligned or neutral) and len(evidence)>=2,evidence


def position_edit(parts):
    with db() as conn:
        conn.execute('BEGIN IMMEDIATE')
        row=conn.execute("SELECT value FROM kv WHERE key='positions'").fetchone()
        active=json.loads(row[0]) if row else []
        history=conn.execute("SELECT value FROM kv WHERE key='position_history'").fetchone()
        history=json.loads(history[0]) if history else []
        ident=int(parts[2])
        p=next((x for x in active if x['id']==ident),None)
        if p is None: return '등록 번호를 확인하세요.'
        previous=dict(p)
        if parts[1]=='add' and len(parts)==5:
            price,margin=map(float,parts[3:])
            if not all(math.isfinite(x) and x>0 for x in (price,margin)): return '가격·증거금은 양수여야 합니다.'
            p['entry']=(p['margin']+margin)/(p['margin']/p['entry']+margin/price)
            p['margin']+=margin
        elif parts[1] in ('reduce','close'):
            fraction=1. if parts[1]=='close' else float(parts[3])/100 if len(parts)==4 else 0.
            if not 0<fraction<=1: return '청산 비율은 0 초과 100 이하입니다.'
            p['margin']*=1-fraction
            if fraction==1: active.remove(p)
        else: return '/pos add 번호 가격 증거금원 · /pos reduce 번호 비율 · /pos close 번호'
        history.append(dict(at=time.time(),operation=parts[1],before=previous,after=dict(p)))
        conn.executemany('INSERT OR REPLACE INTO kv VALUES (?,?)',
                         [('positions',json.dumps(active,ensure_ascii=False)),('position_history',json.dumps(history,ensure_ascii=False))])
    return '실제 등록 포지션 기록을 반영했습니다.'


def position_adjustment(p,m,c):
    current=m['price']
    side=p['side']
    threshold=max(.003, c['atr_ratio'])*current
    old_stop=p.get('stop'); old_target=p.get('target')
    stop=c['stop']; target=c['target']
    improve=(old_stop is not None and side*(stop-old_stop)>=threshold and side*(current-stop)>0)
    closer=(old_target is not None and target is not None and side*(old_target-target)>=threshold and side*(target-current)>0)
    if not improve and not closer: return None
    return stop if improve else old_stop,target if closer else old_target


def review(m,context,seed,stamp=None,event=None):
    c=next((x for x in m['directions'] if x['side']==context.get('side')),m['candidate'])
    side=c['side']
    entry=context.get('entry',c['entry']); stop=context.get('stop',c['stop']); target=context.get('target',c['target'])
    if entry<=0 or stop<=0 or side*(entry-stop)<=0 or (target and side*(target-entry)<=0):
        return f"{m['symbol']} 요청 가격 구조 부적합. 현재 {fmt(m['price'])}, 관측 지지 {fmt(m['support'])}, 저항 {fmt(m['resistance'])}."
    loss=abs(stop/entry-1)+2*live.FEE+live.SLIP
    gain=side*(target/entry-1)-2*live.FEE if target else None
    plan=dict(c,entry=entry,stop=stop,target=target,stop_loss_notional=loss,net_target_return_notional=gain)
    pr,alternatives=margin_choices(plan,seed,get('positions',[]),m.get('funding'),context.get('leverage'))
    lev=context.get('leverage',pr['leverage'] if pr else 0)
    margin=context.get('margin',pr['cap'] if pr else 0)
    mode=get('format','normal')
    label='롱' if side==1 else '숏'
    sym=m['symbol'].removesuffix('USDT')
    status=c['entry_status']
    title=('조건부 진입 후보' if status=='conditional' else '즉시 검토 후보' if status=='entry' else '관찰 후보')
    icon='🟢' if side==1 else '🔴'
    rows=[RULE,f'{icon} {sym} {label} {title}']
    if event: rows.append(f'• {event}')
    if stamp: rows.append('📅 '+timestamp(stamp)+' · 완료 봉')
    rows.extend([f"📊 {c['grade']} | 신뢰도 {c['confidence']}",f"현재가 {fmt(m['price'])} USDT · 상위 방향 {m.get('market_direction','불확실')}",'',RULE,
                 '🎯 추천 매매 설정' if not context.keys()-{'side'} else '🎯 매매 설정',
                 f'• 진입가: {fmt(entry)}',
                 f'• 레버리지: {lev:g}배' if lev else '• 레버리지: 부적합',
                 f'• 증거금: {fmt(margin,0)}원' if margin else '• 증거금: 자료 없음',
                 f'🟢 익절가: {fmt(target)}',f'🔴 손절가: {fmt(stop)}'])
    valid=1<=lev<=125 and 0<margin<=seed
    risk='증거금·배수 또는 위험 여유를 확인하세요.'
    ratio=lambda x: f'{x:.2f} : 1' if x is not None else '자료 없음'
    if valid:
        liq=entry*(1-side*(1/lev-live.FEE))/(1-side*.005)
        safe=side*(stop-liq)/entry>=2*max(c['atr_ratio'],.001)
        rows.append(f'⚠️ 예상 청산가: {fmt(liq)}')
        risk='청산 여유 또는 일반 손실 예산 초과. 증거금 축소·배수 재검토.' if not safe or margin*lev*loss>seed*.01 else '갭과 펀딩에 따라 모형 손익과 달라질 수 있습니다.'
    else:
        rows.append('⚠️ 예상 청산가: 계산 불가')
    def money(r):
        return f'{margin*lev*r:+,.0f}원 ({lev*r:+.2%})' if valid and r is not None else '계산 불가'
    rr=gain/loss if gain is not None else None
    rows.extend(['','💰 예상 손익 · 증거금 기준',f'• 익절 시: {money(gain)}',f'• 손절 시: {money(-loss)}',
                 f'• 손익비: {ratio(rr)}','',RULE,f"{'📈' if side==1 else '📉'} {label} 진입 근거"])
    def evidence(items):
        n=1 if mode=='simple' else len(items) if mode=='detailed' else 3
        return ['• '+reason(a,m) for a,_ in items[:n]] or ['• 뚜렷한 근거 없음']
    rows.extend(evidence(c['reasons']))
    rows.extend(['','⚠️ 반대 근거 및 위험',*evidence(c['conflicts']),'• '+risk,'','⏳ 진입 조건',
                 c['trigger'] or '현재 완료 봉에서 직접 검토할 후보',
                 '', '❌ 추천 무효화 조건',c['invalidation']])
    if not pr or pr['cap']<1_000_000:
        rows.append('• 100만 원 미만: 기존 증거금 상한, 등록 포지션 위험, 손절 예산과 청산 여유를 모두 적용.')
    if context.get('margin') and not any(r['leverage']==lev and r['cap']>=margin for r in alternatives):
        rows.append('• 입력한 증거금은 자동 추천 위험 범위를 넘는 가정입니다.')
    if mode=='detailed' and alternatives:
        rows.append('📊 증거금 비교 · 비용 및 펀딩 1회 스트레스 반영')
        for amount in (500000,1000000,1500000):
            row=max(alternatives,key=lambda r:(r['cap']>=amount,r['cap'],-r['leverage']))
            l=row['leverage']; notional=amount*l; loss_krw=notional*row['loss_ratio']
            rows.append(f"• {fmt(amount,0)}원 / {l}배 · 명목 {fmt(notional,0)}원 · 익절 {fmt(notional*(gain-max(.0001,abs(m.get('funding') or 0.))) if gain is not None else None,0)}원 · 손절 -{fmt(loss_krw,0)}원 ({loss_krw/seed:.2%}) · 청산 {fmt(row['model_liquidation'])} · 스탑 여유 {row['stop_to_liquidation_ratio']:.2%} · "+('가능' if row['cap']>=amount else '위험·증거금 한도 초과'))
    imm=c['immediate_plan']
    if mode!='simple':
        h=c['historical']; n=h.get('sample',0); hits=h.get('target_hits',0)
        rows.extend(['',RULE,'📚 과거 유사 사례',
            f'• 목표 선도달: {hits}/{n}건 ({hits/n:.1%})' if n else '• 비교 표본 없음',
            f'• 손익분기 승률: {loss/(loss+gain):.1%}' if gain is not None and gain>0 else '• 손익분기 승률: 계산 불가',
            '• 과거 관측치이며 미래 승률과 다릅니다.'+(' 표본이 적어 불확실합니다.' if n<50 else ''),
            f"• 현재가 진입안: {fmt(imm['entry'])} · 손익비 {ratio(imm['payoff'])}"])
        if mode=='detailed':
            opposite=next(x for x in m['directions'] if x['side']!=side)
            rows.extend(['', '⚠️ 반대 방향의 근거',*evidence(opposite['reasons']),
                         f"• 일반 대안: {str(pr['leverage'])+'배 / '+fmt(pr['cap'],0)+'원' if pr else '부적합'}",
                         f"• 시장 상태: {m['regime']} · 판단 점수: {c['score']:.2f}"])
    favored='롱' if m['candidate']['side']==1 else '숏'
    view={'conditional':'조건을 확인한 뒤 진입을 검토합니다.',
          'entry':'현재 완료 봉 기준으로 진입 계획을 검토할 수 있습니다.',
          'watch':'지금은 관찰하며 진입 근거를 다시 확인합니다.'}.get(status,'진입 자료를 확인합니다.')
    rows.extend(['','💬 종합 판단',f'상대 우세 {favored}. {view}',RULE])
    return '\n'.join(rows)

HELP='/btc /eth /long eth /short btc /market /positions /paper /seed [원] /status /alerts [important|all|off|on|status] /alerts paper on|off /alerts summary on|off /format simple|normal|detailed /version /update check /help\n예: ETH 30배 롱 / 그럼 숏은? / 진입가 1730 / 증거금 20만 원\n등록: ETH 숏 진입했어. 진입가 1730, 30배, 증거금 20만 원.\n삭제: /close 번호 · 수동 모의 요청: /simulate eth short / 기존 등록에 TP/SL 지정: /levels 번호 익절가 1700 손절가 1750'

def latest():
    return json.loads(LIVE.read_text(encoding='utf-8')) if LIVE.exists() else {}

def handle(message,identity='cmd'):
    low=message.strip().lower()
    if low in ('/version','/update') or low.startswith('/update '): return autoupdate.command(low)
    context=parse(message,get('context/'+identity,{}))
    seed=get('seed',3_000_000)
    if low.startswith('/seed'):
        fields=low.replace(',','').split()
        if len(fields)>1:
            value=int(fields[1])
            if not 1<=value<=10**12: return '시드는 양의 원화 정수로 입력하세요.'
            put('seed',value)
            seed=value
        return f'수동 분석 시드 {seed:,}원. 모의계좌 이력과 시작 시드는 유지됩니다.'
    if low.startswith('/alerts'): return alert_command(low)
    if low=='/format' or low.startswith('/format '):
        args=low.split()
        if len(args)==1: return '현재 상세도: '+get('format','normal')
        if len(args)==2 and args[1] in ('simple','normal','detailed'):
            put('format',args[1])
            return '메시지 상세도: '+args[1]
        return '/format simple | /format normal | /format detailed'
    if low=='/help': return HELP+'\n/pos add 번호 가격 증거금원 · /pos reduce 번호 비율 · /pos close 번호'
    if low=='/status': return json.dumps(get('status',{}),ensure_ascii=False)
    if low=='/paper':
        accounts=latest().get('paper',{}).get('accounts',{})
        if not accounts: return '📊 모의계좌 성과\n모의 기록 없음'
        return '📊 모의계좌 성과\n\n'+'\n\n'.join(
            f"💰 {name}\n• 평가액: {fmt(a['equity_krw'],0)}원\n• 대기 {a['counts'].get('pending',0)}건 / 보유 {a['counts'].get('open',0)}건 / 청산 {a['counts'].get('closed',0)}건"
            for name,a in accounts.items())

    positions=get('positions',[])
    if low.startswith('/pos ') and len(low.split())>=3: return position_edit(low.split())
    if low.startswith('/close '): return position_edit(['/pos','close',low.split()[1]])
    if low.startswith('/levels '):
        ident=int(low.split()[1])
        found=next((p for p in positions if p['id']==ident),None)
        if not found: return '등록 번호를 확인하세요.'
        found.update({k:v for k,v in parse(low,{}).items() if k in ('target','stop')})
        put('positions',positions);return '등록 포지션 감시 가격을 갱신했습니다.'
    report=live.tick(read_only=True,market_api=ORIGINAL_API)
    end=datetime.fromisoformat(report['completed_through_utc'])
    if abs((datetime.now(timezone.utc)-end).total_seconds())>150:
        return '최신 완료 봉 시각을 검증하지 못해 분석을 보류합니다.'
    markets={m['symbol']:m for m in report['market']}
    if low=='/positions' or '보유 중' in low or '가지고 있어' in low:
        rows=[]
        for p in positions:
            m=markets.get(p['symbol'])
            if not m: continue
            price=m.get('mark_price') or m['price']
            pnl=p['margin']*p['leverage']*(p['side']*(price/p['entry']-1)-live.FEE*(1+price/p['entry']))
            rows.append(f"#{p['id']} 펀딩 제외 추정 {fmt(pnl,0)}원\n"+review(m,p,seed,report['completed_through_utc']))
        return '\n\n'.join(rows) or '등록 포지션이 없거나 최신 데이터가 비었습니다.'
    if '진입했' in low or low.startswith('/register'):
        if any(k not in context for k in ('symbol','side','entry','leverage','margin')): return '종목·방향·진입가·배수·증거금을 입력하세요.'
        if min(context['entry'],context['margin'])<=0 or not 1<=context['leverage']<=125: return '진입가, 증거금, 배수를 확인하세요.'
        m=markets.get(context['symbol'])
        if m:
            candidate=next(c for c in m['directions'] if c['side']==context['side'])
            for key in ('target','stop'): context.setdefault(key,candidate[key])
        position=dict(context,id=secrets.randbelow(900000)+100000)
        put('positions',positions+[position])
        return f"#{position['id']} 등록 완료. 거래소 체결은 확인하지 않았습니다."
    put('context/'+identity,context)
    if low.startswith('/simulate'):
        m=markets.get(context.get('symbol'))
        if not m or context.get('side') not in (-1,1): return '예: /simulate eth short'
        if any(k in context for k in ('entry','margin','leverage','stop','target')):
            return '수동 모의 주문은 엔진 기본 가격과 위험 설정을 사용합니다. /simulate eth short default 로 명시적으로 요청하세요.' if ' default' not in low else queue_manual(m,context['side'])
        return queue_manual(m,context['side'])
    query=markets.values() if low=='/market' or '가장' in low or not context.get('symbol') else [markets[context['symbol']]] if context['symbol'] in markets else []
    note='\n익절가를 높일 경우 비교할 가격을 입력하세요. 예: 익절가 1750' if '익절가를 더' in low else ''
    if '아까' in low or '아직 유효' in low:
        note+='\n이전 제안과 비교할 때는 기존 진입가·손절가를 입력하세요. 현재 양방향 근거와 확인 조건을 다시 계산했습니다.'
    return ('\n\n'.join(review(m,context if len(query)==1 else {},seed,report['completed_through_utc']) for m in query) or '최신 분봉을 확보하지 못했습니다.')+note

def queue_manual(m,side):
    ident=secrets.token_hex(6)
    put('manual_request',dict(id=ident,symbol=m['symbol'],side=side,expires=int(time.time()*1000)+180000))
    return f'수동 모의 요청 {ident} 접수. 다음 완료 봉에서 위험·손익비를 검증하며 체결 보장은 없습니다.'

def respond(message,identity,reply):
    try: reply(handle(message,identity))
    except Exception as ex:
        LOG.warning('analysis failed: %s',type(ex).__name__)
        reply('분석 실패. 입력 형식과 데이터 연결을 확인하세요.')

def receive(stop,token,chat):
    offset=get('telegram_offset',0)
    while not stop.is_set():
        try:
            updates=tg(token,'getUpdates',dict(offset=offset,timeout=20,allowed_updates=['message']))
            for u in updates:
                offset=u['update_id']+1
                put('telegram_offset',offset)
                msg=u.get('message',{})
                if str(msg.get('chat',{}).get('id'))!=str(chat) or msg.get('chat',{}).get('type')!='private': continue
                if time.time()-msg.get('date',0)>180 or not msg.get('text'): continue
                respond(msg['text'],'telegram/'+str(chat),lambda text: tg(token,'sendMessage',payload(chat,text)))
        except Exception as ex:
            LOG.warning('Telegram receive/reply failed: %s',type(ex).__name__)
            stop.wait(8)

def backup():
    src=live.OUT/'paper.sqlite3'
    if not src.exists(): return
    target=DATA/('paper-'+datetime.now(KST).strftime('%Y-%m-%d')+'.sqlite3')
    tmp=target.with_suffix('.tmp')
    with sqlite3.connect(src) as a,sqlite3.connect(tmp) as b: a.backup(b)
    tmp.replace(target)
    for old in sorted(DATA.glob('paper-*.sqlite3'))[:-7]: old.unlink()

def publish(report,detected):
    stamp=report.get('completed_through_utc','')
    errors=report.get('unavailable',{})
    healthy={m['symbol'] for m in report.get('market',[])}
    for sym in live.SYMS:
        health(sym,None if sym in healthy else errors.get(sym,'최신 완료 봉 없음'))
    for m in report.get('market',[]):
        market_key='market-state/'+m['symbol']
        previous=get(market_key,{})
        values={k:m.get(k) for k in ('funding','oi_change')}
        for key,threshold in [('funding',.0001),('oi_change',.01)]:
            old,new=previous.get(key),values[key]
            if old is not None and new is not None and abs(new-old)>=threshold:
                notify(market_key+'/'+key,f'{new:.5f}',f"{m['symbol']} {key} {old:.3%} → {new:.3%} · 공개 REST",detected)
        put(market_key,values)
        for p in get('positions',[]):
            if p['symbol']!=m['symbol']: continue
            c=next(x for x in m['directions'] if x['side']==p['side'])
            adverse=m['candidate']['side']!=p['side'] and m['candidate']['score']>=1.65
            key=f"user/{p['id']}/thesis"
            if adverse:
                notify(key,'opposing',f"등록 #{p['id']} 반대 방향 근거 강화 · 보유 위험 재확인",detected)
                proposal=position_adjustment(p,m,c)
                if proposal:
                    stop,target=proposal
                    size=p['margin']*p['leverage']/p['entry']
                    delta=lambda new,old: f'{p["side"]*(new-old)*size:+,.0f}원' if new is not None and old is not None else '자료 없음'
                    oldstop=p.get('stop'); oldtarget=p.get('target')
                    notify(f"user/{p['id']}/adjust",f'{stop}/{target}',
                           f"🎯 등록 #{p['id']} 구조 조정 제안\n반대 방향 근거 강화. 실제 가격은 미변경.\n익절 {fmt(oldtarget)} → {fmt(target)} · 손익 변화 {delta(target,oldtarget)}\n손절 {fmt(oldstop)} → {fmt(stop)} · 손익 변화 {delta(stop,oldstop)}\n손절 위험 확대 없음 · 수수료·갭 제외",detected,cooldown=1800)
            else:
                with db() as conn: conn.execute('DELETE FROM alerts WHERE key=?',(key,))
        for c in m['directions']:
            key=f"idea/{m['symbol']}/{c['side']}"
            old=get(key,{})
            supported,evidence=signal_context(m,c)
            active=c['candidate_visible'] and supported
            structure=m.get('market_direction','불확실')
            current=dict(active=active,status=c['entry_status'],stop=c['stop'],
                         direction=structure,evidence=sorted(evidence))
            invalid=bool(old.get('active') and not active and old.get('stop') and
                         c['side']*(m['completed_1m_close']-old['stop'])<=0)
            confirmed=bool(active and c['entry_status']=='entry' and old.get('status')!='entry')
            shifted=bool(active and old.get('direction') and old['direction']!=structure and
                         structure in ('상승','하락') and len(evidence)>=2)
            significant=bool(active and old.get('active') and old.get('evidence')!=current['evidence'] and
                             len(evidence-set(old.get('evidence',[])))>=2)
            new=active and not old.get('active')
            if invalid:
                notify(key,'invalid/'+str(old['stop']),f"❌ {m['symbol']} 후보 무효화 · {timestamp(stamp)}",detected,force=True)
            elif confirmed or shifted or significant or new:
                label='진입 조건 충족' if confirmed else '상위 방향 변경' if shifted else '구조·수급 확인'
                signature=json.dumps([structure,c['entry_status'],current['evidence']])
                notify(key,signature,review(m,dict(side=c['side']),get('seed',3_000_000),stamp,label),
                       detected,cooldown=1800,force=bool(confirmed or shifted))
            put(key,current)
    grouped={}
    for e in report.get('paper',{}).get('events',[]):
        key=(e.get('symbol'),e.get('side'),e['kind'],e.get('entry',e.get('price')),e.get('at'),e.get('branch'))
        # Some legacy exits carry no symbol; group only by an actual shared price/time.
        if key[3] is None: key=(*key,e.get('id'))
        grouped.setdefault(key,[]).append(e)
    for batch in grouped.values():
        ids='/'.join(sorted(str(e.get('id')) for e in batch))
        e=batch[0]
        prefix='paper/manage' if e['kind'] in ('thesis_review','exit_revision') else 'paper'
        key=f"{prefix}/{e['kind']}/{ids}/{e.get('branch')}"
        notify(key,key,f"모의 {e['kind']} · {e.get('symbol','')} · "+', '.join(str(x.get('account')) for x in batch),detected)
    if healthy:
        hour=stamp[:13]
        notify('summary/hour',hour,stamp+' 요약 · '+', '.join(f"{m['symbol']} {fmt(m['price'])}" for m in report['market']),detected)

WS_BARS={}
WS_LOCK=threading.Lock()
ORIGINAL_API=live.api

def monitor_api(host,path,**query):
    if path!='/fapi/v1/klines' or 'startTime' in query or 'endTime' in query or query.get('interval') not in ('1m','5m','15m'):
        return ORIGINAL_API(host,path,**query)
    key=(query['symbol'],query['interval'])
    interval=int(key[1][:-1])*60_000
    end=(int(time.time()*1000)//interval)*interval
    with WS_LOCK:
        rows=list(WS_BARS.get(key,{}).values())
    rows=sorted((x for x in rows if x[0]<end),key=lambda x:x[0])[-query['limit']:]
    if len(rows)==query['limit'] and rows[-1][0]==end-interval and all(b[0]-a[0]==interval for a,b in zip(rows,rows[1:])):
        return rows
    result=ORIGINAL_API(host,path,**query)
    with WS_LOCK:
        merged={x[0]:x for x in result if x[6]<end}
        merged.update(WS_BARS.get(key,{}))
        WS_BARS[key]=dict(sorted(merged.items())[-240:])
    return [merged[k] for k in sorted(merged)[-query['limit']:]]

def overview(stop,requests):
    saved=None
    last_published=None
    last=None
    live.api=monitor_api
    while not stop.is_set():
        try: detected=requests.get(timeout=2)
        except Empty:
            if last is not None and time.time()-last<60: continue
            detected=time.time()
        if last is not None and time.time()-last<20: continue
        last=time.time()
        try:
            live.tick(quiet=True,manual_request=get('manual_request'))
            report=latest()
            when=datetime.fromisoformat(report['completed_through_utc']).timestamp()
            if abs(time.time()-when)<=150:
                if report['completed_through_utc']!=last_published:
                    publish(report,detected)
                    last_published=report['completed_through_utc']
                health('시장 분석',None)
                put('status',dict(last_completed=report['completed_through_utc'],data_errors=list(report.get('unavailable',{}))))
            else: health('시장 분석','완료 봉 지연')
            today=datetime.now(KST).date()
            if today!=saved:
                backup()
                saved=today
        except Exception:
            LOG.exception('market worker error')
            health('시장 분석','분석 실행 예외')

def trigger(requests,at):
    try: requests.put_nowait(at)
    except Full: pass

def crossing(sym,price,previous,report,at,request_queue,mark=False):
    m=next((x for x in report.get('market',[]) if x['symbol']==sym),None)
    if m and abs(time.time()-datetime.fromisoformat(report['completed_through_utc']).timestamp())<180 and previous:
        levels=[('저항',m['resistance']),('지지',m['support'])]
        levels += [('후보 진입가 '+str(c['side']),c['entry']) for c in m['directions'] if c['candidate_visible']]
        for label,level in levels:
            if (previous-level)*(price-level)<0:
                side='상향' if price>level else '하향'
                notify(f'{sym}/{label}',f'{side}/{round(level,1)}/{int(at//300)}',
                       f'{sym} {label} {fmt(level)} {side} 통과 · 실시간 진행 중 {fmt(price)}',at)
                trigger(request_queue,at)
        baseline=m.get('completed_1m_close',previous)
        if abs(price/baseline-1)>=.003:
            notify(sym+'/jump',str(int(at//60)),f'{sym} 단기 급변 {fmt(baseline)} → {fmt(price)} · 진행 중',at)
            trigger(request_queue,at)
    if not previous: return
    for p in get('positions',[]):
        if p['symbol']!=sym: continue
        for label,k in [('목표','target'),('스탑','stop')]:
            level=p.get(k)
            if level and (previous-level)*(price-level)<=0 and previous!=price:
                notify(f"user/{p['id']}/{k}",str(level),f"등록 #{p['id']} {label} {fmt(level)} 접근/통과 · 거래소 체결 미확인",at)
        lev=p['leverage']; side=p['side']
        liq=p['entry']*(1-side*(1/lev-live.FEE))/(1-side*.005)
        if mark and side*(price-liq)/price<.005:
            notify(f"user/{p['id']}/liq",'near',f"등록 #{p['id']} 마크 {fmt(price)} · 모형 청산 {fmt(liq)} 근접",at)

async def stream(stop,requests):
    delay=2
    last={}
    report={}
    sampled={}
    announced=False
    flow={}
    while not stop.is_set():
        for route in ('/market',''):
            if stop.is_set(): break
            try:
                async with connect(BASE+route+'/stream?streams='+'/'.join(STREAMS),open_timeout=12,close_timeout=2) as ws:
                    while not stop.is_set():
                        packet=json.loads(await asyncio.wait_for(ws.recv(),timeout=20))['data']
                        if not announced:
                            health('WebSocket',None)
                            announced=True
                        delay=2
                        sym=packet.get('s')
                        if not sym: continue
                        at=time.time()
                        event_at=packet.get('E',at*1000)/1000
                        if abs(at-event_at)>30: continue
                        if packet.get('k',{}).get('x'):
                            k=packet['k']
                            row=[k[x] for x in ('t','o','h','l','c','v','T','q','n','V','Q')]+[k.get('B','0')]
                            with WS_LOCK:
                                bars=WS_BARS.setdefault((sym,k['i']),{})
                                bars[k['t']]=row
                                while len(bars)>240: bars.pop(min(bars))
                            trigger(requests,event_at)
                        kind=packet.get('e')
                        if kind not in ('aggTrade','markPriceUpdate'): continue
                        if kind=='aggTrade':
                            bucket=int(event_at//15)
                            old=flow.get(sym,(bucket,0.,0.))
                            if old[0]!=bucket:
                                if old[2]>=100000 and abs(old[1]/old[2])>.65:
                                    label='매수' if old[1]>0 else '매도'
                                    notify(sym+'/flow',f'{label}/{old[0]//4}',f'{sym} 15초 공격 {label} 쏠림 · 진행 중',event_at)
                                    trigger(requests,event_at)
                                old=(bucket,0.,0.)
                            amount=float(packet['p'])*float(packet['q'])
                            flow[sym]=(bucket,old[1]+amount*(-1 if packet['m'] else 1),old[2]+amount)
                        key=(sym,kind); price=float(packet['p'])
                        if at-sampled.get(key,0)<2: continue
                        sampled[key]=at
                        if at-sampled.get('report',0)>5:
                            sampled['report']=at
                            report=latest()
                        crossing(sym,price,last.get(key),report,event_at,requests,kind=='markPriceUpdate')
                        last[key]=price
            except Exception as ex:
                LOG.warning('WebSocket disconnected: %s',type(ex).__name__)
                health('WebSocket',f'{type(ex).__name__}: {ex}')
                last.clear()
                flow.clear()
                announced=False
        await asyncio.to_thread(stop.wait,delay)
        delay=min(60,delay*2)

def setup():
    token=getpass.getpass('Telegram bot token (blank keeps current): ').strip()
    if token:
        import keyring
        if os.name!='nt': raise RuntimeError('Credential setup requires Windows Credential Manager')
        keyring.set_password('CoinFuturesLab','Telegram',token)
    chat=input('Allowed private Telegram chat ID (blank keeps current): ').strip()
    if chat:
        if not chat.isdecimal(): raise ValueError('Use the numeric private chat ID')
        put('chat',chat)
    if os.name=='nt':
        choice=input('Windows login autostart [y/n/Enter keep]: ').strip().lower()
        if choice in ('y','n'):
            path=Path(os.environ['APPDATA'])/'Microsoft/Windows/Start Menu/Programs/Startup/CoinFuturesLab-V18.cmd'
            if choice=='y': path.write_text(f'@call "{ROOT / "Start.bat"}"\n',encoding='utf-8')
            else: path.unlink(missing_ok=True)

def run():
    import socket
    lock=socket.socket()
    try:
        if os.name=='nt': lock.setsockopt(socket.SOL_SOCKET,socket.SO_EXCLUSIVEADDRUSE,1)
        lock.bind(('127.0.0.1',47818))
    except OSError:
        sys.exit(2)
    lock.listen(1)
    STOP.unlink(missing_ok=True)
    stop=threading.Event()
    requests=Queue(1)
    token,chat=credentials()
    def supervise(label,func):
        while not stop.is_set():
            try: func()
            except Exception:
                LOG.exception('%s worker restarted',label)
            if not stop.is_set(): stop.wait(5)
    jobs=[('update',lambda:autoupdate.watch(stop)),('market',lambda: overview(stop,requests)),('stream',lambda: asyncio.run(stream(stop,requests)))]
    if token and chat:
        jobs.extend([('telegram receive',lambda:receive(stop,token,chat)),('telegram send',lambda:sender(stop,token,chat))])
    threads=[threading.Thread(target=supervise,args=(name,func),daemon=True) for name,func in jobs]
    for t in threads: t.start()
    def cli():
        for line in sys.stdin:
            if stop.is_set(): break
            respond(line.strip(),'cmd',print)
    threading.Thread(target=cli,daemon=True).start()
    try:
        while not STOP.exists(): time.sleep(.5)
    except KeyboardInterrupt: pass
    finally:
        stop.set()
        for t in threads: t.join(45)
        backup()
        lock.close()

if __name__=='__main__':
    logging.basicConfig(level=logging.INFO,format='%(asctime)s %(levelname)s %(message)s',handlers=[RotatingFileHandler(DATA/'monitor.log',maxBytes=2_000_000,backupCount=3,encoding='utf-8')])
    parser=argparse.ArgumentParser()
    parser.add_argument('command',choices=('run','ask','setup','stop','check','wait-stop'))
    parser.add_argument('--send',action='store_true')
    parser.add_argument('message',nargs='*')
    a=parser.parse_args()
    if a.command=='run': run()
    elif a.command=='ask': respond(' '.join(a.message),'cmd',print)
    elif a.command=='setup': setup()
    elif a.command=='stop': STOP.touch()
    elif a.command=='wait-stop':
        import socket
        for _ in range(100):
            try:
                with socket.socket() as probe:
                    probe.bind(('127.0.0.1',47818))
                break
            except OSError:
                time.sleep(1)
        else:
            sys.exit('Previous instance is still shutting down.')
    else:
        x=live.tick(read_only=True,market_api=ORIGINAL_API)
        print(x['completed_through_utc'],[m['symbol'] for m in x['market']],x['unavailable'])
        token,chat=credentials()
        print('Telegram:', bool(token and chat))
        if token and chat and (a.send or '--send' in a.message):
            tg(token,'sendMessage',dict(chat_id=chat,text='CoinFuturesLab V18 연결 점검'))
        if len(x['market'])!=2: sys.exit(1)
