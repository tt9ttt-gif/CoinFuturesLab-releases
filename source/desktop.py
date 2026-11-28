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
from message_format import RULE, timestamp, payload, payloads, reason, notice

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
    with urlopen(req,timeout=25 if method=='getUpdates' else 8) as r:
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
    if key.startswith('user/'): return 'position_risk'
    if key.startswith('health/'): return 'important'
    if key.startswith('paper/manage/'): return 'paper_manage'
    if key.startswith('paper/'): return 'paper'
    if key.startswith('summary/'): return 'summary'
    if key.startswith('idea/'): return 'important'
    return 'market'

def permitted(kind):
    cfg=alert_settings()
    return cfg['mode']!='off' and (kind in ('important','position_risk') or
        (kind=='market' and cfg['mode']=='all') or
        (kind in ('paper','paper_manage','summary') and cfg.get(kind,False)))

def notify(key,signature,text,detected=None,cooldown=0,force=False):
    kind=category(key)
    if not permitted(kind):
        return
    now=time.time()
    with db() as c:
        c.execute('BEGIN IMMEDIATE')
        actual=re.match(r'user/(\d+)/',key)
        if actual and not any(p['id']==int(actual[1]) for p in kv_get(c,'positions',[])): return
        row=c.execute('SELECT signature,queued FROM alerts WHERE key=?',(key,)).fetchone()
        if row and (row[0]==signature or ((not force or (actual and row[0] in ('스탑 도달','청산 근접','추세·구조 동시 역행'))) and now-row[1]<cooldown)):
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
            row=c.execute('SELECT id,body,detected,analyzed,category FROM outbox WHERE sent IS NULL AND suppressed=0 ORDER BY (category=?) DESC,id LIMIT 1',('position_risk',)).fetchone()
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

SYMBOL_PATTERNS={
    'BTCUSDT':r'(?<![a-z0-9])(?:btc(?:usdt)?|btcc|bitcoin|btcoin)(?![a-z0-9])|비트(?:코[인잉])?',
    'ETHUSDT':r'(?<![a-z0-9])(?:eth(?:usdt)?|ethe|ether(?:eum)?|ethe?rum)(?![a-z0-9])|이더(?:리[움윰])?|이뎌',
}

def requested_symbols(text):
    msg=text.strip().lower()
    if re.search(r'^/market(?: |$)|둘\s*다|전체\s*시장|두\s*종목',msg):
        return live.SYMS
    return tuple(symbol for symbol,pattern in SYMBOL_PATTERNS.items() if re.search(pattern,msg))

def query_symbols(text,context):
    return requested_symbols(text) or ((context['symbol'],) if context.get('symbol') in live.SYMS else live.SYMS)

def parse(text,prior):
    msg=text.lower()
    out=dict(prior)
    targets=requested_symbols(msg)
    if len(targets)==1:
        sym=targets[0]
        if sym!=out.get('symbol') or re.fullmatch(r'/(btc|eth)',msg):
            out={}
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
        margin=held.get('margin'); lev=held.get('leverage'); entry=held['entry']
        if margin is None or lev is None or held.get('currency','KRW')!='KRW': return None,[]
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
    neutral=direction=='횡보' and c['confirmed']
    structural=m.get('structure_break')==side and 'flow' in evidence and c['entry_status']=='entry'
    correlated_setup=direction=='불확실' and c['entry_status']=='entry' and c['score']>=2.0 and bool(evidence & {'flow','structure'})
    return ((aligned or neutral or structural) and len(evidence)>=2) or correlated_setup,evidence


ENTRY_ANALYSIS='ENTRY_ANALYSIS'
POSITION_OPEN='POSITION_OPEN'
POSITION_CLOSE='POSITION_CLOSE'
NUMBER=r'(?:\d{1,3}(?:,\d{3})+|\d+)(?:\.\d+)?'
MONEY=re.compile(rf'(?<![\d.,+\-])(?P<value>{NUMBER})\s*(?P<unit>만\s*원|원|달러|usdt|usd|\$)(?![a-z])',re.I)
CLOSE=re.compile(rf'(?:(?P<symbol>[^\s]+)\s+)?거래\s*종료(?:\s+(?P<result>수익|손실|손익)\s*(?P<amount>[+-]?{NUMBER}\s*(?:만\s*원|원|달러|usdt|usd|\$)))?',re.I)
COMPLETED=re.compile(r'들어\s*왔|진입\s*(?:했|완료)|체결\s*(?:됐|되었|완료)|(?:매수|매도)\s*완료')
CHECK=re.compile(r'[?？]|어때|어떨|어떠|어땜|어떰|괜찮|분석|갈까|하면|했으면|됐으면|왔으면|하려|해도|할까|맞아')
ENTRY_WORD=re.compile(r'지정가|예약|진입|들어갈까|롱|숏|\blong\b|\bshort\b')
EXECUTION_CLOCK=re.compile(r'(?<![\d:])(?:(?P<day>\d{4}-\d{2}-\d{2})\s+)?(?P<hour>\d{1,2}):(?P<minute>\d{2})(?![\d:])')


def trade_intent(message):
    low=message.strip().lower()
    if CLOSE.fullmatch(low): return POSITION_CLOSE
    if low.startswith('/register '): return POSITION_OPEN
    if low.startswith('/'): return None
    uncertain=CHECK.search(low) or re.search(r'아직|안\s*(?:했|들어)|못\s*(?:했|들어)|않았|오타|아니|미체결|예정|희망',low)
    entered=COMPLETED.search(low)
    if entered and not uncertain: return POSITION_OPEN
    if re.search(r'예약|미체결|예정|희망',low) or (ENTRY_WORD.search(low) and CHECK.search(low)): return ENTRY_ANALYSIS
    if uncertain: return None
    if re.search(r'(?:^|\s)진입(?:가)?\s',low) and re.search(r'롱|숏|\blong\b|\bshort\b',low):
        return POSITION_OPEN
    if '진입가' in low or ('지정가' in low and re.search(NUMBER,low)): return 'confirm_open'
    if '지정가' in low: return ENTRY_ANALYSIS
    return None


def execution_time(value,received):
    match=EXECUTION_CLOCK.fullmatch(value.strip())
    if not match: raise ValueError('invalid execution clock')
    local=received.astimezone(KST)
    day=datetime.fromisoformat(match['day']) if match['day'] else local
    reported=day.replace(hour=int(match['hour']),minute=int(match['minute']),second=0,microsecond=0,tzinfo=KST)
    if reported>local or (not match['day'] and local-reported>timedelta(hours=12)):
        raise ValueError('ambiguous execution date')
    return reported.astimezone(timezone.utc).isoformat()


def money_value(amount,unit):
    unit=re.sub(r'\s+','',unit.lower())
    currency={'원':'KRW','만원':'KRW','달러':'USD','$':'USD','usd':'USD','usdt':'USDT'}[unit]
    return float(amount.replace(',',''))*(10000 if unit=='만원' else 1),currency


def trade_fields(message):
    clock=EXECUTION_CLOCK.search(message)
    if clock:
        message=message[:clock.start()]+' '+message[clock.end():]
    fields=parse(message,{})
    if clock: fields['execution_clock']=clock[0]
    fields.pop('margin',None)
    amounts=list(MONEY.finditer(message))
    if len(amounts)==1:
        amount=amounts[0]
        fields['amount'],fields['currency']=money_value(amount['value'],amount['unit'])
        if re.search(r'증거금\s*[:=]?\s*$',message[:amount.start()]):
            fields['amount_kind']='margin'
        elif re.search(r'(?:포지션(?:\s*금액)?|명목(?:\s*금액)?|총액)\s*[:=]?\s*$',message[:amount.start()]):
            fields['amount_kind']='notional'
    elif amounts:
        raise ValueError('금액 하나만 입력하세요.')
    else:
        explicit=re.search(rf'증거금\s*({NUMBER})(?![\d,.])',message)
        if explicit:
            fields.update(amount=float(explicit[1].replace(',','')),currency='KRW',amount_kind='margin')
    kind=re.search(r'격리|교차|\bisolated\b|\bcross\b',message,re.I)
    if kind: fields['margin_type']='isolated' if kind[0].lower() in ('격리','isolated') else 'cross'
    if 'entry' not in fields:
        cleaned=MONEY.sub('',message)
        cleaned=re.sub(rf'(?:익절가|손절가|증거금)\s*{NUMBER}|{NUMBER}\s*배','',cleaned)
        match=re.search(rf'(?<![\w.,-]){NUMBER}(?![\d.,])',cleaned)
        if match: fields['entry']=float(match[0].replace(',',''))
    return fields


def position_liquidation(p):
    lev=p.get('leverage')
    if not lev or p.get('margin_type')=='cross':
        return None
    side=p['side']
    if p.get('position_notional') and p.get('margin'): lev=p['position_notional']/p['margin']
    return p['entry']*(1-side*(1/lev-live.FEE))/(1-side*.005)


def position_profit(p,price):
    """P&L in the registered currency, with a constant currency conversion assumption."""
    if not p.get('margin') or not p.get('leverage'):
        return None
    notional=p.get('position_notional',p['margin']*p['leverage'])
    ratio=price/p['entry']
    gross=notional*p['side']*(ratio-1)
    estimated_entry=notional*live.FEE
    exit_fee=notional*ratio*live.FEE
    actual_entry=p.get('verified_entry_fee')
    funding=p.get('verified_funding_pnl')
    entry_fee=estimated_entry if actual_entry is None else actual_entry
    net=gross-entry_fee-exit_fee+(funding if funding is not None else 0.)
    # Slippage stress is computed at a separate adverse execution price.
    stressed_ratio=ratio*(1-p['side']*live.SLIP)
    stressed=notional*p['side']*(stressed_ratio-1)-entry_fee-notional*stressed_ratio*live.FEE+(funding if funding is not None else 0.)
    return dict(notional=notional,gross=gross,entry=entry_fee,actual_entry=actual_entry,
                exit=exit_fee,funding=funding,net=net,stress=stressed)


def protection_plan(p,m,price,atr,liq,buffer):
    side=p['side']
    gap=price*atr
    if not math.isfinite(gap) or gap<=0:
        return {'reason':'완료 15분봉 변동성 자료 없음'}
    if liq is None:
        return {'reason':'청산가 또는 안전 여유를 평가할 수 없음'}
    if m.get('position_mark_price',m.get('mark_price')) is None:
        return {'reason':'최신 마크가격 자료 없음. 최근가만으로 청산 여유를 산정하지 않음'}
    if side*(price-liq)<price*buffer:
        return {'reason':'현재 마크가격이 청산 위험 구간에 있음'}
    levels=m.get('position_levels',{})
    stop_key,target_key=('low','high') if side==1 else ('high','low')
    rank=('f1h','f4h','f15m','f1w')
    labels=dict(f1h='1시간',f4h='4시간',f15m='15분',f1w='주봉')
    stops=[]
    targets=[]
    for key in rank:
        for anchor in sorted(levels.get(key,{}).get(stop_key,[]),key=lambda value: side*(price-value)):
            if side*(price-anchor)>=gap*.5:
                level=round(anchor-side*gap*.35,2)
                if side*(price-level)>=gap*.75:
                    stops.append((level,f'{labels[key]} 스윙 {fmt(anchor)} 이탈에 15분 ATR 0.35배 완충'))
        for anchor in levels.get(key,{}).get(target_key,[]):
            if side*(anchor-price)>=gap:
                level=round(anchor-side*gap*.15,2)
                if side*(level-price)>=gap*.75:
                    targets.append((level,f'{labels[key]} 스윙 {fmt(anchor)} 앞에 15분 ATR 0.15배 여유'))
    registered=p.get('stop')
    safe=[(level,why) for level,why in stops
          if (registered is None or side*(level-registered)>=0)
          and side*(level*(1-side*live.SLIP)-liq)>=price*buffer]
    if not safe:
        return {'reason':'변동성 완충·청산 여유·등록 손절 유지 조건을 함께 충족하는 구조 손절 없음'}
    level,why=safe[0]
    stop=position_profit(p,level)
    current=position_profit(p,price)
    if not stop or not current:
        return {'reason':'등록 증거금 또는 배수가 없어 비용과 손실 평가 불가'}
    risk=current['net']-stop['stress']
    if risk<=0:
        return {'reason':'현재가 대비 손절 위험 평가 불가'}
    feasible=[]
    for target,reason in sorted(set(targets),key=lambda row: (side*row[0],row[1])):
        estimate=position_profit(p,target)['stress']
        gain=estimate-current['net']
        if estimate>0 and gain>=risk and (not feasible or side*(target-feasible[-1][0])>=gap):
            feasible.append((target,reason,estimate,gain/risk))
    return dict(stop=level,stop_reason=why,stop_net=stop['stress'],reward=feasible[:2],
                room=side*(level*(1-side*live.SLIP)-liq)/price,atr=gap,
                loss=max(0,-stop['stress'])/p['margin'],reason=None if feasible else '진입가·비용·현재 위험 대비 성립하는 구조 익절 없음')


def holding_evidence(m,side):
    f=m.get('indicators',{})
    a=f.get('f15m',{})
    h=a.get('r6')
    flow=a.get('flow')
    oi=m.get('position_oi')
    funding=m.get('position_funding')
    atr=a.get('atr')
    volume=a.get('volume')
    def impact(direction):
        return '보유 방향에 유리' if direction==side else '보유 방향에 불리' if direction==-side else '보유 방향에 중립'
    evidence=[]
    if oi is not None:
        directional=0
        if oi>0 and h is not None and flow is not None and atr and abs(h)>=atr*.5 and abs(flow)>=.05 and h*flow>0:
            directional=1 if h>0 else -1
        observed=f'OI {oi:+.2%}'
        if h is not None: observed+=f', 15분 6봉 가격 {h:+.2%}'
        if flow is not None: observed+=f', 순공격 체결 비중 {flow:+.1%}'
        if volume is not None: observed+=f', 거래량 {volume:.2f}배'
        meaning=('포지션 잔량이 늘었고 가격과 공격 체결 방향이 일치합니다. 신규 포지션의 방향은 직접 확인할 수 없습니다.' if directional else
                 '미결제약정은 열린 계약의 양입니다. 증감만으로 새 롱·숏의 방향을 판별할 수 없습니다.')
        evidence.append((directional,f'{observed}. {meaning} {impact(directional)}. 다음 1시간봉이 주요 가격대를 유지하는지 확인하고 역행 이탈이면 축소를 검토합니다.'))
    cvd=m.get('position_cvd24')
    change=m.get('position_cvd_price_change')
    if cvd is not None and change is not None:
        direction=0 if cvd*change<=0 else 1 if cvd>0 else -1
        meaning='공격 체결과 가격이 같은 방향입니다' if direction else '공격 체결에 가격이 따라가지 않아 반대 주문의 흡수 가능성이 있습니다'
        evidence.append((direction,f'15분 24봉 누적 순공격 체결(CVD) {cvd:+,.2f} 기초자산, 가격 {change:+.2%}. {meaning}. {impact(direction)}. 돌파·이탈 뒤 종가와 거래량이 동행하는지 확인합니다.'))
    rsi=a.get('rsi')
    if rsi is not None:
        meaning='과매도는 반등 가능성을 시사하나 하락 추세에서는 지속될 수 있습니다' if rsi<=30 else '과매수는 과열 신호이나 상승 추세에서는 지속될 수 있습니다' if rsi>=70 else '최근 상승·하락 강도가 극단 구간에 있지 않습니다'
        evidence.append((0,f'15분 RSI {rsi:.1f}. {meaning}. 보유 방향 판단은 중립입니다. 종가의 구조 회복 여부를 먼저 확인합니다.'))
    if funding is not None:
        pays=funding*side>0
        cost='지급 부담' if pays else '수취 가능' if funding else '비용 중립'
        evidence.append((0,f'공개 펀딩률 {funding:+.4%}. 양수면 롱이 숏에 지급합니다. 현재 방향은 {cost}이며 가격 방향의 근거는 아닙니다. 다음 공시율과 비용을 재확인합니다.'))
    return evidence


def holding_trend(m,side):
    f=m.get('indicators',{})
    labels={'f1w':'주봉','f1d':'일봉','f4h':'4시간','f1h':'1시간'}
    def trend(key):
        ema=f.get(key,{}).get('ema')
        return None if ema is None else 1 if ema>.002 else -1 if ema<-.002 else 0
    words={1:'상승',-1:'하락',0:'보합',None:'미확인'}
    values={k:trend(k) for k in labels}
    primary=values['f4h']
    daily=values['f1d']
    relation='추세 추종' if daily==primary==side else '역추세 보유' if daily==primary==-side else '상위 추세 합의 미확인'
    out=' · '.join(f'{labels[k]} {words[value]}' for k,value in values.items())+f' · {relation}'
    moves=[(label,f.get(key,{}).get('r6')) for key,label in [('f15m','15분'),('f5m','5분'),('f1m','1분')]]
    if primary and any(value is not None and value*primary<0 for _,value in moves):
        out+='\n단기 역행이 관측됩니다. 1시간봉 구조까지 반전하는지 확인하기 전에는 4시간 추세 전환을 유보합니다.'
    return out,values


def protection_lines(plan,money):
    if 'stop' not in plan:
        return ['손절 후보·익절 후보 산정 보류 · '+plan['reason']+'. 위험 축소 검토.']
    out=[f"손절 후보 {fmt(plan['stop'])} USDT · {plan['stop_reason']} · 미설정",
         f"예상 순손익 {money(plan['stop_net'])} · 증거금 손실 {plan['loss']:.2%} · 청산 여유 {plan['room']:.2%}"]
    for i,(target,reason,net,ratio) in enumerate(plan['reward'],1):
        out.append(f"{i}차 익절 후보 {fmt(target)} USDT · {reason} · 미설정\n예상 순손익 {money(net)} · 현재가 기준 비용 반영 손익비 {ratio:.2f}:1")
    if plan['reason']: out.append('익절 후보 산정 보류 · '+plan['reason']+'. 위험 축소 검토.')
    return out


def position_review(p,m,seed,mode='normal',stamp=None,*,prior=None,plan=None):
    side=p['side']
    mark=m.get('position_mark_price',m.get('mark_price'))
    price=mark or m['price']
    c=next(x for x in m['directions'] if x['side']==side)
    f=m.get('indicators',{})
    a=f.get('f15m',{})
    atr=c['atr_ratio']
    direction=m.get('market_direction','불확실')
    structure=m.get('structure_break')==-side
    opposite=direction==('하락' if side==1 else '상승')
    initial=p.get('initial_direction')
    reversed_since_entry=initial==('상승' if side==1 else '하락') and opposite
    adverse=m['candidate']['side']!=side and m['candidate']['score']>=1.65
    flow=a.get('flow')
    impulse=a.get('r6')
    shock=impulse is not None and side*impulse<=-3*atr and a.get('volume',0)>=1.3
    pressure=flow is not None and side*flow<=-.15 and adverse and (structure or opposite)
    stop=p.get('stop'); target=p.get('target')
    hit=stop is not None and side*(price-stop)<=0
    near=stop is not None and side*(price-stop)/price<=max(.002,atr)
    lev,margin=p.get('leverage'),p.get('margin')
    profit=position_profit(p,price)
    liq=position_liquidation(p)
    buffer=max(.005,min(.02,atr*2)) if lev and lev>=25 else .005
    clearance=side*(mark-liq)/mark if liq is not None and mark else None
    liquidation=clearance is not None and clearance<buffer
    stop_clearance=side*(stop-liq)/mark if stop is not None and liq is not None and mark else None
    unsafe_stop=stop_clearance is not None and stop_clearance<buffer
    if plan is None:
        plan=protection_plan(p,m,price,atr,liq,buffer)
    proposal=None
    if adverse and (structure or opposite) and 'stop' in plan:
        proposed_stop=plan['stop'] if stop is not None and side*(plan['stop']-stop)>=price*max(.003,atr) else stop
        proposed_target=target
        if target is not None and plan['reward'] and side*(target-plan['reward'][0][0])>=price*max(.003,atr):
            proposed_target=plan['reward'][0][0]
        if (proposed_stop,proposed_target)!=(stop,target): proposal=(proposed_stop,proposed_target)
    state,action,risk='유지','유지 가능','보통'
    if hit or liquidation or (structure and opposite):
        state='스탑 도달' if hit else '청산 근접' if liquidation else '추세·구조 동시 역행'
        action,risk='전체 청산 검토','매우 높음'
    elif unsafe_stop or near or (stop is None and lev and lev>=25):
        state='손절의 청산가 여유 부족' if unsafe_stop else '스탑 접근' if near else '고배율 손절 미등록'
        action,risk='손절가 설정·조정 우선','높음'
    elif (opposite and lev and lev>=20) or shock or pressure or reversed_since_entry:
        state=('상위 추세 역행·고배율' if opposite and lev and lev>=20 else '불리한 거래량·변동성 확대' if shock
               else '반대 체결 압력 확대' if pressure else '등록 시점 대비 상위 방향 반전')
        action,risk='일부 익절 또는 위험 축소 검토','높음'
    elif (opposite and adverse) or proposal:
        state,action,risk='반대 근거 강화','유지하되 특정 가격 이탈 주의','높음'
    elif stop is None:
        action,risk='손절가 설정·조정 우선','미평가'
    elif profit is None or liq is None or direction=='불확실':
        action,risk='데이터 또는 정보 부족으로 판단 유보','미평가'
    elif structure or adverse:
        action,risk='유지하되 특정 가격 이탈 주의','주의'

    currency=p.get('currency','KRW')
    unit='원' if currency=='KRW' else ' '+currency
    money=lambda amount: '미확인' if amount is None else f'{amount:+,.2f}{unit}'
    lower=m.get('support'); upper=m.get('resistance')
    reason_text=state if state!='유지' else '손절 미등록' if stop is None else '상위 방향·구조와 등록 정보를 함께 확인'
    trigger=stop if stop is not None else lower if side==1 else upper
    lines=[f"📌 등록 #{p['id']} {m.get('symbol',p.get('symbol',''))} {'롱' if side==1 else '숏'} · {action}",
           f"⚠️ 위험 {risk} · {reason_text} · 상위 방향 {direction} · 구조 {'역행' if structure else '이탈 미확인'}",
           f"최근가 {fmt(m['price'])} · 마크 {fmt(mark)} · 신고 진입가 {fmt(p['entry'])} USDT",
           f"등록 증거금 {fmt(margin)}{unit} · {fmt(lev,2)}배 · 진입 명목 {fmt(profit['notional'] if profit else None)}{unit}",
           f"손절 {fmt(stop)} · 익절 {fmt(target)} · 보유 판단 재확인 {fmt(trigger)}",
           f"격리 가정 청산가 {fmt(liq)}"+(f" · 현재 여유 {clearance:+.2%}" if clearance is not None else ' · 마크 또는 계좌 정보 미확인')]
    if profit:
        lines.append(f"가격 미실현 {money(profit['gross'])} · 청산 가정 순손익 {money(profit['net'])} (미확인 펀딩 제외)")
    else:
        lines.append('가격 미실현·청산 가정 순손익 미확인 · 증거금/배수 필요')
    lines.extend(protection_lines(plan,money))
    trend,trend_values=holding_trend(m,side)
    evidence=holding_evidence(m,side)
    if prior is None:
        prior=get(f"held-context/{p['id']}",{}) if stamp else {}
        prior=prior.get('previous',prior) if prior.get('stamp')==stamp else prior
    if mode=='simple':
        lines.append(next((line for signal,line in evidence if signal==-side),evidence[0][1] if evidence else trend.splitlines()[0]))
    elif mode!='simple':
        lines.extend(['📊 시장 구조와 보유 방향',trend])
        if prior.get('trend') and prior['trend']!=trend_values:
            changed=[k for k in trend_values if prior['trend'].get(k)!=trend_values[k]]
            lines.append('직전 자동 관측 대비 추세 변화 '+', '.join(k[1:] for k in changed))
        prior_evidence=prior.get('evidence',{})
        for label,line in ((line.split()[1] if line.startswith('15분 ') else line.split()[0],signal) for signal,line in evidence):
            if label in prior_evidence and prior_evidence[label]!=line:
                lines.append(f'직전 자동 관측 대비 {label}의 방향 근거가 달라졌습니다.')
        if prior.get('direction') and prior['direction']!=direction:
            lines.append(f"직전 자동 관측 상위 방향 {prior['direction']} → {direction}")
        groups=(('핵심 보유 근거',side),('핵심 반대 근거',-side),('조건부 해석',0))
        for heading,wanted in groups:
            items=[line for signal,line in evidence if signal==wanted]
            if items: lines.extend([heading,*items])
        missing=[label for key,label in [('position_oi','OI'),('position_funding','펀딩'),('position_cvd24','CVD')] if m.get(key) is None]
        if missing: lines.append('공개 데이터 미확인 · '+', '.join(missing))
    for label,level in [('손절',stop),('익절',target)]:
        if level is not None and mode!='simple':
            lines.append(f"{label}까지 가격 변동 {level/price-1:+.2%} · 해당 가격 가정 순손익 "+money((position_profit(p,level) or {}).get('net')))
    if mode!='simple':
        lines.extend(['💰 손익 및 위험 근거',
                      f"증거금 {fmt(margin)}{unit} · {fmt(lev,0)}배 · "+{'cross':'교차','isolated':'격리'}.get(p.get('margin_type'),'증거금 유형 미확인'),
                      '등록값 기준 추정 · 거래소 체결·비용 미확인'])
        if profit:
            entry_label='확인된 진입 수수료' if profit['actual_entry'] is not None else '추정 진입 수수료'
            lines.extend([f"{entry_label} {money(-profit['entry'])} · 추정 청산 수수료 {money(-profit['exit'])}",
                          f"확인된 펀딩 {money(profit['funding'])} · 슬리피지 스트레스 순손익 {money(profit['stress'])}",
                          f"증거금 수익률 가격 {profit['gross']/margin:+.2%} / 순손익 추정 {profit['net']/margin:+.2%} · 진입가 기준 ±1% 변동당 약 ±{profit['notional']/margin:.2f}%p",
                          '계좌 대비 손익률 미확인 · 실제 계좌 잔고 필요'])
        if profit and profit['actual_entry'] is None: lines.append('실제 발생 진입 수수료 미확인 · 추정 수수료로 계산')
        if stop is None:
            lines.append('손절 미등록 · 손절 기준 계좌 위험 미확인')
        elif profit:
            lines.append(f"손절 체결 가정 순손익 {money(position_profit(p,stop)['stress'])} (갭 확대 미반영)")
        if stop_clearance is not None:
            lines.append(f"손절과 청산가 간 보호 여유 {stop_clearance:+.2%} · 모형 확인 기준 {buffer:.2%}")
        if stop is not None and target is not None and profit:
            stop_net=position_profit(p,stop)['stress']
            target_net=position_profit(p,target)['net']
            if stop_net<0 and target_net>0:
                lines.append(f"등록 익절·손절의 비용 반영 손익비 {target_net/-stop_net:.2f}:1")
        if near: lines.append('손절 거리가 최근 15분 변동폭 기준에 가까움. 단기 흔들림과 갭 위험을 확인하세요.')
        slow=' / '.join(f"{label} EMA {f[key]['ema']:+.2%}" if f.get(key,{}).get('ema') is not None else f'{label} 미확인'
                        for label,key in [('일봉','f1d'),('4시간','f4h'),('1시간','f1h')])
        fast=' / '.join(f"{label} 6봉 {f[key]['r6']:+.2%}" if f.get(key,{}).get('r6') is not None else f'{label} 미확인'
                        for label,key in [('15분','f15m'),('5분','f5m'),('1분','f1m')])
        pct=lambda value: '미확인' if value is None else f'{value:+.2%}'
        lines.extend(['📊 보유 판단 근거',slow,fast,
                      f"15분 ATR {atr:.2%} · 거래량 배수 {fmt(a.get('volume'))} · 상태 {m.get('regime','미확인')}",
                      f"현물 순수급 {pct(m.get('spot_flow'))} · 선물 순수급 {pct(flow)}",
                      f"OI 변화 {pct(m.get('position_oi'))} · 펀딩 {pct(m.get('position_funding'))} · 롱/숏 비율 {fmt(m.get('long_short'))}"])
        for label,items in (('보유 근거',c.get('reasons',[])),('반대 근거',c.get('conflicts',[]))):
            if items: lines.append(label+' · '+' / '.join(reason(k,m) for k,_ in items[:2]))
        if initial is not None: lines.append(f"등록 직후 상위 방향 {initial} → 현재 {direction}")
        lines.append('🎯 가격대별 대응 (완료 15분봉 기준)')
        for level,up in ((upper,True),(lower,False)):
            if level is None: continue
            favorable=up==(side==1)
            confirmation=('상향 돌파 후 재지지·매수세 확인' if up else '하향 이탈 후 회복 실패·매도세 확인')
            response=('보유를 검토하되 거래량 동반 여부와 다음 목표를 확인' if favorable else
                      '위험 축소 검토. 이탈 지속 또는 손절 접근이면 전체 청산 검토')
            invalidation='종가가 다시 기준선 안쪽으로 돌아오면 이 시나리오 재평가'
            significance='매도 물량이 다시 출회될 수 있는 구간' if up else '매수가 가격을 방어할지 확인하는 구간'
            lines.append(f"{fmt(level)} {'저항' if up else '지지'} · {significance} · {confirmation} · 보유에 {'유리' if favorable else '불리'} · {response}")
            lines.append(f"해당 가격 가정 순손익 {money((position_profit(p,level) or {}).get('net'))} · {invalidation}")
        defended=lower if side==1 else upper
        if defended is not None:
            lines.append(f"{fmt(defended)} {'지지 유지·반등' if side==1 else '저항 유지·반락'} · 재시험 후 종가 방어와 수급 확인 시 유지 검토. 종가 이탈이 지속되면 방어 판단 무효.")
        lines.append('지지·저항은 스윙이나 ATR 보완 기준입니다. 돌파 이후 종가가 되돌아오고 반대 체결 압력과 거래량이 동시에 커지면 재평가하세요.')
    if proposal:
        for name,label,new in zip(('stop','target'),('손절','익절'),proposal):
            old=p.get(name)
            if old is not None and new!=old:
                before=position_profit(p,old)
                after=position_profit(p,new)
                delta=after['net']-before['net'] if before and after else None
                lines.append(f"{label} 조정 검토 {fmt(old)} → {fmt(new)} · 추정 순손익 변화 {money(delta)} · 주문 변경 없음")
    if mode=='detailed':
        for key,label in (('f1w','주봉'),('f1d','일봉'),('f4h','4시간'),('f1h','1시간'),('f15m','15분'),('f5m','5분'),('f1m','1분')):
            row=f.get(key,{})
            def indicator(name, perc=True):
                value=row.get(name)
                return '미확인' if value is None else f'{value:+.3%}' if perc else f'{value:.1f}'
            lines.append(f"{label} EMA20/50 괴리 {indicator('ema')} · RSI {indicator('rsi',False)} · MACD 히스토그램/가격 {indicator('macd')}")
        lines.append('MACD 히스토그램은 추세 모멘텀 변화의 보조 지표입니다. 가격 구조와 반대로 움직이면 추세 판단을 재확인합니다.')
        lines.append('가격 계산: 손절은 확인된 스윙에서 15분 ATR의 0.35배 밖, 목표는 반대 스윙보다 0.15배 앞. 마크와 청산가 사이 여유 및 비용 반영 보상/위험을 검사합니다.')
        if 'stop' in plan:
            lines.append(f"15분 ATR {fmt(plan['atr'])} USDT · 최소 청산 여유 {buffer:.2%} · 목표 손익비 기준 1.00 이상. 추정 갭 추가 손실 미포함.")
        hist=c.get('historical',{})
        indicators=m.get('manual_indicators',{})
        lines.extend(['📚 상세 자료',f"Binance USD-M 공개 완료 봉 기준 {timestamp(stamp or m.get('position_data_utc'))}",
                      f"호가 불균형 {pct(m.get('book_imbalance'))} · 거래량 가격대 근사 POC {fmt(m.get('volume_profile_proxy_poc'))}",
                      f"15분 집계 CVD 24봉 {fmt(m.get('position_cvd24',indicators.get('flow',{}).get('aggregated_CVD24')))} · 다이버전스 {m.get('position_cvd_divergence') or '미확인'}",
                      f"가격 손익 = 진입 명목 × 방향 × (평가가/진입가−1). 수수료 편도 {live.FEE:.3%}, 슬리피지 스트레스 {live.SLIP:.3%}.",
                      f"엔진 유사 진입 관측 {hist.get('sample',0)}건 · 보유 포지션의 예측 확률로 해석 불가"])
    return state,'\n'.join(lines)


def execution_path(p,stamp):
    if p.get('execution_time_source')!='user_reported' or p.get('exposure_changed'):
        return '진입 이후 경로 미확인 · 직접 입력한 체결 시각과 동일한 진입 기준이 필요합니다.'
    minute=60_000
    opened=int(datetime.fromisoformat(p['execution_time']).timestamp()*1000)
    end=int(datetime.fromisoformat(stamp).timestamp()*1000)//minute*minute
    first=(opened+minute-1)//minute*minute
    start=max(first,end-720*minute)
    if end<=start:
        return '체결 이후 완료 1분봉 대기 중'
    count=(end-start)//minute
    rows=manual_rest(live.FUT,'/fapi/v1/klines',symbol=p['symbol'],interval='1m',
                     startTime=start,endTime=end-1,limit=count)
    if len(rows)!=count or any(int(row[0])!=start+i*minute or int(row[6])!=start+(i+1)*minute-1 for i,row in enumerate(rows)):
        return '진입 이후 완료 1분봉에 누락이 있어 MFE·MAE 계산을 보류합니다.'
    entry,side=p['entry'],p['side']
    highs=[float(row[2]) for row in rows]
    lows=[float(row[3]) for row in rows]
    if not all(math.isfinite(value) and value>0 for value in highs+lows):
        raise ValueError('invalid historical price')
    moves=[side*(price/entry-1) for price in (max(highs),min(lows))]
    label='최근 720개 봉만 · 부분 관측' if start>first else '체결 시각 이후'
    result=f'{label} 공개 완료 1분봉 {count}개 · MFE {max(0,*moves):+.2%} · MAE {min(0,*moves):+.2%}'
    if count>=10:
        half=count//2
        def buy_share(group):
            volume=sum(float(row[5]) for row in group)
            buys=sum(float(row[9]) for row in group)
            return buys/volume if volume>0 and 0<=buys<=volume else None
        before,after=buy_share(rows[:half]),buy_share(rows[half:])
        if before is not None and after is not None:
            result+=f'\n공개 선물 공격 매수 비중 전반 {before:.1%} → 후반 {after:.1%}'
    return result


def held_review(p,m,seed,stamp):
    mode=get('format','normal')
    _,body=position_review(p,m,seed,mode,stamp)
    def local(value):
        return datetime.fromisoformat(value).astimezone(KST).strftime('%m-%d %H:%M:%S KST') if value else '미확인'
    reported=p.get('execution_time_source')=='user_reported'
    label='사용자 신고 체결 시각' if reported else '수신 기준 임시 시각'
    times=(f"{label} {local(p.get('execution_time'))}\n"
           f"수신 {local(p.get('received_time'))} · 시장 조회 완료 {local(p.get('analysis_time'))}")
    try:
        path=execution_path(p,stamp)
    except (OSError,ValueError,TypeError,IndexError,KeyError):
        path='진입 이후 공개 분봉을 확인하지 못했습니다.'
    return body+'\n\n'+times+'\n'+path


def mutate_registration(fields,mode,ident=None):
    with db() as conn:
        conn.execute('BEGIN IMMEDIATE')
        active=kv_get(conn,'positions',[])
        existing=[p for p in active if p['symbol']==fields['symbol']]
        if existing and mode is None:
            return None,existing
        old=next((p for p in existing if p['id']==ident),None)
        if mode=='edit' and old is None:
            return None,existing
        used={p['id'] for p in active}
        history=kv_get(conn,'position_history',[])
        used.update(e.get('after',{}).get('id') for e in history if isinstance(e.get('after'),dict))
        new_id=ident if mode=='edit' else max(used|{99999},key=lambda n:n or 0)+1
        previous_fields={k:v for k,v in (old or {}).items() if k not in fields and (old['side']==fields['side'] or k not in ('stop','target'))}
        p=dict(previous_fields,**fields,id=new_id)
        if not old and p.get('received_time') and not p.get('execution_time'):
            p.update(execution_time=p['received_time'],execution_time_source='receipt_proxy')
        if p.get('execution_time')!= (old or {}).get('execution_time'):
            p['analysis_time']=None
        if old and ('amount' in fields or p.get('entry')!=old['entry'] or p['side']!=old['side']):
            p.pop('position_notional',None)
            p['exposure_changed']=True
        if 'amount' not in fields and p.get('amount_kind')=='notional' and p.get('leverage'):
            p['margin']=p['amount']/p['leverage']
        if not all(p.get(k) is None or math.isfinite(p[k]) and p[k]>0 for k in ('entry','leverage','margin','target','stop')):
            raise ValueError('invalid numeric field')
        if p.get('leverage') is not None and not 1<=p['leverage']<=125:
            raise ValueError('invalid leverage')
        if old:
            active[active.index(old)]=p
        else:
            active.append(p)
        history.append(dict(at=time.time(),operation=mode or 'register',before=old,after=p))
        kv_put(conn,'positions',active)
        kv_put(conn,'position_history',history)
        return p,[]


def kv_get(conn,key,default):
    row=conn.execute('SELECT value FROM kv WHERE key=?',(key,)).fetchone()
    return json.loads(row[0]) if row else default


def kv_put(conn,key,value):
    conn.execute('INSERT OR REPLACE INTO kv VALUES (?,?)',(key,json.dumps(value,ensure_ascii=False)))


def trade_command(message,identity,prior,positions,seed):
    low=message.strip().lower()
    received=datetime.fromtimestamp((getattr(REPLY_TIMING,'value',None) or {}).get('received_epoch',time.time()),timezone.utc)
    key='pending-trade/'+identity
    pending=get(key,{})
    if time.time()-pending.get('at',0)>600:
        pending={}
    if low in ('취소','/pos cancel'):
        put(key,{})
        return '입력 확인을 취소했습니다.'
    close=CLOSE.fullmatch(low)
    if close:
        syms=requested_symbols(close['symbol'] or '')
        matches=[p for p in positions if not close['symbol'] or p['symbol'] in syms]
        realized=None
        if close['result']:
            amount=MONEY.fullmatch(close['amount'].lstrip('+-'))
            value,currency=money_value(amount['value'],amount['unit'])
            if close['result']=='손실' or close['amount'].startswith('-'): value=-value
            if not math.isfinite(value): return '실현 손익 금액을 확인하세요.'
            realized=dict(amount=value,currency=currency,source='user')
        if len(matches)==1:
            put(key,{})
            return position_edit(['/pos','close',str(matches[0]['id'])],realized=realized)
        if matches:
            put(key,dict(intent=POSITION_CLOSE,at=time.time(),ids=[p['id'] for p in matches],realized=realized))
            return '종료할 포지션을 선택하세요. /pos close 번호\n'+', '.join(f"#{p['id']} {p['symbol']}" for p in matches)
        return '종료할 등록 포지션이 없습니다.'
    if pending.get('intent')==POSITION_CLOSE and re.fullmatch(r'/pos close \d+',low):
        ident=int(low.split()[2])
        if ident not in pending['ids']: return '선택 가능한 등록 번호를 확인하세요.'
        put(key,{})
        return position_edit(low.split(),realized=pending['realized'])
    intent=trade_intent(message)
    mode=None
    ident=None
    if intent:
        put(key,{})
        try: fields=trade_fields(message)
        except ValueError: return '금액 하나와 통화를 입력하세요.'
        fields['received_time']=received.isoformat()
    elif pending.get('stage')=='open' and low in ('등록','체결','예약 분석'):
        intent=ENTRY_ANALYSIS if low=='예약 분석' else POSITION_OPEN
        fields=pending['fields']
    elif pending.get('stage')=='time' and EXECUTION_CLOCK.fullmatch(low):
        intent=POSITION_OPEN
        fields=dict(pending['fields'],execution_clock=low)
    elif pending.get('stage')=='amount' and low in ('증거금','총액','포지션 금액'):
        intent=pending['intent']
        fields=dict(pending['fields'],amount_kind='margin' if low=='증거금' else 'notional')
    elif pending.get('stage')=='side' and low in ('롱','숏','long','short','확인'):
        intent=pending['intent']
        fields=pending['fields']
        side=pending.get('side') if low=='확인' else 1 if low in ('롱','long') else -1
        if side not in (-1,1): return '롱 또는 숏을 입력하세요.'
        fields['side']=side
    elif pending.get('collision') and re.fullmatch(r'추가|수정\s+\d+',low):
        intent=POSITION_OPEN
        fields=pending['fields']
        mode='add' if low=='추가' else 'edit'
        if mode=='edit': ident=int(low.split()[1])
    else:
        return None
    if 'symbol' not in fields and prior.get('symbol') in live.SYMS and not requested_symbols(message):
        fields['symbol']=prior['symbol']
    values=[fields[k] for k in ('entry','leverage','amount','stop','target') if k in fields]
    if any(not math.isfinite(v) or v<=0 for v in values) or not 1<=fields.get('leverage',1)<=125:
        return '가격·금액은 양수이며 배수는 1~125여야 합니다.'
    if fields.get('symbol') not in live.SYMS or (intent!=ENTRY_ANALYSIS and 'entry' not in fields):
        return '종목 하나와 양의 USDT 가격을 입력하세요.'
    if intent=='confirm_open':
        put(key,dict(stage='open',fields=fields,at=time.time()))
        return '이미 체결된 포지션이면 등록을 입력하세요. 진입 검토라면 예약 분석을 입력하세요.'
    if 'side' not in fields:
        prior_side=prior.get('side') if prior.get('symbol')==fields['symbol'] else None
        put(key,dict(intent=intent,stage='side',fields=fields,side=prior_side,at=time.time()))
        if prior_side in (-1,1):
            return f"이전 대화 방향은 {'롱' if prior_side==1 else '숏'}입니다. 확인, 롱 또는 숏을 입력하세요."
        return '롱인가요, 숏인가요? 롱 또는 숏을 입력하세요.'
    if 'amount' in fields and 'amount_kind' not in fields:
        put(key,dict(intent=intent,stage='amount',fields=fields,at=time.time()))
        return '입력한 금액이 증거금인가요, 레버리지 적용 후 총액인가요? 증거금 또는 총액을 입력하세요.'
    if intent==ENTRY_ANALYSIS:
        put(key,{})
        put('context/'+identity,{k:fields[k] for k in ('symbol','side')})
        m,stamp=manual_market(fields['symbol'])
        if not m: return '최신 완료 봉을 확보하지 못해 진입 분석을 보류합니다.'
        plan={k:v for k,v in fields.items() if k in ('symbol','side','entry','leverage','stop','target')}
        if fields.get('currency')=='KRW':
            if fields['amount_kind']=='margin': plan['margin']=fields['amount']
            elif 'leverage' in plan: plan['margin']=fields['amount']/plan['leverage']
        if 'entry' in plan:
            result=reservation_review(m,plan,seed,stamp)
        else:
            direction=next((row for row in m.get('directions',[]) if row['side']==plan['side']),None)
            if direction:
                immediate=direction.get('immediate_plan',{})
                plan.update({k:immediate[k] for k in ('entry','stop','target') if immediate.get(k) is not None and k not in plan})
            result=review(m,plan,seed,stamp)
        if 'amount' in fields and 'margin' not in plan:
            result+='\n입력 금액에 대한 원화 위험 계산 자료가 부족합니다.'
        return '진입 전 분석 · 조회 전용\n'+result
    if 'execution_clock' in fields:
        try:
            basis=datetime.fromisoformat(fields.get('received_time',received.isoformat()))
            fields['execution_time']=execution_time(fields['execution_clock'],basis)
        except ValueError:
            put(key,dict(stage='time',fields=fields,at=time.time()))
            return '체결 날짜·시각을 확인하세요. KST 기준 YYYY-MM-DD HH:MM으로 보내주세요.'
        fields.pop('execution_clock')
        fields['execution_time_source']='user_reported'
    if 'amount' in fields:
        if fields['amount_kind']=='notional':
            if 'leverage' not in fields:
                put(key,{})
                return '총액을 증거금으로 환산하려면 배수를 포함해 등록 메시지를 다시 입력하세요.'
            fields['margin']=fields['amount']/fields['leverage']
        else:
            fields['margin']=fields['amount']
    p,existing=mutate_registration(fields,mode,ident)
    if not p:
        put(key,dict(intent=intent,fields=fields,collision=True,at=time.time()))
        return ('기존 등록 포지션: '+', '.join(f"#{x['id']}" for x in existing)+
                '. 기존 배수·증거금·가격을 유지하며 수정하려면 수정 번호, 독립 기록으로 추가하려면 추가를 입력하세요.')
    put(key,{})
    put('context/'+identity,{'symbol':p['symbol'],'side':p['side']})
    missing=' · '.join(label for k,label in [('leverage','배수'),('margin','증거금'),('target','익절가'),('stop','손절가')] if p.get(k) is None)
    reply=f"#{p['id']} 사용자 포지션 등록 완료"+(f" · 미입력 {missing}" if missing else '')
    try:
        m,stamp=manual_market(p['symbol'])
        analysis_time=datetime.now(timezone.utc).isoformat()
        if m:
            with db() as conn:
                conn.execute('BEGIN IMMEDIATE')
                active=kv_get(conn,'positions',[])
                current=next((row for row in active if row==p),None)
                if current is not None:
                    current.update(initial_direction=m.get('market_direction','불확실'),initial_analysis_utc=stamp,analysis_time=analysis_time)
                    kv_put(conn,'positions',active)
        return reply+'\n\n'+(held_review(dict(p,analysis_time=analysis_time),m,seed,stamp) if m else '최신 데이터 부족으로 초기 대응 판단을 보류합니다.')
    except (OSError,ValueError,TimeoutError):
        return reply+'\n최신 데이터 조회에 실패했습니다. 등록 기록은 저장했습니다.'


def position_edit(parts,realized=None):
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
        action=parts[1]
        if action=='time' and len(parts)>=4:
            try: corrected=execution_time(' '.join(parts[3:]),datetime.now(timezone.utc))
            except ValueError: return 'KST 기준 과거 시각을 YYYY-MM-DD HH:MM으로 입력하세요.'
            p.update(execution_time=corrected,execution_time_source='user_reported')
        elif action in ('add','margin','leverage'):
            leverage=p.get('leverage')
            margin=p.get('margin')
            notional=p.get('position_notional') or (margin*leverage if margin and leverage else None)
            if notional is None or margin is None or leverage is None:
                return '기존 증거금과 배수가 필요합니다. 먼저 등록 정보를 수정하세요.'
            if action=='leverage' and len(parts)==4:
                replacement=float(parts[3])
                if not 1<=replacement<=125: return '배수는 1~125 범위로 입력하세요.'
                p.update(leverage=replacement,position_notional=notional)
            elif action in ('add','margin'):
                start=4 if action=='add' else 3
                raw=' '.join(parts[start:])
                amount=MONEY.fullmatch(raw)
                if amount:
                    extra,currency=money_value(amount['value'],amount['unit'])
                elif action=='add' and re.fullmatch(NUMBER,raw):
                    extra,currency=float(raw.replace(',','')),'KRW'
                else: return '금액과 통화를 입력하세요. 예: 500 USDT'
                if currency!=p.get('currency','KRW'): return '등록 증거금과 같은 통화를 사용하세요.'
                if not math.isfinite(extra) or extra<=0: return '금액은 양수여야 합니다.'
                if action=='add':
                    price=float(parts[3].replace(',',''))
                    if not math.isfinite(price) or price<=0: return '가격은 양수여야 합니다.'
                    increment=extra*leverage
                    p.update(entry=(notional+increment)/(notional/p['entry']+increment/price),exposure_changed=True)
                    notional+=increment
                p.update(margin=margin+extra,amount=margin+extra,amount_kind='margin',position_notional=notional)
            else: return '/pos leverage 번호 배수'
        elif parts[1]=='levels' and len(parts)==4:
            levels={k:v for k,v in parse(parts[3],{}).items() if k in ('target','stop')}
            if not levels or not all(math.isfinite(v) and v>0 for v in levels.values()): return '양의 익절가 또는 손절가를 입력하세요.'
            p.update(levels)
        elif parts[1] in ('reduce','close'):
            fraction=1. if parts[1]=='close' else float(parts[3])/100 if len(parts)==4 else 0.
            if not 0<fraction<=1: return '청산 비율은 0 초과 100 이하입니다.'
            if p.get('margin') is not None: p['margin']*=1-fraction
            if p.get('amount') is not None: p['amount']*=1-fraction
            if p.get('position_notional') is not None: p['position_notional']*=1-fraction
            p['exposure_changed']=True
            if fraction==1:
                active.remove(p)
                conn.execute("UPDATE outbox SET suppressed=1 WHERE sent IS NULL AND instr(body,?)>0",(f'등록 #{ident} ',))
        else: return '/pos add 번호 가격 금액 통화 · /pos margin 번호 금액 통화 · /pos leverage 번호 배수 · /pos time 번호 날짜 시각 · /pos reduce 번호 비율 · /pos close 번호'
        event=dict(at=time.time(),operation=parts[1],before=previous,after=dict(p))
        if realized is not None: event['realized_pnl']=realized
        history.append(event)
        conn.executemany('INSERT OR REPLACE INTO kv VALUES (?,?)',
                         [('positions',json.dumps(active,ensure_ascii=False)),('position_history',json.dumps(history,ensure_ascii=False))])
    if parts[1]=='close':
        reply=f'등록 #{ident} 종료 기록을 저장했습니다. 일반 시장 감시는 계속됩니다.'
        if realized is not None: reply+=f" 입력 실현 손익 {realized['amount']:+,.2f} {realized['currency']}"
        return reply
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


def manual_assessment(m,c):
    """One vote per independent evidence group, with regime-aware vetoes."""
    side=c['side']
    f=m['indicators']
    ht=m.get('market_direction')
    measures={
        '추세': 1 if ht==('상승' if side==1 else '하락') else -1 if ht in ('상승','하락') else 0,
        '구조': side*m.get('structure_break',0),
        '수급': side*(f['f15m']['flow']+f['f5m']['flow'])/.12,
        '모멘텀': side*(f['f15m']['r6'] or 0)/max(f['f15m']['atr'],.001),
    }
    supporting=[k for k,v in measures.items() if v>=1]
    opposing=[k for k,v in measures.items() if v<=-1]
    if measures['추세']<0:
        status='conditional' if len(supporting)>=2 else 'watch'
    elif ht=='횡보':
        status='entry' if c['confirmed'] and measures['수급']>=1 and not opposing and c['entry_status']=='entry' else 'conditional' if supporting else 'watch'
    else:
        status='entry' if c['entry_status']=='entry' and len(supporting)>=2 and not opposing else 'conditional' if supporting else 'watch'
    return status,supporting,opposing


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
    funding_stress=max(.0001,abs(m.get('funding') or 0.))
    gain=gain-funding_stress if gain is not None else None
    loss+=funding_stress
    lev=context.get('leverage',pr['leverage'] if pr else 0)
    margin=context.get('margin',pr['cap'] if pr else 0)
    mode=get('format','normal')
    label='롱' if side==1 else '숏'
    sym=m['symbol'].removesuffix('USDT')
    status=c['entry_status']
    manual='manual_indicators' in m and event is None
    supporting=[]; opposing=[]
    if manual: status,supporting,opposing=manual_assessment(m,c)
    if context.get('reservation'):
        status='watch' if context['reservation']=='비추천' else 'conditional'
        c=dict(c,trigger=context['reservation_trigger'],invalidation=context['reservation_invalidation'])
    title=('조건부 진입 후보' if status=='conditional' else '즉시 검토 후보' if status=='entry' else '관찰 후보')
    icon='🟢' if side==1 else '🔴'
    rows=[RULE,f'{icon} {sym} {label} {title}',
          '판단 · '+('진입 검토' if status=='entry' else '조건 확인 후 대기' if status=='conditional' else '관망'),
          '핵심 · '+(' / '.join(reason(a,m) for a,_ in c['reasons'][:2]) or '뚜렷한 근거 없음'),
          '위험 · '+c['invalidation']]
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
    rows.extend(['','💰 예상 손익 · 증거금 기준 · 펀딩 1회 가정',f'• 익절 시: {money(gain)}',f'• 손절 시: {money(-loss)}',
                 f'• 손익비: {ratio(rr)}','',RULE,f"{'📈' if side==1 else '📉'} {label} 진입 근거"])
    def evidence(items):
        n=1 if mode=='simple' else len(items) if mode=='detailed' else 3
        return ['• '+reason(a,m) for a,_ in items[:n]] or ['• 뚜렷한 근거 없음']
    if manual:
        rows.extend([f"• 지지 {fmt(m['support'])} / 저항 {fmt(m['resistance'])}",
                     f"• 15분 ATR {m['indicators']['f15m']['atr']:.2%} · 선물 순공격 수급 {m['indicators']['f15m']['flow']:+.1%}"])
        rows.append('• 독립 근거: '+(', '.join(supporting) or '부족')+' / 충돌: '+(', '.join(opposing) or '뚜렷하지 않음'))
        if status=='conditional' and c['entry_status']=='entry':
            rows.append('• 대기 기준: 상위 방향과 구조·수급 일치 재확인.')
        if m.get('manual_change'): rows.append('• 이전 분석 대비: '+m['manual_change'])
        if mode=='detailed':
            for group,values in m['manual_indicators'].items():
                displayed=', '.join(f'{k}: {fmt(v,6)}' for k,v in values.items())
                rows.append(f'• {group} (참고용): {displayed}')
            rows.append('• CVD는 15분봉 집계의 24봉 변화량이며 체결장 원본 시계열과 구분합니다.')
    rows.extend(evidence(c['reasons']))
    rows.extend(['','⚠️ 반대 근거 및 위험',*evidence(c['conflicts']),'• '+risk,'','⏳ 진입 조건',
                 c['trigger'] or ('상위 방향과 구조·수급 일치 재확인' if manual and status!='entry' else '현재 완료 봉에서 직접 검토할 후보'),
                 '', '❌ 추천 무효화 조건',c['invalidation']])
    if not pr or pr['cap']<1_000_000:
        rows.append('• 100만 원 미만: '+('계좌의 기존 증거금 비중 상한으로 100만 원 배정 불가.' if seed<5_000_000 else
            '등록 포지션 반영 후 손절 예산·청산 여유·증거금 상한을 동시에 통과하는 100만 원 설정 없음.'))
    if context.get('margin') and not any(r['leverage']==lev and r['cap']>=margin for r in alternatives):
        rows.append('• 입력한 증거금은 자동 추천 위험 범위를 넘는 가정입니다.')
    if mode=='detailed' and alternatives:
        rows.append('📊 증거금 비교 · 비용 및 펀딩 1회 스트레스 반영')
        for amount in (500000,1000000,1500000):
            row=max(alternatives,key=lambda r:(r['cap']>=amount,r['cap'],-r['leverage']))
            l=row['leverage']; notional=amount*l; loss_krw=notional*row['loss_ratio']
            rows.append(f"• {fmt(amount,0)}원 / {l}배 · 명목 {fmt(notional,0)}원 · 익절 {fmt(notional*gain if gain is not None else None,0)}원 · 손절 -{fmt(loss_krw,0)}원 ({loss_krw/seed:.2%}) · 청산 {fmt(row['model_liquidation'])} · 스탑 여유 {row['stop_to_liquidation_ratio']:.2%} · "+('가능' if row['cap']>=amount else '위험·증거금 한도 초과'))
    imm=c['immediate_plan']
    if mode!='simple':
        h=c['historical']
        if 'manual_indicators' in m and event is None and target:
            h=live.analog(m['symbol'],m['indicators']['f1h'],side,abs(stop/entry-1),abs(target/entry-1),
                          m.get('funding'),m.get('oi_change'),entry_offset=entry/m['completed_1m_close']-1)
        n=h.get('sample',0); hits=h.get('target_hits',0)
        rows.extend(['',RULE,'📚 과거 유사 사례',
            f'• 목표 선도달: {hits}/{n}건 ({hits/n:.1%})' if n else '• 비교 표본 없음',
            f'• 손익분기 승률: {loss/(loss+gain):.1%}' if gain is not None and gain>0 else '• 손익분기 승률: 계산 불가',
            '• 과거 관측치이며 미래 승률과 다릅니다.'+(' 표본이 적어 불확실합니다.' if n<50 else ''),
            f"• 현재가 진입안: {fmt(imm['entry'])} · 손익비 {ratio(imm['payoff'])}"])
        if 'unfilled' in h:
            rows.append(f"• 손절 선도달 {h['stop_hits']} · 미도달 {h['unresolved']} · 미체결 {h['unfilled']}")
            rows.append(f"• 최신 비교 사례 {h['newest_utc'] or '없음'} · 시간봉 보수 체결 · 과거 펀딩 미반영")
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

def reservation_review(m,fields,seed,stamp):
    entry,side=fields['entry'],fields['side']
    current=m['price']
    c=next(x for x in m['directions'] if x['side']==side)
    support,resistance=m['support'],m['resistance']
    atr=max(.001,c['atr_ratio'])
    level=support if side==1 else resistance
    offset=entry/current-1
    marketable=side*offset>=0
    near=abs(entry/level-1)<=max(.002,atr)
    opposing=m.get('market_direction')==('하락' if side==1 else '상승')
    status='비추천' if marketable or (opposing and not near) else '조건부 가능' if not c['confirmed'] or opposing or not near else '예약 적절'
    stop=fields.get('stop',entry-side*max(entry*atr,abs(entry-level)+entry*atr*.25) if near else entry*(1-side*atr*1.5))
    opposite_level=resistance if side==1 else support
    target=fields.get('target',opposite_level if side*(opposite_level-entry)>entry*atr else entry*(1+side*atr*2))
    if side*(entry-stop)<=0 or side*(target-entry)<=0 or min(stop,target)<=0:
        status='비추천'
    trigger=('현재가 기준 주문이 즉시 체결될 수 있어 대기 가격을 재검토하세요.' if marketable else
             '예약가 부근에서 '+('지지 회복 및 매수 우세' if side==1 else '저항 유지 및 매도 우세')+
             '를 확인하세요. 가격 도달만으로 체결을 확인할 수 없습니다.')
    invalidation=f"{fmt(stop)} {'하향' if side==1 else '상향'} 이탈 또는 상위 구조·수급의 반대 방향 전환"
    plan=dict(fields,stop=stop,target=target,reservation=status,
              reservation_trigger=trigger,reservation_invalidation=invalidation)
    costs=2*live.FEE+max(.0001,abs(m.get('funding') or 0.))
    limit_rr=(side*(target/entry-1)-costs)/(abs(stop/entry-1)+costs+live.SLIP)
    immediate=c.get('immediate_plan',{}).get('payoff')
    waiting=c.get('entry')
    alternative=(f"대안 · 엔진의 조건부 진입 기준 {fmt(waiting)} USDT" if waiting and abs(waiting/entry-1)>.001 and side*(waiting-current)<0 else
                 '대안 · 구조·수급 확인 전까지 관망')
    comparison=(f"예약 손익비 {fmt(limit_rr)} / 현재가 시나리오 손익비 {fmt(immediate)} · 각각의 손절·목표 기준")
    if side*(entry-stop)<=0 or side*(target-entry)<=0 or min(stop,target)<=0:
        comparison='요청한 익절·손절 구조가 부적합해 손익비 비교를 보류합니다.'
    return '\n'.join([
        f"📌 {m['symbol']} {'롱' if side==1 else '숏'} 예약 판단 · {status}",
        f"현재 {fmt(current)} → 예약 {fmt(entry)} USDT ({offset:+.2%})",
        f"지지 {fmt(support)} / 저항 {fmt(resistance)} · {'인접 구조 있음' if near else '인접 구조 미확인'}",
        '예약 조건 · '+trigger,
        '취소 기준 · '+invalidation,
        comparison,alternative,
        review(m,plan,seed,stamp),
    ])


def manual_report(symbols):
    timing=getattr(REPLY_TIMING,'value',None)
    data_spans=[]
    def fetch(host,path,**params):
        start=time.monotonic()
        try:
            return monitor_api(host,path,fetch_rest=manual_rest,**params)
        finally:
            data_spans.append((start,time.monotonic()))
    start=time.monotonic()
    try:
        return live.tick(read_only=True,market_api=fetch,symbols=symbols)
    finally:
        if timing is not None and data_spans:
            data=max(end for _,end in data_spans)-start
            timing['market_data_s']=round(data,3)
            timing['market_compute_s']=round(max(0.,time.monotonic()-start-data),3)


def manual_market(symbol):
    report=manual_report((symbol,))
    stamp=report['completed_through_utc']
    if abs(time.time()-datetime.fromisoformat(stamp).timestamp())>150:
        return None,stamp
    return next((m for m in report['market'] if m['symbol']==symbol),None),stamp


REPLY_TIMING=threading.local()


def latest():
    return json.loads(LIVE.read_text(encoding='utf-8')) if LIVE.exists() else {}

def handle(message,identity='cmd'):
    low=message.strip().lower()
    if low in ('/version','/update') or low.startswith('/update '): return autoupdate.command(low)
    prior=get('context/'+identity,{})
    context=parse(message,prior)
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
    if low=='/help': return HELP+'\n등록: 이더 숏 진입가 1750 · 거래종료: 이더 거래종료 /pos close 번호 · 예약 분석: 이더 1700 롱 예약 · 상태: /pos'+'\n/pos add 번호 가격 금액 통화 · /pos margin 번호 금액 통화 · /pos leverage 번호 배수 · /pos time 번호 날짜 시각 · /pos reduce 번호 비율 · /pos close 번호'
    if low=='/status': return json.dumps(dict(get('status',{}),market_monitor='on',managed_positions=len(get('positions',[]))),ensure_ascii=False)
    if low=='/paper':
        accounts=latest().get('paper',{}).get('accounts',{})
        if not accounts: return '📊 모의계좌 성과\n모의 기록 없음'
        return '📊 모의계좌 성과\n\n'+'\n\n'.join(
            f"💰 {name}\n• 평가액: {fmt(a['equity_krw'],0)}원\n• 대기 {a['counts'].get('pending',0)}건 / 보유 {a['counts'].get('open',0)}건 / 청산 {a['counts'].get('closed',0)}건"
            for name,a in accounts.items())

    positions=get('positions',[])
    special=trade_command(message,identity,prior,positions,seed)
    if special is not None: return special
    if low.startswith('/pos ') and len(low.split())>=3: return position_edit(low.split())
    if low.startswith('/close '): return position_edit(['/pos','close',low.split()[1]])
    if low.startswith('/levels '):
        return position_edit(['/pos','levels',low.split()[1],low])
    position_query=low in ('/pos','/positions') or bool(re.search(r'보유\s*중|가지고 있어|내\s*포지션|포지션 (?:확인|분석|상태)',low))
    if position_query and requested_symbols(low):
        positions=[p for p in positions if p['symbol'] in requested_symbols(low)]
    symbols=tuple(dict.fromkeys(p['symbol'] for p in positions)) if position_query else query_symbols(message,context)
    if position_query and not symbols: return '등록 포지션이 없습니다.'
    report=manual_report(symbols)
    end=datetime.fromisoformat(report['completed_through_utc'])
    if abs((datetime.now(timezone.utc)-end).total_seconds())>150:
        return '최신 완료 봉 시각을 검증하지 못해 분석을 보류합니다.'
    markets={m['symbol']:m for m in report['market']}
    if position_query:
        analysis_time=datetime.now(timezone.utc).isoformat()
        rows=[]
        for p in positions:
            m=markets.get(p['symbol'])
            if not m:
                rows.append(f"등록 #{p['id']} {p['symbol']} · 최신 자료 부족으로 판단 유보")
                continue
            rows.append(held_review(dict(p,analysis_time=analysis_time),m,seed,report['completed_through_utc']))
        return '\n\n'.join(rows) or '등록 포지션이 없거나 최신 데이터가 비었습니다.'
    put('context/'+identity,context)
    if low.startswith('/simulate'):
        m=markets.get(context.get('symbol'))
        if not m or context.get('side') not in (-1,1): return '예: /simulate eth short'
        if any(k in context for k in ('entry','margin','leverage','stop','target')):
            return '수동 모의 주문은 엔진 기본 가격과 위험 설정을 사용합니다. /simulate eth short default 로 명시적으로 요청하세요.' if ' default' not in low else queue_manual(m,context['side'])
        return queue_manual(m,context['side'])
    query=[markets[sym] for sym in symbols if sym in markets]
    single=len(symbols)==1
    if single and not query:
        return f'{symbols[0]} 최신 완료 봉을 확보하지 못했습니다.'
    for market in query:
        key='manual-view/'+identity+'/'+market['symbol']
        old=get(key,{})
        direction=market.get('market_direction','불확실')
        current=dict(direction=direction,status=market['candidate']['entry_status'])
        if old and old!=current:
            market['manual_change']=f"{old['direction']} / {old['status']} → {direction} / {current['status']}"
        put(key,current)
    note='\n익절가를 높일 경우 비교할 가격을 입력하세요. 예: 익절가 1750' if '익절가를 더' in low else ''
    if '아까' in low or '아직 유효' in low:
        note+='\n이전 제안과 비교할 때는 기존 진입가·손절가를 입력하세요. 현재 양방향 근거와 확인 조건을 다시 계산했습니다.'
    return ('\n\n'.join(review(m,context if single else {},seed,report['completed_through_utc']) for m in query) or '최신 분봉을 확보하지 못했습니다.')+note

def queue_manual(m,side):
    ident=secrets.token_hex(6)
    put('manual_request',dict(id=ident,symbol=m['symbol'],side=side,expires=int(time.time()*1000)+180000))
    return f'수동 모의 요청 {ident} 접수. 다음 완료 봉에서 위험·손익비를 검증하며 체결 보장은 없습니다.'

def respond(message,identity,reply):
    try: reply(handle(message,identity))
    except Exception as ex:
        LOG.warning('analysis failed: %s',type(ex).__name__)
        reply('분석 실패. 입력 형식과 데이터 연결을 확인하세요.')

def receive(stop,token,chat,manual):
    offset=get('telegram_offset',0)
    while not stop.is_set():
        started=time.monotonic()
        try:
            updates=tg(token,'getUpdates',dict(offset=offset,timeout=20,allowed_updates=['message']))
            received=time.monotonic()
            received_epoch=time.time()
            for u in updates:
                msg=u.get('message',{})
                valid=(str(msg.get('chat',{}).get('id'))==str(chat) and msg.get('chat',{}).get('type')=='private'
                       and time.time()-msg.get('date',0)<=180 and msg.get('text'))
                if valid:
                    manual.put((msg['text'],dict(update=u['update_id'],received=received,received_epoch=received_epoch,
                                                  poll_s=round(received-started,3),
                                                  telegram_age_s=round(time.time()-msg['date'],3))))
                offset=u['update_id']+1
                put('telegram_offset',offset)
        except Exception as ex:
            LOG.warning('Telegram receive failed: %s',type(ex).__name__)
            stop.wait(3)


def manual_worker(stop,chat,incoming,outgoing):
    while not stop.is_set():
        try: message,metrics=incoming.get(timeout=.5)
        except Empty: continue
        started=time.monotonic()
        metrics['queue_s']=round(started-metrics['received'],3)
        REPLY_TIMING.value=metrics
        def enqueue(body):
            metrics['process_s']=round(time.monotonic()-started,3)
            outgoing.put((body,metrics,time.monotonic()))
        try:
            respond(message,'telegram/'+str(chat),enqueue)
        finally:
            REPLY_TIMING.value=None


def manual_sender(stop,token,chat,outgoing):
    while not stop.is_set():
        try: body,metrics,queued=outgoing.get(timeout=.5)
        except Empty: continue
        started=time.monotonic()
        metrics['send_queue_s']=round(started-queued,3)
        try:
            for part in payloads(chat,body):
                try:
                    tg(token,'sendMessage',part)
                except HTTPError as ex:
                    if ex.code!=429: raise
                    try: delay=max(1,int(ex.headers.get('Retry-After',1)))
                    except ValueError: delay=1
                    if delay>5 or stop.wait(delay): raise
                    tg(token,'sendMessage',part)
            metrics['delivered']=True
        except Exception as ex:
            metrics['delivered']=False
            LOG.warning('Telegram manual send failed: %s',type(ex).__name__)
        finally:
            metrics['send_s']=round(time.monotonic()-started,3)
            metrics['total_s']=round(time.monotonic()-metrics.pop('received'),3)
            LOG.info('telegram_manual %s',json.dumps(metrics,sort_keys=True))


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
            observed=f"held-context/{p['id']}"
            previous_held=get(observed,{})
            if previous_held.get('stamp')==stamp and stamp:
                continue
            _,trend_values=holding_trend(m,p['side'])
            evidence={line.split()[1] if line.startswith('15분 ') else line.split()[0]:signal
                      for signal,line in holding_evidence(m,p['side'])}
            mark=m.get('position_mark_price',m.get('mark_price'))
            atr=next(row['atr_ratio'] for row in m['directions'] if row['side']==p['side'])
            buffer=max(.005,min(.02,atr*2)) if p.get('leverage',0) and p['leverage']>=25 else .005
            plan=protection_plan(p,m,mark or m['price'],atr,position_liquidation(p),buffer)
            state,body=position_review(p,m,0,'simple',stamp,prior=previous_held,plan=plan)
            key=f"user/{p['id']}/thesis"
            if state!='유지':
                details=body.splitlines()
                adjustments=[line for line in details if '조정 검토 ' in line]
                notify(key,state,'\n'.join(line for line in details if line not in adjustments),detected,cooldown=1800,force=state in ('스탑 도달','청산 근접','추세·구조 동시 역행'))
                if adjustments:
                    notify(f"user/{p['id']}/adjust",'|'.join(line.split(' · ',1)[0] for line in adjustments),
                           f"등록 #{p['id']} 가격 조정 검토 · {state}\n"+'\n'.join(adjustments),detected,cooldown=1800)
            prior_stop=previous_held.get('stop')
            current_stop=plan.get('stop')
            changed_structure=(m.get('structure_break') in (-1,1) or
                               bool(previous_held.get('trend') and previous_held['trend']!=trend_values))
            if (changed_structure and prior_stop is not None and current_stop is not None
                    and p['side']*(current_stop-prior_stop)>=m['price']*max(.003,atr)):
                notify(f"user/{p['id']}/plan",str(current_stop),
                       f"등록 #{p['id']} 구조 손절 후보 {fmt(prior_stop)} → {fmt(current_stop)} USDT\n"
                       f"{plan['stop_reason']} · 이전 후보 대비 위험 축소 방향 · 미설정",detected,cooldown=1800)
            summary=dict(stamp=stamp,trend=trend_values,direction=m.get('market_direction'),evidence=evidence,stop=current_stop)
            prior_summary={k:v for k,v in previous_held.items() if k!='previous'}
            if prior_summary:
                summary['previous']=prior_summary
            put(observed,summary)
        direction_key='last-alert-direction/'+m['symbol']
        last_direction=get(direction_key)
        for c in m['directions']:
            key=f"idea/{m['symbol']}/{c['side']}"
            old=get(key,{})
            supported,evidence=signal_context(m,c)
            aligned=m.get('market_direction')==('상승' if c['side']==1 else '하락')
            reversing=last_direction is not None and last_direction!=c['side']
            reversal_ok=(aligned and len(evidence)>=2) or (m.get('structure_break')==c['side'] and len(evidence)>=2)
            active=c['candidate_visible'] and supported and (not reversing or reversal_ok)
            structure=m.get('market_direction','불확실')
            current=dict(active=active,status=c['entry_status'],stop=c['stop'],
                         direction=structure,evidence=sorted(evidence))
            invalid=bool(old.get('active') and not active and old.get('stop') and
                         c['side']*(m['completed_1m_close']-old['stop'])<=0)
            conflict=structure!=('상승' if c['side']==1 else '하락') and structure!='횡보'
            confirmed=bool(active and not conflict and c['entry_status']=='entry' and old.get('status')!='entry')
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
                if conflict:
                    shown=dict(c,entry_status='conditional',grade='상위 시간대 충돌 · 조건부',
                               trigger='15분 구조 돌파 유지와 상위 시간대 방향 재확인')
                    display=dict(m,directions=[shown if x['side']==c['side'] else x for x in m['directions']])
                else:
                    display=m
                notify(key,signature,review(display,dict(side=c['side']),get('seed',3_000_000),stamp,label),
                       detected,cooldown=1800,force=bool(confirmed or shifted))
                put(direction_key,c['side'])
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

def manual_rest(host,path,**query):
    required=path in ('/fapi/v1/time','/fapi/v1/klines')
    return ORIGINAL_API(host,path,timeout=10 if required else 5,**query)

def monitor_api(host,path,*,fetch_rest=ORIGINAL_API,**query):
    if path!='/fapi/v1/klines' or 'startTime' in query or 'endTime' in query or query.get('interval') not in ('1m','5m','15m'):
        return fetch_rest(host,path,**query)
    key=(query['symbol'],query['interval'])
    interval=int(key[1][:-1])*60_000
    end=(int(time.time()*1000)//interval)*interval
    with WS_LOCK:
        rows=list(WS_BARS.get(key,{}).values())
    rows=sorted((x for x in rows if x[0]<end),key=lambda x:x[0])[-query['limit']:]
    if len(rows)==query['limit'] and rows[-1][0]==end-interval and all(b[0]-a[0]==interval for a,b in zip(rows,rows[1:])):
        return rows
    result=fetch_rest(host,path,**query)
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
                notify(f"user/{p['id']}/{k}",str(level),f"등록 #{p['id']} 위험 확인 · {label} {fmt(level)} 도달 · 현재 {fmt(price)} · 거래소 체결 미확인",at)
        lev=p.get('leverage'); side=p['side']
        if not mark or not lev: continue
        liq=position_liquidation(p)
        if liq is not None and side*(price-liq)/price<(.01 if lev>=25 else .005):
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
        manual=Queue()
        replies=Queue()
        jobs.extend([('telegram receive',lambda:receive(stop,token,chat,manual)),
                     ('telegram manual',lambda:manual_worker(stop,chat,manual,replies)),
                     ('telegram reply',lambda:manual_sender(stop,token,chat,replies)),
                     ('telegram send',lambda:sender(stop,token,chat))])
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
