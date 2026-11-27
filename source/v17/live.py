"""Public market dashboard and virtual fills. No authenticated routes."""
import argparse
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
import hashlib
import json
import math
import logging
import threading
from urllib.error import HTTPError
from pathlib import Path
import time
from urllib.request import urlopen
from urllib.parse import urlencode
import numpy as np
import pandas as pd
from growth import ACCOUNTS, profiles
from policy import size, exposure, confirm, historical_compare, TOTAL, DIRECTION, SYMBOL, MARGIN

ROOT=Path(__file__).resolve().parent
OUT=ROOT/'results'
FUT='https://fapi.binance.com'
SPOT='https://api.binance.com'
SYMS=('BTCUSDT','ETHUSDT')
MIN=60000
FEE=.0005
SLIP=.0005
PEN=.0001
INITIAL=3_000_000.
ANALOG=ROOT/'analogs.npz'

API_LOCK=threading.Lock()
API_COOLDOWN={}
LOG=logging.getLogger('v18')

def api(host,path,*,timeout=12,**q):
    with API_LOCK:
        if time.monotonic()<API_COOLDOWN.get(host,0):
            raise OSError(f'{path}: HTTP rate-limit cooldown')
    try:
        with urlopen(host+path+'?'+urlencode(q),timeout=timeout) as f:
            result=json.load(f)
    except HTTPError as exc:
        if exc.code in (418,429):
            try: wait=max(1,float(exc.headers.get('Retry-After',60)))
            except (TypeError,ValueError): wait=60
            with API_LOCK: API_COOLDOWN[host]=time.monotonic()+wait
        raise
    if isinstance(result,dict) and isinstance(result.get('code'),int) and result['code']<0:
        raise ValueError(f"Binance {path}: code {result['code']}")
    return result

def finite(value):
    if isinstance(value,dict): return {k:finite(v) for k,v in value.items()}
    if isinstance(value,(list,tuple)): return [finite(v) for v in value]
    if isinstance(value,(float,np.floating)) and not math.isfinite(value): return None
    return value

def optional(raw,key,parse):
    try:
        val=parse(raw[key])
        return finite(val)
    except (KeyError,IndexError,ValueError,TypeError,ZeroDivisionError,AttributeError) as exc:
        if key in raw: LOG.warning('symbol=%s function=analyze API=%s %s: %s',raw.get('_symbol'),key,type(exc).__name__,exc)
        return None

def tasks(sym):
    d={f'f{t}':(FUT,'/fapi/v1/klines',dict(symbol=sym,interval=t,limit=180)) for t in ('1m','5m','15m','1h','4h','1d')}
    d['s15m']=(SPOT,'/api/v3/klines',dict(symbol=sym,interval='15m',limit=96))
    for name,path,kwargs in [
        ('funding','/fapi/v1/premiumIndex',{}),('oi','/futures/data/openInterestHist',dict(period='15m',limit=9)),
        ('crowd','/futures/data/globalLongShortAccountRatio',dict(period='15m',limit=2)),
        ('book','/fapi/v1/depth',dict(limit=20)),('ticker','/fapi/v1/ticker/24hr',{})]:
        d[name]=(FUT,path,dict(symbol=sym,**kwargs))
    return d

def frame(raw,end):
    if not isinstance(raw,list): raise ValueError('klines response must be a list')
    rows={int(x[0]):x for x in raw if len(x)>=10 and int(x[6])<end}
    if not rows: raise ValueError('no completed candles')
    a=np.asarray([rows[k][:10] for k in sorted(rows)],dtype='float64')
    if not np.isfinite(a).all() or (a[:,1:5]<=0).any(): raise ValueError('invalid candle values')
    return pd.DataFrame(dict(t=a[:,0].astype('int64'),o=a[:,1],h=a[:,2],l=a[:,3],c=a[:,4],v=a[:,5],buy=a[:,9]))

def features(f):
    c=f.c; p=c.shift()
    atr=pd.concat([f.h-f.l,(f.h-p).abs(),(f.l-p).abs()],axis=1).max(axis=1).rolling(14).mean()
    e=lambda n:c.ewm(span=n,adjust=False).mean()
    d=c.diff(); up=d.clip(lower=0).ewm(alpha=1/14,adjust=False).mean(); down=(-d.clip(upper=0)).ewm(alpha=1/14,adjust=False).mean()
    mac=e(12)-e(26)
    typ=(f.h+f.l+c)/3
    return pd.DataFrame(dict(r6=c.pct_change(6),r24=c.pct_change(24),ema=e(20)/e(50)-1,atr=atr/c,
                             volume=f.v.rolling(6).sum()/f.v.rolling(48).mean()/6,
                             flow=(2*f.buy-f.v).rolling(6).sum()/f.v.rolling(6).sum(),
                             rsi=100-100/(1+up/down),macd=(mac-mac.ewm(span=9,adjust=False).mean())/c,
                             vwap=(typ*f.v).rolling(24).sum()/f.v.rolling(24).sum()))

def structure(f):
    h=f.h.to_numpy(); l=f.l.to_numpy()
    return ([float(l[i]) for i in range(2,len(f)-2) if l[i]==min(l[i-2:i+3])],
            [float(h[i]) for i in range(2,len(f)-2) if h[i]==max(h[i-2:i+3])])

def analog_path(path,entry,side,stop,target):
    filled=False
    for hour,(hi,lo,_) in enumerate(path,1):
        adverse=lo if side==1 else hi
        favorable=hi if side==1 else lo
        fresh=not filled
        if fresh:
            if hour>3: break
            if side*(entry-adverse)<=entry*PEN: continue
            filled=True
        if side*(entry-adverse)>=entry*stop:
            # Opens and tick order are absent: use the adverse extreme for stops.
            return 'stop_hits',side*(adverse*(1-side*SLIP)/entry-1)-2*FEE
        if not fresh and side*(favorable-entry)>entry*(target+PEN):
            return 'target_hits',target-2*FEE
    return ('unresolved' if filled else 'unfilled'),None

def analog(sym,snapshot,side,stop,target,current_funding=None,current_oi=None,entry_offset=None):
    if not ANALOG.exists(): return {'status':'archive unavailable'}
    with np.load(ANALOG) as z:
        f=z[sym+'_features']; p=z[sym+'_prices']; clock=z[sym+'_clock']
        historical_funding=z[sym+'_funding'] if sym+'_funding' in z else np.full(len(f),np.nan)
        historical_oi=z[sym+'_oi_change'] if sym+'_oi_change' in z else np.full(len(f),np.nan)
    current=np.array([snapshot[k] for k in ('r6','r24','ema','atr','volume','flow')])
    centers=np.nanmedian(f,axis=0); scale=np.nanmedian(abs(f-centers),axis=0)
    dist=np.sqrt(np.mean(((f-current)/np.maximum(scale,1e-9))**2,axis=1))
    if current_funding is not None:
        # Modest adjustment, retaining all eras where the rate is absent.
        adj=np.minimum(2.,abs(historical_funding-current_funding)/.00015)
        dist+=.12*np.where(np.isfinite(adj),adj,1.)
    if current_oi is not None:
        adj=np.minimum(2.,abs(historical_oi-current_oi)/.015)
        dist+=.12*np.where(np.isfinite(adj),adj,1.)
    dist[-48:]=np.nan
    if entry_offset is not None:
        for col,threshold in ((2,.001),(5,.06)):
            if abs(current[col])>=threshold:
                dist[np.sign(f[:,col])!=np.sign(current[col])]=np.nan
    chosen=[]
    for i in np.argsort(dist):
        if not np.isfinite(dist[i]) or dist[i]>2.5: break
        if all(abs(i-j)>48 for j in chosen): chosen.append(i)
        if len(chosen)==30: break
    if entry_offset is not None:
        counts=dict(target_hits=0,stop_hits=0,unresolved=0,unfilled=0)
        resolved=[]
        for i in chosen:
            label,result=analog_path(p[i+1:i+49],p[i,2]*(1+entry_offset),side,stop,target)
            counts[label]+=1
            if result is not None: resolved.append(result)
        return dict(sample=len(chosen),**counts,mean_resolved_net=float(np.mean(resolved)) if resolved else None,
                    newest_utc=datetime.fromtimestamp(int(clock[max(chosen)])/1000,timezone.utc).date().isoformat() if chosen else None)
    outcomes=[]
    for i in chosen:
        close=p[i,2]; adverse=0.; end='unresolved'
        for h,(hi,lo,c) in enumerate(p[i+1:i+49],1):
            worst=lo if side==1 else hi
            best=hi if side==1 else lo
            adverse=max(adverse,-side*(worst/close-1))
            if -side*(worst/close-1)>=stop: end='stop'; break
            if side*(best/close-1)>target+PEN: end='target'; break
        outcomes.append((end,h,adverse,float(side*(p[i+48,2]/close-1))))
    return dict(sample=len(outcomes),target_hits=sum(x[0]=='target' for x in outcomes),
                stop_hits=sum(x[0]=='stop' for x in outcomes),unresolved=sum(x[0]=='unresolved' for x in outcomes),
                mean_hours=float(np.mean([x[1] for x in outcomes])) if outcomes else None,
                median_max_adverse=float(np.median([x[2] for x in outcomes])) if outcomes else None,
                median_48h_directional_return=float(np.median([x[3] for x in outcomes])) if outcomes else None,
                closest_hours_utc=[datetime.fromtimestamp(int(clock[i])/1000,timezone.utc).isoformat() for i in chosen[:3]],
                funding_comparable=int(np.isfinite(historical_funding[chosen]).sum()) if current_funding is not None else 0,
                oi_comparable=int(np.isfinite(historical_oi[chosen]).sum()) if current_oi is not None else 0,
                method='1h trend, vol, volume, futures taker flow; supplemental funding/OI if available; 48h price paths')

def daily_trend(raw,end):
    day=frame(raw,end)
    step=1440*MIN
    if len(day)<50 or day.t.iloc[-1]!=end//step*step-step or not (day.t.diff().dropna()==step).all(): return None
    return float(features(day).ema.iloc[-1])

def market_direction(daily,four):
    if daily is None: return '불확실'
    if daily>.002 and four>.001: return '상승'
    if daily<-.002 and four<-.001: return '하락'
    if abs(daily)<=.002 and abs(four)<=.001: return '횡보'
    return '불확실'

def analyze(sym,raw,end,relative):
    bars={k:frame(raw[k],end) for k in ('f1m','f5m','f15m','f1h','f4h')}
    for key,b in bars.items():
        interval={'f1m':1,'f5m':5,'f15m':15,'f1h':60,'f4h':240}[key]*MIN
        if len(b)<50 or b.t.iloc[-1]!=end//interval*interval-interval or not (b.t.diff().dropna()==interval).all():
            raise ValueError(f'{key}: missing, stale or insufficient completed candles')
    f={k:features(b).iloc[-1].to_dict() for k,b in bars.items()}
    for row in f.values():
        if not math.isfinite(row['rsi']): row['rsi']=50.
        if not all(math.isfinite(value) for value in row.values()):
            raise ValueError('derived indicator is non-finite')
    daily=optional(raw,'f1d',lambda x: daily_trend(x,end))
    a=f['f15m']; short=f['f5m']; fast=f['f1m']; hourly=f['f1h']; longer=f['f4h']
    price=float(bars['f1m'].c.iloc[-1])
    current=optional(raw,'ticker',lambda x:float(x['lastPrice']) if abs(end-int(x['closeTime']))<=180000 else None) or price
    atr=price*a['atr']
    lo,hi=structure(bars['f15m'].iloc[-90:])
    hourly_low,hourly_high=structure(bars['f1h'].iloc[-48:])
    lo+=hourly_low; hi+=hourly_high
    support=sorted(set(x for x in lo if x<price-.15*atr))
    resist=sorted(set(x for x in hi if x>price+.15*atr))
    lower=support[-1] if support else price-atr
    upper=resist[0] if resist else price+atr
    spot_flow=optional(raw,'s15m',lambda x:float(features(frame(x,end)).flow.iloc[-1]))
    oi_delta=optional(raw,'oi',lambda x:float(x[-1]['sumOpenInterest'])/float(x[0]['sumOpenInterest'])-1 if len(x)>1 else None)
    funding=optional(raw,'funding',lambda x:float(x['lastFundingRate']))
    def order_imbalance(x):
        bid=sum(float(p)*float(q) for p,q in x['bids'])
        ask=sum(float(p)*float(q) for p,q in x['asks'])
        return (bid-ask)/(bid+ask)
    imbalance=optional(raw,'book',order_imbalance)
    candle=bars['f15m'].iloc[-1]
    wick=(min(candle.o,candle.c)-candle.l)-(candle.h-max(candle.o,candle.c))
    tail=float(wick/max(candle.h-candle.l,1e-10))
    hours=bars['f15m'].iloc[-96:]
    bin_width=max(atr*.5,price*.00025)
    volume_buckets=hours.v.groupby(np.floor(((hours.h+hours.l+hours.c)/3)/bin_width)).sum()
    profile_poc=float((volume_buckets.idxmax()+.5)*bin_width)
    crowd_ratio=optional(raw,'crowd',lambda x:float(x[-1]['longShortRatio']))
    if crowd_ratio is not None and crowd_ratio<=0: crowd_ratio=None
    impulse=a['r6']/max(a['atr'],1e-8)
    regime='급등' if impulse>4 else '급락' if impulse<-4 else '압축' if a['atr']<hourly['atr']*.32 and a['volume']<.8 else '추세' if abs(hourly['ema'])>.003 else '횡보'
    decisions=[]
    for side in (1,-1):
        pieces=[]
        def item(name,value): pieces.append((name,float(value)))
        item('1h·4h EMA 추세',side*float(np.clip((hourly['ema']+.5*longer['ema'])*190,-2,2))*(.5 if regime=='횡보' else 1.))
        item('5·15분 선물 공격 수급',side*float(np.clip((short['flow']+a['flow'])*7,-1.8,1.8)))
        item('1분 수급·5분 MACD',side*float(np.clip(fast['flow']*2.5+short['macd']*1200,-1,1)))
        item('15분 RSI·VWAP',side*float(np.clip((a['rsi']-50)/23+(price/a['vwap']-1)*60,-1,1)))
        if relative is not None:
            item('BTC 대비 상대강도' if sym=='ETHUSDT' else 'ETH 대비 상대강도',side*float(np.clip(relative*70,-.6,.6)))
        if spot_flow is not None: item('현물 수급 확인',side*float(np.clip(spot_flow*5,-1,1)))
        if oi_delta is not None: item('OI 증가와 가격 방향',side*float(np.clip(oi_delta*45*np.sign(a['r6']),-.6,.6)))
        if funding is not None: item('펀딩 쏠림/보유 비용',-side*float(np.clip(funding*4500,-.6,.6)))
        item('최근 15분 봉 윗꼬리·아랫꼬리',side*tail*.8)
        if crowd_ratio is not None:
            item('롱·숏 계좌 쏠림',-side*float(np.clip(np.log(crowd_ratio)*.35,-.4,.4)))
        if imbalance is not None: item('호가 20단계 스냅샷',side*float(np.clip(imbalance,-.5,.5)))
        if regime=='횡보':
            item('횡보 경계 반등·저항',side*float(np.clip((upper+lower-2*price)/max(upper-lower,atr),-1,1))*1.6)
        if regime=='압축':
            item('압축 이탈 확인',side*float(np.clip((price-a['vwap'])/atr,-1.,1.))*.75)
        if regime in ('급등','급락'):
            item('추격 변동성',-.8 if side*impulse>0 else -.45)
        item('최근 거래량과 가격 방향',side*float(np.clip((a['volume']-1)*np.sign(a['r6']),-.6,.6)))
        item('지지·저항 접근',side*float(np.clip((upper+lower-2*price)/atr,-1,1))*.55)
        score=sum(v for _,v in pieces)
        entry=(max(lower,price-.55*atr) if side==1 else min(upper,price+.55*atr))
        stop=(lower-.42*atr if side==1 else upper+.42*atr)
        targets=resist if side==1 else support[::-1]
        viable=[x-side*.12*atr for x in targets if abs(x-entry)>1.45*abs(entry-stop)]
        tp=viable[0] if viable else None
        grade='강한 진입 후보' if score>=4 else '일반 진입 후보' if score>=2.5 else '공격적 진입 후보' if score>=1.65 else '조건부 진입 후보' if score>=.85 else '관망'
        if tp is None and grade!='관망': grade='조건부 진입 후보'
        risk=abs(entry-stop)/entry
        reward=side*(tp-entry)/entry-2*FEE if tp is not None else None
        loss=risk+2*FEE+SLIP
        tradable=reward is not None and reward/loss>=1.15
        direct=tradable and score>=2.5
        level=upper if side<0 else lower
        confirmed=(int(bars['f5m'].t.iloc[-1])==end//(5*MIN)*(5*MIN)-5*MIN and confirm(bars['f5m'],side,level,atr))
        ready=direct or (tradable and score>=.85 and confirmed)
        if not ready and grade!='관망':
            grade='조건부 진입 후보'
        elif ready and score<2.5:
            grade='공격적 진입 후보'
        trigger=None if direct else (
            f'{level:.2f} 부근 지지 재시험, 5분봉 종가 지지 및 공격 매수 우위' if side>0 else
            f'{level:.2f} 부근 저항 재시험, 5분봉 종가 저항 아래 및 공격 매도 우위')
        decisions.append(dict(symbol=sym,side=side,grade=grade,score=score,entry=entry,entry_band=[entry-.12*atr,entry+.12*atr],
                              stop=stop,target=tp,atr_ratio=atr/entry,entry_status='entry' if ready else 'conditional' if grade!='관망' else 'watch',confirmed=confirmed,
                              net_target_return_notional=reward,stop_loss_notional=loss,
                              payoff=reward/loss if reward is not None else None,
                                confidence='높음' if grade=='강한 진입 후보' else '보통' if grade=='일반 진입 후보' else '낮음',
                              trigger=trigger,
                              invalidation=f'15분 종가가 {stop:.2f} '+('아래' if side==1 else '위')+' 또는 반대 흐름 전환',
                              reasons=sorted([(k,v) for k,v in pieces if v>.12],key=lambda x:-x[1]),
                              conflicts=sorted([(k,v) for k,v in pieces if v<-.12],key=lambda x:x[1])))
    for c in decisions:
        tp=c['target']
        side=c['side']
        c['candidate_visible']=bool(any(v>=.45 for _,v in c['reasons']) or c['score']>=.85)
        c['evidence_label']='혼재·약함' if c['score']<1.65 else c['grade']
        current_loss=abs(price-c['stop'])/price+2*FEE+SLIP
        current_reward=side*(tp-price)/price-2*FEE if tp is not None else None
        c['immediate_plan']=dict(entry=price,stop=c['stop'],target=tp,side=side,atr_ratio=atr/price,
                                 stop_loss_notional=current_loss,net_target_return_notional=current_reward,
                                 payoff=current_reward/current_loss if current_reward is not None else None,
                                 workable=bool(current_reward is not None and current_reward>0 and side*(price-c['stop'])>0))

        c['historical']=analog(sym,hourly,c['side'],abs(c['stop']/c['entry']-1),
                              abs(tp/c['entry']-1) if tp else abs(c['stop']/c['entry']-1)*1.5,funding,oi_delta)
        c['historical_comparison']=historical_compare(c['historical'],c['net_target_return_notional'],c['stop_loss_notional'])
    recent=bars['f15m']
    previous=recent.iloc[-13:-1]
    breakout=1 if recent.c.iloc[-1]>previous.h.max()+.15*atr else -1 if recent.c.iloc[-1]<previous.l.min()-.15*atr else 0
    best=max(decisions,key=lambda x:x['score'])
    if daily is not None:
        f['f1d']={'ema':daily}
    cvd=float((2*recent.buy-recent.v).tail(24).sum())
    change=float(recent.c.iloc[-1]/recent.c.iloc[-25]-1)
    divergence=('상승 가격·순매도' if change>0 and cvd<0 else
                '하락 가격·순매수' if change<0 and cvd>0 else '방향 불일치 미관측')
    return dict(symbol=sym,price=current,completed_1m_close=price,last_open_ms=int(bars['f1m'].t.iloc[-1]),regime=regime,market_direction=market_direction(daily,longer['ema']),structure_break=breakout,
                position_cvd24=cvd,position_cvd_divergence=divergence,
                position_mark_price=optional(raw,'funding',lambda x:float(x['markPrice']) if abs(int(x['time'])-end)<=180000 else None),
                support=lower,resistance=upper,indicators=f,spot_flow=spot_flow,oi_change=oi_delta,funding=funding,
                long_short=crowd_ratio,book_imbalance=imbalance,volume_profile_proxy_poc=profile_poc,
                mark_price=optional(raw,'funding',lambda x:float(x['markPrice'])),
                relative_strength=relative,candidate=best,directions=decisions),bars['f1m']

def get_page(sym,start,end):
    rows=[]
    while start<end:
        raw=api(FUT,'/fapi/v1/klines',symbol=sym,interval='1m',startTime=start,endTime=end-1,limit=1500)
        if not raw: break
        rows.extend(raw)
        start=int(raw[-1][0])+MIN
    return frame(rows,end) if rows else pd.DataFrame()

def settle_paper(state, market, bars, end):
    sym=market['symbol']; candidate=market['candidate']
    events=[]
    active=[p for p in state['orders'] if p['symbol']==sym and p['status'] in ('open','pending')]
    if active:
        start=min(p['cursor'] for p in active)
        if start<end:
            data=bars if start>=bars.t.iloc[0] else get_page(sym,start,end)
            data=data[(data.t>=start)&(data.t<end)]
            if not np.array_equal(data.t,np.arange(start,end,MIN)):
                return [{'kind':'missing_completed_bars','symbol':sym,'from_ms':start}]
            # Published settlement records are fetched over the replay interval.
            funding={}
            cursor=start
            # Fetch even if currently pending because it may fill before a later settlement.
            while cursor<end:
                page=api(FUT,'/fapi/v1/fundingRate',symbol=sym,startTime=cursor,endTime=end-1,limit=1000)
                if not page: break
                for row in page:
                    mark=row.get('markPrice')
                    if mark and float(mark)>0:
                        funding[int(row['fundingTime'])//MIN*MIN]=(float(row['fundingRate']),float(mark))
                    else:
                        return [{'kind':'missing_funding_mark','symbol':sym,'funding_ms':row['fundingTime']}]
                cursor=int(page[-1]['fundingTime'])+1
                if len(page)<1000: break
            for row in data.itertuples():
                j=int(row.t)
                for p in active:
                    if j<p['cursor'] or p['status'] not in ('open','pending'): continue
                    p['cursor']=j+MIN
                    side=p['side']; entry=p['entry']
                    worst=row.l if side==1 else row.h
                    best=row.h if side==1 else row.l
                    fresh=False
                    if p['status']=='pending':
                        if j>=p['expire']:
                            p['status']='expired'
                            events.append(dict(kind='expired',id=p['id'],at=j)); continue
                        pass_through=side*(entry-worst)/entry
                        if pass_through<=PEN:
                            if side*(row.c-p['base']['stop'])<=0.:
                                p['status']='cancelled'
                                events.append(dict(kind='entry_invalidated',id=p['id'],at=j))
                            continue
                        p['status']='open'
                        p['filled_at']=j
                        p['notional']*=.5 if pass_through<2*PEN else 1
                        fresh=True
                        events.append(dict(kind='entry',id=p['id'],at=j,price=entry,notional=p['notional']))
                    if j in funding and not fresh:
                        rate,mark=funding[j]
                        p['carry']-=side*p['notional']*mark/entry*rate
                    p['mae']=max(p['mae'],max(0.,-side*(worst/entry-1)))
                    branches=[p['base']]
                    if p.get('shadow'): branches.append(p['shadow'])
                    for branch in branches:
                        if branch.get('closed'): continue
                        stop=branch['stop']; target=branch['target']
                        reason=None
                        liq=p.get('model_liquidation')
                        if liq and side*(worst-liq)<=0:
                            reason='liquidation_proxy'
                            exit_price=liq
                        elif side*(worst-stop)<=0:
                            reason='stop'
                            base=min(row.o,stop) if side==1 else max(row.o,stop)
                            exit_price=base*(1-side*SLIP)
                        elif not fresh and side*(best-target)>target*PEN:
                            reason='target'
                            exit_price=target
                        if reason:
                            pnl=p['notional']*(side*(exit_price/entry-1)-FEE*(1+exit_price/entry))+p['carry']
                            if reason=='liquidation_proxy':
                                pnl=-p['notional']/p['leverage']
                            outcome=dict(closed=j,reason=reason,price=exit_price,pnl=pnl,funding=p['carry'],mae=p['mae'],
                                         hold_minutes=(j-p['filled_at'])//MIN)
                            branch.update(outcome)
                            events.append(dict(kind='exit',id=p['id'],branch=branch['name'],**outcome))
                    if all(branch.get('closed') for branch in branches): p['status']='closed'
    for p in active:
        if p['status']!='open' or p['base'].get('closed'): continue
        if p['side']!=candidate['side'] and candidate['score']>=2.5 and p.get('review')!=candidate['side']:
            p['review']=candidate['side']
            events.append(dict(kind='thesis_review',id=p['id'],at=end,reasons=candidate['reasons'][:3]))
            stop=market['support'] if p['side']==1 else market['resistance']
            old=p['base']['stop']; mark=market['completed_1m_close']
            side=p['side']
            new_stop=stop if side*(stop-old)>0 and side*(mark-stop)>0 else old
            target=p['base']['target']
            obstacle=market['resistance'] if side==1 else market['support']
            new_target=obstacle if side*(obstacle-mark)>0 and side*(target-obstacle)>0 and side*(obstacle/p['entry']-1)>2*FEE else target
            if p.get('shadow') is None and (new_stop!=old or new_target!=target):
                p['shadow']=dict(name='revision',stop=new_stop,target=new_target)
                events.append(dict(kind='exit_revision',id=p['id'],at=end,original_stop=old,proposed_stop=new_stop,
                                   original_target=target,proposed_target=new_target,
                                   reason='반대 수급·구조 변화를 반영해 이탈 시 손실 또는 다음 가격대까지의 이익 목표를 줄임'))
    return events

def paper_equity(state, markets):
    marks={x['symbol']:(x.get('mark_price') or x['completed_1m_close']) for x in markets}
    equity=INITIAL
    for p in state['orders']:
        b=p['base']
        if b.get('closed'):
            equity+=b['pnl']
        elif p.get('filled_at') is not None:
            last=marks.get(p['symbol'],p.get('last_mark',p['entry']))
            p['last_mark']=last
            equity+=p['notional']*(p['side']*(last/p['entry']-1)-FEE)+p['carry']
    return equity

def fetch_candles_at(fetch, args, cutoff):
    host,path,params=args
    data=fetch(host,path,**params)
    if path!='/fapi/v1/klines':
        return data
    step=int(params['interval'][:-1])*({'m':MIN,'h':60*MIN,'d':1440*MIN}[params['interval'][-1]])
    expected=cutoff//step*step-step
    def complete(rows):
        return sorted({int(r[0]) for r in rows if len(r)>=10 and int(r[6])<cutoff})
    try:
        times=complete(data)
        if len(times)>=50 and times[-1]==expected and all(b-a==step for a,b in zip(times,times[1:])):
            return data
    except (TypeError,ValueError,IndexError):
        pass
    time.sleep(.7)
    return fetch(host,path,**dict(params,endTime=expected+step-1))

def manual_indicators(bars,raw,end):
    """Keep corroborating observations separate from automated score votes."""
    b=bars['f15m']
    close=b.c
    previous=close.shift()
    tr=pd.concat([b.h-b.l,(b.h-previous).abs(),(b.l-previous).abs()],axis=1).max(axis=1)
    up=b.h.diff(); down=-b.l.diff()
    smooth=lambda v:v.ewm(alpha=1/14,adjust=False).mean()
    atr=smooth(tr).replace(0,np.nan)
    plus=100*smooth(up.where((up>down)&(up>0),0))/atr
    minus=100*smooth(down.where((down>up)&(down>0),0))/atr
    adx=smooth(100*(plus-minus).abs()/(plus+minus).replace(0,np.nan))
    center=close.rolling(20).mean()
    box=b.iloc[-21:-1]
    spot=optional(raw,'s15m',lambda x:frame(x,end).iloc[-1].to_dict())
    day=optional(raw,'f1d',lambda x:frame(x,end).iloc[-1].to_dict())
    if day and day['t']!=(end//(1440*MIN)-1)*1440*MIN: day=None
    hourly=bars['f1h']
    today=hourly[hourly.t==end//(1440*MIN)*(1440*MIN)]
    oi=optional(raw,'oi',lambda x:float(x[-1]['sumOpenInterest']) if 0<=end-int(x[-1]['timestamp'])<=45*MIN else None)
    return finite(dict(
        trend=dict(ADX=adx.iloc[-1],plus_DI=plus.iloc[-1],minus_DI=minus.iloc[-1],SMA20=center.iloc[-1]),
        momentum=dict(ROC12_percent=close.pct_change(12).iloc[-1]*100),
        volatility=dict(BB_width_percent=400*close.rolling(20).std(ddof=0).iloc[-1]/center.iloc[-1],
                        realized_15m_percent=100*close.pct_change().tail(24).std()),
        structure=dict(Donchian_low=box.l.min(),Donchian_high=box.h.max(),
                       previous_UTC_day_high=day['h'] if day else None,previous_UTC_day_low=day['l'] if day else None,
                       UTC_day_open=today.o.iloc[0] if not today.empty else None),
        flow=dict(aggregated_CVD24=float((2*b.buy-b.v).tail(24).sum())),
        derivatives=dict(OI=oi,basis_percent=100*(close.iloc[-1]/spot['c']-1) if spot and spot['t']==b.t.iloc[-1] else None)))

def tick(quiet=False, read_only=False, market_api=None, manual_request=None, symbols=None):
    market_api=market_api or api
    end=int(market_api(FUT,'/fapi/v1/time')['serverTime'])//MIN*MIN
    if abs(time.time()*1000-end)>150000:
        raise ValueError('Exchange time is stale')
    selected=tuple(symbols) if read_only and symbols else SYMS
    jobs={(sym,key):args for sym in SYMS for key,args in tasks(sym).items()
          if sym in selected or key=='f1h'}
    raw={s:{} for s in SYMS}; errors={}
    with ThreadPoolExecutor(max_workers=6) as pool:
        futures={k:pool.submit(fetch_candles_at,market_api,args,end) for k,args in jobs.items()}
        for (sym,key),f in futures.items():
            try: raw[sym][key]=f.result()
            except (OSError,ValueError,TypeError,IndexError) as e:
                errors[f'{sym}/{key}']=str(e)
                LOG.warning('symbol=%s API=%s function=tick %s: %s',sym,jobs[sym,key][1],type(e).__name__,e)
    marks={}
    for sym in SYMS:
        raw[sym]['_symbol']=sym
        val=optional(raw[sym],'f1h',lambda x:float(frame(x,end).c.pct_change(6).iloc[-1]))
        if val is not None: marks[sym]=val
    markets=[]; candles={}
    for sym in selected:
        try:
            other=SYMS[1-SYMS.index(sym)]
            relative=marks[sym]-marks[other] if sym in marks and other in marks else None
            market, candle=analyze(sym,raw[sym],end,relative)
            if market['last_open_ms']!=end-MIN:
                raise ValueError('latest completed 1m bar missing')
            if read_only:
                try:
                    closed={k:frame(raw[sym][k],end) for k in ('f15m','f1h')}
                    market['manual_indicators']=manual_indicators(closed,raw[sym],end)
                except (ArithmeticError,ValueError,IndexError,TypeError,KeyError):
                    market['manual_indicators']={}
            markets.append(market); candles[sym]=candle
        except (KeyError,ValueError,IndexError,TypeError,ArithmeticError) as ex:
            errors[sym]=str(ex)
            LOG.warning("symbol=%s function=analyze %s: %s",sym,type(ex).__name__,ex)
    if read_only:
        return finite(dict(market=markets,unavailable=errors,completed_through_utc=datetime.fromtimestamp(end/1000,timezone.utc).isoformat()))
    if not markets:
        raise ValueError('No symbol has current complete data: '+str(errors))
    OUT.mkdir(exist_ok=True)
    import sqlite3
    db=sqlite3.connect(OUT/'paper.sqlite3',timeout=30,isolation_level=None)
    db.execute('CREATE TABLE IF NOT EXISTS snapshots (minute INTEGER PRIMARY KEY, state TEXT NOT NULL, report TEXT NOT NULL)')
    db.execute('BEGIN IMMEDIATE')
    prev=db.execute('SELECT minute,state,report FROM snapshots ORDER BY minute DESC LIMIT 1').fetchone()
    if prev and prev[0]>=end:
        db.rollback(); db.close()
        if not quiet: print(prev[2],flush=True)
        return
    state=json.loads(prev[1]) if prev else dict(orders=[])
    accounts=state.setdefault('accounts',{})
    messages=[]
    account_specs=dict(ACCOUNTS)
    if manual_request or 'manual' in accounts: account_specs['manual']=(.01,.03)
    for name,(trade_risk,total_risk) in account_specs.items():
        account=accounts.setdefault(name,dict(orders=state.get('orders',[]) if name=='standard' else []))
        local=[]
        failed_symbols=set()
        for market in markets:
            try:
                local+=settle_paper(account,market,candles[market['symbol']],end)
            except (OSError,ValueError,KeyError) as ex:
                errors[name+'/'+market['symbol']+'/paper']=str(ex)
                failed_symbols.add(market['symbol'])
        failed_symbols.update(e.get('symbol') for e in local if e['kind'].startswith('missing_'))
        equity=paper_equity(account,markets)
        for m in markets:
            for c in sorted(m['directions'],key=lambda x:-x['score']):
                sym=m['symbol']; side=c['side']
                instant=name=='immediate_research'
                plan=c['immediate_plan'] if instant else c
                caps=exposure(account['orders'],sym,side)
                room=max(0.,min(equity*total_risk-caps['total'],equity*total_risk*.8-caps['direction'],equity*total_risk*.7-caps['symbol']))
                lookup=c.setdefault('risk_profiles',{})
                sizing_mode='standard' if name=='manual' else name
                profile=profiles(plan,equity,room,max(0.,equity*.45-caps['margin']),trade_risk,sizing_mode)
                lookup[name]=profile
                selected=profile['selected']
                ready=(c['candidate_visible'] and c['score']>0 and plan['workable'] and plan['payoff']>=1.) if instant else c['entry_status']=='entry'
                status='await_confirmation' if c['entry_status']=='conditional' and not instant else 'watch'
                if name=='manual':
                    ready=bool(manual_request and manual_request['symbol']==sym and manual_request['side']==side
                               and end<=manual_request['expires'] and c['target'] and c['payoff']>=1.15
                               and not any(p['id']==manual_request['id'] for p in account['orders']))
                if ready:
                    status='blocked_by_data_or_risk'
                if ready and len(markets)==2 and sym not in failed_symbols and not failed_symbols and selected:
                    similar=any(p['symbol']==sym and p['side']==side and (
                        p['status'] in ('open','pending') or (p['status']!='cancelled' and end-p['created']<60*MIN)) and
                        abs(p['entry']/plan['entry']-1)<max(.001,.5*c['atr_ratio']) for p in account['orders'])
                    status='similar_entry_exists' if similar else 'pending'
                    if not similar:
                        ref=hashlib.sha256(f'{name}/{sym}/{side}/{end}'.encode()).hexdigest()[:12]
                        if name=='manual': ref=manual_request['id']
                        account['orders'].append(dict(id=ref,symbol=sym,side=side,entry=plan['entry'],
                            base=dict(name='original',stop=plan['stop'],target=plan['target']),status='pending',cursor=end,
                            expire=end+(5 if instant else 120)*MIN,created=end,mae=0.,carry=0.,
                            notional=selected['notional_krw'],leverage=selected['leverage'],model_liquidation=selected['model_liquidation'],
                            stress_loss_ratio=selected['stress_loss_ratio'],entry_verified=True,grade=c['grade'],
                            reasons=c['reasons'],regime=m['regime']))
                        local.append(dict(id=ref,kind='new_paper_order',symbol=sym,side=side,entry=plan['entry']))
                profile['order_status']=status
        order_index={p['id']:p for p in account['orders']}
        for e in local:
            order=order_index.get(e.get('id'),{})
            messages.append({'account':name,'symbol':order.get('symbol'),'side':order.get('side'),**e})
        closed=sorted((p for p in account['orders'] if p['base'].get('closed')),key=lambda p:p['base']['closed'])
        buckets={}
        streak=longest=0
        for p in closed:
            outcome=p['base']
            streak=streak+1 if outcome['pnl']<0 else 0
            longest=max(longest,streak)
            for key in ['overall',p['regime']+'/'+p['grade']]+[r[0] for r in p['reasons'][:3]]:
                bucket=buckets.setdefault(key,dict(trades=0,targets=0,stops=0,liquidations=0,net_krw=0.))
                bucket['trades']+=1
                bucket['net_krw']+=outcome['pnl']
                bucket['targets']+=outcome['reason']=='target'
                bucket['stops']+=outcome['reason']=='stop'
                bucket['liquidations']+=outcome['reason']=='liquidation_proxy'
                bucket['target_rate']=bucket['targets']/bucket['trades']
                bucket['stop_rate']=bucket['stops']/bucket['trades']
        peak=max(account.get('peak',INITIAL),equity)
        account['peak']=peak
        account['drawdown']=max(account.get('drawdown',0.),1-equity/peak)
        account['summary']=dict(equity_krw=equity,growth=equity/INITIAL-1,observed_max_drawdown=account['drawdown'],
            longest_loss_streak=longest,below_one_tenth_seed=equity<=INITIAL*.1,performance=buckets,
            counts={status:sum(p['status']==status for p in account['orders']) for status in ('pending','open','closed','expired','cancelled')})
    state.pop('orders',None)
    result=dict(method_version='V17.3',scoring='heuristic; analog frequencies are not forecasts',
                completed_through_utc=datetime.fromtimestamp(end/1000,timezone.utc).isoformat(),market=markets,unavailable=errors,
                paper=dict(accounts={k:v['summary'] for k,v in accounts.items()},events=messages),
                hourly_summary_due=not prev or prev[0]//3600000<end//3600000)
    result=finite(result)
    line=json.dumps(result,ensure_ascii=False,allow_nan=False)
    db.execute('INSERT INTO snapshots VALUES (?,?,?)',(end,json.dumps(finite(state),ensure_ascii=False,allow_nan=False),line))
    db.commit(); db.close()
    temp=OUT/'latest.tmp'
    temp.write_text(json.dumps(result,indent=2,ensure_ascii=False)+'\n',encoding='utf-8')
    temp.replace(OUT/'latest.json')
    if not quiet or result['hourly_summary_due'] or messages:
        print(line,flush=True)

if __name__=='__main__':
    p=argparse.ArgumentParser()
    p.add_argument('--loop',action='store_true',help='poll within the current process; stdout carries alerts and hourly snapshots')
    a=p.parse_args()
    while True:
        try: tick(a.loop)
        except (OSError,ValueError) as exc:
            if not a.loop: raise
            print(json.dumps(dict(error=str(exc))),flush=True)
        if not a.loop: break
        time.sleep(max(1.,62-time.time()%60))
