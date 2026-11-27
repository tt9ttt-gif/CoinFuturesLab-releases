"""Paper-account sizing and completed-bar confirmation."""
import math

MMR=.005
FEE=.0005
SLIP=.0005
TOTAL=.01
DIRECTION=.0065
SYMBOL=.0055
MARGIN=.45
SETTINGS=(('conservative',.0015,.10,4,3.),('standard',.0025,.15,3,2.),('aggressive',.0035,.20,2,1.25))

def size(entry,stop,side,atr,account,available_risk=None,available_margin=None):
    gap=max(SLIP,.5*atr)
    stress=abs(stop/entry-1)+2*FEE+gap
    result={}
    for name,budget,margin_cap,buffer,vol_buffer in SETTINGS:
        risk=min(account*budget,available_risk) if available_risk is not None else account*budget
        cash=min(account*margin_cap,available_margin) if available_margin is not None else account*margin_cap
        needed=abs(stop/entry-1)+buffer*gap+vol_buffer*atr
        # Solve isolated liquidation displacement for the maximum allowable integer leverage.
        lev=max(0,math.floor(1/(needed*(1-side*MMR)+MMR+FEE)))
        notional=min(max(0.,risk)/stress,max(0.,cash)*lev)
        if notional<=0 or lev==0 or account<=0:
            result[name]=None
            continue
        # Use the lowest leverage that fits the desired notional within the margin cap.
        use=max(1,min(lev,math.ceil(notional/cash-1e-12)))
        liq=entry*(1-side*(1/use-FEE))/(1-side*MMR)
        result[name]=dict(leverage=use,max_model_leverage=lev,margin_ratio=notional/use/account,notional_krw=notional,
                          model_liquidation=liq,stress_loss_ratio=stress,stress_loss_krw=notional*stress,
                          normal_loss_krw=notional*(abs(stop/entry-1)+2*FEE+SLIP))
    return result

def exposure(orders,symbol,side):
    total=direction=pair=margin=0.
    for p in orders:
        if p['status'] not in ('pending','open') or p['base'].get('closed'): continue
        loss=p['notional']*p.get('stress_loss_ratio',abs(p['base']['stop']/p['entry']-1)+2*FEE+SLIP)
        total+=loss
        direction+=loss*(p['side']==side)
        pair+=loss*(p['symbol']==symbol)
        margin+=p['notional']/p.get('leverage',1)
    return dict(total=total,direction=direction,symbol=pair,margin=margin)

def confirm(bars,side,level,atr):
    before,bar=bars.iloc[-2],bars.iloc[-1]
    flow=(2*bar.buy-bar.v)/bar.v if bar.v else 0.
    tolerance=.25*atr
    if side>0:
        return bool(before.c>level and bar.l<=level+tolerance and bar.c>level and flow>.06)
    return bool(before.c<level and bar.h>=level-tolerance and bar.c<level and flow<-.06)

def historical_compare(sample,reward,loss):
    n=sample.get('sample',0); k=sample.get('target_hits',0)
    cutoff=loss/(loss+reward) if reward is not None and reward>0 else None
    if not n:
        return dict(target_first_rate=None,breakeven_win_rate=cutoff,interval_95=None,uncertainty='no matched sample')
    rate=k/n
    z=1.96
    den=1+z*z/n
    center=(rate+z*z/(2*n))/den
    half=z*math.sqrt(rate*(1-rate)/n+z*z/(4*n*n))/den
    return dict(target_first_rate=rate,breakeven_win_rate=cutoff,delta=rate-cutoff if cutoff is not None else None,
                interval_95=[max(0.,center-half),min(1.,center+half)],
                uncertainty='small/non-random sample; unresolved paths counted in denominator; not a calibrated probability')
