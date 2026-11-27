"""Forward research account sizing, independent of signal scores."""
MMR=.005
FEE=.0005
ACCOUNTS={'conservative':(.005,.015),'standard':(.01,.03),'aggressive':(.02,.05),
          'stress':(.04,.10),'immediate_research':(.02,.05)}

def ladder(equity):
    if equity<10e6: return (5,10,15,20,25,30,40,50),.20
    if equity<30e6: return (5,10,15,20),.18
    if equity<50e6: return (3,5,7,10),.15
    return (1,2,3,5),.12

def profiles(c,equity,room,margin_room,risk,mode):
    levels,margin_cap=ladder(equity)
    entry,stop,side,atr=(c[k] for k in ('entry','stop','side','atr_ratio'))
    stress=c['stop_loss_notional']+max(.0005,.5*atr)
    budget=min(equity*risk,room)
    cash=min(equity*margin_cap,margin_room)
    buffer={'conservative':3,'standard':2,'aggressive':1.5,'stress':1.25,'immediate_research':1.5}[mode]
    rows=[]
    for lev in levels:
        liq=entry*(1-side*(1/lev-FEE))/(1-side*MMR)
        gap=side*(stop-liq)/entry
        ok=gap>=buffer*max(atr,.001)
        notional=min(max(0,budget)/stress,max(0,cash)*lev) if ok else 0.
        rows.append(dict(leverage=lev,model_liquidation=liq,stop_to_liquidation_ratio=gap,eligible=ok,notional_krw=notional,
                         margin_ratio=notional/lev/equity if equity>0 else 0.,risk_krw=notional*stress,stress_loss_ratio=stress,
                         loss_krw=notional*c['stop_loss_notional'],
                         profit_krw=notional*c['net_target_return_notional'] if c['net_target_return_notional'] is not None else None))
    usable=[p for p in rows if p['notional_krw']>0]
    preferred=[p for p in usable if p['leverage']>=10] if equity<10e6 else usable
    pool=preferred or usable
    best=max((p['notional_krw'] for p in pool),default=0.)
    near=[p for p in pool if p['notional_krw']>=.98*best]
    selected=(near[0] if mode=='conservative' else near[len(near)//2] if mode=='standard' else near[-1]) if near else None
    return dict(selected=selected,alternatives=rows)
