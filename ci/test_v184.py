import json
import math
import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0,str(Path('source').resolve()))
import desktop as app

def sample():
    short=dict(side=-1,score=3,entry=100,target=90,stop=102,atr_ratio=.001,confirmed=True,
               entry_status='entry',candidate_visible=True,reasons=[('flow',2),('structure',1)],conflicts=[])
    long=dict(short,side=1,score=-3,candidate_visible=False,target=110,stop=98)
    market=dict(symbol='BTCUSDT',price=100.,completed_1m_close=100.,market_direction='하락',
                candidate=short,directions=[long,short],funding=.0,oi_change=.0,
                indicators={'f1h':{'ema':-.004},'f15m':{'flow':-.3,'volume':1.4,'r6':-.01},'f5m':{'flow':-.2}})
    return market,short

class ReleaseChecks(unittest.TestCase):
    def test_no_tp_sl_notification_without_registered_position(self):
        m,c=sample()
        state={}
        notes=[]
        def send(key,signature,body,*args,**kwargs): notes.append((key,body))
        with patch.object(app,'get',lambda k,d=None:state.get(k,d)),patch.object(app,'put',state.__setitem__), \
             patch.object(app,'health'),patch.object(app,'notify',send),patch.object(app,'review',return_value='entry'):
            report=dict(market=[m],completed_through_utc='2026-10-29T01:00:00+00:00')
            app.publish(report,0)
            c['target']=91
            app.publish(report,60)
            assert sum(k.startswith('idea/') for k,_ in notes)==1
            assert not any(k.startswith('user/') for k,_ in notes)
            state['positions']=[dict(id=7,symbol='BTCUSDT',side=1,entry=99,margin=100000,leverage=10,stop=97,target=112)]
            m['directions'][0]['stop']=98
            app.publish(report,120)
            assert any(k=='user/7/thesis' for k,_ in notes)
            assert any(k=='user/7/adjust' and '97.00 → 98.00' in v for k,v in notes)

    def test_market_conflict_and_protection(self):
        m,c=sample()
        self.assertTrue(app.signal_context(m,c)[0])
        m['market_direction']='불확실'
        self.assertTrue(app.signal_context(m,c)[0])  # strong lower-frame setup remains conditional
        self.assertFalse(app.signal_context(m,dict(c,score=.5))[0])
        held=dict(side=-1,stop=101,target=90)
        self.assertIsNone(app.position_adjustment(held,m,dict(c,stop=103)))
        self.assertEqual(app.live.market_direction(.004,-.01),'불확실')
        self.assertEqual(app.live.market_direction(-.004,-.01),'하락')

    def test_size_after_rounding(self):
        plan=dict(entry=100.,stop=99.9,side=1,atr_ratio=.002,stop_loss_notional=.0025,net_target_return_notional=.01)
        for equity in (3e6,20e6,50e6):
            chosen,rows=app.margin_choices(plan,equity,[],.0005)
            for row in rows:
                self.assertEqual(row['cap']%100000,0)
                self.assertLessEqual(row['cap']*row['leverage']*(row['stress_loss_ratio']+(.0005 if row['leverage']>=20 else 0)),equity*.01+1e-6)
            self.assertLessEqual(chosen['cap'],equity*.20)
        _,rows=app.margin_choices(plan,3e6,[dict(margin=2e6,leverage=10,entry=100,side=1,stop=98)],.0005)
        self.assertFalse(any(r['cap'] for r in rows))

    def test_position_records_keep_original(self):
        with tempfile.TemporaryDirectory() as temp,patch.object(app,'DB',Path(temp)/'desktop.sqlite'):
            app.put('positions',[dict(id=1,entry=100.,margin=100000,stop=98,target=104,leverage=10)])
            app.position_edit(['/pos','add','1','110','100000'])
            self.assertTrue(math.isclose(app.get('positions')[0]['entry'],200000/(100000/100+100000/110)))
            app.position_edit(['/pos','reduce','1','50'])
            self.assertEqual(app.get('positions')[0]['margin'],100000)
            app.position_edit(['/pos','close','1'])
            self.assertEqual(app.get('positions'),[])
            self.assertEqual(len(app.get('position_history')),3)

if __name__=='__main__':
    unittest.main()
