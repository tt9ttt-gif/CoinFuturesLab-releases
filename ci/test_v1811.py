import os
import time
import unittest
from test_v188 import app, market, position


class HeldEngine(unittest.TestCase):
    def test_open_interest_and_oversold_are_neutral_in_isolation(self):
        m=market()
        m.update(position_oi=.032,position_funding=.0001)
        m['indicators']['f15m']={'rsi':25}
        for side in (1,-1):
            evidence=app.holding_evidence(m,side)
            self.assertTrue(all(v==0 for v,_ in evidence))
            self.assertIn('과매도',str(evidence))
        m['indicators']['f15m'].update(r6=-.01,flow=-.25,atr=.003,volume=1.8)
        self.assertEqual(app.holding_evidence(m,1)[0][0],-1)
        m.update(position_cvd24=100,position_cvd_price_change=-.01)
        self.assertIn('흡수 가능성',str(app.holding_evidence(m,1)))

    def test_structure_protection_both_sides_and_cost(self):
        for side in (1,-1):
            p=position(side=side,entry=100,leverage=5)
            m=market(price=100.)
            m['position_levels']={'f1h':{'low':[98,94,90],'high':[102,106,110]}}
            plan=app.protection_plan(p,m,100.,.01,app.position_liquidation(p),.005)
            self.assertEqual(plan['stop'],98.-.35 if side==1 else 102.+.35)
            self.assertAlmostEqual(plan['stop_net'],app.position_profit(p,plan['stop'])['stress'])
            self.assertEqual(len(plan['reward']),2)
            self.assertTrue(all(side*(price-100)>0 and ratio>=1 for price,_,_,ratio in plan['reward']))
            p['stop']=100-side
            self.assertNotIn('stop',app.protection_plan(p,m,100.,.01,app.position_liquidation(p),.005))
        p=position(entry=100,leverage=50)
        self.assertNotIn('stop',app.protection_plan(p,m,100.,.01,app.position_liquidation(p),.01))
        m['position_mark_price']=None
        self.assertIn('마크가격',app.protection_plan(p,m,100.,.01,98.,.005)['reason'])

    @unittest.skipUnless(os.environ.get('CFL_PUBLIC_TEST')=='1','optional Binance public API probe')
    def test_public_market(self):
        start=time.monotonic()
        m,stamp=app.manual_market('ETHUSDT')
        self.assertIsNotNone(m)
        self.assertTrue(m['position_levels']['f1h']['low'])
        price=m.get('position_mark_price') or m['price']
        p=position(entry=price,leverage=10,margin=1000,currency='USDT')
        buffer=.005
        plan=app.protection_plan(p,m,price,m['directions'][0]['atr_ratio'],app.position_liquidation(p),buffer)
        print('Binance:',stamp,'round trip',round(time.monotonic()-start,2),'s',plan)
        if 'stop' in plan:
            self.assertGreater(plan['room'],buffer)
            self.assertLess(plan['stop'],price)
        else:
            self.assertTrue(plan['reason'])

    def test_trend_context_and_missing_week(self):
        m=market()
        m['indicators'].update(f4h={'ema':-.008},f1h={'ema':-.0025},f5m={'r6':.006})
        trend,_=app.holding_trend(m,1)
        for phrase in ('주봉 미확인','역추세 보유','단기 역행'):
            self.assertIn(phrase,trend)


if __name__=='__main__':
    unittest.main()
