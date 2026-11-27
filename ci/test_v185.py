from pathlib import Path
import sys
import unittest
from unittest.mock import patch
import pandas as pd

sys.path.insert(0,str(Path('source').resolve()))
import desktop as app

class ManualTests(unittest.TestCase):
    def test_routing(self):
        old={'symbol':'BTCUSDT','side':-1}
        for query,expected in [
            ('이더 분석해줘',('ETHUSDT',)),('이더 지금 어때',('ETHUSDT',)),('/eth',('ETHUSDT',)),
            ('ETH 롱 어때',('ETHUSDT',)),('이뎌 지금 어때',('ETHUSDT',)),('etherum',('ETHUSDT',)),
            ('비트 분석해줘',('BTCUSDT',)),('비트 지금 어때',('BTCUSDT',)),('/btc',('BTCUSDT',)),
            ('BTC 숏 어때',('BTCUSDT',)),('btcc',('BTCUSDT',)),('이더 가장 좋아?',('ETHUSDT',)),
            ('둘 다 분석해줘',app.live.SYMS),('전체 시장',app.live.SYMS),('/market',app.live.SYMS),
            ('이더와 비트 비교',app.live.SYMS),('지금 어때',('BTCUSDT',))]:
            with self.subTest(query=query):
                self.assertEqual(app.query_symbols(query,app.parse(query,old)),expected)
        self.assertEqual(app.parse('이더 지금 20배 롱 진입하면 어때?',old),{'symbol':'ETHUSDT','side':1,'leverage':20.})
        self.assertEqual(app.parse('비트 지금 숏 들어가도 돼?',{}),old)
        self.assertEqual(app.query_symbols('지금 어때',{}),app.live.SYMS)

    def test_card_isolation(self):
        from datetime import datetime,timezone
        now=datetime.now(timezone.utc).isoformat()
        for text,want in [('이더 지금 20배 롱 진입하면 어때?','ETHUSDT'),('비트 지금 숏 들어가도 돼?','BTCUSDT')]:
            with patch.object(app,'get',side_effect=lambda k,d=None:d),patch.object(app,'put'),\
                 patch.object(app.live,'tick',return_value=dict(completed_through_utc=now,market=[dict(symbol=s,candidate={'entry_status':'watch'}) for s in app.live.SYMS])) as tick,\
                 patch.object(app,'review',side_effect=lambda m,*a:m['symbol']):
                self.assertEqual(app.handle(text),want)
                self.assertEqual(tick.call_args.kwargs['symbols'],(want,))

    def test_analog_order_and_costs(self):
        path=app.live.analog_path
        self.assertEqual(path([(105,100,101)]*5,100,1,.05,.03),('unfilled',None))
        self.assertEqual(path([(105,99,101)],100,1,.05,.03),('unresolved',None))
        outcome,net=path([(101,99,100),(106,99,105)],100,1,.05,.03)
        self.assertEqual(outcome,'target_hits')
        self.assertAlmostEqual(net,.029)
        outcome,net=path([(108,94,100)],100,1,.05,.03)
        self.assertEqual(outcome,'stop_hits')
        self.assertLess(net,-.061)

    def test_group_indicators(self):
        b=pd.DataFrame(dict(c=[100+i*.2 for i in range(80)],h=[101+i*.2 for i in range(80)],
                            l=[99+i*.2 for i in range(80)],v=[10.]*80,buy=[6.]*80))
        b['t']=range(-79*900000,900000,900000)
        b['o']=b.c
        with patch.object(app.live,'optional',return_value=None):
            a=app.live.manual_indicators({'f15m':b,'f1h':b},{},0)
        self.assertGreater(a['trend']['plus_DI'],a['trend']['minus_DI'])
        self.assertEqual(a['flow']['aggregated_CVD24'],48)
        self.assertIsNone(a['derivatives']['OI'])

if __name__=='__main__':
    unittest.main()
