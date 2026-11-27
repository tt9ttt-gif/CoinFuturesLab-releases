from html import unescape
from pathlib import Path
import re
import sys
import unittest

sys.path.insert(0,str(Path('source').resolve()))
import desktop as app


def position(**changes):
    return dict(dict(id=1,symbol='ETHUSDT',side=1,entry=1675,margin=1000,leverage=25,
                     currency='USD',margin_type='isolated'),**changes)


def market(direction='하락',price=1674.53):
    long=dict(side=1,score=.2,atr_ratio=.003,stop=1660,target=1690)
    short=dict(long,side=-1,score=2,stop=1690,target=1660)
    return dict(symbol='ETHUSDT',price=price,mark_price=price,market_direction=direction,
                support=1665,resistance=1686,directions=[long,short],candidate=short,
                indicators={'f15m':dict(flow=-.1,r6=-.002,volume=1.2),'f1d':{'ema':-.01}},position_cvd24=-120)


class PositionAnalysis(unittest.TestCase):
    def test_reported_fee_case(self):
        result=app.position_profit(position(),1674.53)
        self.assertAlmostEqual(result['gross'],-7.014925373,places=6)
        self.assertAlmostEqual(result['entry'],12.5)
        self.assertAlmostEqual(result['exit'],12.496492537,places=6)
        self.assertAlmostEqual(result['net'],-32.011417910,places=6)
        self.assertIsNone(result['actual_entry'])
        self.assertIsNone(result['funding'])
        self.assertLess(result['stress'],result['net'])

    def test_short_and_confirmed_cost_inputs(self):
        result=app.position_profit(position(side=-1,verified_entry_fee=8.,verified_funding_pnl=-2.),1674.53)
        self.assertAlmostEqual(result['gross'],7.014925373,places=6)
        self.assertEqual(result['entry'],8)
        self.assertEqual(result['funding'],-2)
        self.assertAlmostEqual(result['net'],result['gross']-8-result['exit']-2)
        self.assertIsNone(app.position_profit(position(margin=None),1674))

    def test_high_leverage_against_trend_in_both_directions(self):
        for side,direction,stop in ((1,'하락',1655),(-1,'상승',1695)):
            state,text=app.position_review(position(side=side,stop=stop),market(direction),3_000_000)
            self.assertEqual(state,'상위 추세 역행·고배율')
            self.assertIn('위험 축소',text.splitlines()[0])

    def test_scenarios_and_protection(self):
        p=position()
        _,normal=app.position_review(p,market(),3_000_000)
        _,detailed=app.position_review(p,market(),3_000_000,'detailed','2026-11-27T15:00:00+00:00')
        _,simple=app.position_review(p,market(),3_000_000,'simple')
        self.assertIn('손절가 설정·조정 우선',normal.splitlines()[0])
        for term in ('1,665.00 지지','1,686.00 저항','손절 후보','익절 후보','미설정','추정 진입 수수료','확인된 펀딩 미확인'):
            self.assertIn(term,normal)
        self.assertNotIn('가격대별 대응',simple)
        self.assertIn('가격 손익 =',detailed)
        self.assertIn('KST',detailed)
        self.assertNotIn('stop',p)
        state,_=app.position_review(position(stop=1615,leverage=50),market(price=1647),3_000_000)
        self.assertEqual(state,'청산 근접')
        _,cross=app.position_review(position(margin_type='cross'),market(),3_000_000)
        self.assertIn('청산가 자료 없음',cross)

    def test_telegram_html_chunking(self):
        text='📌 <보유> & 위험\n'+'🥶'*4100+'\n🎯 다음 구간'
        parts=list(app.payloads(1,text))
        self.assertGreater(len(parts),2)
        unpacked=[]
        for part in parts:
            raw=part['text']
            self.assertEqual(raw.count('<b>'),raw.count('</b>'))
            plain=unescape(re.sub(r'</?b>','',raw))
            self.assertLessEqual(len(plain.encode('utf-16-le'))//2,3990)
            self.assertNotIn('생략',plain)
            unpacked.append(plain)
        self.assertEqual(''.join(unpacked).replace('\n',''),text.replace('\n',''))


if __name__=='__main__':
    unittest.main()
