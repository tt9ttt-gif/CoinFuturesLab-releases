from pathlib import Path
import sys
from tempfile import TemporaryDirectory
from unittest import TestCase,main
from unittest.mock import patch

sys.path.insert(0,str(Path('source').resolve()))
import desktop as app


class TradeModes(TestCase):
    def setUp(self):
        tmp=TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        for change in (
            patch.object(app,'DB',Path(tmp.name)/'desktop.sqlite3'),
            patch.object(app,'manual_market',return_value=(None,None)),
        ):
            change.start()
            self.addCleanup(change.stop)

    def test_entry_examples_never_create_order(self):
        for text,price in (
            ('이더 1700 롱 지정가 괜찮아?',1700),
            ('비트 61000 숏 예약 어떠냐',61000),
            ('ETH 1650 롱 들어갈까?',1650),
            ('비트 62000 숏 20배 어떰?',62000),
            ('이더 1700 롱 예약',1700),
            ('이더 숏 진입가 1750으로 진입했어?',1750),
        ):
            with self.subTest(text=text):
                self.assertEqual(app.trade_intent(text),app.ENTRY_ANALYSIS)
                self.assertEqual(app.trade_fields(text)['entry'],price)
                with patch.object(app,'manual_market',return_value=({},None)) as market:
                    self.assertIn('진입 분석',app.handle(text))
                    market.assert_called_once()
        self.assertEqual(app.get('positions',[]),[])
        self.assertIsNone(app.get('manual_request'))
        self.assertEqual(app.trade_intent('이더 지금 롱 괜찮아?'),app.ENTRY_ANALYSIS)
        self.assertIn('등록을 입력',app.handle('이더 진입가 1700'))
        self.assertIn('롱 또는 숏',app.handle('이더 1700 예약'))
        self.assertEqual(app.get('positions',[]),[])

    def test_open_amount_and_currency(self):
        self.assertIn('총액인가요',app.handle('이더 1700 롱 25배 2000달러 들어왔어'))
        self.assertEqual(app.get('positions',[]),[])
        self.assertIn('등록 완료',app.handle('총액'))
        eth=app.get('positions')[0]
        self.assertEqual((eth['entry'],eth['side'],eth['leverage'],eth['margin'],eth['currency'],eth['amount_kind']),
                         (1700,1,25,80,'USD','notional'))
        self.assertIn('등록 완료',app.handle('비트 61000 숏 50배 증거금 1000달러 교차 진입했어'))
        btc=app.get('positions')[1]
        self.assertEqual((btc['entry'],btc['side'],btc['margin_type'],btc['margin']),(61000,-1,'cross',1000))
        self.assertIsNone(app.position_liquidation(btc))
        self.assertEqual(app.trade_fields('이더 1700에 숏으로 30배 100만원어치 들어왔어')['amount'],1000000)
        self.assertEqual(app.trade_fields('ETH 롱 진입가 1650 20배 증거금 1500 USDT')['currency'],'USDT')
        self.assertEqual(app.trade_intent('ETH 롱 진입가 1650 20배 증거금 1500 USDT'),app.POSITION_OPEN)

    def test_close_realized_and_continue_market(self):
        app.put('positions',[dict(id=71,symbol='ETHUSDT',side=1,entry=1700,margin=100000),
                             dict(id=72,symbol='BTCUSDT',side=-1,entry=61000,margin=100000)])
        app.put('position_history',[{'operation':'legacy'}])
        app.notify('user/71/thesis','risk','등록 #71 위험',force=True)
        self.assertIn('+459.35 USD',app.handle('이더 거래종료 수익 459.35달러'))
        self.assertEqual([p['id'] for p in app.get('positions')],[72])
        history=app.get('position_history')
        self.assertEqual(history[0]['operation'],'legacy')
        self.assertEqual(history[-1]['realized_pnl'],dict(amount=459.35,currency='USD',source='user'))
        app.notify('user/71/thesis','new risk','등록 #71 위험',force=True)
        with app.db() as conn:
            self.assertEqual(conn.execute('SELECT suppressed FROM outbox').fetchall(),[(1,)])
        self.assertIn('일반 시장 감시는 계속',app.handle('거래종료'))
        self.assertNotIn('realized_pnl',app.get('position_history')[-1])
        self.assertEqual(app.get('positions'),[])
        self.assertIn('"market_monitor": "on"',app.handle('/status'))

    def test_close_selection_preserves_provided_loss(self):
        for text in ('이더 1700 롱 진입했어','비트 61000 숏 진입했어'):
            self.assertIn('등록 완료',app.handle(text))
        first,second=app.get('positions')
        self.assertIn('선택',app.handle('거래종료 손실 2만원'))
        self.assertEqual(len(app.get('positions')),2)
        app.handle(f"/pos close {second['id']}")
        self.assertEqual(app.get('positions'),[first])
        self.assertEqual(app.get('position_history')[-1]['realized_pnl']['amount'],-20000)


if __name__=='__main__':
    main()
