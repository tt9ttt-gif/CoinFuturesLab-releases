from datetime import datetime, timezone
from pathlib import Path
import sys
from tempfile import TemporaryDirectory
from unittest import TestCase, main
from unittest.mock import patch

sys.path.insert(0,str(Path('source').resolve()))
import desktop as app


class PositionRegistration(TestCase):
    def setUp(self):
        directory=TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        received=datetime(2026,11,28,13,tzinfo=timezone.utc)
        for mock in (
            patch.object(app,'DB',Path(directory.name)/'state.sqlite3'),
            patch.object(app,'manual_market',return_value=(None,None)),
            patch.object(app.REPLY_TIMING,'value',{'received_epoch':received.timestamp()},create=True),
        ):
            mock.start()
            self.addCleanup(mock.stop)

    def test_intent_examples(self):
        for message,intent in [
            ('이더 1643.4 지정가 25배 롱 2055달러','confirm_open'),
            ('이더 진입 1643.4 롱 25배 2055달러',app.POSITION_OPEN),
            ('이더 진입 21:40 롱 1643.4 격리 25배 증거금 2055 USDT',app.POSITION_OPEN),
            ('이더 1643.4 지정가 롱 25배 진입했어',app.POSITION_OPEN),
            ('이더 예약 1643.4 롱 25배',app.ENTRY_ANALYSIS),
            ('이더 1643.4 롱 들어갈까?',app.ENTRY_ANALYSIS),
            ('이더 거래종료',app.POSITION_CLOSE),
            ('예약 주문이 체결됐어',app.POSITION_OPEN),
            ('이더 1643.4 롱 진입하면?',app.ENTRY_ANALYSIS),
        ]:
            with self.subTest(message=message):
                self.assertEqual(app.trade_intent(message),intent)
        self.assertIn('등록',app.handle('이더 1643.4 지정가 25배 롱 2055달러'))
        self.assertEqual(app.get('positions',[]),[])
        self.assertIn('진입 분석',app.handle('이더 예약 1643.4 롱 25배'))
        self.assertEqual(app.get('positions',[]),[])

    def test_reported_time_actual_margin_and_close(self):
        message='이더 진입 21:40 롱 1643.4 격리 25배 증거금 2055 USDT'
        self.assertIn('등록 완료',app.handle(message))
        pos=app.get('positions')[0]
        self.assertEqual(pos['entry'],1643.4)
        self.assertEqual(pos['margin'],2055)
        self.assertEqual(pos['currency'],'USDT')
        self.assertEqual(app.position_profit(pos,1643.4)['notional'],51375)
        self.assertEqual(pos['execution_time'],'2026-11-28T12:40:00+00:00')
        self.assertEqual(pos['received_time'],'2026-11-28T13:00:00+00:00')
        self.assertIsNone(pos['analysis_time'])
        self.assertIn('기존 등록',app.handle(message))
        self.assertEqual(len(app.get('positions')),1)
        app.handle(f"/pos margin {pos['id']} 100 USDT")
        updated=app.get('positions')[0]
        self.assertEqual(updated['margin'],2155)
        self.assertEqual(app.position_profit(updated,1643.4)['notional'],51375)
        self.assertIn('종료',app.handle('이더 거래종료 수익 42 USDT'))
        self.assertEqual(app.get('positions'),[])
        self.assertEqual(app.get('position_history')[-1]['realized_pnl']['amount'],42)

    def test_missing_and_ambiguous_clock(self):
        self.assertIn('날짜',app.handle('이더 진입 23:50 롱 1643.4 25배'))
        self.assertEqual(app.get('positions',[]),[])
        self.assertIn('등록 완료',app.handle('2026-11-27 23:50'))
        self.assertIn('등록 완료',app.handle('비트 진입 61000 숏 20배'))
        pos=app.get('positions')[-1]
        self.assertEqual(pos['execution_time'],pos['received_time'])
        self.assertEqual(pos['execution_time_source'],'receipt_proxy')

    def test_execution_path_uses_only_completed_bars(self):
        time='2026-11-28T12:40:30+00:00'
        start=int(datetime(2026,11,28,12,41,tzinfo=timezone.utc).timestamp()*1000)
        row=[start,'100','105','97','99','2',start+59999]
        pos=dict(execution_time=time,execution_time_source='user_reported',entry=100,side=1,symbol='ETHUSDT')
        cutoff='2026-11-28T12:42:00+00:00'
        with patch.object(app,'manual_rest',return_value=[row]) as fetch:
            path=app.execution_path(pos,cutoff)
            self.assertIn('MFE +5.00% · MAE -3.00%',path)
            self.assertEqual(fetch.call_args.kwargs['startTime'],start)
        with patch.object(app,'manual_rest',return_value=[]):
            self.assertIn('누락',app.execution_path(pos,cutoff))


if __name__=='__main__':
    main()
