from pathlib import Path
import sys
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

sys.path.insert(0,str(Path('source').resolve()))
import desktop as app


class RegistrationTests(unittest.TestCase):
    def setUp(self):
        self.folder=TemporaryDirectory()
        self.addCleanup(self.folder.cleanup)
        for change in (
            patch.object(app,'DB',Path(self.folder.name)/'desktop.sqlite3'),
            patch.object(app,'manual_market',return_value=(None,None)),
        ):
            change.start()
            self.addCleanup(change.stop)

    def test_missing_direction_and_amounts(self):
        self.assertIn('롱인가요',app.handle('이더 진입가 1700'))
        self.assertEqual(app.get('positions',[]),[])
        self.assertIn('미입력',app.handle('숏'))
        p,=app.get('positions')
        self.assertEqual((p['symbol'],p['side'],p['entry']),('ETHUSDT',-1,1700))
        self.assertIsNone(p.get('leverage'))
        self.assertIsNone(p.get('margin'))
        self.assertIsNone(p.get('stop'))

    def test_context_requires_confirmation(self):
        app.put('context/cmd',{'symbol':'BTCUSDT','side':1,'margin':1000000})
        self.assertIn('확인',app.handle('비트 진입가 61000'))
        self.assertEqual(app.get('positions',[]),[])
        app.handle('확인')
        self.assertIsNone(app.get('positions')[0].get('margin'))

    def test_collision_close_and_preservation(self):
        app.handle('비트 롱 62000 20배 증거금 100만원')
        p,=app.get('positions')
        self.assertEqual((p['entry'],p['leverage'],p['margin']),(62000,20,1000000))
        self.assertIn('수정 번호',app.handle('비트 롱 진입가 61000'))
        app.handle('수정 '+str(p['id']))
        self.assertEqual(app.get('positions')[0]['margin'],1000000)
        app.handle('이더 숏 진입가 1750')
        self.assertIn('선택',app.handle('거래종료'))
        app.handle('비트 거래종료')
        remaining,=app.get('positions')
        self.assertEqual(remaining['symbol'],'ETHUSDT')
        app.notify(f"user/{p['id']}/liq",'risk','should never queue',force=True)
        with app.db() as db:
            self.assertEqual(db.execute('SELECT count(*) FROM outbox').fetchone()[0],0)
        self.assertEqual(app.get('position_history')[-1]['operation'],'close')

    def test_reservation_does_not_change_positions_or_paper(self):
        self.assertIn('롱인가요',app.handle('이더 1680에 지정가 걸어두면 어떨까?'))
        with patch.object(app,'manual_market',return_value=({'symbol':'ETHUSDT'},'now')), patch.object(app,'reservation_review',return_value='조건부 가능') as review:
            self.assertEqual(app.handle('롱'),'조건부 가능')
            self.assertEqual(review.call_args.args[1]['entry'],1680)
        self.assertEqual(app.get('positions',[]),[])
        self.assertIsNone(app.get('manual_request'))


    def test_missing_market_keeps_registration_and_recovery(self):
        with patch.object(app,'manual_market',side_effect=TimeoutError):
            self.assertIn('기록은 저장',app.handle('이더 숏 진입가 1750'))
        p,=app.get('positions')
        c={'side':-1,'atr_ratio':.01,'stop':1800,'target':1600}
        market={'symbol':'ETHUSDT','price':1740,'support':1700,'resistance':1800,
                'directions':[c],'candidate':c,'market_direction':'하락'}
        status,output=app.position_review(p,market,3_000_000)
        self.assertEqual(status,'유지')
        self.assertIn('추정 손익 자료 없음',output)
        self.assertIn('유지',output.splitlines()[0])

    def test_polling_does_not_wait_for_analysis(self):
        from threading import Event,Thread
        import time
        stop=Event()
        queued=app.Queue()
        polled=Event()
        date=time.time()
        request={'update_id':7,'message':{'date':date,'chat':{'id':123,'type':'private'},'text':'/eth'}}
        def fetch(token,method,params):
            if params['offset']==0: return [request]
            polled.set()
            stop.set()
            return []
        with patch.object(app,'tg',side_effect=fetch):
            worker=Thread(target=app.receive,args=(stop,'fake',123,queued))
            worker.start()
            self.assertTrue(polled.wait(2))
            worker.join(2)
        self.assertEqual(queued.get_nowait()[0],'/eth')
        self.assertEqual(app.get('telegram_offset'),8)


if __name__=='__main__':
    unittest.main()
