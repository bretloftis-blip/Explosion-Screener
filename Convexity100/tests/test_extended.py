import asyncio
import tempfile
import unittest
from pathlib import Path
from datetime import date, datetime, timezone
from unittest.mock import patch
from urllib.request import urlopen
from urllib.error import HTTPError
from scanner import Scanner, Settings, cutoff_ms, deliver_outbox, start_health_server

class IntegrationTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.sc = Scanner(Settings(db_path=str(Path(self.tmp.name)/'scan.db'), telegram_token='test', telegram_chat_id='test'))
        self.now = int(datetime.now(timezone.utc).timestamp()*1000)
    def tearDown(self):
        self.sc.store.conn.close()
        self.tmp.cleanup()
    def seed(self, side='C', flat=False):
        sym=f'O:SPY261218{side}00600000'
        # Use dates safely within permitted 10DTE independent of execution date.
        from datetime import timedelta
        expiry=(datetime.now(timezone.utc)+timedelta(days=3)).strftime('%y%m%d')
        sym=f'O:SPY{expiry}{side}00600000'
        for i,(c,v) in enumerate(zip([1,1,1,1 if flat else 1.18],[20,20,20,100])):
            self.sc.on_aggregate(dict(sym=sym,e=self.now+(i-3)*60000,c=c,op=1,v=v,av=300))
        return sym
    def quote(self,sym,bid,ask,offset=1,size=10):
        t=self.now+offset
        return self.sc.on_quote(dict(sym=sym,t=t,bp=bid,ap=ask,bs=size,**{'as':size}),now_ms=t)
    def test_both_sides_all_targets_restart_and_delivery(self):
        for side in ['C','P']:
            sym=self.seed(side)
            self.assertIsNotNone(self.quote(sym,1.15,1.25))
            cost=1.25+.01+.0065
            for multiple,offset in [(2,100),(3,200),(4,300)]:
                bid=cost*multiple+.0165+.00001
                self.quote(sym,bid,bid+.05,offset)
        rows=self.sc.store.conn.execute('SELECT target100_ms,target200_ms,target300_ms FROM signals').fetchall()
        self.assertEqual(len(rows),2)
        self.assertTrue(all(all(r) for r in rows))
        async def send():
            with patch('scanner.post_telegram',return_value=200) as mocked:
                task=asyncio.create_task(deliver_outbox(self.sc))
                for _ in range(50):
                    await asyncio.sleep(.05)
                    if self.sc.store.conn.execute('SELECT COUNT(*) FROM outbox WHERE sent=0').fetchone()[0]==0:break
                task.cancel()
                try:await task
                except asyncio.CancelledError:pass
                self.assertEqual(mocked.call_count,8)
        asyncio.run(send())
        restored=Scanner(self.sc.settings)
        self.assertEqual(restored.store.summary(),[('HIT_100',2)])
        restored.store.conn.close()
    def test_flat_accumulation_before_momentum(self):
        self.assertIn(self.seed(flat=True),self.sc.candidates)
    def test_duplicate_aggregate_and_capacity(self):
        sym=self.seed()
        before=len(self.sc.bars[sym])
        self.sc.on_aggregate(dict(sym=sym,e=self.now,c=1.2,op=1,v=300,av=400))
        self.assertEqual(len(self.sc.bars[sym]),before)
        self.sc.settings.max_quote_subs=1
        self.assertNotIn(self.seed('P'),self.sc.candidates)
    def test_zero_size_stale_nan_and_ordering(self):
        sym=self.seed()
        self.assertIsNone(self.quote(sym,1.1,1.2,size=0))
        self.assertIsNone(self.quote(sym,float('nan'),1.2))
        self.assertIsNone(self.sc.on_quote(dict(sym=sym,t=self.now-10000,bp=1,ap=1.2,bs=1,**{'as':1}),now_ms=self.now))
        self.quote(sym,1.15,1.25)
        self.quote(sym,.5,.6,100)
        self.quote(sym,6,6.1,50)
        self.assertEqual(self.sc.store.summary(),[('STOP_FIRST',1)])
    def test_calendar_holiday_and_half_day(self):
        stamp=int(datetime(2026,11,25,20,tzinfo=timezone.utc).timestamp()*1000)
        out=datetime.fromtimestamp(cutoff_ms(stamp,date(2026,12,1))/1000,timezone.utc)
        self.assertEqual(out,datetime(2026,11,27,18,tzinfo=timezone.utc))
    def test_health_liveness_not_readiness(self):
        with patch.dict('os.environ',{'PORT':'0'}):server=start_health_server(self.sc)
        root=f'http://127.0.0.1:{server.server_port}'
        self.assertEqual(urlopen(root+'/healthz').status,200)
        with self.assertRaises(HTTPError):urlopen(root+'/readyz')
        server.shutdown();server.server_close()

class TransportTests(unittest.IsolatedAsyncioTestCase):
    async def test_websocket_auth_subscription_and_quote_pipeline(self):
        import json
        from scanner import run_live
        tmp=tempfile.TemporaryDirectory()
        now=int(datetime.now(timezone.utc).timestamp()*1000)
        from datetime import timedelta
        expiry=(datetime.now(timezone.utc)+timedelta(days=3)).strftime('%y%m%d')
        sym=f'O:SPY{expiry}P00600000'
        bars=[dict(ev='AM',sym=sym,e=now+(i-3)*60000,c=c,op=1,v=v,av=300) for i,(c,v) in enumerate(zip([1,1,1,1.18],[20,20,20,100]))]
        quote=dict(ev='Q',sym=sym,t=now+1,bp=1.15,ap=1.25,bs=10,**{'as':10})
        class Socket:
            def __init__(self):self.sent=[];self.events=[json.dumps([{'ev':'status','status':'auth_success'}]),json.dumps(bars),json.dumps([quote])]
            async def send(self,body):self.sent.append(json.loads(body))
            async def recv(self):
                if self.events:return self.events.pop(0)
                await asyncio.sleep(100)
            async def __aenter__(self):return self
            async def __aexit__(self,*args):return False
        ws=Socket()
        settings=Settings(key='fake-key',db_path=str(Path(tmp.name)/'db.sqlite'))
        with patch('websockets.connect',return_value=ws),patch('scanner.start_health_server'):
            task=asyncio.create_task(run_live(settings))
            await asyncio.sleep(.2)
            task.cancel()
            with self.assertRaises(asyncio.CancelledError):await task
        self.assertEqual(ws.sent[0],{'action':'auth','params':'fake-key'})
        self.assertIn({'action':'subscribe','params':'AM.*'},ws.sent)
        self.assertIn({'action':'subscribe','params':f'Q.{sym}'},ws.sent)
        import sqlite3
        with sqlite3.connect(settings.db_path) as conn:
            self.assertEqual(conn.execute('SELECT side FROM signals').fetchone(),('PUT',))
        tmp.cleanup()
