import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo
from scanner import Scanner, Settings, parse_contract, cutoff_ms

class ScannerTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.sc = Scanner(Settings(db_path=str(Path(self.temp.name)/"scan.db"), slippage=0, fee=0))
        self.now = datetime.now(timezone.utc)
        date = (self.now.astimezone(ZoneInfo("America/New_York")).date() + timedelta(days=3))
        self.sym = f"O:SPY{date:%y%m%d}P00550000"
        self.base = int(self.now.timestamp()*1000) - 3*60_000

    def tearDown(self):
        self.temp.cleanup()

    def signal(self):
        for i, (c, v) in enumerate(zip([1,1,1.02,1.18],[20,24,18,100])):
            t=self.base + i*60000
            self.sc.on_aggregate({"ev":"AM","sym":self.sym,"s":t-60000,"e":t,
                                  "c":c,"op":0.90,"v":v,"av":200+i*100})
        return self.base+3*60000

    def test_contract(self):
        c=parse_contract(self.sym)
        self.assertEqual(c['side'], 'PUT')
        self.assertEqual(c['strike'], 550.)

    def test_alert_and_executable_target(self):
        t=self.signal()
        self.assertIn(self.sym,self.sc.candidates)
        self.assertIsNone(self.sc.on_quote({"sym":self.sym,"t":t-1000,"bp":1.15,"ap":1.25,"bs":10,"as":10}))
        msg=self.sc.on_quote({"sym":self.sym,"t":t+3000,"bp":1.15,"ap":1.25,"bs":10,"as":10})
        self.assertIn('target',msg.lower() if msg else '')
        row=self.sc.store.conn.execute("SELECT status,entry_ask FROM signals").fetchone()
        self.assertEqual(row, ('OPEN',1.25))
        self.sc.on_quote({"sym":self.sym,"t":t+4000,"bp":2.49,"ap":2.61,"bs":10,"as":10})
        self.assertEqual(self.sc.store.summary(), [('OPEN',1)])
        self.sc.on_quote({"sym":self.sym,"t":t+5000,"bp":2.50,"ap":2.61,"bs":10,"as":10})
        self.assertEqual(self.sc.store.summary(), [('HIT_100',1)])

    def test_reject_wide_spread(self):
        t=self.signal()
        msg=self.sc.on_quote({"sym":self.sym,"t":t+1000,"bp":.60,"ap":1.25,"bs":10,"as":10})
        self.assertIsNone(msg)

    def test_no_repeated_alert(self):
        t=self.signal()
        self.sc.on_quote({"sym":self.sym,"t":t+1000,"bp":1.10,"ap":1.20,"bs":10,"as":10})
        self.assertIsNone(self.sc.on_quote({"sym":self.sym,"t":t+2000,"bp":1.12,"ap":1.22,"bs":10,"as":10}))
        self.assertEqual(self.sc.store.summary(), [('OPEN',1)])

if __name__ == '__main__':
    unittest.main()
