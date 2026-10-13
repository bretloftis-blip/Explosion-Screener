"""Read-only paper signal audit for Convexity100. No statistical confidence claims."""
import argparse
import csv
from datetime import datetime, timezone
import sqlite3
from zoneinfo import ZoneInfo

FIELDS = ['ticker', 'underlying', 'side', 'strike', 'expiration', 'signal_ms',
          'alert_ms', 'entry_ask', 'entry_bid', 'spread_pct', 'priority',
          'cutoff_ms', 'status', 'target100_ms', 'target200_ms',
          'target300_ms', 'stop_ms']


def pretty(ms):
    if ms is None:
        return ''
    return datetime.fromtimestamp(ms/1000, timezone.utc).astimezone(
        ZoneInfo('America/New_York')).strftime('%Y-%m-%d %H:%M:%S ET')


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--db', default='data/scanner.sqlite')
    parser.add_argument('--csv', help='Export auditable signal records to CSV')
    args = parser.parse_args()
    with sqlite3.connect(f'file:{args.db}?mode=ro', uri=True) as db:
        rows = [dict(zip(FIELDS, row)) for row in db.execute(
            'SELECT '+','.join(FIELDS)+' FROM signals ORDER BY alert_ms DESC')]
    pending = sum(r['status']=='OPEN' for r in rows)
    settled = sum(r['status'] in {'COMPLETE','STOP_FIRST','TIMEOUT'} for r in rows)
    success = sum(r['target100_ms'] is not None and not r['stop_ms'] and r['status'] in {'COMPLETE','STOP_FIRST','TIMEOUT'} for r in rows)
    stop = sum(r['status']=='STOP_FIRST' for r in rows)
    timeouts = sum(r['status']=='TIMEOUT' for r in rows)
    print(f'Total paper alerts: {len(rows)} | pending: {pending} | 100% before stop: {success} | stops: {stop} | timeouts: {timeouts}')
    print(f'Settled success fraction (unadjusted for slippage or fill sizes): {success}/{settled}' if settled else 'No settled alerts yet')
    for r in rows[:20]:
        print(pretty(r['alert_ms']), r['ticker'], r['status'], 'entry ask', r['entry_ask'], 'target100', pretty(r['target100_ms']))
    if args.csv:
        with open(args.csv, 'w', newline='', encoding='utf8') as handle:
            writer=csv.DictWriter(handle, fieldnames=FIELDS)
            writer.writeheader(); writer.writerows(rows)
        print('Exported:', args.csv)

if __name__=='__main__':
    main()
