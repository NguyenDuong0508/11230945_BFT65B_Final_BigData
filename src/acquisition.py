"""Acquire an auditable FDIC snapshot; preserve financial values without cleaning.

Sources: course fdic_download_starter.py (pagination/resume design), FDIC API.
See README for attribution, scope, and snapshot limitations.
"""
from __future__ import annotations
import argparse
from concurrent.futures import ThreadPoolExecutor
from collections import deque
import hashlib
import json
import time
from datetime import date, datetime, timezone
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests
import yaml

BASE = 'https://api.fdic.gov/banks'
FIELDS = {
    'financials': 'CERT REPDTE STNAME ASSET DEP EQ CHBAL SC LNLSGR LNATRES NCLNLS NETINC ROA ROE NIMY RBC1AAJ RBCRWAJ BRO'.split(),
    'failures': 'CERT NAME CITYST FAILDATE FAILYR COST QBFASSET QBFDEP RESTYPE'.split(),
    'institutions': 'CERT NAME STNAME BKCLASS ACTIVE ESTYMD ENDEFYMD CHANGEC1 INSDATE'.split(),
}
DICTIONARIES = {'financials': 'risview_properties.yaml', 'failures': 'failure_properties.yaml', 'institutions': 'institution_properties.yaml'}
TEXT = {'STNAME', 'NAME', 'CITYST', 'RESTYPE', 'BKCLASS', 'ACTIVE', 'CHANGEC1', 'ESTYMD', 'ENDEFYMD', 'INSDATE', 'FAILDATE'}


def utcnow():
    return datetime.now(timezone.utc).isoformat()


def digest(path):
    h = hashlib.sha256()
    with Path(path).open('rb') as f:
        for block in iter(lambda: f.read(1024 * 1024), b''):
            h.update(block)
    return h.hexdigest()


def save_json(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + '.tmp')
    tmp.write_text(json.dumps(value, indent=2, ensure_ascii=False, allow_nan=False), encoding='utf-8')
    tmp.replace(path)


def quarters(start='1984-03-31', end='2026-06-30'):
    first, last = date.fromisoformat(start), date.fromisoformat(end)
    values = [date(y, m, d).strftime('%Y%m%d') for y in range(first.year, last.year + 1)
              for m, d in [(3, 31), (6, 30), (9, 30), (12, 31)]
              if first <= date(y, m, d) <= last]
    if not values:
        raise ValueError('The requested interval contains no quarter end.')
    return values


def schema(endpoint):
    fields = FIELDS[endpoint] + (['SOURCE_ID'] if endpoint == 'failures' else [])
    return pa.schema([(c, pa.string() if c in TEXT or c == 'SOURCE_ID' else pa.int64() if c in {'CERT', 'FAILYR', 'REPDTE'} else pa.float64()) for c in fields])


class Client:
    def __init__(self, pause=0.15):
        self.session = requests.Session()
        self.session.headers['User-Agent'] = 'BFT65B-acquisition/1.0 (academic project)'
        self.pause = pause

    def get(self, url, params=None):
        for attempt in range(6):
            try:
                r = self.session.get(url, params=params, timeout=(20, 180))
                if r.status_code in {429, 500, 502, 503, 504}:
                    r.raise_for_status()
                elif r.status_code >= 400:
                    raise ValueError(f'Non-retryable HTTP {r.status_code}: {r.url}')
                time.sleep(self.pause)
                return r
            except requests.RequestException:
                if attempt == 5:
                    raise
                time.sleep(min(30, 2 ** attempt))

    def fetch(self, endpoint, quarter=None, page_size=10000):
        rows, offset, expected, source_index = [], 0, None, None
        while True:
            params = {'fields': ','.join(FIELDS[endpoint]), 'limit': page_size, 'offset': offset,
                      'sort_by': 'CERT', 'sort_order': 'ASC', 'format': 'json'}
            if quarter:
                params['filters'] = f'REPDTE:{quarter}'
            response = self.get(f'{BASE}/{endpoint}', params)
            payload = response.json()
            total = int(payload['meta']['total'])
            index = payload['meta'].get('index')
            if expected is None:
                expected, source_index = total, index
            elif total != expected or index != source_index:
                raise RuntimeError('Source changed during pagination; rerun this table/quarter.')
            page = [record['data'] for record in payload['data']]
            if not page and offset < expected:
                raise RuntimeError(f'Premature empty API page: {endpoint}, offset {offset}/{expected}')
            rows.extend(page)
            offset += len(page)
            if offset >= expected:
                break
        if len(rows) != expected or expected == 0:
            raise RuntimeError(f'Invalid acquisition count: {len(rows)} versus {expected}')
        return rows, {'api_total': expected, 'source_index': source_index,
                      'endpoint': f'{BASE}/{endpoint}', 'filters': params.get('filters'),
                      'fields': FIELDS[endpoint], 'downloaded_at_utc': utcnow()}


def archive_dictionaries(root, client):
    """Record the source definitions used to select fields; fail on unknown fields."""
    root = Path(root)
    definitions = {}
    for endpoint, filename in DICTIONARIES.items():
        path = root / 'metadata' / filename
        path.parent.mkdir(parents=True, exist_ok=True)
        if not path.exists():
            content = client.get(f'{BASE}/docs/{filename}').content
            tmp = path.with_suffix('.tmp')
            tmp.write_bytes(content)
            tmp.replace(path)
        obj = yaml.safe_load(path.read_text())
        properties = obj['properties']['data']['properties']
        missing = set(FIELDS[endpoint]) - set(properties)
        if missing:
            raise ValueError(f'Fields absent from FDIC dictionary: {endpoint}: {missing}')
        definitions[endpoint] = {'url': f'{BASE}/docs/{filename}', 'sha256': digest(path),
                                 'fields': {c: properties[c] for c in FIELDS[endpoint]}}
    save_json(root / 'metadata' / 'selected_fields.json', definitions)
    return definitions


def to_table(rows, endpoint):
    """Strict conversion; missing fields stay null and invalid numbers raise errors."""
    arrays = []
    for field in schema(endpoint):
        values = [r.get('ID' if field.name == 'SOURCE_ID' else field.name) for r in rows]
        if pa.types.is_string(field.type):
            values = [None if x is None else str(x) for x in values]
        else:
            values = pd.to_numeric(pd.Series(values), errors='raise')
        arrays.append(pa.array(values, type=field.type, from_pandas=True, safe=True))
    return pa.Table.from_arrays(arrays, schema=schema(endpoint))


def validate_table(table, endpoint, quarter=None):
    if table.schema != schema(endpoint):
        raise ValueError(f'Schema mismatch: {endpoint}')
    if endpoint != 'failures' and table['CERT'].null_count:
        raise ValueError('Null CERT; acquisition cannot be certified.')
    keys = ['CERT', 'REPDTE'] if endpoint == 'financials' else ['CERT'] if endpoint == 'institutions' else ['SOURCE_ID']
    frame = table.select(keys).to_pandas()
    if frame.isna().any().any():
        raise ValueError(f'Null source key {keys}')
    if frame.duplicated().any():
        raise ValueError(f'Duplicate source key {keys}; investigate without dropping records.')
    if quarter and (frame['REPDTE'].isna().any() or not frame['REPDTE'].eq(int(quarter)).all()):
        raise ValueError('Unexpected reporting date in quarter response.')


def relative_path(endpoint, quarter):
    return f'financials/year={quarter[:4]}/q{quarter[4:6]}.parquet' if quarter else f'{endpoint}.parquet'


def file_is_valid(root, record, endpoint, quarter):
    try:
        path = Path(root) / record['path']
        if digest(path) != record['sha256']:
            return False
        table = pq.ParquetFile(path).read()
        validate_table(table, endpoint, quarter)
        return table.num_rows == record['rows'] == record['api_total']
    except (OSError, KeyError, ValueError, pa.ArrowException):
        return False


def acquire(root, start='1984-03-31', end='2026-06-30', client=None, workers=4):
    injected_client = client
    root, client = Path(root), client or Client()
    if workers not in range(1, 5):
        raise ValueError('Use between one and four download workers.')
    root.mkdir(parents=True, exist_ok=True)
    archive_dictionaries(root, client)
    manifest_path = root / 'manifest.json'
    manifest = json.loads(manifest_path.read_text()) if manifest_path.exists() else {'version': 1, 'records': {}}
    jobs = [('financials', q) for q in quarters(start, end)] + [('failures', None), ('institutions', None)]
    pending = []
    for endpoint, quarter in jobs:
        key = f'{endpoint}/{quarter or "all"}'
        previous = manifest['records'].get(key)
        if previous and previous['fields'] == FIELDS[endpoint] and file_is_valid(root, previous, endpoint, quarter):
            print(f'VERIFIED RESUME {key}: {previous["rows"]:,}', flush=True)
            continue
        pending.append((endpoint, quarter))
    def fetch(job):
        # Each worker owns its HTTP session; no requests.Session is shared across threads.
        return (injected_client or Client(pause=0.5)).fetch(*job)
    with ThreadPoolExecutor(max_workers=workers) as pool:
        iterator = iter(pending)
        queue = deque()
        for _ in range(workers):
            job = next(iterator, None)
            if job is not None:
                queue.append((job, pool.submit(fetch, job)))
        while queue:
            (endpoint, quarter), future = queue.popleft()
            rows, record = future.result()
            key = f'{endpoint}/{quarter or "all"}'
            table = to_table(rows, endpoint)
            validate_table(table, endpoint, quarter)
            path = root / relative_path(endpoint, quarter)
            path.parent.mkdir(parents=True, exist_ok=True)
            tmp = path.with_suffix('.tmp')
            pq.write_table(table, tmp, compression='zstd', compression_level=3, row_group_size=65536)
            if not pq.ParquetFile(tmp).read().equals(table):
                raise RuntimeError('Parquet round-trip mismatch.')
            tmp.replace(path)
            record.update(path=str(path.relative_to(root)), rows=table.num_rows, sha256=digest(path), bytes=path.stat().st_size,
                          null_counts={c: table[c].null_count for c in table.column_names})
            manifest['records'][key] = record
            save_json(manifest_path, manifest)
            print(f'DOWNLOADED {key}: {table.num_rows:,}', flush=True)
            job = next(iterator, None)
            if job is not None:
                queue.append((job, pool.submit(fetch, job)))
    return audit(root, start, end)


def audit(root, start='1984-03-31', end='2026-06-30'):
    root = Path(root)
    manifest = json.loads((root / 'manifest.json').read_text())
    expected = [('financials', q) for q in quarters(start, end)] + [('failures', None), ('institutions', None)]
    problems, counts, bank_ids = [], {}, set()
    quarter_rows = []
    for endpoint, quarter in expected:
        key = f'{endpoint}/{quarter or "all"}'
        record = manifest['records'].get(key)
        if not record or not file_is_valid(root, record, endpoint, quarter):
            problems.append(key)
            continue
        counts[endpoint] = counts.get(endpoint, 0) + record['rows']
        if quarter:
            quarter_rows.append({'quarter': quarter, 'rows': record['rows'], 'api_total': record['api_total']})
            bank_ids.update(pq.ParquetFile(root / record['path']).read(columns=['CERT'])['CERT'].to_pylist())
    expected_paths = {relative_path(e, q) for e, q in expected}
    actual_paths = {str(p.relative_to(root)) for p in root.rglob('*.parquet')}
    extras = sorted(actual_paths - expected_paths)
    auxiliary_notes = {}
    for endpoint in ['failures', 'institutions']:
        path = root / f'{endpoint}.parquet'
        if path.exists():
            table = pq.ParquetFile(path).read()
            auxiliary_notes[endpoint] = {'null_CERT': table['CERT'].null_count}
            if endpoint == 'failures':
                auxiliary_notes[endpoint]['resolution_counts'] = table['RESTYPE'].to_pandas().value_counts(dropna=False).to_dict()
    result = {'auxiliary_source_notes': auxiliary_notes, 'checked_at_utc': utcnow(), 'start': start, 'end': end,
              'full_course_scope': start == '1984-03-31' and end == '2026-06-30',
              'expected_quarters': len(quarters(start, end)), 'verified_quarters': len(quarter_rows),
              'rows': counts, 'distinct_financial_banks': len(bank_ids), 'quarter_rows': quarter_rows,
              'missing_or_invalid': problems, 'unexpected_parquet_files': extras,
              'passed': not problems and not extras,
              'scope_note': 'Completeness is relative to the recorded API snapshot and selected fields; historical revisions are not reconstructed.'}
    save_json(root / 'audit.json', result)
    if not result['passed']:
        raise RuntimeError(f'Acquisition audit failed; inspect {root / "audit.json"}')
    return result


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--root', required=True)
    parser.add_argument('--start', default='1984-03-31')
    parser.add_argument('--end', default='2026-06-30')
    args = parser.parse_args()
    print(json.dumps(acquire(args.root, args.start, args.end), indent=2))
