"""Offline regression tests for acquisition integrity and storage equivalence."""
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pyarrow.parquet as pq
from src import acquisition as a
from src.storage import prepare, read_query, LAYOUTS, QUERIES


class FakeClient:
    def __init__(self):
        self.calls = []
    def fetch(self, endpoint, quarter=None):
        self.calls.append((endpoint, quarter))
        row = {c: None for c in a.FIELDS[endpoint]}
        row['CERT'] = 1
        if quarter:
            row.update(REPDTE=quarter, ASSET=1234.56, DEP=1000, EQ=-2.0, STNAME='Test')
        elif endpoint == 'failures':
            row.update(ID='event-1', FAILDATE='01/01/2000', RESTYPE='FAILURE')
        return [row], {'api_total': 1, 'fields': a.FIELDS[endpoint]}


class Tests(unittest.TestCase):
    def test_scope(self):
        q = a.quarters()
        self.assertEqual((len(q), q[0], q[-1]), (170, '19840331', '20260630'))

    def test_invalid_types_and_duplicate_keys(self):
        with self.assertRaises((ValueError, TypeError)):
            a.to_table([{'CERT': 'broken'}], 'financials')
        table = a.to_table([{'CERT': 1, 'REPDTE': '20230331'}] * 2, 'financials')
        with self.assertRaises(ValueError):
            a.validate_table(table, 'financials', '20230331')

    @patch.object(a, 'archive_dictionaries', return_value={})
    def test_resume_corruption_missing_quarter_and_storage(self, _):
        with tempfile.TemporaryDirectory() as d:
            root, client = Path(d) / 'data', FakeClient()
            args = (root, '2023-03-31', '2023-06-30')
            self.assertTrue(a.acquire(*args, client=client)['passed'])
            self.assertEqual(len(client.calls), 4)
            a.acquire(*args, client=client)
            self.assertEqual(len(client.calls), 4)
            (root / a.relative_path('financials', '20230331')).write_bytes(b'corrupted')
            a.acquire(*args, client=client)
            self.assertEqual(len(client.calls), 5)
            work = Path(d) / 'bench'
            prepare(root, work)
            for query in QUERIES:
                reference = read_query(work, 'csv', query, 2023)
                for layout in LAYOUTS[1:]:
                    self.assertTrue(reference.equals(read_query(work, layout, query, 2023)))
            manifest = json.loads((root / 'manifest.json').read_text())
            del manifest['records']['financials/20230630']
            a.save_json(root / 'manifest.json', manifest)
            with self.assertRaises(RuntimeError):
                a.audit(*args)

    def test_failure_missing_cert_is_preserved(self):
        table = a.to_table([{'ID': 'source-1', 'CERT': None, 'FAILDATE': '01/01/2000'}], 'failures')
        a.validate_table(table, 'failures')
        self.assertEqual(table['CERT'].null_count, 1)
        self.assertEqual(table['SOURCE_ID'].to_pylist(), ['source-1'])

    def test_pagination_empty_page_is_failure(self):
        class Response:
            def __init__(self, rows): self.rows = rows
            def json(self): return {'meta': {'total': 2}, 'data': self.rows}
        client = a.Client(pause=0)
        with patch.object(client, 'get', side_effect=[Response([{'data': {'CERT': 1}}]), Response([])]):
            with self.assertRaises(RuntimeError):
                client.fetch('institutions', page_size=1)

    def test_pagination_source_change_is_failure(self):
        class Response:
            def __init__(self, total): self.total = total
            def json(self): return {'meta': {'total': self.total}, 'data': [{'data': {'CERT': 1}}]}
        client = a.Client(pause=0)
        with patch.object(client, 'get', side_effect=[Response(2), Response(3)]):
            with self.assertRaises(RuntimeError):
                client.fetch('institutions', page_size=1)


if __name__ == '__main__':
    unittest.main()
