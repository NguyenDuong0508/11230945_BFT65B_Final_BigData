"""Controlled CSV/Parquet storage experiment on the verified Financials snapshot."""
from __future__ import annotations
import argparse
import importlib.metadata
import json
import os
import platform
import random
import psutil
import shutil
import subprocess
import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.csv as csv
import pyarrow.dataset as ds
import pyarrow.parquet as pq

from .acquisition import audit, save_json, schema

LAYOUTS = ['csv', 'parquet_snappy', 'parquet_zstd', 'year_zstd']
QUERIES = ['full_scan', 'projection', 'year_filter']
PROJECT = ['CERT', 'REPDTE', 'ASSET', 'DEP', 'EQ']


def source_table(root):
    files = sorted((Path(root) / 'financials').rglob('*.parquet'))
    # Explicit schema avoids deriving the Hive year column from acquisition paths.
    return ds.dataset([str(p) for p in files], format='parquet', schema=schema('financials')).to_table()


def prepare(root, work):
    work = Path(work)
    work.mkdir(parents=True, exist_ok=True)
    table = source_table(root)
    csv.write_csv(table, work / 'financials.csv')
    for codec in ['snappy', 'zstd']:
        pq.write_table(table, work / f'{codec}.parquet', compression=codec, row_group_size=65536)
    folder = work / 'year_zstd'
    if folder.exists():
        shutil.rmtree(folder)
    years = pc.divide_checked(table['REPDTE'], pa.scalar(10000, pa.int64()))
    for year in sorted(set(years.to_pylist())):
        subset = table.filter(pc.equal(years, year))
        target = folder / f'year={year}' / 'part.parquet'
        target.parent.mkdir(parents=True, exist_ok=True)
        pq.write_table(subset, target, compression='zstd', row_group_size=65536)
    # The checkpoint layout is also measured for size/file count, but is not a fifth timing candidate.
    return table.num_rows


def read_query(work, layout, query, year):
    work = Path(work)
    columns = schema('financials').names if query == 'full_scan' else PROJECT
    lower, upper = year * 10000 + 101, year * 10000 + 1231
    if layout == 'csv':
        table = csv.read_csv(work / 'financials.csv', convert_options=csv.ConvertOptions(
            column_types=schema('financials'), include_columns=columns,
            strings_can_be_null=True, quoted_strings_can_be_null=False))
        if query == 'year_filter':
            table = table.filter(pc.and_(pc.greater_equal(table['REPDTE'], lower), pc.less_equal(table['REPDTE'], upper)))
        return table
    if layout == 'year_zstd':
        dataset = ds.dataset(work / 'year_zstd', format='parquet', partitioning='hive')
    else:
        dataset = ds.dataset(work / ('snappy.parquet' if layout == 'parquet_snappy' else 'zstd.parquet'), format='parquet')
    condition = None
    if query == 'year_filter':
        condition = (ds.field('REPDTE') >= lower) & (ds.field('REPDTE') <= upper)
        if layout == 'year_zstd':
            condition = condition & (ds.field('year') == year)
    return dataset.to_table(columns=columns, filter=condition)


def worker(work, layout, query, year):
    pa.set_cpu_count(2)
    pa.set_io_thread_count(2)
    start = time.perf_counter()
    table = read_query(work, layout, query, year)
    elapsed = time.perf_counter() - start
    return {'layout': layout, 'query': query, 'seconds': elapsed, 'rows': table.num_rows}


def benchmark(root, work, output, repeats=3, year=2009, start='1984-03-31', end='2026-06-30'):
    if repeats < 3:
        raise ValueError('Use at least three measured repetitions.')
    verification = audit(root, start, end)
    t0 = time.perf_counter()
    rows = prepare(root, work)
    prep_seconds = time.perf_counter() - t0
    # Compare every selected value before timing; CSV decimal round trips allow tiny floating error.
    equivalence = []
    for query in QUERIES:
        reference = read_query(work, 'csv', query, year).to_pandas().sort_values(['CERT', 'REPDTE']).reset_index(drop=True)
        if reference.empty:
            raise ValueError('The selected benchmark year is outside the acquired snapshot.')
        for layout in LAYOUTS[1:]:
            candidate = read_query(work, layout, query, year).to_pandas().sort_values(['CERT', 'REPDTE']).reset_index(drop=True)
            pd.testing.assert_frame_equal(reference, candidate, check_exact=False, rtol=1e-12, atol=1e-12)
            equivalence.append({'query': query, 'layout': layout, 'matched': True, 'rows': len(reference)})
    del reference, candidate
    env = dict(os.environ, OMP_NUM_THREADS='2', OPENBLAS_NUM_THREADS='2', MKL_NUM_THREADS='2')
    def run(layout, query):
        cmd = [sys.executable, '-m', 'src.storage', '--worker', '--work', str(work), '--layout', layout, '--query', query, '--year', str(year)]
        child = subprocess.Popen(cmd, env=env, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        process = psutil.Process(child.pid)
        peak = 0
        while child.poll() is None:
            try:
                peak = max(peak, process.memory_info().rss)
            except psutil.NoSuchProcess:
                pass
            time.sleep(0.002)
        stdout, stderr = child.communicate()
        if child.returncode:
            raise RuntimeError(f'Benchmark worker failed: {stderr}')
        result = json.loads(stdout)
        result['sampled_peak_process_rss_mib'] = peak / 2**20
        return result
    for layout in LAYOUTS:
        for query in QUERIES:
            run(layout, query)  # one unmeasured warm-up per configuration
    order = [(layout, query, rep) for rep in range(repeats) for layout in LAYOUTS for query in QUERIES]
    random.Random(42).shuffle(order)
    timings = []
    for layout, query, rep in order:
        result = run(layout, query)
        result['repeat'] = rep + 1
        timings.append(result)
    summary = []
    for layout in LAYOUTS:
        for query in QUERIES:
            samples = [x for x in timings if x['layout'] == layout and x['query'] == query]
            summary.append({'layout': layout, 'query': query,
                            'median_seconds': float(np.median([x['seconds'] for x in samples])),
                            'min_seconds': min(x['seconds'] for x in samples),
                            'max_seconds': max(x['seconds'] for x in samples),
                            'median_sampled_peak_process_rss_mib': float(np.median([x['sampled_peak_process_rss_mib'] for x in samples]))})
    paths = {'csv': [Path(work) / 'financials.csv'], 'parquet_snappy': [Path(work) / 'snappy.parquet'],
             'parquet_zstd': [Path(work) / 'zstd.parquet'], 'year_zstd': list((Path(work) / 'year_zstd').rglob('*.parquet')),
             'acquisition_checkpoints': list((Path(root) / 'financials').rglob('*.parquet'))}
    sizes = [{'layout': k, 'bytes': sum(p.stat().st_size for p in v), 'files': len(v)} for k, v in paths.items()]
    result = {'scope': {'start': start, 'end': end, 'rows': rows, 'full_course_scope': verification['full_course_scope']},
              'year_filter': year, 'repeats': repeats, 'seed': 42, 'arrow_cpu_threads': 2, 'arrow_io_threads': 2,
              'python': platform.python_version(), 'platform': platform.platform(), 'cpu': platform.processor(),
              'logical_cpus': os.cpu_count(), 'versions': {p: importlib.metadata.version(p) for p in ['numpy', 'pandas', 'pyarrow']},
              'preparation_seconds': prep_seconds, 'sizes': sizes, 'equivalence': equivalence, 'summary': summary, 'timings': timings,
              'protocol': 'Same fields/rows, Arrow readers, local disk, fresh process per run, one unmeasured warm-up, randomized order. Timed region is materialized read/filter only; process startup and validation excluded. OS cache is not flushed. RSS is sampled by the parent every 2 ms over the worker lifetime, includes imports/interpreter, and may miss short peaks; it is not incremental allocation. Results do not compare pandas with Polars and do not simulate a Spark cluster.'}
    save_json(output, result)
    return result


if __name__ == '__main__':
    p = argparse.ArgumentParser()
    p.add_argument('--worker', action='store_true')
    p.add_argument('--work', required=True)
    p.add_argument('--layout', choices=LAYOUTS)
    p.add_argument('--query', choices=QUERIES)
    p.add_argument('--year', type=int, default=2009)
    a = p.parse_args()
    if not a.worker:
        p.error('Use benchmark() from the notebook; this CLI is the isolated measurement worker.')
    print(json.dumps(worker(a.work, a.layout, a.query, a.year)))
