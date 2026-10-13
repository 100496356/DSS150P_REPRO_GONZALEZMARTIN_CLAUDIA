"""One command per pipeline step. Airflow calls these commands; all logic lives in the src modules.

  python -m src.cli <command> [options]
  validate-env | init-db | ingest | validate-raw | transform | build-curated | load | validate-curated
  train | evaluate | repro-check | publish | benchmark | load-partition | run-all
"""
import argparse
import json
import os
import sys

import pandas as pd

from src import ingest as ing
from src.config import DB, ROOT, SETTINGS, path_for
from src.observability import current_run_id, stage, update_summary, utc_now


def maybe_fail(step: str) -> None:
    """Deliberate, reversible failure for Airflow demos (FAIL_TASK / FAIL_MODE set by DAG params)."""
    if os.getenv('FAIL_TASK') != step:
        return
    mode, tries = os.getenv('FAIL_MODE', 'none'), int(os.getenv('TRY_NUMBER', '1'))
    if mode == 'always' or (mode == 'once' and tries <= 1):
        raise RuntimeError(f'Injected failure in step "{step}" (mode={mode}, try={tries})')


def mode() -> str:
    return os.getenv('INGEST_MODE', 'full')


def seed() -> int:
    return int(os.getenv('MODEL_SEED', SETTINGS['model']['seed']))


def _split_path(run_id: str):
    return path_for('curated_dir') / f'split_{run_id}.json'


def cmd_validate_env(_a, run_id):
    print('project root:', ROOT)
    print('database:', f"{DB['user']}@{DB['host']}:{DB['port']}/{DB['dbname']}")
    from src.db import query
    print('postgres:', query('SELECT version()')[0][0][:60])
    print('source checksum OK:', ing.verify_source()[:16], '...')


def cmd_init_db(_a, run_id):
    from src.db import init_db
    init_db()
    print('schema ready (curated, audit)')


def cmd_ingest(a, run_id):
    with stage(run_id, 'ingest_source') as info:
        maybe_fail('ingest_source')
        m = a.mode or mode()
        if os.getenv('INJECT_BAD_DATA', 'false').lower() == 'true':
            bad = path_for('raw_dir').parent / 'tmp' / 'bad_copy_missing_column.csv'
            bad.parent.mkdir(parents=True, exist_ok=True)
            pd.read_csv(path_for('source_file')).drop(columns=['mean_area']).to_csv(bad, index=False)
            print('FAILURE INJECTION: ingesting a temporary copy without column mean_area (checksum check skipped)')
            res = ing.ingest_full(run_id, source=bad, verify=False)
            # keep the expected file name so the next step reads this copy
            snap = path_for('raw_dir') / f'run_id={run_id}'
            (snap / bad.name).replace(snap / path_for('source_file').name)
        elif m == 'incremental':
            res = ing.ingest_incremental(run_id, max_batches=a.max_batches, force=a.force)
        else:
            res = ing.ingest_full(run_id)
        info.update(res)
    update_summary(run_id, ingest_mode=m)


def cmd_validate_raw(_a, run_id):
    from src.validate import validate_raw, write_report
    with stage(run_id, 'validate_raw') as info:
        maybe_fail('validate_raw')
        raw = ing.read_raw(run_id, mode())
        rep = validate_raw(raw)
        write_report(run_id, 'validate_raw', rep)
        info.update(rows=rep['rows'], problems=rep['problems'])
        update_summary(run_id, counts={'raw': len(raw)})
        if not rep['ok']:
            raise ValueError(f"Raw data violates the contract: {rep['problems']}")


def cmd_transform(_a, run_id):
    from src.transform import to_staging, write_layer
    with stage(run_id, 'transform_staging') as info:
        maybe_fail('transform_staging')
        staging, quarantine = to_staging(ing.read_raw(run_id, mode()), run_id)
        write_layer(staging, 'staging_dir', 'breast_cancer_staging')
        qdir = path_for('quarantine_dir')
        qdir.mkdir(parents=True, exist_ok=True)
        quarantine.to_csv(qdir / f'quarantine_{run_id}.csv', index=False)
        info.update(staging_rows=len(staging), quarantined=len(quarantine),
                    duplicates_removed=staging.attrs.get('duplicates_removed', 0))
        update_summary(run_id, counts={'staging': len(staging), 'quarantine': len(quarantine)})


def cmd_build_curated(_a, run_id):
    from src.storage import write_partitions
    from src.transform import check_or_write_split_manifest, to_curated, write_layer
    with stage(run_id, 'build_curated') as info:
        maybe_fail('build_curated')
        staging = pd.read_parquet(path_for('staging_dir') / 'breast_cancer_staging.parquet')
        curated, split = to_curated(staging, run_id, seed())
        write_layer(curated, 'curated_dir', 'breast_cancer_features')
        _split_path(run_id).write_text(json.dumps(split))
        info.update(curated_rows=len(curated), train_rows=split['train_rows'], test_rows=split['test_rows'],
                    split_manifest=check_or_write_split_manifest(split), partitions=write_partitions(curated))
        update_summary(run_id, counts={'curated': len(curated), 'train': split['train_rows'], 'test': split['test_rows']})


def _curated():
    return pd.read_parquet(path_for('curated_dir') / 'breast_cancer_features.parquet')


def _split(run_id):
    p = _split_path(run_id)
    if not p.exists():
        raise FileNotFoundError(f'No split for run {run_id}: run build-curated first')
    return json.loads(p.read_text())


def cmd_load(_a, run_id):
    from src.db import init_db, record_run, upsert_curated
    with stage(run_id, 'load_postgres') as info:
        maybe_fail('load_postgres')
        init_db()
        stats = upsert_curated(_curated())
        record_run(run_id, status='RUNNING', curated_rows=stats['table_rows'])
        info.update(stats)


def cmd_validate_curated(_a, run_id):
    from src.db import query
    from src.validate import contract, validate_curated, write_report
    with stage(run_id, 'validate_curated') as info:
        maybe_fail('validate_curated')
        cur = _curated()
        rep = validate_curated(cur, contract()['feature_columns'])
        total, distinct = query('SELECT count(*), count(DISTINCT record_id) FROM curated.breast_cancer_features')[0]
        rep['postgres'] = {'rows': total, 'distinct_record_id': distinct}
        if total != distinct or total < len(cur):
            rep['ok'] = False
            rep['problems'].append(f'postgres rows={total}, distinct={distinct}, curated={len(cur)}')
        write_report(run_id, 'validate_curated', rep)
        info.update(checks=rep['checks'], postgres=rep['postgres'], problems=rep['problems'])
        if not rep['ok']:
            raise ValueError(f"Curated data failed quality checks: {rep['problems']}")


def cmd_train(_a, run_id):
    from src.train import train
    with stage(run_id, 'train_model') as info:
        maybe_fail('train_model')
        info.update(train(_curated(), _split(run_id), run_id))


def cmd_evaluate(_a, run_id):
    from src.evaluate import evaluate
    with stage(run_id, 'evaluate_model') as info:
        maybe_fail('evaluate_model')
        res = evaluate(_curated(), _split(run_id), run_id)
        info.update(metrics=res['metrics'], test_rows=res['test_rows'])
        update_summary(run_id, counts={'predictions': res['test_rows']}, metrics=res['metrics'])


def cmd_repro_check(_a, run_id):
    from src.evaluate import reproducibility_check
    with stage(run_id, 'reproducibility_check') as info:
        maybe_fail('reproducibility_check')
        comp = reproducibility_check()
        info.update(passed=comp['passed'], metrics={k: (v['baseline'], v['reproduced']) for k, v in comp['metrics'].items()})
        update_summary(run_id, reproducibility_passed=comp['passed'])
        if not comp['passed']:
            raise ValueError('Reproduced metrics are outside the declared tolerance (see outputs/metrics_comparison.json)')


def cmd_publish(_a, run_id):
    from src.db import record_run, upsert_predictions
    with stage(run_id, 'publish_outputs') as info:
        maybe_fail('publish_outputs')
        preds = pd.read_csv(path_for('outputs_dir') / 'predictions.csv')
        n = upsert_predictions(preds)
        summ = update_summary(run_id, status='SUCCESS', finished_at_utc=utc_now())
        c = summ['counts']
        record_run(run_id, status='SUCCESS', finished_at_utc=utc_now(), raw_rows=c.get('raw'), staging_rows=c.get('staging'),
                   curated_rows=c.get('curated'), train_rows=c.get('train'), test_rows=c.get('test'),
                   prediction_rows=c.get('predictions'), accuracy=summ.get('metrics', {}).get('accuracy'),
                   reproducibility_passed=summ.get('reproducibility_passed'), message='published')
        info.update(predictions_upserted=n, summary_counts=c)


def cmd_benchmark(a, run_id):
    from src.storage import run_benchmark
    with stage(run_id, 'benchmark') as info:
        res = run_benchmark(_curated(), a.repeats)
        print(res.to_string(index=False))
        info.update(rows=len(res))


def cmd_load_partition(a, run_id):
    from src.db import connect, init_db, upsert_curated
    from src.storage import read_partition
    with stage(run_id, 'load_partition') as info:
        init_db()
        part = read_partition(a.diagnosis)
        cols = [c for c in _curated().columns]
        stats = upsert_curated(part[cols])
        key = f'diagnosis={a.diagnosis}'
        with connect() as conn:
            conn.execute('INSERT INTO audit.partition_loads (partition_key, row_count, pipeline_run_id) VALUES (%s,%s,%s) '
                         'ON CONFLICT (partition_key) DO UPDATE SET row_count = EXCLUDED.row_count, '
                         'pipeline_run_id = EXCLUDED.pipeline_run_id, loaded_at_utc = now()', [key, len(part), run_id])
        info.update(partition=key, **stats)


STEPS = ['ingest', 'validate-raw', 'transform', 'build-curated', 'load', 'validate-curated',
         'train', 'evaluate', 'repro-check', 'publish']


def main(argv=None):
    p = argparse.ArgumentParser(prog='python -m src.cli')
    p.add_argument('command')
    p.add_argument('--mode', choices=['full', 'incremental'])
    p.add_argument('--max-batches', type=int)
    p.add_argument('--force', action='store_true', help='full refresh: ignore the incremental checkpoint')
    p.add_argument('--repeats', type=int)
    p.add_argument('--diagnosis', default='M', choices=['M', 'B'])
    a = p.parse_args(argv)
    if a.mode:
        os.environ['INGEST_MODE'] = a.mode
    cmds = {'validate-env': cmd_validate_env, 'init-db': cmd_init_db, 'ingest': cmd_ingest, 'validate-raw': cmd_validate_raw,
            'transform': cmd_transform, 'build-curated': cmd_build_curated, 'load': cmd_load,
            'validate-curated': cmd_validate_curated, 'train': cmd_train, 'evaluate': cmd_evaluate,
            'repro-check': cmd_repro_check, 'publish': cmd_publish, 'benchmark': cmd_benchmark,
            'load-partition': cmd_load_partition}
    if a.command == 'run-all':
        run_id = current_run_id(new=True)
        update_summary(run_id, status='RUNNING')
        for step in STEPS:
            cmds[step](a, run_id)
        print('run-all finished, run id:', run_id)
        return 0
    if a.command not in cmds:
        p.error(f'unknown command {a.command}')
    run_id = current_run_id(new=(a.command == 'ingest'))
    try:
        cmds[a.command](a, run_id)
    except Exception as exc:
        print(f'[error] {a.command} failed: {exc}', file=sys.stderr)
        try:
            from src.db import record_run
            record_run(run_id, status='FAILED', message=f'{a.command}: {str(exc)[:250]}')
        except Exception:
            pass
        raise
    return 0


if __name__ == '__main__':
    sys.exit(main())
