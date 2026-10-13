"""DSS150P Lab 4: reproducible breast-cancer classification pipeline.

The DAG only orders and parameterises CLI commands; all logic lives in src/.

schedule=None: the source is a static, checksummed snapshot, so there is nothing new to process on a
timer. Runs are triggered manually (or by CI) when code, config or the source manifest changes.
catchup=False for the same reason: there are no missed intervals to back-fill.

Params (Trigger DAG w/ config):
  ingest_mode     full | incremental     full = copy the snapshot; incremental = checkpointed batches
  model_seed      integer (default 42)   any other seed is expected to FAIL the reproducibility check
  inject_bad_data true | false           ingest a temporary copy without column mean_area (fails at validate_raw)
  fail_task       none | <task_id>       inject a failure in one task
  failure_mode    none | once | always   once = fails first try, recovers on retry
"""
from datetime import datetime, timedelta
import json
from pathlib import Path

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.bash import BashOperator

PROJECT = '/opt/airflow/project'
EVENTS = Path(PROJECT) / 'data' / 'runs' / 'airflow_events.jsonl'
TASKS = ['ingest_source', 'validate_raw', 'transform_staging', 'build_curated', 'load_postgres',
         'validate_curated', 'train_model', 'evaluate_model', 'reproducibility_check', 'publish_outputs']
COMMANDS = {
    'ingest_source': 'ingest',
    'validate_raw': 'validate-raw',
    'transform_staging': 'transform',
    'build_curated': 'build-curated',
    'load_postgres': 'load',
    'validate_curated': 'validate-curated',
    'train_model': 'train',
    'evaluate_model': 'evaluate',
    'reproducibility_check': 'repro-check',
    'publish_outputs': 'publish',
}
# A failed data/quality/reproducibility check will fail again with the same input: do not retry those.
NO_RETRY = {'validate_raw', 'validate_curated', 'reproducibility_check'}


def _record(event, context):
    ti = context['task_instance']
    exc = context.get('exception')
    rec = {'event': event, 'dag_id': ti.dag_id, 'run_id': context['run_id'], 'task_id': ti.task_id,
           'try_number': ti.try_number, 'params': dict(context.get('params') or {}),
           'error': str(exc)[:300] if exc else None, 'logged_at_utc': datetime.utcnow().isoformat() + 'Z'}
    print(f'{event.upper()}:', json.dumps(rec))
    try:
        EVENTS.parent.mkdir(parents=True, exist_ok=True)
        with EVENTS.open('a', encoding='utf-8') as f:
            f.write(json.dumps(rec) + '\n')
    except OSError as err:
        print('could not write events file:', err)


def on_failure(context):
    _record('task_failed', context)


def on_retry(context):
    _record('task_retry', context)


ENV = (f'cd {PROJECT} && export PIPELINE_RUN_ID="{{{{ run_id }}}}" '
       'INGEST_MODE="{{ params.ingest_mode }}" MODEL_SEED="{{ params.model_seed }}" '
       'INJECT_BAD_DATA="{{ params.inject_bad_data }}" FAIL_TASK="{{ params.fail_task }}" '
       'FAIL_MODE="{{ params.failure_mode }}" TRY_NUMBER="{{ ti.try_number }}" && ')

with DAG(
    dag_id='predictive_repro_pipeline',
    description='Breast cancer classification: ingest -> validate -> curate -> load -> train -> evaluate -> reproduce',
    start_date=datetime(2026, 10, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args={'owner': 'dss150p', 'retries': 2, 'retry_delay': timedelta(seconds=20),
                  'execution_timeout': timedelta(minutes=10), 'on_failure_callback': on_failure,
                  'on_retry_callback': on_retry},
    params={
        'ingest_mode': Param('full', enum=['full', 'incremental']),
        'model_seed': Param(42, type='integer'),
        'inject_bad_data': Param('false', enum=['false', 'true']),
        'fail_task': Param('none', enum=['none'] + TASKS),
        'failure_mode': Param('none', enum=['none', 'once', 'always']),
    },
    tags=['dss150p', 'lab4'],
) as dag:
    previous = None
    for task_id in TASKS:
        task = BashOperator(
            task_id=task_id,
            bash_command=ENV + f'python -m src.cli {COMMANDS[task_id]}',
            retries=0 if task_id in NO_RETRY else 2,
            execution_timeout=timedelta(minutes=5) if task_id == 'train_model' else timedelta(minutes=10),
        )
        if previous:
            previous >> task
        previous = task
