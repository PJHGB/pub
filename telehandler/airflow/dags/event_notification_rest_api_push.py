import logging
import os
from datetime import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator

from plugins.event_router import get_targets_for_event, load_config
from plugins.target_notifier import TargetNotifier
from plugins.vault_secrets_manager import VaultSecretsManager

logger = logging.getLogger(__name__)

CONFIG_PATH = os.path.join(os.path.dirname(__file__), '..', 'config', 'infrastructure.yaml')


def route_and_notify(**kwargs) -> None:
    conf = kwargs.get('dag_run').conf or {}
    event = conf.get('event', {})
    event_type = conf.get('event_type', '')

    if not event_type:
        raise ValueError("dag_run conf must include 'event_type'")

    config = load_config(CONFIG_PATH)

    vault_url = Variable.get('VAULT_URL')
    vault_token = Variable.get('VAULT_TOKEN')
    vault = VaultSecretsManager(url=vault_url, token=vault_token)

    notifier = TargetNotifier(vault=vault, config=config)
    targets = get_targets_for_event(event_type, config)

    if not targets:
        logger.warning("No targets matched event_type='%s'", event_type)
        return

    errors = []
    for target in targets:
        try:
            logger.info("Notifying %s for event_type='%s'", target, event_type)
            notifier.notify(target, event)
        except Exception as exc:
            logger.error("Failed to notify %s: %s", target, exc)
            errors.append((target, exc))

    if errors:
        failed = ', '.join(t for t, _ in errors)
        raise RuntimeError(f"Notification failed for targets: {failed}")


with DAG(
    dag_id='event_notification_rest_api_push',
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=['event', 'rest_api'],
) as dag:
    route_and_notify_task = PythonOperator(
        task_id='route_and_notify',
        python_callable=route_and_notify,
    )
