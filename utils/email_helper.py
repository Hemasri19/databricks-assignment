from airflow.exceptions import AirflowFailException
from airflow.sdk import Variable, get_current_context
import requests
import logging

logger = logging.getLogger(__name__)

def notify_email_onfailure(title: str, message_body: str):
    """
    Send a Slack notification on DAG failure or Databricks job failure
    """
    webhook_url = Variable.get("slack_webhook_id")
    context = get_current_context()  

    dag_id = context["dag"].dag_id
    run_id = context["run_id"]
    task_id = context["task_instance"].task_id
    log_url = context["task_instance"].log_url

    message = f"""
        :red_circle: *Airflow Alert*
        *Title:* {title}
        DAG: `{dag_id}`
        Task: `{task_id}`
        Run: `{run_id}`
        Check logs: {log_url}
        *Details:* {message_body}
    """

    try:
        response = requests.post(
            webhook_url,
            json={"text": message},
            headers={"Content-Type": "application/json"}
        )
        if response.status_code != 200:
            logger.error(f"Slack notification failed: {response.status_code}, {response.text}")
    except Exception as e:
        logger.error(f"Slack notification error: {str(e)}")
        raise AirflowFailException(f"Failed to send Slack notification: {str(e)}")
