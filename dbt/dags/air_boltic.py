from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import timedelta

# =============================================================================
# 1. Global Failure Callback (The "Messenger")
# =============================================================================
def slack_alert_on_failure(context):
    """
    This function runs automatically whenever ANY task in this DAG fails.
    It sends a notification to Slack with the task name and error log link.
    """
    # In a real setup, you would use the SlackWebhookOperator or requests library here
    task_instance = context.get('task_instance')
    print(f"🚨 ALERT: Task {task_instance.task_id} failed! Sending Slack notification...")
    # Code to send JSON payload to Slack Webhook URL goes here.

# =============================================================================
# 2. Default Arguments
# =============================================================================
default_args = {
    'owner': 'analytics_engineering',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': slack_alert_on_failure, # <-- Hooks up the global alert
}

# =============================================================================
# 3. The DAG Definition
# =============================================================================
with DAG(
    dag_id='air_boltic_nightly_build',
    default_args=default_args,
    description='Nightly dbt build for Air Boltic analytics platform',
    schedule_interval='0 3 * * *', # Runs daily at 3:00 AM UTC
    start_date="2025-01-01",
    catchup=False,
    tags=['dbt', 'core', 'air_boltic'],
) as dag:

    # --- Configuration ---
    # Point to where your dbt project lives in the Airflow container
    DBT_PROJECT_DIR = "/opt/airflow/dbt/air_boltic"
    DBT_PROFILES_DIR = "/opt/airflow/dbt"
    
    # Environment variables for dbt to connect to Databricks
    # These are usually injected via Airflow Connections or Kubernetes Secrets
    env_vars = {
        "DBT_PROFILES_DIR": DBT_PROFILES_DIR,
        "DATABRICKS_HOST": "{{ var.value.databricks_host }}",
        "DATABRICKS_HTTP_PATH": "{{ var.value.databricks_http_path }}",
        "DATABRICKS_TOKEN": "{{ var.value.databricks_token }}",
    }

    # =========================================================================
    # Task 1: The Gatekeeper (Source Freshness)
    # =========================================================================
    # Checks if data in S3/Unity Catalog is fresh. 
    # If this fails (Exit Code 1), the pipeline STOPS here to save cost/time.
    dbt_source_freshness = BashOperator(
        task_id='dbt_source_freshness',
        bash_command=f'dbt source freshness --project-dir {DBT_PROJECT_DIR}',
        env=env_vars
    )

    # =========================================================================
    # Task 2: The Builder (Build & Test)
    # =========================================================================
    # Uses `dbt build` to run models and tests in dependency order.
    # 1. Runs incremental tests on Source (Gatekeeper #2)
    # 2. Runs Staging Views
    # 3. Runs Fact/Dim Tables
    # 4. Runs Tests on Facts/Dims
    dbt_build = BashOperator(
        task_id='dbt_build',
        bash_command=f'dbt build --project-dir {DBT_PROJECT_DIR}',
        env=env_vars
    )

    # =========================================================================
    # 4. Dependencies
    # =========================================================================
    
    dbt_source_freshness >> dbt_build