from ast import arguments
from datetime import datetime

from emr_serverless.operators.emr import (
    EmrServerlessCreateApplicationOperator,
    EmrServerlessDeleteApplicationOperator,
    EmrServerlessStartJobOperator,
)

from airflow import DAG
from airflow.models import Variable

# Replace these with your correct values
JOB_ROLE_ARN = Variable.get("emr_serverless_job_role")
S3_LOGS_BUCKET = Variable.get("emr_serverless_log_bucket")

DEFAULT_MONITORING_CONFIG = {
    "monitoringConfiguration": {
        "s3MonitoringConfiguration": {"logUri": f"s3://{S3_LOGS_BUCKET}/logs/"}
    },
}

raw_to_staged = [str({'job_name': 'raw_to_staged', 'input_path': 's3://cjmm-datalake-raw', 'output_path': 's3://cjmm-datalake-staged'})]
staged_to_curated = [str({'job_name': 'staged_to_curated', 'input_path': 's3://cjmm-datalake-staged', 'output_path': 's3://cjmm-datalake-curated'})]

jobs = []

steps_for_process = {
    'raw_to_staged': raw_to_staged,
    'staged_to_curated': staged_to_curated
}

with DAG(
    dag_id="processar_dados_movielens_emr_serverless",
    schedule_interval=None,
    start_date=datetime(2021, 1, 1),
    tags=["example"],
    catchup=False,
) as dag:
    create_app = EmrServerlessCreateApplicationOperator(
        task_id="create_spark_app",
        job_type="SPARK",
        release_label="emr-6.6.0",
        config={"name": "sample-job"},
    )

    application_id = create_app.output

    for name, step_config in steps_for_process.items():
        jobs.append(
            EmrServerlessStartJobOperator(
                task_id=name,
                application_id=application_id,
                execution_role_arn=JOB_ROLE_ARN,
                job_driver={
                    "sparkSubmit": {
                        "entryPoint": "s3://cjmm-code-spark/latest/main.py",
                        "entryPointArguments": step_config,
                        "sparkSubmitParameters": "--conf spark.submit.pyFiles=s3://cjmm-code-spark/latest/modules.zip  --conf spark.executor.cores=1 --conf spark.executor.memory=4g --conf spark.driver.cores=1 --conf spark.driver.memory=4g --conf spark.executor.instances=1",
                    }
                },
                configuration_overrides=DEFAULT_MONITORING_CONFIG,
            )
        )

    delete_app = EmrServerlessDeleteApplicationOperator(
        task_id="delete_app",
        application_id=application_id,
        trigger_rule="all_done",
    )

    create_app >> jobs[0] >> jobs[-1] >> delete_app
