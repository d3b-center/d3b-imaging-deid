import uuid
from datetime import timedelta

from airflow import DAG
from airflow.providers.amazon.aws.operators.s3_file_transform import (
    S3FileTransformOperator
)
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email": ["example@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 5,
    "retry_delay": timedelta(minutes=1),
    "start_date": days_ago(1),
    "schedule_interval": None,
}

s3sourcepath = '{{ dag_run.conf["s3sourcepath"] }}'
s3destdir= '{{ dag_run.conf["s3destdir"] }}'
destfile = str(uuid.uuid4()) + ".svs"
s3destpath = s3destdir.rstrip("/") + "/" + destfile

with DAG(
    "image_deid",
    default_args=default_args,
    description="Download image, deid, upload image",
    schedule_interval=None,
) as dag:

    create_table = PostgresOperator(
        task_id="create_table",
        postgres_conn_id="postgres_connection",
        sql="""
            CREATE TABLE IF NOT EXISTS deidentified_svs_files(
                infile text primary key,
                outfile text not null unique
            );
        """
    )

    record = PostgresOperator(
        task_id="record_association",
        postgres_conn_id="postgres_connection",
        sql="""
            INSERT INTO deidentified_svs_files (infile, outfile) VALUES (%s, %s)
            ON CONFLICT (infile) DO UPDATE 
            SET outfile = excluded.outfile;
            """,
        parameters=(s3sourcepath, destfile),
    )

    deid_svs = S3FileTransformOperator(
        task_id="deid",
        source_aws_conn_id='s3_connection',
        dest_aws_conn_id='s3_connection',
        source_s3_key=s3sourcepath,
        dest_s3_key=s3destpath,
        replace=True,
        transform_script="scripts/deid/tifftool_aperio.py"
    )

    create_table >> record >> deid_svs
