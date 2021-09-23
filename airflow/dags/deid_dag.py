# USES AIRFLOW CONNECTIONS:
# deid_postgres_connection
# deid_s3_source_connection
# deid_s3_dest_connection

import hashlib
import os
import secrets
from datetime import timedelta
from tempfile import TemporaryDirectory

from airflow import DAG
from airflow.exceptions import AirflowFailException
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago
import scripts.deid.nondestructive_aperio


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
s3destdir = '{{ dag_run.conf["s3destdir"] }}'


def hash_file(filepath):
    """
    Generate sha256 hash of file at filepath.
    """
    hasher = hashlib.sha256()
    with open(filepath, "rb") as f:
        for data in iter(lambda: f.read(128 * 1024), b""):
            hasher.update(data)
    return hasher


def deid_with_hashes(source_s3_key, dest_s3_dir):
    """
    Copies a file from a source S3 location to a temporary location on the
    local filesystem, hashes it, deidentifies the file, hashes that,
    uploads that to a destination S3 location, and returns the hashes.

    Similar to
    airflow.providers.amazon.aws.operators.s3_file_transform.S3FileTransformOperator
    but here I can hash the files before they get deleted.
    """
    import logging
    source_s3 = S3Hook(aws_conn_id="deid_s3_source_connection")
    dest_s3 = S3Hook(aws_conn_id="deid_s3_dest_connection")

    ext = os.path.splitext(source_s3_key)[1]
    dest_s3_key = dest_s3_dir.rstrip("/") + "/" + secrets.token_hex(32) + ext
    logging.info("test info")
    logging.warning("test warning")
    logging.error("test error")

    with TemporaryDirectory() as f_dir:
        infile_path = os.path.join(f_dir, "infile")
        print(f"Downloading {source_s3_key} to temporary location {infile_path}")
        source_s3.get_key(source_s3_key).download_file(infile_path)
        print(f"Downloaded file is {os.path.getsize(infile_path)} bytes.")

        print(f"Hashing source copy")
        hasher1 = hash_file(infile_path)
        print(f"Source file hash: {hasher1.hexdigest()}")

        outfile_path = os.path.join(f_dir, "outfile")
        print(f"De-identifying to new temporary file {outfile_path}")

        try:
            message = nondestructive_aperio.deid_aperio_svs(infile_path, outfile_path)
        except Exception as e:
            raise AirflowFailException("Not a valid Aperio SVS file?") from e

        print(f"De-identification status output: {message}")
        print(f"Output file is {os.path.getsize(outfile_path)} bytes.")

        print(f"Hashing output file")
        hasher2 = hash_file(outfile_path)
        print(f"Output file hash: {hasher2.hexdigest()}")

        print(f"Uploading output file to {dest_s3_key}")
        dest_s3.load_file(filename=outfile_path, key=dest_s3_key)
        print("Done uploading")

    # return gets pushed to xcom
    return {
        "source_key": source_s3_key,
        "dest_key": dest_s3_key,
        "source_hash": hasher1.hexdigest(),
        "dest_hash": hasher2.hexdigest(),
    }


def submit_to_db(**context):
    """
    Add entry for infile, infile_sha256, outfile, outfile_sha256 to
    deidentified_files table in the warehouse database.
    """
    db_hook = PostgresHook(postgres_conn_id="deid_postgres_connection")
    deid_xcom = context["task_instance"].xcom_pull(task_ids="deid_with_hashes")

    print(f"Recording in database")
    db_hook.run(
        """
        INSERT INTO deidentified_files (infile, infile_sha256, outfile, outfile_sha256)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (infile, infile_sha256) DO UPDATE 
        SET (outfile, outfile_sha256) = (EXCLUDED.outfile, EXCLUDED.outfile_sha256);
        """,
        True,
        parameters=(
            deid_xcom["source_key"],
            deid_xcom["source_hash"],
            deid_xcom["dest_key"],
            deid_xcom["dest_hash"],
        ),
    )
    for output in db_hook.conn.notices:
        print(output)


with DAG(
    "image_deid",
    default_args=default_args,
    description="Download image, deid, upload image",
    schedule_interval=None,
) as dag:

    # I really struggled with the decision of which column(s) to make the
    # primary key here, but I think this is right. - Avi
    create_table = PostgresOperator(
        task_id="create_table",
        postgres_conn_id="deid_postgres_connection",
        sql="""
            CREATE TABLE IF NOT EXISTS deidentified_files(
                infile text,
                infile_sha256 text,
                outfile text not null unique,
                outfile_sha256 text not null,
                PRIMARY KEY (infile, infile_sha256)
            );
        """,
    )

    # TODO: This operator does too many things: downloading, hashing,
    # deidentifying, and uploading in one step. It's made this way _right_
    # _now_ because I know that Airflow tasks are capable of being run on
    # different machines from each other depending on which scheduler devops
    # sets up, and setting up shared storage between tasks is yet another
    # devops task that I don't want to have to worry about right now.
    #
    # Once this initial version is working, the next steps should be making
    # sure that tasks will all definitely share storage and then breaking up
    # the "deid_with_hashes" Operator into separate steps for downloading,
    # hashing/deidentifying, uploading with their own retries as appropriate.
    # It will also need a new step at the end, probably using
    # TriggerRule.ALL_DONE or something, to clean up the workspace in case
    # something went wrong since you won't be able to use scoped temporary
    # files like I've used in the deid_with_hashes function.
    #
    # Once that's working, I would also advocate uploading the output file to
    # S3 _after_ writing the record to the database instead of before as it is
    # now, since a record without a file causes less pain than a file without
    # a record.
    # - Avi, Sep 15,2021
    deid = PythonOperator(
        task_id="deid_with_hashes",
        python_callable=deid_with_hashes,
        op_kwargs={
            "source_s3_key": s3sourcepath,
            "dest_s3_dir": s3destdir,
        },
    )

    record = PythonOperator(
        task_id="submit_to_db",
        python_callable=submit_to_db,
        provide_context=True,
    )

    create_table >> deid >> record
