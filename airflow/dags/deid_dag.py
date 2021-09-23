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
from scripts.deid.nondestructive_aperio import deid_aperio_svs

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
    Generate sha256 and md5 hashes of file at filepath.
    """
    sha256 = hashlib.sha256()
    md5 = hashlib.md5()
    with open(filepath, "rb") as f:
        for data in iter(lambda: f.read(128 * 1024), b""):
            sha256.update(data)
            md5.update(data)
    return sha256.hexdigest(), md5.hexdigest()


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

    # TemporaryDirectory context makes sure that the local files get deleted
    with TemporaryDirectory() as f_dir:
        infile_path = os.path.join(f_dir, "infile")
        logging.info(f"Downloading {source_s3_key} to temporary location {infile_path}")
        source_s3.get_key(source_s3_key).download_file(infile_path)
        logging.info(f"Downloaded file is {os.path.getsize(infile_path)} bytes.")

        logging.info(f"Hashing source copy")
        sha256_1, md5_1 = hash_file(infile_path)
        logging.info(f"Source file sha256: {sha256_1}")
        logging.info(f"Source file md5: {md5_1}")

        outfile_path = os.path.join(f_dir, "outfile")
        logging.info(f"De-identifying to new temporary file {outfile_path}")

        # NOTE: This only does SVS files. To handle other/additional file
        # types, you'll want to change this and only raise the
        # AirflowFailException at the end if none succeed.
        try:
            message = deid_aperio_svs(infile_path, outfile_path)
        except Exception as e:
            raise AirflowFailException("Not a valid Aperio SVS file?") from e

        logging.info(f"De-identification status output: {message}")
        logging.info(f"Output file is {os.path.getsize(outfile_path)} bytes.")

        logging.info(f"Hashing output file")
        sha256_2, md5_2 = hash_file(outfile_path)
        logging.info(f"Output file sha256: {sha256_2}")
        logging.info(f"Output file md5: {md5_2}")

        ext = os.path.splitext(source_s3_key)[1]
        dest_s3_key = dest_s3_dir.rstrip("/") + "/" + sha256_2 + ext
        # or if you want randomly generated output filenames, use
        # dest_s3_key = dest_s3_dir.rstrip("/") + "/" + secrets.token_hex(32) + ext

        logging.info(f"Uploading output file to {dest_s3_key}")
        dest_s3.load_file(
            filename=outfile_path,
            key=dest_s3_key,
            replace=True,
            acl_policy="bucket-owner-full-control",
        )
        logging.info("Done uploading")

    # return gets pushed to xcom
    return {
        "source_key": source_s3_key,
        "dest_key": dest_s3_key,
        "source_sha256": sha256_1,
        "source_md5": md5_1,
        "dest_sha256": sha256_2,
        "dest_md5": md5_2,
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
        INSERT INTO deidentified_files (infile, infile_sha256, infile_md5, outfile, outfile_sha256, outfile_md5)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (infile, infile_sha256, infile_md5) DO UPDATE 
        SET (outfile, outfile_sha256, outfile_md5) = (EXCLUDED.outfile, EXCLUDED.outfile_sha256, EXCLUDED.outfile_md5);
        """,
        True,
        parameters=(
            deid_xcom["source_key"],
            deid_xcom["source_sha256"],
            deid_xcom["source_md5"],
            deid_xcom["dest_key"],
            deid_xcom["dest_sha256"],
            deid_xcom["dest_md5"],
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
                infile_md5 text,
                outfile text not null unique,
                outfile_sha256 text not null,
                outfile_md5 text not null,
                PRIMARY KEY (infile, infile_sha256, infile_md5)
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
