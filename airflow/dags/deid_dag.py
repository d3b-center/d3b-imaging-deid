# USES AIRFLOW CONNECTIONS:
# deid_postgres_connection
# deid_s3_source_connection
# deid_s3_dest_connection

import hashlib
import secrets
from datetime import timedelta
from tempfile import TemporaryFile

from airflow import DAG
from airflow.operators import PythonOperator
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

s3_source_path = '{{ dag_run.conf["s3sourcepath"] }}'
s3_dest_dir = '{{ dag_run.conf["s3destdir"] }}'
destfile = secrets.token_hex(32) + ".svs"
s3_dest_path = s3_dest_dir.rstrip("/") + "/" + destfile


def hash_file(filepath):
    """
    Generate sha256 hash of file at filepath.
    """
    hasher = hashlib.sha256()
    with open(filepath, "rb") as f:
        for data in iter(lambda: f.read(128 * 1024), b""):
            hasher.update(data)
    return hasher


def deid_with_hashes(source_s3_key, dest_s3_key):
    """
    Copies a file from a source S3 location to a temporary location on the
    local filesystem, hashes it, deidentifies the file, hashes that,
    uploads that to a destination S3 location, and returns the hashes.

    Similar to
    airflow.providers.amazon.aws.operators.s3_file_transform.S3FileTransformOperator
    but here I can hash the files before they get deleted.
    """
    source_s3 = S3Hook(aws_conn_id="deid_s3_source_connection")
    dest_s3 = S3Hook(aws_conn_id="deid_s3_dest_connection")

    with TemporaryFile("wb") as f_source, TemporaryFile("wb") as f_dest:
        print(f"Downloading {s3_source_path} to temporary copy {f_source.name}")
        source_s3.get_key(s3_source_path).download_fileobj(Fileobj=f_source)
        f_source.flush()

        print(f"Hashing source copy")
        hasher1 = hash_file(f_source.name)
        print(f"Source file {hasher1.name} hash: {hasher1.hexdigest()}")

        # This specifically does Aperio SVS. Want to do other formats as well?
        # You could catch UnrecognizedFile (in nondestructive_aperio) and move on.
        print(f"De-identifying to new temporary file {f_dest.name}")
        message = deid_aperio_svs(f_source.name, f_dest.name)
        f_dest.flush()
        print(f"De-identification status output: {message}")

        print(f"Hashing de-identified file")
        hasher2 = hash_file(f_dest.name)
        print(f"De-identified file {hasher2.name} hash: {hasher2.hexdigest()}")

        print(f"Uploading de-dentified file to {dest_s3_key}")
        dest_s3.load_file(filename=f_dest.name, key=dest_s3_key)
        print("Done uploading")

    # return gets pushed to xcom
    return {"source": hasher1.hexdigest(), "dest": hasher2.hexdigest()}


def submit_to_db(source_s3_key, dest_s3_key, **kwargs):
    """
    Add entry for infile, infile_sha256, outfile, outfile_sha256 to 
    deidentified_svs_files table in the warehouse database.
    """
    db_hook = PostgresHook(postgres_conn_id="deid_postgres_connection")
    hashes = kwargs["task_instance"].xcom_pull(task_ids="deid_with_hashes")
    print(f"Recording in database")
    db_hook.run(
        """
        INSERT INTO deidentified_svs_files (infile, infile_sha256, outfile, outfile_sha256)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (infile, infile_sha256) DO UPDATE 
        SET (outfile, outfile_sha256) = (EXCLUDED.outfile, EXCLUDED.outfile_sha256);
        """,
        True,
        parameters=(
            source_s3_key,
            hashes["source"],
            dest_s3_key,
            hashes["dest"],
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
            CREATE TABLE IF NOT EXISTS deidentified_svs_files(
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
            "source_s3_key": s3_source_path,
            "dest_s3_key": s3_dest_path,
        },
    )

    record = PythonOperator(
        task_id="submit_to_db",
        python_callable=submit_to_db,
        op_kwargs={
            "source_s3_key": s3_source_path,
            "dest_s3_key": s3_dest_path,
        },
    )

    create_table >> deid >> record
