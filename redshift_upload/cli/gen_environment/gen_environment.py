import boto3
import time
import logging
import click
import os
from pathlib import Path
import sys
import psycopg2
from typing import List, Dict, Tuple

try:
    from ...credential_store import credential_store
except ImportError:
    sys.path.insert(0, Path(__file__).parents[2])
    from credential_store import credential_store
os.chdir(Path(__file__).parent)
# TODO paginate results


cloudformation = boto3.client("cloudformation")
redshift = boto3.client("redshift")
iam = boto3.client("iam")
s3 = boto3.resource("s3")


def check_stack_status(stack_name: str) -> str:
    stacks = cloudformation.list_stacks(
        StackStatusFilter=[
            "CREATE_IN_PROGRESS",
            "CREATE_FAILED",
            "CREATE_COMPLETE",
            "ROLLBACK_IN_PROGRESS",
            "ROLLBACK_FAILED",
            "ROLLBACK_COMPLETE",
            "DELETE_IN_PROGRESS",
            "DELETE_FAILED",
            "UPDATE_IN_PROGRESS",
            "UPDATE_COMPLETE_CLEANUP_IN_PROGRESS",
            "UPDATE_COMPLETE",
            "UPDATE_FAILED",
            "UPDATE_ROLLBACK_IN_PROGRESS",
            "UPDATE_ROLLBACK_FAILED",
            "UPDATE_ROLLBACK_COMPLETE_CLEANUP_IN_PROGRESS",
            "UPDATE_ROLLBACK_COMPLETE",
            "REVIEW_IN_PROGRESS",
            "IMPORT_IN_PROGRESS",
            "IMPORT_COMPLETE",
            "IMPORT_ROLLBACK_IN_PROGRESS",
            "IMPORT_ROLLBACK_FAILED",
            "IMPORT_ROLLBACK_COMPLETE",
        ]
    )["StackSummaries"]
    for stack in stacks:
        if stack["StackName"] == stack_name:
            return stack["StackStatus"]


def build_stack(stack: str) -> None:
    if check_stack_status(stack) is not None:
        logging.error("Stack already exists")
        raise ValueError("Stack already exists")

    start = time.time()
    cloudformation.create_stack(
        StackName=stack,
        TemplateBody=open("template.yaml").read(),
        OnFailure="DELETE",
        Capabilities=["CAPABILITY_IAM"],
    )

    while check_stack_status(stack) == "CREATE_IN_PROGRESS":
        logging.debug("Waiting for stack")
        time.sleep(5)

    final_status = check_stack_status(stack)
    if final_status != "CREATE_COMPLETE":
        logging.ERROR(f"The formation completed with status: {final_status}.")
        raise ValueError(f"The formation completed with status: {final_status}.")
    logging.info(
        f"Stack creation took {round(time.time() - start, 2)} seconds to complete"
    )


def create_redshift_users(redshift_id: str) -> List[Dict]:
    cluster_info = redshift.describe_clusters(ClusterIdentifier=redshift_id)[
        "Clusters"
    ][0]
    base = {
        "host": cluster_info["Endpoint"]["Address"],
        "port": cluster_info["Endpoint"]["Port"],
        "dbname": cluster_info["DBName"],
        "password": "Password1",
    }
    ret = [
        {**base, "user": user}
        for user in [cluster_info["MasterUsername"], "analyst1", "analyst2"]
    ]
    with psycopg2.connect(
        **ret[0],
        connect_timeout=5,
    ) as conn:
        cursor = conn.cursor()
        for user in ["analyst1", "analyst2"]:
            cursor.execute(f"create user {user} with password 'Password1'")
            cursor.execute(f"grant usage on schema public to {user}")
            cursor.execute(f"grant all on all tables in schema public to {user}")
            cursor.execute(f"grant select on table stv_locks to {user}")
            cursor.execute(f"grant select on table svv_table_info to {user}")
    return ret


def get_stack_resources(stack: str) -> Tuple[str, str, List[Dict]]:
    resources = cloudformation.list_stack_resources(StackName=stack)[
        "StackResourceSummaries"
    ]
    redshift_id = bucket = None
    usernames = []
    for resource in resources:
        if resource["ResourceType"] == "AWS::Redshift::Cluster":
            redshift_id = resource["PhysicalResourceId"]
        elif resource["ResourceType"] == "AWS::S3::Bucket":
            bucket = resource["PhysicalResourceId"]
        elif resource["ResourceType"] == "AWS::IAM::User":
            usernames.append(resource["PhysicalResourceId"])

    if redshift_id is None or bucket is None or not usernames:
        raise ValueError
    return redshift_id, bucket, usernames


def get_access_keys(username: str) -> Dict[str, str]:
    creds = iam.create_access_key(UserName=username)["AccessKey"]
    return {"access_key": creds["AccessKeyId"], "secret_key": creds["SecretAccessKey"]}


def create_stack(stack: str) -> None:
    build_stack(stack)
    redshift_id, bucket, usernames = get_stack_resources(stack)

    credential_store.set_store("test-library")
    credential_store.credentials.clear()

    creds = {}
    creds["s3"] = get_access_keys(usernames[0])
    creds["constants"] = {
        "default_schema": "public",
        "bucket": bucket,
        "logging_endpoint": None,
        "logging_endpoint_type": None,
        "get_table_lock": True,
    }
    redshift_users = create_redshift_users(redshift_id)
    for redshift in redshift_users:
        credential_store.credentials.add({**creds, "db": redshift})


def delete_stack(stack: str) -> None:
    _, bucket, _ = get_stack_resources(stack)
    for obj in s3.Bucket(bucket).objects.filter():
        s3.Object(bucket, obj.key).delete()
    cloudformation.delete_stack(StackName=stack)


@click.command()
@click.option(
    "--stack-name",
    default="test-library",
    help="The name of the stack to build/destroy",
)
@click.option(
    "--destroy",
    is_flag=True,
    type=bool,
    help="If this flag is set, the stack will be removed. Otherwise the stack will be created",
)
@click.option("--logging-level", default="ERROR", type=str)
def gen_environment(stack_name: str, destroy: bool, logging_level: str) -> None:
    "Sets up a basic Redshift environment for testing"
    logging.basicConfig(
        format="[%(name)s, %(levelname)s] %(asctime)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        level=logging_level,
    )
    if not destroy:
        create_stack(stack_name)
    if destroy:
        delete_stack(stack_name)


if __name__ == "__main__":
    gen_environment()
