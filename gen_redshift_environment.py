from botocore.client import Config
import boto3
import json
import botocore
from pprint import pprint
import time
import datetime
aws_creds = json.load(open('aws_creds.json'))
redshift = boto3.client(
    'redshift', 
    region_name='us-east-2',
    aws_access_key_id=aws_creds['aws_access_key_id'],
    aws_secret_access_key=aws_creds['aws_secret_access_key'],
    config=Config(connect_timeout=5),
)
s3 = boto3.resource(
    's3', 
    aws_access_key_id=aws_creds['aws_access_key_id'],
    aws_secret_access_key=aws_creds['aws_secret_access_key'],
    config=Config(connect_timeout=5),
)
bucket_name = 'test-bucket-2834523'


def create_resources():
    redshift.create_cluster(
        ClusterIdentifier='test-cluster',
        NodeType='dc2.large',
        MasterUsername='master',
        MasterUserPassword='Password1',
        DBName='dev',
        ClusterType='single-node'
    )
    s3.create_bucket(
        Bucket=bucket_name,
        CreateBucketConfiguration={
            'LocationConstraint': 'us-east-2',
        },
    )


def check_redshift_up():
    for cluster in redshift.describe_clusters()['Clusters']:
        if cluster['ClusterIdentifier'] != 'test-cluster':
            continue
        if cluster['ClusterAvailabilityStatus'] != 'Available':
            print(f"Cluster {cluster['ClusterIdentifier']} is currently {cluster['ClusterAvailabilityStatus']} ({datetime.datetime.today().strftime('%H:%M:%S')}).\nIt was initially created: {cluster.get('ClusterCreateTime')}")
            time.sleep(5)
            return None
        else:
            print("Cluster is up")
            return cluster


def delete_resources():
    try:
        redshift.delete_cluster(ClusterIdentifier="test-cluster", SkipFinalClusterSnapshot=True)
    except redshift.exceptions.ClusterNotFoundFault:
        print("Redshift cluster did not exist")
    try:
        bucket = s3.Bucket(bucket_name)
        for key in bucket.objects.all():
            key.delete()
        bucket.delete()
    except boto3.client('s3').exceptions.NoSuchBucket:  # bro, WTF
        print("S3 bucket didn't exist")

def gen_test_creds(cluster_info):
    x = {
        "redshift_username": "master",
        "redshift_password": "Password1",
        "access_key": aws_creds['aws_access_key_id'],
        "secret_key": aws_creds['aws_secret_access_key'],
        "bucket": bucket_name,
        "host": cluster_info['Endpoint']['Address'],
        "dbname": "dev",
        "port": cluster_info['Endpoint']['Port']
    }
    with open("test.json", "w") as f:
        json.dump(x, f, indent=4)


def main():
    create_resources()
    while True:
        cluster_info = check_redshift_up()
        if cluster_info is not None:
            break
    gen_test_creds(cluster_info)
    delete_resources()


if __name__ == '__main__':
    main()