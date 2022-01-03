import boto3
import time
import logging
import os; os.chdir(os.path.dirname(os.path.abspath(__file__)))
# TODO paginate results


cloudformation = boto3.client('cloudformation')
redshift = boto3.client('redshift')


def check_stack_status(stack_name):
    stacks = cloudformation.list_stacks(
        StackStatusFilter=['CREATE_IN_PROGRESS', 'CREATE_FAILED', 'CREATE_COMPLETE', 'ROLLBACK_IN_PROGRESS', 'ROLLBACK_FAILED', 'ROLLBACK_COMPLETE', 'DELETE_IN_PROGRESS', 'DELETE_FAILED', 'UPDATE_IN_PROGRESS', 'UPDATE_COMPLETE_CLEANUP_IN_PROGRESS', 'UPDATE_COMPLETE', 'UPDATE_FAILED', 'UPDATE_ROLLBACK_IN_PROGRESS', 'UPDATE_ROLLBACK_FAILED', 'UPDATE_ROLLBACK_COMPLETE_CLEANUP_IN_PROGRESS', 'UPDATE_ROLLBACK_COMPLETE', 'REVIEW_IN_PROGRESS', 'IMPORT_IN_PROGRESS', 'IMPORT_COMPLETE', 'IMPORT_ROLLBACK_IN_PROGRESS', 'IMPORT_ROLLBACK_FAILED', 'IMPORT_ROLLBACK_COMPLETE']
    )['StackSummaries']
    for stack in stacks:
        if stack['StackName'] == stack_name:
            return stack['StackStatus']


def build_stack(stack):
    if check_stack_status(stack) is not None:
        logging.ERROR("Stack already exists")
        raise ValueError("Stack already exists")

    start = time.time()
    cloudformation.create_stack(
        StackName=stack,
        TemplateBody=open("template.yaml").read(),
        OnFailure='DELETE'
    )

    while check_stack_status(stack) == 'CREATE_IN_PROGRESS':
        logging.debug("Waiting for stack")
        time.sleep(5)
    
    final_status = check_stack_status(stack)
    if final_status != 'CREATE_COMPLETE':
        logging.ERROR(f"The formation completed with status: {final_status}.")
        raise ValueError(f"The formation completed with status: {final_status}.")
    logging.INFO(f"Stack creation took {round(time.time() - start, 2)} seconds to complete")


def create_redshift_users(redshift_id):
    cluster_info = redshift.describe_clusters(
        ClusterIdentifier=redshift_id
    )['Clusters'][0]
    return {
        'host': cluster_info['Endpoint']['Address'],
        'port': cluster_info['Endpoint']['Port'],
        'dbname': cluster_info['DBName'],
        'redshift_username': cluster_info['MasterUsername'],
        'redshift_password': 'Password1'
    }


def get_stack_resources(stack):
    resources = cloudformation.list_stack_resources(StackName=stack)['StackResourceSummaries']
    redshift_id = bucket = None
    for resource in resources:
        if resource['ResourceType'] == 'AWS::Redshift::Cluster':
            redshift_id = resource['PhysicalResourceId']
        if resource['ResourceType'] == 'AWS::S3::Bucket':
            bucket = resource['PhysicalResourceId']

    if redshift_id is None or bucket is None:
        raise ValueError
    return redshift_id, bucket


def main(stack):
    # build_stack(stack)
    # create_s3_users(stack)
    redshift_id, bucket = get_stack_resources(stack)
    creds = create_redshift_users(redshift_id)
    creds['bucket'] = bucket
    print(creds)
    exit()


if __name__ == '__main__':
    # logging.basicConfig(
    #     format='[%(name)s, %(levelname)s] %(asctime)s: %(message)s',
    #     datefmt="%Y-%m-%d %H:%M:%S",
    #     level=logging.DEBUG
    # )
    # main('test')
    main('test-library2')