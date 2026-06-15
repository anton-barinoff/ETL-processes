import uuid
import datetime
from airflow import DAG
from airflow.utils.trigger_rule import TriggerRule
from airflow.providers.yandex.operators.yandexcloud_dataproc import (
    DataprocCreateClusterOperator,
    DataprocCreatePysparkJobOperator,
    DataprocDeleteClusterOperator,
)

YC_DP_AZ = 'ru-central1-b'
YC_DP_SSH_PUBLIC_KEY = 'ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABAQDVOYcpz5tyWESiAN8r1mpUERjyvZpqP0Q7+FLQ2svjOab9X6R4TM7cRVPZg0JfCwrW64GMeDRIXc/yosUNFsTxUAjIr/pLVPbBMFKr+yGSeTZ9Vd6A/EigODd+A8b5Bi27xBQYSBQCggvBMR3sDXo1dVh+H70orMDBiOK9ngyEKn2W3NMd484wmHxMMpRDckDwpLT0zeIcJZVI7K0nCVS9ZkuAPWkhCTsR0GqqM0s3eAy2AqMyzuCVUNXA0z7jySNdsWaOgrvtNF2UfvZNjOCL4lmpXWyY9xGUmAYaq1F45Z1Nu0fxS2McBqvMvQqhUpwJdVBXpGvNOW8sZBRF47uh'
YC_DP_SUBNET_ID = 'e2lv911ubiafm9620jc9'
YC_DP_SA_ID = 'ajetst2cje74rj9cf5v2'
YC_DP_METASTORE_URI = '10.0.0.9'
YC_BUCKET = 'bank-credits-bucket'

INPUT_FILE = f's3a://{YC_BUCKET}/credit_applications.csv'
OUTPUT_PATH = f's3a://{YC_BUCKET}/output/'
PYSPARK_SCRIPT = f's3a://{YC_BUCKET}/scripts/create-table.py'

with DAG(
    'DATA_INGEST',
    schedule='@hourly',
    tags=['data-processing-and-airflow'],
    start_date=datetime.datetime.now(),
    max_active_runs=1,
    catchup=False
) as ingest_dag:

    create_spark_cluster = DataprocCreateClusterOperator(
        task_id='dp-cluster-create-task',
        cluster_name=f'tmp-dp-{uuid.uuid4()}',
        ssh_public_keys=YC_DP_SSH_PUBLIC_KEY,
        service_account_id=YC_DP_SA_ID,
        subnet_id=YC_DP_SUBNET_ID,
        s3_bucket=YC_BUCKET,
        zone=YC_DP_AZ,
        cluster_image_version='2.3.0',
        masternode_resource_preset='s2.micro',
        masternode_disk_type='network-hdd',
        masternode_disk_size=50,
        computenode_resource_preset='s2.micro',
        computenode_disk_type='network-hdd',
        computenode_disk_size=50,
        computenode_count=2,
        computenode_max_hosts_count=5,
        services=['YARN', 'SPARK'],
        datanode_count=0,
        properties={
            'spark:spark.hive.metastore.uris': f'thrift://{YC_DP_METASTORE_URI}:9083',
        },
    )

    run_spark_job = DataprocCreatePysparkJobOperator(
    task_id='dp-cluster-pyspark-task',
    main_python_file_uri=PYSPARK_SCRIPT,
    args=[OUTPUT_PATH],
)

    delete_spark_cluster = DataprocDeleteClusterOperator(
        task_id='dp-cluster-delete-task',
        trigger_rule=TriggerRule.ALL_DONE,
    )

    create_spark_cluster >> run_spark_job >> delete_spark_cluster