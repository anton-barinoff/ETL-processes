import uuid
import datetime
from airflow import DAG
from airflow.utils.trigger_rule import TriggerRule
from airflow.providers.yandex.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocCreatePysparkJobOperator,
    DataprocDeleteClusterOperator,
)

YC_DP_SSH_PUBLIC_KEY = 'ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABAQDVOYcpz5tyWESiAN8r1mpUERjyvZpqP0Q7+FLQ2svjOab9X6R4TM7cRVPZg0JfCwrW64GMeDRIXc/yosUNFsTxUAjIr/pLVPbBMFKr+yGSeTZ9Vd6A/EigODd+A8b5Bi27xBQYSBQCggvBMR3sDXo1dVh+H70orMDBiOK9ngyEKn2W3NMd484wmHxMMpRDckDwpLT0zeIcJZVI7K0nCVS9ZkuAPWkhCTsR0GqqM0s3eAy2AqMyzuCVUNXA0z7jySNdsWaOgrvtNF2UfvZNjOCL4lmpXWyY9xGUmAYaq1F45Z1Nu0fxS2McBqvMvQqhUpwJdVBXpGvNOW8sZBRF47uh'
YC_DP_SUBNET_ID = 'e2l8b73j2g23ehdeiqrk'
YC_DP_SA_ID = 'ajettegm8r9sm04gsne4'
YC_DP_METASTORE_URI = '10.0.0.9'
YC_BUCKET = 'bank-credits-bucket'

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
        cluster_description='Временный кластер для выполнения PySpark-задания',
        ssh_public_keys=YC_DP_SSH_PUBLIC_KEY,
        service_account_id=YC_DP_SA_ID,
        subnet_id=YC_DP_SUBNET_ID,
        s3_bucket=YC_BUCKET,
        zone='ru-central1-b',
        cluster_image_version='2.3.0',
        masternode_resource_preset='s2.micro',
        masternode_disk_type='network-hdd',
        masternode_disk_size=20,
        computenode_resource_preset='s2.micro',
        computenode_disk_type='network-hdd',
        computenode_disk_size=20,
        computenode_count=1,
        services=['YARN', 'SPARK'],
        datanode_count=0,
        properties={
            'spark:spark.hive.metastore.uris': f'thrift://{YC_DP_METASTORE_URI}:9083',
        },
    )

    poke_spark_processing = DataprocCreatePysparkJobOperator(
        task_id='dp-cluster-pyspark-task',
        main_python_file_uri=f's3a://{YC_BUCKET}/scripts/create-table.py',
        cluster_id=create_spark_cluster.cluster_id,
        properties={
            "spark.submit.deployMode": "client",
            "spark.master": "local[*]",
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
            "spark.hadoop.fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider",
        },
    )
    delete_spark_cluster = DataprocDeleteClusterOperator(
        task_id='dp-cluster-delete-task',
        trigger_rule=TriggerRule.ALL_DONE,
    )

    create_spark_cluster >> poke_spark_processing >> delete_spark_cluster