import sys
import json
import boto3
import time
import hashlib
from datetime import datetime
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

# Inicializa o GlueContext
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Função para hash
def hash_value(value):
    if value:
        return hashlib.sha256(value.encode('utf-8')).hexdigest()
    return None

# UDF para aplicar a função hash ao DataFrame
hash_udf = F.udf(hash_value, StringType())

# Inicializando o cliente do DynamoDB
dynamodb = boto3.resource('dynamodb')
parameters_table = dynamodb.Table('parameters')

# Lendo o item 'breweries' da tabela DynamoDB
response = parameters_table.get_item(Key={'table_name': 'breweries'})

# Inicializa a variável para health_check_silver_anom
health_check_silver_anom = None

# Verifica se o item existe
if 'Item' in response and 'health_check_silver_anom' in response['Item']:
    health_check_silver_anom = response['Item']['health_check_silver_anom']

# Converte o timestamp para datetime se não for None ou vazio
if health_check_silver_anom and health_check_silver_anom.strip():
    health_check_datetime = datetime.strptime(health_check_silver_anom, '%Y-%m-%d %H:%M:%S')
else:
    health_check_datetime = None

# Lógica de execução do job
current_datetime = datetime.now()
current_date = current_datetime.date()  # Pega apenas a data (ano, mês, dia)

# Se health_check_silver_anom for None ou menor que a data atual, executa o job
if health_check_datetime is None or health_check_datetime.date() < current_date:
    # Lê os dados do arquivo JSON
    input_path = 's3://imbev-bronze/file/breweries_data.json'
    df = spark.read.json(input_path)

    # Anonimiza os campos 'name' e 'phone'
    df = df.withColumn('name', hash_udf(F.col('name'))) \
           .withColumn('phone', hash_udf(F.col('phone')))

    # Escreve os dados no formato Parquet no S3
    output_path = 's3://imbev-silver/breweries_silver_anom/'
    df.write.partitionBy("country", "state").mode("overwrite").parquet(output_path)

    # Função para executar a consulta Athena
    def run_athena_query(query, database):
        athena_client = boto3.client('athena')
        response = athena_client.start_query_execution(
            QueryString=query,
            QueryExecutionContext={'Database': database},
            ResultConfiguration={
                'OutputLocation': 's3://athena-results-breweries/Unsaved/',
            }
        )
        return response['QueryExecutionId']

    def wait_for_query_to_complete(query_execution_id):
        athena_client = boto3.client('athena')
        while True:
            response = athena_client.get_query_execution(QueryExecutionId=query_execution_id)
            status = response['QueryExecution']['Status']['State']
            if status in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
                return status
            time.sleep(2)

    # Executa o comando MSCK REPAIR TABLE
    database_name = 'layer_silver'
    repair_query = 'MSCK REPAIR TABLE breweries_silver_anom;'
    query_execution_id = run_athena_query(repair_query, database_name)

    # Aguarda a conclusão da consulta
    status = wait_for_query_to_complete(query_execution_id)

    if status == 'SUCCEEDED':
        print("Partições atualizadas com sucesso.")
    else:
        print(f"Erro ao atualizar partições: {status}")

    # Atualizando o timestamp no DynamoDB
    new_health_check_silver_anom = current_datetime.strftime('%Y-%m-%d %H:%M:%S')
    parameters_table.update_item(
        Key={'table_name': 'breweries'},
        UpdateExpression='SET health_check_silver_anom = :val',
        ExpressionAttributeValues={':val': new_health_check_silver_anom}
    )
else:
    print("Nenhuma atualização necessária para health_check_silver_anom.")

# Finaliza o job
job.commit()