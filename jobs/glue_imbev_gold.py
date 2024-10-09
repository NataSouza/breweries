import sys
import json
import boto3
import time
from datetime import datetime
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F

# Inicializa o GlueContext
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Inicializando o cliente do DynamoDB
dynamodb = boto3.resource('dynamodb')
parameters_table = dynamodb.Table('parameters')

# Lendo o item 'breweries' da tabela DynamoDB
response = parameters_table.get_item(Key={'table_name': 'breweries'})

# Inicializa a variável para health_check_gold
health_check_gold = None

# Verifica se o item existe
if 'Item' in response and 'health_check_gold' in response['Item']:
    health_check_gold = response['Item']['health_check_gold']

# Converte o timestamp para datetime se não for None ou vazio
if health_check_gold and health_check_gold.strip():
    health_check_datetime = datetime.strptime(health_check_gold, '%Y-%m-%d %H:%M:%S')
else:
    health_check_datetime = None

# Lógica de execução do job
current_datetime = datetime.now()
current_date = current_datetime.date()

# Se health_check_gold for None ou menor que a data atual, executa o job
if health_check_datetime is None or health_check_datetime.date() < current_date:
    # Lê os dados da tabela 'breweries_silver'
    input_path = 's3://imbev-silver/breweries_silver/'
    df = spark.read.parquet(input_path)

    # Realiza a agregação
    aggregated_df = df.groupBy('brewery_type', 'country', 'state') \
                      .agg(F.count('*').cast('int').alias('aggregated'))

    # Escreve os dados agregados no formato Parquet no S3
    output_path = 's3://imbev-gold/breweries_gold/'
    aggregated_df.write.partitionBy('country', 'state') \
                        .mode('overwrite') \
                        .parquet(output_path)

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

    # Executa o comando MSCK REPAIR TABLE para atualizar partições
    database_name = 'layer_gold'
    repair_query = 'MSCK REPAIR TABLE breweries_gold;'
    query_execution_id = run_athena_query(repair_query, database_name)

    # Aguarda a conclusão da consulta
    status = wait_for_query_to_complete(query_execution_id)

    if status == 'SUCCEEDED':
        print("Partições atualizadas com sucesso.")
    else:
        print(f"Erro ao atualizar partições: {status}")

    # Atualizando o timestamp no DynamoDB
    new_health_check_gold = current_datetime.strftime('%Y-%m-%d %H:%M:%S')
    parameters_table.update_item(
        Key={'table_name': 'breweries'},
        UpdateExpression='SET health_check_gold = :val',
        ExpressionAttributeValues={':val': new_health_check_gold}
    )
else:
    print("Nenhuma atualização necessária para health_check_gold.")

# Finaliza o job
job.commit()