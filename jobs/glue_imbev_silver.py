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

# Inicializa a variável para health_check_silver
health_check_silver = None

# Verifica se o item existe
if 'Item' in response and 'health_check_silver' in response['Item']:
    health_check_silver = response['Item']['health_check_silver']

# Converte o timestamp para datetime se não for None ou vazio
if health_check_silver and health_check_silver.strip():
    health_check_datetime = datetime.strptime(health_check_silver, '%Y-%m-%d %H:%M:%S')
else:
    health_check_datetime = None

# Lógica de execução do job
current_datetime = datetime.now()
current_date = current_datetime.date()

# Se health_check_silver for None ou menor que a data atual, executa o job
if health_check_datetime is None or health_check_datetime.date() < current_date:
    # Lê os dados do arquivo JSON
    input_path = 's3://imbev-bronze/file/breweries_data.json'
    df = spark.read.json(input_path)

    # Escreve os dados no formato Parquet no S3
    output_path = 's3://imbev-silver/breweries_silver/'
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
    repair_query = 'MSCK REPAIR TABLE breweries_silver;'
    query_execution_id = run_athena_query(repair_query, database_name)

    # Aguarda a conclusão da consulta
    status = wait_for_query_to_complete(query_execution_id)

    if status == 'SUCCEEDED':
        print("Partições atualizadas com sucesso.")
    else:
        print(f"Erro ao atualizar partições: {status}")

    # Atualizando o timestamp no DynamoDB
    new_health_check_silver = current_datetime.strftime('%Y-%m-%d %H:%M:%S')
    parameters_table.update_item(
        Key={'table_name': 'breweries'},
        UpdateExpression='SET health_check_silver = :val',
        ExpressionAttributeValues={':val': new_health_check_silver}
    )

else:
    print("Nenhuma atualização necessária para health_check_silver.")

# Finaliza o job
job.commit()