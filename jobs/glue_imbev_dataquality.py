import sys
import boto3
from datetime import datetime
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

# Inicializa a variável para health_check_dataquality
health_check_dataquality = None

# Verifica se o item existe
if 'Item' in response and 'health_check_dataquality' in response['Item']:
    health_check_dataquality = response['Item']['health_check_dataquality']

# Converte o timestamp para datetime se não for None ou vazio
if health_check_dataquality and health_check_dataquality.strip():
    health_check_datetime = datetime.strptime(health_check_dataquality, '%Y-%m-%d %H:%M:%S')
else:
    health_check_datetime = None

# Lógica de execução do job
current_datetime = datetime.now()
current_date = current_datetime.date()

# Se health_check_dataquality for None ou menor que a data atual, executa as verificações de qualidade de dados
if health_check_datetime is None or health_check_datetime.date() < current_date:
    # Lê os dados da tabela 'breweries_silver'
    input_path = 's3://imbev-silver/breweries_silver/'
    df = spark.read.parquet(input_path)

    # Verifica PK repetida
    duplicate_pk = df.groupBy('id').agg(F.count('*').alias('count')) \
                      .filter(F.col('count') > 1)

    if duplicate_pk.count() > 0:
        print("PKs repetidas encontradas:")
        duplicate_pk.show()
        raise Exception("PKs repetidas encontradas, retire as duplicidades para reexecutar o processo")

    # Verifica PK nula
    null_pk_count = df.filter(F.col('id').isNull()).count()

    if null_pk_count > 0:
        print(f"Número de PKs nulas encontradas: {null_pk_count}")
        raise Exception("PKs nulas encontradas, verifique a origem dos dados para reexecutar o processo")

    # Verifica tabela vazia
    total_count = df.count()

    if total_count == 0:
        print("A tabela está vazia.")
        raise Exception("A tabela está vazia, verifique a origem dos dados para reexecutar o processo")
    else:
        print(f"Número total de registros na tabela: {total_count}")

    # Atualizando o timestamp no DynamoDB
    new_health_check_dataquality = current_datetime.strftime('%Y-%m-%d %H:%M:%S')
    parameters_table.update_item(
        Key={'table_name': 'breweries'},
        UpdateExpression='SET health_check_dataquality = :val',
        ExpressionAttributeValues={':val': new_health_check_dataquality}
    )
else:
    print("Nenhuma atualização necessária para health_check_dataquality.")

# Finaliza o job
job.commit()