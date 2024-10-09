import json
import requests
import boto3
from datetime import datetime

def getData(event, context):
    # Inicializando o cliente do Step Functions
    stepfunctions = boto3.client('stepfunctions')

    # Verificando se taskToken está presente
    if 'taskToken' not in event:
        raise ValueError("Missing 'taskToken' in event.")
    task_token = event['taskToken']

    # Inicializando o cliente do DynamoDB
    dynamodb = boto3.resource('dynamodb')
    parameters_table = dynamodb.Table('parameters')

    # Lendo o item 'breweries' da tabela DynamoDB
    response = parameters_table.get_item(Key={'table_name': 'breweries'})
    
    # Inicializa a variável para health_check_bronze
    health_check_bronze = None

    # Verifica se o item existe
    if 'Item' in response and 'health_check_bronze' in response['Item']:
        health_check_bronze = response['Item']['health_check_bronze']

    # Converte o timestamp para datetime se não for None ou vazio
    if health_check_bronze and health_check_bronze.strip():
        health_check_datetime = datetime.strptime(health_check_bronze, '%Y-%m-%d %H:%M:%S')
    else:
        health_check_datetime = None

    # Lógica de execução do job
    current_datetime = datetime.now()
    current_date = current_datetime.date()

    # Se health_check_bronze for None ou menor que a data atual, executa o job
    if health_check_datetime is None or health_check_datetime.date() < current_date:
        # Faz a requisição para obter os dados
        response = requests.get('https://api.openbrewerydb.org/breweries')
        
        if response.status_code == 200:
            data = response.json()
            
            # Inicializando o cliente S3
            s3 = boto3.client('s3')
            
            # Definindo o nome do arquivo e o bucket
            bucket_name = 'imbev-bronze'
            file_name = 'file/breweries_data.json'

            # Convertendo os dados em JSON e enviando para o S3
            s3.put_object(
                Bucket=bucket_name,
                Key=file_name,
                Body=json.dumps(data),
                ContentType='application/json'
            )

            # Atualizando o timestamp no DynamoDB
            new_health_check_bronze = current_datetime.strftime('%Y-%m-%d %H:%M:%S')
            parameters_table.update_item(
                Key={'table_name': 'breweries'},
                UpdateExpression='SET health_check_bronze = :val',
                ExpressionAttributeValues={':val': new_health_check_bronze}
            )

            # Enviando o callback de sucesso
            stepfunctions.send_task_success(
                taskToken=task_token,
                output=json.dumps({'message': 'Dados salvos com sucesso!'})
            )

            return {
                'statusCode': 200,
                'body': json.dumps({'message': 'Dados salvos com sucesso!'})
            }
        else:
            # Enviando o callback de falha em caso de erro na requisição
            stepfunctions.send_task_failure(
                taskToken=task_token,
                error='RequestError',
                cause=json.dumps({'error': 'Erro na requisição'})
            )
            return {
                'statusCode': response.status_code,
                'body': json.dumps({'error': 'Erro na requisição'})
            }
    else:
        # Enviando o callback de sucesso mesmo quando não há atualização
        stepfunctions.send_task_success(
            taskToken=task_token,
            output=json.dumps({'message': 'Nenhuma atualização necessária.'})
        )
        return {
            'statusCode': 200,
            'body': json.dumps({'message': 'Nenhuma atualização necessária.'})
        }