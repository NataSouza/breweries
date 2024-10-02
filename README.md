<h1> Breweries <h1>

## Introdução

Todo o projeto foi construído em nuvem, as ferramentas utilizadas são da AWS e estão listadas abaixo:

-Stepfunctions: Orquestração do processo
-Simple notification Service (SNS): Notificação de error handling
-DynamoDB: Parâmetros de healthcheck do processo
-S3: Storage dos dados
-Lambda: Extração de dados da API ciabilizando a construção camada bronze
-GlueETL job: Scripts das camadas silver, silver anonimização, dataquality e gold
-Athena: Visualização dos dados persistidos nas camadas silver e gold
-Cloudwatch: Visibilidade de logs 
-IAM: Controle de acessos
-Lakeformation: Data types de tabelas criadas no Athena


## Orquestração (Stepfunctions)

Para a orquestração do processo foi utilizado o stepfunctions, essa é uma ferramenta muito volátil da AWS que possui uma interface gráfica que facilita o acompanhamento em tempo real de execução e facilita a manutenção, é uma ferramenta escalável que se integra com uma ampla variedade de serviços dentro da AWS e com serviços externos, disponibiliza no seu escopo tratamento de erros e retries. 

Logo abaixo segue uma imagem de como ficou o processo de orquestção:

![Imagem 1](https://github.com/user-attachments/assets/53f6783b-782b-4527-9a66-b13d136f10f2)

Não existe a necessidade de um payload inicial para a inicialização do processo, portanto basta executar o start do state machine para iniciar o fluxo. (Para start automático do processo em um horário predeterminado é possível utilizar a ferramenta Glue Scheduler, não foi utilizada nesse projeto devido as execuções serem esporádicas e manuais)

O primeiro job é um lambda responsável por realizar a extração dos dados da API https://api.openbrewerydb.org/breweries e persisti-los na AWS sem nenhuma alteração, construindo assim a camada bronze. 
Dentro desse e dos demais Jobs existe um processo de retry e um processo notificação de erro:

![Imagem 2](https://github.com/user-attachments/assets/94166a74-63df-4acc-b42b-08d804de9911)

Caso haja algum erro no Lambda que se encaixe em algum dos perfis de exceções, o processo irá realizar um intervalo de 2 segundos e tentar a execução novamente. Cada nova execução vai esperar o dobro do tempo esperado pela última execução e serão executadas um total de 3 tentativas até que o processo falhe definitivamente.

Se o job falhar definitivamente o processo entra na segunda tratativa de catch error, como mostrado na figura abaixo:

![Imagem 3](https://github.com/user-attachments/assets/44ec316e-ad38-4af5-8f1c-23b82bfaa572)

Nesse momento o job envia todas as mesagens de erro para o step “Informa_erro_email” que é um tópico de SNS que por opção nesse caso envia um e-mail para os integrantes cadastrados com todas as mensagens de erro capturadas no processo acima. 

Segue um exemplo do tópico de SNS:

![Imagem 4](https://github.com/user-attachments/assets/688bb663-e2c7-471b-a92a-8d00da6b6fd9)

Com o tópico de SNS é possível encaminhar també SMS, ou enviar gatilhos para outros processos como lambda para iniciar novos processos de tratativa de erros com python.

Após a execução do primeiro job de lambda executado com sucesso, os demais jobs são iniciados, sempre respeitando a ordem de quando um job finalizar com sucesso o outro inicia. E todos os Jobs possuem tratamento de retry e notificação de erros.


## Parametrização (Dynamo)

O Amazon DynamoDB é um banco de dados de chave-valor NoSQL, projetado para executar aplicações de alta performance em qualquer escala, é um banco projetado para latências consistentes, seu tempo de resposta é de 10 a 20 milissegundos para leitura e gravação simples. Sendo assim essa se mostra muito eficaz para controle de parâmetros desse miniprojeto.

Dentro do Dynamo foi criada a tabela: “parameters” e dentro dessa tabela temos a PK (partition-key) e os campos de heathcheck:

![Imagem 5](https://github.com/user-attachments/assets/eda78974-560b-4dc7-b689-f9e4e6cff69e)

Toda vez que algum dos Jobs executa com sucesso, um timestamp do fim da execução é incluído nessa tabela. Dessa forma é possível acompanhar se o job executou com sucesso no último batch por exemplo.

Dentro dos códigos existe também um controle de execução. Pensando em um modelo batch que execute uma vez ao dia, o job verifica a última execução na tabela de “parameters” e compara com o odate atual, caso o job já tenha sido executado no dia vigente, o processo continua, sem a necessidade de executar novamente o mesmo job.

Exemplo de trativa em código:

![Imagem 6](https://github.com/user-attachments/assets/6437c939-79c4-47a7-87ca-b3a0f24826ff)


## Storage de dados(S3)

Nos storages foram criados 3 buckets (imbev-bronze, imbev-silver, imbev-gold) esses buckets são responsáveis pela segregação de camadas.

Também foram criados outros 2 buckets: “athena-results-breweries” (utilizado para controle de consultas do Athena) e aws-glue-assets-339665883547-us-east-1 (utilizado para controle de arquivos do Glue).

![Imagem 7](https://github.com/user-attachments/assets/35f2308e-bed0-44a9-8523-765788bba7cd)


## Camada bronze (Lambda)

Para a função lambda foi necessário subir um modulo da biblioteca requests do python como uma layer pois essa biblioteca não é nativa na AWS:

![Imagem 8](https://github.com/user-attachments/assets/061e3e73-9d0f-49a5-afc7-3cdbe45c626f)

Com essa biblioteca foi possível realizar a extração dos dados da API e transferi-las para o S3 como JSON, sem nenhuma trativa, mantendo apenas o dado original:

![Imagem 9](https://github.com/user-attachments/assets/64aa26f6-9f8d-4b89-a0cc-e6c5074c7787)


## Camada silver (Glue ETL job)

Esse job tem como principal objetivo processar o arquivo jsno gerado na camada bronze e transformá-lo em um parquet colunar particionado por duas colunas: "country" e "state". A ideia de uma partição composta foi por analisar que os dados não são apenas dos EUA, mas também de Ireland, sendo assim, particionar dados a nível mundial apenas por cidade pode tornar a partição muito granular e impactar o tempo de processamento dos processos seguintes em caso do crescimento dessa tabela futuramente, por isso a partição foi realizada com base nessas duas colunas.

Os dados são inseridos no bucket imbev-silver:

![Imagem 10](https://github.com/user-attachments/assets/b7b64b55-c088-44e0-8389-dff2490e1e35)

Após a inserção os dados ficam visíveis no Athena, graças aos datatypes da tabela inseridos no Lakeformation (database layer_silver):

![Imagem 11](https://github.com/user-attachments/assets/045bfafc-ea95-4a24-b0ad-bfaa40fb0992)

Nessa tabela os dados são persistidos sem nenhum tratamento de anonimização. A ideia é que tenha acesso a ela, apenas pessoas autorizadas a trabalharem com dados sensíveis.


## Camada silver anonimizado (Glue ETL job)

Esse job possui as mesmas características de estrutura e particionamento do do job da camada silver sem anonimização. Porém o seu diferencial é anonimizar dados sensíveis.

OBS: Não foi realizado a anonimização de todos os dados sensíveis da tabela para facilitar a visualização da mesma, pois a tabela é majoritariamente composta por dados sensíveis e anonimizar todos prejudicaria o intuito de analise, por isso foram anonimizados os campos de nome e telefone, porem para anonimizar os outros campos basta executar o mesmo processo para os demais campos.

Os dados são inseridos no bucket imbev-silver (anonimizados):

![Imagem 12](https://github.com/user-attachments/assets/2dc4cc23-2de6-4e3d-b662-1cfe7335e0e9)

Os campos name e phone estão anonimizados com hash sha256 (database layer_silver):

![Imagem 13](https://github.com/user-attachments/assets/1c175935-fec4-460e-ae15-2d5f8bf29848)

A ideia é que tenha acesso a essa tabela qualquer desenvolvedor, pois os dados sensíveis serão anonimizados


## Data quality (Glue ETL job)

O job de data quality tem como objetivo analisar se os dados da camada silver possui alguma divergência em relação a e métricas: chave primaria duplicada, chave primaria nula, tabela vazia.

Esse job executa ao fim do processo da camada silver e em caso de problemas um abend é gerado, o processo para e é encaminhado um e-mail aos envolvidos, o e-mail é enviado através do processo de notificação, já descrito no stepfunctions.


## Camada Gold (Glue ETL job)

O job da camada gold tem por objetivo criar uma visão agrupada de cervejarias por tipo e localidade, também persistindo os dados em parquet e com particionamento por localidade.

Os dados são inseridos no bucket imbev-gold:

![Imagem 14](https://github.com/user-attachments/assets/9f601b13-0082-4e58-8671-05218556d5c3)

Os dados também estão visíveis no Athena (database layer_gold):

![Imagem 15](https://github.com/user-attachments/assets/9fd5140b-73a8-4231-bb79-017092973533)
