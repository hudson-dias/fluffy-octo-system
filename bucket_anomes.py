from datetime import datetime

# Carrega as configurações do arquivo JSON
with open('parametros-dev.json') as json_file:
    config = json.load(json_file)

# Obtém a referência ao bucket S3 especificado nas configurações
s3 = boto3.resource('s3')
bucket = s3.Bucket(config['nome_do_bucket'])

# Obtém o ano e o mês atuais
agora = datetime.datetime.now()
ano_mes = agora.strftime('%Y%m')

# Cria a pasta no bucket S3 com o nome do mês e ano atual
bucket.put_object(Key=ano_mes + '/')

# Salva o arquivo CSV no bucket S3 na pasta criada anteriormente
bucket.put_object(Key=ano_mes + '/NomeDoArquivo.csv', Body=conteudo_do_csv)

