from datetime import datetime

# Carrega as configura��es do arquivo JSON
with open('parametros-dev.json') as json_file:
    config = json.load(json_file)

# Obt�m a refer�ncia ao bucket S3 especificado nas configura��es
s3 = boto3.resource('s3')
bucket = s3.Bucket(config['nome_do_bucket'])

# Obt�m o ano e o m�s atuais
agora = datetime.datetime.now()
ano_mes = agora.strftime('%Y%m')

# Cria a pasta no bucket S3 com o nome do m�s e ano atual
bucket.put_object(Key=ano_mes + '/')

# Salva o arquivo CSV no bucket S3 na pasta criada anteriormente
bucket.put_object(Key=ano_mes + '/NomeDoArquivo.csv', Body=conteudo_do_csv)

