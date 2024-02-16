#Bibliotecas
from selenium import webdriver
from selenium.webdriver.chrome.service import Service 
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from bs4 import BeautifulSoup
from selenium.webdriver.support.ui import WebDriverWait 
from selenium.webdriver.support import expected_conditions as EC 
import time
import pandas as pd
from datetime import datetime
import boto3
from io import StringIO
import pyarrow as pa
import pyarrow.parquet as pq
from io import BytesIO
import numpy as np
from airflow import DAG
from airflow.operators.python import PythonOperator



#CONFIGURA O ENDPOINT PARA O S3 NO LOCALSTACK
aws_access_key_id = 'fake'
aws_secret_access_key = 'fake'
s3 = boto3.client("s3", aws_access_key_id = aws_access_key_id ,
                  aws_secret_access_key=aws_secret_access_key,
                  endpoint_url="http://host.docker.internal:4566")

target_bucket = 'data'
subfolder_01 = 'raw/'
subfolder_02 = 'trusted/'

def cria_bucket():

    #LISTA OS BUCKETS E SE NÃO TIVER CRIADO ELE CRIA COM OS DIRETÓRIOS ESPECÍFICOS
    lista_buckets = s3.list_buckets()
    nome_buckets = []

    for bucket in lista_buckets["Buckets"]:
        nome_buckets.append(bucket['Name'])

    if target_bucket not in nome_buckets:
        s3.create_bucket(Bucket = target_bucket)

        s3.put_object(Bucket = target_bucket, Key=subfolder_01)
        s3.put_object(Bucket = target_bucket, Key=subfolder_02)



#Config webdriver (opções configuradas para rodar em segundo plano, sem abrir o chrome)
servico = Service(ChromeDriverManager().install())
chromeOptions = webdriver.ChromeOptions()
chromeOptions.add_argument('--headless')
chromeOptions.add_argument('--disable-gpu')
chromeOptions.add_argument('--no-sandbox')
chromeOptions.add_argument('--disable-dev-shm-usage')
navegador = webdriver.Chrome(service=servico, options=chromeOptions)


def extrair_dados(ti):
    navegador.get('https://books.toscrape.com/')

    element = WebDriverWait(navegador, 30).until(EC.presence_of_element_located((By.XPATH, '//*[@id="default"]/div/div/div/aside/div[2]/ul/li/ul')))


    # quantifica as categorias
    categorias_livros = navegador.find_element(By.XPATH, '//*[@id="default"]/div/div/div/aside/div[2]/ul/li/ul').text
    qtd_categorias = len(categorias_livros.split('\n'))
    qtd_categorias = int(qtd_categorias)

    # listas para armazenar as informações
    lista_conteudo = []

    for i in range(1,qtd_categorias):

        nome_categoria = navegador.find_element(By.XPATH, f'//*[@id="default"]/div/div/div/aside/div[2]/ul/li/ul/li[{i}]/a').text

        selecionar_categoria = navegador.find_element(By.XPATH, f'//*[@id="default"]/div/div/div/aside/div[2]/ul/li/ul/li[{i}]/a')
        selecionar_categoria.click()

        qtd_livros = navegador.find_element(By.XPATH, f'//*[@id="default"]/div/div/div/div/form').text
        qtd_livros = qtd_livros.split(' ')
        qtd_livros = qtd_livros[0]
        qtd_livros = int(qtd_livros)


        if qtd_livros > 20:

            qtd_paginas = navegador.find_element(By.XPATH, '//*[@id="default"]/div/div/div/div/section/div[2]/div/ul/li[1]').text
            qtd_paginas = qtd_paginas.split(" ")
            qtd_paginas = int(qtd_paginas[-1])


            # Passa por cada página e pega todas as informações
            for i in range(qtd_paginas):

                pagina = BeautifulSoup(navegador.page_source, 'html.parser')
                livros = pagina.find_all('article', attrs = {'class':'product_pod'})


                #Pega as informações de cada livro
                for livro in livros:

                    nome_livro = str(livro.find('h3'))
                    nome_livro = nome_livro.split('title=')[1].split('>')[0].replace('"','').replace("'",'').strip()

                    preco_livro = livro.find('p', attrs = {'class':'price_color'}).text
                    estoque = livro.find('p', attrs = {'class':'instock availability'}).text.strip()

                    avaliacao = str(livro.find('p'))
                    avaliacao = avaliacao.split(">")[0].split("=")[-1].split(" ")[-1].replace('"','').strip()

                    conteudo = {'nome_livro': nome_livro, 'preco': preco_livro, 'disponibilidade':estoque, 'avaliacao': avaliacao, 'categoria': nome_categoria}
                    lista_conteudo.append(conteudo)


                #se tiver mais que uma página ele passa para a próxima
                if i+1 < qtd_paginas:
                    prox_pagina = navegador.find_element(By.LINK_TEXT, 'next').click()


        # se tiver menos que 20 livros ele pega apenas da primeira pagina
        else:

            pagina = BeautifulSoup(navegador.page_source, 'html.parser')
            livros = pagina.find_all('article', attrs = {'class':'product_pod'})

            for livro in livros:

                nome_livro = str(livro.find('h3'))
                nome_livro = nome_livro.split('title=')[1].split('>')[0].replace('"','').replace("'",'').strip()

                preco_livro = livro.find('p', attrs = {'class':'price_color'}).text
                estoque = livro.find('p', attrs = {'class':'instock availability'}).text.strip()

                avaliacao = str(livro.find('p'))
                avaliacao = avaliacao.split(">")[0].split("=")[-1].split(" ")[-1].replace('"','').strip()

                conteudo = {'nome_livro': nome_livro, 'preco': preco_livro, 'disponibilidade':estoque, 'avaliacao': avaliacao, 'categoria': nome_categoria}
                lista_conteudo.append(conteudo)

    ti.xcom_push(key="dados_extraidos", value=lista_conteudo)


#PEGA OS DADOS NO XCOM E CARREGA NO BUCKET RAW 
def carregar_dados(ti):

    pulled_value = ti.xcom_pull(task_ids='extracao_dados', key= 'dados_extraidos')

    df = pd.DataFrame.from_records(pulled_value)

    table = pa.Table.from_pandas(df)

    buf = pa.BufferOutputStream()
    pq.write_table(table, buf)

    buffer_bytes = buf.getvalue().to_pybytes()

    s3.put_object(Body = buffer_bytes, Bucket = target_bucket, Key = f"{subfolder_01}r_livros.parquet")



#PUXA OS DADOS DO RAW, TRATA E JOGA NO BUCKET TRUSTED NO FORMAT PARQUET
def transformar_dados():

    response = s3.get_object(Bucket=target_bucket, Key=f"{subfolder_01}r_livros.parquet")
    body = response['Body'].read()
    table = pq.read_table(BytesIO(body))
    df_t = table.to_pandas()

    df_t['preco'] = df_t['preco'].str.replace("£",'')
    df_t['preco'] = df_t['preco'].astype(float)

    df_t['disponibilidade'] = df_t['disponibilidade'].str.replace('In stock', 'Em estoque')

    condicoes = [(df_t['avaliacao'] == 'One'), (df_t['avaliacao'] == 'Two'), (df_t['avaliacao'] == 'Three'),(df_t['avaliacao'] == 'Four'), (df_t['avaliacao'] == 'Five')]
    resultados = [1,2,3,4,5]
    df_t['avaliacao'] = np.select(condicoes,resultados,default=np.NAN)
    df_t['avaliacao'] = df_t['avaliacao'].astype(int)

    table = pa.Table.from_pandas(df_t)

    buf = pa.BufferOutputStream()
    pq.write_table(table, buf)

    buffer_bytes = buf.getvalue().to_pybytes()

    s3.put_object(Body = buffer_bytes, Bucket = target_bucket, Key = f"{subfolder_02}t_livros.parquet")



#DEFINE OS ARGUMENTOS DA DAG
args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024,1,23),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1}

with DAG(
    "ETL_books",
    default_args = args,
    description = 'Pipeline da loja de livros',
    schedule_interval = None

) as dag:
    
    task_cria_bucket = PythonOperator(
        task_id ='cria_bucket',
        python_callable = cria_bucket)
    
    task_extracao = PythonOperator(
            task_id = 'extracao_dados', 
            python_callable = extrair_dados)
    
    task_load = PythonOperator(
        task_id = 'carregar_dados',
        python_callable = carregar_dados)
    
    task_transform = PythonOperator(
        task_id = 'transformar_dados',
        python_callable = transformar_dados)
    

    task_cria_bucket >> task_extracao >> task_load >> task_transform