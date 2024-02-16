import streamlit as st
import boto3
import pyarrow.parquet as pq
from io import BytesIO
import pyarrow as pa
import pandas as pd 


st.set_page_config(
    page_title="Livraria Analytics",
    page_icon="📚",
    layout="wide",
    initial_sidebar_state="auto")


# PUXA OS DADOS DO BUCKET TRUSTED
@st.cache_data
def load_data():
    aws_access_key_id = 'fake'
    aws_secret_access_key = 'fake'
    s3 = boto3.client("s3", aws_access_key_id = aws_access_key_id ,
                    aws_secret_access_key=aws_secret_access_key,
                    endpoint_url="http://host.docker.internal:4566")

    target_bucket = 'data'
    subfolder_02 = 'trusted/'

    response = s3.get_object(Bucket=target_bucket, Key=f"{subfolder_02}t_livros.parquet")
    body = response['Body'].read()
    table = pq.read_table(BytesIO(body))
    df = table.to_pandas()

    return df 

try:
    data = load_data()

    #INFOS FILTROS
    tp_livros = data['nome_livro'].unique()
    tp_categoria = data['categoria'].unique()
    min_preco = data['preco'].min()
    max_preco = data['preco'].max()

    #ACRESCENTA OPCAO DE TODOS NO LIVRO
    list_tp_livros = []
    list_tp_livros.insert(0,'Todos')
    for i in tp_livros:
        list_tp_livros.append(i)

    #ACRESCENTA OPCAO DE TODAS NAS CATEGORIAS
    list_tp_categoria = []
    list_tp_categoria.insert(0,'Todos')
    for i in tp_categoria:
        list_tp_categoria.append(i)

    #CRIA O DASHBOARD
    st.title("Livraria Analytics")
    st.caption("Informações do site: books.toscrape.com")
    st.header(" ")

    #FILTROS E DEFINE COMO AS OPÇÕES DA CATEGORIA APARECERÃO 
    #SE UM LIVRO TIVER SELECIONADO, TRAZ AUTOMATICAMENTE APENAS A CATEGORIA DAQUELE LIVRO, E TIRA OS RANGES DE VALOR E AVALIACAO CASO CONTRARIO TRAZ TUDO
    f1, f2, f3,f4 = st.columns(4)

    livro = f1.selectbox("Livro", list_tp_livros)

    if livro == 'Todos':
        cat = f1.selectbox("Categoria", list_tp_categoria)
        preco = f2.slider("Preço", min_preco,max_preco,(min_preco,max_preco))
        avaliacao = f2.slider("Avaliação", 1,5,(1,5))

        #VERIFICA E PEGA OS FILTROS APLICADOS NA PAGINA 
        min_preco, max_preco = preco
        min_aval, max_aval = avaliacao

        #JA DEIXA FILTRADO O RANGE DE VALORES
        data = data[data['preco'].between(min_preco,max_preco) & data['avaliacao'].between(min_aval,max_aval)]

        #MOSTRA OS FILTROS APLICADOS
        f3.write("")
        f4.subheader("Filtros")
        f4.write(f'Categoria: {cat}')
        f4.write(f'Preço: {min_preco} a {max_preco}')
        f4.write(f'Avaliação: {min_aval} a {max_aval}')
        
    else:
        data = data[(data['nome_livro'] == livro)]
        cat = f1.selectbox("Categoria", data['categoria'].unique())
        f2.write("")
        f3.subheader("Filtros")
        f3.write(f'Livro: {livro}')
        f3.write(f'Categoria: {cat}')


    #APLICAS OS FILTROS MEDIANTE A SELEÇÃO
    if livro != 'Todos':
        data = data[(data['nome_livro'] == livro)]


    elif cat != 'Todos':
        data = data[(data['categoria'] == cat)]


    #PEGA AS INFORMAÇÕES METRICAS
    qtd_livros = data['nome_livro'].count()
    qtd_categ = data['categoria'].nunique()
    media_avaliacao = data['avaliacao'].mean().round(0)
    soma_preco = data['preco'].sum().round(2)
    media_preco = data['preco'].mean().round(2)
    min_preco = data['preco'].min()
    max_preco = data['preco'].max()

    qtd_estoque = data.loc[data['disponibilidade'] == 'Em estoque']
    qtd_estoque = qtd_estoque.shape[0]
    disponibilidade = round((qtd_estoque / qtd_livros) *100,1)

    #CRIA AS MÉTRICAS
    st.header(" ")
    m1, m2, m3, m4, m5, m6 =  st.columns(6)

    m1.metric(label='Quantidade de livros', value=qtd_livros)
    m2.metric(label='Avaliação média', value=media_avaliacao)
    m3.metric(label='Preço total', value=f'R$ {soma_preco}')
    m4.metric(label='Preço médio', value=f'R$ {media_preco}')
    m5.metric(label='Categorias', value=qtd_categ)
    m6.metric(label='Estoque', value=f'{disponibilidade}%')


    #AJUSTA O MINIMO DO VALOR QUANDO O MIN_PRECO É IGUAL O MAX_PRECO
    if min_preco == max_preco:
        min_preco = 0

    #EXIBIR DATAFRAME
    st.dataframe(data,
        column_config={'avaliacao': st.column_config.ProgressColumn("avaliação", format ='%f', min_value=0, max_value = 5),
                    'preco': st.column_config.ProgressColumn("preço", format ='R$ %f', min_value=min_preco, max_value = max_preco) },
        height=800,
        width= 2000,
        hide_index=False)


    #CRIA UM IF PARA SABER A POSIÇÃO QUE O BOTÃO DE DOWNLOAD FICARÁ
    csv = data.to_csv(index=False).encode('utf-8')

    if livro == 'Todos':
        f4.download_button(label = 'download analítico', data = csv, file_name='Dados.csv', mime='text/csv')

    else:
        f3.download_button(label = 'download analítico', data = csv, file_name='Dados.csv', mime='text/csv')

except:
    st.title('AVISO!')
    st.header('É necessário rodar a task no airflow para ter as informações disponíveis.')
