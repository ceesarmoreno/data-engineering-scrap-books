FROM apache/airflow:2.8.0
COPY requirements.txt /requirements.txt 

RUN pip install --upgrade pip
RUN pip install -r /requirements.txt

USER root
# Instala o Chrome

RUN apt-get update && apt-get install -y wget gnupg
RUN wget -q -O - https://dl-ssl.google.com/linux/linux_signing_key.pub | apt-key add -
RUN sh -c 'echo "deb [arch=amd64] http://dl.google.com/linux/chrome/deb/ stable main" >> /etc/apt/sources.list.d/google-chrome.list'
RUN apt-get update && apt-get install -y google-chrome-stable


# Definir o diretório de trabalho e copiar o código do aplicativo Streamlit
WORKDIR /app
COPY streamlit_app.py /app/streamlit_app.py 
