# data-engineering-scrap-books
**Extração** das informações do site books.toscrape.com com python, utilizando as bibliotecas selenium e bs4.
**Armazenamento** no bucket raw do s3 (localstack). 
**Transformação** dos dados com pandas e armazenamento no bucket trusted do s3 (localstack). 
**Orquestração** do pipeline com Airflow. 
**Visualização** dos dados com Streamlit
![arquitetura ](https://github.com/ceesarmoreno/data-engineering-scrap-books/assets/63748142/a5e45b33-6e13-4b99-bf5b-eb0d11a25dbb)


**Comandos necessários no docker** 

docker build -t my_airflow_image .

docker compose up airflow-init

docker compose up


**Airflow:**
http://localhost:8080

user: airflow 
pass:airflow
  
**Streamlit:**
http://localhost:8501
