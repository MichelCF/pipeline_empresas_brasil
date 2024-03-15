import concurrent.futures
import os
from functools import partial

from dotenv import load_dotenv
from pyspark.sql import SparkSession
from sqlalchemy import create_engine

import bronze
import gold
import silver

URL = "https://dadosabertos.rfb.gov.br/CNPJ/"

BRONZE = "camadas/bronze/"
INGESTION = "camadas/bronze/ingestion/"
SOCIAL_SILVER = "camadas/silver/social/"
EMPRESA_SILVER = "camadas/silver/empresa/"
EMPRESA_GOLD = "camadas/gold/empresa/"

EMPRESA_METADATA = {
    "arquivo": "Empresas0.zip",
    "colunas": [
        "cnpj",
        "razao_social",
        "natureza_juridica",
        "qualificacao_responsavel",
        "capital_social",
        "cod_porte",
        "ente_federativo",
    ],
    "dtype": {
        "cnpj": "string",
        "razao_social": "string",
        "natureza_juridica": "int64",
        "qualificacao_responsavel": "string",
        "capital_social": "string",
        "cod_porte": "string",
        "ente_federativo": "string",
    },
    "encoding": "ISO-8859-1",
    "sep": ";",
}

SOCIOS_METADATA = {
    "arquivo": "Socios0.zip",
    "colunas": [
        "cnpj",
        "tipo_socio",
        "nome_socio",
        "documento_socio",
        "codigo_qualificacao_socio",
        "data_entrada",
        "pais",
        "representante_legal",
        "nome_representante",
        "qualificacao_representante",
        "faixa_etaria",
    ],
    "dtype": {
        "cnpj": "string",
        "tipo_socio": "int64",
        "nome_socio": "string",
        "documento_socio": "string",
        "codigo_qualificacao_socio": "string",
        "data_entrada": "string",
        "pais": "string",
        "representante_legal": "string",
        "nome_representante": "string",
        "qualificacao_representante": "string",
        "faixa_etaria": "string",
    },
    "encoding": "ISO-8859-1",
    "sep": ";",
}
METADATAS = [EMPRESA_METADATA, SOCIOS_METADATA]

spark = (
    SparkSession.builder.master("local")
    .appName("pipeline_empresas_brasil")
    .getOrCreate()
)
#load_dotenv()
#db_string = "postgresql://{}:{}@{}:{}/{}".format(
#    os.getenv("DB_USER"),
#    os.getenv("DB_PASS"),
#    os.getenv("DB_HOST"),
#    os.getenv("DB_PORT"),
#    os.getenv("DB_NAME"),
#)
#print(db_string)
#db = create_engine(db_string)
#print(db)

downlod_multi_files = partial(bronze.downlod_file, url=URL, output=INGESTION)
unzipall_files = partial(bronze.unzip_file, path=INGESTION, output=INGESTION)
all_csv_to_parquet = partial(bronze.csv_to_parquet, path=BRONZE, output=BRONZE)

files = [METADATAS[0]["arquivo"], METADATAS[1]["arquivo"]]
with concurrent.futures.ThreadPoolExecutor(max_workers=4) as thread_executor:
    downalod_files = thread_executor.map(downlod_multi_files, files)

for file in files:
    unzipall_files(file)

bronze.ingestion_to_csv(
    input_path=INGESTION,
    output_path=BRONZE,
    metadata=EMPRESA_METADATA,
    end_file="EMPRECSV",
)
bronze.ingestion_to_csv(
    input_path=INGESTION,
    output_path=BRONZE,
    metadata=SOCIOS_METADATA,
    end_file="SOCIOCSV",
)

files = list(filter(lambda files: files.endswith(".csv") == True, os.listdir(BRONZE)))
with concurrent.futures.ProcessPoolExecutor(max_workers=4) as process_executor:
    parquet_files = process_executor.map(all_csv_to_parquet, files)
bronze.remove_all_files(INGESTION, "CSV")
silver.social_bronze_to_silver("SOCIO.parquet", BRONZE, SOCIAL_SILVER, secao=spark)
silver.empresas_bronze_to_silver("EMPRE.parquet", BRONZE, EMPRESA_SILVER, secao=spark)
gold.silver_to_gold(
    path_socios=SOCIAL_SILVER,
    path_empresas=EMPRESA_SILVER,
    output=EMPRESA_GOLD,
    secao=spark,
)
spark.stop()
