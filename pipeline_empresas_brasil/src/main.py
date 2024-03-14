import concurrent.futures
from functools import partial
import os
import bronze

URL = "https://dadosabertos.rfb.gov.br/CNPJ/"

EMPRESAS = "Empresas0.zip"
SOCIOS = "Socios0.zip"

BRONZE = "camadas/bronze/"
INGESTION = "camadas/bronze/ingestion/"

SCHEMA_EMPRESAS = {
    "cnpj": str,
    "razão_social": str,
    "natureza_juridica": int,
    "qualificacao_responsavel": int,
    "capital_social": float,
    "cod_porte": str,
}

SCHEMA_SOCIOS = {
    "cnpj": str,
    "tipo_socio": int,
    "nome_socio": str,
    "documento_socio": str,
    "codigo_qualificacao_socio": str,
}

FILES = [EMPRESAS, SOCIOS]
SCHEMAS = [SCHEMA_EMPRESAS, SCHEMA_SOCIOS]

downlod_multi_files = partial(bronze.downlod_file, url=URL, output=INGESTION)
unzipall_files = partial(bronze.unzip_file, path=INGESTION, output=INGESTION)
all_csv_to_parquet = partial(bronze.csv_to_parquet, path=INGESTION, output=BRONZE)

with concurrent.futures.ThreadPoolExecutor(max_workers=4) as thread_executor:
    downalod_files = thread_executor.map(downlod_multi_files, FILES)

with concurrent.futures.ProcessPoolExecutor(max_workers=4) as process_executor:
    unzip_files = process_executor.map(unzipall_files, FILES)

files_with_schema = list(
    filter(lambda files: files.endswith(".csv") == True, os.listdir(INGESTION))
)

files_with_schema = list(zip(files_with_schema, SCHEMAS))

with concurrent.futures.ProcessPoolExecutor(max_workers=4) as process_executor:
    parquet_files = process_executor.map(all_csv_to_parquet, files_with_schema)
bronze.remove_all_files(INGESTION, ".csv")
