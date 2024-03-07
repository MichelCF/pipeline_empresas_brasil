import bronze
import concurrent.futures
from functools import partial


URL = "https://dadosabertos.rfb.gov.br/CNPJ/"
EMPRESAS = "Empresas1.zip"
SOCIOS = "Socios0.zip"
FILES = [EMPRESAS, SOCIOS]
BRONZE = "camadas/BRONZE/"
INGESTION = "camadas/BRONZE/INGESTION/"

downlod_multi_files = partial(bronze.downlod_file, url=URL, output=INGESTION)
with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
    files = executor.map(downlod_multi_files, FILES)

unzipall_files = partial(bronze.unzip_file, path=INGESTION, output=INGESTION)
with concurrent.futures.ProcessPoolExecutor(max_workers=4) as executor:
    files = executor.map(unzipall_files, FILES)

bronze.remove_all_files(INGESTION, ".zip")

bronze.csv_to_parquet(INGESTION, BRONZE)
