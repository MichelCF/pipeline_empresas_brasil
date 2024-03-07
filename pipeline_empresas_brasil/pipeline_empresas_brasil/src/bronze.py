import wget, os
from zipfile import ZipFile
import pyarrow.csv as pv
import pyarrow.parquet as pq
import logs_decorator as logs

URL = "https://dadosabertos.rfb.gov.br/CNPJ/"
EMPRESAS = "Empresas1.zip"
SOCIOS = "Socios0.zip"
FILES = [EMPRESAS, SOCIOS]
BRONZE = "/home/caiafa/pipeline_empresas_brasil/BRONZE/"
INGESTION = "/home/caiafa/pipeline_empresas_brasil/BRONZE/INGESTION/"


@logs.tempo_execucao
def downlod_file(filename: str, url: str, output: str) -> bool:

    try:
        wget.download(url + filename, out=output)
        print(f"O Download do aqruivo {filename} foi concluido.")
        return True
    except Exception as error:
        print(f"Ocorreu o erro {error}")
        return False


# Etapa 2
def unzip_file(filename: str, path: str, output: str) -> bool:
    try:
        with ZipFile(path + filename, "r") as zObject:
            zObject.extractall(path=output)
        return True
    except Exception as error:
        print(f"Ocorreu um erro {error}")
        return False


# ETAPA 3
def remove_all_files(path: str, extension: str):
    for file in os.listdir(path):
        if file.endswith(extension):
            os.remove(os.path.join(path, file))


@logs.csv_to_parquet
def csv_to_parquet(path: str, output: str):
    options = pv.ParseOptions(delimiter=";")
    
    for file in os.listdir(path):
        if file.endswith("CSV"):
            new_file = file.split(".")[-1].replace("CSV", ".csv")
            os.rename(os.path.join(path, file), os.path.join(path, new_file))
            
    for file in os.listdir(path):
        if file.endswith(".csv"):
            table = pv.read_csv(os.path.join(path, file), parse_options=options)
            pq.write_table(
                table, os.path.join(output, file.replace("csv", "parquet"))
            )
    return os.listdir(output)
