import os
from zipfile import ZipFile

import logs_decorator as logs
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.csv as pv
import pandas as pd
import wget

URL = "https://dadosabertos.rfb.gov.br/CNPJ/"
EMPRESAS = "Empresas1.zip"
SOCIOS = "Socios0.zip"
FILES = [EMPRESAS, SOCIOS]


@logs.tempo_execucao
def downlod_file(filename: str, url: str, output: str) -> bool:

    try:
        wget.download(url + filename, out=output)
        print(f"O Download do aqruivo {filename} foi concluido.")
        return True
    except Exception as error:
        print(f"Ocorreu o erro {error}")
        return False


def unzip_file(filename: str, path: str, output: str) -> bool:
    try:
        with ZipFile(path + filename, "r") as zObject:
            zObject.extractall(path=output)
        rename_unzip_file(output)
        return True
    except Exception as error:
        print(f"Ocorreu um erro {error}")
        return False


def remove_all_files(path: str, extension: str):
    for file in os.listdir(path):
        if file.endswith(extension):
            os.remove(os.path.join(path, file))


def rename_unzip_file(path: str):
    for file in os.listdir(path):
        if file.endswith("CSV"):
            new_file = file.split(".")[-1].replace("CSV", ".csv")
            os.rename(os.path.join(path, file), os.path.join(path, new_file))
    print(os.listdir(path))


@logs.csv_to_parquet
def csv_to_parquet(file_with_schema: tuple, path: str, output: str):
    file = file_with_schema[0]
    schema = file_with_schema[1]
    df_stream = pd.read_csv(
        os.path.join(path, file),
        sep=";",
        encoding="ISO-8859-1",
        low_memory=False,
        chunksize=100000,
    )
    for i, chunk in enumerate(df_stream):
        if i == 0:
            parquet_writer = pq.ParquetWriter(
                os.path.join(output, file.replace("csv", "parquet")),
                schema,
                compression="snappy",
            )
        table = pa.Table.from_pandas(chunk, schema=schema)
        parquet_writer.write_table(table)
    return os.listdir(output)
