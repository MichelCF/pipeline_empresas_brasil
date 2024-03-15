import os
from zipfile import ZipFile

import pandas as pd
import wget

import logs_decorator as logs

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
        return True
    except Exception as error:
        print(f"Ocorreu um erro {error}")
        return False


def remove_all_files(path: str, extension: str):
    for file in os.listdir(path):
        if file.endswith(extension):
            os.remove(os.path.join(path, file))


def ingestion_to_csv(input_path: str, output_path: str, metadata: dict, end_file: str):
    for file in os.listdir(input_path):
        if file.endswith(end_file):
            df = pd.read_csv(
                input_path + metadata["arquivo"],
                encoding=metadata["encoding"],
                sep=metadata["sep"],
                names=metadata["colunas"],
                dtype=metadata["dtype"],
            )
            df.to_csv(
                output_path + file.split(".")[-1].replace("CSV", ".csv"),
                encoding="utf-8",
                sep=";",
                index=False,
            )


@logs.csv_to_parquet
def csv_to_parquet(file: str, path: str, output: str):
    df = pd.read_csv(os.path.join(path, file), sep=";")
    df.to_parquet(
        (os.path.join(output, file.replace(".csv", ".parquet"))),
        index=False,
        engine="pyarrow",
    )
    # return os.listdir(output)
