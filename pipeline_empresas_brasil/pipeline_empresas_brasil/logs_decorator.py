import logging
import time
from functools import wraps

logging.basicConfig(
    level=logging.INFO,
    filename="logs/logs.txt",
    filemode="a",
    encoding="utf-8",
    format="%(levelname)s:%(asctime)s:%(message)s",
)


def tempo_execucao(func):
    @wraps(func)
    def wrap_tempo_execicao(*args, **kwargs):
        inicio = time.time()
        resultado = func(*args, **kwargs)
        fim = time.time()
        if resultado:
            logging.info(
                f"O arquivo {args[0]} demorou {fim-inicio} segundos para ser baixado."
            )
        else:
            logging.info(f"O arquivo {args[0]} não concluiu o download")
        return resultado

    return wrap_tempo_execicao


def csv_to_parquet(func):
    @wraps(func)
    def wrap_csv_to_parquet(*args, **kwargs):
        inicio = time.time()
        resultado = func(*args, **kwargs)
        fim = time.time()
        for file in resultado:
            logging.info(
                f"O arquivo {args[0]} demorou {fim-inicio} para ser convertido em parquet na camada Bronze."
            )
        return resultado

    return wrap_csv_to_parquet
