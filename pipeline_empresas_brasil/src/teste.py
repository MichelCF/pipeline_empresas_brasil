import pandas as pd
import psycopg2 as pg
from sqlalchemy import create_engine

engine = create_engine("postgresql://username:secret@localhost:5432/database")
host = "db"
port = "5432"
dbname = "database"
user = "username"
password = "secret"
tbname = "empresas"


def gold_to_bd():
    engine = create_engine("postgresql://username:secret@localhost:5432/database")

    data = pd.read_parquet("camadas/gold/empresa/EMPRESA.parquet").head(100)

    try:
        with engine.begin() as conn:
            data.to_sql(
                "empresas", conn, if_exists="replace", index=False, method="multi"
            )
            conn.commit()
        print("Os dados foram carregados com sucesso no banco de dados.")
    except Exception as e:
        print("Ocorreu um erro ao carregar os dados no banco de dados:", e)
