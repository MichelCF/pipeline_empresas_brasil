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


def gold_to_bd(
    parquet_path: str,
    port: str = "5432",
    user: str = "username",
    password: str = "password",
    dbname: str = "database",
    db_table: str = "empresas",
    chunk_size: int = 10000,
):
    #engine = create_engine(f"postgresql://{user}:{password}@localhost:{port}/{dbname}")
    engine = create_engine("postgresql://username:secret@localhost:5432/database")

    data_reader = pd.read_parquet(parquet_path)

# Dividir o DataFrame em chunks
    chunks = [data_reader[i:i + chunk_size] for i in range(0, data_reader.shape[0], chunk_size)]

    try:
        with engine.connect() as conn:
            for i, chunk in enumerate(chunks):
                chunk.to_sql(
                    "empresas", 
                    conn, 
                    if_exists="append",
                    index=False, 
                    method="multi"
                )
                print(f"Chunk {i+1} carregado com sucesso no banco de dados.")

        print("Todos os dados foram carregados com sucesso no banco de dados.")
    except Exception as e:
        print("Ocorreu um erro ao carregar os dados no banco de dados:", e)


df = pd.read_sql_query("select * from EMPRESAS", con=engine)
print(df.info())
# gold_to_bd()
