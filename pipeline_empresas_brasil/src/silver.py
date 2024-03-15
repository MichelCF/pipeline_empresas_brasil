from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import FloatType, IntegerType, StringType


def empresas_bronze_to_silver(file: str, input: str, output: str, secao: SparkSession):
    df = secao.read.format("parquet").option("header", "true").load(input + file)
    cols = [
        "cnpj",
        "razao_social",
        "natureza_juridica",
        "qualificacao_responsavel",
        "capital_social",
        "cod_porte",
    ]
    df = df.select(*cols)
    df_silver = (
        df.withColumn("cnpj", df.cnpj.cast(StringType()))
        .withColumn("razao_social", df.razao_social.cast(StringType()))
        .withColumn("natureza_juridica", df.natureza_juridica.cast(IntegerType()))
        .withColumn(
            "qualificacao_responsavel", df.qualificacao_responsavel.cast(IntegerType())
        )
        .withColumn("capital_social", df.capital_social.cast(FloatType()))
        .withColumn("cod_porte", df.cod_porte.cast(StringType()))
    )
    df_silver.write.mode("overwrite").parquet(output)


def social_bronze_to_silver(file: str, input: str, output: str, secao: SparkSession):
    df = secao.read.format("parquet").option("header", "true").load(input + file)
    cols = [
        "cnpj",
        "tipo_socio",
        "nome_socio",
        "documento_socio",
        "codigo_qualificacao_socio",
    ]
    df = df.select(*cols)
    df_silver = (
        df.withColumn("cnpj", df.cnpj.cast(StringType()))
        .withColumn("tipo_socio", df.tipo_socio.cast(IntegerType()))
        .withColumn("nome_socio", df.nome_socio.cast(StringType()))
        .withColumn("documento_socio", df.documento_socio.cast(StringType()))
        .withColumn(
            "codigo_qualificacao_socio", df.codigo_qualificacao_socio.cast(StringType())
        )
    )
    df_silver.write.mode("overwrite").parquet(output)
