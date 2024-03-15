from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import FloatType, IntegerType, StringType


def silver_to_gold(
    path_empresas: str, path_socios: str, output: str, secao: SparkSession
):
    df_empresas = (
        secao.read.format("parquet").option("header", "true").load(EMPRESA_SILVER)
    )
    df_socios = (
        secao.read.format("parquet").option("header", "true").load(SOCIAL_SILVER)
    )
    left_df = df_empresas.join(df_socios, on=["cnpj"], how="left").orderBy("cnpj")
    df_qtd_socios = left_df.groupBy("cnpj").agg(
        count("documento_socio").alias("qtde_socios")
    )

    left_df = left_df.join(df_qtd_socios, on=["cnpj"], how="left").orderBy("cnpj")
    left_df = (
        left_df.select("cnpj", "qtde_socios", "tipo_socio", "cod_porte")
        .withColumn(
            "flag_socio_estrangeiro",
            (when(col("tipo_socio") == 3, True).otherwise(False)),
        )
        .withColumn(
            "doc_alvo",
            when((col("cod_porte") == 3) & (col("qtde_socios") > 1), True).otherwise(
                False
            ),
        )
    )
    left_df.write.mode("overwrite").parquet(output)
