import pandas as pd


def silver_to_gold(file_empresa: str, file_socios: str, output: str):
    # Carregando os DataFrames de empresas e sócios
    df_empresas = pd.read_parquet(file_empresa)
    df_socios = pd.read_parquet(file_socios)

    # Juntando os DataFrames de empresas e sócios
    merged_df = pd.merge(df_empresas, df_socios, on="cnpj", how="left").sort_values(
        "cnpj"
    )

    # Calculando a quantidade de sócios por CNPJ
    qtde_socios = merged_df.groupby("cnpj").size().reset_index(name="qtde_socios")

    # Juntando a quantidade de sócios com o DataFrame principal
    merged_df = pd.merge(merged_df, qtde_socios, on="cnpj", how="left").sort_values(
        "cnpj"
    )

    # Criando as colunas "flag_socio_estrangeiro" e "doc_alvo"
    merged_df["flag_socio_estrangeiro"] = merged_df["tipo_socio"] == 3
    merged_df["doc_alvo"] = (merged_df["cod_porte"] == 3) & (
        merged_df["qtde_socios"] > 1
    )

    # Selecionando e escrevendo o DataFrame resultante em um arquivo Parquet
    final_df = merged_df[
        [
            "cnpj",
            "qtde_socios",
            "flag_socio_estrangeiro",
            "doc_alvo",
        ]
    ]
    final_df.to_parquet(output)
