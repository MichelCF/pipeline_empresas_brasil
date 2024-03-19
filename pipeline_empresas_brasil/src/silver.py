import pandas as pd


def empresas_bronze_to_silver(file: str, input: str, output: str):
    df = pd.read_parquet(input + file)

    cols = [
        "cnpj",
        "razao_social",
        "natureza_juridica",
        "qualificacao_responsavel",
        "capital_social",
        "cod_porte",
    ]
    df = df[cols]

    df["cnpj"] = df["cnpj"].astype(str)
    df["razao_social"] = df["razao_social"].astype(str)
    df["natureza_juridica"] = df["natureza_juridica"].astype(int)
    df["qualificacao_responsavel"] = df["qualificacao_responsavel"].astype(int)
    df["capital_social"] = df["capital_social"].str.replace(",", ".").astype(float)
    df["cod_porte"] = df["cod_porte"].astype(str)

    df.to_parquet(output + file)


def social_bronze_to_silver(file: str, input: str, output: str):
    df = pd.read_parquet(input + file)

    cols = [
        "cnpj",
        "tipo_socio",
        "nome_socio",
        "documento_socio",
        "codigo_qualificacao_socio",
    ]
    df = df[cols]

    df["cnpj"] = df["cnpj"].astype(str)
    df["tipo_socio"] = df["tipo_socio"].astype(int)
    df["nome_socio"] = df["nome_socio"].astype(str)
    df["documento_socio"] = df["documento_socio"].astype(str)
    df["codigo_qualificacao_socio"] = df["codigo_qualificacao_socio"].astype(str)

    df.to_parquet(output + file)
