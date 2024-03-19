CREATE TABLE IF NOT EXISTS EMPRESAS (
    cnpj VARCHAR PRIMARY KEY,
    qtde_socios BIGINT,
    flag_socio_estrangeiro BOOLEAN,
    doc_alvo BOOLEAN
);