# Pipeline Empresas Brasil



O projeto Pipeline Empresas Brasil é uma protótipo de uma pipeline de dados que busca dados brutos da iniciativa dados abertos. A pipeline segue o modelo medalhão possuindo 3 camada Bronze, Silver e Gold, além de ao finalizar a etapa Gold insere os dados em um banco de dados (postgresql) para facilitar o consumo em sistemas transacionais.

A pasta Bronze possui uma subpasta chamada ingestion, que preserva os arquivos originais.

O Processo de ETL busca um arquivo com dados de sócios e um arquivo com dados das empresas. Na etapa bronze os arquivos são apenas transformados em parquet, na etapa silver colunas dos arquivos são removidas e da etapa gold os arquivos são combinados para gerar um novo arquivo com informações calculadas.

A pipeline possui um pequeno sistema de logs que ainda está em evolução.

Todo o projeto está conteinerizado com docker.
Para executar o projeto:

1. É preciso ter docker e docker compose instalado na máquina.
2. Baixe este repositório e execute o comando abaixo.

	docker-compose -f docker-compose.yml up --build -d
