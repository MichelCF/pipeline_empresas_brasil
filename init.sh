#!/bin/bash

# Banco de dados
docker-compose -f ./database/docker-compose.yml build

docker-compose -f ./database/docker-compose.yml up -d

# Aplicação
docker build -t pipeline_empresas_brasil ./pipeline_empresas_brasil

docker run -d --name pipeline_empresas_brasil_app \
    -v $(pwd)/pipeline_empresas_brasil/camadas:/app/camadas \
    -p 8000:8000 \
    pipeline_empresas_brasil