import os


def cria_camadas():
    os.mkdir("./camadas/bronze")
    os.mkdir("./camadas/silver")
    os.mkdir("./camadas/silver/socios/")
    os.mkdir("./camadas/silver/empresa/")
    os.mkdir("./camadas/gold")
    os.mkdir("./camadas/gold/empresa/")
    os.mkdir("./camadas/bronze/ingestion")
