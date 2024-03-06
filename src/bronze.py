import wget, os, time
import concurrent.futures
from functools import partial
from zipfile import ZipFile
import pyarrow.csv as pv
import pyarrow.parquet as pq


URL = 'https://dadosabertos.rfb.gov.br/CNPJ/'
EMPRESAS = 'Empresas1.zip'
SOCIOS =    'Socios0.zip'
FILES = [EMPRESAS,SOCIOS]
BRONZE = '/home/caiafa/Stone/BRONZE/'
INGESTION = '/home/caiafa/Stone/BRONZE/INGESTION/'
#ETAPA 1
def downlod_file(filename: str, url: str, output: str) -> bool:

    try:
        wget.download(url+filename, out=output)
        print(f'O Download do aqruivo {filename} foi concluido.')
        return True
    except Exception as error:
        print(f"Ocorreu o erro {error}")
        return False
#etapa 2
def unzip_file(filename: str, path: str, output: str) -> bool:
    try:
        with ZipFile(path+filename, 'r') as zObject:
            zObject.extractall(path=output)
        return True
    except Exception as error:
        print(f'Ocorreu um erro {error}')
        return False


downlod_multi_files = partial(downlod_file,url = URL, output = INGESTION)
with concurrent.futures.ThreadPoolExecutor() as executor:
     files = executor.map(downlod_multi_files,FILES)

for file in files:
   print(f'Arquivo {file} baixado!!')

unzipall_files = partial(unzip_file,path = INGESTION, output = INGESTION)
with concurrent.futures.ProcessPoolExecutor() as executor:
    files = executor.map(unzipall_files,FILES)

#ETAPA 3
for file in os.listdir(INGESTION):
  if file.endswith('.zip'):
      os.remove(os.path.join(INGESTION, file))
for file in os.listdir(INGESTION):
   new_file = file.split('.')[-1].replace('CSV','.csv')
   os.rename(os.path.join(INGESTION, file), os.path.join(INGESTION, new_file))

#ETAPA 4
for file in os.listdir(INGESTION):
   print(os.path.join(INGESTION, file))
   print(os.path.join(BRONZE,file))
   options = pv.ParseOptions(delimiter=';')
   table = pv.read_csv(os.path.join(INGESTION, file),parse_options = options)
   pq.write_table(table, os.path.join(BRONZE,file.replace('csv', 'parquet')))
