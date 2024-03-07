from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

SPARK = (
    SparkSession.builder
    .master('local')
    .appName('pipeline_empresas_brasil')
    .getOrCreate()
)