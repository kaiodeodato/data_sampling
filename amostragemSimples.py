from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("my_app").getOrCreate()

df = spark.read.option("delimiter", ",").csv("C:/Users/USER/Downloads/CODE/TOKIO/bigdata/modulo 5/EDA/stocks_2021.csv", header=True, inferSchema=True)

stock1 = 'GOLD'

var1 = df.filter(df.ticker == stock1) 

amostra = var1.sample(0.05)

amostra.show()