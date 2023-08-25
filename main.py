from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("my_app").getOrCreate()

df = spark.read.option("delimiter", ",").csv("C:/Users/USER/Downloads/CODE/TOKIO/bigdata/modulo 5/EDA/stocks_2021.csv", header=True, inferSchema=True)

quantidadeCada = df.groupBy('ticker').count().orderBy('ticker')
quantidadeCada.show()

amostra = df.sampleBy('ticker', fractions = {'TSLA': 0.03, 'GOLD': 0.03, 'MSFT':0.03})

amostra.show()
