from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("my_app").getOrCreate()

df = spark.read.option("delimiter", ",").csv("C:/Users/USER/Downloads/CODE/TOKIO/bigdata/modulo 5/EDA/stocks_2021.csv", header=True, inferSchema=True)

amostra = df.sampleBy('ticker', fractions = {'HON': 0.03, 'WMT': 0.05, 'BA':0.01})

amostra.show()