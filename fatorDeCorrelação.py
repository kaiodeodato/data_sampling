from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("my_app").getOrCreate()

df = spark.read.option("delimiter", ",").csv("C:/Users/USER/Downloads/CODE/TOKIO/bigdata/modulo 5/EDA/stocks_2021.csv", header=True, inferSchema=True)

stock1 = 'MSFT'
stock2 = 'AAPL'

var1 = df.filter(df.ticker == stock1) 
var2 = df.filter(df.ticker == stock2)

var1 = var1.withColumnRenamed('open','open_stock1')
var1 = var1.withColumnRenamed('date','date_stock1')

var2 = var2.withColumnRenamed('open','open_stock2')
var2 = var2.withColumnRenamed('date','date_stock2')

finalVar = var2.join(var1, var2.date_stock2 == var1.date_stock1, how='inner')

finalVar = finalVar.withColumn('open_stock1', finalVar.open_stock1.cast('double'))
finalVar = finalVar.withColumn('open_stock2', finalVar.open_stock2.cast('double'))

correlation = finalVar.stat.corr('open_stock2','open_stock1')

print(f'O fator de correlação entre {stock1} e {stock2} é: {correlation}')



