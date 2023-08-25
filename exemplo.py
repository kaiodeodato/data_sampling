from pyspark.sql import SparkSession
from pprint import pprint
spark = SparkSession.builder.appName("my_app").getOrCreate()

df = spark.read.option("delimiter", ",").csv("C:/Users/USER/Downloads/CODE/TOKIO/bigdata/modulo 5/EDA/stocks_2021.csv", header=True, inferSchema=True)

all_tickers = df.select('ticker').distinct().collect()

ticker_array = []

for item in all_tickers:
    ticker = item['ticker']
    ticker_array.append(ticker)

# print(ticker_array)

# df.groupBy('ticker').count().show()

def calculus(stock1, stock2):

    select1 = df.filter(df.ticker == stock1)
    select2 = df.filter(df.ticker == stock2)

    select1 = select1.withColumnRenamed('open','open_stock1')
    select2 = select2.withColumnRenamed('open','open_stock2')

    select1 = select1.withColumnRenamed('date','date_stock1')
    select2 = select2.withColumnRenamed('date','date_stock2')

    finalSelect = select2.join(select1, select2.date_stock2 == select1.date_stock1, how='inner')

    finalSelect = finalSelect.withColumn('open_stock1', finalSelect.open_stock1.cast('double'))
    finalSelect = finalSelect.withColumn('open_stock2', finalSelect.open_stock2.cast('double'))

    correlation = finalSelect.stat.corr('open_stock2','open_stock1')

    # print(f'O fator de correlação entre {stock1} e {stock2} é: {correlation}')
    return correlation

# calculus('GOLD','TSLA')

def analiseGeral(stock):
    correlation_TSLA = []
    for ticker in ticker_array:
        value = calculus(stock,ticker)
        correlation_TSLA.append(([stock,ticker], value))

    sorted_correlation_TSLA = sorted(correlation_TSLA, key=lambda x: x[1], reverse=True)
    # pprint(sorted_correlation_TSLA[1])
    return sorted_correlation_TSLA[1]

list_best_correlations = []
for ticker in ticker_array:
    list_best_correlations.append(analiseGeral(ticker))

sorted_list_best_correlations = sorted(list_best_correlations, key=lambda x: x[1], reverse=True)
pprint(sorted_list_best_correlations)

#O fator de correlação entre TSLA e GOLD é: -0.4226823030418725
#O fator de correlação entre TSLA e AAPL é: 0.6845071664554809
#O fator de correlação entre AAPL e MSFT é: 0.8857053424342596
#O fator de correlação entre SAN.MC e REP.MC é: 0.7306779235336994
#O fator de correlação entre NRG e SAN.MC é: -0.14467494169822231
#O fator de correlação entre EA e DIS é: 0.3528158662516365

# fazer uma array com todos os tickers presentes na tabela
# calcular todos os fatores de correlação possiveis 
# organizar em uma outra array
# ordenar por ordem decrescente