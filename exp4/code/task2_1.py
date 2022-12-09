import pyspark
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession

# conf = SparkConf().setMaster('local[2]').setAppName('task2_1')
# sc = SparkContext(conf=conf)

spark = SparkSession.builder.config("spark.driver.memory", "16g").getOrCreate()
df1 = spark.read.options(header='True').csv('../data/dividends_small.csv')
df1.createOrReplaceTempView("dividends_small")
df2 = spark.read.options(header='True').csv('../data/stock_small.csv')
df2.createOrReplaceTempView("stock_small")

ans = spark.sql("select date, stock_symbol, stock_price_close from stock_small where stock_symbol = 'IBM' and date in (select date from dividends_small where symbol = 'IBM')")
ans = ans.toPandas()
ans.rename(columns={'date': '交易日期','stock_symbol': '股票代码', 'stock_price_close': '收盘价'}, inplace=True)
ans.to_csv('../result/task2_1.csv', index=None)
