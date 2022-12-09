import pyspark
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession

# conf = SparkConf().setMaster('local[2]').setAppName('task1_2')
# sc = SparkContext(conf=conf)

file = '../data/stock_small.csv'
spark = SparkSession.builder.config("spark.driver.memory", "16g").getOrCreate()
df = spark.read.options(header='True').csv(file)

df.createOrReplaceTempView("stock_small")

ans = spark.sql("select * from stock_small order by abs(stock_price_close - stock_price_open) DESC limit 10")
ans.show()
ans = ans.toPandas()
ans = ans[['exchange', 'stock_symbol', 'date', 'stock_price_close', 'stock_price_open']]
ans['差价'] = abs(ans['stock_price_close'].astype(float) - ans['stock_price_open'].astype(float))
ans.rename(columns={'exchange': '交易所', 'stock_symbol': '股票代码', 'date': '交易日期', 'stock_price_close': '收盘价', 'stock_price_open': '开盘价'}, inplace=True)
ans.to_csv('../result/task1_2.csv', index=None)
