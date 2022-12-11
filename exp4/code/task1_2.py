import pyspark
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import functions as func

# conf = SparkConf().setMaster('local[2]').setAppName('task1_2')
# sc = SparkContext(conf=conf)

spark = SparkSession.builder.config("spark.driver.memory", "16g").getOrCreate()
df = spark.read.options(header='True').csv('../data/stock_small.csv')

df = df.select(df['exchange'], df['stock_symbol'], df['date'], df['stock_price_close'], df['stock_price_open'], (func.abs(df['stock_price_close'] - df['stock_price_open'])).alias('tmp'))
ans = df.sort(df['tmp'].desc()).limit(10)
ans.show()

ans = ans.toPandas()
ans.rename(columns={'exchange': '交易所', 'stock_symbol': '股票代码', 'date': '交易日期', 'stock_price_close': '收盘价', 'stock_price_open': '开盘价', 'tmp': '差价'}, inplace=True)
ans.to_csv('../result/task1_2.csv', index=None)
