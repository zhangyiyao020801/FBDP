import pyspark
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession

# conf = SparkConf().setMaster('local[2]').setAppName('task2_1')
# sc = SparkContext(conf=conf)

spark = SparkSession.builder.config("spark.driver.memory", "16g").getOrCreate()
df = spark.read.options(header='True').csv('../data/stock_small.csv')
df.createOrReplaceTempView("stock_small")

df = spark.sql("select year(date) as year, stock_price_adj_close as price from stock_small where stock_symbol = 'AAPL'")
df.createOrReplaceTempView("AAPL")

ans = spark.sql("select year, avg from (select year, AVG(price) as avg from AAPL group by year) where avg > 50 order by year")
ans = ans.toPandas()
ans.rename(columns={'year': '年份', 'avg': '年平均调整后收盘价'}, inplace=True)
ans.to_csv('../result/task2_2.csv', index=None)
