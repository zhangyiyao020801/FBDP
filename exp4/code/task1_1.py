import pyspark
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession

# conf = SparkConf().setMaster('local[2]').setAppName('task1_1')
# sc = SparkContext(conf=conf)

file = '../data/stock_small.csv'
spark = SparkSession.builder.config("spark.driver.memory", "16g").getOrCreate()
df = spark.read.options(header='True').csv(file)
df.createOrReplaceTempView("stock_small")

df = spark.sql("select year, stock_symbol, vol from (select year(date) as year, stock_symbol, SUM(stock_volume) as vol from stock_small group by stock_symbol, year(date))")
df.createOrReplaceTempView("vol_year")

y = 2000
while y < 2010:
    ans = spark.sql("select * from vol_year where year = {} order by vol desc".format(y))
    ans = ans.toPandas()
    ans.rename(columns={"year": "年份", "stock_symbol": "股票代码", "vol": "交易数量"}, inplace=True)
    ans.to_csv('../result/task1_1/{}.csv'.format(y), index=None)
    y += 1
