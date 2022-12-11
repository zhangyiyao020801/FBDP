import pyspark
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import functions as func

# conf = SparkConf().setMaster('local[2]').setAppName('task1_1')
# sc = SparkContext(conf=conf)

file = '../data/stock_small.csv'
spark = SparkSession.builder.config("spark.driver.memory", "16g").getOrCreate()
df = spark.read.options(header='True').csv(file)

y = 2000
while y < 2010:
    tmp = df.filter(func.year(df['date']) == y)
    tmp = tmp.groupBy('stock_symbol').agg({'stock_volume': 'sum'})
    ans = tmp.sort(tmp['sum(stock_volume)'].desc())
    ans.show()
    
    ans = ans.toPandas()
    ans.rename(columns={"stock_symbol": "股票代码", "sum(stock_volume)": "交易数量"}, inplace=True)
    ans['年份'] = y
    ans = ans[['年份', '股票代码', '交易数量']]
    ans.to_csv('../result/task1_1/{}.csv'.format(y), index=None)
    y += 1
