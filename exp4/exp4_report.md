## 实验四实验报告

### 1.spark安装

在https://spark.apache.org/downloads.html下载spark安装包，将安装包解压后移动到```/usr/local/Cellar```路径下。运行样例脚本如下，正常输出结果。

```
bin/spark-submit --class org.apache.spark.examples.SparkPi --master 'local[2]' ./examples/jars/spark-examples_2.12-3.3.1.jar 100
```

<center><img src="https://s1.imagehub.cc/images/2022/12/06/d48f98b27c8e6451a6c39fdf517032d5.png" width="60%"></center>

### 2.任务一

#### 2.1 编写Spark程序，统计stocks_small.csv表中每⽀股票每年的交易数量，并按年份，将股票交易数量从⼤到⼩进⾏排序。

#### 2.2 编写Spark程序，统计stocks_small.csv表中收盘价（price_close）⽐开盘价（price_open）差价最⾼的前十条记录。

### 3.任务二

#### 3.1 统计IBM公司（stock_symbol = IBM）从2000年起所有⽀付股息的交易⽇（dividends表中有对应记录）的收盘价（stock_price_close）。

利用spark.read读取stock_small.csv和dividens_small.csv文件，并创建视图，利用spark_sql以及以下sql语句先统计出从2000年取所有支付股息的交易日，再统计出对应交易日的收盘价。

```
select date, stock_symbol, stock_price_close from stock_small where stock_symbol = 'IBM' and date in (select date from dividends_small where symbol = 'IBM')
```

将结果保存在csv文件中，结果截图如下所示。

<center><img src="https://s1.imagehub.cc/images/2022/12/09/fb7a51f86d8934b51f871dd4b9524471.png" width="20%"></center>

#### 3.2 统计苹果公司 (stock_symbol = AAPL) 年平均调整后收盘价(stock_price_adj_close) ⼤于50美元的年份以及当年的年平均调整后收盘价。

同样地，利用spark.read读取stock_small.csv文件，并创建视图，利用spark_sql以及以下sql语句先求出苹果公司每年的年平均调整后收盘价，再筛选出其中大于50美元的年份。

```
select year(date) as year, stock_price_adj_close as price from stock_small where stock_symbol = 'AAPL'

select year, avg from (select year, AVG(price) as avg from AAPL group by year) where avg > 50
```
将结果保存在csv文件中，结果截图如下所示。

<center><img src="https://s1.imagehub.cc/images/2022/12/09/3659a90608ed08b796f04a735c8bd7c7.png" width="20%"></center>

### 4.任务三：根据表stock_data.csv 中的数据，基于Spark MLlib 或者Spark ML 编写程序在收盘之前预测当日股票的涨跌，并评估实验结果的准确率。