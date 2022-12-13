## 实验四实验报告

### 1.spark安装

在https://spark.apache.org/downloads.html下载spark安装包，将安装包解压后移动到```/usr/local/Cellar```路径下。运行样例脚本如下，正常输出结果。

```
bin/spark-submit --class org.apache.spark.examples.SparkPi --master 'local[2]' ./examples/jars/spark-examples_2.12-3.3.1.jar 100
```

<center><img src="https://s1.imagehub.cc/images/2022/12/06/d48f98b27c8e6451a6c39fdf517032d5.png" width="60%"></center>

### 2.任务一

#### 2.1 编写Spark程序，统计stocks_small.csv表中每⽀股票每年的交易数量，并按年份，将股票交易数量从⼤到⼩进⾏排序。

利用spark.read读取stock_small.csv，将其存储为dataframe。按年份，先筛选出对应年份的交易，再根据stock_symbol对交易量进行求和，最后从大到小进行排序，将每年的统计结果单独存在一张表中。对应的代码以及运行结果截图如下。

```
tmp = df.filter(func.year(df['date']) == y)
tmp = tmp.groupBy('stock_symbol').agg({'stock_volume': 'sum'})
ans = tmp.sort(tmp['sum(stock_volume)'].desc())
```

<center><img src="https://s1.imagehub.cc/images/2022/12/11/71dc6836795c4c27bfe3dec166a4a4d7.png" width="10%"></center>

#### 2.2 编写Spark程序，统计stocks_small.csv表中收盘价（price_close）⽐开盘价（price_open）差价最⾼的前十条记录。

利用spark.read读取stock_small.csv，将其存储为dataframe。计算每条记录收盘价与开盘价差的绝对值，然后从大到小进行排序，选取前十条保存。对应的代码以及运行结果截图如下。

```
df = df.select(df['exchange'], df['stock_symbol'], df['date'], df['stock_price_close'], df['stock_price_open'], (func.abs(df['stock_price_close'] - df['stock_price_open'])).alias('tmp'))
ans = df.sort(df['tmp'].desc()).limit(10)
```

<center><img src="https://s1.imagehub.cc/images/2022/12/11/e7270f985a4fc6ab62b6eb73b57deafb.png" width="30%"></center>

### 3.任务二

#### 3.1 统计IBM公司（stock_symbol = IBM）从2000年起所有⽀付股息的交易⽇（dividends表中有对应记录）的收盘价（stock_price_close）。

利用spark.read读取stock_small.csv和dividens_small.csv文件，并创建视图，利用spark_sql以及以下sql语句先统计出从2000年取所有支付股息的交易日，再统计出对应交易日的收盘价。

```
select date, stock_symbol, stock_price_close from stock_small where stock_symbol = 'IBM' and date in (select date from dividends_small where symbol = 'IBM')
```

将结果保存在csv文件中，结果截图如下所示。

<center><img src="https://s1.imagehub.cc/images/2022/12/09/fb7a51f86d8934b51f871dd4b9524471.png" width="10%"></center>

#### 3.2 统计苹果公司 (stock_symbol = AAPL) 年平均调整后收盘价(stock_price_adj_close) ⼤于50美元的年份以及当年的年平均调整后收盘价。

同样地，利用spark.read读取stock_small.csv文件，并创建视图，利用spark_sql以及以下sql语句先求出苹果公司每年的年平均调整后收盘价，再筛选出其中大于50美元的年份。

```
select year(date) as year, stock_price_adj_close as price from stock_small where stock_symbol = 'AAPL'

select year, avg from (select year, AVG(price) as avg from AAPL group by year) where avg > 50
```
将结果保存在csv文件中，结果截图如下所示。

<center><img src="https://s1.imagehub.cc/images/2022/12/09/3659a90608ed08b796f04a735c8bd7c7.png" width="20%"></center>

### 4.任务三：根据表stock_data.csv 中的数据，基于Spark MLlib 或者Spark ML 编写程序在收盘之前预测当日股票的涨跌，并评估实验结果的准确率。

首先注意到stock_data.csv中的数据保存的类型是string，因此需要进行数据类型的转换。

```
for x in ['stock_price_open', 'stock_price_high', 'stock_price_low', 'stock_volume', 'label']:
    df = df.withColumn(x, df[x].astype('float'))
```

接着需要划分特征和想要预测的标签。

```
vectorAssembler = VectorAssembler(inputCols=['stock_price_open', 'stock_price_high', 'stock_price_low', 'stock_volume'], outputCol = 'features')
new_df = vectorAssembler.transform(df)
new_df = new_df.select(['features', 'label'])
```

同时，需要去除重复数据和缺失值。

```
new_df = new_df.dropDuplicates()
new_df = new_df.na.drop()
```

按8:2的比例划分数据集，得到训练集和测试集。

```
train, test = new_df.randomSplit([0.8, 0.2], seed = 10)
```

接下来利用不同的模型进行训练，以对率回归为例，首先训练模型再对模型进行评估。

```
lr = LogisticRegression(featuresCol='features', labelCol='label')
lr_model = lr.fit(train)
predictions = lr_model.transform(test)
lr_evaluator = BinaryClassificationEvaluator().setLabelCol('label')
accuracy = lr_evaluator.evaluate(predictions)
tp = predictions[(predictions.label == 1) & (predictions.prediction == 1)].count()
tn = predictions[(predictions.label == 0) & (predictions.prediction == 0)].count()
fp = predictions[(predictions.label == 0) & (predictions.prediction == 1)].count()
fn = predictions[(predictions.label == 1) & (predictions.prediction == 0)].count()
recall = float(tp) / (tp + fn)
precision = float(tp) / (tp + fp)
f1 = 2 * recall * precision / (recall + precision)
result.append(['Logistic Regression', accuracy, precision, recall, f1])
```

一共使用了四种不同的模型：对率回归，决策树，随机森林，朴素贝叶斯。模型评估结果保存在result文件夹中的task3.csv中。观察结果可以发现，对率回归的准确率较高，达到80%，但四个模型的f1值都偏低，可能的原因是模型普遍会判断当日股票下跌，使得f1偏低。因此需要改进数据以及数据输入的特征，实现更可靠的预测。

<center><img src="https://s1.imagehub.cc/images/2022/12/11/2602c69e805e7e82600c92469e6cac24.png" width="60%"></center>
