## 实验四实验报告

### 1.spark安装

在https://spark.apache.org/downloads.html下载spark安装包，将安装包解压后移动到```/usr/local/Cellar```路径下。运行样例脚本如下，正常输出结果。

```
bin/spark-submit --class org.apache.spark.examples.SparkPi --master 'local[2]' ./examples/jars/spark-examples_2.12-3.3.1.jar 100
```

<center><img src="https://s1.imagehub.cc/images/2022/12/06/d48f98b27c8e6451a6c39fdf517032d5.png" width="60%"></center>

### 2.任务一

#### 2.1 编写Spark程序，统计stocks_small.csv 表中每⽀支股票每年年的交易易数量量，并按年年份，将股票交易数量从⼤到⼩进⾏排序。

#### 2.2 编写Spark程序，统计stocks_small.csv表中收盘价（price_close）⽐开盘价（price_open）差价最⾼的前十条记录。
