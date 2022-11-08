## Homework5

## 代码仓库链接：https://github.com/zhangyiyao020801/FBDP.git

首先，观察原始数据集，RedditNews.csv中共有两列分别是日期和新闻标题，
在新闻标题中会出现换行符等情况。
因此，为了方便进行词频统计，利用python对原始数据进行预处理，
去除原始数据标题中的换行符并将日期和对应新闻标题相拼接，每个新闻标题占一行。
将处理后的数据保存在RedditNews_new.txt中。
具体代码在RedditNews_preprocess.ipynb中，如下所示。

```
import csv
import pandas as pd

csvFile = open("RedditNews.csv","r")
reader = csv.reader(csvFile)
listReader =list(reader)
txtFile = 'RedditNews_new.txt'
writer = open(txtFile, 'w')

for row in listReader:
    line = row[0] + row[1]
    line = line.replace('\n', ' ')
    line += '\n'
    writer.write(line)
```

预处理后的txt文件截图如下。

<center><img src='https://s1.imagehub.cc/images/2022/10/23/2022-10-23-21.19.51.png' size=20%></center>

将RedditNews_new.txt和stop-word-list.txt上传到hdfs文件管理系统。

```
bin/hdfs dfs -put /Users/zhangyiyao/Desktop/大三上/金融大数据处理技术/FBDP/RedditNews_new.txt input/RedditNews_new.txt
bin/hdfs dfs -put /Users/zhangyiyao/Desktop/大三上/金融大数据处理技术/FBDP/stop-word-list.txt input/stop-word-list.txt
```

实验利用vscode完成，利用maven构建管理代码，生成java文件对应的jar包后，需要在命令行输入```zip -d FBDP.jar META-INF/LICENSE```，删除META-INF/LICENSE文件。

### 1.输出数据集出现的前100个高频单词；

在wordcount1.0的基础上，需要考虑忽略单词大小写，忽略停词以及对单词进行排序。

参考wordcount2.0以及使用正则表达式可以完成忽略单词大小写，忽略停词，具体代码在src/main/java/wc/WordCount.java中TokenizerMapper类的map函数实现。

对于单词排序，可以加入SortMapper和DescWritableComparator类，完成单词的倒序排列。

详细代码可以参考src/main/java/wc/WordCount.java。

使用以下命令运行，得到的输出文件在output/wc/result_wc中。
```
bin/hadoop jar /Users/zhangyiyao/Desktop/大三上/金融大数据处理技术/FBDP/FBDP.jar input/RedditNews_new.txt result_wc/wordcount result_wc/wordsort -skip input/stop-word-list.txt
bin/hdfs dfs -get result_wc /Users/zhangyiyao/Desktop/大三上/金融大数据处理技术/FBDP/output/wc
```

output/result_wc/wordsort/part-r-00000中展示了前100个高频单词，截图如下。

<center><img src='https://s1.imagehub.cc/images/2022/10/23/2022-10-23-17.10.01.png' size=20%></center>

### 2.输出每年热点新闻中出现的前100个高频单词。

相较于上一个任务，需要按照新闻的年份进行分类。因此可以考虑在key中加入对应的年份，并利用SortPartitioner完成按年份的分类。

详细代码可以参考src/main/java/wc/WordCount_by_year.java。

使用以下命令运行，得到的输出文件在output/wc/result_wc中。
```
bin/hadoop jar /Users/zhangyiyao/Desktop/ 大三上/金融大数据处理技术/FBDP/FBDP.jar input/RedditNews_new.txt result_wc_by_year/wordcount result_wc_by_year/wordsort -skip input/stop-word-list.txt
bin/hdfs dfs -get result_wc_by_year /Users/zhangyiyao/Desktop/大三上/金融大数据处理技术/FBDP/output/wc
```

output/result_wc_by_year/wordsort中的9个part文件分别展示了2008～2016年的前100个高频单词，以part-r-00000中2008年的高频单词为例，截图如下。

<center><img src='https://s1.imagehub.cc/images/2022/10/23/2022-10-23-17.12.45.png' size=20%></center>

### 3.补充

在完成过程中，利用正则表达式忽略停词过程中会出现一些特殊情况，
此处使用多次循环替换忽略停词，可以考虑在预处理阶段更好地避免一些特殊情况。

另外，在按年份分类的任务中，我根据观察数据集发现新闻年份在2008年～2016年之间，
因此固定了9个类别，对于新闻年份范围难以得到的数据集，可扩展性有待进一步提高。
