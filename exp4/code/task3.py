import pandas as pd
import pyspark
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import LogisticRegression, DecisionTreeClassifier, RandomForestClassifier, NaiveBayes
from pyspark.ml.evaluation import BinaryClassificationEvaluator

# conf = SparkConf().setMaster('local[2]').setAppName('task3')
# sc = SparkContext(conf=conf)

spark = SparkSession.builder.config("spark.driver.memory", "16g").getOrCreate()
df = spark.read.options(header='True').csv('../data/stock_data.csv')

df = df.drop('exchange', 'stock_symbol', 'date')

for x in ['stock_price_open', 'stock_price_high', 'stock_price_low', 'stock_volume', 'label']:
    df = df.withColumn(x, df[x].astype('float'))

vectorAssembler = VectorAssembler(inputCols=['stock_price_open', 'stock_price_high', 'stock_price_low', 'stock_volume'], outputCol = 'features')
new_df = vectorAssembler.transform(df)
new_df = new_df.select(['features', 'label'])
new_df = new_df.dropDuplicates()
new_df = new_df.na.drop()
train, test = new_df.randomSplit([0.8, 0.2], seed = 10)

result = []

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

dt = DecisionTreeClassifier(featuresCol='features', labelCol='label', maxDepth=5)
dt_model = dt.fit(train)
predictions = dt_model.transform(test)
dt_evaluator = BinaryClassificationEvaluator().setLabelCol('label')
accuracy = dt_evaluator.evaluate(predictions)
tp = predictions[(predictions.label == 1) & (predictions.prediction == 1)].count()
tn = predictions[(predictions.label == 0) & (predictions.prediction == 0)].count()
fp = predictions[(predictions.label == 0) & (predictions.prediction == 1)].count()
fn = predictions[(predictions.label == 1) & (predictions.prediction == 0)].count()
recall = float(tp) / (tp + fn)
precision = float(tp) / (tp + fp)
f1 = 2 * recall * precision / (recall + precision)
result.append(['Decision Tree', accuracy, precision, recall, f1])

rf = RandomForestClassifier(featuresCol='features', labelCol='label')
rf_model = rf.fit(train)
predictions = rf_model.transform(test)
rf_evaluator = BinaryClassificationEvaluator().setLabelCol('label')
accuracy = rf_evaluator.evaluate(predictions)
tp = predictions[(predictions.label == 1) & (predictions.prediction == 1)].count()
tn = predictions[(predictions.label == 0) & (predictions.prediction == 0)].count()
fp = predictions[(predictions.label == 0) & (predictions.prediction == 1)].count()
fn = predictions[(predictions.label == 1) & (predictions.prediction == 0)].count()
recall = float(tp) / (tp + fn)
precision = float(tp) / (tp + fp)
f1 = 2 * recall * precision / (recall + precision)
result.append(['Random Forest', accuracy, precision, recall, f1])

nb = NaiveBayes(featuresCol='features', labelCol='label')
nb_model = nb.fit(train)
predictions = nb_model.transform(test)
nb_evaluator = BinaryClassificationEvaluator().setLabelCol('label')
accuracy = nb_evaluator.evaluate(predictions)
tp = predictions[(predictions.label == 1) & (predictions.prediction == 1)].count()
tn = predictions[(predictions.label == 0) & (predictions.prediction == 0)].count()
fp = predictions[(predictions.label == 0) & (predictions.prediction == 1)].count()
fn = predictions[(predictions.label == 1) & (predictions.prediction == 0)].count()
recall = float(tp) / (tp + fn)
precision = float(tp) / (tp + fp)
f1 = 2 * recall * precision / (recall + precision)
result.append(['Naive Bayes', accuracy, precision, recall, f1])

print(result)

df = pd.DataFrame(result)
df.columns = ['Model', 'Accuracy', 'precision', 'recall', 'f1']
df.to_csv('../result/task3.csv', index=None)
