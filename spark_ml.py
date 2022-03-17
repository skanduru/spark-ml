#!/usr/bin/env python

"""
Pyspark - Machine Learning Pipelines
Use pyspark for big data analysis and modeling to perform metric evaluation and model tuning.

Tasks to be performed:
 - fit a Logistic Regression model in PySpark;
 - perform cross-validation in PySPark;
 - evaluate the model performances;
 - perform inference on new, unseen data.

"""

import os
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-8-openjdk-amd64"

"""
1. Dataset Creation¶
"""

from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
file_path = "data/tips.csv"
tips = spark.read.csv(file_path, header=True)
tips.createOrReplaceTempView("tips")
tips_table = spark.table("tips")
tips_table = tips_table.withColumn("perc_tips", (tips.tip/tips.total_bill)*100)

tips_table = tips_table.withColumn("total_bill",tips_table.total_bill.cast("double"))
tips_table = tips_table.withColumn("tip", tips_table.tip.cast("double"))
tips_table = tips_table.withColumn("size", tips_table.size.cast("integer"))

tips_table.show()

"""
Build a Machine Learning Pipeline with PySpark
Assume that our restaurant's clients can be divided in two categories, based on their generosity. In particular, we can state that one is generous if he/she gives a tip of at least 15 USD, otherwise we call him/her a stingy person. We can easily build a labelled dataset using this information by performing some data transformation with PySpark.

To do so, we use the withColumn method to create two columns:

a binary one called is_stingy, which is based on the boolean condition tips.perc_tips < 15;
another one called label which actually converts the boolean condition into a binary label - 0 if generous, else 1.
"""

tips_table = tips_table.withColumn("is_stingy", tips_table.perc_tips < 15)
tips_table = tips_table.withColumn("label", tips_table.is_stingy.cast("integer"))

tips_table.show()

"""
Let's check the distribution of the label column with respect to the gender:

"""

by_label_sex = tips_table.groupBy("label", "sex")
by_label_sex.count().show()

"""
Encode categorical variables using PySpark

When fitting a Machine Learning model we need to perform a few steps of data manipulation and transformation, such as converting categorical features into numerical ones. To do so, we can employ the PySpark StringIndexer function. The StringIndexer allows to encode a categorical feature into a numerical one. It starts by assigning the lowest rank - i.e. zero - to the most frequent value of that column.

"""
from pyspark.ml.feature import StringIndexer

"""
We here apply the StringIndexer to the sex column as follows: we pass the raw column sex, and we specify the output column as sex_index. We then apply a fit and transform method on the tips_table to convert the sex column into a categorical one
"""

sex_indexer = StringIndexer(inputCol="sex", outputCol="sex_index")

df1 = sex_indexer.fit(tips_table).transform(tips_table)
df1.show()

"""
Please note that the value `"Male"` gets index 0 because it is the most frequent, and we can easily check this:
"""

tips_table.groupBy("sex").count().show()

"""
We now apply to sex, smoker, day and time both the StringIndexer and the OneHotEncoder: while the former has been discussed just a few lines above, the latter deserves a few more words. In a nutshell, this transformer class creates, in a new Spark DataFrame, a dummy variable for each single value observed in the column.

For example with 4 categories, an input value of 2.0 would map to an output vector of [0.0, 0.0, 1.0]. The last category is not included by default (configurable via the argument dropLast), because it makes the vector entries sum up to one, and hence linearly dependent.
"""

from pyspark.ml.feature import OneHotEncoder

# sex
sex_indexer = StringIndexer(inputCol="sex", outputCol="sex_index")
sex_encoder = OneHotEncoder(inputCol="sex_index", outputCol="sex_fact")

# smoker
smoker_indexer = StringIndexer(inputCol="smoker", outputCol="smoker_index")
smoker_encoder = OneHotEncoder(inputCol="smoker_index", outputCol="smoker_fact")

# day
day_indexer = StringIndexer(inputCol="day", outputCol="day_index")
day_encoder = OneHotEncoder(inputCol="day_index", outputCol="day_fact")

# time
time_indexer = StringIndexer(inputCol="time", outputCol="time_index")
time_encoder = OneHotEncoder(inputCol="time_index", outputCol="time_fact")

"""
Assembling Columns using VectorAssembler
Now, our goal is to combine the numerical features ["total_bill", "tip", "time_fact", "day_fact", "smoker_fact", "sex_fact",  "size", "perc_tips"] into a single vector column, that we call for simplicity features. To do so, we use the VectorAssembler, which is a transformer that combines a given list of columns into a single vector column. VectorAssembler will have two parameters:

inputCols: list of features to combine into a single vector column;
outputCol: the new column that will contain the transformed vector.
Let’s create our assembler
"""

from pyspark.ml.feature import VectorAssembler
vec_assembler = VectorAssembler(
    inputCols=["total_bill", "tip", "time_fact", "day_fact", "smoker_fact", "sex_fact",  "size", "perc_tips"], 
    outputCol="features"
    )

"""
Now using this assembler we can transform the raw dataset and take a look as the result. However, we have an issue: in the original dataset, we do not have access to, for instance, the columns time_index and time_fact, since they come from the StringIndexer and OneHotEncoder, respectively. So we need to somehow combine them using the Pipeline object.

"""

"""
Create the Data Pipeline in PySpark
"""

from pyspark.ml import Pipeline
tips_pipe = Pipeline(
    stages=[
            sex_indexer, sex_encoder, 
            smoker_indexer, smoker_encoder, 
            day_indexer, day_encoder, 
            time_indexer, time_encoder, 
            vec_assembler
            ]
    )

piped_data = tips_pipe.fit(tips_table).transform(tips_table)

"""
Now using this pipeline we can transform the original dataset and take a look as the result:
"""

piped_data.show()

"""
We see that the last column features is the combination of several columns obtained by applying the StringIndexer, OneHotEncoder and VectorAssembler.
"""

"""
Classify customers based on their "generosity"¶

Let us split the data into the standard rule 80-20: 80% of the raw data is used to train a classifier, and the rest to test the model on unseed data.

"""
training, test = piped_data.randomSplit([.8, .2])

"""
We use a Logistic Regression to classify our clients in those two categories. In case you want to master your knowledge on Logistic Regression, please check our content library: we have a few courses where this concept is explained. Among many, you can refer to Building Machine Learning Pipelines with scikit-learn - Part 2.
"""

from pyspark.ml.classification import LogisticRegression
lr = LogisticRegression(featuresCol='features', labelCol='label')

"""
We can fit easily the modelby passing the training data, and then we can check the model performances by looking at the raw data, as follows:
"""

lr_fit = lr.fit(training)
lr_fit.transform(training).select('features', 'rawPrediction', 'probability', 'prediction', 'label').show()

"""
Qualitatively, we are not doing bad. However, this is just a qualitative evaluation. We need to be much more consistent on both the way we trained the model and the evaluation process. As for the first, model's performances depend on the way the data is splitted. So a way to avoid splitting bias is to perform cross-validation. Also, please keep in mind that any model is characterized by a set of hyperparameters, that typically need to be estimated. We can therefore add a specific grid for a set of hyperparameters, and perform a search to look for the best hyperparameters combination.
"""

from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.ml.evaluation import BinaryClassificationEvaluator
import numpy as np
grid = ParamGridBuilder()
grid = grid.addGrid(lr.regParam, np.arange(0, .1, .01))
grid = grid.addGrid(lr.elasticNetParam, [0,1]) # used to perform regularization
grid = grid.build()
evaluator = BinaryClassificationEvaluator(metricName="areaUnderROC")

"""
We initialize a CrossValidator instance by passing the model we wish to use - in our case a Logistic Regression, the hyperparameters we wish to explore, and the metric used to choose the best model.
"""

cv = CrossValidator(estimator=lr,
               estimatorParamMaps=grid,
               evaluator=evaluator
               )

"""
The following snippet might take a while, since we need to perform grid search cross validation:
"""

models = cv.fit(training)
best_lr = models.bestModel

"""
We can easily access to the best model, as follows:

Testing the model on unseen data

Let us evaluate the model on new data: we pick the best model, we transform the raw test data with respect to that model, and we evaluate the performances.
"""

test_results = best_lr.transform(test)
test_results.select('features', 'rawPrediction', 'probability', 'prediction', 'label').show()
metric_test = evaluator.evaluate(test_results)
print(f'Area Under Receiver Operating Characteristic: {metric_test}')

"""
We are perfectly predicting the test labels.

"""

with open('vcf_01.txt', 'w') as f:
    f.write("%s\n" % metric_test)
