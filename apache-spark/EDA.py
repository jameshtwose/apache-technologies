# %%
from pyspark import SparkFiles
from pyspark.sql import SparkSession
import seaborn as sns

# %%
# create a spark session
spark = SparkSession.builder.appName('EDA').getOrCreate()
# create a spark context
sc = spark.sparkContext
# %%
# add the file to the spark context
git_path = 'https://raw.githubusercontent.com/jameshtwose/example_deliverables/main/classification_examples/pima_diabetes/diabetes.csv'
sc.addFile(git_path)
path  = SparkFiles.get('diabetes.csv')

# %%
# read the csv file into a spark dataframe
df = spark.read.csv(f"file://{path}", header=True, inferSchema=True)
feature_list = df.columns[:-1]
# %%
# print the schema
df.printSchema()
# %%
# print the first 5 rows
df.show(5)
# %%
# show the summary statistics
df.describe().show()
# %%
_ = df.toPandas().hist(figsize=(10, 10))
# %%
plot_df = df.select(feature_list[0:3] + ["Outcome"]).toPandas()
_ = sns.pairplot(plot_df, hue='Outcome')
