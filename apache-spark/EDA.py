# %%
import pyspark
from pyspark import SparkFiles
from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import LogisticRegression
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np

# %%
# create a spark session
spark = SparkSession.builder.appName("EDA").getOrCreate()
# create a spark context
sc = spark.sparkContext
# %%
# add the file to the spark context
git_path = "https://raw.githubusercontent.com/jameshtwose/example_deliverables/main/classification_examples/pima_diabetes/diabetes.csv"
sc.addFile(git_path)
path = SparkFiles.get("diabetes.csv")

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
_ = sns.pairplot(plot_df, hue="Outcome")

# %%
# create a vector assembler
assembler = VectorAssembler(inputCols=feature_list, outputCol="features")
prepped_df = assembler.transform(df)

# %%
lr = LogisticRegression(featuresCol="features", labelCol="Outcome")
# %%
# fit the model
lrModel = lr.fit(prepped_df)
# %%
# get the summary statistics (ROC, AUC)
roc_df = lrModel.summary.roc.toPandas()
roc_auc = lrModel.summary.areaUnderROC
# %%
_ = sns.lineplot(x="FPR", y="TPR", data=roc_df)
_ = plt.title("ROC Curve")
_ = plt.annotate(f"AUC: {roc_auc:.3f}", (0.8, 0.1))

# %%
true_pred_df = lrModel.summary.predictions.select("Outcome", "prediction")


# %%
def plot_confusion_matrix(true_pred_df: pyspark.sql.DataFrame):
    """
    Parameters
    ----------
    true_pred_df : pyspark.sql.DataFrame
        A dataframe with the columns 'Outcome' and 'prediction'.
        The values in the 'Outcome' and 'prediction' column should be 0 or 1,

    Returns
    -------
    fig : matplotlib.pyplot.figure
        A confusion matrix plot
    axes : matplotlib.pyplot.axes
        The axes of the confusion matrix plot

    """

    tp = true_pred_df.filter("Outcome == 1 AND prediction == 1").count()
    fp = true_pred_df.filter("Outcome == 0 AND prediction == 1").count()
    tn = true_pred_df.filter("Outcome == 0 AND prediction == 0").count()
    fn = true_pred_df.filter("Outcome == 1 AND prediction == 0").count()

    cf_matrix = np.array([[tp, fp], [fn, tn]])

    fig, axes = plt.subplots()
    _ = axes.set_title("Confusion Matrix")
    _ = sns.heatmap(cf_matrix, annot=True, fmt="d", cmap="Blues", ax=axes)
    _ = axes.set_xlabel("Predicted")
    _ = axes.set_ylabel("Actual")
    _ = axes.set_xticks([0.5, 1.5])
    _ = axes.set_xticklabels(["Negative", "Positive"])
    _ = axes.set_yticks([0.5, 1.5])
    _ = axes.set_yticklabels(["Positive", "Negative"])

    return fig, axes


# %%
fig, ax = plot_confusion_matrix(true_pred_df)
# %%
