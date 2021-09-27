from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import unix_timestamp, monotonically_increasing_id, col
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
from datetime import datetime

sc = SparkContext()
spark = SparkSession(sc)


def data_import(file = ""):
    if file == "":
        return None

    df = spark.read.format("csv").load(file, inferSchema=True, header=True)

    print("\n\n\nImported Train/Test Dataset Preview:")
    df.show(5)

    return df


def data_preprocessing(df):

    df = df.drop('Currency')

    feature_cols = df.drop('Date', 'Closing Price (USD)').columns
    df = df.withColumn('Date', df.Date.cast('timestamp'))

    return df, feature_cols


def feature_vector(feature_columns):

    assembler = VectorAssembler(inputCols=feature_columns, outputCol="features")
    assembler = assembler.transform(data)

    return assembler


def lregresor(train, test):

    lr = LinearRegression(featuresCol="features", labelCol="Closing Price (USD)")
    model = lr.fit(train)

    evaluation = model.evaluate(test)
    print("Absolute mean error: ", evaluation.meanAbsoluteError)
    print("Mean squared error, root: ", evaluation.rootMeanSquaredError)
    print("r2: ", evaluation.r2)

    predict = model.transform(test)
    return predict, evaluation


def get_timeline(str_date):
    return datetime.strptime(str_date, "%Y-%m-%d")


def fields_to_csv(df, fields=None, timeline=False, timeline_col=None, file="output"):

    if fields is None:
        return None

    sel = df.select(fields)

    if timeline is True:
        if timeline_col is None:
            return None
        sel = sel.sort(timeline_col, ascending=True)

    sel = sel.persist()

    print("\n\n\nSaved " + file + "Dataset Preview:")
    sel.show(n=20, truncate=False)

    sel.write.save(file + ".csv", format="csv")


data = data_import(file="eth_USD_daily_prices.csv")

data, feature_columns = data_preprocessing(df=data)

assembled_data = feature_vector(feature_columns=feature_columns)

train = assembled_data[assembled_data['Date'] < get_timeline("2019-06-01")]
print("\n\n\nTrain Dataset Preview, before Just 1st, 2019:")
train.show(5)

test = assembled_data[assembled_data['Date'] >= get_timeline("2019-06-01")]
print("\n\n\nTest Dataset Preview, after June 1st, 2019 (inclusive):")
test.show(5)

predictions, _ = lregresor(train=train, test=test)
predictions = predictions.withColumn('Date', predictions.Date.cast('timestamp'))

print("\nPredicted values: ")
predictions.show(5)

print(predictions.dtypes)

fields_to_csv(df=predictions, fields=['Date', 'Closing Price (USD)', 'prediction'],
              timeline=True, timeline_col='Date',
              file="EVAL_eth_USD_forecasting")
