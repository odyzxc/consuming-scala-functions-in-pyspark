import pytest
from pyspark.sql.types import StringType
import pyspark.sql.functions as F

from pyspark.sql import DataFrame

from pyspark.sql import SQLContext
import time
import random

from consume_my_functions.name_cleansing import cleanse_names, match_parent_name


@pytest.fixture()
def test_df(spark):
    test_data = [{"Col1": f"{random.choice(['one', 'two', 'three'])}_{random.randint(0, 100)}"} for _ in range(1000)]
    test_df = spark.createDataFrame(test_data)
    return test_df


@pytest.fixture()
def cleaned_cases_df(spark):
    test_cases_df = (
        spark.read.option("header", True)
        .option("inferSchema", True)
        .csv("./resources/test_cases.csv")
    )

    cleaned_cases_df = cleanse_names(test_cases_df, "CompanyName")
    cleaned_cases_df = cleaned_cases_df.withColumn("Supplier Parent Name", F.lit(None))
    return cleaned_cases_df


big_batch_of_data = [{"Supplier ID": i, "Supplier Cleaned Name": f"Sup{i}", "Supplier Address": "11 Broomfield Place", "Suppliers ISO Code": "GBR"} for i in range(10000)]


@pytest.fixture()
def big_batch_df(spark):
    test_df = spark.createDataFrame(big_batch_of_data)

    test_df = test_df.withColumn("Supplier Parent Name", F.lit(None))
    return test_df


def test_python_udf(spark, test_df):
    zduporyzuj_udf = F.udf(lambda value: value + "dupa", StringType())

    start_time = time.time()

    test_df = test_df.withColumn("zduporyzowano", zduporyzuj_udf(F.col("Col1")))

    print(test_df.show(20, False))
    print(f"Python udf execution time: {time.time() - start_time}")


def test_python_standard_approach(spark, test_df):
    start_time = time.time()

    test_df = test_df.withColumn("zduporyzowano", F.concat(F.col('Col1'), F.lit('dupa')))

    print(test_df.show(20, False))
    print(f"Python standard approach execution time: {time.time() - start_time}")


def test_scala_udf_poc(spark, test_df):
    spark.udf.registerJavaFunction("poc_udf", "pl.odyzxc.myfunctions.PocUdf", StringType())

    start_time = time.time()

    test_df = test_df.withColumn("zduporyzowano", F.expr("poc_udf(Col1)"))

    print(test_df.show(20, False))
    print(f"Scala udf execution time: {time.time() - start_time}")


def test_performance_of_python_module(spark, cleaned_cases_df):
    start_time = time.time()

    matched_cases_df = match_parent_name(cleaned_cases_df)

    print(matched_cases_df.count())
    print(f"Python execution time: {time.time() - start_time}")

    incorrect_df = matched_cases_df.filter(
        matched_cases_df["Supplier Parent Name"]
        != matched_cases_df["ParentCompanyNameCleanse"]
    )

    assert incorrect_df.count() == 0


def test_performance_of_scala_module(spark, cleaned_cases_df):
    start_time = time.time()

    jvm = spark.sparkContext._jvm
    ssqlContext = SQLContext(spark.sparkContext)._ssql_ctx
    jdf = cleaned_cases_df._jdf
    parent_name_matcher = jvm.pl.odyzxc.myfunctions.ParentNameMatcher(ssqlContext, jdf, "Supplier Cleaned Name",
                                                                      "Supplier Parent Name", 1, "Supplier ID")
    matched_cases_df = DataFrame(parent_name_matcher.matchNames(), spark)

    print(matched_cases_df.count())
    print(f"Scala execution time: {time.time() - start_time}")

    incorrect_df = matched_cases_df.filter(
        matched_cases_df["Supplier Parent Name"]
        != matched_cases_df["ParentCompanyNameCleanse"]
    )

    assert incorrect_df.count() == 0


def test_performance_of_python_module_big_batch(spark, big_batch_df):
    start_time = time.time()

    matched_cases_df = match_parent_name(big_batch_df)

    print(matched_cases_df.count())
    print(f"Python big batch execution time: {time.time() - start_time}")


def test_performance_of_scala_module_big_batch(spark, big_batch_df):
    start_time = time.time()

    jvm = spark.sparkContext._jvm
    ssqlContext = SQLContext(spark.sparkContext)._ssql_ctx
    jdf = big_batch_df._jdf
    parent_name_matcher = jvm.pl.odyzxc.myfunctions.ParentNameMatcher(ssqlContext, jdf, "Supplier Cleaned Name",
                                                                      "Supplier Parent Name", 1, "Supplier ID")
    matched_cases_df = DataFrame(parent_name_matcher.matchNames(), spark)

    print(matched_cases_df.count())
    print(f"Scala big batch execution time: {time.time() - start_time}")
