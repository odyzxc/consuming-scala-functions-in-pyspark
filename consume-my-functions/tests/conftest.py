import shutil
import tempfile

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark(request):
    TEMP_DIR = tempfile.mkdtemp()
    spark = SparkSession.builder\
        .config('spark.jars', '../my-functions-assembly-0.0.1.jar') \
        .getOrCreate()
    spark.sparkContext.setCheckpointDir(TEMP_DIR)

    def teardown():
        shutil.rmtree(TEMP_DIR)

    request.addfinalizer(teardown)
    return spark
