package pl.odyzxc.myfunctions

import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import org.scalatest.FunSpec

class ParentNameMatcherSpec
  extends FunSpec
    with SparkSessionTestWrapper
    with DataFrameComparer {

  import spark.implicits._

  describe(".matchNames") {

    spark.sparkContext.setCheckpointDir("./temp")
    it("happy path") {
      val sourceDF = Seq(
        (1, "test1", null),
        (2, "test2", "test2 override"),
        (3, "test3", null),
        (4, "some other", null),
        (5, "some others", null)
      ).toDF("Supplier ID", "Supplier Cleaned Name", "Supplier Parent Name")

      val matcher = new ParentNameMatcher(spark.sqlContext, sourceDF)
      val resultDF = matcher.matchNames()

      val expectedDF = Seq(
        (1, "test1", "test1"),
        (2, "test2", "test2 override"),
        (3, "test3", "test1"),
        (4, "some other", "some others"),
        (5, "some others", "some others"),
      ).toDF("Supplier ID", "Supplier Cleaned Name", "Supplier Parent Name")

      assertSmallDataFrameEquality(resultDF, expectedDF, ignoreNullable = true)

    }

    it("should not affect other columns") {
      val sourceDF = Seq(
        (1, "test1", null, "value do not touch"),
        (2, "test2", "test2 override", "value do not touch"),
        (3, "test3", null, "value do not touch")
      ).toDF("Supplier ID", "Supplier Cleaned Name", "Supplier Parent Name", "Some other column")

      val matcher = new ParentNameMatcher(spark.sqlContext, sourceDF)
      val resultDF = matcher.matchNames()

      val expectedDF = Seq(
        (1, "test1", "value do not touch", "test1"),
        (2, "test2", "value do not touch", "test2 override"),
        (3, "test3", "value do not touch", "test1")
      ).toDF("Supplier ID", "Supplier Cleaned Name", "Some other column", "Supplier Parent Name")

      assertSmallDataFrameEquality(resultDF, expectedDF, ignoreNullable = true)

    }

    it("performance tests") {
      val sourceDF = spark.read.option("header", value = true)
        .option("inferSchema", value = true).csv("src/test/resources/test_cases.csv")

      val start = System.nanoTime()

      val matcher = new ParentNameMatcher(spark.sqlContext, sourceDF, "ParentCompanyNameCleanse")
      val resultDF = matcher.matchNames()

      val end = System.nanoTime()
      println("Elapsed time: " + (end - start) / 1000000000 + "s")

      println(resultDF.show(20, false))

    }


  }

}
