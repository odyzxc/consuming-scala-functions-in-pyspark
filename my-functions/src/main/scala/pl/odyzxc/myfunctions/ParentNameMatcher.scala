package pl.odyzxc.myfunctions

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SQLContext}

class ParentNameMatcher(sqlContext: SQLContext, dataframe: DataFrame, columnName: String = "Supplier Cleaned Name",
                        targetColumnName: String = "Supplier Parent Name",
                        distanceThreshold: Int = 1,
                        uniqueId: String = "Supplier ID") {

  def matchNames(): DataFrame = {
    import sqlContext.implicits._

    var joined = dataframe.repartition(100)
      .select(col(uniqueId), col(columnName)).as("left")
      .crossJoin(
        dataframe.select(col(uniqueId), col(columnName)).as("right")
      )

    joined = joined.checkpoint(eager=true)

    joined = joined.filter(
      abs(length(col(s"left.${columnName}")) - length(col(s"right.${columnName}")))
        <= distanceThreshold
    )

    joined = joined.withColumn(
        "distance",
        levenshtein(lower(col(s"left.${columnName}")),
          lower(col(s"right.${columnName}")))
      )
    joined = joined.checkpoint(eager=true)

    joined = joined.filter(joined("distance") <= distanceThreshold)

    joined = joined.withColumn("count", count(s"right.${uniqueId}").over(Window.partitionBy(s"right.${uniqueId}")))

    joined.select(
      s"right.${uniqueId}",
      s"left.${uniqueId}",
      "distance",
      s"left.${columnName}",
      s"right.${columnName}",
      "count",
    ).sort(s"right.${uniqueId}", "distance")

    joined = joined.withColumn("row", row_number().over(Window.partitionBy(s"left.${uniqueId}").orderBy(col("count").desc)))

    joined = joined.filter(joined("row") === 1)
      .drop("row")

    val tempTargetColumnName = "temp"

    joined = joined.select(s"left.${uniqueId}", s"right.${columnName}")
    joined = joined.withColumn(tempTargetColumnName, joined(s"right.${columnName}"))

    val inputCols = dataframe.columns.filter(colName => colName != targetColumnName).map(dfColumn => col(s"input.${dfColumn}"))
    val selectCols = inputCols :+ col(targetColumnName)

    var result = dataframe.alias("input").join(joined.alias("joined"), uniqueId)

    if (dataframe.columns.contains(targetColumnName)) {
      result = result.withColumn(
        targetColumnName,
        coalesce(result(s"input.${targetColumnName}"), result(s"joined.${tempTargetColumnName}")),
      )
    } else {
      result = result.withColumn(
        targetColumnName,
        result(s"joined.${tempTargetColumnName}"),
      )
    }

    result = result.select(selectCols:_*).orderBy(uniqueId)

    result
  }
}