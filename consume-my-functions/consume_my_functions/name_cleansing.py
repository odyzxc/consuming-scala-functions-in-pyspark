import pyspark.sql.functions as F
from cleanco import basename, prepare_default_terms
from pyspark.sql import DataFrame
from pyspark.sql.types import StringType
from pyspark.sql.window import Window

cleanco_unsupported_prefixes = r"(,\s*(uk|UK)$)|(Limited Partnership$)|(Limited Liability Partnership$)|(ltd\.$)|(limited\.$)|([,-]?[\s]*The$)|(^The\s)|([,.]?Limited[.]?$)|(.Ltd$)|(Co.,Ltd.$)"  # noqa:E501

protection_char_sequence = "@a"

protected_cleanco_tokens = ["pc"]


def cleanse_names(
    dataframe: DataFrame,
    column_name: str = "Supplier Name",
    target_clean_column_name: str = "Supplier Cleaned Name",
):
    """
    Cleaning values (removing ltd, limited, 'the' etc.) from given
    column_name (default "Supplier Name") and storing those
    in target_clean_column_name (default "Supplier Cleaned Name")
    :param dataframe: input dataframe
    :param column_name: column name which should be cleaned
    :param target_clean_column_name: column name to be added with cleaned name
    :return: result dataframe
    """
    cleaned_df = dataframe.withColumn(
        target_clean_column_name,
        F.regexp_replace(column_name, cleanco_unsupported_prefixes, ""),
    )

    cleanco_terms = prepare_default_terms()
    terms_to_use = [
        (_, x)
        for (_, x) in cleanco_terms
        if not any(elem in x for elem in protected_cleanco_tokens)
    ]

    # adding a char sequence which most probably won't occur in company name
    # in order to protect parentheses from being removed by cleanco
    # removing char sequence previously added in order to restore initial name
    udf_cleanco = F.udf(
        lambda name: basename(
            name.replace(")", f"){protection_char_sequence}"), terms=terms_to_use
        ).replace(f"){protection_char_sequence}", ")"),
        StringType(),
    )

    cleaned_df = cleaned_df.withColumn(
        target_clean_column_name, udf_cleanco(F.col(target_clean_column_name))
    )
    return cleaned_df


def rename_duplicate_columns(dataframe: DataFrame):
    """
    Renaming duplicate columns to {column}_2
    :param dataframe:
    :return:
    """
    columns = dataframe.columns
    duplicate_column_indices = list(
        set([columns.index(col) for col in columns if columns.count(col) == 2])
    )
    for index in duplicate_column_indices:
        columns[index] = columns[index] + "_2"
    dataframe = dataframe.toDF(*columns)
    return dataframe


def match_parent_name(
    dataframe: DataFrame,
    column_name: str = "Supplier Cleaned Name",
    target_clean_column_name: str = "Supplier Parent Name",
    distance_threshold: int = 1,
    unique_id: str = "Supplier ID",
):
    """
    Matching possible duplicate names from given
    column_name (default "Supplier Cleaned Name") based on levenshtein distance
    :param dataframe:
    :param column_name:
    :param target_clean_column_name:
    :param distance_threshold:
    :param unique_id:
    :return:
    """
    dataframe = dataframe.repartition(100)
    joined = dataframe.select([unique_id, column_name]).crossJoin(
        dataframe.select([unique_id, column_name])
    )

    joined = joined.checkpoint(eager=True)

    joined = rename_duplicate_columns(joined).withColumnRenamed(
        f"{unique_id}_2", "group_no"
    )
    joined = joined.filter(
        F.abs(F.length(f"{column_name}_2") - F.length(column_name))
        <= distance_threshold
    )

    joined = joined.withColumn(
        "distance",
        F.levenshtein(F.lower(F.col(column_name)), F.lower(F.col(f"{column_name}_2"))),
    )
    joined = joined.checkpoint(eager=True)

    joined = joined.filter(joined["distance"] <= distance_threshold)

    window = Window.partitionBy("group_no")
    joined = joined.select(
        "group_no",
        unique_id,
        "distance",
        column_name,
        f"{column_name}_2",
        F.count("group_no").over(window).alias("count"),
    ).sort("group_no", "distance")

    window = Window.partitionBy(unique_id).orderBy(F.col("count").desc())
    joined = (
        joined.withColumn("row", F.row_number().over(window))
        .filter(F.col("row") == 1)
        .drop("row")
    )

    # matched_df = joined.select([f"{column_name}_2", F.col(unique_id).alias("match_id")])
    # joined = joined.select(["group_no", unique_id, column_name]).join(
    #     matched_df, joined["group_no"] == matched_df["match_id"], how="left"
    # )

    joined = joined.select(unique_id, f"{column_name}_2")
    dataframe = dataframe.join(joined, unique_id, "inner")

    dataframe = dataframe.withColumn(
        target_clean_column_name,
        F.coalesce(dataframe[target_clean_column_name], dataframe[f"{column_name}_2"]),
    ).drop(f"{column_name}_2")
    return dataframe
