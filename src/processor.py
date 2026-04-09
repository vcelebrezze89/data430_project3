from pathlib import Path

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql.functions import (
    array_distinct,
    col,
    expr,
    length,
    monotonically_increasing_id,
    row_number,
    size,
    split,
    trim,
    when,
)

RAW_DATA_PATH = "data/raw/quotes_raw.json"
PROCESSED_QUOTES_PATH = "data/processed/quotes"
PROCESSED_AUTHORS_PATH = "data/processed/authors"
PROCESSED_TAGS_PATH = "data/processed/tags"
PROCESSED_QUOTE_TAGS_PATH = "data/processed/quote_tags"


def create_spark_session() -> SparkSession:
    """
    Create and return a Spark session.
    """
    spark = (
        SparkSession.builder
        .appName("QuotesPipeline")
        .master("local[*]")
        .config("spark.hadoop.fs.permissions.enabled", "false")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    return spark


def load_raw_data(spark: SparkSession, file_path: str = RAW_DATA_PATH) -> DataFrame:
    return spark.read.option("multiline", "true").json(file_path)


def clean_quotes(df: DataFrame) -> DataFrame:
    """
    Clean and standardize the quote data.
    """
    cleaned_df = (
        df.withColumn("quote_text", trim(col("quote_text")))
          .withColumn("author", trim(col("author")))
          .withColumn("tags", when(col("tags").isNull(), expr("array()")).otherwise(col("tags")))
          .dropDuplicates(["quote_text", "author"])
          .filter(col("quote_text").isNotNull() & (col("quote_text") != ""))
          .filter(col("author").isNotNull() & (col("author") != ""))
          .withColumn("quote_length", length(col("quote_text")))
          .withColumn("word_count", size(split(col("quote_text"), " ")))
          .withColumn("tags", array_distinct(col("tags")))
    )

    return cleaned_df


def create_quotes_table(df: DataFrame) -> DataFrame:
    """
    Create the quotes table with a unique quote ID.
    """
    quotes_df = df.select(
        "quote_text",
        "author",
        "page_number",
        "scraped_at",
        "quote_length",
        "word_count"
    ).distinct()

    window_spec = Window.orderBy(monotonically_increasing_id())

    quotes_final = quotes_df.withColumn(
        "quote_id",
        row_number().over(window_spec)
    )

    return quotes_final


def create_authors_table(df: DataFrame) -> DataFrame:
    """
    Create a unique authors table with author IDs.
    """
    authors_df = df.select("author").distinct()

    window_spec = Window.orderBy("author")

    authors_final = authors_df.withColumn(
        "author_id",
        row_number().over(window_spec)
    )

    return authors_final


def create_tags_table(df: DataFrame) -> DataFrame:
    """
    Create a unique tags table with tag IDs.
    """
    tags_df = df.selectExpr("explode(tags) as tag").distinct()

    window_spec = Window.orderBy("tag")

    tags_final = tags_df.withColumn(
        "tag_id",
        row_number().over(window_spec)
    )

    return tags_final


def create_quote_tags_table(clean_df: DataFrame, quotes_df: DataFrame, tags_df: DataFrame) -> DataFrame:
    """
    Create a bridge table connecting quotes and tags.
    """
    exploded_df = clean_df.select("quote_text", "author", "tags").selectExpr(
        "quote_text",
        "author",
        "explode(tags) as tag"
    )

    quote_tags_df = (
        exploded_df.join(
            quotes_df.select("quote_id", "quote_text", "author"),
            on=["quote_text", "author"],
            how="inner"
        )
        .join(
            tags_df,
            on="tag",
            how="inner"
        )
        .select("quote_id", "tag_id")
        .distinct()
    )

    return quote_tags_df


def save_dataframe(df: DataFrame, output_path: str) -> None:
    """
    Save a Spark DataFrame as a single CSV file using pandas.
    This avoids the Windows/Hadoop write issue.
    """
    output_dir = Path(output_path)
    output_dir.mkdir(parents=True, exist_ok=True)

    output_file = output_dir / "data.csv"

    pandas_df = df.toPandas()
    pandas_df.to_csv(output_file, index=False)

    print(f"Saved: {output_file}")


def main() -> None:
    """
    Run the PySpark processing pipeline.
    """
    spark = create_spark_session()

    raw_df = load_raw_data(spark)
    clean_df = clean_quotes(raw_df)

    quotes_df = create_quotes_table(clean_df)
    authors_df = create_authors_table(clean_df)
    tags_df = create_tags_table(clean_df)
    quote_tags_df = create_quote_tags_table(clean_df, quotes_df, tags_df)

    print("\nCleaned Quotes Preview:")
    clean_df.show(5, truncate=False)

    print("\nQuotes Table Preview:")
    quotes_df.show(5, truncate=False)

    print("\nAuthors Table Preview:")
    authors_df.show(5, truncate=False)

    print("\nTags Table Preview:")
    tags_df.show(5, truncate=False)

    print("\nQuote-Tags Table Preview:")
    quote_tags_df.show(5, truncate=False)

    save_dataframe(quotes_df, PROCESSED_QUOTES_PATH)
    save_dataframe(authors_df, PROCESSED_AUTHORS_PATH)
    save_dataframe(tags_df, PROCESSED_TAGS_PATH)
    save_dataframe(quote_tags_df, PROCESSED_QUOTE_TAGS_PATH)

    spark.stop()


if __name__ == "__main__":
    main()
    