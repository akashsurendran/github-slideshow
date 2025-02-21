from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, expr

def read_metadata(spark, metadata_path):
    """Read metadata CSV from S3."""
    return spark.read.option("header", "true").csv(metadata_path)

def read_source_data(spark, src_path):
    """Read Parquet source data from S3."""
    return spark.read.parquet(src_path)

def apply_transformations(df, metadata_df, feature_group):
    """Dynamically apply transformations based on metadata."""
    
    group_meta = metadata_df.filter(col("feature_group") == feature_group).collect()
    
    for row in group_meta:
        agg_type = row["agg_type"]
        agg_func = row["agg_func"]
        base_column = row["base_column"]
        group_by_cols = row["group_by_columns"].split(",") if row["group_by_columns"] else []
        partition_column = row["partition_column"]
        window_size = row["window_size_in_days"]
        round_decimal = row["round_decimal_point"]
        window_offset = row["window_offset"]
        filter_condition = row["filter"]

        if filter_condition and filter_condition.lower() != "null":
            df = df.filter(expr(filter_condition))

        if agg_type == "base_aggregation":
            df = df.transform(base_aggregation, agg_func=agg_func, cols_to_agg=[base_column], group_by_col=group_by_cols)
        elif agg_type == "rolling_aggregation":
            df = df.transform(rollingwindowagg, agg_func=agg_func, num_cols_agg=[base_column],
                              windowsize=window_size, partition_by=[partition_column],
                              round_decimal_point=round_decimal, window_offset=window_offset)
    return df

def main():
    spark = SparkSession.builder.appName("MetadataDrivenAggregation").getOrCreate()
    
    metadata_path = "s3://your-bucket/metadata.csv"  # Change this
    metadata_df = read_metadata(spark, metadata_path)
    
    feature_groups = metadata_df.select("feature_group").distinct().rdd.flatMap(lambda x: x).collect()
    
    for feature_group in feature_groups:
        src_path = metadata_df.filter(col("feature_group") == feature_group).select("src_path").first()[0]
        df = read_source_data(spark, src_path)
        df = apply_transformations(df, metadata_df, feature_group)
        target_path = metadata_df.filter(col("feature_group") == feature_group).select("target_path").first()[0]
        df.write.mode("overwrite").partitionBy("business_day").parquet(target_path)
    
    spark.stop()

if __name__ == "__main__":
    main()
