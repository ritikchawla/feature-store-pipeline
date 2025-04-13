from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import os
from datetime import datetime, timedelta

def create_spark_session():
    """Initialize Spark session with required configurations"""
    return SparkSession.builder \
        .appName("FeatureTransformations") \
        .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
        .getOrCreate()

def load_transaction_data(spark, data_path):
    """Load raw transaction data"""
    return spark.read.parquet(data_path)

def compute_customer_features(transactions_df):
    """
    Compute customer-level features from transaction data
    
    Features:
    - total_spend_30d: Total spending in last 30 days
    - avg_transaction_value_30d: Average transaction value in last 30 days
    - transaction_count_30d: Number of transactions in last 30 days
    - days_since_last_transaction: Days since customer's last transaction
    """
    # Create window specs for different time periods
    days_30 = Window.partitionBy("customer_id") \
        .orderBy("transaction_timestamp") \
        .rangeBetween(-timedelta(days=30).total_seconds(), 0)

    return transactions_df \
        .withColumn("total_spend_30d", 
                   F.sum("transaction_amount").over(days_30)) \
        .withColumn("transaction_count_30d", 
                   F.count("transaction_id").over(days_30)) \
        .withColumn("avg_transaction_value_30d",
                   F.col("total_spend_30d") / F.col("transaction_count_30d")) \
        .withColumn("days_since_last_transaction",
                   F.datediff(F.current_timestamp(),
                            F.max("transaction_timestamp").over(Window.partitionBy("customer_id"))))

def validate_features(features_df):
    """Perform basic validation on computed features"""
    # Check for nulls
    null_counts = features_df.select([
        F.count(F.when(F.col(c).isNull(), c)).alias(c)
        for c in features_df.columns
    ]).collect()[0]

    # Check for negative values in numeric columns
    numeric_columns = ["total_spend_30d", "avg_transaction_value_30d", 
                      "transaction_count_30d", "days_since_last_transaction"]
    negative_counts = features_df.select([
        F.count(F.when(F.col(c) < 0, c)).alias(c)
        for c in numeric_columns
    ]).collect()[0]

    return {
        "null_counts": null_counts.asDict(),
        "negative_counts": negative_counts.asDict()
    }

def save_features(features_df, output_path):
    """Save computed features as parquet files"""
    features_df.write \
        .mode("overwrite") \
        .partitionBy("dt") \
        .parquet(output_path)

def main():
    """Main ETL script to compute customer features"""
    spark = create_spark_session()

    # Paths
    input_path = "data/raw/transactions"
    output_path = "data/processed/customer_features"

    try:
        # Load data
        transactions_df = load_transaction_data(spark, input_path)

        # Compute features
        features_df = compute_customer_features(transactions_df)

        # Add timestamp column for feature freshness
        features_df = features_df.withColumn("dt", F.current_date())

        # Validate features
        validation_results = validate_features(features_df)
        print("Validation Results:", validation_results)

        # Save features
        save_features(features_df, output_path)

        print("Feature computation completed successfully")

    except Exception as e:
        print(f"Error in feature computation: {str(e)}")
        raise

    finally:
        spark.stop()

if __name__ == "__main__":
    main()