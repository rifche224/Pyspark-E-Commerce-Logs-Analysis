"""
Revenue Analysis for E-commerce Data
This script calculates various revenue metrics from transaction data.
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, col, count, avg, when, date_format, to_date

def calculate_revenue(spark, transactions_path):
    """
    Calculate total revenue and related metrics from transaction data
    
    Args:
        spark: SparkSession object
        transactions_path: Path to the transactions CSV file
        
    Returns:
        Dictionary containing revenue metrics
    """
    # Load transactions data
    transactions_df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(transactions_path)
    
    # Display schema and sample data
    print("Transactions Schema:")
    transactions_df.printSchema()
    print("\nSample Transactions Data:")
    transactions_df.show(5)
    
    # Calculate total revenue (only from completed transactions)
    total_revenue = transactions_df.filter(col("status") == "completed") \
                                  .agg(sum("amount").alias("total_revenue")) \
                                  .collect()[0]["total_revenue"]
    
    # Calculate revenue by payment method
    revenue_by_payment = transactions_df.filter(col("status") == "completed") \
                                       .groupBy("payment_method") \
                                       .agg(sum("amount").alias("revenue")) \
                                       .orderBy(col("revenue").desc())
    
    # Calculate revenue by day
    daily_revenue = transactions_df.filter(col("status") == "completed") \
                                  .withColumn("date", to_date("timestamp")) \
                                  .groupBy("date") \
                                  .agg(sum("amount").alias("daily_revenue")) \
                                  .orderBy("date")
    
    # Calculate average transaction value
    avg_transaction_value = transactions_df.filter(col("status") == "completed") \
                                          .agg(avg("amount").alias("avg_transaction_value")) \
                                          .collect()[0]["avg_transaction_value"]
    
    # Calculate transaction count by status
    transaction_status_counts = transactions_df.groupBy("status") \
                                              .agg(count("*").alias("count"), 
                                                   sum("amount").alias("total_amount")) \
                                              .orderBy(col("count").desc())
    
    # Return results
    return {
        "total_revenue": total_revenue,
        "revenue_by_payment": revenue_by_payment,
        "daily_revenue": daily_revenue,
        "avg_transaction_value": avg_transaction_value,
        "transaction_status_counts": transaction_status_counts
    }

if __name__ == "__main__":
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("E-Commerce Revenue Analysis") \
        .getOrCreate()
    
    # Path to transactions data
    transactions_path = "src/data_generation/data/raw/transactions.csv"
    
    # Calculate revenue metrics
    revenue_metrics = calculate_revenue(spark, transactions_path)
    
    # Display results
    print("\n=== REVENUE ANALYSIS ===")
    print(f"Total Revenue: ${revenue_metrics['total_revenue']:,.2f}")
    print("\nRevenue by Payment Method:")
    revenue_metrics['revenue_by_payment'].show()
    
    print("\nDaily Revenue:")
    revenue_metrics['daily_revenue'].show(10)  # Show first 10 days
    
    print(f"\nAverage Transaction Value: ${revenue_metrics['avg_transaction_value']:,.2f}")
    
    print("\nTransaction Status Counts:")
    revenue_metrics['transaction_status_counts'].show()
    
    # Stop Spark session
    spark.stop()
