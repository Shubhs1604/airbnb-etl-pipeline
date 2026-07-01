import dlt
from pyspark.sql import functions as F
from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Bronze layer: Ingest raw sales data from existing table
@dlt.table(
    name="bronze_airbnb_sales",
    comment="Clean sales data from table",
    table_properties={"quality": "bronze"}
)
def bronze_airbnb_sales():
    return spark.readStream.table("retail.airbnb_clean_data.airbnb_clean")

# Gold layer: Business analytics and aggregations

# 1. Overall Performance Summary
@dp.materialized_view(
    name="gold_airbnb_total_summary",
    comment="Overall business performance metrics",
    table_properties={"quality": "gold"}
)
def gold_airbnb_total_summary():
    df = spark.read.table("bronze_airbnb_sales")
    return df.agg(
        F.count("booking_id").alias("total_bookings"),
        F.sum("total_revenue").alias("total_revenue"),
        F.sum("booking_amount").alias("total_booking_amount"),
        F.sum("cleaning_fee").alias("total_cleaning_fees"),
        F.sum("service_fee").alias("total_service_fees"),
        F.sum("nights_booked").alias("total_nights_booked"),
        F.avg("average_nightly_rate").alias("avg_nightly_rate"),
        F.countDistinct("listing_id").alias("unique_listings")
    )


# 2. Monthly Revenue Trends
@dp.materialized_view(
    name="gold_monthly_revenue_trends",
    comment="Monthly aggregated revenue and booking metrics",
    table_properties={"quality": "gold"}
)
def gold_monthly_revenue_trends():
    df = spark.read.table("bronze_airbnb_sales")
    return df.groupBy("booking_year", "booking_month").agg(
        F.count("booking_id").alias("total_bookings"),
        F.sum("total_revenue").alias("monthly_revenue"),
        F.sum("nights_booked").alias("total_nights"),
        F.avg("average_nightly_rate").alias("avg_nightly_rate"),
        F.countDistinct("listing_id").alias("active_listings")
    ).orderBy("booking_year", "booking_month")


# 3. Listing Performance Analysis
@dp.materialized_view(
    name="gold_listing_performance",
    comment="Performance metrics by listing",
    table_properties={"quality": "gold"}
)
def gold_listing_performance():
    df = spark.read.table("bronze_airbnb_sales")
    return df.groupBy("listing_id").agg(
        F.count("booking_id").alias("total_bookings"),
        F.sum("total_revenue").alias("total_revenue"),
        F.sum("nights_booked").alias("total_nights_booked"),
        F.avg("average_nightly_rate").alias("avg_nightly_rate"),
        F.max("booking_date").alias("last_booking_date"),
        F.min("booking_date").alias("first_booking_date")
    ).orderBy(F.desc("total_revenue"))


# 4. Booking Status Analysis
@dp.materialized_view(
    name="gold_booking_status_analysis",
    comment="Breakdown by booking status (confirmed vs cancelled)",
    table_properties={"quality": "gold"}
)
def gold_booking_status_analysis():
    df = spark.read.table("bronze_airbnb_sales")
    return df.groupBy("booking_status").agg(
        F.count("booking_id").alias("booking_count"),
        F.sum("total_revenue").alias("total_revenue"),
        F.avg("total_revenue").alias("avg_revenue_per_booking"),
        F.sum("nights_booked").alias("total_nights")
    )


# 5. Day of Week Analysis
@dp.materialized_view(
    name="gold_day_of_week_analysis",
    comment="Booking patterns by day of week",
    table_properties={"quality": "gold"}
)
def gold_day_of_week_analysis():
    df = spark.read.table("bronze_airbnb_sales")
    return df.groupBy("booking_day_of_week").agg(
        F.count("booking_id").alias("total_bookings"),
        F.sum("total_revenue").alias("total_revenue"),
        F.avg("average_nightly_rate").alias("avg_nightly_rate"),
        F.avg("nights_booked").alias("avg_nights_per_booking")
    ).orderBy("booking_day_of_week")


# 6. Revenue Component Breakdown
@dp.materialized_view(
    name="gold_revenue_breakdown",
    comment="Breakdown of revenue components",
    table_properties={"quality": "gold"}
)
def gold_revenue_breakdown():
    df = spark.read.table("bronze_airbnb_sales")
    return df.agg(
        F.sum("booking_amount").alias("total_booking_amount"),
        F.sum("cleaning_fee").alias("total_cleaning_fees"),
        F.sum("service_fee").alias("total_service_fees"),
        F.sum("total_revenue").alias("total_revenue"),
        (F.sum("booking_amount") / F.sum("total_revenue") * 100).alias("booking_amount_pct"),
        (F.sum("cleaning_fee") / F.sum("total_revenue") * 100).alias("cleaning_fee_pct"),
        (F.sum("service_fee") / F.sum("total_revenue") * 100).alias("service_fee_pct")
    )


# 7. Top Performing Listings (Top 20)
@dp.materialized_view(
    name="gold_top_listings",
    comment="Top 20 listings by revenue",
    table_properties={"quality": "gold"}
)
def gold_top_listings():
    df = spark.read.table("bronze_airbnb_sales")
    
    window_spec = Window.orderBy(F.desc("total_revenue"))
    
    return df.groupBy("listing_id").agg(
        F.count("booking_id").alias("total_bookings"),
        F.sum("total_revenue").alias("total_revenue"),
        F.avg("average_nightly_rate").alias("avg_nightly_rate"),
        F.sum("nights_booked").alias("total_nights")
    ).withColumn("rank", F.row_number().over(window_spec)) \
     .filter(F.col("rank") <= 20) \
     .orderBy("rank")
