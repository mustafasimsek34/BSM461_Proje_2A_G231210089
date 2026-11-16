# -*- coding: utf-8 -*-
import argparse
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, sum, count, round
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType

sys.path.insert(0, "/kit/araclar")
from ortak import SABLONLAR

ap = argparse.ArgumentParser()
ap.add_argument("--sablon", required=True, choices=["log", "satis", "iot", "haber"])
a = ap.parse_args()

spark = (SparkSession.builder
         .appName(f"akis-{a.sablon}")
         .config("spark.sql.shuffle.partitions", "4")
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")

# Gelen Kafka mesajları için şema
sema = StructType([
    StructField("olay_zamani", TimestampType(), True),
    StructField("siparis_id", StringType(), True),
    StructField("magaza_id", IntegerType(), True),
    StructField("urun_id", IntegerType(), True),
    StructField("adet", IntegerType(), True),
    StructField("birim_fiyat", DoubleType(), True),
    StructField("tur", StringType(), True),
])

# Kafka'dan okuma (Z6)
df = (spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "kafka:29092")
      .option("subscribe", SABLONLAR[a.sablon]["konu"])
      .option("startingOffsets", "latest")
      .load())

# JSON veriyi ayrıştırma
parsed_df = df.selectExpr("CAST(value AS STRING)").select(from_json(col("value"), sema).alias("data")).select("data.*")

# Watermark ve Zaman Çerçevesi (Z8)
# 10 dakikalık watermark, 5 dakikalık sliding window (1 dakika kayma)
aggregated_df = (parsed_df
                 .withWatermark("olay_zamani", "1 minute")
                 .groupBy(
                     window(col("olay_zamani"), "5 minutes", "1 minute"),
                     col("tur")
                 )
                 .agg(
                     round(sum(col("adet") * col("birim_fiyat")), 2).alias("toplam_tutar"),
                     count("siparis_id").alias("islem_sayisi")
                 ))

# Konsola yazdırma (Append modu, truncate=False ile geniş çıktı)
query = (aggregated_df.writeStream
         .outputMode("append")
         .format("console")
         .option("truncate", "false")
         .trigger(processingTime="10 seconds")
         .start())

query.awaitTermination()