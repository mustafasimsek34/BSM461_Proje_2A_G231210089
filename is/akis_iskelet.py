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

# Şehir boyutunu ekle ve günlük satış sorgusunu oluştur (Z5, Z8)
magazalar = (spark.read
             .option("header", True)
             .option("inferSchema", True)
             .csv("s3a://ham/boyut/satis/magazalar.csv"))

aggregated_df = (parsed_df
                 .withWatermark("olay_zamani", "1 minute")
                 .filter(col("tur") == "satis")
                 .join(magazalar, "magaza_id")
                 .groupBy(
                     col("sehir"),
                     window(col("olay_zamani"), "1 day").alias("gun")
                 )
                 .agg(
                     round(sum(col("adet") * col("birim_fiyat")), 2).alias("gunluk_ciro"),
                     count("siparis_id").alias("islem_sayisi")
                 )
                 .select(
                     col("sehir"),
                     col("gun.start").alias("tarih"),
                     col("gunluk_ciro"),
                     col("islem_sayisi")
                 ))

# Konsola şehir/günlük sorgu sonucunu yazdır
query = (aggregated_df.writeStream
         .outputMode("update")
         .format("console")
         .option("truncate", "false")
         .trigger(processingTime="10 seconds")
         .start())

query.awaitTermination()