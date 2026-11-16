# -*- coding: utf-8 -*-
import argparse, csv, os, time
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

ap = argparse.ArgumentParser()
ap.add_argument("--sablon", required=True, choices=["log", "satis", "iot", "haber"])
a = ap.parse_args()

spark = (SparkSession.builder
         .appName(f"toplu-{a.sablon}")
         .config("spark.sql.shuffle.partitions", "8")
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")

os.makedirs("/kit/kanit", exist_ok=True)
olcumler = []

def ekle_olcum(kategori, metrik, deger, birim, aciklama=""):
    olcumler.append([kategori, metrik, deger, birim, aciklama])
    print(f"[ÖLÇÜM] {kategori} | {metrik}: {deger} {birim} ({aciklama})")

def s3_boyut_ve_sayi(yol):
    jvm = spark._jvm
    conf = spark._jsc.hadoopConfiguration()
    p = jvm.org.apache.hadoop.fs.Path(yol)
    fs = p.getFileSystem(conf)
    if not fs.exists(p):
        return 0, 0.0
    it = fs.listFiles(p, True)
    toplam_bayt = 0
    dosya_sayisi = 0
    while it.hasNext():
        f = it.next()
        if not f.getPath().getName().startswith("_"):
            toplam_bayt += f.getLen()
            dosya_sayisi += 1
    return dosya_sayisi, round(toplam_bayt / (1024 * 1024), 2)

HAM = "s3a://ham/satis/"
PARQUET = "s3a://islenmis/satis/"
TABLO = "s3a://tablolar/satis"
KARANTINA = "s3a://tablolar/satis_karantina"

# 1. Elle Şema Vererek Ham Veriyi Oku (Z2 - Schema-on-read optimizasyonu)
sema = StructType([
    StructField("olay_zamani", StringType(), True),
    StructField("siparis_id", StringType(), True),
    StructField("magaza_id", IntegerType(), True),
    StructField("urun_id", IntegerType(), True),
    StructField("adet", IntegerType(), True),
    StructField("birim_fiyat", DoubleType(), True),
    StructField("tur", StringType(), True),
])

t0 = time.time()
ham = spark.read.schema(sema).json(HAM)
ham = ham.withColumn("olay_ts", F.to_timestamp("olay_zamani")).withColumn("tarih", F.to_date("olay_ts"))
ham.cache()
toplam_ham = ham.count()
json_okuma_sn = round(time.time() - t0, 2)
ekle_olcum("Z2_Format", "JSON_Satir_Sayisi", toplam_ham, "satir", "Ham veri toplam kayit")
ekle_olcum("Z2_Format", "JSON_Okuma_Suresi", json_okuma_sn, "saniye", "Elle sema ile JSON count")

# 2. Kirli Veri Analizi ve Temizleme (Z10)
eksik_null = ham.filter(
    F.col("olay_ts").isNull() | F.col("siparis_id").isNull() |
    F.col("urun_id").isNull() | F.col("magaza_id").isNull() |
    (F.col("adet") <= 0) | (F.col("birim_fiyat") <= 0)
)
kirli_sayisi = eksik_null.count()
eksik_null.write.format("delta").mode("overwrite").save(KARANTINA)

gecerli = ham.filter(
    F.col("olay_ts").isNotNull() & F.col("siparis_id").isNotNull() &
    F.col("urun_id").isNotNull() & F.col("magaza_id").isNotNull() &
    (F.col("adet") > 0) & (F.col("birim_fiyat") > 0)
)
tekrar_sayisi = gecerli.count() - gecerli.dropDuplicates(["siparis_id"]).count()
temiz = gecerli.dropDuplicates(["siparis_id"]).cache()
temiz_sayisi = temiz.count()

ekle_olcum("Z10_Kalite", "Kirli_Eksik_Kayit", kirli_sayisi, "adet", "Null veya <=0 degerli kayitlar")
ekle_olcum("Z10_Kalite", "Tekrar_Eden_Kayit", tekrar_sayisi, "adet", "Ayni siparis_id tekrari")
ekle_olcum("Z10_Kalite", "Temiz_Kayit_Sayisi", temiz_sayisi, "adet", "Analize giren net kayit")

# 3. Sıkıştırma Kodekleri ve Boyut Karşılaştırması (Z2, Z4)
json_dosya, json_mb = s3_boyut_ve_sayi(HAM)
ekle_olcum("Z2_Format", "Ham_JSON_Boyut", json_mb, "MB", f"{json_dosya} adet JSONL dosyasi")

for kodek in ["snappy", "zstd", "gzip"]:
    hedef = f"s3a://islenmis/test_{kodek}/"
    t0 = time.time()
    temiz.write.mode("overwrite").option("compression", kodek).parquet(hedef)
    yazma_sn = round(time.time() - t0, 2)
    
    t0 = time.time()
    _ = spark.read.parquet(hedef).agg(F.sum("adet")).collect()
    okuma_sn = round(time.time() - t0, 2)
    
    d_sayi, d_mb = s3_boyut_ve_sayi(hedef)
    ekle_olcum("Z2_Kodek", f"Parquet_{kodek}_Boyut", d_mb, "MB", f"{d_sayi} dosya")
    ekle_olcum("Z2_Kodek", f"Parquet_{kodek}_Yazma", yazma_sn, "saniye", "3M satir yazma")
    ekle_olcum("Z2_Kodek", f"Parquet_{kodek}_Okuma", okuma_sn, "saniye", "Sum(adet) sorgu suresi")

# Ana Parquet çıktısını tarih bölümlemeli (partitionBy) olarak yaz
temiz.write.mode("overwrite").partitionBy("tarih").option("compression", "snappy").parquet(PARQUET)
pq_dosya, pq_mb = s3_boyut_ve_sayi(PARQUET)
ekle_olcum("Z4_Depo", "Tarih_Bolumlu_Parquet_Dosya", pq_dosya, "adet", f"Toplam {pq_mb} MB, ort {round(pq_mb/max(pq_dosya,1),2)} MB/dosya")

# 4. Sıcak Nokta (Hot Spot) Analizi (Z3)
for anahtar in ["urun_id", "magaza_id"]:
    dagilim = temiz.groupBy(anahtar).count().orderBy(F.desc("count")).collect()
    en_buyuk = dagilim[0]["count"]
    pay = round(100 * en_buyuk / temiz_sayisi, 2)
    # 8 bölüme hash dağıtımı simülasyonu
    bolumler = temiz.withColumn("bolum", F.pmod(F.hash(F.col(anahtar)), F.lit(8))).groupBy("bolum").count().collect()
    sayilar = [r["count"] for r in bolumler]
    dengesizlik = round(max(sayilar) / (sum(sayilar) / len(sayilar)), 2)
    ekle_olcum("Z3_Bolumleme", f"{anahtar}_EnBuyukPay", pay, "yuzde", f"En yogun anahtar: {dagilim[0][anahtar]}")
    ekle_olcum("Z3_Bolumleme", f"{anahtar}_MaxBolOrt_8Bolum", dengesizlik, "kat", "8 bolumde en dolu / ortalama")

# 5. Delta Lake Yazma ve Zaman Yolculuğu (Z7)
temiz.write.format("delta").mode("overwrite").option("overwriteSchema", "true").partitionBy("tarih").save(TABLO)
# İkinci sürüm oluşturmak için örnek bir güncelleme/ekleme (sürüm 1)
temiz.limit(100).write.format("delta").mode("append").save(TABLO)
print("\n--- DELTA LAKE GEÇMİŞİ (DESCRIBE HISTORY) ---")
spark.sql(f"DESCRIBE HISTORY delta.`{TABLO}`").select("version", "timestamp", "operation").show(truncate=False)
v0_sayisi = spark.read.format("delta").option("versionAsOf", 0).load(TABLO).count()
v1_sayisi = spark.read.format("delta").option("versionAsOf", 1).load(TABLO).count()
ekle_olcum("Z7_Delta", "Surum_0_Satir", v0_sayisi, "satir", "Ilk yazma surumu")
ekle_olcum("Z7_Delta", "Surum_1_Satir", v1_sayisi, "satir", "Ikinci yazma (append) surumu")

# 6. Toplu Sorgular, Join Stratejisi Performans Ölçümü ve Explain (Z5)
tablo = spark.read.format("delta").option("versionAsOf", 0).load(TABLO)
urunler = spark.read.option("header", True).option("inferSchema", True).csv("s3a://ham/boyut/satis/urunler.csv")
magazalar = spark.read.option("header", True).option("inferSchema", True).csv("s3a://ham/boyut/satis/magazalar.csv")

# Performans Kararı: Broadcast Join KAPALI (SortMergeJoin) vs AÇIK (BroadcastHashJoin) - 3'er tekrar
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
sureler_smj = []
for _ in range(3):
    t0 = time.time()
    s1 = (tablo.join(urunler, "urun_id")
          .groupBy("kategori")
          .agg(
              F.round(F.sum(F.when(F.col("tur") == "satis", F.col("adet") * F.col("birim_fiyat")).otherwise(0)), 2).alias("toplam_ciro"),
              F.sum(F.when(F.col("tur") == "satis", F.col("adet")).otherwise(0)).alias("satis_adedi"),
              F.sum(F.when(F.col("tur") == "iade", F.col("adet")).otherwise(0)).alias("iade_adedi")
          )
          .withColumn("iade_orani_yuzde", F.round(100 * F.col("iade_adedi") / F.col("satis_adedi"), 2))
          .orderBy(F.desc("toplam_ciro")))
    s1.collect()
    sureler_smj.append(time.time() - t0)

spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "10485760") # 10 MB Broadcast açık
sureler_bhj = []
for _ in range(3):
    t0 = time.time()
    s1_opt = (tablo.join(F.broadcast(urunler), "urun_id")
              .groupBy("kategori")
              .agg(
                  F.round(F.sum(F.when(F.col("tur") == "satis", F.col("adet") * F.col("birim_fiyat")).otherwise(0)), 2).alias("toplam_ciro"),
                  F.sum(F.when(F.col("tur") == "satis", F.col("adet")).otherwise(0)).alias("satis_adedi"),
                  F.sum(F.when(F.col("tur") == "iade", F.col("adet")).otherwise(0)).alias("iade_adedi")
              )
              .withColumn("iade_orani_yuzde", F.round(100 * F.col("iade_adedi") / F.col("satis_adedi"), 2))
              .orderBy(F.desc("toplam_ciro")))
    s1_opt.collect()
    sureler_bhj.append(time.time() - t0)

ort_smj = round(sum(sureler_smj) / 3, 3)
ort_bhj = round(sum(sureler_bhj) / 3, 3)
ekle_olcum("Z5_Performans", "SortMergeJoin_Ort_3Tekrar", ort_smj, "saniye", "Broadcast kapali join")
ekle_olcum("Z5_Performans", "BroadcastJoin_Ort_3Tekrar", ort_bhj, "saniye", "Broadcast acik join")

# Explain çıktısını kanit/explain-1.txt dosyasına kaydet
def df_to_csv(df, dosya_yolu):
    satirlar = df.collect()
    with open(dosya_yolu, "w", newline="", encoding="utf-8") as f:
        yazici = csv.writer(f)
        yazici.writerow(df.columns)
        for r in satirlar:
            yazici.writerow([r[c] for c in df.columns])

# Explain çıktısını kanit/explain-1.txt dosyasına kaydet
explain_metni = s1_opt._jdf.queryExecution().simpleString()
with open("/kit/kanit/explain-1.txt", "w", encoding="utf-8") as f:
    f.write(explain_metni)
print("\n--- TOPLU SORGU 1: KATEGORİ BAZLI CİRO VE İADE ORANI ---")
s1_opt.show(truncate=False)
df_to_csv(s1_opt, "/kit/kanit/sonuc_kategori.csv")

# Toplu Sorgu 2: Şehir ve Mağaza Bazlı Günlük Satış Performansı
sehir_kol = [c for c in magazalar.columns if "ehir" in c][0]
s2 = (tablo.filter(F.col("tur") == "satis")
      .join(F.broadcast(magazalar), "magaza_id")
      .groupBy(sehir_kol, "tarih")
      .agg(
          F.round(F.sum(F.col("adet") * F.col("birim_fiyat")), 2).alias("gunluk_ciro"),
          F.count("siparis_id").alias("islem_sayisi")
      )
      .orderBy("tarih", F.desc("gunluk_ciro")))
print("\n--- TOPLU SORGU 2: ŞEHİR VE GÜNLÜK SATIŞ PERFORMANSI ---")
s2.show(15, truncate=False)
df_to_csv(s2, "/kit/kanit/sonuc_sehir_gunluk.csv")

# Tüm ölçümleri kanit/olcumler.csv dosyasına yaz
with open("/kit/kanit/olcumler.csv", "w", newline="", encoding="utf-8") as f:
    yazici = csv.writer(f)
    yazici.writerow(["Kategori", "Metrik", "Deger", "Birim", "Aciklama"])
    yazici.writerows(olcumler)

print("\n[TAMAMLANDI] Tum olcumler kanit/olcumler.csv ve kanit/explain-1.txt dosyalarina kaydedildi.")
print("Spark UI ekran goruntusu almak icin 15 saniye bekleniyor (http://localhost:4040)...")
time.sleep(15)
spark.stop()