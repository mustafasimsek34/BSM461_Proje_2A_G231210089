# Satış Veri Hattı

Bu proje, mağaza zincirine ait satış olaylarının Kafka üzerinden alınmasını,
Apache Spark ile işlenmesini ve şehir/günlük satış sonuçlarına dönüştürülmesini
sağlayan Docker tabanlı bir veri hattıdır.

Proje iki farklı çalışma biçimi içerir:

- **Toplu işleme:** S3 üzerindeki geçmiş satış verilerini temizler, Parquet ve
  Delta tabloları oluşturur, kalite ve performans ölçümleri üretir.
- **Gerçek zamanlı işleme:** Kafka'dan gelen satış olaylarını Spark Structured
  Streaming ile okuyarak şehir ve gün bazında ciro/işlem sayısı hesaplar.

Bu sürümde kullanılan grup kodu `G231210089` değeridir.

## 1. Mimari

```text
Python veri üreticisi
        |
        +--> Kafka: satislar --------> Spark Structured Streaming
        |                                      |
        |                                      +--> şehir/günlük satış tablosu
        |
        +--> SeaweedFS S3 ------------> Spark toplu işleme
                                               |
                                               +--> Parquet
                                               +--> Delta Lake
                                               +--> kalite ve sorgu çıktıları
```

### Servisler

| Servis | Görevi | Adres |
|---|---|---|
| Kafka | Gerçek zamanlı olayları topic üzerinde tutar | Container içi `kafka:29092`, bilgisayar üzerinden `localhost:9092` |
| SeaweedFS S3 | Ham JSONL, boyut tabloları, Parquet ve Delta verilerini saklar | `http://localhost:8333` |
| Spark | Toplu ve gerçek zamanlı veri işleme | Spark UI: `http://localhost:4040` |
| `araclar` | Python veri üreticisi ve doğrulama komutlarını çalıştırır | Container içi çalışma ortamı |

Kafka topic'leri ve anahtarları [`araclar/ortak.py`](./araclar/ortak.py) içinde
tanımlıdır. Satış şablonunun topic'i `satislar`, Kafka anahtarı ise `urun_id`dir.

## 2. Gereksinimler

- Docker Desktop ve Docker Compose
- Windows'ta Docker Desktop'ın Linux container/WSL 2 backend'i
- En az 8 GB RAM; Docker'a en az 5 GB bellek ayrılması önerilir
- İlk kurulumda Docker imajlarını ve Spark kütüphanelerini indirebilmek için
  internet bağlantısı

Windows'ta bütün komutlar PowerShell'de, proje klasöründe çalıştırılabilir.

## 3. Kurulum ve servisleri başlatma

Proje klasöründe:

```powershell
docker compose up -d
docker compose ps
```

`kafka`, `s3`, `spark` ve `araclar` servislerinin çalıştığı görülmelidir.
Kafka ve S3 healthcheck sonuçları `healthy` olmalıdır.

Temel bağlantıları doğrulamak için:

```powershell
docker compose exec araclar python araclar/dogrula.py
```

Spark, Delta ve Kafka erişimini doğrulamak için:

```powershell
docker compose exec spark spark-submit is/dogrula_spark.py
```

İlk Spark çalıştırmasında gerekli kütüphaneler indirildiği için işlem birkaç dakika
uzayabilir. Sonraki çalıştırmalarda önbellek kullanılır.

## 4. Proje dosyaları

```text
docker-compose.yml       Docker servisleri ve volume tanımları
spark-defaults.conf      Spark'ın Kafka, S3 ve Delta ayarları
araclar/
  uretici.py             Kafka/S3 için satış olayı üreticisi
  senaryo.py             Grup koduna göre senaryo bilgilerini gösterir
  grup.py                Grup kodundan veri ayarlarını türetir
  ortak.py               Kafka topic, S3 ve servis adresleri
  dogrula.py             Kafka ve S3 bağlantı testleri
  kova_kur.py            S3 kovalarını oluşturur
is/
  toplu_is.py            Toplu veri işleme hattı
  akis_iskelet.py        Gerçek zamanlı satış akışı
  dogrula_spark.py       Spark doğrulama testi
kanit/                   Toplu işlem kanıt ve ölçüm dosyaları
```

## 5. Veri üreticisi

Üretici, satış olaylarını iki şekilde oluşturabilir:

1. Kafka'ya sürekli veya belirli süre boyunca olay gönderebilir.
2. Geçmiş günlere ait olayları JSONL olarak S3'e yazabilir.

Satış olaylarının alanları:

| Alan | Açıklama |
|---|---|
| `olay_zamani` | Event-time zaman damgası |
| `siparis_id` | Siparişi benzersiz olarak tanımlar |
| `magaza_id` | Mağaza boyut tablosundaki anahtar |
| `urun_id` | Ürün boyut tablosundaki anahtar |
| `adet` | Satılan ürün adedi |
| `birim_fiyat` | Bir ürünün satış fiyatı |
| `tur` | `satis`, `iade` veya `stok_girisi` |

Satış şablonunda ayrıca şu boyut dosyaları oluşturulur:

- `urunler.csv`: ürün kategorisi, marka ve maliyet bilgileri
- `magazalar.csv`: mağaza, şehir ve metrekare bilgileri
- `satis_baslangic_stok.csv`: başlangıç stok bilgileri

### Senaryo bilgilerini görüntüleme

```powershell
docker compose exec araclar python araclar/senaryo.py --grup G231210089 --sablon satis
```

Bu komut, grup kodundan türetilen hız, geç olay ve veri kusuru ayarlarını gösterir.

### Üretici seçenekleri

| Seçenek | İşlevi |
|---|---|
| `--grup G231210089` | Verinin grup koduna göre üretilmesini sağlar |
| `--sablon satis` | Veri şablonunu seçer (`log`, `satis`, `iot`, `haber`) |
| `--hiz 10` | Akışta saniyede yaklaşık 10 olay üretir |
| `--sure 60` | Akışı 60 saniye çalıştırır; verilmezse `Ctrl+C` ile durur |
| `--toplu 100000` | S3'e toplam 100.000 geçmiş olay yazar |
| `--gun 7` | Toplu verinin 7 günlük zaman aralığına yayılmasını sağlar |
| `--ekle` | Toplu üretimde mevcut ham verinin üzerine ekleme yapar |
| `--gec-orani 0.2` | Geç olay oranını yüzde 20 yapar |
| `--gec-en-fazla-dk 10` | Geç olay zamanını en fazla 10 dakika geriye çeker |
| `--tekrar-orani 0.01` | Tekrar üretilen olay oranını yüzde 1 yapar |
| `--kirli-orani 0.01` | Eksik veya geçersiz kayıt oranını yüzde 1 yapar |
| `--acks all` | Kafka broker onayı gelmeden gönderimi başarılı saymaz |
| `--idempotent-kapali` | Idempotent Kafka üretimini kapatır |

Tüm seçenekleri görmek için:

```powershell
docker compose exec araclar python araclar/uretici.py --help
```

## 6. Gerçek zamanlı satış akışı

Akışta iki ayrı PowerShell terminali kullanılır. Önce Spark tüketicisi başlatılır,
ardından Kafka üreticisi çalıştırılır.

### Terminal 2 — Spark akışı

```powershell
docker compose exec spark spark-submit is/akis_iskelet.py --sablon satis
```

[`is/akis_iskelet.py`](./is/akis_iskelet.py) şu adımları gerçekleştirir:

1. Kafka'daki `satislar` topic'ine `kafka:29092` adresiyle bağlanır.
2. Kafka mesajlarının JSON içeriğini satış şemasına dönüştürür.
3. `olay_zamani` alanına bir dakikalık watermark uygular.
4. `magazalar.csv` ile `magaza_id` değerini `sehir` bilgisine dönüştürür.
5. Yalnızca `tur == "satis"` olaylarını seçer.
6. Olayları şehir ve bir günlük event-time penceresinde gruplar.
7. `adet * birim_fiyat` toplamından günlük ciroyu hesaplar.
8. İşlem sayısını hesaplar ve sonucu konsola yazdırır.

Akış sorgusunun çıktısı:

```text
sehir | tarih | gunluk_ciro | islem_sayisi
```

Sorguda kullanılan temel ayarlar:

```python
.withWatermark("olay_zamani", "1 minute")
window(col("olay_zamani"), "1 day")
.writeStream.outputMode("update")
.trigger(processingTime="10 seconds")
```

`update` modu, gün tamamlanmadan değişen şehir toplamlarını gösterir. Bir mikro-batch
içinde yeni veya değişen grup yoksa boş tablo görülmesi normaldir. Watermark süresinden
daha geç gelen olaylar, ilgili pencere ilerledikten sonra sonuca dahil edilmeyebilir.

### Terminal 1 — Kafka üreticisi

```powershell
docker compose exec araclar python araclar/uretici.py --grup G231210089 --sablon satis --hiz 10
```

Bu komut Kafka'ya saniyede yaklaşık 10 satış olayı gönderir. Üreticiyi durdurmak için
`Ctrl+C` kullanılır. Belirli süreli bir test için:

```powershell
docker compose exec araclar python araclar/uretici.py --grup G231210089 --sablon satis --hiz 10 --sure 60
```

Bu komut yaklaşık 60 saniye sonra kendiliğinden kapanır.

### Gerçek zamanlı sonuçları izleme

Akış sonucu Spark UI'da değil, Spark komutunun çalıştığı Terminal 2'de görünür.
Spark UI'ı iş çalışırken `http://localhost:4040` adresinden açabilirsiniz. UI;
Jobs, Stages, SQL planı, süreler ve shuffle bilgilerini gösterir.

## 7. Toplu satış hattı

Önce S3'e geçmiş satış verisi yazılır:

```powershell
docker compose exec araclar python araclar/uretici.py --grup G231210089 --sablon satis --toplu 100000 --gun 7
```

Ardından Spark toplu işi çalıştırılır:

```powershell
docker compose exec spark spark-submit is/toplu_is.py --sablon satis
```

[`is/toplu_is.py`](./is/toplu_is.py) aşağıdaki işlemleri yapar:

1. Ham JSONL verisini elle tanımlanmış şema ile okur.
2. Eksik, null, sıfır veya negatif değerli kayıtları tespit eder.
3. Geçersiz kayıtları `s3a://tablolar/satis_karantina` Delta tablosuna yazar.
4. `siparis_id` alanına göre tekrar kayıtları temizler.
5. Snappy, Zstd ve Gzip Parquet sıkıştırmalarını karşılaştırır.
6. Temiz veriyi `tarih` alanına göre bölümlenmiş Snappy Parquet olarak yazar.
7. Ürün ve mağaza boyutlarıyla broadcast join uygular.
8. Ürün/mağaza anahtar dağılımındaki sıcak noktaları ölçer.
9. Temiz veriyi Delta tablosuna yazar ve iki sürüm için time travel testi yapar.
10. Kategori bazlı ve şehir/günlük satış sorgularını çalıştırır.

Toplu işin oluşturduğu kanıt dosyaları:

```text
kanit/olcumler.csv
kanit/explain-1.txt
kanit/sonuc_kategori.csv
kanit/sonuc_sehir_gunluk.csv
```

`sonuc_sehir_gunluk.csv` dosyasında şehir, tarih, günlük ciro ve işlem sayısı
bulunur. `olcumler.csv` içinde kayıt sayıları, kirli/tekrar kayıt ölçümleri,
Parquet boyutları, yazma-okuma süreleri, bölümleme ve join süreleri yer alır.

## 8. Veri kalitesi ve güvenilirlik

Üretici, gerçek veri ortamlarını taklit etmek için kontrollü kusurlar oluşturur:

| Kusur | Hattaki karşılığı |
|---|---|
| Geç olay | Akışta bir dakikalık watermark ile yönetilir |
| Tekrar kayıt | Toplu hatta `siparis_id` ile tekilleştirilir |
| Kirli kayıt | Geçersiz kayıtlar karantinaya ayrılır |
| Çarpık anahtar | Toplu işte anahtar yoğunluğu ve bölüm dengesizliği ölçülür |

Kafka üreticisi varsayılan olarak `acks=all` ve idempotent gönderim kullanır.
Bu ayarlar broker onayını ve yeniden denemelerde güvenilirliği artırır; ancak
tek broker kullanıldığı için sistem tam yüksek erişilebilirlik sağlamaz.

## 9. Kullanılan adresler

Bu projede tarayıcıdan açılması gereken tek arayüz Spark UI'dır:

```text
http://localhost:4040
```

`localhost:9092` Kafka'nın bilgisayardan erişim adresidir; bir web sitesi değildir.
`localhost:8333` SeaweedFS'in S3 API adresidir; veri hattı bu adresi arka planda
kullanır ve tarayıcıda web arayüzü sunmaz.

## 10. Durdurma ve veri temizleme

Container'ları durdurup volume verilerini korumak için:

```powershell
docker compose stop
```

Servisleri durdurup kaldırmak, fakat volume verilerini korumak için:

```powershell
docker compose down
```

Servisleri ve volume verilerini tamamen silmek için:

```powershell
docker compose down -v
```

`down -v` Kafka, S3 ve Spark önbellek verilerini siler.

## 11. Sık karşılaşılan sorunlar

| Hata veya belirti | Çözüm |
|---|---|
| `failed to connect to the docker API` | Docker Desktop'ı başlatın ve Linux engine tamamen hazır olana kadar bekleyin. |
| `service "spark" is not running` | `docker compose up -d` çalıştırın, ardından `docker compose ps` ile durumu kontrol edin. |
| `service "araclar" is not running` | Araç container'ını `docker compose up -d` ile başlatın. |
| `UnknownTopicOrPartitionException` | Üretici ve Spark tarafında aynı şablonu kullanın. Satış topic'i `satislar`dır. |
| `TimeoutException ... satislar-0` | Eski Spark sürecini `Ctrl+C` ile kapatıp akışı yeniden başlatın. |
| `SparkUI could not bind on port 4040` | Başka bir Spark işi çalışıyor olabilir; eski işi kapatın veya 4041 adresini kullanın. |
| Boş akış batch'i | `update` modunda o batch'te değişen grup olmayabilir; normaldir. |
| `bsm461-s3 is unhealthy` | `docker compose logs s3` ile logları kontrol edin. Güncel healthcheck `127.0.0.1:9333` kullanır. |
| `port is already allocated` | İlgili portu kullanan uygulamayı kapatın veya Compose dosyasındaki host portunu değiştirin. |
| PowerShell `<kodunuz>` hatası | Yer tutucu kullanmayın; bu projede `G231210089` yazın. |

## 12. Teknik sınırlılıklar

Bu yapı yerel geliştirme ve test amacıyla tek bilgisayarda çalışır:

- Kafka tek broker ve `replication factor = 1` ile çalışır. Broker arızasında
  veri kaybı yaşanabilir.
- SeaweedFS S3 tek düğümlüdür; otomatik çoğaltma ve yedekleme yoktur.
- Spark `local[*]` kipindedir; sürücü ve yürütücüler aynı bilgisayardadır.
- Servislerde TLS ve kimlik doğrulama kapalıdır; sistem internete açık
  bırakılmamalıdır.

Üretim ortamında Kafka çoğaltması, dağıtık S3, Spark kümesi, kalıcı checkpoint,
TLS, kimlik doğrulama, yetkilendirme, yedekleme ve izleme eklenmelidir.

b
