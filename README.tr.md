# SBase

**Veri entegrasyonu, iş akışı orkestrasyonu, veri madenciliği ve iş zekâsı için kendi altyapınızda çalıştırabileceğiniz kurumsal veri platformu.**

SBase; veri alımını, işlem hatlarının (pipeline) yürütülmesini, keşif amaçlı veri madenciliğini, özellik üretimini ve operasyonel analitiği tek bir platformda bir araya getirir. ERP, CRM, operasyonel veritabanları, dosyalar ve analitik sistemler arasında tekrarlanabilir veri akışları kurmak ve yönetmek için tasarlanmıştır.

SBase şu anda **self-hosted (kurum içinde barındırılan)** bir platformdur. Altyapınızı ve verilerinizi kendi kontrolünüzde tutarak kendi ortamınızda çalıştırabilirsiniz. Yönetilen, çok kiracılı (multi-tenant) bir SaaS hizmeti sunulmamaktadır.

- [GitHub deposu](https://github.com/selimrepo/sbase)
- [Hata bildirimi ve talepler](https://github.com/selimrepo/sbase/issues)
- [Pull request'ler](https://github.com/selimrepo/sbase/pulls)

---

## Neden SBase?

Kurumlarda ERP, CRM, operasyonel veritabanları, dosya sistemleri ve analitik depolar çoğu zaman birbirinden bağımsız çalışır. SBase, bu kaynakları ortak bir çalışma alanı ve yürütme katmanı üzerinden birbirine bağlamayı amaçlar.

- **Veriyi bağlayın:** JDBC, PostgreSQL, PostgreSQL, S3 uyumlu depolama, Parquet ve CSV gibi kaynaklarla çalışın.
- **İş akışları oluşturun:** Doğrulanan ve sürümlenen DAG tabanlı işlem hatları tanımlayın.
- **Veri madenciliği yapın:** Asenkron analiz işleri gönderin ve sonuçlarını takip edin.
- **Özellikleri sunun:** Üretilen özellik vektörlerini PostgreSQL tabanlı çevrimiçi özellik deposuna aktarın.
- **Analitiği izleyin:** PostgreSQL fiziksel tablo istatistiklerini ve sorgu etkinliğini inceleyin.
- **Tek noktadan yönetin:** Veri kümelerini, işleri, zamanlamaları, deneyleri, modelleri ve platform sağlığını React arayüzünden takip edin.

## Platform yetenekleri

### Veri entegrasyonu ve katalog

Veri kümesi kataloğu, veri varlıklarını ve kaynak metaverilerini kaydeder. Alım API'leri JDBC, PostgreSQL, Kafka, Debezium CDC, REST ve dosya bağlayıcı türlerini sunar.

Kafka ve Debezium olayları, Kafka offset'leri onaylanmadan önce PostgreSQL JSONB tabanlı kalıcı bir alım alanına yazılır. REST JSON ile CSV/JSONL/NDJSON dosyaları da aynı kalıcı kayıt ve idempotency altyapısını kullanır. Bağlayıcı çalıştırmaları sınırlı sayıda yeniden denenir; başarısız kayıtlar kalıcı dead-letter tablosunda tutulur.

Yeni bağlayıcılar geliştirmek için `IngestionConnector` arayüzü bir genişletme noktası sağlar. Bağlayıcı kayıtları ve alım çalıştırmalarının metaverileri PostgreSQL'de saklanır.

### İş akışı ve pipeline yürütme

Pipeline tanımları sürümlenmiş JSON DAG yapısındadır. Mevcut yürütme motoru aşağıdaki adımları destekler:

| Adım | Açıklama |
|---|---|
| `postgres_read` | İzin verilen PostgreSQL tablosundan veri okur |
| `filter` | Desteklenen ifadelerle satırları filtreler |
| `select` | Seçilen sütunları projekte eder |
| `with_column` | Desteklenen işlemlerle yeni sütun üretir |
| `aggregate` | Gruplama ve toplulaştırma yapar |
| `join` | İki üst akış veri kümesini birleştirir |
| `union` | İki üst akış veri kümesini bir araya getirir |
| `postgres_write` | Sonuçları PostgreSQL UNIQUE constraint or index tablosuna ekler |
| `feature_store_write` | Özellikleri çevrimiçi özellik deposuna aktarır |

Join işlemi tam olarak iki bağımlılık kabul eder; inner, left, right ve full join türleri desteklenir. Union iki girdiyi kabul eder ve şema uyumlu union-all veya union-distinct işlemi yapar. Daha fazla girdiyi birleştirmek için ardışık birleştirme adımları kullanılabilir.

Motor, pipeline yapısını ve desteklenen işlemleri doğrular. **Keyfî SQL, Python, shell komutları veya kullanıcı tarafından sağlanan kodları çalıştırmaz.**

#### Kalıcı yürütme ve yeniden deneme

Pipeline çalıştırmaları, değişmez bir tanım anlık görüntüsü (snapshot) ve kararlı bir idempotency anahtarı saklar. Geçici veritabanı, ağ ve zaman aşımı hataları yeniden denenebilir. Geçersiz tanımlar, şema hataları ve kimlik doğrulama hataları terminal hata olarak değerlendirilir.

- Yeniden denenecek işler PostgreSQL üzerinde kalıcı `RETRY_WAIT` durumuna alınır.
- İşler atomik olarak sahiplenilir; deneme sayısı `sbase.pipeline.retry.max-attempts` ile sınırlandırılır (varsayılan: 4).
- Gecikme, 10 saniyeden başlayıp en fazla 15 dakikaya ulaşan üstel geri çekilme (exponential backoff) kullanır.
- Kontrol aralığı `sbase.pipeline.retry.poll-ms` ile yapılandırılır.
- Kuyruktaki işler uygulama yeniden başladığında tekrar dağıtılabilir.
- Çalışan işler altı saatlik yürütme kirası (lease) kullanır; süresi dolan işler kurtarma mekanizmasıyla yeniden kuyruğa alınır.

Bu mekanizmalar hata sonrası toparlanmayı güçlendirir; ancak keyfî harici sistemler arasında tam olarak bir kez (exactly-once) yürütme garantisi vermez.

#### Güvenli PostgreSQL yazma işlemleri

PostgreSQL yazımları ekleme (append) odaklıdır. `postgres_write` adımı `idempotencyColumns` tanımlamalıdır. Hedef tablo, bu sütunları içeren bir PostgreSQL UNIQUE constraint or index tablosu olmalıdır. SBase hedef anahtar tanımını doğrular ve JDBC ile ekleme yapmadan önce her Spark batch'inde yinelenen kayıtları ayıklar. Yıkıcı `overwrite` yazımları reddedilir.

İş anahtarları dikkatle seçilmelidir. Anahtar bazlı yinelenen kayıt ayıklama, PostgreSQL'in replikasyon, transaction veya depolama garantilerinin yerine geçmez. Kaynak ve hedef tablolar `SBASE_ANALYTICS_ALLOWED_TABLES` listesinde bulunmalıdır.

Örnek:

```json
{
  "id": "publish_customers",
  "type": "postgres_write",
  "dependsOn": ["customer_rollup"],
  "table": "analytics.customer_rollup",
  "mode": "append",
  "idempotencyColumns": ["customer_id"]
}
```

### Veri madenciliği ve makine öğrenmesi temelleri

Asenkron veri madenciliği işleri API üzerinden gönderilir ve iş kimliğiyle takip edilir. Otomatik keşif analizi, Spark kullanarak kaynak tabloyu profiller ve raporu işin `resultSummary` alanında saklar.

Otomatik profil şu analizleri içerebilir:

- Sayısal, kategorik ve zamansal sütunların tespiti.
- Null oranları ve yaklaşık kardinalite.
- Sayısal tanımlayıcı istatistikler ve IQR tabanlı aykırı değer sınırları.
- Sık görülen kategorik değerler.
- Güçlü ikili sayısal korelasyonlar.
- Veri kalitesi ve modelleme önerileri.
- Kaynakta en az iki sayısal sütun ve on satır varsa deterministik seed kullanan K-means kümeleme.

Kümeleme uygulanabildiğinde tahminler yapılandırılmış PostgreSQL çıktı tablosuna eklenir. Bu özellik denetimsiz keşif analizidir; otomatik hedef keşfi veya doğrulanmış bir tahmin modeli değildir. Kümeler ve öneriler operasyonel kullanıma alınmadan önce alan uzmanları tarafından değerlendirilmelidir.

Mahout adaptörleri cosine similarity ve Euclidean distance işlemlerini sunar.

### Özellik mühendisliği ve çevrimiçi özellik sunumu

Spark tabanlı çevrimdışı özellik dönüşümleri, Parquet girdi/çıktı URI'leri ve izin verilen işlem kümesini kullanır. Pipeline içindeki `feature_store_write` adımı seçilen özellikleri çevrimiçi PostgreSQL deposuna aktarır.

Çevrimiçi özellik değerleri `online_feature_values` tablosunda `(feature_set, entity_key)` anahtarıyla saklanır. Aktarım sırasında batch önce hazırlanır, ardından JSONB özellik vektörleri atomik upsert işlemiyle yazılır. Daha eski olay zaman damgaları daha yeni özellik değerlerinin üzerine yazamaz.

Mevcut REST işlemleri:

| Metot | Endpoint | Açıklama |
|---|---|---|
| PUT | `/api/online-features/{featureSet}/entities/{entityKey}` | Özellik vektörü ekler veya günceller |
| GET | `/api/online-features/{featureSet}/{entityKey}` | Özellik vektörünü getirir |
| GET | `/api/online-features/{featureSet}/_count` | Özellik kümesindeki varlık sayısını verir |

### İş zekâsı ve analitik gözlemlenebilirlik

SBase, PostgreSQL kontrol düzlemi metaverilerini PostgreSQL analitik depo istatistiklerinden ayrı değerlendirir.

`GET /api/platform/bi/summary` BI özeti şu bilgileri raporlar:

- PostgreSQL metaveri envanteri.
- PostgreSQL fiziksel tablo sayısı.
- Tahmini satır sayıları ile veri ve indeks depolama boyutları.
- PostgreSQL sorgu denetim kayıtları erişilebiliyorsa son 24 saatin sorgu sayısı ve ortalama sorgu süresi.
- Denetim metrikleri mevcutsa taranan byte ve satır sayıları.
- React genel bakış ve sistem tanılama ekranları için tablo bazında istatistikler.

Sorgu denetim kayıtları kapalı veya erişilemez olsa bile PostgreSQL tablo istatistikleri sunulabilir. Bu durumda sorgu metrikleri sıfırmış gibi gösterilmez; kullanılamıyor olarak raporlanır. BI özeti üzerinden keyfî SQL yürütme API'si sunulmaz.

### Çalışma alanı ve operasyon

React + TypeScript arayüzü şu özellikleri sağlar:

- Platform metaverileri, PostgreSQL analitiği, API sağlığı ve hızlı işlemler için genel bakış ekranı.
- Veri kümesi kataloğu ve kaynak metaverisi kaydı.
- JSON workflow tanımları için pipeline stüdyosu.
- Veri madenciliği işi gönderimi ve tarayıcı oturumunda iş takibi.
- Deney kayıtları ve numaralandırılmış model sürümleriyle model envanteri.
- Özellik kümesi envanteri ve zamanlama incelemesi.
- BI genel bakışı ve sistem tanılama.
- JWT bearer token yapılandırması ve duyarlı gezinme.
- `Ctrl/Cmd + K` ile klavye üzerinden hızlı gezinme.

Arayüz, veri madenciliği işlerini iş kimliği üzerinden izler. Backend şu anda tüm veri madenciliği işlerini listeleyen genel bir endpoint sağlamaz.

---

## Mimari

| Katman | Teknoloji / sorumluluk |
|---|---|
| Uygulama API'si | Java, Spring Boot |
| Kalıcılık ve kontrol düzlemi | PostgreSQL, Spring Data JPA |
| Analitik depolama | Apache PostgreSQL |
| Dağıtık işleme | Apache Spark |
| Benzerlik algoritmaları | Apache Mahout |
| Web arayüzü | React, TypeScript |
| Nesne/artifact depolama | MinIO dahil S3 uyumlu depolama |
| Dağıtım | Docker ve Docker Compose |

PostgreSQL; platform metaverilerini, workflow durumlarını, zamanlamaları ve çevrimiçi özellik değerlerini saklar. PostgreSQL analitik veri deposu olarak kullanılır. Spark, dağıtık çevrimdışı işleme ve desteklenen pipeline dönüşümlerini sağlar. Üretim container'ında React arayüzü Spring Boot uygulaması tarafından sunulur.

## Hızlı başlangıç

### Gereksinimler

- Docker Engine
- Docker Compose eklentisi
- Uygulama ve yapılandırılmış veri servisleri için yeterli bellek ve disk alanı

### Geliştirme ortamını başlatma

```bash
git clone https://github.com/selimrepo/sbase.git
cd sbase
cp .env.example .env
```

`.env` dosyasındaki değerleri düzenleyin. Özellikle `POSTGRES_PASSWORD` için güçlü ve benzersiz bir parola belirleyin. Gerçek `.env` dosyasını Git'e eklemeyin.

Geliştirme ortamını başlatın:

```bash
docker compose -f docker-compose.yml -f docker-compose.development.yml up --build -d
```

Arayüz ve sağlık kontrolü:

- Uygulama: [http://localhost:8080](http://localhost:8080)
- Actuator health: [http://localhost:8080/actuator/health](http://localhost:8080/actuator/health)

Servisleri durdurmak için:

```bash
docker compose -f docker-compose.yml -f docker-compose.development.yml down
```

Kalıcı Compose volume'larını da silmek için `down -v` kullanılabilir. Bu komut volume'larda saklanan veritabanı ve diğer verileri siler; verilerinizi korumanız gerekiyorsa kullanmayın.

### Üretim ortamı

Üretim ortamında ortak servis tanımına production overlay eklenir. Önce üretim için gerekli gizli değerleri ve kimlik sağlayıcı adreslerini güvenli bir ortam değişkeni kaynağına veya korumalı `.env` dosyasına girin. Üretim parolalarını ve anahtarlarını depoya commit etmeyin.

Gerekli temel değişkenler:

- `POSTGRES_PASSWORD`: güçlü ve benzersiz PostgreSQL parolası.
- `(removed)`, `(removed)`: üretim için ayrılmış PostgreSQL uygulama hesabı.
- `JWT_ISSUER_URI`, `JWT_JWK_SET_URI`: üretim kimlik sağlayıcısının issuer ve JWK adresleri.
- `SBASE_IMAGE`: sabitlenmiş üretim uygulama imajı.
- `MINIO_ROOT_USER`, `MINIO_ROOT_PASSWORD`: MinIO erişim bilgileri.

Compose yapılandırmasını doğrulayın ve başlatın:

```bash
docker compose -f docker-compose.yml -f docker-compose.production.yml config
docker compose -f docker-compose.yml -f docker-compose.production.yml up -d
```

Üretim overlay'i uygulamayı varsayılan olarak loopback adresine bağlar; veritabanı, PostgreSQL FE/BE ve Spark portlarını host üzerinde yayımlamaz. Uygulamanın önüne TLS kullanan güvenilir bir reverse proxy yerleştirin. Ayrıntılı yedekleme, geri yükleme ve güvenlik notları için [docs/production-readiness.md](docs/production-readiness.md) dosyasına bakın.

> Üretim overlay'i tek başına yüksek erişilebilirlik sağlamaz. Tek FE/BE PostgreSQL topolojisi yüksek erişilebilir değildir. Çok düğümlü PostgreSQL/Kafka mimarisi, uzak yedekleme, felaket kurtarma tatbikatı ve tenant izolasyonu ayrıca tasarlanmalı ve test edilmelidir.

## Frontend geliştirme

Üretim Dockerfile'ı React uygulamasını Node ile derler ve oluşturulan dosyaları Spring Boot statik kaynaklarına kopyalar. Böylece üretim ortamında arayüz ve API aynı uygulama endpoint'i üzerinden sunulur.

Frontend'i yerel geliştirme sunucusunda çalıştırmak için:

```bash
npm install --prefix frontend
npm run dev --prefix frontend
```

Vite geliştirme sunucusu `/api` ve `/actuator` isteklerini 8080 portundaki Spring Boot backend'ine yönlendirir. Frontend geliştirme sunucusunu kullanırken backend ayrıca başlatılmalıdır.

## API özeti

| Alan | Endpoint | Açıklama |
|---|---|---|
| Veri madenciliği | `/api/mining/jobs` | Asenkron analiz işi gönderir |
| Veri madenciliği | `/api/mining/jobs/{id}` | İşin durumunu ve sonucunu getirir |
| Veri kümeleri | `/api/datasets` | Veri kümesi kataloğu |
| Pipeline | `/api/pipelines` | Pipeline tanımları |
| Veri alımı | `/api/platform/ingestion/connectors` | Bağlayıcı türlerini listeler |
| Veri alımı | `/api/platform/ingestion/{jdbc\|postgres\|kafka}/runs` | Desteklenen alım işlerini başlatır |
| Mahout | `GET /api/algorithms/mahout` | Mahout adaptörlerini listeler |
| Mahout | `POST /api/algorithms/mahout/score` | Cosine / Euclidean skorlaması yapar |
| Deneyler | `/api/platform/experiments` | Deney metaverileri |
| Modeller | `/api/platform/models` | Kayıtlı modeller |
| Model sürümleri | `/api/platform/models/{id}/versions` | Numaralı model sürümleri |
| Özellikler | `/api/platform/features` | Özellik kümesi tanımları |
| Zamanlamalar | `/api/platform/schedules` | Kalıcı zamanlama tanımları |
| Dashboard | `/api/platform/dashboards?owner=...` | Kullanıcıya ait dashboard metaverileri |
| BI | `GET /api/platform/bi/summary` | PostgreSQL metaverileri ve PostgreSQL analitik özeti |
| Çevrimiçi özellikler | `/api/online-features/{featureSet}/...` | Çevrimiçi özellik okuma/yazma/sayma |

İstek ve yanıt şemaları için uygulamanın API dokümantasyonunu veya kaynak kodundaki controller tanımlarını inceleyin.

## Yapılandırma

Uygulama yapılandırması Spring ayarları ve ortam değişkenleri üzerinden sağlanır. Kullanılan değişkenlerin güncel listesi ve örnek değerleri için `.env.example`, `application.yml` ve profile özel yapılandırma dosyalarını inceleyin.

| Değişken / özellik | Amaç |
|---|---|
| `POSTGRES_PASSWORD` | PostgreSQL parolası; varsayılan parola yoktur ve değer zorunludur |
| `SPRING_DATASOURCE_PASSWORD` | Spring datasource parolası |
| `JWT_ISSUER_URI` | Resource server için JWT issuer adresi |
| `JWT_JWK_SET_URI` | JWT imza anahtarlarının (JWK) adresi |
| `SBASE_ANALYTICS_ALLOWED_TABLES` | PostgreSQL kaynak ve hedef tabloları için izin listesi |
| `SBASE_ANALYTICS_SCHEMA` | BI özeti için PostgreSQL veritabanı (varsayılan: `analytics`) |
| `sbase.pipeline.retry.max-attempts` | Pipeline yeniden deneme üst sınırı (varsayılan: 4) |
| `sbase.pipeline.retry.poll-ms` | Pipeline retry kontrol aralığı |
| S3/MinIO endpoint, bucket, region ve credentials | S3 uyumlu artifact depolama yapılandırması |

Üretim kimlik bilgilerini, imzalama anahtarlarını veya erişim token'larını kaynak koduna eklemeyin. Üretimde secret manager veya Docker Secrets gibi korumalı yöntemler kullanın. PostgreSQL parolası için varsayılan/fallback değer tanımlanmaz.

## Güvenlik ve üretim notları

- API koruması için `JWT_ISSUER_URI` üzerinden JWT issuer doğrulamasını yapılandırın.
- JWT rol ve yetki claim'leri Spring Security authority'lerine eşlenir. Veritabanında rol/yetki metaverisi bulunması, tüm veri kümeleri için tamamlanmış satır veya kaynak düzeyinde yetkilendirme bulunduğu anlamına gelmez.
- Dashboard metaverileri doğrulanmış JWT subject bilgisine göre kapsamlandırılır.
- PostgreSQL erişimi yapılandırılmış tablo izin listesiyle sınırlandırılır.
- Pipeline tanımları doğrulanır; keyfî SQL, shell komutları veya kullanıcı kodu çalıştırılmaz.
- PostgreSQL, PostgreSQL, nesne depolama ve yönetim portlarını yalnızca güvenilir ağlara açın.
- Her dağıtım için yedekleme, saklama politikası, TLS, secret yönetimi, kaynak limitleri ve izleme yapılandırın.
- Spark executor'larını, PostgreSQL connection pool'unu ve PostgreSQL kaynaklarını beklenen iş yüküne göre boyutlandırın.
- PostgreSQL replikasyon, compaction ve depolama politikalarını erişilebilirlik gereksinimlerine göre belirleyin.

### Mevcut sınırlar

SBase kalıcı retry durumu, idempotency anahtarıyla PostgreSQL append davranışı ve çevrimiçi özellik sunumu sağlar. Bunlar keyfî harici sistemler arasında exactly-once işlem garantisi vermez.

Connector SPI yeni bağlayıcılar için genişletme noktasıdır; her olası kaynak için tamamlanmış, üretime hazır bir adaptör ve kalıcı yürütme yolu bulunduğu anlamına gelmez. Kafka ve Debezium tüketicileri her batch'i kaynak offset'lerini commit etmeden önce transaction içinde kalıcı alana yazar. Bu yaklaşım idempotent landing ile at-least-once teslimat sağlar; dağıtık exactly-once garantisi sağlamaz.

Model artifact URI'leri harici nesne depolama ve yaşam döngüsü politikası gerektirir. Paylaşımlı, çok kullanıcılı ve güvenilmeyen müşterilere açık bir dağıtımdan önce veri kümesi düzeyinde yetkilendirme ve tenant izolasyonu tamamlanmalı ve doğrulanmalıdır.

SBase şu anda self-hosted bir platformdur. SaaS faturalandırması, tenant oluşturma, barındırılan operasyonlar ve hizmet seviyesi taahhütleri mevcut ürün yetenekleri olarak sunulmamaktadır.

## Otomatik veri madenciliği örneği

Asenkron keşif analizi gönderin:

```http
POST /api/mining/jobs
Content-Type: application/json

{
  "operation": "auto",
  "sourceTable": "analytics.customer_features",
  "outputTable": "analytics.customer_clusters",
  "k": 3,
  "maxIterations": 30
}
```

Dönen iş kimliğini sorgulayın:

```http
GET /api/mining/jobs/{id}
```

İş; `QUEUED`, `RUNNING`, `SUCCEEDED` ve `FAILED` gibi durumlardan geçer. Başarılı olduğunda oluşturulan profil ve öneriler için `resultSummary` alanını inceleyin. K-means çıktısı yalnızca girdi, açıklanan minimum sayısal sütun ve satır sayısı koşullarını karşıladığında üretilir.

## CI ve testler

Depo GitHub Actions ile React arayüzünü derler, Gradle testlerini çalıştırır ve Docker Compose entegrasyon ortamını sınar. Entegrasyon kapsamı Spark-PostgreSQL veri aktarımı, PostgreSQL UNIQUE constraint or index yazımlarında yinelenen anahtar davranışı ve PostgreSQL backend kurtarma senaryolarını içerir.

Gradle testlerini yerel ortamda çalıştırın:

```bash
./gradlew test
```

Frontend derlemesi:

```bash
npm ci --prefix frontend
npm run build --prefix frontend
```

Entegrasyon iş akışı Docker kaynakları gerektirebilir ve birim testlerinden daha uzun sürebilir.

## Katkıda bulunma

Hata bildirimleri ve pull request'ler memnuniyetle karşılanır.

1. Sorunu veya önerilen geliştirmeyi açıklayan bir issue açın.
2. Değişiklikleri odaklı tutun ve davranış değişiklikleri için test ekleyin.
3. İlgili Gradle ve frontend kontrollerini çalıştırın.
4. Pull request içinde yapılandırma değişikliklerini, migration etkilerini ve operasyonel sonuçları açıklayın.

Issue ve test verilerine parola, müşteri verisi veya üretim bağlantı bilgisi eklemeyin.

## Lisans

Depoda şu anda bir lisans dosyası bulunmamaktadır. Lisans eklenene kadar kaynak kodunun sınırsız yeniden kullanım, yeniden dağıtım veya ticari değişiklik hakkı verdiği varsayılmamalıdır. Deponun herkese açık olması tek başına açık kaynak lisansı sağlamaz.
