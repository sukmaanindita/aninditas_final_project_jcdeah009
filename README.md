# Final Project JCDEAH-009 — Monitoring Laporan Kebakaran Indonesia

## 1. Project Overview

Project ini adalah **Final Project Data Engineering (JCDEAH-009)** yang membangun pipeline data end-to-end untuk memonitor laporan kejadian kebakaran di Indonesia, menggunakan data dari **PetaBencana API**.

**Problem statement**: Bagaimana membantu monitoring jumlah dan distribusi laporan kejadian kebakaran di Indonesia berdasarkan waktu dan wilayah, sehingga pengguna dapat mengidentifikasi area dan periode dengan intensitas laporan yang tinggi?

**Tujuan bisnis dashboard**:
- Mengetahui total laporan kebakaran yang tercatat.
- Melihat sebaran laporan berdasarkan provinsi dan kabupaten/kota.
- Memantau tren laporan dari waktu ke waktu (harian/mingguan).
- Menampilkan laporan terbaru sebagai bahan monitoring near-real-time.
- Menampilkan sebaran geografis laporan pada peta.

## 2. Dataset

- **Sumber**: [PetaBencana API](https://data.petabencana.id/), endpoint `reports/archive`.
- **Disaster type yang diproses**: `fire` (jenis bencana lain ikut masuk ke RAW tapi difilter di STAGING).
- **Periode batch historis**: Januari–Agustus 2026 (`BATCH_HISTORICAL_START_DATE`/`BATCH_HISTORICAL_END_DATE` di `.env`).
- API di-query **per hari** menggunakan parameter `start`/`end` (format `YYYY-MM-DDT00:00:00Z` s/d `YYYY-MM-DDT23:59:59Z`), karena API tidak menyediakan mode "ambil semua data historis sekaligus" maupun filter `disaster_type` di sisi server.
- Hasil setiap hari disimpan sebagai **1 file JSON per tanggal** di GCS (bukan 1 file gabungan), agar:
  - setiap tanggal dapat diproses ulang (retry) secara independen tanpa mengulang tanggal lain,
  - proses ingestion dapat berjalan incremental (1 tanggal = 1 unit kerja),
  - kegagalan pada satu tanggal tidak menghambat tanggal lain.

## 3. Architecture

**Batch**
```
PetaBencana API
      |
      v
Python ingestion (ingestion/fetch_batch_to_gcs.py)
      |
      v
GCS (raw/petabencana/fire/year=YYYY/month=MM/YYYY-MM-DD.json)
      |
      v
BigQuery RAW (batch/load_raw_to_bigquery.py, MERGE by record_key)
      |
      v
dbt STAGING (filter disaster_type = 'fire')
      |
      v
dbt DATA QUALITY (validasi, tidak memblokir)
      |
      v
dbt CURATED (union + dedup + enrichment dimension)
      |
      v
Dashboard (Looker Studio)
```

**Streaming (near-real-time, scheduled polling)**
```
PetaBencana API (polling window bergulir)
      |
      v
Python publisher (ingestion/publish_stream_to_pubsub.py)
      |
      v
Google Pub/Sub (topic + subscription)
      |
      v
Python consumer (stream/load_stream_to_bigquery.py, bounded pull)
      |
      v
BigQuery RAW (MERGE by record_key)
      |
      v
dbt STAGING -> dbt DATA QUALITY -> dbt CURATED (union dengan batch)
      |
      v
Dashboard (Looker Studio)
```

**Dimension**
```
GitHub JSON (regencies.json, provinces.json)
      |
      v
Python extractor (dimension/extract_dimension_to_postgres.py)
      |
      v
PostgreSQL (schema "dimension" di container Postgres milik Airflow)
      |
      v
Python loader (dimension/load_dimension_to_bigquery.py)
      |
      v
BigQuery DIMENSION (dim_region, dim_province)
      |
      v
Dipakai oleh CURATED (enrichment) dan DQ (validasi region_code)
```

## 4. Technology Stack

| Teknologi | Fungsi di project ini |
|---|---|
| **GCP** | Platform cloud utama (project `jcdeah-009`) |
| **GCS** | Data lake — menyimpan JSON mentah hasil ingestion batch |
| **BigQuery** | Data warehouse — layer RAW, STAGING, DQ, CURATED, DIMENSION |
| **Airflow** (LocalExecutor, Docker Compose) | Orkestrasi jadwal & urutan task batch dan streaming |
| **Docker / Docker Compose** | Menjalankan Airflow (webserver, scheduler, metadata Postgres) secara lokal |
| **Python** | Ingestion, loading ke BigQuery, publisher/consumer streaming, dimension pipeline |
| **Pub/Sub** | Message broker untuk streaming (bukan polling langsung ke BigQuery) |
| **dbt** | Transformasi SQL: staging, data quality, curated |
| **PostgreSQL** | Staging area sementara untuk dimension pipeline (reuse container Postgres milik Airflow, schema terpisah) |
| **Looker Studio** | Tools dashboard |

## 5. Batch Pipeline

1. **API ingestion** (`ingestion/fetch_batch_to_gcs.py`): mengambil data 1 hari via `start`/`end`, dengan retry (3 percobaan, backoff 2s/4s) untuk HTTP 429/500/502/503/504, timeout, dan `ConnectionError`.
2. **Daily JSON**: setiap tanggal disimpan sebagai 1 file JSON, path GCS: `raw/petabencana/fire/year=YYYY/month=MM/YYYY-MM-DD.json` (overwrite jika tanggal yang sama diproses ulang).
3. **BigQuery RAW loading** (`batch/load_raw_to_bigquery.py`): membaca file GCS per tanggal, memuat seluruh `disaster_type` apa adanya (tidak difilter di RAW), lalu **MERGE** ke `disaster_batch_raw` menggunakan `record_key`.
4. **`record_key` deterministik**: `SHA256(pkey|created_at)` jika `pkey` tersedia, atau `SHA256(no_pkey|url|created_at)` sebagai fallback jika `pkey` NULL. Formula ini identik dipakai di RAW batch maupun stream.
5. **MERGE/idempotency**: proses re-run untuk tanggal yang sama tidak membuat baris duplikat — baris yang sudah ada di-update, bukan di-insert ulang.
6. **dbt STAGING** (`disaster_batch_staging`): parsing field dari `raw_geometry` (JSON) + filter `disaster_type = 'fire'`.
7. **dbt DATA QUALITY** (`disaster_batch_staging_dq`).
8. **dbt CURATED** (`disaster_curated`): union dengan staging stream, dedup, enrichment dimension.

**Mekanisme jadwal batch (kondisi aktual saat ini)**: DAG `fire_ingestion_dag` berjalan setiap 15 menit (`schedule="*/15 * * * *"`, `max_active_runs=1`). Setiap run memproses **maksimal 1 tanggal historis**, ditentukan otomatis lewat cursor state (`_state/last_completed_date.txt` di GCS) — bukan lewat input manual tanggal. Kalau satu tanggal gagal diproses, cursor tidak maju, sehingga run berikutnya otomatis mengulang tanggal yang sama (retry, bukan skip). Setelah seluruh rentang Januari–Agustus 2026 selesai, run berikutnya menjadi no-op.

## 6. Streaming Pipeline

Perlu ditekankan secara jujur: implementasi streaming pada project ini **bukan continuous real-time streaming**, melainkan **near-real-time / scheduled polling micro-batch**:

- `ingestion/publish_stream_to_pubsub.py` melakukan **polling** ke endpoint arsip PetaBencana yang sama dengan batch (`/reports/archive`), dengan rolling (`stream_lookback_minutes` menit ke belakang dari `stream_end`).
- Hasil polling dipublish sebagai **satu pesan Pub/Sub** per polling run.
- `stream/load_stream_to_bigquery.py` adalah **bounded consumer** — melakukan satu kali `pull()` (maks. 10 pesan) per eksekusi, MERGE ke BigQuery, lalu **ack** hanya setelah MERGE berhasil. Ini bukan proses yang berjalan terus-menerus (continuous worker/daemon).
- Dijadwalkan lewat Airflow (`fire_stream_ingestion_dag`, `schedule="*/5 * * * *"`) — polling terjadi setiap 5 menit dengan lookback 10 menit (ada overlap 5 menit sebagai buffer, aman karena idempotent via `record_key`).

**Alasan pendekatan ini**: PetaBencana **tidak menyediakan endpoint push/webhook/realtime**. Satu-satunya endpoint data yang tersedia adalah endpoint arsip berbasis rentang waktu (`start`/`end`). Karena keterbatasan ini, pendekatan yang paling sesuai dengan kemampuan sumber data adalah scheduled polling dengan rolling, dipublish lewat message broker (Pub/Sub) untuk tetap memenuhi requirement arsitektur streaming — bukan simulasi/klaim continuous streaming yang sebenarnya tidak didukung oleh source API.

## 7. Data Model

| Layer | Isi | Grain |
|---|---|---|
| **RAW** (`disaster_batch_raw`, `disaster_stream_raw`) | Seluruh laporan bencana apa adanya dari API (semua `disaster_type`), tanpa transformasi bisnis | 1 baris = 1 laporan bencana (`record_key`) |
| **STAGING** (`disaster_batch_staging`, `disaster_stream_staging`) | Hanya `disaster_type='fire'`, field di-parse dari JSON mentah | 1 baris = 1 laporan kebakaran |
| **DQ** (`disaster_batch_staging_dq`, `disaster_stream_staging_dq`) | Hasil validasi staging (lihat bagian 8) | 1 baris = 1 pelanggaran rule per record |
| **CURATED** (`disaster_curated`) | Union batch+stream, dedup by `record_key`, enrichment nama wilayah, exclude record simulasi | 1 baris = 1 laporan kebakaran final untuk dashboard |
| **DIMENSION** (`dim_region`, `dim_province`) | Data referensi wilayah Indonesia (BPS regency/province code) | 1 baris = 1 wilayah |

**`record_key`** adalah identifier utama di seluruh layer (RAW s/d CURATED) — dipakai untuk MERGE (idempotency) dan sebagai dedup key di CURATED.

**Enrichment region/province**: CURATED melakukan `LEFT JOIN` `disaster_curated.region_code` ke `dim_region.id`, lalu `dim_region.province_id` ke `dim_province.id`, menghasilkan `region_name` dan `province_name`. Menggunakan `LEFT JOIN` (bukan `INNER JOIN`) karena sebagian `region_code` bernilai NULL atau tidak ditemukan di dimension — baris tetap dipertahankan di CURATED, hanya nama wilayahnya kosong.

## 8. Data Quality

DQ dievaluasi lewat 5 rule berikut (di `disaster_batch_staging_dq` dan `disaster_stream_staging_dq`):

1. **Required field check** — field wajib (`record_key`, `created_at`, `disaster_type`, `fire_lat`, `fire_lng`, `person_lat`, `person_lng`, `url`) tidak boleh NULL.
2. **Region code null check** — `region_code` bernilai NULL.
3. **Region code not found check** — `region_code` terisi tapi tidak ditemukan di `dim_region.id`.
4. **Source inconsistency check** — nama `city` dari source tidak cocok (setelah normalisasi deterministik: uppercase, hapus prefix "KABUPATEN"/"KOTA", hapus spasi) dengan `dim_region.name` untuk `region_code` yang sama.
5. **Duplicate check** — `record_key` muncul lebih dari sekali di staging (secara desain seharusnya tidak terjadi karena staging bersifat incremental dengan `unique_key=record_key`; rule ini adalah pengecekan defensif).

**Penting — DQ adalah monitoring/audit layer, BUKAN hard gate**: implementasi aktual **tidak melakukan filtering apa pun** terhadap staging berdasarkan hasil DQ. Konvensi yang dipakai: DQ hanya mencatat baris untuk **check yang GAGAL** (satu baris = satu pelanggaran); tidak ada baris berarti check tersebut lolos untuk record itu. Alasan desain:

- Laporan bencana tetap dipertahankan apa adanya — data source tidak pernah dibuang hanya karena tidak lolos satu rule kualitas data.
- Record bermasalah tetap dapat diidentifikasi secara eksplisit lewat tabel DQ terpisah, tanpa mengubah/menyembunyikan data staging.
- Hasil DQ disimpan di tabel terpisah (`*_staging_dq`), bukan sebagai kolom tambahan di staging — menjaga staging tetap murni representasi data yang sudah difilter tipe bencananya saja.
- Pendekatan ini mencegah kehilangan informasi source akibat validasi yang terlalu ketat, mengingat data laporan bencana publik secara alami memiliki inkonsistensi (lihat rule 4).

## 9. Idempotency

- **`record_key`** dibangun deterministik: `SHA256(pkey|created_at)`, atau `SHA256(no_pkey|url|created_at)` jika `pkey` NULL — formula yang **sama persis** dipakai di jalur batch (`batch/load_raw_to_bigquery.py`) maupun stream (`stream/load_stream_to_bigquery.py`).
- Setiap loading ke BigQuery RAW menggunakan pola **load ke temporary table → MERGE ke tabel target by `record_key`** (`sql/raw/merge_disaster_batch_raw.sql`, dipakai ulang oleh kedua jalur).
- Efeknya: menjalankan ulang proses yang sama (baik retry manual, retry otomatis oleh Airflow, maupun redelivery pesan Pub/Sub yang bersifat at-least-once) **tidak pernah menghasilkan baris duplikat** — baris yang sudah ada di-update di tempat.
- Di layer CURATED, deduplikasi tambahan dilakukan dengan `QUALIFY ROW_NUMBER() OVER (PARTITION BY record_key ORDER BY staged_at DESC) = 1`, untuk mengantisipasi kemungkinan (meski belum pernah teramati) satu laporan yang sama muncul dari jalur batch maupun stream sekaligus.

## 10. Error Handling & Alerting

- **HTTP retry**: `ingestion/fetch_batch_to_gcs.py` dan `ingestion/publish_stream_to_pubsub.py` melakukan retry hingga 3 kali (backoff 2 detik, lalu 4 detik) khusus untuk status HTTP 429, 500, 502, 503, 504.
- **Timeout/connection handling**: request menggunakan `timeout=30` detik; `requests.exceptions.Timeout` dan `ConnectionError` juga masuk skema retry yang sama; error lain (mis. 404) tidak di-retry dan langsung dianggap gagal.
- **Airflow task failure**: setiap task adalah `BashOperator` yang keluar dengan exit code non-zero saat gagal (`sys.exit(1)` pada script, atau exception yang tidak tertangkap). Airflow menandai task tersebut `FAILED`, dan task downstream (bergantung pada dependency `>>`) tidak dijalankan.
- **Email alert**: `default_args={"email_on_failure": True, "email": [ALERT_EMAIL_TO]}` pada kedua DAG — Airflow mengirim email otomatis ke alamat di `.env` (`ALERT_EMAIL_TO`) setiap kali task gagal, menggunakan konfigurasi SMTP Gmail (App Password) yang di-set lewat variabel `AIRFLOW__SMTP__*` di `.env`. Tidak ada email yang dikirim saat pipeline berhasil.
- **Behavior saat task gagal**: khusus batch, karena `load_raw_to_bigquery.py` hanya menandai satu tanggal "selesai" (memajukan cursor GCS) **setelah** MERGE sukses, kegagalan pada satu tanggal membuat run berikutnya otomatis mengulang tanggal yang sama (bukan lanjut ke tanggal berikutnya). Untuk consumer stream, pesan Pub/Sub yang gagal diproses **tidak di-ack**, sehingga akan di-redeliver secara otomatis oleh Pub/Sub pada pull berikutnya.

## 11. Dashboard

**Status**: kebutuhan query analytical sudah diaudit dan didesain (lihat hasil audit terpisah), **dashboard Looker Studio itu sendiri belum dibangun** pada tahap ini.

**Tujuan dashboard**: monitoring sebaran dan tren laporan kebakaran Indonesia berdasarkan `fp_aninditas_curated.disaster_curated`.

**Visual yang direncanakan** (berdasarkan kolom aktual yang tersedia di curated — `record_key`, `created_at`, `province_name`, `region_name`, `city`, `fire_lat`, `fire_lng`, `status`, `text`):
- KPI: total fire reports.
- Peta sebaran geografis (`fire_lat`/`fire_lng`).
- Chart jumlah laporan per provinsi.
- Chart jumlah laporan per region/kabupaten-kota.
- Time series tren laporan (harian/mingguan).
- Tabel laporan terbaru (diurutkan berdasarkan `created_at`).

Tidak ada metric lain (mis. distribusi status) yang dicantumkan karena saat ini kolom `status` di curated hanya memiliki satu nilai (`confirmed`), sehingga belum informatif sebagai chart terpisah.

## 12. How to Run

> Command di bawah adalah command aktual yang sudah dites berjalan di project ini (lewat `auto_command.sh`, Docker Compose, dan dbt CLI langsung).

**Menjalankan Airflow (Docker Compose)**:
```bash
docker compose -f airflow/docker-compose.yaml up -d
```
DAG baru default dalam keadaan *paused* di Airflow — perlu di-*unpause* lewat UI atau:
```bash
docker exec airflow-airflow-scheduler-1 airflow dags unpause fire_ingestion_dag
docker exec airflow-airflow-scheduler-1 airflow dags unpause fire_stream_ingestion_dag
```
*(Catatan: perintah `unpause` di atas diketahui dari histori command yang sudah dijalankan selama pengembangan, bukan dari file konfigurasi tertulis.)*

Setelah mengubah `.env`, container yang sedang berjalan **wajib** di-recreate agar variabel baru terbaca (env_file hanya dibaca saat container start):
```bash
docker compose -f airflow/docker-compose.yaml up -d --force-recreate airflow-webserver airflow-scheduler
```

**Menjalankan pipeline tanpa Airflow** (`auto_command.sh`, dijalankan dari root project dengan `.venv` host):
```bash
./auto_command.sh batch [start_date end_date]      # default: mode next_date otomatis dari .env
./auto_command.sh stream [lookback_minutes stream_end]
./auto_command.sh dimension
```

**dbt** (dari folder `dbt/`, dengan `DBT_PROFILES_DIR` menunjuk ke folder ini dan variabel `.env` sudah di-export ke shell). Command di bawah mengikuti persis apa yang dipakai masing-masing DAG:
```bash
dbt parse
dbt run   --select disaster_batch_staging      # sama seperti task dbt_staging di fire_ingestion_dag
dbt run   --select disaster_batch_staging_dq   # sama seperti task dbt_dq di fire_ingestion_dag
dbt build --select disaster_stream_staging     # sama seperti task dbt_stream_staging di fire_stream_ingestion_dag
dbt build --select disaster_stream_staging_dq  # sama seperti task dbt_stream_dq di fire_stream_ingestion_dag
dbt build --select disaster_curated            # tidak ada task Airflow untuk ini (curated adalah VIEW, selalu fresh) - dijalankan manual saat validasi
```
`dbt seed --select dim_region` masih dapat dijalankan (file seed belum dihapus), **tapi tidak disarankan** — akan menimpa tabel `dim_region` yang sekarang bersumber dari pipeline PostgreSQL, bukan dari seed CSV lagi (lihat bagian 14).

**Autentikasi GCP**: project ini menggunakan Application Default Credentials (`gcloud auth application-default login`), bukan service account key file.

## 13. Project Structure

```
final_project/
├── ingestion/              # fetch_batch_to_gcs.py, publish_stream_to_pubsub.py
├── batch/                  # load_raw_to_bigquery.py (batch RAW loader)
├── stream/                 # load_stream_to_bigquery.py (Pub/Sub consumer)
├── dimension/              # extract_dimension_to_postgres.py, load_dimension_to_bigquery.py
├── dbt/
│   ├── models/
│   │   ├── staging/        # disaster_batch_staging(.sql/_dq.sql), disaster_stream_staging(.sql/_dq.sql)
│   │   ├── curated/         # disaster_curated.sql
│   │   └── sources.yml
│   ├── seeds/               # dim_region.csv (masih ada, lihat bagian 14)
│   ├── macros/               # generate_schema_name override
│   ├── dbt_project.yml
│   └── profiles.yml
├── airflow/
│   ├── dags/                # fire_ingestion_dag.py, fire_stream_ingestion_dag.py
│   ├── docker-compose.yaml
│   ├── Dockerfile
│   └── requirements-airflow.txt
├── sql/raw/                 # merge_disaster_batch_raw.sql (dipakai ulang batch & stream)
├── auto_command.sh          # runner batch/stream/dimension tanpa Airflow
├── requirements.txt          # dependency Python (host)
├── .env                       # konfigurasi (tidak di-commit ke git)
└── CLAUDE.md                  # panduan arsitektur & gaya kode project
```

