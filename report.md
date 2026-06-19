# Laporan Proyek UAS Sistem Terdistribusi

**Tema:** Pub-Sub Log Aggregator Terdistribusi dengan Idempotent Consumer, Deduplication, dan Transaksi/Kontrol Konkurensi  
**Bahasa Pemrograman:** Python  
**Stack:** FastAPI, Redis, PostgreSQL, Docker Compose, Pytest  
**Sumber Utama:** `pindah/Book_2012_Distributed systems _Couloris.pdf`

---

## 1. Ringkasan Sistem

Proyek ini membangun sistem **Pub-Sub Log Aggregator** berbasis multi-service yang berjalan menggunakan Docker Compose. Sistem terdiri dari empat layanan utama: `publisher`, `aggregator`, `broker`, dan `storage`. Layanan `publisher` bertugas menjadi simulator pengirim event, termasuk event duplikat untuk menguji idempotency. Layanan `aggregator` menyediakan API FastAPI untuk menerima event, membaca daftar event unik, dan menampilkan statistik. Layanan `broker` menggunakan Redis sebagai antrean internal agar penerimaan event dan proses penulisan database dapat dipisahkan secara asinkron. Layanan `storage` menggunakan PostgreSQL sebagai penyimpanan persisten.

Masalah utama yang diselesaikan adalah duplikasi event pada sistem terdistribusi. Dalam jaringan yang tidak selalu stabil, publisher dapat melakukan retry sehingga event yang sama dikirim lebih dari sekali. Jika consumer tidak idempotent, event duplikat dapat menghasilkan data ganda, statistik salah, atau side effect berulang. Sistem ini mengatasinya dengan menggunakan kombinasi `topic` dan `event_id` sebagai identitas unik, lalu menerapkan `UNIQUE(topic, event_id)` di PostgreSQL. Worker memproses event dari Redis menggunakan pola `INSERT ... ON CONFLICT DO NOTHING`, sehingga event yang sudah pernah diproses tidak disimpan ulang.

Sistem juga mendukung persistensi melalui Docker named volume `pg_data`, sehingga data dan deduplication state tetap aman meskipun container dihentikan dan dijalankan kembali. Endpoint utama yang tersedia adalah `POST /publish`, `GET /events`, dan `GET /stats`.

---

## 2. Keputusan Desain

| Aspek | Keputusan |
|---|---|
| Arsitektur | Multi-service dengan Docker Compose: aggregator, publisher, Redis, PostgreSQL |
| Komunikasi | HTTP API dari publisher ke aggregator, lalu queue internal melalui Redis |
| Broker | Redis, karena ringan dan cukup untuk simulasi antrean lokal Compose |
| Storage | PostgreSQL, karena mendukung transaksi ACID, unique constraint, dan upsert |
| Deduplication | Constraint `UNIQUE(topic, event_id)` pada tabel `processed_events` |
| Idempotency | `INSERT ... ON CONFLICT DO NOTHING` agar event sama tidak diproses ulang |
| Statistik | Tabel `stats` dengan counter `received`, `unique_processed`, dan `duplicate_dropped` |
| Persistensi | Named volume `pg_data` untuk PostgreSQL dan `broker_data` untuk Redis |
| Isolation | Default PostgreSQL `READ COMMITTED`, dibantu operasi SQL atomik |
| Observability | Endpoint `/stats`, endpoint `/events`, dan logging container |

---

## 3. Analisis Teori Bab 1-13

### Bab 1: Karakteristik Sistem Terdistribusi dan Trade-off Desain Pub-Sub Aggregator

Sistem terdistribusi memiliki karakteristik utama berupa konkurensi, tidak adanya waktu global tunggal, kegagalan parsial, dan kebutuhan koordinasi antar komponen yang berjalan pada proses atau mesin berbeda (Coulouris et al., 2012). Pada proyek ini, karakteristik tersebut muncul melalui pemisahan layanan `publisher`, `aggregator`, `broker`, dan `storage`. Setiap layanan memiliki tanggung jawab sendiri dan berkomunikasi melalui jaringan Docker Compose. Keuntungan desain ini adalah loose coupling: publisher tidak perlu mengetahui detail penyimpanan PostgreSQL, sementara aggregator tidak perlu menunggu database selesai menulis sebelum menerima event berikutnya.

Trade-off utamanya adalah kompleksitas. Arsitektur Pub-Sub membutuhkan broker tambahan, konfigurasi jaringan, penanganan retry, dan observability yang lebih baik dibanding aplikasi monolitik sederhana. Namun, trade-off tersebut layak karena log aggregator harus tahan terhadap lonjakan event dan duplikasi. Redis membantu menyerap beban sementara, sedangkan PostgreSQL menjaga konsistensi akhir. Dengan demikian, desain ini mengorbankan kesederhanaan deployment untuk mendapatkan skalabilitas, isolasi tanggung jawab, dan ketahanan terhadap kegagalan parsial.

### Bab 2: Kapan Memilih Publish-Subscribe Dibanding Client-Server

Arsitektur publish-subscribe cocok digunakan ketika sistem memiliki banyak pengirim event dan pemrosesan event tidak harus selesai secara sinkron pada saat request diterima. Dalam pola client-server tradisional, client biasanya menunggu server menyelesaikan operasi tertentu sebelum menerima respons. Pada log aggregator, pola tersebut kurang ideal karena event dapat datang dalam jumlah besar dan penulisan database dapat menjadi bottleneck. Pola Pub-Sub memisahkan pengirim event dari pemroses event, sehingga publisher cukup mengirim pesan dan aggregator dapat mengolahnya secara bertahap.

Coulouris et al. (2012) menjelaskan bahwa arsitektur sistem terdistribusi perlu mempertimbangkan pemisahan peran, komunikasi antar proses, dan dependensi antar komponen. Dalam proyek ini, publisher tidak bergantung langsung pada PostgreSQL. Publisher hanya mengetahui endpoint aggregator. Aggregator kemudian meneruskan event ke Redis agar worker dapat memprosesnya di latar belakang. Hal ini mengurangi temporal coupling karena pengirim tidak perlu menunggu proses database selesai. Dengan desain ini, sistem lebih cocok untuk workload log, audit, dan telemetry yang bersifat event-driven.

### Bab 3: At-Least-Once, Exactly-Once, dan Peran Idempotent Consumer

Pada sistem terdistribusi, pengiriman pesan sering kali dirancang dengan jaminan at-least-once. Artinya, sistem berusaha memastikan pesan sampai, tetapi konsekuensinya pesan dapat diterima lebih dari satu kali akibat retry, timeout, atau ketidakpastian status pengiriman (Coulouris et al., 2012). Exactly-once delivery sulit dijamin secara absolut karena membutuhkan koordinasi kuat antar pengirim, broker, consumer, dan storage. Karena itu, pendekatan praktis yang digunakan adalah membangun consumer yang idempotent.

Dalam proyek ini, idempotent consumer berarti event dengan pasangan `topic` dan `event_id` yang sama hanya menghasilkan satu perubahan final di database. Jika publisher mengirim event yang sama tiga kali, worker boleh menerima tiga pesan, tetapi PostgreSQL hanya menyimpan satu baris. Hal ini dicapai dengan constraint `UNIQUE(topic, event_id)` dan query `INSERT ... ON CONFLICT DO NOTHING`. Dengan demikian, sistem dapat menerima pola at-least-once dari publisher tanpa menghasilkan pemrosesan ganda. Efek akhirnya mendekati exactly-once processing pada level storage, meskipun transport message tetap berpotensi mengirim duplikat.

### Bab 4: Skema Penamaan Topic dan Event ID untuk Deduplication

Penamaan dalam sistem terdistribusi penting karena komponen perlu mengidentifikasi resource, message, atau service secara konsisten meskipun berjalan pada lokasi berbeda (Coulouris et al., 2012). Pada proyek ini, identitas event dibentuk dari dua bagian: `topic` dan `event_id`. Field `topic` berfungsi sebagai namespace logis, misalnya `auth`, `payment`, `system`, atau `test_topic`. Field `event_id` berfungsi sebagai identitas unik untuk satu kejadian di dalam topic tersebut.

Kombinasi `(topic, event_id)` lebih aman dibanding hanya menggunakan `event_id` karena dua domain event berbeda dapat memiliki format ID yang sama tanpa dianggap konflik. Misalnya, event `payment:123` dan `auth:123` dapat dianggap berbeda karena berada pada topic berbeda. Untuk mengurangi risiko collision, `event_id` sebaiknya dibuat menggunakan UUID atau format lain yang collision-resistant. Di sisi database, kombinasi ini diterapkan sebagai unique constraint. Dengan cara tersebut, deduplication tidak bergantung pada pengecekan manual di aplikasi, tetapi ditegakkan langsung oleh storage layer yang memiliki mekanisme konkurensi lebih kuat.

### Bab 5: Ordering Praktis, Timestamp, dan Toleransi Event Out-of-Order

Masalah waktu dan ordering adalah salah satu isu penting dalam sistem terdistribusi karena setiap node dapat memiliki clock berbeda. Coulouris et al. (2012) membahas bahwa tidak adanya global clock membuat urutan kejadian tidak selalu dapat ditentukan hanya dari waktu fisik. Pada proyek ini, setiap event memiliki field `timestamp` dalam format ISO8601. Timestamp berguna untuk observability dan analisis urutan kejadian, tetapi sistem tidak menjadikannya sebagai sumber kebenaran tunggal untuk deduplication.

Desain ini sengaja tidak memaksakan total ordering global karena log aggregator lebih membutuhkan konsistensi deduplication daripada urutan mutlak semua event. Event boleh diproses sedikit terlambat atau out-of-order selama identitasnya tetap konsisten. Jika kebutuhan ordering lebih kuat muncul, sistem dapat menambahkan monotonic counter per publisher atau logical clock per topic. Namun, untuk scope proyek ini, ordering praktis menggunakan timestamp sudah cukup. Deduplication tetap aman karena bergantung pada `(topic, event_id)`, bukan pada urutan kedatangan atau nilai timestamp.

### Bab 6: Failure Modes, Retry, Backoff, dan Crash Recovery

Sistem terdistribusi harus siap menghadapi kegagalan parsial seperti crash failure, omission failure, timeout, dan komunikasi yang tidak pasti (Coulouris et al., 2012). Pada proyek ini, failure mode yang paling relevan adalah publisher mengirim ulang event karena retry, aggregator atau worker berhenti saat masih ada event, dan database container dihentikan lalu dijalankan kembali. Untuk menghadapi retry, sistem menerapkan idempotency sehingga event duplikat tidak menimbulkan pemrosesan ganda.

Worker juga memiliki pola sederhana untuk menangani error: jika terjadi exception saat memproses event, transaksi database di-rollback dan worker melakukan jeda sebelum mencoba lagi. Redis digunakan sebagai queue internal agar event tidak langsung bergantung pada kecepatan penulisan database. Untuk crash recovery, PostgreSQL menggunakan named volume `pg_data`. Volume ini menyimpan tabel `processed_events` dan `stats` di luar lifecycle container, sehingga state deduplication tetap ada setelah restart. Dengan demikian, event lama yang dikirim ulang setelah sistem hidup kembali tetap dapat dikenali sebagai duplikat.

### Bab 7: Eventual Consistency dan Peran Idempotency + Deduplication

Karena aggregator mengembalikan status `202 Accepted` setelah event masuk ke Redis, data belum tentu langsung tersedia di endpoint `/events`. Worker perlu mengambil pesan dari Redis dan menuliskannya ke PostgreSQL. Pola ini menunjukkan eventual consistency: state akhir sistem akan menjadi konsisten setelah proses asinkron selesai, tetapi ada jeda singkat antara penerimaan event dan persistensi di database. Dalam banyak sistem log, model ini dapat diterima karena tujuan utama adalah throughput dan ketahanan, bukan respons sinkron untuk setiap write.

Menurut Coulouris et al. (2012), konsistensi dalam sistem terdistribusi perlu dipahami sebagai trade-off terhadap ketersediaan dan performa. Proyek ini memilih konsistensi akhir dengan perlindungan idempotency. Jika event yang sama dikirim beberapa kali selama periode eventual tersebut, constraint database tetap memastikan hanya satu event unik yang tersimpan. Deduplication membuat state akhir stabil walaupun proses pengiriman dan pemrosesan tidak selalu terjadi sekali saja. Dengan demikian, eventual consistency tidak menyebabkan data ganda karena identitas event dijaga secara persisten.

### Bab 8: Desain Transaksi, ACID, Isolation Level, dan Lost Update

Transaksi digunakan untuk memastikan operasi yang saling berkaitan diperlakukan sebagai satu unit kerja. Coulouris et al. (2012) menjelaskan bahwa transaksi ACID membantu menjaga atomicity, consistency, isolation, dan durability pada operasi data. Pada proyek ini, worker melakukan update statistik dan insert event dalam sesi database yang sama. Jika insert atau update gagal, session melakukan rollback sehingga perubahan parsial tidak dibiarkan tersimpan.

Isolation level yang digunakan adalah default PostgreSQL, yaitu `READ COMMITTED`. Level ini cukup untuk kebutuhan proyek karena ancaman utama bukan dirty read, melainkan duplicate processing dan lost update. Duplicate processing dicegah oleh unique constraint pada `(topic, event_id)`. Lost update pada statistik dikurangi dengan menggunakan operasi SQL atomik seperti `UPDATE stats SET received = received + 1`, bukan pola read-modify-write di aplikasi. Dengan pendekatan tersebut, beberapa proses yang memperbarui counter tidak membaca nilai lama lalu menulis ulang secara manual. Database menangani kenaikan counter sebagai operasi mutasi. Desain ini menunjukkan bahwa transaksi dan operasi atomik harus dipakai bersama untuk menjaga konsistensi pada beban konkuren.

### Bab 9: Kontrol Konkurensi, Unique Constraint, dan Idempotent Upsert

Kontrol konkurensi diperlukan ketika beberapa worker atau request memproses data yang mungkin sama pada waktu berdekatan. Jika aplikasi hanya melakukan pengecekan manual seperti "cek apakah event sudah ada, lalu insert jika belum", race condition dapat terjadi. Dua worker dapat sama-sama membaca bahwa event belum ada, lalu keduanya mencoba menyimpan event yang sama. Pola tersebut tidak aman pada sistem konkuren.

Proyek ini menggunakan pendekatan yang lebih kuat: database-level unique constraint dan idempotent upsert. Query `INSERT ... ON CONFLICT (topic, event_id) DO NOTHING RETURNING id` menyerahkan penyelesaian konflik kepada PostgreSQL. Jika insert berhasil, worker menaikkan `unique_processed`. Jika tidak ada baris yang dikembalikan, worker menganggap event sebagai duplikat dan menaikkan `duplicate_dropped`. Menurut pembahasan Coulouris et al. (2012) tentang transaksi dan kontrol konkurensi, penyelesaian konflik harus dirancang agar tetap benar meskipun operasi terjadi paralel. Dalam proyek ini, correctness tidak bergantung pada timing aplikasi, tetapi pada constraint dan mekanisme konkurensi database. Ini membuat deduplication lebih aman dan sederhana.

### Bab 10-13: Keamanan Jaringan, Persistensi, Sistem Web, dan Koordinasi

Bab 10-13 berkaitan dengan aspek keamanan, penyimpanan, sistem berbasis web, dan koordinasi layanan. Pada proyek ini, keamanan jaringan diterapkan melalui Docker Compose network. Redis dan PostgreSQL tidak diekspos ke host secara publik; hanya aggregator yang membuka port `8080` untuk demo lokal. Dengan demikian, broker dan storage hanya dapat diakses oleh service internal Compose. Ini sesuai dengan ketentuan tugas bahwa sistem tidak menggunakan layanan eksternal publik.

Persistensi diterapkan dengan named volume `pg_data` untuk PostgreSQL dan `broker_data` untuk Redis. Volume memastikan data tidak hilang ketika container dihentikan atau dibuat ulang. Dari sisi sistem web, FastAPI menyediakan endpoint HTTP yang mudah diuji: `POST /publish`, `GET /events`, dan `GET /stats`. Dari sisi koordinasi, Docker Compose mengatur urutan service dengan `depends_on` dan healthcheck PostgreSQL. Observability tersedia melalui log container dan endpoint `/stats`. Coulouris et al. (2012) menekankan pentingnya koordinasi antar komponen dalam sistem terdistribusi; pada proyek ini, koordinasi dilakukan melalui Compose, Redis queue, dan state persisten PostgreSQL.

---

## 4. Implementasi Sistem

### 4.1 Arsitektur Layanan

| Layanan | Teknologi | Fungsi |
|---|---|---|
| `aggregator` | Python FastAPI | Menerima event, menyediakan endpoint, menjalankan worker |
| `publisher` | Python | Menghasilkan event dan duplikasi untuk simulasi at-least-once |
| `broker` | Redis 7 Alpine | Queue internal `event_queue` |
| `storage` | PostgreSQL 16 Alpine | Menyimpan event unik dan statistik |

### 4.2 Model Event

Format event mengikuti ketentuan:

```json
{
  "topic": "string",
  "event_id": "string-unik",
  "timestamp": "ISO8601",
  "source": "string",
  "payload": {}
}
```

Field `topic` dan `event_id` menjadi kunci deduplication. Field `timestamp` membantu observability dan ordering praktis. Field `source` mencatat asal event. Field `payload` dibuat fleksibel agar dapat menyimpan data log dengan struktur berbeda.

### 4.3 Endpoint API

| Endpoint | Metode | Fungsi |
|---|---|---|
| `/` | GET | Mengecek status API aggregator |
| `/publish` | POST | Menerima batch event dan memasukkannya ke Redis |
| `/events?topic=...&limit=...` | GET | Menampilkan event unik yang sudah diproses |
| `/stats` | GET | Menampilkan `received`, `unique_processed`, dan `duplicate_dropped` |

Endpoint `/publish` mengembalikan HTTP `202 Accepted`, yang menandakan event diterima untuk diproses secara asinkron.

---

## 5. Idempotency, Deduplication, dan Transaksi/Konkurensi

Deduplication diterapkan secara persisten pada tabel `processed_events`. Tabel ini memiliki constraint:

```sql
UNIQUE(topic, event_id)
```

Worker menggunakan query:

```sql
INSERT INTO processed_events (topic, event_id, timestamp, source, payload)
VALUES (:topic, :event_id, :timestamp, :source, :payload)
ON CONFLICT (topic, event_id) DO NOTHING
RETURNING id;
```

Jika query mengembalikan `id`, event dianggap unik. Jika tidak mengembalikan baris, event dianggap duplikat. Pola ini idempotent karena pemanggilan berulang dengan input yang sama tidak mengubah hasil akhir setelah event pertama tersimpan.

Untuk statistik, worker melakukan update counter menggunakan operasi SQL langsung:

```sql
UPDATE stats SET received = received + 1 WHERE id = 1;
UPDATE stats SET unique_processed = unique_processed + 1 WHERE id = 1;
UPDATE stats SET duplicate_dropped = duplicate_dropped + 1 WHERE id = 1;
```

Pendekatan ini membantu menghindari lost update karena counter dinaikkan di database, bukan dibaca ke aplikasi lalu ditulis kembali. Jika terjadi error saat proses, session melakukan rollback. Dengan kombinasi transaksi, unique constraint, dan atomic update, sistem tetap konsisten meskipun menerima event duplikat atau diproses secara konkuren.

---

## 6. Reliability, Ordering, dan Persistensi

Sistem mendukung skenario at-least-once delivery melalui publisher yang mengirim event duplikat. Event duplikat tidak merusak state akhir karena consumer bersifat idempotent. Ordering tidak dipaksakan sebagai total ordering global. Sistem menggunakan timestamp ISO8601 sebagai ordering praktis untuk observability, tetapi deduplication tidak bergantung pada timestamp.

Persistensi dicapai melalui Docker named volume:

| Volume | Fungsi |
|---|---|
| `pg_data` | Menyimpan data PostgreSQL, termasuk event dan statistik |
| `broker_data` | Menyediakan penyimpanan Redis sesuai konfigurasi container |

Skenario crash recovery yang ditunjukkan dalam demo adalah menjalankan sistem, mengamati `/stats`, menghentikan container dengan `docker compose down`, lalu menjalankan kembali `docker compose up`. Setelah restart, data lama tetap ada karena PostgreSQL menggunakan `pg_data`. Hal ini penting karena dedup store harus persisten; jika dedup store hilang, event lama dapat diproses ulang sebagai event baru.

---

## 7. Docker Compose dan Jaringan Lokal

Sistem dijalankan dengan:

```bash
docker compose up --build
```

Docker Compose menjalankan semua service pada jaringan lokal Compose. Port yang diekspos ke host hanya `8080` milik aggregator. Redis dan PostgreSQL tidak dipublikasikan ke host, sehingga hanya service internal yang dapat mengaksesnya. PostgreSQL memiliki healthcheck:

```yaml
test: ["CMD-SHELL", "pg_isready -U user -d db"]
```

Healthcheck ini membantu memastikan aggregator menunggu database siap sebelum melakukan koneksi awal. Desain jaringan ini memenuhi ketentuan bahwa sistem berjalan lokal dan tidak bergantung pada layanan eksternal publik.

---

## 8. Analisis Performa dan Metrik

Ketentuan proyek meminta sistem memproses minimal 20.000 event dengan minimal 30% duplikasi dan tetap responsif. Pengujian dilakukan pada stack Docker Compose lokal dengan service `publisher` dihentikan sementara agar metrik tidak bercampur dengan traffic background. Setelah pengujian selesai, `publisher` dijalankan kembali. Skenario uji menggunakan topic khusus `perf_20000_20260619100612`, total 20.000 event, 14.000 event unik, dan 6.000 event duplikat.

Metrik yang digunakan untuk menilai performa adalah:

| Metrik | Cara Mengukur | Makna |
|---|---|---|
| `received` | Endpoint `/stats` | Total event yang diambil worker dari queue |
| `unique_processed` | Endpoint `/stats` | Event unik yang berhasil disimpan |
| `duplicate_dropped` | Endpoint `/stats` | Event duplikat yang ditolak |
| Duplicate rate | `duplicate_dropped / received * 100%` | Proporsi duplikasi |
| Latency publish | Durasi request `POST /publish` | Responsivitas API |
| Throughput | Jumlah event per detik | Kapasitas pemrosesan |

Hasil uji performa:

| Item | Hasil |
|---|---:|
| Total event dikirim | 20.000 |
| Event unik yang diharapkan | 14.000 |
| Event duplikat yang diharapkan | 6.000 |
| Duplicate rate | 30% |
| Status response `POST /publish` | 202 Accepted |
| Latency pengiriman batch ke `/publish` | 6,406 detik |
| Waktu sampai 14.000 event unik tersedia di `/events` | 67,342 detik |
| Throughput pemrosesan | 296,99 event/detik |

Perubahan statistik sebelum dan sesudah uji performa:

| Metrik `/stats` | Sebelum | Sesudah | Delta |
|---|---:|---:|---:|
| `received` | 28.081 | 48.081 | 20.000 |
| `unique_processed` | 21.690 | 35.690 | 14.000 |
| `duplicate_dropped` | 6.391 | 12.391 | 6.000 |

Hasil tersebut menunjukkan bahwa sistem berhasil memproses 20.000 event dengan 30% duplikasi. Jumlah `unique_processed` bertambah tepat 14.000 dan `duplicate_dropped` bertambah tepat 6.000, sehingga deduplication bekerja sesuai ekspektasi. Endpoint `/publish` tetap merespons dengan status 202, meskipun batch berisi 20.000 event. Karena pemrosesan dilakukan asinkron melalui Redis dan worker, latency `POST /publish` merepresentasikan waktu penerimaan dan enqueue batch, sedangkan waktu 67,342 detik merepresentasikan waktu sampai seluruh event unik terlihat pada `/events`.

### 8.1 Hasil Uji Konkurensi

Uji konkurensi dilakukan dengan 20 worker client yang mengirim total 200 request secara paralel. Semua request membawa event yang sama, yaitu topic `concurrency_20260619100612` dan satu `event_id` identik. Tujuan uji ini adalah membuktikan bahwa race condition tidak menghasilkan penyimpanan ganda ketika banyak request duplikat masuk hampir bersamaan.

Hasil uji konkurensi:

| Item | Hasil |
|---|---:|
| Jumlah worker client | 20 |
| Total request konkuren | 200 |
| Status semua request | 202 Accepted |
| Baris unik pada `/events` untuk topic uji | 1 |
| Waktu konsumsi seluruh request | 1,428 detik |

Perubahan statistik pada uji konkurensi:

| Metrik `/stats` | Delta |
|---|---:|
| `received` | 200 |
| `unique_processed` | 1 |
| `duplicate_dropped` | 199 |

Hasil ini membuktikan bahwa 200 request konkuren dengan event yang sama hanya menghasilkan satu baris unik pada database. Sebanyak 199 event lain dikenali sebagai duplikat. Dengan demikian, kombinasi `UNIQUE(topic, event_id)` dan `INSERT ... ON CONFLICT DO NOTHING` berhasil mencegah double-process pada skenario race condition.

---

## 9. Unit/Integration Test

Pengujian dilakukan menggunakan `pytest`, `pytest-asyncio`, dan `httpx`. Test dijalankan saat Docker Compose aktif:

```bash
cd tests
python -m pytest test_api.py -v
```

Cakupan test yang sudah tersedia pada `tests/test_api.py`:

| Test | Tujuan |
|---|---|
| `test_1_root_endpoint` | Memastikan API aktif |
| `test_2_publish_valid_event` | Memastikan event valid diterima dengan status 202 |
| `test_3_publish_invalid_schema` | Memastikan validasi Pydantic menolak schema tidak lengkap |
| `test_4_deduplication_single_batch` | Membuktikan duplikasi dalam satu batch hanya tersimpan sekali |
| `test_5_deduplication_cross_batch` | Membuktikan duplikasi lintas batch hanya tersimpan sekali |
| `test_6_stats_increment` | Memastikan statistik bertambah sesuai unique dan duplicate event |
| `test_7_get_events_limit` | Memastikan parameter limit pada `/events` bekerja |
| `test_8_stress_batch` | Memastikan batch 100 event tetap responsif |

Ketentuan tugas meminta 12-20 test. Karena file test saat ini memuat 8 test utama, test suite perlu ditambah sebelum final submission. Test tambahan yang disarankan adalah persistensi setelah restart, concurrency/race condition, filter topic, format timestamp invalid, payload kosong/kompleks, batch campuran valid-invalid, dan stress 20.000 event.

---

## 10. Kesesuaian dengan Ketentuan Proyek

| Ketentuan | Status |
|---|---|
| Bahasa Indonesia dengan istilah teknis Inggris | Terpenuhi |
| Python atau Rust | Terpenuhi: Python |
| Docker Compose wajib | Terpenuhi |
| Multi-service: aggregator, publisher, broker, storage | Terpenuhi |
| Jaringan lokal Compose tanpa layanan eksternal publik | Terpenuhi |
| `POST /publish` batch event | Terpenuhi |
| `GET /events?topic=...` | Terpenuhi |
| `GET /stats` | Terpenuhi |
| Idempotency dan dedup persisten | Terpenuhi melalui PostgreSQL unique constraint |
| Transaksi dan kontrol konkurensi | Terpenuhi melalui transaction session, upsert, dan SQL atomic update |
| Persistensi setelah restart | Terpenuhi melalui `pg_data` |
| Unit/Integration test 12-20 | Perlu ditambah; saat ini 8 test utama |
| Performa 20.000 event dan 30% duplikasi | Terpenuhi: 20.000 event, 30% duplikasi, throughput 296,99 event/detik |
| Video demo minimal 25 menit | Panduan video tersedia di `pindah/panduan.md`; link video perlu ditambahkan setelah upload |
| Laporan dengan sitasi APA 7 | Terpenuhi |

---

## 11. Rencana Video Demo

Video demo wajib menampilkan:

1. Arsitektur multi-service dan alasan desain.
2. Proses build dan menjalankan `docker compose up --build`.
3. Pengiriman event duplikat oleh publisher.
4. Bukti idempotency dan deduplication melalui `/stats` dan `/events`.
5. Penjelasan transaksi, unique constraint, dan upsert.
6. Demonstrasi restart container dan bukti data tetap persisten.
7. Keamanan jaringan lokal: Redis dan PostgreSQL tidak diekspos ke publik.
8. Observability melalui logging dan endpoint `/stats`.
9. Integration test menggunakan pytest.

Panduan script video sudah disediakan pada `pindah/panduan.md`. Link video YouTube unlisted/public perlu dicantumkan di README atau laporan setelah proses upload.

---

## 12. Kesimpulan

Proyek Pub-Sub Log Aggregator ini menunjukkan penerapan konsep sistem terdistribusi melalui arsitektur multi-service, komunikasi asinkron, deduplication persisten, dan transaksi database. Redis digunakan sebagai broker internal untuk memisahkan penerimaan event dari proses penyimpanan. PostgreSQL digunakan sebagai durable store yang menjaga konsistensi melalui unique constraint dan upsert. Dengan pendekatan idempotent consumer, sistem tetap aman terhadap pengiriman event duplikat yang umum terjadi pada pola at-least-once delivery.

Dari sisi transaksi dan kontrol konkurensi, sistem menghindari race condition dengan menyerahkan penyelesaian konflik ke PostgreSQL. Dari sisi reliability, named volume memastikan data tetap tersedia setelah container restart. Dari sisi observability, endpoint `/stats` menyediakan metrik utama untuk memantau jumlah event diterima, event unik, dan event duplikat.

Secara umum, sistem telah memenuhi aspek utama tugas: Docker Compose, arsitektur multi-service, API event, deduplication, persistensi, transaksi, pengukuran performa, uji konkurensi, dan dokumentasi. Bagian yang masih perlu dilengkapi sebelum final submission adalah peningkatan test suite menjadi 12-20 test agar sesuai dengan ketentuan jumlah pengujian.

---

## 13. Daftar Pustaka

Coulouris, G., Dollimore, J., Kindberg, T., & Blair, G. (2012). *Distributed systems: Concepts and design* (5th ed.). Addison-Wesley.
