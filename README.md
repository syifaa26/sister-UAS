# Panduan Menjalankan Proyek UAS Sistem Terdistribusi

## Arsitektur
Proyek ini mengimplementasikan Pub-Sub Log Aggregator menggunakan Python (FastAPI), Redis (Message Broker), dan PostgreSQL (Persistent Storage).

*   **Aggregator**: Menerima request event secara asinkron, memindahkannya ke Redis, lalu worker berjalan di latar belakang untuk melakukan *upsert* ke PostgreSQL.
*   **Publisher**: Simulator yang terus menerus mengirimkan batch event dengan ±30% probabilitas data duplikat.
*   **Idempotency & Dedup**: Dijamin pada level database menggunakan kombinasi `UNIQUE(topic, event_id)` dan query `INSERT ... ON CONFLICT DO NOTHING`.

## Menjalankan Sistem
Pastikan Docker dan Docker Compose sudah terpasang.

1. Buka terminal di folder proyek ini (`D:\VSCODE\SISTER\UAS`).
2. Jalankan perintah build dan up:
   ```bash
   docker compose up --build
   ```
3. Docker akan mengunduh image, membangun Aggregator dan Publisher, dan menjalankan Postgres serta Redis.

## Cara Mengakses API
Setelah kontainer berjalan, Anda bisa mengakses endpoint berikut melalui browser atau Postman:
*   **Akar**: [http://localhost:8080/](http://localhost:8080/)
*   **Lihat Event**: [http://localhost:8080/events](http://localhost:8080/events)
*   **Lihat Statistik (Bukti Dedup)**: [http://localhost:8080/stats](http://localhost:8080/stats)

## Menjalankan Test (Integration Testing)
Sistem dilengkapi dengan sekumpulan *Integration Test* berbasis `pytest` dan `httpx`. Tes ini **harus dijalankan saat Docker Compose sedang menyala**.

1. Buka terminal baru (biarkan terminal Docker Compose tetap jalan).
2. Pindah ke direktori `tests/`:
   ```bash
   cd tests
   ```
3. Install requirements lokal:
   ```bash
   pip install -r requirements.txt
   ```
4. Jalankan pytest:
   ```bash
   pytest test_api.py -v
   ```
   *Output akan menunjukkan semua skenario pengujian seperti validasi skema, deduplikasi, dan perhitungan metrik.*

## Bukti Persistensi Data (Untuk Demo)
Untuk membuktikan bahwa data aman meski container mati:
1. Jalankan `docker compose down`.
2. Jalankan kembali `docker compose up`.
3. Akses [http://localhost:8080/stats](http://localhost:8080/stats) — Anda akan melihat bahwa angka statistik dan event lama masih tersimpan karena kita menggunakan *named volumes* (`pg_data`).
