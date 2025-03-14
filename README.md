# Data-Sync-Cron-Job
Automated scripts to extract, transform, and load data from PostgreSQL and MySQL, and perform data cleansing. This process is run periodically using cron to ensure efficient data synchronization between databases. Comes with Google Chat notifications to monitor the status of the process.

This script connects to PostgreSQL and MySQL databases to extract, transform, and load data. The process includes:
- Extracting data from PostgreSQL and MySQL.
- Transforming data by adding columns and manipulating data.
- Loading data into PostgreSQL with upsert operations.
- Comparing and deleting unnecessary data at the destination.
- Sending Google Chat notifications regarding the process status.
- Using batch processing for efficiency.
- Automating the schedule with cron to run the process periodically.

-------------------------------------------------------------------------------------------------------------------------

Script otomatis untuk mengekstrak, mentransformasi, dan memuat data dari PostgreSQL dan MySQL, serta melakukan pembersihan data. Proses ini dijalankan secara berkala menggunakan cron untuk memastikan sinkronisasi data yang efisien antar database. Dilengkapi dengan notifikasi Google Chat untuk memantau status proses.

Script ini menghubungkan ke database PostgreSQL dan MySQL untuk mengekstrak, mentransformasi, dan memuat data. Prosesnya meliputi:
- Mengekstrak data dari PostgreSQL dan MySQL.
- Mentrasformasikan data dengan menambah kolom dan manipulasi data.
- Memuat data ke PostgreSQL dengan operasi upsert.
- Membandingkan dan menghapus data yang tidak diperlukan di tujuan.
- Mengirim notifikasi Google Chat terkait status proses.
- Menggunakan pemrosesan batch untuk efisiensi.
- Jadwal otomatis menggunakan cron untuk menjalankan proses ini secara berkala.
