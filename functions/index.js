const functions = require('firebase-functions');
const admin = require('firebase-admin');
const { Storage } = require('@google-cloud/storage');
const csv = require('csv-parser');

// Inisialisasi Firebase Admin SDK
admin.initializeApp();
const db = admin.firestore();
const storage = new Storage();

// Nama bucket Firebase Storage Anda (biasanya project-id.appspot.com)
// functions.config().firebase.storageBucket akan otomatis terisi saat deploy
const BUCKET_NAME = functions.config().firebase.storageBucket; 

/**
 * Cloud Function yang terpicu saat file CSV diunggah ke Firebase Storage.
 * File harus diunggah ke folder 'admin-imports/'.
 */
exports.processGraduationStatusCsv = functions.storage.bucket(BUCKET_NAME).object().onFinalize(async (object) => {
    const fileBucket = object.bucket; // The Storage bucket that contains the file.
    const filePath = object.name;     // File path in the bucket.
    const contentType = object.contentType; // File content type.

    // Pastikan ini adalah file CSV dan berada di folder 'admin-imports/'
    if (!filePath.startsWith('admin-imports/') || !contentType.startsWith('text/csv')) {
        console.log('File bukan CSV atau tidak berada di folder admin-imports/. Melewatkan.');
        return null;
    }

    // Dapatkan referensi ke file yang baru diunggah
    const bucket = storage.bucket(fileBucket);
    const file = bucket.file(filePath);

    console.log(`Memulai pemrosesan file: ${filePath}`);

    const results = [];
    let successCount = 0;
    let failCount = 0;
    const batch = db.batch(); // Gunakan batch untuk penulisan efisien

    try {
        // Baca file CSV baris per baris
        await new Promise((resolve, reject) => {
            file.createReadStream()
                .pipe(csv())
                .on('data', (data) => results.push(data))
                .on('end', resolve)
                .on('error', reject);
        });

        if (results.length === 0) {
            console.log('File CSV kosong atau tidak ada data yang valid.');
            // Hapus file jika kosong
            await file.delete();
            console.log(`File ${filePath} (kosong) berhasil dihapus.`);
            return null;
        }

        console.log(`Ditemukan ${results.length} baris data di CSV.`);

        for (const row of results) {
            const nama = row['Nama'] ? String(row['Nama']).trim() : '';
            const nrp = row['NRP'] ? String(row['NRP']).trim() : '';
            const status = row['Status'] ? String(row['Status']).trim() : '';

            // Validasi dasar data dari CSV
            if (!nama || !nrp || !status) {
                console.warn(`Baris dilewati (data tidak lengkap): Nama: "${nama}", NRP: "${nrp}", Status: "${status}"`);
                failCount++;
                continue;
            }

            // Validasi format NRP (harus 10 digit angka)
            if (!/^[0-9]{10}$/.test(nrp)) {
                console.warn(`Baris dilewati (NRP tidak valid): ${nrp}`);
                failCount++;
                continue;
            }

            // Validasi status yang diizinkan
            const validStatuses = ['Lulus', 'Tidak Lulus', 'Menunggu'];
            if (!validStatuses.includes(status)) {
                console.warn(`Baris dilewati (Status tidak valid): ${status} untuk NRP ${nrp}`);
                failCount++;
                continue;
            }

            try {
                // Cari dokumen pendaftaran berdasarkan NRP
                const querySnapshot = await db.collection('pendaftaran')
                                            .where('nrp', '==', nrp)
                                            .limit(1)
                                            .get();

                if (!querySnapshot.empty) {
                    const docRef = querySnapshot.docs[0].ref;
                    // Perbarui status kelulusan dan nama (jika ada perubahan)
                    batch.update(docRef, {
                        statusKelulusan: status,
                        name: nama // Update nama jika ada koreksi di CSV
                    });
                    successCount++;
                } else {
                    console.warn(`NRP ${nrp} tidak ditemukan di koleksi 'pendaftaran'. Data tidak diupdate.`);
                    failCount++;
                }
            } catch (firestoreError) {
                console.error(`Gagal memperbarui Firestore untuk NRP ${nrp}:`, firestoreError);
                failCount++;
            }
        }

        // Commit semua operasi batch ke Firestore
        if (successCount > 0 || failCount > 0) { // Commit even if only failures to log the batch result
            await batch.commit();
            console.log(`Impor selesai! ${successCount} data berhasil diupdate, ${failCount} data gagal/dilewati.`);
        } else {
            console.log(`Tidak ada data yang berhasil diupdate atau gagal.`);
        }

        // Hapus file CSV setelah diproses (opsional, untuk menjaga kebersihan bucket)
        await file.delete();
        console.log(`File ${filePath} berhasil dihapus setelah diproses.`);

        return { success: true, updated: successCount, failed: failCount };

    } catch (error) {
        console.error(`Gagal memproses file ${filePath}:`, error);
        // Jika terjadi error, jangan hapus file agar bisa diinvestigasi
        return { success: false, error: error.message };
    }
});
