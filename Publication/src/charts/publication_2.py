import json
import os
from collections import defaultdict

# Fungsi untuk memproses data jurnal dan menghasilkan informasi yang dibutuhkan
def process_journal(data):
    # Mengumpulkan data tanggal publikasi
    publication_years = defaultdict(int)

    for article in data['articles']:
        if 'date' in article:
            print(article['date'])
            year = int(article['date'].split('-')[0])
            publication_years[year] += 1

    # Konversi ke list yang berisi dictionary dengan format yang diminta
    years = sorted(publication_years.keys())
    processed_data = []

    for i, year in enumerate(years):
        current_year_count = publication_years[year]
        previous_year_count = publication_years[years[i-1]] if i > 0 else 0
        processed_data.append({
            "year": year,
            "tahun_sekarang": current_year_count,
            "tahun_sebelumnya": previous_year_count
        })
    
    # Membuat dictionary hasil akhir untuk jurnal ini
    return {
        "jid": data["jid"],
        "eissn": data["eissn"],
        "data": processed_data
    }

# Folder yang berisi file JSON
folder_path = 'journals_v4'  # Ubah path ini ke lokasi folder Anda

# Mengumpulkan semua file JSON dalam folder
json_files = [pos_json for pos_json in os.listdir(folder_path) if pos_json.endswith('.json')]

# Memproses setiap file JSON dalam folder
output_data = []
for json_file in json_files:
    with open(os.path.join(folder_path, json_file), 'r') as file:
        try:
            journal_data = json.load(file)
            # Mengecek apakah 'articles' ada dalam JSON
            if 'articles' in journal_data:
                output_data.append(process_journal(journal_data))
            else:
                print(f"Kunci 'articles' tidak ditemukan dalam file: {json_file}")
        except json.JSONDecodeError:
            print(f"Error dalam membaca file JSON: {json_file}")

# Menyimpan hasil ke file JSON
with open('year_now_vs_year_before.json', 'w') as outfile:
    json.dump(output_data, outfile, indent=4)

print("File JSON telah disimpan sebagai 'output_journals_data.json'.")