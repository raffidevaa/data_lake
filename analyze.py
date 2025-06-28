# analyze.py
import pandas as pd
import os
import re
import csv 
import pdfplumber
import camelot

from collections import defaultdict, Counter

# Daftar stopwords umum dalam bahasa Inggris
STOPWORDS = {
    "the", "a", "an", "is", "are", "was", "were", "and", "or", "of", "to", "in", "for", "on", "with", "as", "at",
    "by", "from", "that", "this", "it", "be", "has", "had", "have", "not", "but", "they", "i", "you", "we", "he",
    "she", "them", "his", "her", "its", "our", "their", "if", "so", "do", "does", "did", "been", "will", "can"
}

def clean_word(word):
    return re.sub(r"[^\w]", "", word.lower())

def analyze_txt(input_folder, output_folder="structured/txt"):
    os.makedirs(output_folder, exist_ok=True)
    word_counter = Counter()
    all_rows = []

    for filename in os.listdir(input_folder):
        if filename.endswith(".txt"):
            file_path = os.path.join(input_folder, filename)
            with open(file_path, "r", encoding="utf-8") as file:
                for line in file:
                    parts = line.strip().split(" | ")
                    if len(parts) == 3:
                        username, tweet, date = parts
                        all_rows.append([username, tweet, date])
                        words = [clean_word(w) for w in tweet.split()]
                        filtered_words = [w for w in words if w and w not in STOPWORDS]
                        word_counter.update(filtered_words)

    # Simpan structured_tweets.csv (optional, tetap disimpan)
    # raw_output_file = os.path.join(output_folder, "structured_tweets.csv")
    # with open(raw_output_file, "w", newline='', encoding="utf-8") as f:
    #     writer = csv.writer(f)
    #     writer.writerow(["username", "tweet", "date"])
    #     writer.writerows(all_rows)

    # Simpan word frequency per tanggal
    word_freq_output = os.path.join(output_folder, "words_freq.csv")
    with open(word_freq_output, "w", newline='', encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["word", "frequency"])
        for word, freq in word_counter.items():
            writer.writerow([word, freq])

    # Gabungkan semua untuk ditampilkan di terminal
    # overall_counter = Counter()
    # for counter in word_date_counter.values():
    #     overall_counter.update(counter)

    # return dict(overall_counter.most_common())

def analyze_csv(input_folder, output_folder="structured/csv"):
    os.makedirs(output_folder, exist_ok=True)
    all_results = []

    for filename in os.listdir(input_folder):
        if filename.endswith(".csv"):
            file_path = os.path.join(input_folder, filename)
            df = pd.read_csv(file_path)

            # Pastikan kolom timestamp menjadi datetime
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            df['date'] = df['timestamp'].dt.date

            # Agregasi suhu per hari
            daily_summary = df.groupby('date')['value'].agg(['min', 'max', 'mean']).reset_index()
            daily_summary.rename(columns={'min': 'min_temp', 'max': 'max_temp', 'mean': 'avg_temp'}, inplace=True)

            # Simpan hasil ke structured/csv
            output_file = os.path.join(output_folder, f'structured_{filename}')
            daily_summary.to_csv(output_file, index=False)

            all_results.append(daily_summary)

    # Gabungkan seluruh file (jika ada banyak file .csv di folder)
    # if all_results:
    #     final_df = pd.concat(all_results)
    #     print(final_df)
    #     return final_df
    # else:
    #     print("[INFO] Tidak ditemukan file CSV yang valid.")
    #     return pd.DataFrame()


# pdf menggunakan data dummy
# def analyze_pdf(input_folder, output_folder="structured/pdf"):
#     os.makedirs(output_folder, exist_ok=True)
#     structured_data = []

#     for filename in os.listdir(input_folder):
#         if filename.endswith(".pdf"):
#             file_path = os.path.join(input_folder, filename)

#             with pdfplumber.open(file_path) as pdf:
#                 text = ""
#                 for page in pdf.pages:
#                     text += page.extract_text() + "\n"

#                 # Ekstrak Company Name
#                 company_match = re.search(r"Company Name:\s*(.*)", text)
#                 company_name = company_match.group(1).strip() if company_match else None

#                 # Ekstrak Report Date
#                 date_match = re.search(r"Report Date:\s*([\d\-]+)", text)
#                 report_date = date_match.group(1).strip() if date_match else None

#                 # Ekstrak Revenue
#                 revenue_match = re.search(r"Total Revenue:\s*\$([\d,\.]+)", text)
#                 if revenue_match:
#                     revenue_str = revenue_match.group(1).replace(",", "")
#                     revenue = float(revenue_str)
#                 else:
#                     revenue = None

#                 if company_name and report_date and revenue:
#                     structured_data.append([company_name, report_date, revenue])
#                 else:
#                     print(f"[WARNING] Gagal ekstrak data lengkap dari file: {filename}")

#     # Simpan hasil ke CSV
#     output_file = os.path.join(output_folder, "structured_revenue.csv")
#     with open(output_file, "w", newline='', encoding="utf-8") as csvfile:
#         writer = csv.writer(csvfile)
#         writer.writerow(["company_name", "report_date", "revenue"])
#         writer.writerows(structured_data)

#     print(f"[INFO] Analyze PDF selesai. {len(structured_data)} record berhasil diproses.")


#pdf menggunakan data asli PT Sepeda Bersama Indonesia Tbk
def analyze_pdf(input_folder="raw/pdf", output_folder="structured/pdf"):
    company_name="PT Sepeda Bersama Indonesia Tbk"
    os.makedirs(output_folder, exist_ok=True)
    structured_data = []

    for filename in os.listdir(input_folder):
        if not filename.endswith(".pdf"):
            continue

        file_path = os.path.join(input_folder, filename)
        print(f"[INFO] Memproses file: {filename}")

        try:
            tables = camelot.read_pdf(file_path, pages="7", flavor="lattice", strip_text="\n")
        except Exception as e:
            print(f"[ERROR] Gagal membaca tabel dari {filename}: {e}")
            continue

        if tables.n == 0:
            print(f"[WARNING] Tidak ada tabel ditemukan di halaman 7: {filename}")
            continue

        for table in tables:
            df = table.df
            for _, row in df.iterrows():
                desc = str(row[0]).lower().replace(" ", "")
                if "lababruto" in desc or "grossprofit" in desc:
                    for year, col in zip([2024, 2023, 2022], [1, 2, 3]):
                        val = row[col].replace(".", "").replace(",", ".").replace("(", "-").replace(")", "")
                        try:
                            value = float(val)
                            structured_data.append([company_name, f"{year}-12-31", value])
                        except:
                            continue
                    break  # keluar setelah gross profit ditemukan

    # Simpan ke CSV
    output_file = os.path.join(output_folder, "structured_revenue.csv")
    with open(output_file, "w", newline='', encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["company_name", "report_date", "revenue"])
        writer.writerows(structured_data)

    print(f"[INFO] Analyze PDF selesai. {len(structured_data)} record berhasil diproses.")