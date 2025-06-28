import os
import pdfplumber
import csv
import re

import camelot

# def extract_financial_table_with_camelot(pdf_path, output_folder="structured/pdf", target_page="7"):
#     os.makedirs(output_folder, exist_ok=True)

#     print(f"[INFO] Memproses file: {pdf_path}, halaman: {target_page}")
#     tables = camelot.read_pdf(pdf_path, pages=target_page, strip_text="\n", flavor="lattice")

#     if tables.n == 0:
#         print("[WARNING] Tidak ada tabel ditemukan di halaman tersebut.")
#         return

#     output_file = os.path.join(output_folder, "financial_statement_raw.csv")
#     tables[0].to_csv(output_file)

#     print(f"[SUCCESS] {tables.n} tabel ditemukan.")
#     print(f"[INFO] Tabel pertama disimpan ke: {output_file}")

#     # Opsional: print isi singkat
#     df = tables[0].df
#     print("\n[PREVIEW]")
#     print(df.head())

# if __name__ == "__main__":
#     pdf_path = "source_files/AR-SBI-2024-27-Maret-25-v.1.pdf"
#     extract_financial_table_with_camelot(pdf_path)

######

# with pdfplumber.open("source_files/AR-SBI-2024-27-Maret-25-v.1.pdf") as pdf:
#     page = pdf.pages[12]  # coba halaman tempat tabel laba rugi muncul
#     print(page.extract_text())

#######
def extract_gross_profit(pdf_path, output_path="structured/pdf/structured_revenue.csv", company_name="PT Sepeda Bersama Indonesia Tbk"):
    tables = camelot.read_pdf(pdf_path, pages="7", flavor="lattice", strip_text="\n")
    extracted = []

    for table in tables:
        df = table.df
        for _, row in df.iterrows():
            description = str(row[0]).lower().replace(" ", "")
            if "lababruto" in description or "grossprofit" in description:
                for year, col in zip([2024, 2023, 2022], [1, 2, 3]):
                    value_str = row[col].replace(".", "").replace(",", ".").replace("(", "-").replace(")", "")
                    try:
                        value = float(value_str)
                        report_date = f"{year}-12-31"
                        extracted.append([company_name, report_date, value])
                    except:
                        continue
                break  # berhenti jika sudah ketemu barisnya

    # Simpan hasil ke CSV
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["company_name", "report_date", "revenue"])
        writer.writerows(extracted)

    print(f"[INFO] Gross profit berhasil diekstrak ke: {output_path}")

if __name__ == "__main__":
    extract_gross_profit("source_files/AR-SBI-2024-27-Maret-25-v.1.pdf")
