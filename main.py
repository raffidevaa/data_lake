# main.py

from organize import organize_files_by_type
from analyze import analyze_txt, analyze_csv, analyze_pdf
from passtostg import load_txt_to_staging, load_csv_to_staging, load_pdf_to_staging
from transform import transform_txt, transform_csv, transform_pdf
from passtodw import passtodw

def main():
    source_folder = 'source_files'
    raw_folder = 'raw'

    print("[INFO] Organizing files...")
    organize_files_by_type(source_folder, raw_folder)

    # print("[INFO] Analyzing files...")
    # analyze_txt('raw/txt')
    # analyze_csv('raw/csv')
    # analyze_pdf('raw/pdf')


    # print("[INFO] Loading structured data to staging...")
    # load_txt_to_staging()
    # load_csv_to_staging()
    # load_pdf_to_staging()

    # print("[INFO] Transform data into star schema")
    # transform_txt()
    # transform_csv()
    # transform_pdf()

    
    # print("[INFO] Loading data to data warehouse")
    # passtodw()

if __name__ == "__main__":
    main()
