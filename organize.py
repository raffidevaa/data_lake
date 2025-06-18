import os
import shutil

def organize_files_by_type(source_dir: str, raw_dir: str):
    """
    Memindahkan file dari source_dir ke dalam folder berdasarkan ekstensi file di dalam raw_dir/.
    raw_dir sekarang langsung berada di root folder, bukan dalam data_lake/.
    """

    # Mapping ekstensi ke subfolder
    file_type_mapping = {
        '.txt': 'txt',
        '.csv': 'csv',
        '.pdf': 'pdf'
    }

    # Validasi folder
    # missing_folders = []
    # for folder in file_type_mapping.values():
    #     folder_path = os.path.join(raw_dir, folder)
    #     if not os.path.exists(folder_path):
    #         missing_folders.append(folder_path)

    # if missing_folders:
    #     print("[ERROR] Folder berikut belum tersedia:")
    #     for folder in missing_folders:
    #         print(f"  - {folder}")
    #     print("Silakan buat folder yang dibutuhkan sebelum menjalankan script ini.")
    #     return

    # Pindahkan file ke folder yang sesuai
    for filename in os.listdir(source_dir):
        filepath = os.path.join(source_dir, filename)
        if os.path.isfile(filepath):
            ext = os.path.splitext(filename)[1].lower()
            if ext in file_type_mapping:
                target_folder = os.path.join(raw_dir, file_type_mapping[ext])
                shutil.move(filepath, os.path.join(target_folder, filename))
                print(f"[INFO] Moved '{filename}' → {target_folder}")
            else:
                print(f"[WARN] Skipped '{filename}' (unknown extension: {ext})")
