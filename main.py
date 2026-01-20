import pandas as pd
import numpy as np
from datetime import datetime
import pytz
import os
from pathlib import Path

#EXTRACT 
def extract_csv_to_dataframe(file_path, delete_index=False):
    try:
        if delete_index:
            df = pd.read_csv(file_path, index_col=0)
        else:
            df = pd.read_csv(file_path)
        print(f" Berhasil membaca file dari: {file_path}")
        return df
    except FileNotFoundError:
        print(f" Error: File tidak ditemukan di path: {file_path}")
        return None
    except Exception as e:
        print(f" Error saat membaca file: {e}")
        return None

#Perubahan TIMEZONE
def transform_timezone_to_jakarta(df, date_column):
    if date_column not in df.columns:
        print(f" Kolom '{date_column}' tidak ditemukan dalam DataFrame")
        return df
    
    try:
        df[date_column] = pd.to_datetime(df[date_column], errors='coerce')
        
        # Identifikasi baris dengan tanggal valid
        valid_dates = df[date_column].notna()
        if not valid_dates.any():
            print(f" Tidak ada tanggal valid di kolom '{date_column}'")
            return df
        
        # Jika kolom sudah memiliki timezone, konversi ke 'Asia/Jakarta'
        if df[date_column].dt.tz is not None:
            df.loc[valid_dates, date_column] = df.loc[valid_dates, date_column].dt.tz_convert('Asia/Jakarta')
        else:
            # Asumsikan waktu dalam UTC
            df.loc[valid_dates, date_column] = df.loc[valid_dates, date_column].dt.tz_localize('UTC').dt.tz_convert('Asia/Jakarta')
        
        print(f" Kolom '{date_column}' berhasil dikonversi ke timezone Asia/Jakarta")
        return df
    except Exception as e:
        print(f" Error saat mengkonversi timezone: {e}")
        return df

def transform_currency_to_float(df, currency_column):
    if currency_column not in df.columns:
        print(f" Kolom '{currency_column}' tidak ditemukan dalam DataFrame")
        return df
    
    try:
        # Backup original untuk perbandingan
        original_sample = df[currency_column].head(3).tolist()
        
        # Hapus simbol mata uang dan karakter non-numerik (kecuali titik desimal)
        df[currency_column] = df[currency_column].astype(str).replace('[\$,]', '', regex=True)
        
        # Konversi ke float
        df[currency_column] = pd.to_numeric(df[currency_column], errors='coerce')
        
        print(f" Kolom '{currency_column}' berhasil dikonversi ke float")
        print(f" Contoh perubahan: {original_sample} -> {df[currency_column].head(3).tolist()}")
        return df
    except Exception as e:
        print(f" Error saat mengkonversi mata uang: {e}")
        return df

#DEMOGRAFI
def data_demography(df, file_name=""):

    if df is None or df.empty:
        print(" DataFrame kosong atau tidak valid")
        return None
    
    demography = {}
    
    # Informasi file
    if file_name:
        demography['nama_file'] = file_name
    
    # 1. Jumlah baris dan kolom
    demography['jumlah_baris'] = df.shape[0]
    demography['jumlah_kolom'] = df.shape[1]
    
    # 2. Daftar kolom dan tipe data
    demography['kolom_dan_tipe'] = df.dtypes.astype(str).to_dict()
    
    # 3. Jumlah data missing per kolom
    demography['data_missing'] = df.isnull().sum().to_dict()
    
    # 4. Persentase data missing per kolom
    demography['persentase_missing'] = (df.isnull().sum() / len(df) * 100).round(2).to_dict()
    
    # 5. Statistik deskriptif untuk kolom numerik
    numeric_cols = df.select_dtypes(include=[np.number]).columns
    if len(numeric_cols) > 0:
        demography['statistik_numerik'] = df[numeric_cols].describe().to_dict()
    else:
        demography['statistik_numerik'] = "Tidak ada kolom numerik"
    
    # 6. Jumlah unique value untuk kolom kategorikal (object)
    categorical_cols = df.select_dtypes(include=['object']).columns
    demography['unique_values_kategorikal'] = {col: df[col].nunique() for col in categorical_cols}
    
    # 7. Frekuensi nilai untuk setiap kolom kategorikal (top 5)
    demography['frekuensi_kategorikal'] = {}
    for col in categorical_cols:
        if df[col].nunique() > 0:
            demography['frekuensi_kategorikal'][col] = df[col].value_counts().head().to_dict()
        else:
            demography['frekuensi_kategorikal'][col] = "Kolom kosong"
    
    return demography

def process_single_file(file_path, delete_index, file_number=1, total_files=1):
    """Proses satu file CSV"""
    print(f"FILE {file_number}/{total_files}: {os.path.basename(file_path)}")
    
    # Extract data
    df = extract_csv_to_dataframe(file_path, delete_index)
    
    if df is None:
        return None, None
    
    file_name = os.path.basename(file_path)
    
    print(f" Data berhasil diekstrak:")
    print(f"   - Dimensi: {df.shape[0]} baris x {df.shape[1]} kolom")
    print(f"   - Kolom: {list(df.columns)}")
    
    # Tampilkan preview data
    preview = input(f"\nTampilkan preview data? (y/n): ").strip().lower()
    if preview == 'y':
        rows = input("Jumlah baris yang ditampilkan ( Rekomendasi 5): ").strip()
        rows = int(rows) if rows.isdigit() else 5
        print(f"\nPreview data ({rows} baris pertama):")
        print(df.head(rows))
    
    # Transformasi data
    print(f"TRANSFORMASI DATA")
    
    # Transformasi tanggal
    date_transform = input(f"\nApakah ingin mengubah kolom tanggal ke timezone Asia/Jakarta? (y/n): ").strip().lower()
    if date_transform == 'y':
        print(f"Kolom yang tersedia: {list(df.columns)}")
        date_column = input("Masukkan nama kolom tanggal: ").strip()
        if date_column in df.columns:
            df = transform_timezone_to_jakarta(df, date_column)
        else:
            print(f" Kolom '{date_column}' tidak ditemukan")
    
    # Transformasi mata uang
    currency_transform = input(f"\nApakah ingin mengubah kolom mata uang ke float? (y/n): ").strip().lower()
    if currency_transform == 'y':
        print(f"Kolom yang tersedia: {list(df.columns)}")
        currency_column = input("Masukkan nama kolom mata uang: ").strip()
        if currency_column in df.columns:
            df = transform_currency_to_float(df, currency_column)
        else:
            print(f" Kolom '{currency_column}' tidak ditemukan")
    
    # Analisis demografi
    print(f"ANALISIS DEMOGRAFI DATA")
    
    demography = data_demography(df, file_name)
    
    if demography:
        print(f"\n JUMLAH DATA:")
        print(f"   Baris: {demography['jumlah_baris']:,}")
        print(f"   Kolom: {demography['jumlah_kolom']}")
        
        missing_cols = {col: missing for col, missing in demography['data_missing'].items() 
                       if missing > 0}
        if missing_cols:
            print(f"\n DATA KURANG:")
            for col, missing in missing_cols.items():
                percentage = demography['persentase_missing'][col]
                print(f"   {col}: {missing:,} ({percentage}%)")
        else:
            print(f"\nTidak ada data yang kurang")
        
        print(f"\n TIPE DATA:")
        for col, dtype in list(demography['kolom_dan_tipe'].items())[:10]:  # Tampilkan maksimal 10 kolom
            print(f"   {col}: {dtype}")
        if len(demography['kolom_dan_tipe']) > 10:
            print(f"   ... dan {len(demography['kolom_dan_tipe']) - 10} kolom lainnya")
    
    return df, demography

# ==================== FUNGSI UTAMA ====================
def main():
    print("ANALISIS DATA CSV - MODE FOLDER")
    
    # 1. Input path folder
    while True:
        folder_path = input("\n Masukkan path folder yang berisi file CSV: ").strip()
        
        # Coba beberapa format path
        if not os.path.isdir(folder_path):
            # Coba sebagai path relatif
            if os.path.isdir(f"./{folder_path}"):
                folder_path = f"./{folder_path}"
            elif os.path.isdir(f"./{folder_path}/"):
                folder_path = f"./{folder_path}/"
            else:
                print(f"  Folder '{folder_path}' tidak ditemukan")
                print("  Tips: Gunakan path lengkap seperti 'C:/Data/CSV' atau './data'")
                continue
        
        break
    
    # 2. Cari semua file CSV dalam folder
    csv_files = []
    for root, dirs, files in os.walk(folder_path):
        for file in files:
            if file.lower().endswith('.csv'):
                full_path = os.path.join(root, file)
                csv_files.append(full_path)
    
    if not csv_files:
        print(f"  Tidak ditemukan file CSV dalam folder '{folder_path}'")
        print(" Pastikan folder berisi file dengan ekstensi .csv")
        return
    
    # Urutkan file berdasarkan nama
    csv_files.sort()
    
    print(f"\n Ditemukan {len(csv_files)} file CSV:")
    for i, file_path in enumerate(csv_files, 1):
        file_name = os.path.basename(file_path)
        file_size = os.path.getsize(file_path)
        print(f"  {i:2d}. {file_name} ({file_size:,} bytes)")
    
    # 3. Konfigurasi pemrosesan
    print("KONFIGURASI PEMROSESAN")
    
    print("\nPilih file yang akan diproses:")
    print("1. Semua file")
    print("2. Pilih file tertentu")
    
    file_selection = input("Pilih opsi (1/2): ").strip()
    
    selected_files = []
    
    if file_selection == "1":
        selected_files = csv_files
        print(f"\n✓ Akan memproses semua {len(csv_files)} file")
    elif file_selection == "2":
        print("\nMasukkan nomor file yang akan diproses (pisahkan dengan koma):")
        print("Contoh: 1,3,5 atau 1-3 untuk range")
        
        selection_input = input("Pilihan: ").strip()
        
        selected_indices = set()
        for part in selection_input.split(','):
            part = part.strip()
            if '-' in part:
                start, end = part.split('-')
                try:
                    start_idx = int(start) - 1
                    end_idx = int(end) - 1
                    if 0 <= start_idx < len(csv_files) and 0 <= end_idx < len(csv_files):
                        for i in range(min(start_idx, end_idx), max(start_idx, end_idx) + 1):
                            selected_indices.add(i)
                except:
                    pass
            else:
                try:
                    idx = int(part) - 1
                    if 0 <= idx < len(csv_files):
                        selected_indices.add(idx)
                except:
                    pass
        
        if not selected_indices:
            print(" Tidak ada file yang dipilih")
            return
        
        selected_files = [csv_files[i] for i in sorted(selected_indices)]
        print(f"\n✓ Akan memproses {len(selected_files)} file:")
        for i, file_path in enumerate(selected_files, 1):
            print(f"  {i}. {os.path.basename(file_path)}")
    else:
        print(" Pilihan tidak valid")
        return
    
    # 4. Konfigurasi umum
    delete_index_input = input("\nApakah kolom pertama adalah index di semua file? (y/n): ").strip().lower()
    delete_index = delete_index_input == 'y'
    
    # 5. Proses setiap file
    dataframes = {}
    demography_reports = {}
    
    print("MEMULAI PEMROSESAN")

    
    for i, file_path in enumerate(selected_files, 1):
        df, demography = process_single_file(
            file_path, 
            delete_index, 
            file_number=i, 
            total_files=len(selected_files)
        )
        
        if df is not None and demography is not None:
            file_name = os.path.basename(file_path)
            dataframes[file_name] = df
            demography_reports[file_name] = demography
    
    if not dataframes:
        print("\n Tidak ada file yang berhasil diproses")
        return
    
    # 6. Gabungkan semua DataFrame (opsional)
    if len(dataframes) > 1:
        combine = input(f"\nGabungkan semua {len(dataframes)} dataset menjadi satu? (y/n): ").strip().lower()
        if combine == 'y':
            combined_df = pd.concat(dataframes.values(), ignore_index=True)
            print(" Dataset berhasil digabungkan:")
            print(f"  Total baris: {combined_df.shape[0]:,}")
            print(f"  Total kolom: {combined_df.shape[1]}")
            
            # Analisis demografi untuk data gabungan
            combined_demography = data_demography(combined_df, "DATA_GABUNGAN")
            demography_reports["DATA_GABUNGAN"] = combined_demography
            
            # Tambahkan ke dictionary dataframes
            dataframes["DATA_GABUNGAN"] = combined_df
    
    # 7. Simpan laporan demografi
    if demography_reports:
        save_report = input("\n Simpan laporan demografi ke file? (y/n): ").strip().lower()
        if save_report == 'y':
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            report_filename = input(f"Nama file laporan (default: laporan_demografi_{timestamp}.txt): ").strip()
            report_filename = report_filename if report_filename else f"laporan_demografi_{timestamp}.txt"
            
            with open(report_filename, 'w', encoding='utf-8') as f:
                f.write("LAPORAN DEMOGRAFI DATA - ANALISIS FOLDER\n")
                f.write(f"Folder sumber: {folder_path}\n")
                f.write(f"Waktu analisis: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                f.write(f"Jumlah file diproses: {len(demography_reports)}\n\n")
                
                for file_name, demography in demography_reports.items():
            
                    f.write(f"FILE: {file_name}\n")
                    
                    f.write(f"JUMLAH DATA:\n")
                    f.write(f"  Baris: {demography['jumlah_baris']:,}\n")
                    f.write(f"  Kolom: {demography['jumlah_kolom']}\n\n")
                    
                    f.write("DATA MISSING:\n")
                    for col, missing in demography['data_missing'].items():
                        percentage = demography['persentase_missing'][col]
                        if missing > 0:
                            f.write(f"  {col}: {missing:,} ({percentage}%)\n")
                    
                    f.write("\nTIPE DATA:\n")
                    for col, dtype in demography['kolom_dan_tipe'].items():
                        f.write(f"  {col}: {dtype}\n")
                    
                    f.write("\nKOLOM KATEGORIKAL (Unique Values):\n")
                    for col, unique in demography['unique_values_kategorikal'].items():
                        f.write(f"  {col}: {unique}\n")
                    
                    f.write("\n")
            
            print(f" Laporan demografi berhasil disimpan ke: {report_filename}")
    
    print("ANALISA SELESAI")
    
    
    # 8. Simpan data yang sudah diolah
    if dataframes:
        save_data = input(f"\n Simpan data yang sudah diolah? (y/n): ").strip().lower()
        if save_data == 'y':
            print("\nPilih opsi penyimpanan:")
            print("1. Simpan semua file terpisah")
            print("2. Simpan ke folder output")
            if "DATA_GABUNGAN" in dataframes:
                print("3. Simpan file gabungan saja")
            
            save_option = input("Pilih opsi (1/2/3): ").strip()
            
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            
            if save_option == "1":
                # Simpan semua file terpisah
                for file_name, df in dataframes.items():
                    if file_name == "DATA_GABUNGAN":
                        continue
                    
                    base_name = os.path.splitext(file_name)[0]
                    output_filename = f"olah_{base_name}_{timestamp}.csv"
                    
                    try:
                        df.to_csv(output_filename, index=False)
                        print(f"  {file_name} -> {output_filename}")
                    except Exception as e:
                        print(f" Error saat menyimpan {file_name}: {e}")
            
            elif save_option == "2":
                # Simpan ke folder output
                output_folder = f"output_{timestamp}"
                os.makedirs(output_folder, exist_ok=True)
                
                for file_name, df in dataframes.items():
                    if file_name == "DATA_GABUNGAN":
                        output_filename = f"data_gabungan_{timestamp}.csv"
                    else:
                        base_name = os.path.splitext(file_name)[0]
                        output_filename = f"olah_{base_name}_{timestamp}.csv"
                    
                    output_path = os.path.join(output_folder, output_filename)
                    
                    try:
                        df.to_csv(output_path, index=False)
                        print(f" {file_name} -> {output_path}")
                    except Exception as e:
                        print(f" Error saat menyimpan {file_name}: {e}")
                
                print(f"\n Semua file disimpan di folder: {output_folder}")
            
            elif save_option == "3" and "DATA_GABUNGAN" in dataframes:
                # Simpan file gabungan saja
                output_filename = f"data_gabungan_{timestamp}.csv"
                
                try:
                    dataframes["DATA_GABUNGAN"].to_csv(output_filename, index=False)
                    print(f" Data gabungan -> {output_filename}")
                except Exception as e:
                    print(f" Error saat menyimpan data gabungan: {e}")
            else:
                print(" Opsi tidak valid")
    
    # 9. Ringkasan akhir
    print("RINGKASAN PEMROSESAN")
    print(f"Total file diproses: {len(dataframes)}")
    for file_name, df in dataframes.items():
        print(f"  - {file_name}: {df.shape[0]:,} baris x {df.shape[1]} kolom")
    
    print(f"\n Proses selesai! Folder: {folder_path}")

# ==================== JALANKAN PROGRAM ====================
if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n Program dihentikan oleh pengguna")
    except Exception as e:
        print(f"\n Terjadi error: {e}")