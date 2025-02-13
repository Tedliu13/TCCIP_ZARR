import os
import tkinter as tk
from tkinter import messagebox, scrolledtext, ttk
import s3fs
import xarray as xr
import warnings
import urllib3
import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta
import time
import json
import cftime
import threading
#20250211 修改popup資訊，下載時間改為3天6小時
#20250213 修正historical的資料讀取，GWL_models_tas.csv 以及 GWL_models_pr.csv增加historical欄位

def show_warning():
    warning_window = tk.Toplevel(root)
    warning_window.title("TCCIP雲端資料下載工具 提醒")

    # 設定視窗大小
    warning_width = 740
    warning_height = 200
    
    # 取得螢幕寬度與高度
    screen_width = root.winfo_screenwidth()
    screen_height = root.winfo_screenheight()
    
    # 計算視窗左上角的 X 和 Y 座標，讓視窗置中
    position_x = (screen_width // 2) - (warning_width // 2)
    position_y = (screen_height // 2) - (warning_height // 2)
    
    # 設定 Tkinter 視窗的大小與位置
    warning_window.geometry(f"{warning_width}x{warning_height}+{position_x}+{position_y}")
    
    # 設定視窗 Icon（正確的方法）
    warning_window.iconbitmap("tccip.ico")  # 設定視窗圖示（僅適用於 Windows）

    label = tk.Label(
        warning_window, 
        text="TCCIP AR6 1km統計降尺度日資料，因解析度高，資料量龐大，需要花費較長時間下載，\n"
             "以桃園為例，共1223個網格，2度C所有模式情境86組，每組各20年，共1720組資料\n"
             "處理加下載時間約3天6小時，敬請您做好準備與耐心等候~\n\n"
             "本工具需另外申請雲端金鑰，若您尚未取得，請洽TCCIP取得雲端資料庫金鑰。\n"
             "若您需要不同的下載格點，請修改grids.csv或將不同縣市的格點檔案置換成grids.csv即可",
        font=("微軟正黑體", 12),  # 調整字體大小
        justify="left"
    )
    label.pack(padx=20, pady=10)

    ok_button = tk.Button(warning_window, text="確定", command=warning_window.destroy, font=("微軟正黑體", 12))
    ok_button.pack(pady=10)

def select_data():
    selected_option = var.get()
    selected_variable = var_var.get()
    if selected_option == "":
        messagebox.showerror("錯誤", "請選擇一個資料段！")
        return
    if selected_variable == "":
        messagebox.showerror("錯誤", "請選擇一個變數！")
        return

    global no, center_yr, GWL, variable, variable_c
    no = int(selected_option)
    variable = selected_variable
    log_message(f"selected_option={selected_option}")

    # 建立變數中文名稱對應
    variable_map = {
        "tas": "平均溫",
        "tasmax": "日最高溫",
        "tasmin": "日最低溫",
        "pr": "降雨量"
    }
    variable_c = variable_map[variable]

    # **修正 GWL 變數**
    GWL_options = {0: "historical", 1: "1.5°C", 2: "2°C", 3: "3°C", 4: "4°C"}
    GWL = GWL_options.get(no, None)  # 若 no 為 0，則 GWL 為 None
    #log_message(f"雲端資料變數時期為{GWL}")

    start_processing_thread()

def log_message(message):
    text_output.insert(tk.END, message + "\n")
    text_output.yview(tk.END)
    root.update_idletasks()

def process_data():
    log_message("開始處理資料...")
    

    if not Path('AccessKey.txt').is_file():
        log_message("❌ AccessKey.txt 文件不存在！")
        return
    with open('AccessKey.txt', 'r', encoding='utf-8') as file:
        data = json.load(file)

    aws_access_key_id = data['AccessKey']['AccessKeyId']
    aws_secret_access_key = data['AccessKey']['SecretAccessKey']
    endpoint_url = 'https://140.109.172.98'

    fs = s3fs.S3FileSystem(
        key=aws_access_key_id,
        secret=aws_secret_access_key,
        client_kwargs={'endpoint_url': endpoint_url, 'verify': False}
    )
    bucket_path0 = f'bucket/test/CMIP6_QDM_0.01deg/{variable}_QDM_'

    grids = pd.read_csv('grids.csv', header=None, names=['lon', 'lat'])
    grids['lon'] = grids['lon'].round(2)
    grids['lat'] = grids['lat'].round(2)

    file_path = f'GWL-models-{variable}.csv'
    
    GWLdata = pd.read_csv(file_path, encoding='big5')
    model = GWLdata['model']
    scenarios = GWLdata['scenario']
    center_yr = GWLdata[GWL]

    # **計算進度條最大值**
    #valid_scenarios_per_model = GWLdata.groupby("model")["scenario"].count()
    #total_model_scenarios = valid_scenarios_per_model.sum()
    total_model_scenarios=0
    for i, modelname in enumerate(model):
        if pd.isna(center_yr[i]):
            continue
        total_model_scenarios+=1
    #log_message(f"{total_model_scenarios}")
    years_per_model = 20  # 每個 model 有 20 年
    #years_per_model = 1  # 每個 model 有 20 年
    total_progress_steps = total_model_scenarios * years_per_model

    progress_bar["maximum"] = total_progress_steps
    progress_bar["value"] = 0  # 進度條初始化

    # 創建輸出目錄
    output_dir = Path("output_csv")
    output_dir.mkdir(exist_ok=True)
    start_time = time.time()

    for i, modelname in enumerate(model):
        bucket_path = bucket_path0 + scenarios[i] + '_' + modelname + '.zarr' if no!=0 else bucket_path0 + 'historical_' + modelname + '.zarr'
        log_message(f"雲端資料為 {bucket_path}")
        file0 = bucket_path + '/'
        warnings.simplefilter('ignore', urllib3.exceptions.InsecureRequestWarning)

        try:
            ds = xr.open_zarr(s3fs.S3Map(file0, s3=fs), use_cftime=True)
            tas_filtered = ds[variable]
            log_message(f"成功讀取{bucket_path}")
        except Exception as e:
            log_message(f"❌ 錯誤: 無法讀取 {modelname} - {e}")
            continue
        # **檢查 center_yr 是否為 NaN**
        if pd.isna(center_yr[i]):
            log_message(f"跳過 {model[i]} - {scenarios[i]}，沒有有效的GWL年份") if no!=0 else log_message(f"跳過 {model[i]} - historical，因為重複所以不用處理")
            continue  # 跳過這個模型，避免 NaN 轉換錯誤

        start_year = int(center_yr[i]) - 9
        end_year = int(center_yr[i]) + 10
        #end_year = int(center_yr[i]) -9 if no != 0 else 1995

        # **檢查曆法類型**
        calendar_type = ds['time'].dt.calendar if hasattr(ds['time'].dt, 'calendar') else "standard"
        if calendar_type == '360_day':
            date_type = cftime.Datetime360Day
            log_message(f"📅 {modelname} 使用 360 天曆法")
        elif calendar_type == 'noleap':
            date_type = cftime.DatetimeNoLeap
        else:
            date_type = datetime

        for year in range(start_year, end_year + 1):
            model_scenarios_year = f"{modelname}_{scenarios[i]}_{year}" if no!=0 else f"{modelname}_historical_{year}"
            log_message(f"🔄 處理資料: {model_scenarios_year} ({progress_bar['value']+1}/{total_progress_steps})")

            progress_bar["value"] += 1
            root.update_idletasks()

            time_start_of_year = date_type(year, 1, 1)
            time_end_of_year = date_type(year, 12, 30) if calendar_type == '360_day' else date_type(year, 12, 31)

            yearly_data = tas_filtered.sel(time=slice(time_start_of_year, time_end_of_year))

            rows = []
            for _, row in grids.iterrows():
                lon, lat = row['lon'], row['lat']
                try:
                    point_data = yearly_data.sel(lon=lon, lat=lat, method="nearest").to_dataframe().reset_index()
                    point_data['time'] = point_data['time'].apply(lambda t: t.strftime('%Y-%m-%d'))
                    point_data = point_data[['time', variable]].rename(columns={'time': 'date', variable: 'value'})
                    point_data['lon'] = lon
                    point_data['lat'] = lat
                    rows.append(point_data)
                except KeyError:
                    log_message(f"⚠️ 跳過點 ({lon}, {lat}) - 無數據")

            if rows:
                combined_data = pd.concat(rows, ignore_index=True)
                combined_data = combined_data.pivot_table(index=['lon', 'lat'], columns='date', values='value').reset_index()
                combined_data.columns.name = None  # 移除多餘的列名

                output_filename = f"AR6_統計降尺度_日資料_{variable_c}_{scenarios[i]}_{modelname}_{year}.csv" if no!=0 else f"AR6_統計降尺度_日資料_{variable_c}_historical_{modelname}_{year}.csv"
                output_filepath = output_dir / output_filename  # 確保完整路徑
                combined_data.to_csv(output_filepath, index=False)
                log_message(f"✅ {output_filepath} 已儲存")


    end_time = time.time()
    elapsed_time = str(timedelta(seconds=int(end_time - start_time)))
    log_message(f"🎉 完成！總耗時: {elapsed_time}")

def start_processing_thread():
    thread = threading.Thread(target=process_data)
    thread.start()

root = tk.Tk()
root.protocol("WM_DELETE_WINDOW", lambda: os._exit(0))
root.title("TCCIP AR6 1km 統計降尺度日資料 雲端下載工具")
# 設定視窗大小
window_width = 600
window_height = 600

# 取得螢幕寬度與高度
screen_width = root.winfo_screenwidth()
screen_height = root.winfo_screenheight()

# 計算視窗左上角的 X 和 Y 座標，讓視窗置中
position_x = (screen_width // 2) - (window_width // 2)
position_y = (screen_height // 2) - (window_height // 2)

# 設定 Tkinter 視窗的大小與位置
root.geometry(f"{window_width}x{window_height}+{position_x}+{position_y}")

# 設定視窗 Icon（正確的方法）
root.iconbitmap("tccip.ico")  # 設定視窗圖示（僅適用於 Windows）

var = tk.StringVar(value="0")
var_var = tk.StringVar(value="tas")

frame1 = tk.LabelFrame(root, text="選擇資料段", font=("微軟正黑體", 12))
frame1.pack(fill="x", padx=20, pady=10)

options = [
    ("Baseline (1995~2014)", 0),
    ("GWL 1.5°C", 1),
    ("GWL 2°C", 2),
    ("GWL 3°C", 3),
    ("GWL 4°C", 4)
]

for text, value in options:
    tk.Radiobutton(frame1, text=text, variable=var, value=value).pack(anchor="w")

frame2 = tk.LabelFrame(root, text="選擇變數", font=("微軟正黑體", 12))
frame2.pack(fill="x", padx=20, pady=10)

variables = [
    ("平均溫 (tas)", "tas"),
    ("日最高溫 (tasmax)", "tasmax"),
    ("日最低溫 (tasmin)", "tasmin"),
    ("降雨量 (pr)", "pr")
]

for text, value in variables:
    tk.Radiobutton(frame2, text=text, variable=var_var, value=value).pack(anchor="w")

ttk.Button(root, text="開始", command=select_data).pack(pady=10)
progress_bar = ttk.Progressbar(root, length=560)
progress_bar.pack(pady=5)
text_output = scrolledtext.ScrolledText(root, height=20)
text_output.pack(padx=20, pady=10)

root.after(100, show_warning)
root.mainloop()
