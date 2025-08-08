import os
import re
import json
import time
import random
import requests
import pytz
import pandas as pd
from datetime import datetime
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import StaleElementReferenceException

# --- ค่าคงที่ ---
SINGBURI_URL = "https://singburi.thaiwater.net/wl"
DISCHARGE_URL = 'https://tiwrm.hii.or.th/DATA/REPORT/php/chart/chaopraya/small/chaopraya.php'
# HISTORICAL_DATA_FILE = 'data/dam_discharge_history_complete.csv' # No longer needed
LINE_TOKEN = os.environ.get('LINE_CHANNEL_ACCESS_TOKEN')
LINE_API_URL = "https://api.line.me/v2/bot/message/broadcast"

# -- อ่านข้อมูลย้อนหลังจาก Excel --
THAI_MONTHS = {
    'มกราคม':1, 'กุมภาพันธ์':2, 'มีนาคม':3, 'เมษายน':4,
    'พฤษภาคม':5, 'มิถุนายน':6, 'กรกฎาคม':7, 'สิงหาคม':8,
    'กันยายน':9, 'ตุลาคม':10, 'พฤศจิกายน':11, 'ธันวาคม':12
}
def get_historical_from_excel(year_be: int) -> int | None:
    """
    อ่านไฟล์ data/ระดับน้ำปี{year_be}.xlsx
    คืนค่า discharge (ลบ.ม./วิ) ของวัน–เดือน ปัจจุบัน
    """
    path = f"data/ระดับน้ำปี{year_be}.xlsx"
    try:
        if not os.path.exists(path):
            print(f"⚠️ ไม่พบไฟล์ข้อมูลย้อนหลังที่: {path}")
            return None
        df = pd.read_excel(path)
        # เปลี่ยนเป็นแม็ปชื่อคอลัมน์ให้ตรงกับไฟล์ Excel จริง
        # (ดูชื่อ header ในไฟล์ว่าตรงนี้คือ 'ปริมาณน้ำ (ลบ.ม./วินาที)')
        df = df.rename(columns={'ปริมาณน้ำ (ลบ.ม./วินาที)': 'discharge'})
        df['month_num'] = df['เดือน'].map(THAI_MONTHS)

        now = datetime.now(pytz.timezone('Asia/Bangkok'))
        today_d, today_m = now.day, now.month
        match = df[(df['วันที่']==today_d) & (df['month_num']==today_m)]
        if not match.empty:
            print(f"✅ พบข้อมูลย้อนหลังสำหรับปี {year_be}: {int(match.iloc[0]['discharge'])} ลบ.ม./วินาที")
            return int(match.iloc[0]['discharge'])
        else:
            print(f"⚠️ ไม่พบข้อมูลสำหรับวันที่ {today_d}/{today_m} ในไฟล์ปี {year_be}")
            return None
    except Exception as e:
        print(f"❌ ERROR: ไม่สามารถโหลดข้อมูลย้อนหลังจาก Excel ได้ ({path}): {e}")
        return None

# --- ดึงระดับน้ำสรรพยา --- # <--- เปลี่ยนแปลง
def get_sapphaya_data(url: str, timeout: int = 45, retries: int = 3): # <--- เปลี่ยนแปลง
    opts = Options()
    opts.add_argument("--headless")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    
    driver = None
    for attempt in range(retries):
        try:
            driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=opts)
            driver.get(url)
            WebDriverWait(driver, timeout).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "th[scope='row']"))
            )
            html = driver.page_source
            
            soup = BeautifulSoup(html, "html.parser")
            for th in soup.select("th[scope='row']"):
                if "สรรพยา" in th.get_text(strip=True): # <--- เปลี่ยนแปลง: ค้นหา "สรรพยา"
                    tr = th.find_parent("tr")
                    cols = tr.find_all("td")
                    # จาก HTML โครงสร้างคือ: [แม่น้ำ, ที่ตั้ง, ระดับน้ำ, ระดับตลิ่ง, ...]
                    water_level = float(cols[2].get_text(strip=True)) # <--- เปลี่ยนแปลง: index 2
                    bank_level = float(cols[3].get_text(strip=True))  # <--- เปลี่ยนแปลง: index 3
                    print(f"✅ พบข้อมูลสรรพยา: ระดับน้ำ={water_level}, ระดับตลิ่ง={bank_level}") # <--- เปลี่ยนแปลง
                    if driver: driver.quit()
                    return water_level, bank_level
            
            print("⚠️ ไม่พบข้อมูลสถานี 'สรรพยา' ในตาราง") # <--- เปลี่ยนแปลง
            if driver: driver.quit()
            return None, None
        except StaleElementReferenceException:
            print(f"⚠️ เจอ Stale Element Reference (ครั้งที่ {attempt + 1}/{retries}), กำลังลองใหม่...")
            if driver: driver.quit()
            time.sleep(3)
            continue
        except Exception as e:
            print(f"❌ ERROR: get_sapphaya_data: {e}") # <--- เปลี่ยนแปลง
            if driver: driver.quit()
            return None, None
    return None, None

# --- ดึงข้อมูลเขื่อนเจ้าพระยา (เพิ่ม Cache Busting) ---
def fetch_chao_phraya_dam_discharge(url: str, timeout: int = 30):
    try:
        # เพิ่ม headers เพื่อพยายามไม่ให้ติด cache
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Cache-Control': 'no-cache',
            'Pragma': 'no-cache'
        }
        # เพิ่มตัวเลขสุ่มต่อท้าย URL (Cache Busting)
        cache_buster_url = f"{url}?cb={random.randint(10000, 99999)}"
        
        response = requests.get(cache_buster_url, headers=headers, timeout=10)
        response.raise_for_status()
        response.encoding = 'utf-8'
        
        match = re.search(r'var json_data = (\[.*\]);', response.text)
        if not match:
            print("❌ ERROR: ไม่พบข้อมูล JSON ในหน้าเว็บ")
            return None
            
        json_string = match.group(1)
        data = json.loads(json_string)
        
        water_storage = data[0]['itc_water']['C13']['storage']
        if water_storage is not None:
            if isinstance(water_storage, (int, float)):
                value = float(water_storage)
            else:
                value = float(str(water_storage).replace(',', ''))
                
            print(f"✅ พบข้อมูลเขื่อนเจ้าพระยา: {value}")
            return value
    except Exception as e:
        print(f"❌ ERROR: fetch_chao_phraya_dam_discharge: {e}")
    return None

# --- วิเคราะห์และสร้างข้อความ ---
def analyze_and_create_message(water_level, dam_discharge, bank_height, hist_2567=None, hist_2554=None): # <--- เปลี่ยนแปลง
    distance_to_bank = bank_height - water_level
    
    ICON = ""
    HEADER = ""
    summary_text = ""

    if dam_discharge > 2400 or distance_to_bank < 1.0:
        ICON = "🟥"
        HEADER = "‼️ ประกาศเตือนภัยระดับสูงสุด ‼️"
        summary_text = "คำแนะนำ:\n1. เตรียมพร้อมอพยพหากอยู่ในพื้นที่เสี่ยง\n2. ขนย้ายทรัพย์สินขึ้นที่สูงโดยด่วน\n3. งดใช้เส้นทางสัญจรริมแม่น้ำ"
    elif dam_discharge > 1800 or distance_to_bank < 2.0:
        ICON = "🟨"
        HEADER = "‼️ ประกาศเฝ้าระวัง ‼️"
        summary_text = "คำแนะนำ:\n1. บ้านเรือนริมตลิ่งนอกคันกั้นน้ำ ให้เริ่มขนของขึ้นที่สูง\n2. ติดตามสถานการณ์อย่างใกล้ชิด"
    else:
        ICON = "🟩"
        HEADER = "สถานะปกติ"
        summary_text = "สถานการณ์น้ำยังปกติ ใช้ชีวิตได้ตามปกติครับ"

    now = datetime.now(pytz.timezone('Asia/Bangkok'))
    TIMESTAMP = now.strftime('%d/%m/%Y %H:%M')

    msg_lines = [
        f"{ICON} {HEADER}",
        "",
        f"📍 รายงานสถานการณ์น้ำเจ้าพระยา (สถานี C.2 สรรพยา)", # <--- เปลี่ยนแปลง
        f"🗓️ วันที่: {TIMESTAMP} น.",
        "",
        "🌊 ระดับน้ำ + ระดับตลิ่ง",
        f"  • สรรพยา: {water_level:.2f} ม.รทก.", # <--- เปลี่ยนแปลง
        f"  • ตลิ่ง: {bank_height:.2f} ม.รทก. (ต่ำกว่า {distance_to_bank:.2f} ม.)",
        "",
        "💧 ปริมาณน้ำปล่อยเขื่อนเจ้าพระยา",
        f"  {dam_discharge:,} ลบ.ม./วินาที",
        "",
        "🔄 เปรียบเทียบย้อนหลัง",
    ]
    if hist_2567 is not None:
        msg_lines.append(f"  • ปี 2567: {hist_2567:,} ลบ.ม./วินาที")
    if hist_2554 is not None:
        msg_lines.append(f"  • ปี 2554: {hist_2554:,} ลบ.ม./วินาที")
    msg_lines += [
        "",
        summary_text
    ]
    return "\n".join(msg_lines)

# --- สร้างข้อความ Error ---
def create_error_message(station_status, discharge_status): # <--- เปลี่ยนแปลง
    now = datetime.now(pytz.timezone('Asia/Bangkok'))
    return (
        f"⚙️❌ เกิดข้อผิดพลาดในการดึงข้อมูล ❌⚙️\n"
        f"เวลา: {now.strftime('%d/%m/%Y %H:%M')} น.\n\n"
        f"• สถานะข้อมูลระดับน้ำสรรพยา: {station_status}\n" # <--- เปลี่ยนแปลง
        f"• สถานะข้อมูลเขื่อนเจ้าพระยา: {discharge_status}\n\n"
        f"กรุณาตรวจสอบ Log บน GitHub Actions เพื่อดูรายละเอียดข้อผิดพลาดครับ"
    )

# --- ส่งข้อความ LINE ---
def send_line_broadcast(message):
    if not LINE_TOKEN:
        print("❌ ไม่พบ LINE_CHANNEL_ACCESS_TOKEN!")
        return
    headers = {"Content-Type": "application/json", "Authorization": f"Bearer {LINE_TOKEN}"}
    payload = {"messages": [{"type": "text", "text": message}]}
    try:
        res = requests.post(LINE_API_URL, headers=headers, json=payload, timeout=10)
        res.raise_for_status()
        print("✅ ส่งข้อความ Broadcast สำเร็จ!")
    except Exception as e:
        print(f"❌ ERROR: LINE Broadcast: {e}")

# --- Main (เพิ่ม Cache Busting) ---
if __name__ == "__main__":
    print("=== เริ่มการทำงานระบบแจ้งเตือนน้ำ ===") # <--- เปลี่ยนแปลง
    
    # เพิ่มตัวเลขสุ่มต่อท้าย URL ของ Selenium (Cache Busting)
    station_cache_buster_url = f"{SINGBURI_URL}?cb={random.randint(10000, 99999)}"
    
    water_level, bank_level = get_sapphaya_data(station_cache_buster_url) # <--- เปลี่ยนแปลง
    dam_discharge = fetch_chao_phraya_dam_discharge(DISCHARGE_URL)
    
    # ดึงข้อมูลย้อนหลังจาก Excel
    hist_2567 = get_historical_from_excel(2567)
    hist_2554 = get_historical_from_excel(2554)

    if water_level is not None and bank_level is not None and dam_discharge is not None:
        final_message = analyze_and_create_message(water_level, dam_discharge, bank_level, hist_2567, hist_2554)
    else:
        station_status = "สำเร็จ" if water_level is not None else "ล้มเหลว" # <--- เปลี่ยนแปลง
        discharge_status = "สำเร็จ" if dam_discharge is not None else "ล้มเหลว"
        final_message = create_error_message(station_status, discharge_status)

    print("\n📤 ข้อความที่จะแจ้งเตือน:")
    print(final_message)
    print("\n🚀 ส่งข้อความไปยัง LINE...")
    send_line_broadcast(final_message)
    print("✅ เสร็จสิ้นการทำงาน")
