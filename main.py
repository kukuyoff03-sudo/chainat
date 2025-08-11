import os
import re
import json
import time
import random
import requests
import pytz
import pandas as pd
from datetime import datetime
from typing import List, Tuple, Dict, Any

# --- Constants and Configuration ---

# If the user has set an environment variable, it will be used.
OPENWEATHER_API_KEY = os.environ.get(
    "OPENWEATHER_API_KEY", "c55ccdd65d09909976428698e8da16ec"
)
LINE_TOKEN = os.environ.get('LINE_CHANNEL_ACCESS_TOKEN')
LINE_API_URL = "https://api.line.me/v2/bot/message/broadcast"

# Data Source URLs
TMD_RADAR_URL = "https://weather.tmd.go.th/chaophraya.php"
SINGBURI_URL = "https://singburi.thaiwater.net/wl"
DISCHARGE_URL = 'https://tiwrm.hii.or.th/DATA/REPORT/php/chart/chaopraya/small/chaopraya.php'

# Location Coordinates for Pho Nang Dam Ok
WEATHER_LAT = 15.120
WEATHER_LON = 100.283

# Thai month mapping for Excel data
THAI_MONTHS = {
    'มกราคม':1, 'กุมภาพันธ์':2, 'มีนาคม':3, 'เมษายน':4,
    'พฤษภาคม':5, 'มิถุนายน':6, 'กรกฎาคม':7, 'สิงหาคม':8,
    'กันยายน':9, 'ตุลาคม':10, 'พฤศจิกายน':11, 'ธันวาคม':12
}


# --- NEW: Enhanced Weather Analysis Functions (Based on Suggestions) ---

def improved_weather_description(
    code: int, precipitation: float, temp_max: float
) -> str:
    """
    Improved weather code translation with more detailed and tropical-specific logic.
    Considers precipitation thresholds and extreme heat.
    """
    # Thunderstorm
    if code in {95, 96, 99}:
        return "พายุฝนฟ้าคะนอง"
    # Rain (adjusted thresholds for tropical climate)
    if code in {51, 53, 55, 56, 57, 61, 63, 65, 66, 67, 80, 81, 82}:
        if precipitation >= 20.0:
            return "ฝนตกหนักมาก"
        elif precipitation >= 10.0:
            return "ฝนตกหนัก"
        elif precipitation >= 5.0:
            return "ฝนปานกลาง"
        else:
            return "ฝนตกเล็กน้อย"
    # Heat warnings
    if code <= 3: # Clear or partly cloudy
        if temp_max >= 37.0:
            return "มีเมฆน้อยและอากาศร้อนจัด"
        elif temp_max >= 35.0:
            return "มีเมฆน้อยและอากาศร้อน"
    # General cases
    if code == 0:
        return "ท้องฟ้าแจ่มใส"
    if code in {1, 2, 3}:
        return "มีเมฆเป็นส่วนใหญ่"
    if code in {45, 48}:
        return "มีหมอก"
    if code in {71, 73, 75, 77, 85, 86}:
        return "หิมะ" # Unlikely but kept for completeness
    return "ไม่สามารถระบุสภาพอากาศได้"


def get_openweather_data(lat: float, lon: float, api_key: str) -> Dict[str, Any] | None:
    """Fetches current day forecast data from OpenWeather."""
    try:
        url = (
            f"https://api.openweathermap.org/data/2.5/forecast?lat={lat}&lon={lon}"
            f"&appid={api_key}&units=metric"
        )
        resp = requests.get(url, timeout=15)
        resp.raise_for_status()
        data = resp.json()

        analysis = {'temp_max': -999.0, 'rain_chance_time': None, 'humidity': 0}
        tz = pytz.timezone("Asia/Bangkok")
        today_str = datetime.now(tz).strftime("%Y-%m-%d")
        
        entry_count = 0
        total_humidity = 0

        for entry in data.get("list", []):
            if today_str not in entry.get("dt_txt", ""):
                continue
            
            main_data = entry.get("main", {})
            temp = main_data.get("temp")
            humidity = main_data.get("humidity")

            if temp is not None and temp > analysis['temp_max']:
                analysis['temp_max'] = temp
            
            if humidity is not None:
                total_humidity += humidity
                entry_count += 1

            weather = entry.get("weather", [])
            if weather and analysis['rain_chance_time'] is None:
                weather_id = weather[0].get("id")
                # Detects thunderstorms (2xx) or rain (5xx)
                if 200 <= weather_id < 600:
                    analysis['rain_chance_time'] = entry.get("dt_txt", "")[11:16]
        
        if entry_count > 0:
            analysis['humidity'] = total_humidity / entry_count

        return analysis
    except Exception as e:
        print(f"❌ ERROR: get_openweather_data: {e}")
        return None


def get_tmd_radar_nowcast(target_area: str = "ชัยนาท") -> Dict[str, Any] | None:
    """
    Provides a short-term rain 'nowcast' from TMD radar page.
    Returns a dictionary indicating if rain is detected.
    """
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        response = requests.get(TMD_RADAR_URL, headers=headers, timeout=20)
        response.raise_for_status()
        response.encoding = 'utf-8'
        soup = BeautifulSoup(response.text, 'html.parser')
        page_text = soup.get_text()

        if target_area in page_text:
            # Check for specific keywords indicating rain
            if "ฝนปานกลาง" in page_text or "ฝนหนัก" in page_text:
                return {"rain_incoming": True, "intensity": "ปานกลางถึงหนัก"}
        return {"rain_incoming": False}
    except Exception as e:
        print(f"❌ ERROR: get_tmd_radar_nowcast: {e}")
        return None

def calculate_heat_index(temp_c: float, humidity: float) -> float:
    """Estimates the heat index (feels-like temperature). Simplified formula."""
    if temp_c < 26.7: # Heat index is typically not calculated for lower temps
        return temp_c
        
    # Simplified formula, for approximation
    heat_index = -8.78469475556 + 1.61139411 * temp_c + 2.33854883889 * humidity + \
                 -0.14611605 * temp_c * humidity + -0.012308094 * (temp_c**2) + \
                 -0.0164248277778 * (humidity**2) + 0.002211732 * (temp_c**2) * humidity + \
                 0.00072546 * temp_c * (humidity**2) + \
                 -0.000003582 * (temp_c**2) * (humidity**2)
    return heat_index

def get_local_weather_advice(analysis: Dict[str, Any]) -> str | None:
    """Generates specific advice based on the weather analysis."""
    advice = []
    # Advice for farmers on heat
    if analysis.get('temp_max', 0) > 35:
        advice.append("🌾 ชาวนา: ควรหลีกเลี่ยงการทำงานกลางแจ้งช่วง 10:00-15:00 น.")
    # Advice for riverside residents
    if analysis.get('rain_incoming'):
         advice.append("📍 พื้นที่ริมน้ำ: โปรดระวังระดับน้ำที่อาจเพิ่มขึ้นจากฝนตกหนัก")
    return " • ".join(advice) if advice else None

def get_enhanced_weather_alert(lat: float, lon: float) -> str:
    """
    Generates a comprehensive and actionable weather alert by combining
    data from OpenWeather and TMD radar.
    """
    try:
        # Step 1: Fetch data from all available sources
        openweather_data = get_openweather_data(lat, lon, OPENWEATHER_API_KEY)
        radar_data = get_tmd_radar_nowcast(target_area="ชัยนาท")
        
        # Step 2: Analyze and combine data
        if not openweather_data:
            return "• ไม่สามารถดึงข้อมูลพยากรณ์อากาศได้"

        messages = []
        
        # Radar Nowcast (highest priority)
        if radar_data and radar_data.get('rain_incoming'):
            messages.append(
                f"🛰️ เรดาร์ตรวจพบกลุ่มฝน'ความแรง{radar_data['intensity']}'"
                f"บริเวณ จ.ชัยนาท อาจมีฝนตกใน 1-2 ชั่วโมงนี้"
            )
            
        # Temperature and Heat Index Alert
        temp_max = openweather_data.get('temp_max', 0)
        humidity = openweather_data.get('humidity', 0)
        if temp_max >= 37.0:
            heat_index = calculate_heat_index(temp_max, humidity)
            messages.append(
                f"🌡️ อากาศร้อนจัด! อุณหภูมิสูงสุด {temp_max:.1f}°C "
                f"(รู้สึกเหมือน {heat_index:.1f}°C)"
            )
            messages.append("💧 ควรดื่มน้ำบ่อยๆ และหลีกเลี่ยงกิจกรรมกลางแจ้ง")
        elif temp_max >= 35.0:
            messages.append(f"🌡️ อากาศร้อน! อุณหภูมิสูงสุด {temp_max:.1f}°C")

        # Rain forecast from OpenWeather (if radar doesn't show immediate rain)
        rain_time = openweather_data.get('rain_chance_time')
        if rain_time and not (radar_data and radar_data.get('rain_incoming')):
            messages.append(f"🌦️ คาดว่าอาจมีฝนช่วงเวลาประมาณ {rain_time} น.")
            
        # Add local advice
        local_advice = get_local_weather_advice({
            "temp_max": temp_max,
            "rain_incoming": radar_data.get('rain_incoming') if radar_data else False
        })
        if local_advice:
            messages.append(f"\nคำแนะนำเพิ่มเติม:\n{local_advice}")

        return "\n".join(messages) if messages else "• สภาพอากาศวันนี้โดยรวมปกติ"

    except Exception as e:
        return f"❌ เกิดข้อผิดพลาดในการสร้างข้อมูลพยากรณ์อากาศ: {e}"


# --- Core Data Fetching Functions (Largely Unchanged) ---

def get_historical_from_excel(year_be: int) -> int | None:
    path = f"data/ระดับน้ำปี{year_be}.xlsx"
    try:
        if not os.path.exists(path):
            print(f"⚠️ ไม่พบไฟล์ข้อมูลย้อนหลังที่: {path}")
            return None
        df = pd.read_excel(path)
        df = df.rename(columns={'ปริมาณน้ำ (ลบ.ม./วินาที)': 'discharge'})
        df['month_num'] = df['เดือน'].map(THAI_MONTHS)
        now = datetime.now(pytz.timezone('Asia/Bangkok'))
        today_d, today_m = now.day, now.month
        match = df[(df['วันที่']==today_d) & (df['month_num']==today_m)]
        if not match.empty:
            discharge_val = int(match.iloc[0]['discharge'])
            print(f"✅ พบข้อมูลย้อนหลังปี {year_be}: {discharge_val} ลบ.ม./วินาที")
            return discharge_val
        else:
            print(f"⚠️ ไม่พบข้อมูลสำหรับวันที่ {today_d}/{today_m} ในไฟล์ปี {year_be}")
            return None
    except Exception as e:
        print(f"❌ ERROR: ไม่สามารถโหลดข้อมูล Excel ({path}): {e}")
        return None

def get_sapphaya_data(retries: int = 3):
    api_url = "https://api-v3.thaiwater.net/api/v1/thaiwater30/public/waterlevel?province_code=18"
    for attempt in range(retries):
        try:
            headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'}
            response = requests.get(api_url, headers=headers, timeout=15)
            response.raise_for_status()
            data = response.json().get("data", [])
            for item in data:
                if (item.get("station", {}).get("tele_station_name", {}).get("th") == "สรรพยา" and
                    item.get("geocode", {}).get("tumbon_name", {}).get("th") == "โพนางดำออก"):
                    water_level = float(item.get("waterlevel_msl"))
                    bank_level = 13.87 # Fixed value
                    print(f"✅ พบข้อมูลสรรพยา: ระดับน้ำ={water_level}, ระดับตลิ่ง={bank_level}")
                    return water_level, bank_level
            print(f"⚠️ ไม่พบสถานี 'สรรพยา' ที่ ต.โพนางดำออก ในการเรียก API ครั้งที่ {attempt + 1}")
        except Exception as e:
            print(f"❌ ERROR: get_sapphaya_data (ครั้งที่ {attempt + 1}): {e}")
        if attempt < retries - 1:
            time.sleep(3)
    return None, None

def fetch_chao_phraya_dam_discharge(url: str):
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Cache-Control': 'no-cache',
            'Pragma': 'no-cache'
        }
        response = requests.get(f"{url}?cb={random.randint(10000, 99999)}", headers=headers, timeout=20)
        response.raise_for_status()
        response.encoding = 'utf-8'
        match = re.search(r'var json_data = (\[.*\]);', response.text)
        if not match:
            print("❌ ERROR: ไม่พบ JSON ในหน้าเว็บเขื่อนเจ้าพระยา")
            return None
        data = json.loads(match.group(1))
        # Navigate through the specific JSON structure for dam C13
        value = float(data[0]['itc_water']['C13']['storage'])
        print(f"✅ พบข้อมูลเขื่อนเจ้าพระยา: {value}")
        return value
    except Exception as e:
        print(f"❌ ERROR: fetch_chao_phraya_dam_discharge: {e}")
    return None


# --- Message Creation & Sending ---

def analyze_and_create_message(
    water_level: float, dam_discharge: float, bank_height: float,
    hist_2567: int | None, hist_2554: int | None
) -> str:
    distance_to_bank = bank_height - water_level
    if dam_discharge > 2400 or distance_to_bank < 1.0:
        ICON, HEADER = "🟥", "‼️ ประกาศเตือนภัยระดับสูงสุด ‼️"
        summary = ["คำแนะนำ:", "1. เตรียมพร้อมอพยพหากอยู่ในพื้นที่เสี่ยง",
                   "2. ขนย้ายทรัพย์สินขึ้นที่สูงโดยด่วน", "3. งดใช้เส้นทางสัญจรริมแม่น้ำ"]
    elif dam_discharge > 1800 or distance_to_bank < 2.0:
        ICON, HEADER = "🟨", "‼️ ประกาศเฝ้าระวัง ‼️"
        summary = ["คำแนะนำ:", "1. บ้านเรือนริมตลิ่งนอกคันกั้นน้ำ ให้เริ่มขนของขึ้นที่สูง",
                   "2. ติดตามสถานการณ์อย่างใกล้ชิด"]
    else:
        ICON, HEADER = "🟩", "สถานะปกติ"
        summary = [f"ระดับน้ำยังห่างตลิ่ง {distance_to_bank:.2f} ม. ถือว่า \"ปลอดภัย\" ✅",
                   "ประชาชนใช้ชีวิตได้ตามปกติครับ"]

    now = datetime.now(pytz.timezone("Asia/Bangkok"))
    msg = [
        f"{ICON} {HEADER}",
        f"📍 ต.โพนางดำออก อ.สรรพยา จ.ชัยนาท",
        f"🗓️ วันที่: {now.strftime('%d/%m/%Y %H:%M')} น.",
        "",
        "🌊 **ระดับน้ำ + ตลิ่ง**",
        f"• ระดับน้ำ: {water_level:.2f} ม.รทก.",
        f"• ตลิ่ง: {bank_height:.2f} ม.รทก. (ต่ำกว่า {distance_to_bank:.2f} ม.)",
        "",
        "💧 **ปริมาณน้ำปล่อยเขื่อนเจ้าพระยา**",
        f"• {dam_discharge:,.0f} ลบ.ม./วินาที",
        "",
        "📊 **เปรียบเทียบย้อนหลัง (ณ วันเดียวกัน)**",
        f"• ปี 2567: {hist_2567:,} ลบ.ม./วินาที" if hist_2567 else "• ปี 2567: ไม่มีข้อมูล",
        f"• ปี 2554: {hist_2554:,} ลบ.ม./วินาที" if hist_2554 else "• ปี 2554: ไม่มีข้อมูล",
        "",
        "🧾 **สรุปสถานการณ์**",
        *summary
    ]
    return "\n".join(msg)

def create_error_message(station_status: str, discharge_status: str) -> str:
    now = datetime.now(pytz.timezone('Asia/Bangkok'))
    return (
        f"⚙️❌ เกิดข้อผิดพลาดในการดึงข้อมูลหลัก ❌⚙️\n"
        f"เวลา: {now.strftime('%d/%m/%Y %H:%M')} น.\n\n"
        f"• สถานะข้อมูลระดับน้ำสรรพยา: {station_status}\n"
        f"• สถานะข้อมูลเขื่อนเจ้าพระยา: {discharge_status}\n\n"
        f"ระบบจะพยายามอีกครั้งในรอบถัดไป"
    )

def send_line_broadcast(message: str):
    if not LINE_TOKEN:
        print("❌ ไม่พบ LINE_CHANNEL_ACCESS_TOKEN! ไม่สามารถส่งข้อความได้")
        return
    headers = {"Content-Type": "application/json", "Authorization": f"Bearer {LINE_TOKEN}"}
    payload = {"messages": [{"type": "text", "text": message}]}
    try:
        res = requests.post(LINE_API_URL, headers=headers, json=payload, timeout=10)
        res.raise_for_status()
        print("✅ ส่งข้อความ Broadcast สำเร็จ!")
    except Exception as e:
        print(f"❌ ERROR: LINE Broadcast: {e.response.text if hasattr(e, 'response') else e}")

# --- Main Execution ---

if __name__ == "__main__":
    print("=== เริ่มการทำงานระบบแจ้งเตือนน้ำ (เวอร์ชันปรับปรุง) ===")
    
    # --- 1. Fetch Core Water Data ---
    print("\n💧 กำลังดึงข้อมูลระดับน้ำและเขื่อน...")
    water_level, bank_level = get_sapphaya_data()
    dam_discharge = fetch_chao_phraya_dam_discharge(DISCHARGE_URL)
    hist_2567 = get_historical_from_excel(2567)
    hist_2554 = get_historical_from_excel(2554)

    # --- 2. Build Core Water Message ---
    if all([water_level, bank_level, dam_discharge]):
        core_message = analyze_and_create_message(
            water_level, dam_discharge, bank_level, hist_2567, hist_2554
        )
    else:
        station_status = "สำเร็จ" if water_level is not None else "ล้มเหลว"
        discharge_status = "สำเร็จ" if dam_discharge is not None else "ล้มเหลว"
        core_message = create_error_message(station_status, discharge_status)

    # --- 3. Build Enhanced Weather Message ---
    print("\n🌦️  กำลังดึงและวิเคราะห์ข้อมูลพยากรณ์อากาศ...")
    weather_section = (
        "🌡️ **พยากรณ์อากาศวันนี้**\n" +
        get_enhanced_weather_alert(WEATHER_LAT, WEATHER_LON)
    )
    
    # --- 4. Assemble Final Message for LINE ---
    final_message = (
        f"{core_message}\n\n"
        f"{weather_section}\n\n"
        f"ที่มา: เทศบาลตำบลโพนางดำออก"
    )

    print("\n📤 ข้อความฉบับสมบูรณ์ที่จะแจ้งเตือน:")
    print("-" * 40)
    print(final_message)
    print("-" * 40)
    
    # --- 5. Send to LINE ---
    print("\n🚀 กำลังส่งข้อความไปยัง LINE...")
    send_line_broadcast(final_message)
    
    print("\n✅ การทำงานเสร็จสิ้น")
