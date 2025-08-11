import os
import re
import json
import time
import random
import requests
import pytz
import pandas as pd
from datetime import datetime
from typing import List, Tuple
from bs4 import BeautifulSoup

# We will integrate a second weather source (OpenWeather) for more
# descriptive alerts about today's conditions.  The following
# constants and helper function are adapted from the original
# `อากาศ.py` script provided by the user.  This ensures the script
# produces both a multi‑day forecast (via Open‑Meteo) and an
# immediate weather alert (via OpenWeather) within the same
# notification message.

# --- OpenWeather configuration ---
# If the user has set an environment variable named
# `OPENWEATHER_API_KEY`, it will be used to override the default key.
OPENWEATHER_API_KEY = os.environ.get(
    "OPENWEATHER_API_KEY", "c55ccdd65d09909976428698e8da16ec"
)

# --- TMD Data Sources (NEW) ---
# URL for TMD's radar page for the Chao Phraya basin. This page is
# monitored for near-real-time rain "nowcasting".
TMD_RADAR_URL = "https://weather.tmd.go.th/chaophraya.php"

def get_openweather_alert(
    lat: float | None = None,
    lon: float | None = None,
    api_key: str = OPENWEATHER_API_KEY,
    timezone: str = "Asia/Bangkok",
    timeout: int = 15,
) -> str:
    """
    Fetch a 5‑day/3‑hour forecast from OpenWeather and generate a
    succinct alert for today.  It summarises whether there will be
    exceptionally hot weather or a likelihood of rain/thunderstorms.
    If neither condition is met, it returns a generic message.  Any
    errors encountered will result in a descriptive error string.

    Parameters
    ----------
    lat : float
        Latitude of the location.
    lon : float
        Longitude of the location.
    api_key : str
        OpenWeather API key.  If not provided, a default key is used.
    timezone : str
        IANA timezone string for localising timestamps.
    timeout : int
        Timeout in seconds for the HTTP request.

    Returns
    -------
    str
        A message describing today's expected weather conditions.
    """
    try:
        # Use global coordinates if none are provided at call time.
        if lat is None:
            # Defer import to runtime to ensure WEATHER_LAT is defined.
            lat = WEATHER_LAT
        if lon is None:
            lon = WEATHER_LON
        # Build the OpenWeather API URL.  Using metric units to obtain
        # temperatures in Celsius directly.
        url = (
            f"https://api.openweathermap.org/data/2.5/forecast?lat={lat}&lon={lon}"
            f"&appid={api_key}&units=metric"
        )
        resp = requests.get(url, timeout=timeout)
        resp.raise_for_status()
        data = resp.json()
        # Establish local timezone and today's date string for filtering.
        tz = pytz.timezone(timezone)
        now = datetime.now(tz)
        today_str = now.strftime("%Y-%m-%d")
        max_temp = -999.0
        rain_detected_time: str | None = None
        # Iterate over forecast entries.  Each entry has a timestamp and
        # weather conditions.  We're only interested in entries for the
        # current local day.
        for entry in data.get("list", []):
            ts = entry.get("dt_txt", "")
            if today_str not in ts:
                continue
            temp = entry.get("main", {}).get("temp")
            weather = entry.get("weather", [])
            if temp is not None and isinstance(temp, (int, float)):
                if temp > max_temp:
                    max_temp = temp
            if weather:
                weather_id = weather[0].get("id")
                # Weather codes: thunderstorms (2xx) or heavy rain (5xx)
                if 200 <= weather_id < 300 or 500 <= weather_id < 600:
                    if not rain_detected_time:
                        # Extract HH:MM portion of the timestamp (YYYY‑MM‑DD HH:MM:SS)
                        rain_detected_time = ts[11:16] if len(ts) >= 16 else None
        # Construct messages based on conditions.
        messages = []
        if max_temp >= 35.0:
            messages.append(
                f"• พื้นที่ ต.โพนางดำออก อุณหภูมิสูงสุดประมาณ {round(max_temp, 1)}°C"
            )
        if rain_detected_time:
            messages.append(
                f"• คาดว่ามีฝนตกช่วงเวลา {rain_detected_time} น."
            )
        if not messages:
            messages.append("• สภาพอากาศปกติ ไม่มีฝนตก")
        return "\n".join(messages)
    except Exception as e:
        return f"❌ เกิดข้อผิดพลาดในการดึงข้อมูลอากาศ: {e}"

def get_tmd_radar_nowcast(
    radar_url: str = TMD_RADAR_URL,
    target_area: str = "ชัยนาท"
) -> str | None:
    """
    Provides a short-term rain forecast (nowcast) by checking the TMD
    radar page for mentions of significant rain in a target area.

    Parameters
    ----------
    radar_url : str
        The URL to the TMD weather radar page.
    target_area : str
        The name of the province/area to check for (e.g., "ชัยนาท").

    Returns
    -------
    str | None
        A nowcast message if rain is imminent, otherwise None.
    """
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        response = requests.get(radar_url, headers=headers, timeout=20)
        response.raise_for_status()
        response.encoding = 'utf-8'

        soup = BeautifulSoup(response.text, 'html.parser')
        page_text = soup.get_text()

        if target_area in page_text:
            if "ฝนปานกลาง" in page_text or "ฝนหนัก" in page_text:
                return f"🛰️ เรดาร์ตรวจพบกลุ่มฝนบริเวณ จ.{target_area} อาจมีฝนตกใน 1-2 ชั่วโมง"
        return None
    except Exception as e:
        print(f"❌ ERROR: get_tmd_radar_nowcast: {e}")
        return None

# --- ค่าคงที่ ---
SINGBURI_URL = "https://singburi.thaiwater.net/wl"
DISCHARGE_URL = 'https://tiwrm.hii.or.th/DATA/REPORT/php/chart/chaopraya/small/chaopraya.php'
LINE_TOKEN = os.environ.get('LINE_CHANNEL_ACCESS_TOKEN')
LINE_API_URL = "https://api.line.me/v2/bot/message/broadcast"

# -- อ่านข้อมูลย้อนหลังจาก Excel --
THAI_MONTHS = {
    'มกราคม':1, 'กุมภาพันธ์':2, 'มีนาคม':3, 'เมษายน':4,
    'พฤษภาคม':5, 'มิถุนายน':6, 'กรกฎาคม':7, 'สิงหาคม':8,
    'กันยายน':9, 'ตุลาคม':10, 'พฤศจิกายน':11, 'ธันวาคม':12
}

# --- พยากรณ์อากาศ ---
WEATHER_LAT = 15.120
WEATHER_LON = 100.283

def weather_code_to_description(code: int, precipitation: float) -> str:
    if code in {95, 96, 99}:
        return "พายุฝนฟ้าคะนอง"
    if code == 0:
        return "ท้องฟ้าแจ่มใส"
    if code in {1, 2, 3}:
        return "มีเมฆเป็นส่วนใหญ่"
    if code in {45, 48}:
        return "มีหมอก"
    if code in {51, 53, 55, 56, 57, 61, 63, 65, 66, 67, 80, 81, 82}:
        if precipitation >= 10.0:
            return "ฝนตกหนัก"
        if precipitation >= 2.0:
            return "ฝนปานกลาง"
        return "ฝนตกเล็กน้อย"
    if code in {71, 73, 75, 77, 85, 86}:
        return "หิมะ"
    return "สภาพอากาศไม่ทราบแน่ชัด"

def get_weather_forecast(
    lat: float = WEATHER_LAT,
    lon: float = WEATHER_LON,
    days: int = 3,
    timezone: str = "Asia/Bangkok",
    timeout: int = 15,
) -> List[Tuple[str, str]]:
    try:
        params = {
            "latitude": lat,
            "longitude": lon,
            "daily": "weathercode,precipitation_sum",
            "timezone": timezone,
        }
        resp = requests.get(
            "https://api.open-meteo.com/v1/forecast",
            params=params,
            timeout=timeout,
        )
        resp.raise_for_status()
        data = resp.json().get("daily", {})
        dates = data.get("time", [])
        codes = data.get("weathercode", [])
        precipitation_list = data.get("precipitation_sum", [])
        forecast = []
        for i in range(min(days, len(dates))):
            date = dates[i]
            code = codes[i] if i < len(codes) else None
            prec = precipitation_list[i] if i < len(precipitation_list) else 0.0
            desc = weather_code_to_description(code, prec) if code is not None else "-"
            forecast.append((date, desc))
        return forecast
    except Exception as e:
        print(f"❌ ERROR: get_weather_forecast: {e}")
        return []

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
            print(f"✅ พบข้อมูลย้อนหลังสำหรับปี {year_be}: {int(match.iloc[0]['discharge'])} ลบ.ม./วินาที")
            return int(match.iloc[0]['discharge'])
        else:
            print(f"⚠️ ไม่พบข้อมูลสำหรับวันที่ {today_d}/{today_m} ในไฟล์ปี {year_be}")
            return None
    except Exception as e:
        print(f"❌ ERROR: ไม่สามารถโหลดข้อมูลย้อนหลังจาก Excel ได้ ({path}): {e}")
        return None

def get_sapphaya_data(
    province_code: str = "18",
    target_tumbon: str = "โพนางดำออก",
    target_station_name: str = "สรรพยา",
    timeout: int = 15,
    retries: int = 3,
):
    api_url_template = (
        "https://api-v3.thaiwater.net/api/v1/thaiwater30/public/waterlevel?province_code={code}"
    )
    for attempt in range(retries):
        try:
            url = api_url_template.format(code=province_code)
            headers = {
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/91.0.4472.124 Safari/537.36"
                ),
            }
            response = requests.get(url, headers=headers, timeout=timeout)
            response.raise_for_status()
            data = response.json().get("data", [])
            for item in data:
                geocode = item.get("geocode", {})
                tumbon_name = geocode.get("tumbon_name", {}).get("th", "")
                station_info = item.get("station", {})
                station_name = station_info.get("tele_station_name", {}).get("th", "")
                if tumbon_name == target_tumbon and station_name == target_station_name:
                    wl_str = item.get("waterlevel_msl")
                    water_level = None
                    if wl_str is not None:
                        try:
                            water_level = float(wl_str)
                        except ValueError:
                            water_level = None
                    bank_level = 13.87
                    print(
                        f"✅ พบข้อมูลสรรพยา: ระดับน้ำ={water_level}, ระดับตลิ่ง={bank_level} (กำหนดเอง)"
                    )
                    return water_level, bank_level
            print(
                f"⚠️ ไม่พบข้อมูลสถานี '{target_station_name}' ที่ {target_tumbon} ในการเรียก API ครั้งที่ {attempt + 1}"
            )
        except Exception as e:
            print(f"❌ ERROR: get_sapphaya_data (ครั้งที่ {attempt + 1}): {e}")
        if attempt < retries - 1:
            time.sleep(3)
    return None, None

def fetch_chao_phraya_dam_discharge(url: str, timeout: int = 30):
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                          'AppleWebKit/537.36 (KHTML, like Gecko) '
                          'Chrome/91.0.4472.124 Safari/537.36',
            'Cache-Control': 'no-cache',
            'Pragma': 'no-cache'
        }
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

def analyze_and_create_message(
    water_level: float,
    dam_discharge: float,
    bank_height: float,
    hist_2567: int | None = None,
    hist_2554: int | None = None,
    weather_summary: List[Tuple[str, str]] | None = None,
) -> str:
    distance_to_bank = bank_height - water_level
    if dam_discharge is not None and (dam_discharge > 2400 or distance_to_bank < 1.0):
        ICON = "🟥"
        HEADER = "‼️ ประกาศเตือนภัยระดับสูงสุด ‼️"
        summary_lines = [
            "คำแนะนำ:",
            "1. เตรียมพร้อมอพยพหากอยู่ในพื้นที่เสี่ยง",
            "2. ขนย้ายทรัพย์สินขึ้นที่สูงโดยด่วน",
            "3. งดใช้เส้นทางสัญจรริมแม่น้ำ",
        ]
    elif dam_discharge is not None and (dam_discharge > 1800 or distance_to_bank < 2.0):
        ICON = "🟨"
        HEADER = "‼️ ประกาศเฝ้าระวัง ‼️"
        summary_lines = [
            "คำแนะนำ:",
            "1. บ้านเรือนริมตลิ่งนอกคันกั้นน้ำ ให้เริ่มขนของขึ้นที่สูง",
            "2. ติดตามสถานการณ์อย่างใกล้ชิด",
        ]
    else:
        ICON = "🟩"
        HEADER = "สถานะปกติ"
        summary_lines = [
            f"ระดับน้ำยังห่างตลิ่ง {distance_to_bank:.2f} ม. ถือว่า \"ปลอดภัย\" ✅",
            "ประชาชนใช้ชีวิตได้ตามปกติครับ",
        ]
    now = datetime.now(pytz.timezone("Asia/Bangkok"))
    TIMESTAMP = now.strftime("%d/%m/%Y %H:%M")
    msg_lines: List[str] = []
    msg_lines.append(f"{ICON} {HEADER}")
    msg_lines.append(f"📍 ต.โพนางดำออก อ.สรรพยา จ.ชัยนาท")
    msg_lines.append(f"🗓️ วันที่: {TIMESTAMP} น.")
    msg_lines.append("")
    msg_lines.append("🌊 ระดับน้ำ + ตลิ่ง")
    msg_lines.append(f"• ระดับน้ำ: {water_level:.2f} ม.รทก.")
    msg_lines.append(f"• ตลิ่ง: {bank_height:.2f} ม.รทก. (ต่ำกว่า {distance_to_bank:.2f} ม.)")
    msg_lines.append("")
    msg_lines.append("💧 ปริมาณน้ำปล่อยเขื่อนเจ้าพระยา")
    if dam_discharge is not None:
        msg_lines.append(f"{dam_discharge:,} ลบ.ม./วินาที")
    else:
        msg_lines.append("ข้อมูลไม่พร้อมใช้งาน")
    msg_lines.append("")
    msg_lines.append("📊 เปรียบเทียบย้อนหลัง")
    if hist_2567 is not None:
        msg_lines.append(f"• ปี 2567: {hist_2567:,} ลบ.ม./วินาที")
    if hist_2554 is not None:
        msg_lines.append(f"• ปี 2554: {hist_2554:,} ลบ.ม./วินาที")
    msg_lines.append("")
    msg_lines.append("🧾 สรุปสถานการณ์")
    for line in summary_lines:
        msg_lines.append(line)
    return "\n".join(msg_lines)

def create_error_message(station_status, discharge_status):
    now = datetime.now(pytz.timezone('Asia/Bangkok'))
    return (
        f"⚙️❌ เกิดข้อผิดพลาดในการดึงข้อมูล ❌⚙️\n"
        f"เวลา: {now.strftime('%d/%m/%Y %H:%M')} น.\n\n"
        f"• สถานะข้อมูลระดับน้ำสรรพยา: {station_status}\n"
        f"• สถานะข้อมูลเขื่อนเจ้าพระยา: {discharge_status}\n\n"
        f"กรุณาตรวจสอบ Log บน GitHub Actions เพื่อดูรายละเอียดข้อผิดพลาดครับ"
    )

def send_line_broadcast(message):
    if not LINE_TOKEN:
        print("❌ ไม่พบ LINE_CHANNEL_ACCESS_TOKEN!")
        return
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {LINE_TOKEN}"
    }
    payload = {"messages": [{"type": "text", "text": message}]}
    try:
        res = requests.post(LINE_API_URL, headers=headers, json=payload, timeout=10)
        res.raise_for_status()
        print("✅ ส่งข้อความ Broadcast สำเร็จ!")
    except Exception as e:
        print(f"❌ ERROR: LINE Broadcast: {e}")

if __name__ == "__main__":
    print("=== เริ่มการทำงานระบบแจ้งเตือนน้ำ (เวอร์ชันปรับปรุง) ===")
    
    # --- Fetch Core Data ---
    water_level, bank_level = get_sapphaya_data()
    dam_discharge = fetch_chao_phraya_dam_discharge(DISCHARGE_URL)
    hist_2567 = get_historical_from_excel(2567)
    hist_2554 = get_historical_from_excel(2554)

    # --- Build Core Message ---
    if water_level is not None and bank_level is not None and dam_discharge is not None:
        core_message = analyze_and_create_message(
            water_level,
            dam_discharge,
            bank_level,
            hist_2567,
            hist_2554,
        )
    else:
        station_status = "สำเร็จ" if water_level is not None else "ล้มเหลว"
        discharge_status = "สำเร็จ" if dam_discharge is not None else "ล้มเหลว"
        core_message = create_error_message(station_status, discharge_status)

    # --- Weather Section (Improved) ---
    print("\n🌦️  กำลังดึงข้อมูลพยากรณ์อากาศ...")
    # 1. Get daily forecast from OpenWeather (for temp & general rain chance)
    weather_alert_daily = get_openweather_alert()

    # 2. Get short-term "nowcast" from TMD Radar
    radar_nowcast = get_tmd_radar_nowcast(target_area="ชัยนาท")

    # 3. Construct the final weather message section
    weather_section_lines = ["🌡️ พยากรณ์อากาศวันนี้"]
    if radar_nowcast:
        # If radar detects imminent rain, prioritize that message
        weather_section_lines.append(radar_nowcast)
    
    if weather_alert_daily:
        # Append the daily summary from OpenWeather
        weather_section_lines.append(weather_alert_daily)
    
    if len(weather_section_lines) == 1:
        # Fallback if both weather sources fail
        weather_section_lines.append("• ไม่สามารถดึงข้อมูลพยากรณ์อากาศได้")
    
    weather_section = "\n".join(weather_section_lines)

    # --- Assemble Final Message for LINE ---
    final_message = (
        f"{core_message}\n\n"
        f"{weather_section}\n\n"
        f"เทศบาลตำบลโพนางดำออก"
    )

    print("\n📤 ข้อความที่จะแจ้งเตือน:")
    print(final_message)
    print("\n🚀 กำลังส่งข้อความไปยัง LINE...")
    send_line_broadcast(final_message)
    print("✅ เสร็จสิ้นการทำงาน")
