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
                # Weather codes: thunderstorms (95,96,99) or heavy rain (500‑504)
                if weather_id in {95, 96, 99, 500, 501, 502, 503, 504} and not rain_detected_time:
                    # Extract HH:MM portion of the timestamp (YYYY‑MM‑DD HH:MM:SS)
                    rain_detected_time = ts[11:16] if len(ts) >= 16 else None
        # Construct messages based on conditions.
        messages = []
        if max_temp >= 35.0:
            messages.append(
                f"โพนางดำออกวันนี้... แดดแรงเหมือนโกรธใครมา! 🥵\n\n"
                f"อุณหภูมิสูงสุดพุ่งไปถึง {round(max_temp, 1)}°C เลยนะ พกร่มพกน้ำให้พร้อม! 🍳"
            )
        if rain_detected_time:
            messages.append(
                f"ชาวโพนางดำออก! ⛈️ เมฆกำลังตั้งตี้สาดน้ำ!\n\n"
                f"มีแววฝนจะเทลงมาช่วงประมาณ {rain_detected_time} น. พกร่มไปด้วยนะ เดี๋ยวเปียก! 😎"
            )
        # If no significant events detected, provide a default message.
        if not messages:
            messages.append("📍 วันนี้ที่โพนางดำออก: อากาศปกติ ☀️ ไม่มีเหตุพิเศษครับ")
        return "\n\n".join(messages)
    except Exception as e:
        return f"❌ เกิดข้อผิดพลาดในการดึงข้อมูลอากาศ: {e}"

# Even though some of these imports are no longer used directly (e.g. Selenium),
# we retain them here for compatibility with the existing deployment
# environment. Selenium was previously used for scraping, but the updated
# implementation relies entirely on the ThaiWater API for real‑time data.

# --- ค่าคงที่ ---
# The old SINGBURI_URL is preserved for backwards compatibility, but
# water level data is now fetched via the API.  The province code for
# Chai Nat (ชัยนาท) is '18'.
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
# Define the coordinates for ต.โพนางดำออก (Pho Nang Dam Ok) in Chai Nat.
# These values are approximate and derived from publicly available
# administrative datasets.  The open‑meteom API will use these
# coordinates to return location‑specific weather forecasts.
WEATHER_LAT = 15.120
WEATHER_LON = 100.283

# Mapping from WMO weather codes (used by Open‑Meteo) to a human‑readable
# description in Thai.  The categories focus on the presence of sunshine,
# rain, heavy rain, or thunderstorm for easy comprehension in a daily
# summary.  The precipitation amount (mm) will further refine the
# classification (e.g., heavy rain vs. light rain).
def weather_code_to_description(code: int, precipitation: float) -> str:
    """
    Convert a WMO weather code and precipitation amount into a concise
    description in Thai.  Codes are documented by Open‑Meteo.  We also
    consider the precipitation sum to categorise light vs. heavy rain.

    Parameters
    ----------
    code : int
        The WMO weather code.
    precipitation : float
        Total precipitation (mm) for the day.

    Returns
    -------
    str
        A short description in Thai summarising the daily weather.
    """
    # Thunderstorm codes (95, 96, 99) indicate storms.  Precipitation
    # amount doesn't change the classification because storms are
    # inherently severe.
    if code in {95, 96, 99}:
        return "พายุฝนฟ้าคะนอง"
    # Codes 0–3 correspond to clear or cloudy conditions.
    if code == 0:
        return "ท้องฟ้าแจ่มใส"
    if code in {1, 2, 3}:
        return "มีเมฆเป็นส่วนใหญ่"
    # Fog or mist (45, 48).
    if code in {45, 48}:
        return "มีหมอก"
    # Drizzle codes (51–57) and rain codes (61–67, 80–82) are
    # differentiated by precipitation amount.
    if code in {51, 53, 55, 56, 57, 61, 63, 65, 66, 67, 80, 81, 82}:
        # Heavy rain if precipitation exceeds 10 mm.
        if precipitation >= 10.0:
            return "ฝนตกหนัก"
        # Moderate rain if precipitation between 2 and 10 mm.
        if precipitation >= 2.0:
            return "ฝนปานกลาง"
        # Light rain for precipitation < 2 mm.
        return "ฝนตกเล็กน้อย"
    # Snow codes are rare in Thailand but we include them for completeness.
    if code in {71, 73, 75, 77, 85, 86}:
        return "หิมะ"
    # Default fallback description.
    return "สภาพอากาศไม่ทราบแน่ชัด"

def get_weather_forecast(
    lat: float = WEATHER_LAT,
    lon: float = WEATHER_LON,
    days: int = 3,
    timezone: str = "Asia/Bangkok",
    timeout: int = 15,
) -> List[Tuple[str, str]]:
    """
    Fetch a daily weather forecast for the given coordinates using the
    Open‑Meteo API.  It returns a list of tuples containing the
    date (YYYY‑MM‑DD) and a concise description.  Only the next `days`
    entries are returned.

    Parameters
    ----------
    lat : float
        Latitude of the location.
    lon : float
        Longitude of the location.
    days : int
        Number of days of forecast to return.  Defaults to 3.
    timezone : str
        Timezone for date interpretation.  Defaults to Asia/Bangkok.
    timeout : int
        Timeout in seconds for the HTTP request.

    Returns
    -------
    list[tuple[str, str]]
        A list of (date, description) tuples.  If the API call
        fails, an empty list is returned.
    """
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
    """
    Read historical flow data from an Excel file.  The file should be
    named with the Buddhist Era year and stored under the 'data' folder.
    Returns the discharge (cubic metres per second) corresponding to
    today's date, or None if not available.
    """
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


# --- ดึงระดับน้ำสรรพยา ---
def get_sapphaya_data(
    province_code: str = "18",
    target_tumbon: str = "โพนางดำออก",
    target_station_name: str = "สรรพยา",
    timeout: int = 15,
    retries: int = 3,
):
    """
    Query the ThaiWater API for real‑time water level data for the
    Chao Phraya River at Pho Nang Dam Ok, Sapphaya District, Chai Nat Province.

    This function calls the official ThaiWater API endpoint to fetch
    water level information for a given province, then filters the
    results by subdistrict (ต.โพนางดำออก) and station name (สรรพยา).
    The water level (in metres above mean sea level) and the station's
    minimum bank height are returned as floats. If either piece of
    information cannot be found, the function returns (None, None).

    Parameters
    ----------
    province_code : str
        The numeric code of the province to query. Chai Nat is '18'.
    target_tumbon : str
        The Thai name of the subdistrict to filter on.
    target_station_name : str
        The Thai name of the telemetered water level station.
    timeout : int
        Timeout in seconds for the HTTP request.
    retries : int
        Number of times to retry the request in case of a transient failure.

    Returns
    -------
    tuple[float | None, float | None]
        A tuple of (water_level_msl, bank_level). If the data is not
        found or an error occurs, (None, None) is returned.
    """

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
                    # Override the bank height with a fixed value instead of
                    # retrieving it from the API.  This ensures the alert
                    # system always references a constant benchmark (13.87 m MSL).
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


# --- ดึงข้อมูลเขื่อนเจ้าพระยา (เพิ่ม Cache Busting) ---
def fetch_chao_phraya_dam_discharge(url: str, timeout: int = 30):
    """
    Fetch the current discharge from the Chao Phraya Dam.  This function
    uses a cache‑busting query parameter to ensure fresh data.
    """
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


# --- วิเคราะห์และสร้างข้อความ ---
def analyze_and_create_message(
    water_level: float,
    dam_discharge: float,
    bank_height: float,
    hist_2567: int | None = None,
    hist_2554: int | None = None,
    weather_summary: List[Tuple[str, str]] | None = None,
) -> str:
    """
    Construct a human‑readable message summarising the current water
    situation for ต.โพนางดำออก.  This version omits daily forecasts,
    separates key sections (location, water level, dam discharge,
    historical comparison and summary), and is designed for readability
    on mobile devices.

    Parameters
    ----------
    water_level : float
        Current water level at the Sapphaya station (m MSL).
    dam_discharge : float
        Current discharge of the Chao Phraya Dam (m^3/s).
    bank_height : float
        Bank height used for comparison (m MSL).
    hist_2567 : int | None
        Historical discharge for year 2567 (optional).
    hist_2554 : int | None
        Historical discharge for year 2554 (optional).
    weather_summary : list[tuple[str, str]] | None
        Deprecated.  Left for backward compatibility but ignored.

    Returns
    -------
    str
        A formatted message without weather details or municipal line.
    """
    # Calculate the remaining distance from the water surface to the top of the bank.
    distance_to_bank = bank_height - water_level
    # Determine the risk category and prepare guidance lines accordingly.
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
    # Current timestamp.
    now = datetime.now(pytz.timezone("Asia/Bangkok"))
    TIMESTAMP = now.strftime("%d/%m/%Y %H:%M")
    # Assemble the message as a list of lines.
    msg_lines: List[str] = []
    msg_lines.append(f"{ICON} {HEADER}")
    msg_lines.append(f"📍 ต.โพนางดำออก อ.สรรพยา จ.ชัยนาท")
    msg_lines.append(f"🗓️ วันที่: {TIMESTAMP} น.")
    # Water section.
    msg_lines.append("")
    msg_lines.append("🌊 ระดับน้ำ + ตลิ่ง")
    msg_lines.append(f"• ระดับน้ำ: {water_level:.2f} ม.รทก.")
    msg_lines.append(f"• ตลิ่ง: {bank_height:.2f} ม.รทก. (ต่ำกว่า {distance_to_bank:.2f} ม.)")
    # Dam discharge.
    msg_lines.append("")
    msg_lines.append("💧 ปริมาณน้ำปล่อยเขื่อนเจ้าพระยา")
    if dam_discharge is not None:
        msg_lines.append(f"{dam_discharge:,} ลบ.ม./วินาที")
    else:
        msg_lines.append("ข้อมูลไม่พร้อมใช้งาน")
    # Historical comparison.
    msg_lines.append("")
    msg_lines.append("📊 เปรียบเทียบย้อนหลัง")
    if hist_2567 is not None:
        msg_lines.append(f"• ปี 2567: {hist_2567:,} ลบ.ม./วินาที")
    if hist_2554 is not None:
        msg_lines.append(f"• ปี 2554: {hist_2554:,} ลบ.ม./วินาที")
    # Summary.
    msg_lines.append("")
    msg_lines.append("🧾 สรุปสถานการณ์")
    for line in summary_lines:
        msg_lines.append(line)
    # Return the assembled text.  Weather and municipality information are appended later.
    return "\n".join(msg_lines)


# --- สร้างข้อความ Error ---
def create_error_message(station_status, discharge_status):
    now = datetime.now(pytz.timezone('Asia/Bangkok'))
    return (
        f"⚙️❌ เกิดข้อผิดพลาดในการดึงข้อมูล ❌⚙️\n"
        f"เวลา: {now.strftime('%d/%m/%Y %H:%M')} น.\n\n"
        f"• สถานะข้อมูลระดับน้ำสรรพยา: {station_status}\n"
        f"• สถานะข้อมูลเขื่อนเจ้าพระยา: {discharge_status}\n\n"
        f"กรุณาตรวจสอบ Log บน GitHub Actions เพื่อดูรายละเอียดข้อผิดพลาดครับ"
    )


# --- ส่งข้อความ LINE ---
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


# --- Main ---
if __name__ == "__main__":
    print("=== เริ่มการทำงานระบบแจ้งเตือนน้ำ ===")
    # Fetch water level and bank height using the API
    water_level, bank_level = get_sapphaya_data()
    # Fetch dam discharge (fallback to existing endpoint)
    dam_discharge = fetch_chao_phraya_dam_discharge(DISCHARGE_URL)
    # Historical references for comparison
    hist_2567 = get_historical_from_excel(2567)
    hist_2554 = get_historical_from_excel(2554)
    if water_level is not None and bank_level is not None and dam_discharge is not None:
        # Build the core message.  We no longer append multi‑day forecasts here.
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
    # Generate an immediate weather alert via OpenWeather.
    weather_alert = get_openweather_alert()
    # Construct the final message: include a heading for the weather section
    # and always conclude with the municipality name.  If no weather alert
    # is available, simply append the municipality name.
    if weather_alert:
        final_message = (
            f"{core_message}\n\n"
            f"🌡️ พยากรณ์อากาศวันนี้:\n{weather_alert}\n\n"
            f"เทศบาลตำบลโพนางดำออก"
        )
    else:
        final_message = f"{core_message}\n\nเทศบาลตำบลโพนางดำออก"
    print("\n📤 ข้อความที่จะแจ้งเตือน:")
    print(final_message)
    print("\n🚀 ส่งข้อความไปยัง LINE...")
    send_line_broadcast(final_message)
    print("✅ เสร็จสิ้นการทำงาน")
