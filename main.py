import os
import re
import json
import time
import random
import requests
import pytz
import pandas as pd
from datetime import datetime

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
                    bank_level = None
                    if station_info:
                        bank_level = station_info.get("min_bank")
                        if bank_level is None:
                            left_bank = station_info.get("left_bank")
                            right_bank = station_info.get("right_bank")
                            if left_bank is not None and right_bank is not None:
                                bank_level = (left_bank + right_bank) / 2
                            elif left_bank is not None:
                                bank_level = left_bank
                            elif right_bank is not None:
                                bank_level = right_bank
                    print(
                        f"✅ พบข้อมูลสรรพยา: ระดับน้ำ={water_level}, ระดับตลิ่ง={bank_level} (API)"
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
def analyze_and_create_message(water_level, dam_discharge, bank_height, hist_2567=None, hist_2554=None):
    """
    Construct a human‑readable message summarising the current water
    situation.  It compares the water level to the bank height and
    categorises the risk level accordingly.  Historical discharge
    statistics can be passed in for comparison.
    """
    distance_to_bank = bank_height - water_level
    ICON = ""
    HEADER = ""
    summary_text = ""
    if dam_discharge is not None and (dam_discharge > 2400 or distance_to_bank < 1.0):
        ICON = "🟥"
        HEADER = "‼️ ประกาศเตือนภัยระดับสูงสุด ‼️"
        summary_text = ("คำแนะนำ:\n"
                        "1. เตรียมพร้อมอพยพหากอยู่ในพื้นที่เสี่ยง\n"
                        "2. ขนย้ายทรัพย์สินขึ้นที่สูงโดยด่วน\n"
                        "3. งดใช้เส้นทางสัญจรริมแม่น้ำ")
    elif dam_discharge is not None and (dam_discharge > 1800 or distance_to_bank < 2.0):
        ICON = "🟨"
        HEADER = "‼️ ประกาศเฝ้าระวัง ‼️"
        summary_text = ("คำแนะนำ:\n"
                        "1. บ้านเรือนริมตลิ่งนอกคันกั้นน้ำ ให้เริ่มขนของขึ้นที่สูง\n"
                        "2. ติดตามสถานการณ์อย่างใกล้ชิด")
    else:
        ICON = "🟩"
        HEADER = "สถานะปกติ"
        summary_text = "สถานการณ์น้ำยังปกติ ใช้ชีวิตได้ตามปกติครับ"
    now = datetime.now(pytz.timezone('Asia/Bangkok'))
    TIMESTAMP = now.strftime('%d/%m/%Y %H:%M')
    msg_lines = [
        f"{ICON} {HEADER}",
        "",
        "📍 รายงานสถานการณ์น้ำเจ้าพระยา (สถานี สรรพยา)",
        f"🗓️ วันที่: {TIMESTAMP} น.",
        "",
        "🌊 ระดับน้ำ + ระดับตลิ่ง",
        f"  • สรรพยา: {water_level:.2f} ม.รทก.",
        f"  • ตลิ่ง: {bank_height:.2f} ม.รทก. (ต่ำกว่า {distance_to_bank:.2f} ม.)",
        "",
        "💧 ปริมาณน้ำปล่อยเขื่อนเจ้าพระยา",
    ]
    if dam_discharge is not None:
        msg_lines.append(f"  {dam_discharge:,} ลบ.ม./วินาที")
    else:
        msg_lines.append("  ข้อมูลไม่พร้อมใช้งาน")
    msg_lines += [
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
        final_message = analyze_and_create_message(
            water_level,
            dam_discharge,
            bank_level,
            hist_2567,
            hist_2554,
        )
    else:
        station_status = "สำเร็จ" if water_level is not None else "ล้มเหลว"
        discharge_status = "สำเร็จ" if dam_discharge is not None else "ล้มเหลว"
        final_message = create_error_message(station_status, discharge_status)
    print("\n📤 ข้อความที่จะแจ้งเตือน:")
    print(final_message)
    print("\n🚀 ส่งข้อความไปยัง LINE...")
    send_line_broadcast(final_message)
    print("✅ เสร็จสิ้นการทำงาน")
