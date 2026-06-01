"""
매일 실행되는 데이터 수집 스크립트
- KAMIS API: 농산물 소매가격
- 기상청 API: 종관기상관측(ASOS) 일자료
- 결과: merged.parquet 갱신 (기존 데이터 + 신규 데이터 병합)
"""

import os
import requests
import pandas as pd
from datetime import datetime, timedelta
import time

# ==========================================
# 설정
# ==========================================
KAMIS_KEY = os.environ["KAMIS_API_KEY"]
KMA_KEY   = os.environ["KMA_API_KEY"]

# 노트북과 동일한 품목 코드
# 221:배추, 222:무, 223:상추, 224:딸기, 225:토마토, 226:시금치, 231:호박, 233:생강
ITEM_CODES = {
    "221": {"name": "배추", "category": "200"},
    "222": {"name": "무",   "category": "200"},
    "223": {"name": "상추", "category": "200"},
    "224": {"name": "딸기", "category": "200"},
    "225": {"name": "토마토","category": "200"},
    "226": {"name": "시금치","category": "200"},
    "231": {"name": "호박", "category": "200"},
    "233": {"name": "생강", "category": "200"},
}

# 기상청 관측소 (서울 108번 기준 — 필요시 변경)
KMA_STN = 108

PARQUET_PATH = "merged.parquet"


# ==========================================
# 1. 기존 parquet 로드 & 마지막 날짜 확인
# ==========================================
def load_existing():
    try:
        df = pd.read_parquet(PARQUET_PATH)
        df["date"] = pd.to_datetime(df["date"])
        last_date = df["date"].max()
        print(f"기존 데이터 마지막 날짜: {last_date.date()}")
        return df, last_date
    except Exception:
        print("기존 parquet 없음 — 새로 생성합니다")
        return pd.DataFrame(), None


# ==========================================
# 2. KAMIS 가격 수집
# ==========================================
def fetch_kamis(start_date: datetime, end_date: datetime) -> pd.DataFrame:
    rows = []
    current = start_date

    while current <= end_date:
        yyyy = current.strftime("%Y")
        mm   = current.strftime("%m")
        dd   = current.strftime("%d")

        url = (
            "https://www.kamis.or.kr/service/price/xml.do"
            "?action=dailyPriceByCategoryList"
            f"&p_cert_key={KAMIS_KEY}"
            "&p_cert_id=admin"
            "&p_returntype=json"
            f"&p_yyyy={yyyy}&p_mm={mm}&p_dd={dd}"
            "&p_itemcategorycode=200"   # 채소류
            "&p_convert_kg_yn=Y"
        )

        try:
            res = requests.get(url, timeout=10)
            data = res.json()

            items = data.get("data", {})
            if isinstance(items, dict):
                for item in items.get("item", []):
                    code = str(item.get("itemcode", ""))
                    if code in ITEM_CODES:
                        price_str = item.get("price", "0").replace(",", "")
                        try:
                            price = float(price_str)
                        except ValueError:
                            price = None
                        rows.append({
                            "date":      current,
                            "item_code": int(code),
                            "item_name": ITEM_CODES[code]["name"],
                            "retail_price": price,
                        })
        except Exception as e:
            print(f"KAMIS 오류 {current.date()}: {e}")

        current += timedelta(days=1)
        time.sleep(0.2)   # API 과호출 방지

    df = pd.DataFrame(rows)
    print(f"KAMIS 수집: {len(df)}행")
    return df


# ==========================================
# 3. 기상청 ASOS 일자료 수집
# ==========================================
def fetch_kma(start_date: datetime, end_date: datetime) -> pd.DataFrame:
    rows = []
    current = start_date

    while current <= end_date:
        tm = current.strftime("%Y%m%d")
        url = (
            "https://apihub.kma.go.kr/api/typ01/url/kma_sfcdd.php"
            f"?tm={tm}&stn={KMA_STN}&help=0&authKey={KMA_KEY}"
        )

        try:
            res = requests.get(url, timeout=10)
            text = res.text.strip()

            # 응답 형식: #날짜 지점 평균기온 최저기온 최고기온 강수량 일조시간 습도 ...
            for line in text.splitlines():
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                parts = line.split()
                if len(parts) < 8:
                    continue
                try:
                    rows.append({
                        "date":          pd.to_datetime(parts[0], format="%Y%m%d"),
                        "avg_temp":      float(parts[2]) if parts[2] != "-9999" else None,
                        "min_temp":      float(parts[3]) if parts[3] != "-9999" else None,
                        "max_temp":      float(parts[4]) if parts[4] != "-9999" else None,
                        "precipitation": float(parts[5]) if parts[5] != "-9999" else 0.0,
                        "sunshine":      float(parts[7]) if parts[7] != "-9999" else None,
                        "humidity":      float(parts[8]) if len(parts) > 8 and parts[8] != "-9999" else None,
                    })
                except (ValueError, IndexError):
                    continue

        except Exception as e:
            print(f"기상청 오류 {current.date()}: {e}")

        current += timedelta(days=1)
        time.sleep(0.2)

    df = pd.DataFrame(rows)
    print(f"기상청 수집: {len(df)}행")
    return df


# ==========================================
# 4. 병합 & 저장
# ==========================================
def main():
    existing_df, last_date = load_existing()

    # 수집 시작일 결정
    if last_date is not None:
        start = last_date + timedelta(days=1)
    else:
        start = datetime(2020, 1, 1)   # 기존 데이터 없으면 2020년부터

    # 어제까지만 수집 (오늘은 데이터 미확정)
    end = datetime.now() - timedelta(days=1)
    end = end.replace(hour=0, minute=0, second=0, microsecond=0)

    if start > end:
        print("이미 최신 상태입니다. 종료.")
        return

    print(f"수집 기간: {start.date()} ~ {end.date()}")

    # API 수집
    price_df   = fetch_kamis(start, end)
    weather_df = fetch_kma(start, end)

    if price_df.empty:
        print("신규 가격 데이터 없음. 종료.")
        return

    # 날씨 join
    new_df = price_df.merge(weather_df, on="date", how="left")

    # 기존 데이터와 합치기
    if not existing_df.empty:
        combined = pd.concat([existing_df, new_df], ignore_index=True)
        combined = combined.drop_duplicates(subset=["date", "item_code"], keep="last")
        combined = combined.sort_values(["date", "item_code"]).reset_index(drop=True)
    else:
        combined = new_df

    # 저장
    combined.to_parquet(PARQUET_PATH, index=False)
    print(f"저장 완료: {PARQUET_PATH} ({len(combined)}행, 마지막 날짜: {combined['date'].max().date()})")


if __name__ == "__main__":
    main()
