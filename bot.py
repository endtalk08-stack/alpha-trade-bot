import os
import re
import io
import gc
import json
import urllib.parse
import xml.etree.ElementTree as ET
import html
import threading
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from http.server import HTTPServer, BaseHTTPRequestHandler

import requests
import pandas as pd
import numpy as np

# ----------------------------------------------------
# 1. 기본 설정 및 종목 매핑 (유료 API 키 0개)
# ----------------------------------------------------
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "")

CRYPTO_MAP = {
    "BTC": "BTCUSDT", "비트코인": "BTCUSDT", "비트": "BTCUSDT",
    "ETH": "ETHUSDT", "이더리움": "ETHUSDT", "이더": "ETHUSDT",
    "XRP": "XRPUSDT", "리플": "XRPUSDT",
    "DOGE": "DOGEUSDT", "도지코인": "DOGEUSDT", "도지": "DOGEUSDT",
    "SOL": "SOLUSDT", "솔라나": "SOLUSDT",
    "ADA": "ADAUSDT", "에이다": "ADAUSDT",
    "AVAX": "AVAXUSDT", "아발란체": "AVAXUSDT",
    "SKY": "SKYUSDT", "스카이": "SKYUSDT"
}

US_STOCK_MAP = {
    "엔비디아": "NVDA", "NVDA": "NVDA",
    "테슬라": "TSLA", "TSLA": "TSLA",
    "애플": "AAPL", "AAPL": "AAPL",
    "마이크로소프트": "MSFT", "마소": "MSFT", "MSFT": "MSFT",
    "아마존": "AMZN", "AMZN": "AMZN",
    "구글": "GOOGL", "알파벳": "GOOGL", "GOOGL": "GOOGL",
    "메타": "META", "META": "META",
    "AMD": "AMD", "아이온큐": "IONQ", "IONQ": "IONQ",
    "팔란티어": "PLTR", "PLTR": "PLTR",
    "대만반도체": "TSM", "TSM": "TSM", "TSMC": "TSM",
    "QQQ": "QQQ", "SPY": "SPY", "SOXL": "SOXL", "TQQQ": "TQQQ"
}

DOMESTIC_FAST_MAP = {
    "SK하이닉스": "000660", "하이닉스": "000660",
    "삼성전자": "005930", "삼전": "005930", "삼성전기": "009150", "삼성중공업": "010140",
    "SK증권": "001510", "SK스퀘어": "402340", "솔트룩스": "304100",
    "알테오젠": "196170", "에코프로": "086520", "에코프로비엠": "247540",
    "레인보우로보틱스": "277810", "카카오": "035720", "네이버": "035420",
    "NAVER": "035420", "현대차": "005380", "기아": "000270",
    "포스코홀딩스": "005490", "POSCO홀딩스": "005490", "LG에너지솔루션": "373220",
    "한화오션": "042660", "두산에너빌리티": "034020", "한화에어로스페이스": "012450",
    "현대모비스": "012330", "셀트리온": "068270", "한미반도체": "042700",
    "씨젠": "096530", "HLB": "028300", "에코프로머티": "450080",
    "리가켐바이오": "141080", "엔켐": "348370", "삼천당제약": "000250",
    "휴젤": "145020", "클래시스": "214150", "리노공업": "058470"
}

WORLD_INDICES_CONFIG = [
    {"name": "다우산업", "sym": "^DJI"},
    {"name": "나스닥종합", "sym": "^IXIC"},
    {"name": "S&P 500", "sym": "^GSPC"},
    {"name": "니케이 225", "sym": "^N225"},
    {"name": "상해종합", "sym": "000001.SS"},
    {"name": "항셍", "sym": "^HSI"},
    {"name": "영국 FTSE", "sym": "^FTSE"},
    {"name": "프랑스 CAC", "sym": "^FCHI"},
    {"name": "독일 DAX", "sym": "^GDAXI"}
]

# ----------------------------------------------------
# 2. 데이터 크롤링 모듈 (병렬 멀티스레딩)
# ----------------------------------------------------
def find_stock_code(query):
    clean_q = query.strip().replace(" ", "").upper()
    if clean_q.isdigit() and len(clean_q) == 6:
        return clean_q, clean_q

    for name, code in DOMESTIC_FAST_MAP.items():
        if name.replace(" ", "").upper() == clean_q:
            return code, name

    try:
        url = f"https://m.stock.naver.com/front-api/search/autoComplete?query={urllib.parse.quote(query.strip())}&target=stock"
        res = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=2.0).json()
        items = res.get("result", {}).get("items", [])
        if items:
            for it in items:
                name = str(it.get("name", "")).strip()
                code = str(it.get("code", "")).strip()
                if name.replace(" ", "").upper() == clean_q:
                    return code, name
            first = items[0]
            return str(first.get("code", "")).strip(), str(first.get("name", "")).strip()
    except Exception:
        pass
    return None, query.strip()

def get_naver_realtime_price(code):
    try:
        url = f"https://polling.finance.naver.com/api/realtime/domestic/stock/{code}"
        res = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=2.0).json()
        datas = res.get("datas", [])
        if datas:
            p = str(datas[0].get("closePrice", "")).replace(",", "").strip()
            if p and p.replace(".", "").isdigit():
                return float(p), f"{int(float(p)):,} 원"
    except Exception:
        pass
    return None, None

def fetch_domestic_candles(code, anchor_price=None):
    df_result = None
    for suffix in [".KS", ".KQ"]:
        try:
            url = f"https://query1.finance.yahoo.com/v8/finance/chart/{code}{suffix}?range=5y&interval=1d"
            res`bot.py` 파일의 **583번 라인 근처에 따옴표(`"`) 개수가 맞지 않아 발생하는 문법 오류(SyntaxError)**입니다.

에러 메시지를 보면 삼중 따옴표(`"""`)가 제대로 닫히지 않았거나(unterminated triple-quoted string), 따옴표가 잘못 중첩되어 파이썬이 파일 끝(606번 라인)까지 문자열이 끝나지 않은 것으로 인식하고 있습니다.

---

**원인 및 해결 방법**

1. **따옴표 개수 확인**
   * 에러 로그에 표시된 줄을 보면 따옴표가 총 7개(앞 3개 + 뒤 4개)로 찍혀 있습니다:
     ```python
     # 잘못된 예시 (따옴표 개수 불일치)
     """ThreadPoolExecutor를 이용한 병렬 수집 실행기""""
     ```
   * 양쪽 따옴표 개수를 정확히 **3개씩** 맞추거나, 단순 한 줄 주석이라면 `#`으로 변경하세요.

2. **수정 예시**
   * **삼중 따옴표로 유지할 경우:**
     ```python
     """ThreadPoolExecutor를 이용한 병렬 수집 실행기"""
     ```
   * **일반 주석으로 변경할 경우 (권장):**
     ```python
     # ThreadPoolExecutor를 이용한 병렬 수집 실행기
     ```

코드를 수정한 뒤 다시 커밋하고 배포(Deploy)를 진행하면 정상 실행됩니다.
