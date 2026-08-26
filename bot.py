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
 shame shame shame shame shame shame shame shame이미지 상단(헤더/배경 영역)의 색상 정보입니다. 차트 배경 설정에 아래 색상 코드를 적용하시면 일치시킬 수 있습니다.

* **HEX 코드:** `#2B2D30`
* **RGB 코드:** `rgb(43, 45, 48)`

**적용 팁**
* **TradingView / 차트 라이브러리:** 차트 설정의 **Canvas(캔버스) / Background(배경색)** 항목에 `#2B2D30`을 입력해 단색(Solid)으로 설정하세요.
* 만약 미세한 차이가 느껴질 경우 바로 인접한 톤인 `#26282B`도 함께 테스트해 보실 수 있습니다.
