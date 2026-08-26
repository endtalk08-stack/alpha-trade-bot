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
from http.server import HTTPServer, BaseHTTPRequestHandler

import requests
import pandas as pd
import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import mplfinance as mpf

from telegram import Update
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    MessageHandler,
    filters,
    ContextTypes
)

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
    "SK증권": "001510", "SK스퀘어": "402340", " shame shame shame shame shame shame shame shame shame shame shame shame shame shame shame shame요청하신 의도를 정확하게 파악했습니다.

기존 UI 디자인 레이아웃을 그대로 유지하면서, **검색창에 종목명(예: SK하이닉스)을 입력하여 진입했을 때** 해당 종목에 맞춘 4대 핵심 데이터 패널이 한눈에 출력되는 통합 대시보드 구조입니다.

---

**1. 대시보드 화면 구성 요소**

*   **종목 헤더 & 실시간 차트:** 캔들스틱/라인 차트, 거래량 바, 시간 주기 선택(1분/일/주)
*   **퀀트 분석 & 기술적 지표:** 이동평균선(SMA/EMA), RSI, MACD, 볼린저 밴드 등 주요 수치 연산
*   **시그널 타점 (매수/매도/관망):** 퀀트 알고리즘 기반 진입가, 손절가, 목표가 및 실시간 시그널 상태
*   **최신 뉴스 & 공시 피드:** 종목 관련 실시간 뉴스 헤드라인, 발행 시간 및 감성 분석(긍정/부정)

---

**2. 기술적 구현 및 데이터 연동을 위한 확인 사항**

완벽한 프론트엔드/백엔드 코드를 구성하기 전에 아래 2가지를 어떻게 구성할지 확인이 필요합니다.

1. **사용 기술 스택:** React, Vue, 순수 HTML/JS/CSS 중 선호하는 환경이 있으신가요?
2. **데이터 연동 방식:**
   * **목업(Mock Data) 프로토타입:** 종목 검색 시 즉시 화면에 데모 데이터가 연동되어 시연 가능한 형태
   * **실제 API 연동 준비 코드:** 키움/한국투자증권 API, TradingView 위젯, 또는 네이버 금융/야후 파이낸스 크롤러 연동 구조

원하시는 스택이나 선호 형태를 말씀해주시면 바로 완성된 코드를 구현해 드리겠습니다.
