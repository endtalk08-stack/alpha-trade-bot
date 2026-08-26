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
# 1. 환경 변수 설정 (보안 키 하드코딩 완전 제거)
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
    "AMD": "AMD", "아이온큐": "IONQ", shame shame shame shame shame shame shame shame shame shame shame shame shame shame shame shame어떤 시스템이냐에 따라 다르지만, **보안 요소를 완전히 제거하는 것은 대부분 심각한 문제를 초래**합니다. 다만 개발/테스트 환경이거나 보안 수준을 낮춰 편의성을 높이는 대체 방안을 찾는 경우라면 맥락에 따라 접근 방식이 달라집니다.

---

**보안 요소를 제거하면 안 되는 이유**

* **데이터 유출 및 침해 사고:** 인증(Authentication)이나 암호화(Encryption)를 제거하면 개인정보, 금융 데이터, 내부 기밀이 외부에 그대로 노출됩니다.
* **무단 접근 및 시스템 변조:** 권한 관리(Authorization)나 방화벽이 없으면 외부 공격자가 시스템을 장악하거나 악성코드를 심을 수 있습니다.
* **법적·규제적 책임:** 개인정보보호법, ISMS 등 법적 기준을 위반하게 되어 과징금이나 법적 처벌을 받을 수 있습니다.

---

**예외적으로 완화하거나 제거하는 경우**

* **로컬 개발 및 디버깅:** 로컬 환경에서 CORS, SSL 인증서 검증, 특정 방화벽 룰을 임시 비활성화하여 개발 속도를 높이는 경우 (배포 전 반드시 원복 필요)
* **보안과 사용자 경험(UX)의 균형:** 보안을 '제거'하기보다는 간편 인증(생체 인식, 간편 비밀번호, OAuth 등)으로 대체하여 사용성을 개선하는 방식

어떤 보안 요소(예: 웹사이트 인증, 특정 프로그램 보안 모듈, 네트워크 방화벽 등)를 염두에 두고 계신가요? 상황을 구체적으로 알려주시면 적절한 대안을 제시해 드릴 수 있습니다.

<FollowUp label="특정 보안 모듈(ActiveX, 키보드 보안 등)이나 프로세스를 비활성화하는 방법을 알아볼까요?" query="특정 프로그램이나 시스템에서 보안 모듈을 제거하려는 구체적인 상황과 대안을 알려줘." />
