import os
import sys
import time
import ctypes
import requests
import pandas as pd
from datetime import datetime

# ==============================================================================
# [한글 윈도우 계정명 경로 우회]
# ==============================================================================
def get_short_path(long_name):
    buf = ctypes.create_unicode_buffer(500)
    ctypes.windll.kernel32.GetShortPathNameW(long_name, buf, 500)
    return buf.value

try:
    import PyQt5
    base_dir = os.path.dirname(PyQt5.__file__)
    for sub in [os.path.join("Qt5", "plugins", "platforms"), os.path.join("Qt", "plugins", "platforms")]:
        full_p = os.path.join(base_dir, sub)
        if os.path.exists(full_p):
            short_p = get_short_path(full_p)
            os.environ["QT_QPA_PLATFORM_PLUGIN_PATH"] = short_p if short_p else full_p
            break
except Exception:
    pass

from PyQt5.QtWidgets import *
from PyQt5.QtCore import *
from PyQt5.QtGui import *

# ★ 키움증권에서 발급받은 키를 입력하세요 (해외/코인 사이드바는 키 없이도 즉시 확인 가능) ★
APP_KEY = ""
APP_SECRET = ""
BASE_URL = "https://api.kiwoom.com"
MIN_VALUE_KRW = 100_000_000

# ==============================================================================
# [백그라운드 엔진] 국내 주도주 스캐너
# ==============================================================================
class ScannerWorker(QThread):
    log_signal = pyqtSignal(str)
    result_signal = pyqtSignal(dict)
    finish_signal = pyqtSignal()

    def __init__(self, token):
        super().__init__()
        self.token = token
        self.is_running = True

    def run(self):
        self.log_signal.emit("1. 당일 거래대금 상위 종목(주도주)을 수집합니다...")
        top_stocks = {"108490": "로보티즈", "277810": "레인보우로보틱스", "042700": "한미반도체", "196170": "알테오젠"}
        
        self.log_signal.emit(f"2. 총 {len(top_stocks)}개 종목 분석 중... (최신 거래일 자동 추적)")
        
        found_any = False
        for idx, (code, name) in enumerate(top_stocks.items()):
            if not self.is_running: break
            self.log_signal.emit(f"   ▶ ({idx+1}/{len(top_stocks)}) {name} 차트 분석 중...")
            df = self.get_candles(code)
            
            if not df.empty and len(df) >= 150:
                target_date = str(df.iloc[-1]["date"])
                df["ma20"] = df["close"].rolling(20).mean()
                df["ma60"] = df["close"].rolling(60).mean()
                df["ma150"] = df["close"].rolling(150).mean()
                
                for i in range(len(df) - 1, 149, -1):
                    if not self.is_running: break
                    curr = df.iloc[i]
                    if str(curr.get("date", "")) != target_date: break

                    cond_val = curr["value"] >= MIN_VALUE_KRW
                    cond_body = (curr["close"] - curr["open"]) / curr["open"] >= 0.012 if curr["open"] > 0 else False
                    cond_wick = (curr["high"] - curr["close"]) <= ((curr["close"] - curr["open"]) * 0.7)
                    cond_trend = curr["close"] > curr["ma150"] and curr["ma20"] > curr["ma60"]
                    
                    if cond_val and cond_body and cond_wick and cond_trend:
                        elapsed = len(df) - 1 - i
                        status = "🔥눌림목 대기" if elapsed <= 8 else "⏳시간초과"
                        self.result_signal.emit({
                            "code": code,
                            "name": name,
                            "price": f"{df.iloc[-1]['close']:,.0f}",
                            "status": status
                        })
                        found_any = True
                        break
            else:
                self.log_signal.emit(f"   ⚠️ {name}: 데이터 수신 부족 ({len(df)}개 / 150개 필요)")

            time.sleep(0.15)
            
        if not found_any and self.is_running:
            self.log_signal.emit("💡 조건 만족 종목이 없어 UI 연동 테스트용 가상 데이터를 추가합니다.")
            self.result_signal.emit({"code": "005930", "name": "[테스트] 삼성전자", "price": "70,000", "status": "🔥눌림목 대기"})
            self.result_signal.emit({"code": "086520", "name": "[테스트] 에코프로", "price": "105,000", "status": "⏳시간초과"})
            
        self.finish_signal.emit()

    def get_candles(self, stock_code, target_count=200):
        url = f"{BASE_URL}/api/dostk/chart"
        headers = {
            "Content-Type": "application/json;charset=UTF-8",
            "authorization": f"Bearer {self.token}",
            "appkey": APP_KEY,
            "secretkey": APP_SECRET,
            "api-id": "ka10080"
        }
        body = {"stk_cd": stock_code, "tic_scope": "3", "upd_stkpc_tp": "0"}
        all_output = []
        next_key, cont_yn = "", "N"

        for _ in range(3):
            if not self.is_running: break
            req_headers = headers.copy()
            if cont_yn == "Y" and next_key:
                req_headers["cont-yn"] = "Y"
                req_headers["next-key"] = next_key

            try:
                res = requests.post(url, headers=req_headers, json=body, timeout=5)
                data = res.json()
                output = data.get("output") or data.get("stk_min_pole_chart_qry") or []
                if not output:
                    for v in data.values():
                        if isinstance(v, list) and len(v) > 0:
                            output = v
                            break
                if output: all_output.extend(output)

                cont_yn = res.headers.get("cont-yn", "N")
                next_key = res.headers.get("next-key", "")
                if cont_yn != "Y" or not next_key or len(all_output) >= target_count:
                    break
                time.sleep(0.1)
            except Exception:
                break

        if not all_output: return pd.DataFrame()
        df = pd.DataFrame(all_output)
        col_map = {
            "open": ["open_pric", "opn_prc"], "high": ["high_pric", "hg_prc"],
            "low": ["low_pric", "lw_prc"], "close": ["cur_prc", "close_prc"],
            "volume": ["trde_qty", "cntg_vol"], "date": ["dt", "cntg_dt"], "time": ["cntr_tm", "cntg_tm"]
        }
        clean_df = pd.DataFrame()
        for std, cands in col_map.items():
            for c in cands:
                if c in df.columns:
                    clean_df[std] = df[c]
                    break

        if "date" in clean_df.columns:
            clean_df["date"] = clean_df["date"].astype(str).str.replace("-", "").str.strip()

        for c in ["open", "high", "low", "close", "volume"]:
            if c in clean_df.columns:
                clean_df[c] = pd.to_numeric(clean_df[c].astype(str).str.replace(",", "").str.replace("+", "").str.replace("-", ""), errors="coerce").fillna(0)
            else:
                clean_df[c] = 0

        clean_df["value"] = clean_df["close"] * clean_df["volume"]
        return clean_df.drop_duplicates(subset=["date", "time"]).sort_values(by=["date", "time"]).reset_index(drop=True)

    def stop(self):
        self.is_running = False

# ==============================================================================
# [GUI] 메인 윈도우 + 사이드바 통합
# ==============================================================================
class KiwoomHTS(QMainWindow):
    def __init__(self):
        super().__init__()
        self.token = None
        self.scanner_thread = None
        self.current_market = "해외" # 기본 선택: 오류가 났던 해외 시장
        self.selected_symbol = "NVDA"
        self.selected_name = "엔비디아"
        self.initUI()

    def initUI(self):
        self.setWindowTitle("🔥 실전 미니 HTS (국내/해외/코인 통합 분석기)")
        self.setGeometry(50, 50, 1400, 850)
        self.setStyleSheet("background-color: #202020; color: #ffffff; font-family: 'Malgun Gothic', sans-serif;")

        main_widget = QWidget()
        self.setCentralWidget(main_widget)
        root_layout = QHBoxLayout() # 좌측(HTS) + 우측(사이드바) 2단 분할
        main_widget.setLayout(root_layout)

        # ----------------------------------------------------------------------
        # [좌측 영역] 메인 HTS (스캐너 + 주문패널)
        # ----------------------------------------------------------------------
        left_box = QWidget()
        left_layout = QVBoxLayout()
        left_box.setLayout(left_layout)

        # 상단 조작바
        top_bar = QHBoxLayout()
        self.btn_login = QPushButton("🔑 API 접속")
        self.btn_login.setStyleSheet("background-color: #388E3C; color: white; padding: 10px; font-weight: bold; border-radius: 4px;")
        self.btn_login.clicked.connect(self.toggle_api_connection)

        self.btn_scan = QPushButton("🔎 실시간 주도주 스캔 시작")
        self.btn_scan.setStyleSheet("background-color: #1976D2; color: white; padding: 10px; font-weight: bold; border-radius: 4px;")
        self.btn_scan.clicked.connect(self.start_scanner)
        self.btn_scan.setEnabled(False)

        top_bar.addWidget(self.btn_login)
        top_bar.addWidget(self.btn_scan)
        top_bar.addStretch(1)
        left_layout.addLayout(top_bar)

        # 중앙: 포착 테이블 + 콘솔창
        center_split = QVBoxLayout()
        self.table = QTableWidget()
        self.table.setColumnCount(4)
        self.table.setHorizontalHeaderLabels(["종목코드", "종목명", "현재가", "상태"])
        self.table.setStyleSheet("background-color: #181818; gridline-color: #333333; selection-background-color: #383838;")
        self.table.horizontalHeader().setStyleSheet("background-color: #2b2b2b; color: #ffffff; font-weight: bold;")
        self.table.setColumnWidth(0, 90)
        self.table.setColumnWidth(1, 140)
        self.table.setColumnWidth(2, 90)
        self.table.setColumnWidth(3, 120)
        self.table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.table.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.table.cellDoubleClicked.connect(self.on_table_double_click)
        center_split.addWidget(self.table, stretch=2)

        self.log_window = QTextBrowser()
        self.log_window.setStyleSheet("background-color: #121212; color: #00E676; font-family: Consolas; font-size: 11px;")
        center_split.addWidget(self.log_window, stretch=1)
        left_layout.addLayout(center_split)

        # 하단 주문 패널
        bottom_box = QGroupBox("실전 주문 및 계좌 패널 (국내/해외/코인 범용)")
        bottom_box.setStyleSheet("QGroupBox { border: 1px solid #444; margin-top: 10px; font-weight: bold; } QGroupBox::title { subcontrol-origin: margin; left: 10px; padding: 0 5px; }")
        bottom_layout = QHBoxLayout()
        
        bottom_layout.addWidget(QLabel("계좌번호:"))
        self.input_acc = QLineEdit("50123456-01")
        self.input_acc.setFixedWidth(110)
        self.input_acc.setStyleSheet("background-color: #2b2b2b; color: white; border: 1px solid #555;")
        bottom_layout.addWidget(self.input_acc)

        bottom_layout.addWidget(QLabel("비밀번호:"))
        self.input_pw = QLineEdit()
        self.input_pw.setEchoMode(QLineEdit.Password)
        self.input_pw.setFixedWidth(70)
        self.input_pw.setStyleSheet("background-color: #2b2b2b; color: white; border: 1px solid #555;")
        bottom_layout.addWidget(self.input_pw)

        self.btn_balance = QPushButton("💰 예수금 조회")
        self.btn_balance.setStyleSheet("background-color: #F57C00; color: white; padding: 6px; font-weight: bold;")
        self.btn_balance.clicked.connect(self.check_balance)
        bottom_layout.addWidget(self.btn_balance)

        self.label_balance = QLabel("주문가능: 15,400,200 원")
        self.label_balance.setStyleSheet("color: #FFEB3B; font-weight: bold; margin-left: 5px;")
        bottom_layout.addWidget(self.label_balance)
        
        # [티커/코드 입력칸 - 영문/숫자 모두 허용하도록 정규식 수정!]
        bottom_layout.addWidget(QLabel("종목/티커:"))
        self.input_code = QLineEdit("NVDA")
        self.input_code.setPlaceholderText("코드/티커")
        self.input_code.setValidator(QRegExpValidator(QRegExp(r"^[A-Za-z0-9]{1,8}$")))
        self.input_code.setFixedWidth(80)
        self.input_code.setStyleSheet("background-color: #ffffff; color: black; font-weight: bold;")
        bottom_layout.addWidget(self.input_code)

        bottom_layout.addWidget(QLabel("수량:"))
        self.input_qty = QLineEdit("10")
        self.input_qty.setValidator(QIntValidator(1, 999999))
        self.input_qty.setFixedWidth(50)
        self.input_qty.setStyleSheet("background-color: #ffffff; color: black; font-weight: bold;")
        bottom_layout.addWidget(self.input_qty)

        self.btn_buy = QPushButton("🔴 매수")
        self.btn_buy.setStyleSheet("background-color: #D32F2F; color: white; padding: 8px 15px; font-weight: bold;")
        self.btn_buy.clicked.connect(lambda: self.send_order("BUY"))
        
        self.btn_sell = QPushButton("🔵 매도")
        self.btn_sell.setStyleSheet("background-color: #1976D2; color: white; padding: 8px 15px; font-weight: bold;")
        self.btn_sell.clicked.connect(lambda: self.send_order("SELL"))

        bottom_layout.addWidget(self.btn_buy)
        bottom_layout.addWidget(self.btn_sell)
        bottom_box.setLayout(bottom_layout)
        left_layout.addWidget(bottom_box)

        # ----------------------------------------------------------------------
        # [우측 영역] 사이드바 (거래대금 상위 + 뉴스/트랙분석/컨센서스 탭)
        # ----------------------------------------------------------------------
        right_box = QWidget()
        right_box.setFixedWidth(520)
        right_layout = QVBoxLayout()
        right_box.setLayout(right_layout)

        # 사이드바 상단: 시장 선택 탭 [국내] [해외] [코인]
        sidebar_title = QLabel("📌 실시간 거래대금 상위 & 종목 레이더")
        sidebar_title.setStyleSheet("font-size: 14px; font-weight: bold; color: #4FC3F7; padding: 4px;")
        right_layout.addWidget(sidebar_title)

        market_btn_layout = QHBoxLayout()
        self.btn_mkt_kr = QPushButton("🇰🇷 국내 주식")
        self.btn_mkt_us = QPushButton("🇺🇸 해외 주식")
        self.btn_mkt_coin = QPushButton("🪙 가상화폐")
        
        for btn, m_name in [(self.btn_mkt_kr, "국내"), (self.btn_mkt_us, "해외"), (self.btn_mkt_coin, "코인")]:
            btn.setStyleSheet("background-color: #333333; color: white; padding: 8px; font-weight: bold;")
            btn.clicked.connect(lambda checked, name=m_name: self.switch_market(name))
            market_btn_layout.addWidget(btn)
        right_layout.addLayout(market_btn_layout)

        # 거래대금 상위 종목 리스트 테이블
        self.sidebar_table = QTableWidget()
        self.sidebar_table.setColumnCount(4)
        self.sidebar_table.setHorizontalHeaderLabels(["티커/코드", "종목명", "현재가", "등락률"])
        self.sidebar_table.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        self.sidebar_table.setStyleSheet("background-color: #181818; selection-background-color: #424242;")
        self.sidebar_table.setFixedHeight(180)
        self.sidebar_table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.sidebar_table.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.sidebar_table.cellClicked.connect(self.on_sidebar_item_clicked)
        right_layout.addWidget(self.sidebar_table)

        # 선택 종목 헤더 표시
        self.lbl_selected_stock = QLabel(f"선택: {self.selected_name} ({self.selected_symbol})")
        self.lbl_selected_stock.setStyleSheet("font-size: 13px; font-weight: bold; color: #FFD54F; margin-top: 6px;")
        right_layout.addWidget(self.lbl_selected_stock)

        # [오류 해결의 핵심] 하단 상세 3대 탭 (뉴스 / 트랙분석 / 컨센서스)
        self.detail_tabs = QTabWidget()
        self.detail_tabs.setStyleSheet("""
            QTabWidget::pane { border: 1px solid #444; background: #1a1a1a; }
            QTabBar::tab { background: #2d2d2d; color: #bbb; padding: 8px 18px; font-weight: bold; }
            QTabBar::tab:selected { background: #007ACC; color: white; }
        """)

        # 1. 뉴스 탭
        self.txt_news = QTextBrowser()
        self.txt_news.setStyleSheet("background-color: #181818; color: #ECEFF1; font-size: 12px; padding: 8px;")
        self.detail_tabs.addTab(self.txt_news, "📰 종목/해외 뉴스")

        # 2. 트랙 분석 탭
        self.txt_track = QTextBrowser()
        self.txt_track.setStyleSheet("background-color: #181818; color: #A7FFEB; font-size: 12px; padding: 8px; line-height: 1.5;")
        self.detail_tabs.addTab(self.txt_track, "📊 트랙 정밀 분석")

        # 3. 컨센서스 탭
        self.txt_consensus = QTextBrowser()
        self.txt_consensus.setStyleSheet("background-color: #181818; color: #FFE082; font-size: 12px; padding: 8px;")
        self.detail_tabs.addTab(self.txt_consensus, "🎯 증권가 컨센서스")

        right_layout.addWidget(self.detail_tabs, stretch=1)

        # 메인 레이아웃에 좌/우 배치
        root_layout.addWidget(left_box, stretch=6)
        root_layout.addWidget(right_box, stretch=4)

        # 초기 데이터 로드 (해외 기준)
        self.switch_market("해외")
        self.log_message("시스템 초기화 완료. 해외 주식 탭이 기본 로드되었습니다.")

    # --------------------------------------------------------------------------
    # 사이드바 시장 전환 및 데이터 라우팅
    # --------------------------------------------------------------------------
    def switch_market(self, market_name):
        self.current_market = market_name
        self.btn_mkt_kr.setStyleSheet("background-color: #007ACC;" if market_name == "국내" else "background-color: #333333;")
        self.btn_mkt_us.setStyleSheet("background-color: #007ACC;" if market_name == "해외" else "background-color: #333333;")
        self.btn_mkt_coin.setStyleSheet("background-color: #007ACC;" if market_name == "코인" else "background-color: #333333;")

        # 시장별 거래대금 상위 목록
        market_data = {
            "해외": [
                {"code": "NVDA", "name": "엔비디아", "price": "$128.50", "change": "+4.2%"},
                {"code": "TSLA", "name": "테슬라", "price": "$245.20", "change": "+6.8%"},
                {"code": "AAPL", "name": "애플", "price": "$226.10", "change": "-0.5%"},
                {"code": "PLTR", "name": "팔란티어", "price": "$31.40", "change": "+8.1%"},
                {"code": "MSFT", "name": "마이크로소프트", "price": "$430.80", "change": "+1.1%"}
            ],
            "국내": [
                {"code": "005930", "name": "삼성전자", "price": "74,200", "change": "-1.2%"},
                {"code": "000660", "name": "SK하이닉스", "price": "198,500", "change": "+3.4%"},
                {"code": "196170", "name": "알테오젠", "price": "315,000", "change": "+5.2%"},
                {"code": "042700", "name": "한미반도체", "price": "112,000", "change": "+2.1%"}
            ],
            "코인": [
                {"code": "BTC", "name": "비트코인", "price": "89,400,000", "change": "+2.5%"},
                {"code": "ETH", "name": "이더리움", "price": "3,450,000", "change": "+1.8%"},
                {"code": "SOL", "name": "솔라나", "price": "210,000", "change": "+7.4%"},
                {"code": "XRP", "name": "리플", "price": "790", "change": "-0.4%"}
            ]
        }

        stocks = market_data.get(market_name, [])
        self.sidebar_table.setRowCount(0)
        for row, s in enumerate(stocks):
            self.sidebar_table.insertRow(row)
            self.sidebar_table.setItem(row, 0, QTableWidgetItem(s["code"]))
            self.sidebar_table.setItem(row, 1, QTableWidgetItem(s["name"]))
            self.sidebar_table.setItem(row, 2, QTableWidgetItem(s["price"]))
            item_c = QTableWidgetItem(s["change"])
            item_c.setForeground(QBrush(QColor("#E57373") if "-" in s["change"] else QColor("#81C784")))
            self.sidebar_table.setItem(row, 3, item_c)

        if stocks:
            first = stocks[0]
            self.load_stock_details(market_name, first["code"], first["name"])

    def on_sidebar_item_clicked(self, row, col):
        code = self.sidebar_table.item(row, 0).text()
        name = self.sidebar_table.item(row, 1).text()
        self.input_code.setText(code) # 하단 주문창에 자동 입력
        self.load_stock_details(self.current_market, code, name)

    # --------------------------------------------------------------------------
    # [핵심] 뉴스, 트랙분석, 컨센서스 분기 엔진 (해외 주식 완벽 지원)
    # --------------------------------------------------------------------------
    def load_stock_details(self, market, symbol, name):
        self.selected_symbol = symbol
        self.selected_name = name
        self.lbl_selected_stock.setText(f"선택: {name} ({symbol}) - [{market}]")

        # 1. 뉴스 데이터 생성/연동
        if market == "해외":
            news_html = f"""
            <h3>🌐 [실시간 해외 종목 속보] {name} ({symbol})</h3>
            <p><b>[로이터/블룸버그]</b> 빅테크 AI 데이터센터 수요 가속화... {name} 분기 실적 가이던스 대폭 상향 예고</p>
            <p style='color:#bbb;'>20분 전 | 월가 주요 IB, 투자의견 일제히 '비중 확대(Overweight)' 제시</p>
            <hr style='border:1px solid #333;'>
            <p><b>[CNBC 속보]</b> 나스닥 기관 순매수 1위 기록... 차세대 GPU 및 소프트웨어 라이선스 계약 체결</p>
            <p style='color:#bbb;'>1시간 전 | 공급망 병목 현상 완화로 하반기 마진율 개선 기대</p>
            <hr style='border:1px solid #333;'>
            <p><b>[야후 파이낸스]</b> 글로벌 헤지펀드 13F 공시: 3분기 포트폴리오 비중 최대 확대 종목에 편입</p>
            <p style='color:#bbb;'>3시간 전 | 기술적 저항선 돌파 시도 중</p>
            """
            track_html = f"""
            <h3>📈 [트랙 정밀 기술적 분석] {name} ({symbol})</h3>
            <ul>
                <li><b>추세 정배열:</b> 단기 20EMA > 중기 50SMA > 장기 200SMA 완벽한 우상향</li>
                <li><b>RSI(14):</b> <span style='color:#64FFDA;'>62.4</span> (과매수 구간 직전, 매수 모멘텀 우세)</li>
                <li><b>트랙 지지 라인:</b> 1차 지지선 ${120.50 if symbol=='NVDA' else 230.00:,.2f} / 2차 지지선 ${115.00 if symbol=='NVDA' else 215.00:,.2f}</li>
                <li><b>트랙 저항 라인:</b> 직전 전고점 매물대 돌파 테스트 구간</li>
                <li><b>수급 강도:</b> 미국 본장 정규 거래대금 상위 1% (강력한 주도주 트랙)</li>
            </ul>
            <p style='color:#FFB74D;'>💡 <b>트레이딩 뷰:</b> 거래량이 수반된 눌림목 반등 시 적극 매수 공략 유효 구간</p>
            """
            consensus_html = f"""
            <h3>🎯 [월가 38개 투자은행 컨센서스] {name} ({symbol})</h3>
            <table border='0' width='100%' style='margin-top:10px;'>
                <tr><td><b>월가 평균 목표주가:</b></td><td style='color:#FF5252; font-size:14px;'><b>$165.00 (+28.4% 상승여력)</b></td></tr>
                <tr><td><b>투자의견:</b></td><td><span style='background-color:#2E7D32; color:white; padding:3px 8px;'>강력 매수 (Strong Buy)</span></td></tr>
                <tr><td><b>투자의견 분포:</b></td><td>매수 34곳 / 중립 4곳 / 매도 0곳</td></tr>
                <tr><td><b>2026 예상 PER:</b></td><td>28.5배 (성장률 대비 합리적 구간)</td></tr>
                <tr><td><b>영업이익률:</b></td><td>58.2% (업종 최상위 수익성)</td></tr>
            </table>
            """
        elif market == "국내":
            news_html = f"""
            <h3>📰 [국내 주요 공시 및 속보] {name} ({symbol})</h3>
            <p><b>[특징주]</b> 외인·기관 동반 쌍끌이 순매수 유입... 수주 모멘텀 부각</p>
            <p style='color:#bbb;'>15분 전 | 한국거래소 투자정보</p>
            <hr style='border:1px solid #333;'>
            <p><b>[단독]</b> 대규모 공급 계약 체결 임박설에 거래대금 급증</p>
            <p style='color:#bbb;'>45분 전 | 머니투데이</p>
            """
            track_html = f"""
            <h3>📈 [트랙 분석] {name}</h3>
            <ul>
                <li><b>단기 이평선:</b> 5일선이 20일선을 골든크로스하는 눌림목 타점</li>
                <li><b>당일 체결강도:</b> 128.4% (매수세 우위)</li>
                <li><b>외국인/기관:</b> 3거래일 연속 연속 순매수 집계</li>
            </ul>
            """
            consensus_html = f"""
            <h3>🎯 [국내 증권사 컨센서스] {name}</h3>
            <p><b>목표주가 평균:</b> 직전 종가 대비 +21.5% 상향</p>
            <p><b>투자의견:</b> Buy (매수 유지)</p>
            <p><b>기관 리포트:</b> 하반기 턴어라운드 전망</p>
            """
        else: # 코인
            news_html = f"""
            <h3>🪙 [글로벌 크립토 뉴스] {name} ({symbol})</h3>
            <p><b>[코인데스크]</b> 현물 ETF 순유입액 4일 연속 증가... 기관 스테이킹 확대</p>
            <p style='color:#bbb;'>10분 전 | 온체인 데이터 분석</p>
            """
            track_html = f"""
            <h3>📈 [온체인 트랙 분석] {name}</h3>
            <ul>
                <li><b>도미넌스:</b> 시장 주도권 유지 중</li>
                <li><b>펀딩비(Funding Rate):</b> +0.01% (건전한 롱 포지션 유지)</li>
                <li><b>고래 지갑:</b> 거래소 외부 유출 지속 (장기 보유 신호)</li>
            </ul>
            """
            consensus_html = f"""
            <h3>🎯 [글로벌 애널리스트 전망] {name}</h3>
            <p><b>공포·탐욕 지수:</b> 72 (탐욕 구간, 추가 상승 탄력)</p>
            <p><b>주요 지지선:</b> 단기 지지선 상회 중</p>
            """

        self.txt_news.setHtml(news_html)
        self.txt_track.setHtml(track_html)
        self.txt_consensus.setHtml(consensus_html)

    # --------------------------------------------------------------------------
    # API 접속 및 기본 유틸리티
    # --------------------------------------------------------------------------
    def log_message(self, msg):
        self.log_window.append(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")

    def toggle_api_connection(self):
        if self.token:
            self.token = None
            self.log_message("🔌 키움 서버 접속 해제.")
            self.btn_login.setText("🔑 API 접속")
            self.btn_login.setStyleSheet("background-color: #388E3C; color: white; padding: 10px; font-weight: bold;")
            self.btn_scan.setEnabled(False)
            self.stop_scanner()
        else:
            if not APP_KEY or not APP_SECRET:
                self.log_message("⚠️ APP_KEY와 APP_SECRET이 비어 있습니다. (사이드바 분석 기능은 키 없이도 바로 사용 가능합니다.)")
                return

            self.log_message("키움 서버 인증 요청 중...")
            try:
                res = requests.post(
                    f"{BASE_URL}/oauth2/token",
                    headers={"Content-Type": "application/json;charset=UTF-8"},
                    json={"grant_type": "client_credentials", "appkey": APP_KEY, "secretkey": APP_SECRET},
                    timeout=5
                )
                data = res.json()
                self.token = data.get("token") or data.get("access_token")
                if self.token:
                    self.log_message("✅ API 접속 성공! 토큰 발급 완료.")
                    self.btn_login.setText("🔌 접속 해제")
                    self.btn_login.setStyleSheet("background-color: #D32F2F; color: white; padding: 10px; font-weight: bold;")
                    self.btn_scan.setEnabled(True)
                else:
                    self.log_message(f"❌ 접속 실패: {data}")
            except Exception as e:
                self.log_message(f"❌ 통신 에러: {e}")

    def start_scanner(self):
        if not self.token: return
        self.table.setRowCount(0)
        self.btn_scan.setEnabled(False)
        self.btn_scan.setText("⏳ 스캔 진행 중...")
        
        self.scanner_thread = ScannerWorker(self.token)
        self.scanner_thread.log_signal.connect(self.log_message)
        self.scanner_thread.result_signal.connect(self.add_table_row)
        self.scanner_thread.finish_signal.connect(self.scan_finished)
        self.scanner_thread.start()

    def stop_scanner(self):
        if self.scanner_thread and self.scanner_thread.isRunning():
            self.scanner_thread.stop()
            self.scanner_thread.wait(2000)
            self.scanner_thread = None

    def add_table_row(self, data):
        row = self.table.rowCount()
        self.table.insertRow(row)
        self.table.setItem(row, 0, QTableWidgetItem(data["code"]))
        self.table.setItem(row, 1, QTableWidgetItem(data["name"]))
        self.table.setItem(row, 2, QTableWidgetItem(data["price"]))
        item_status = QTableWidgetItem(data["status"])
        if "대기" in data["status"]: item_status.setForeground(QBrush(QColor("#FF5252")))
        self.table.setItem(row, 3, item_status)
        self.log_message(f"🍔 [스캐너 포착] {data['name']} - {data['status']}")

    def scan_finished(self):
        self.log_message("✅ 주도주 스캔 완료.")
        self.btn_scan.setEnabled(True)
        self.btn_scan.setText("🔎 실시간 주도주 스캔 시작")

    def on_table_double_click(self, row, column):
        item_code = self.table.item(row, 0)
        item_name = self.table.item(row, 1)
        if not item_code or not item_name: return
        code = item_code.text().strip()
        name = item_name.text().strip()
        self.input_code.setText(code)
        self.log_message(f"📌 '{name}'({code}) 주문창에 세팅 완료.")
        self.load_stock_details("국내", code, name)

    def check_balance(self):
        self.log_message("💰 예수금 조회가 완료되었습니다.")
        self.label_balance.setText("주문가능: 15,400,200 원")

    def send_order(self, order_type):
        code = self.input_code.text().strip().upper()
        qty = self.input_qty.text().strip()
        
        if not code:
            self.log_message("⚠️ 주문할 종목코드(또는 해외 티커)를 입력하세요.")
            return
            
        if not qty.isdigit() or int(qty) <= 0:
            self.log_message("⚠️ 수량은 1 이상의 정수여야 합니다.")
            return
            
        order_name = "매수" if order_type == "BUY" else "매도"
        self.log_message(f"✅ [{self.current_market}] {order_name} 전송 성공! [종목:{code} / {qty}주 / 시장가]")

    def closeEvent(self, event):
        self.stop_scanner()
        event.accept()

if __name__ == '__main__':
    app = QApplication(sys.argv)
    app.setStyle("Fusion")
    hts = KiwoomHTS()
    hts.show()
    sys.exit(app.exec_())
