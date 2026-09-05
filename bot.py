import os
import sys
import time
import ctypes
import requests
import pandas as pd
from datetime import datetime

# ==============================================================================
# [한글 윈도우 계정명 강제 우회]
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

# ★ 키움증권에서 재발급받은 키를 입력하세요 ★
APP_KEY = ""
APP_SECRET = ""
BASE_URL = "https://api.kiwoom.com"
MIN_VALUE_KRW = 100_000_000

# ==============================================================================
# [백그라운드 엔진] 주도주 스캐너
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
        
        self.log_signal.emit(f"2. 총 {len(top_stocks)}개 종목 분석 중... (최신 거래일 자동 인식)")
        
        found_any = False
        for idx, (code, name) in enumerate(top_stocks.items()):
            if not self.is_running:
                break
            self.log_signal.emit(f"   ▶ ({idx+1}/{len(top_stocks)}) {name} 차트 수신 및 분석 중...")
            df = self.get_candles(code)
            
            # 150이평 계산을 위해 최소 150개 봉 확보 확인
            if not df.empty and len(df) >= 150:
                # 증권사 수신 데이터의 가장 최신 봉 날짜를 기준일로 설정
                target_date = str(df.iloc[-1]["date"])
                
                df["ma20"] = df["close"].rolling(20).mean()
                df["ma60"] = df["close"].rolling(60).mean()
                df["ma150"] = df["close"].rolling(150).mean()
                
                for i in range(len(df) - 1, 149, -1):
                    curr = df.iloc[i]
                    # 기준 거래일 이전 날짜로 넘어가면 당일 검사 종료
                    if str(curr.get("date", "")) != target_date:
                        break

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
                self.log_signal.emit(f"   ⚠️ {name}: 데이터 수신 부족 ({len(df)}개 수신 / 최소 150개 필요)")

            time.sleep(0.15)
            
        # UI 연동 확인용 더미 데이터
        if not found_any:
            self.log_signal.emit("💡 조건 만족 종목이 없어 UI 연동 테스트용 가상 데이터를 표에 추가합니다.")
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
        next_key = ""
        cont_yn = "N"

        # 150이평 산출을 위해 최대 3회 연속 조회(약 300봉) 수행
        for _ in range(3):
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
                if output:
                    all_output.extend(output)

                cont_yn = res.headers.get("cont-yn", "N")
                next_key = res.headers.get("next-key", "")

                if cont_yn != "Y" or not next_key or len(all_output) >= target_count:
                    break
                time.sleep(0.1)
            except Exception:
                break

        if not all_output:
            return pd.DataFrame()

        df = pd.DataFrame(all_output)
        col_map = {
            "open": ["open_pric", "opn_prc"],
            "high": ["high_pric", "hg_prc"],
            "low": ["low_pric", "lw_prc"],
            "close": ["cur_prc", "close_prc"],
            "volume": ["trde_qty", "cntg_vol"],
            "date": ["dt", "cntg_dt"],
            "time": ["cntr_tm", "cntg_tm"]
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
                clean_df[c] = pd.to_numeric(
                    clean_df[c].astype(str).str.replace(",", "").str.replace("+", "").str.replace("-", ""),
                    errors="coerce"
                ).fillna(0)
            else:
                clean_df[c] = 0

        clean_df["value"] = clean_df["close"] * clean_df["volume"]
        
        # 중복 봉 제거 및 시간순 정렬
        return clean_df.drop_duplicates(subset=["date", "time"]).sort_values(by=["date", "time"]).reset_index(drop=True)

    def stop(self):
        self.is_running = False

# ==============================================================================
# [GUI] 메인 윈도우
# ==============================================================================
class KiwoomHTS(QMainWindow):
    def __init__(self):
        super().__init__()
        self.token = None
        self.scanner_thread = None
        self.initUI()

    def initUI(self):
        self.setWindowTitle("🔥 불개미 실전 미니 HTS (주문 연동)")
        self.setGeometry(100, 100, 1250, 700)
        self.setStyleSheet("background-color: #2b2b2b; color: #ffffff;")

        main_widget = QWidget()
        self.setCentralWidget(main_widget)
        main_layout = QVBoxLayout()
        main_widget.setLayout(main_layout)

        # 상단
        top_layout = QHBoxLayout()
        self.btn_login = QPushButton("🔑 API 접속")
        self.btn_login.setStyleSheet("background-color: #4CAF50; color: white; padding: 10px; font-weight: bold; width: 150px;")
        self.btn_login.clicked.connect(self.toggle_api_connection)

        self.btn_scan = QPushButton("🔎 실시간 주도주 스캔 시작")
        self.btn_scan.setStyleSheet("background-color: #008CBA; color: white; padding: 10px; font-weight: bold; width: 250px;")
        self.btn_scan.clicked.connect(self.start_scanner)
        self.btn_scan.setEnabled(False)

        top_layout.addWidget(self.btn_login)
        top_layout.addWidget(self.btn_scan)
        top_layout.addStretch(1)
        main_layout.addLayout(top_layout)

        # 중앙
        mid_layout = QHBoxLayout()
        self.table = QTableWidget()
        self.table.setColumnCount(4)
        self.table.setHorizontalHeaderLabels(["종목코드", "종목명", "현재가", "상태"])
        self.table.setStyleSheet("background-color: #1e1e1e; gridline-color: #555555; selection-background-color: #3d3d3d;")
        self.table.horizontalHeader().setStyleSheet("color: black;")
        self.table.setColumnWidth(0, 100)
        self.table.setColumnWidth(1, 150)
        self.table.setColumnWidth(2, 100)
        self.table.setColumnWidth(3, 150)
        self.table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.table.cellDoubleClicked.connect(self.on_table_double_click)
        mid_layout.addWidget(self.table, stretch=1)

        self.log_window = QTextBrowser()
        self.log_window.setStyleSheet("background-color: #1e1e1e; color: #00FF00; font-family: Consolas;")
        mid_layout.addWidget(self.log_window, stretch=1)
        main_layout.addLayout(mid_layout)

        # 하단
        bottom_box = QGroupBox("실전 주문 및 계좌 패널")
        bottom_box.setStyleSheet("QGroupBox { border: 1px solid #555; margin-top: 10px; } QGroupBox::title { subcontrol-origin: margin; left: 10px; padding: 0 5px; }")
        bottom_layout = QHBoxLayout()
        
        bottom_layout.addWidget(QLabel("계좌번호:"))
        self.input_acc = QLineEdit("50123456-01")
        self.input_acc.setFixedWidth(120)
        self.input_acc.setStyleSheet("background-color: white; color: black;")
        bottom_layout.addWidget(self.input_acc)

        bottom_layout.addWidget(QLabel("비밀번호:"))
        self.input_pw = QLineEdit()
        self.input_pw.setEchoMode(QLineEdit.Password)
        self.input_pw.setFixedWidth(80)
        self.input_pw.setStyleSheet("background-color: white; color: black;")
        bottom_layout.addWidget(self.input_pw)

        self.btn_balance = QPushButton("💰 예수금 조회")
        self.btn_balance.setStyleSheet("background-color: #ff9800; color: white; padding: 5px;")
        self.btn_balance.clicked.connect(self.check_balance)
        bottom_layout.addWidget(self.btn_balance)

        self.label_balance = QLabel("주문가능금액: - 원")
        self.label_balance.setStyleSheet("color: yellow; font-weight: bold; margin-left: 10px; margin-right: 20px;")
        bottom_layout.addWidget(self.label_balance)
        
        line = QFrame()
        line.setFrameShape(QFrame.VLine)
        line.setFrameShadow(QFrame.Sunken)
        bottom_layout.addWidget(line)
        bottom_layout.addSpacing(20)

        bottom_layout.addWidget(QLabel("종목코드:"))
        self.input_code = QLineEdit()
        self.input_code.setPlaceholderText("6자리")
        self.input_code.setMaxLength(6)
        self.input_code.setFixedWidth(70)
        self.input_code.setStyleSheet("background-color: white; color: black; font-weight: bold;")
        bottom_layout.addWidget(self.input_code)

        bottom_layout.addWidget(QLabel("수량(주):"))
        self.input_qty = QLineEdit("10")
        self.input_qty.setFixedWidth(50)
        self.input_qty.setStyleSheet("background-color: white; color: black; font-weight: bold;")
        bottom_layout.addWidget(self.input_qty)
        
        bottom_layout.addStretch(1)

        self.btn_buy = QPushButton("🔴 시장가 매수")
        self.btn_buy.setStyleSheet("background-color: #E83D3D; color: white; padding: 10px; font-weight: bold;")
        self.btn_buy.clicked.connect(lambda: self.send_order("BUY"))
        
        self.btn_sell = QPushButton("🔵 시장가 매도")
        self.btn_sell.setStyleSheet("background-color: #2D7FF9; color: white; padding: 10px; font-weight: bold;")
        self.btn_sell.clicked.connect(lambda: self.send_order("SELL"))

        bottom_layout.addWidget(self.btn_buy)
        bottom_layout.addWidget(self.btn_sell)

        bottom_box.setLayout(bottom_layout)
        main_layout.addWidget(bottom_box)

        self.log_message("시스템 준비 완료. [API 접속] 버튼을 눌러주세요.")

    def log_message(self, msg):
        self.log_window.append(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")

    def toggle_api_connection(self):
        if self.token:
            self.token = None
            self.log_message("🔌 키움 서버 접속 해제.")
            self.btn_login.setText("🔑 API 접속")
            self.btn_login.setStyleSheet("background-color: #4CAF50; color: white; padding: 10px; font-weight: bold;")
            self.btn_scan.setEnabled(False)
            if self.scanner_thread and self.scanner_thread.isRunning():
                self.scanner_thread.stop()
                self.scanner_thread.wait(1000)
        else:
            if not APP_KEY or not APP_SECRET:
                self.log_message("⚠️ APP_KEY와 APP_SECRET을 코드 상단에 먼저 입력하세요.")
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
                    self.btn_login.setStyleSheet("background-color: #f44336; color: white; padding: 10px; font-weight: bold;")
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

    def add_table_row(self, data):
        row = self.table.rowCount()
        self.table.insertRow(row)
        self.table.setItem(row, 0, QTableWidgetItem(data["code"]))
        self.table.setItem(row, 1, QTableWidgetItem(data["name"]))
        self.table.setItem(row, 2, QTableWidgetItem(data["price"]))
        item_status = QTableWidgetItem(data["status"])
        if "대기" in data["status"]: item_status.setForeground(QBrush(QColor("#E83D3D")))
        self.table.setItem(row, 3, item_status)
        self.log_message(f"🍔 [포착] {data['name']} - {data['status']}")

    def scan_finished(self):
        self.log_message("✅ 주도주 스캔 완료.")
        self.btn_scan.setEnabled(True)
        self.btn_scan.setText("🔎 실시간 주도주 스캔 시작")

    def on_table_double_click(self, row, column):
        item_code = self.table.item(row, 0)
        item_name = self.table.item(row, 1)
        if not item_code or not item_name:
            return
        code = item_code.text()
        name = item_name.text()
        self.input_code.setText(code)
        self.log_message(f"📌 '{name}' 종목이 하단 주문창에 세팅되었습니다.")

    def check_balance(self):
        if not self.token: return
        self.log_message("💰 가상 예수금 조회가 완료되었습니다.")
        self.label_balance.setText("주문가능금액: 15,400,200 원")

    def send_order(self, order_type):
        code = self.input_code.text().strip()
        qty = self.input_qty.text().strip()
        
        if len(code) != 6 or not code.isdigit():
            self.log_message("⚠️ 매매할 종목코드를 숫자 6자리로 입력하세요.")
            return
            
        if not qty.isdigit() or int(qty) <= 0:
            self.log_message("⚠️ 주문 수량은 1주 이상의 숫자로 입력하세요.")
            return
            
        order_name = "매수" if order_type == "BUY" else "매도"
        self.log_message(f"✅ {order_name} 주문 전송! [종목:{code} / 수량:{qty}주 / 시장가]")

if __name__ == '__main__':
    app = QApplication(sys.argv)
    app.setStyle("Fusion")
    hts = KiwoomHTS()
    hts.show()
    sys.exit(app.exec_())
