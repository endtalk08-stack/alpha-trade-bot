import os
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


HOST = "0.0.0.0"
PORT = int(os.environ.get("PORT", "10000"))


HTML = """<!DOCTYPE html>
<html lang="ko">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>gaemiGTP</title>
    <style>
        * {
            box-sizing: border-box;
        }

        body {
            margin: 0;
            min-height: 100vh;
            background: #1e1f20;
            color: #f1f1f1;
            font-family: Arial, sans-serif;
        }

        .page {
            min-height: 100vh;
            display: flex;
            flex-direction: column;
            align-items: center;
            justify-content: center;
            padding: 24px;
        }

        .logo {
            font-size: 42px;
            font-weight: 700;
            margin-bottom: 10px;
        }

        .sub {
            color: #a0a0a0;
            margin-bottom: 28px;
            text-align: center;
        }

        .search-box {
            width: min(720px, 100%);
            display: flex;
            gap: 10px;
        }

        input {
            flex: 1;
            padding: 16px 18px;
            border: 1px solid #3b3d40;
            border-radius: 14px;
            background: #292b2e;
            color: white;
            font-size: 16px;
            outline: none;
        }

        button {
            padding: 0 22px;
            border: 0;
            border-radius: 14px;
            background: #f0f0f0;
            color: #111;
            font-size: 16px;
            font-weight: 700;
            cursor: pointer;
        }

        .status {
            margin-top: 24px;
            color: #8f9499;
            font-size: 14px;
        }

        .card {
            width: min(720px, 100%);
            margin-top: 24px;
            padding: 20px;
            border-radius: 16px;
            background: #292b2e;
            display: none;
        }
    </style>
</head>
<body>
    <div class="page">
        <div class="logo">gaemiGTP</div>
        <div class="sub">
            종목 하나로 시작하는 금융 분석 플랫폼
        </div>

        <div class="search-box">
            <input
                id="symbol"
                type="text"
                placeholder="예: SK하이닉스, NVDA, BTC"
                onkeydown="if(event.key === 'Enter') analyze();"
            >
            <button onclick="analyze()">분석</button>
        </div>

        <div class="status" id="status">
            사이트 기본 엔진 정상 작동 중
        </div>

        <div class="card" id="result"></div>
    </div>

    <script>
        async function analyze() {
            const input = document.getElementById("symbol");
            const status = document.getElementById("status");
            const result = document.getElementById("result");
            const symbol = input.value.trim();

            if (!symbol) {
                status.textContent = "종목명을 입력해주세요.";
                return;
            }

            status.textContent = "분석 엔진 연결 준비 중...";
            result.style.display = "block";
            result.textContent =
                "현재는 사이트 기본 뼈대 단계입니다.\\n\\n" +
                "입력 종목: " + symbol + "\\n\\n" +
                "다음 단계에서 실제 주가 데이터와 분석 엔진을 연결합니다.";

        }
    </script>
</body>
</html>
"""


class AppHandler(BaseHTTPRequestHandler):

    def send_text(self, status_code, content, content_type="text/html; charset=utf-8"):
        data = content.encode("utf-8")

        self.send_response(status_code)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()

        self.wfile.write(data)

    def do_GET(self):
        if self.path == "/" or self.path.startswith("/?"):
            self.send_text(200, HTML)
            return

        if self.path == "/health":
            self.send_text(
                200,
                '{"status":"ok","service":"gaemiGTP"}',
                "application/json; charset=utf-8"
            )
            return

        self.send_text(
            404,
            '{"error":"not_found"}',
            "application/json; charset=utf-8"
        )

    def log_message(self, format, *args):
        print("[WEB]", format % args)


def main():
    server = ThreadingHTTPServer((HOST, PORT), AppHandler)

    print(f"gaemiGTP web server started on port {PORT}")

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
