<!DOCTYPE html>
<html lang="ko">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>gaemiGTP - 10초 금융 분석 엔진</title>
    <style>
        * { box-sizing: border-box; margin: 0; padding: 0; font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; }
        body { background-color: #212121; color: #ececec; min-height: 100vh; display: flex; flex-direction: column; justify-content: space-between; }
        
        /* 상단 바 */
        header { display: flex; justify-content: space-between; align-items: center; padding: 16px 24px; }
        .logo-top { font-weight: 700; font-size: 1.1rem; color: #b4b4b4; letter-spacing: -0.5px; }
        .user-icon { width: 32px; height: 32px; border-radius: 50%; background: #343541; display: flex; align-items: center; justify-content: center; font-size: 0.85rem; font-weight: bold; color: #fff; }

        /* 메인 중앙 영역 */
        main { flex: 1; display: flex; flex-direction: column; align-items: center; justify-content: center; padding: 20px; max-width: 800px; margin: 0 auto; width: 100%; }
        .hero-title { font-size: 3rem; font-weight: 800; margin-bottom: 8px; letter-spacing: -1.5px; color: #f5f5f5; }
        .hero-sub { font-size: 1.05rem; color: #8e8ea0; margin-bottom: 36px; font-weight: 400; }

        /* 미니멀 검색 바 */
        .search-box { width: 100%; background: #2f2f2f; border: 1px solid #424242; border-radius: 28px; padding: 12px 20px; display: flex; align-items: center; box-shadow: 0 4px 20px rgba(0,0,0,0.25); transition: border-color 0.2s; }
        .search-box:focus-within { border-color: #666; }
        .search-box input { flex: 1; background: transparent; border: none; outline: none; color: #fff; font-size: 1.05rem; padding: 4px 10px; }
        .search-box input::placeholder { color: #8e8ea0; }
        .search-btn { background: #3b82f6; color: white; border: none; border-radius: 20px; padding: 8px 18px; font-size: 0.9rem; font-weight: 600; cursor: pointer; display: flex; align-items: center; gap: 6px; }
        .search-btn:hover { background: #2563eb; }

        /* 인기 종목 태그 */
        .popular-tags { display: flex; gap: 10px; margin-top: 18px; flex-wrap: wrap; justify-content: center; }
        .tag-btn { background: #2f2f2f; border: 1px solid #3e3e3e; border-radius: 16px; padding: 6px 14px; font-size: 0.85rem; color: #c5c5d2; cursor: pointer; transition: 0.2s; }
        .tag-btn:hover { background: #3a3a3a; color: #fff; border-color: #555; }

        /* 결과 영역 */
        #result-container { width: 100%; margin-top: 30px; display: none; }
        .card { background: #2f2f2f; border: 1px solid #424242; border-radius: 16px; padding: 24px; margin-bottom: 20px; white-space: pre-wrap; line-height: 1.6; }
        .card-header { font-size: 1.25rem; font-weight: 700; color: #60a5fa; margin-bottom: 12px; }
        .detail-btn { width: 100%; background: #374151; border: 1px solid #4b5563; color: #e5e7eb; padding: 12px; border-radius: 10px; font-weight: 600; cursor: pointer; margin-top: 10px; }
        .detail-btn:hover { background: #4b5563; }
        .detail-content { display: none; margin-top: 16px; padding-top: 16px; border-top: 1px solid #444; color: #d1d5db; }

        /* 로딩 스피너 */
        .loading { display: none; margin: 30px 0; text-align: center; color: #9ca3af; font-size: 0.95rem; }
        
        /* 하단 푸터 */
        footer { text-align: center; padding: 20px; color: #666; font-size: 0.8rem; }
    </style>
</head>
<body>

    <header>
        <div class="logo-top">gaemiGTP</div>
        <div class="user-icon">민</div>
    </header>

    <main>
        <div class="hero-title">gaemiGTP</div>
        <div class="hero-sub">종목명 하나면 10초 만에 끝납니다</div>

        <div class="search-box">
            <input type="text" id="queryInput" placeholder="무엇이든 물어보세요 (예: 삼성전자, 엔비디아, 비트코인)" onkeypress="if(event.keyCode==13) doSearch();">
            <button class="search-btn" onclick="doSearch()">✨ AI 모드</button>
        </div>

        <div class="popular-tags">
            <button class="tag-btn" onclick="searchStock('삼성전자')">📈 삼성전자</button>
            <button class="tag-btn" onclick="searchStock('엔비디아')">🔥 엔비디아</button>
            <button class="tag-btn" onclick="searchStock('테슬라')">🚗 테슬라</button>
            <button class="tag-btn" onclick="searchStock('비트코인')">₿ 비트코인</button>
            <button class="tag-btn" onclick="searchStock('QQQ')">📊 QQQ</button>
        </div>

        <div class="loading" id="loading">
            ⚡ 실시간 호가/수급/재무/뉴스 및 퀀트 팩터를 종합 분석 중입니다...
        </div>

        <div id="result-container">
            <div class="card">
                <div class="card-header" id="resTitle"></div>
                <div id="resSummary"></div>
                <button class="detail-btn" id="toggleDetailBtn" onclick="toggleDetail()">심층 분석 및 근거 보기</button>
                <div class="detail-content" id="resDetail"></div>
            </div>
        </div>
    </main>

    <footer>
        본 서비스는 금융데이터 기반 기술적 분석 도구이며 투자 권유가 아닙니다.
    </footer>

    <script>
        // ⚠️ Render 배포 서버 주소로 변경해 주세요 (로컬 테스트 시 http://localhost:10000)
        const API_BASE_URL = window.location.origin;

        function searchStock(name) {
            document.getElementById('queryInput').value = name;
            doSearch();
        }

        async function doSearch() {
            const query = document.getElementById('queryInput').value.trim();
            if (!query) return;

            document.getElementById('loading').style.display = 'block';
            document.getElementById('result-container').style.display = 'none';
            document.getElementById('resDetail').style.display = 'none';
            document.getElementById('toggleDetailBtn').innerText = '심층 분석 및 근거 보기';

            try {
                const response = await fetch(`${API_BASE_URL}/api/analyze?q=${encodeURIComponent(query)}`);
                const data = await response.json();

                if (!response.ok || data.error) {
                    alert(data.error || '분석 중 오류가 발생했습니다.');
                    document.getElementById('loading').style.display = 'none';
                    return;
                }

                document.getElementById('resTitle').innerText = `${data.name} (${data.symbol})`;
                document.getElementById('resSummary').innerText = data.summary;
                document.getElementById('resDetail').innerText = data.detail;

                document.getElementById('loading').style.display = 'none';
                document.getElementById('result-container').style.display = 'block';
            } catch (err) {
                alert('서버와 통신할 수 없습니다. 백엔드 주소를 확인해 주세요.');
                document.getElementById('loading').style.display = 'none';
            }
        }

        function toggleDetail() {
            const d = document.getElementById('resDetail');
            const btn = document.getElementById('toggleDetailBtn');
            if (d.style.display === 'block') {
                d.style.display = 'none';
                btn.innerText = '심층 분석 및 근거 보기';
            } else {
                d.style.display = 'block';
                btn.innerText = '타점 카드만 보기';
            }
        }
    </script>
</body>
</html>
