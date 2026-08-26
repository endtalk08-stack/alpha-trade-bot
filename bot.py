<!DOCTYPE html>
<html lang="ko">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>gaemiGTP - 애널리스트 컨센서스 위젯 예시</title>
    <style>
        * { box-sizing: border-box; margin: 0; padding: 0; font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; }
        body { background-color: #1e1f20; color: #e3e3e3; display: flex; justify-content: center; align-items: center; min-height: 100vh; padding: 20px; }
        .widget-wrap { width: 100%; max-width: 440px; }
        .card { background: #282a2c; border-radius: 16px; padding: 22px; border: 1px solid #33363a; box-shadow: 0 4px 20px rgba(0,0,0,0.25); }
        
        /* 탭 전환 버튼 */
        .tab-wrap { display: flex; gap: 8px; margin-bottom: 16px; }
        .tab-btn { flex: 1; padding: 8px 0; background: #1e1f20; border: 1px solid #37393b; color: #8e918f; border-radius: 8px; cursor: pointer; font-size: 0.85rem; font-weight: 600; text-align: center; }
        .tab-btn.active { background: #38bdf8; color: #121212; border-color: #38bdf8; }

        /* 상단 타이틀 */
        .header-row { display: flex; justify-content: space-between; align-items: center; margin-bottom: 18px; }
        .card-title { font-size: 1.05rem; font-weight: 700; color: #f2f2f2; }
        .analyst-count { font-size: 0.78rem; color: #8e918f; }

        /* 원형 게이지 & 종합의견 */
        .gauge-section { display: flex; align-items: center; gap: 16px; padding-bottom: 18px; border-bottom: 1px solid #33363a; }
        .circle-gauge { width: 62px; height: 62px; border-radius: 50%; border: 4.5px solid #10b981; display: flex; flex-direction: column; align-items: center; justify-content: center; background: #1e1f20; flex-shrink: 0; }
        .gauge-score { font-size: 1.15rem; font-weight: 800; color: #fff; line-height: 1; }
        .gauge-label { font-size: 0.58rem; color: #8e918f; margin-top: 3px; }
        .opinion-status { display: flex; align-items: center; gap: 6px; margin-bottom: 5px; }
        .badge-buy { background: rgba(16, 185, 129, 0.15); color: #10b981; font-size: 0.75rem; font-weight: 700; padding: 2px 8px; border-radius: 10px; }
        .opinion-dots { font-size: 0.76rem; color: #8e918f; }

        /* 가격 비교 영역 */
        .price-compare { display: flex; justify-content: space-between; align-items: flex-end; margin-top: 16px; }
        .price-sub { font-size: 0.75rem; color: #8e918f; margin-bottom: 2px; }
        .target-avg-val { font-size: 1.35rem; font-weight: 800; color: #10b981; }
        .curr-price-val { font-size: 1.2rem; font-weight: 700; color: #ffffff; }

        /* 목표가 슬라이더 바 */
        .slider-wrap { margin: 24px 0 16px; position: relative; }
        .slider-top-labels { display: flex; justify-content: space-between; font-size: 0.72rem; margin-bottom: 6px; }
        .bar-track { width: 100%; height: 6px; background: #1e1f20; border-radius: 4px; position: relative; }
        .bar-fill { height: 100%; background: linear-gradient(90deg, #38bdf8, #10b981); border-radius: 4px; }
        .dot-curr { position: absolute; top: -3px; width: 12px; height: 12px; border-radius: 50%; background: #ffffff; border: 2px solid #282a2c; }
        .dot-target { position: absolute; top: -4px; width: 14px; height: 14px; border-radius: 50%; background: #10b981; border: 2px solid #282a2c; transform: translateX(-50%); }
        .slider-bottom-labels { display: flex; justify-content: space-between; font-size: 0.7rem; color: #6b7280; margin-top: 6px; }

        /* 최근 리포트 리스트 */
        .report-section { margin-top: 18px; border-top: 1px solid #33363a; padding-top: 14px; }
        .report-header { font-size: 0.85rem; font-weight: 700; color: #c4c7c5; margin-bottom: 10px; display: flex; justify-content: space-between; }
        .source-tag { font-size: 0.7rem; color: #8e918f; font-weight: normal; }
        .report-item { display: flex; justify-content: space-between; align-items: center; padding: 8px 0; border-bottom: 1px solid #2e3034; font-size: 0.82rem; }
        .report-item:last-child { border-bottom: none; }
        .broker-name { color: #ffffff; font-weight: 700; }
        .broker-meta { font-size: 0.7rem; color: #8e918f; margin-top: 2px; }
        .target-val { color: #ffffff; font-weight: 700; text-align: right; }
        .upside-val { color: #10b981; font-size: 0.72rem; font-weight: 700; text-align: right; }
    </style>
</head>
<body>

<div class="widget-wrap">
    <!-- 국내 / 해외 예시 전환 탭 -->
    <div class="tab-wrap">
        <div class="tab-btn active" onclick="switchData('kr')">🇰🇷 국내주식 (한경컨센서스 기반)</div>
        <div class="tab-btn" onclick="switchData('us')">🇺🇸 미국주식 (TipRanks/Yahoo 기반)</div>
    </div>

    <!-- 카드 본체 -->
    <div class="card">
        <div class="header-row">
            <span class="card-title">gaemiGTP 애널리스트 컨센서스</span>
            <span class="analyst-count" id="analystCount">최근 리포트 18개 종합</span>
        </div>

        <!-- 원형 게이지 & 투자의견 -->
        <div class="gauge-section">
            <div class="circle-gauge">
                <span class="gauge-score" id="scoreNum">18</span>
                <span class="gauge-label">기관의견</span>
            </div>
            <div>
                <div class="opinion-status">
                    <span style="font-size:0.8rem; color:#8e918f;">전체 의견</span>
                    <span class="badge-buy" id="badgeOpinion">적극 매수 (BUY)</span>
                </div>
                <div class="opinion-dots" id="dotsText">
                    <span style="color:#10b981;">● 매수 16</span> &nbsp;
                    <span style="color:#f97316;">● 보유 2</span> &nbsp;
                    <span style="color:#ef4444;">● 매도 0</span>
                </div>
            </div>
        </div>

        <!-- 가격 비교 -->
        <div class="price-compare">
            <div>
                <div class="price-sub">평균 목표주가</div>
                <div class="target-avg-val" id="targetAvgPrice">245,000 원</div>
            </div>
            <div style="text-align: right;">
                <div class="price-sub">현재가</div>
                <div class="curr-price-val" id="currentPrice">178,500 원</div>
            </div>
        </div>

        <!-- 슬라이더 바 -->
        <div class="slider-wrap">
            <div class="slider-top-labels">
                <span id="sliderCurrPrice" style="color:#8e918f;">178,500원</span>
                <span id="sliderTargetPrice" style="color:#10b981; font-weight:700;">평균 245,000원</span>
                <span id="sliderHighPrice" style="color:#8e918f;">300,000원</span>
            </div>
            <div class="bar-track">
                <div class="bar-fill" id="barFill" style="width: 65%;"></div>
                <div class="dot-curr" style="left: 0%;"></div>
                <div class="dot-target" id="dotTarget" style="left: 65%;"></div>
            </div>
            <div class="slider-bottom-labels">
                <span id="sliderLowText">증권사 최저 200,000원</span>
                <span id="sliderHighText">증권사 최고 300,000원</span>
            </div>
        </div>

        <!-- 최근 증권사별 리포트 리스트 -->
        <div class="report-section">
            <div class="report-header">
                <span>최근 증권사 리포트 평가</span>
                <span class="source-tag" id="sourceTag">출처: 한경컨센서스</span>
            </div>
            <div id="reportList"></div>
        </div>
    </div>
</div>

<script>
    const sampleData = {
        kr: {
            analystCount: "최근 리포트 18개 종합",
            score: "18",
            opinion: "적극 매수 (BUY)",
            dots: '<span style="color:#10b981;">● 매수 16</span> &nbsp; <span style="color:#f97316;">● 보유 2</span> &nbsp; <span style="color:#ef4444;">● 매도 0</span>',
            targetAvg: "245,000 원",
            currPrice: "178,500 원",
            sliderCurr: "178,500원",
            sliderTarget: "평균 245,000원",
            sliderHigh: "300,000원",
            sliderLowTxt: "증권사 최저 210,000원",
            sliderHighTxt: "증권사 최고 300,000원",
            progressPct: 62,
            source: "출처: 한경컨센서스 실시간 취합",
            reports: [
                { broker: "미래에셋증권", analyst: "김영건 연구원", opinion: "BUY", date: "2026.08.24", target: "260,000원", upside: "+45.65%" },
                { broker: "신한투자증권", analyst: "김형태 연구원", opinion: "BUY", date: "2026.08.20", target: "250,000원", upside: "+40.05%" },
                { broker: "하나증권", analyst: "김록호 연구원", opinion: "BUY", date: "2026.08.18", target: "280,000원", upside: "+56.86%" },
                { broker: "NH투자증권", analyst: "류영호 연구원", opinion: "BUY", date: "2026.08.12", target: "230,000원", upside: "+28.85%" }
            ]
        },
        us: {
            analystCount: "월가 애널리스트 32명 종합",
            score: "32",
            opinion: "강력 매수 (Strong Buy)",
            dots: '<span style="color:#10b981;">● Buy 28</span> &nbsp; <span style="color:#f97316;">● Hold 3</span> &nbsp; <span style="color:#ef4444;">● Sell 1</span>',
            targetAvg: "$248.50",
            currPrice: "$159.55",
            sliderCurr: "$159.55",
            sliderTarget: "평균 $248.50",
            sliderHigh: "$320.00",
            sliderLowTxt: "월가 최저 $180.00",
            sliderHighTxt: "월가 최고 $320.00",
            progressPct: 68,
            source: "출처: Wall Street Consensus",
            reports: [
                { broker: "Morgan Stanley", analyst: "Joseph Moore", opinion: "Overweight", date: "2026.08.25", target: "$260.00", upside: "+62.96%" },
                { broker: "Goldman Sachs", analyst: "Toshiya Hari", opinion: "Buy", date: "2026.08.22", target: "$250.00", upside: "+56.69%" },
                { broker: "Barclays", analyst: "Tom O'Malley", opinion: "Overweight", date: "2026.08.19", target: "$275.00", upside: "+72.36%" },
                { broker: "JPMorgan", analyst: "Harlan Sur", opinion: "Overweight", date: "2026.08.14", target: "$235.00", upside: "+47.28%" }
            ]
        }
    };

    function render(data) {
        document.getElementById('analystCount').innerText = data.analystCount;
        document.getElementById('scoreNum').innerText = data.score;
        document.getElementById('badgeOpinion').innerText = data.opinion;
        document.getElementById('dotsText').innerHTML = data.dots;
        document.getElementById('targetAvgPrice').innerText = data.targetAvg;
        document.getElementById('currentPrice').innerText = data.currPrice;

        document.getElementById('sliderCurrPrice').innerText = data.sliderCurr;
        document.getElementById('sliderTargetPrice').innerText = data.sliderTarget;
        document.getElementById('sliderHighPrice').innerText = data.sliderHigh;
        document.getElementById('sliderLowText').innerText = data.sliderLowTxt;
        document.getElementById('sliderHighText').innerText = data.sliderHighTxt;

        document.getElementById('barFill').style.width = data.progressPct + '%';
        document.getElementById('dotTarget').style.left = data.progressPct + '%';
        document.getElementById('sourceTag').innerText = data.source;

        const rList = document.getElementById('reportList');
        rList.innerHTML = '';
        data.reports.forEach(r => {
            rList.innerHTML += `
                <div class="report-item">
                    <div>
                        <span class="broker-name">${r.broker}</span>
                        <div class="broker-meta">
                            <span style="color:#10b981; font-weight:700;">${r.opinion}</span> · ${r.analyst} · ${r.date}
                        </div>
                    </div>
                    <div>
                        <div class="target-val">${r.target}</div>
                        <div class="upside-val">${r.upside}</div>
                    </div>
                </div>
            `;
        });
    }

    function switchData(type) {
        document.querySelectorAll('.tab-btn').forEach((b, idx) => {
            b.classList.toggle('active', (type === 'kr' && idx === 0) || (type === 'us' && idx === 1));
        });
        render(sampleData[type]);
    }

    // 초기 렌더링 (국내주식)
    render(sampleData.kr);
</script>

</body>
</html>
