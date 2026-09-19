(function () {
    'use strict';

    function render(data) {
        const displayName = String((data && (data.name || data.formal_name || data.symbol)) || '이 종목').trim();
        let materialText = '';
    const mr = data.movement_reason;
    const mrCard = document.getElementById('movementReasonCard');
    if (mrCard && mr) {
        const confidenceStyle =
            mr.confidence === '높음'
                ? 'background:rgba(16,185,129,0.14);color:#10b981;border:1px solid rgba(16,185,129,0.25);'
                : mr.confidence === '중간'
                    ? 'background:rgba(245,158,11,0.14);color:#f59e0b;border:1px solid rgba(245,158,11,0.25);'
                    : 'background:rgba(148,163,184,0.12);color:var(--text-muted);border:1px solid var(--border-line);';

        const evidenceReasons = (mr.reasons || []).slice().sort((a,b) => {
            const a5 = a.key === 'market_5d' ? 1 : 0;
            const b5 = b.key === 'market_5d' ? 1 : 0;
            if (a5 !== b5) return a5 - b5;
            return Math.abs(Number(b.score || 0)) - Math.abs(Number(a.score || 0));
        });
        const evidenceCount = evidenceReasons.filter(x => Number(x.score || 0) !== 0).length;


        // 기존에 확정한 "왜 움직였을까?" 고정 코멘트 생성 로직을
        // 트랙 분석 디자인 변경과 분리해 그대로 유지한다.
        const scoreOf = key => { const x=evidenceReasons.find(r=>r.key===key); return x ? Number(x.score||0) : 0; };
        const detailOf = key => { const x=evidenceReasons.find(r=>r.key===key); return x ? String(x.detail||'') : ''; };
        const materialReason = evidenceReasons.find(r => r.key === 'material') || null;

        const priceText = data.price || data.price_str || '-';
        const pct = Number(data.current_change_pct);
        const topicParticle = (() => {
            const text = String(displayName || '').trim();
            const last = text.charCodeAt(text.length - 1);
            if (last >= 0xAC00 && last <= 0xD7A3) {
                return ((last - 0xAC00) % 28) === 0 ? '는' : '은';
            }
            return '은';
        })();

        const directionText =
            Number.isFinite(pct)
                ? (pct > 0 ? '상승' : pct < 0 ? '하락' : '보합')
                : (mr.direction === '하락' ? '하락' : mr.direction === '상승' ? '상승' : '보합');

        const boardItems = Array.isArray(data.world_board) ? data.world_board : [];
        const findBoard = (...names) => boardItems.find(item =>
            names.some(name => String(item?.name || '').toLowerCase() === String(name).toLowerCase())
        );
        const rateNumber = item => {
            if (!item) return null;
            const n = Number(String(item.rate ?? '').replace('%','').replace('+',''));
            return Number.isFinite(n) ? n : null;
        };
        const upDown = n => n > 0 ? '상승' : n < 0 ? '하락' : '보합';

        const marketType = String(data.market || data.market_name || data.market_type || '').toLowerCase();
        const isUSMarket = marketType.includes('미국주식') || marketType.includes('미국') || marketType.includes('us');
        const isCryptoMarket = marketType.includes('암호화폐') || marketType.includes('암호') || marketType.includes('crypto') || marketType.includes('코인');
        const selectedMarketName = isUSMarket
            ? '나스닥'
            : (String(data.market_index_name || '').toUpperCase() === 'KOSDAQ' ? '코스닥' : '코스피');
        const marketItem = isUSMarket
            ? findBoard('QQQ')
            : findBoard(selectedMarketName, selectedMarketName === '코스피' ? 'KOSPI' : 'KOSDAQ');
        const marketRate = rateNumber(marketItem);

        const newsBlob = [
            displayName,
            ...(Array.isArray(mr.news_evidence) ? mr.news_evidence.map(x => x?.title || '') : []),
            ...(Array.isArray(materialReason?.news_clusters) ? materialReason.news_clusters.flatMap(c => c?.keywords || []) : [])
        ].join(' ').toLowerCase();
        const semiconductorKeywords = ['반도체','hbm','hbm4','dram','nand','메모리','gpu','semiconductor'];
        const isSemiconductor = semiconductorKeywords.some(k => newsBlob.includes(k));
        const soxxItem = findBoard('SOXX');
        const soxxRate = rateNumber(soxxItem);

        // STEP 43: "왜 움직였을까?"는 AI가 뉴스 본문을 재해석하지 않고
        // 이미 확보된 하단 데이터(movement_reason / DART / 시장지수)만으로
        // 고정 템플릿을 조립한다. 화면의 숫자와 문장이 서로 어긋나지 않도록
        // 근거가 없는 원인·수치·인과관계는 문장에 넣지 않는다.
        const parts = [];

        // 1) 첫 문장: 현재가/등락률은 화면의 실제 응답값 그대로 사용
        parts.push(
            Number.isFinite(pct)
                ? `🐜 개미들아! ${displayName}${topicParticle} 오늘 ${pct >= 0 ? '+' : ''}${pct.toFixed(2)}% ${directionText} 중이야.`
                : `🐜 개미들아! ${displayName}${topicParticle} 현재 흐름을 확인하고 있어.`
        );

        // STEP 48: 전역 공통 함수만 사용한다.
        // 이 블록 안에서 const domesticFlowValue를 다시 선언하지 않는다.
        // 2) 수급 근거: 실제 5거래일 순매수/순매도 수치가 있는 경우에만 출력
        const flowCandidates = [
            { key: 'foreign', label: '외국인', value: getDomesticFlowValue(mr, 'foreign') },
            { key: 'institution', label: '기관', value: getDomesticFlowValue(mr, 'institution') }
        ].filter(x => x.value !== null && Number.isFinite(x.value));

        const selectedFlow = flowCandidates.length
            ? flowCandidates.slice().sort((a,b) => Math.abs(b.value) - Math.abs(a.value))[0]
            : null;

        // 3) 공시/기업 재료와 거시 이벤트를 분리한다.
        // STEP 44: 종목 뉴스에 FOMC/연준이 단순 언급된 것만으로
        // 거시 원인을 선택하지 않는다. 거시 이벤트는 global_news + 일정에서
        // 독립 출처가 2개 이상 확인되거나 실제 일정 이벤트가 있을 때만 채택한다.
        const disclosureItems = Array.isArray(data.dart_disclosures) ? data.dart_disclosures : [];
        const disclosurePositive = disclosureItems.filter(x => String(x?.tag_type || '') === 'positive').length;
        const disclosureNegative = disclosureItems.filter(x => String(x?.tag_type || '') === 'negative').length;
        const hasDisclosure = disclosureItems.length > 0;
        const hasMaterialCluster = !!(
            materialReason &&
            Array.isArray(materialReason.news_clusters) &&
            materialReason.news_clusters.length
        );

        // 종목 재료에서 거시 키워드는 제거한다.
        if (hasMaterialCluster) {
            const cluster = materialReason.news_clusters[0] || null;
            const kws = cluster && Array.isArray(cluster.keywords) ? cluster.keywords : [];
            const map = {
                'hbm4':'HBM4','hbm':'HBM','ai':'AI 수요','인공지능':'AI 수요','gpu':'GPU 수요',
                '수요':'수요 확대','공급':'공급 확대','공급망':'공급망 확대','증설':'증설',
                '실적':'실적','earnings':'실적','매출':'매출 성장','영업이익':'영업이익',
                '수주':'수주','계약':'계약','관세':'관세','aws':'AWS'
            };
            const mapped = kws.map(k => map[String(k).toLowerCase()] || String(k)).filter(Boolean);
            const generic = new Set(['주가','시장','관련','소식','전망','기대','확대','증가','감소','하락','상승']);
            const macroOnly = new Set([
                'fomc','연준','fed','파월','금리','금리인하','금리인상','기준금리',
                'cpi','물가','소비자물가','인플레이션','고용','비농업','실업률','관세','무역'
            ]);
            const filtered = mapped.filter(x => !generic.has(x) && !macroOnly.has(String(x).toLowerCase()));
            const uniq = [...new Set(filtered)];
            if (uniq.length) materialText = uniq.slice(0, 2).join(' ');
        }

        // STEP 45: 거시 이벤트는 '현재 영향이 있는 자료'만 사용한다.
        // 과거 7월 FOMC처럼 오래된 뉴스/일정을 9월 현재 원인으로 재사용하지 않는다.
        // 뉴스는 pubDate 기준 최근 72시간만 인정하고, 캘린더는 오늘 기준
        // 과거 2일 이내 또는 향후 7일 이내의 이벤트만 인정한다.
        const nowMs = Date.now();
        const newsFreshMs = 72 * 60 * 60 * 1000;
        const parseNewsDate = (x) => {
            const raw = x?.pubDate || x?.publishedAt || x?.date || x?.datetime || '';
            if (!raw) return null;
            const t = Date.parse(String(raw));
            return Number.isFinite(t) ? t : null;
        };
        const globalMacroItems = Array.isArray(data.global_news) ? data.global_news.filter(x => {
            const title = String(x?.title || '').toLowerCase();
            const t = parseNewsDate(x);
            const fresh = t !== null && Math.abs(nowMs - t) <= newsFreshMs;
            return fresh && /\bfomc\b|연준|fed|파월|기준금리|금리인하|금리인상|cpi|소비자물가|인플레이션|고용|비농업|실업률/.test(title);
        }) : [];
        const macroSources = new Set(
            globalMacroItems.map(x => String(x?.media || x?.source || '').trim().toLowerCase()).filter(Boolean)
        );
        const calendarFomcItems = Array.isArray(data.market_calendar) ? data.market_calendar.filter(x => {
            const text = JSON.stringify(x).toLowerCase();
            if (!/\bfomc\b|fomc statement|fomc press conference|연준/.test(text)) return false;
            const rawDate = x?.date_key || x?.date || x?.event_date || '';
            if (!rawDate) return false;
            const t = Date.parse(String(rawDate).slice(0, 10) + 'T00:00:00');
            if (!Number.isFinite(t)) return false;
            const days = (t - nowMs) / 86400000;
            return days >= -2 && days <= 7;
        }) : [];
        const calendarHasFomc = calendarFomcItems.length > 0;
        const validatedMacro = macroSources.size >= 2 || calendarHasFomc;

        let macroEventText = '';
        if (validatedMacro && globalMacroItems.length) {
            const macroBlob = globalMacroItems.map(x => String(x?.title || '')).join(' ').toLowerCase();
            if (/\bfomc\b|연준|fed|파월/.test(macroBlob)) {
                if (/금리인하|인하 기대|기대 약화|기대 후퇴|금리인상|매파|긴축/.test(macroBlob)) {
                    macroEventText = 'FOMC·연준의 금리 경계';
                } else if (/기자회견|발언|성명/.test(macroBlob)) {
                    macroEventText = 'FOMC·연준 발언';
                } else {
                    macroEventText = 'FOMC·연준 관련 이슈';
                }
            } else if (/cpi|소비자물가|인플레이션|물가/.test(macroBlob)) {
                macroEventText = '미국 물가 관련 이슈';
            } else if (/고용|비농업|실업률/.test(macroBlob)) {
                macroEventText = '미국 고용 관련 이슈';
            }
        }

        // 4) 우선순위: 실제 수급 → 실제 공시/기업재료 → 검증된 거시 이벤트 → 시장 연동
        if (selectedFlow) {
            const flowWord = selectedFlow.value >= 0 ? '순매수' : '순매도';
            parts.push(
                `최근 5거래일 동안 ${selectedFlow.label}이(가) ${Math.abs(selectedFlow.value).toLocaleString('ko-KR')}주 ${flowWord}를 기록했어. 하단 수급 차트와 퀀트 지표를 함께 확인해봐!`
            );
        } else if (hasDisclosure || (hasMaterialCluster && materialText)) {
            const materialLabel = materialText || '기업 관련 재료';
            if (hasDisclosure) {
                parts.push(`최근 ${materialLabel} 관련 공시·재료가 확인됐어. 세부 공시 원문은 아래 DART 위젯에서 확인할 수 있어.`);
            } else {
                parts.push(`최근 ${materialLabel} 관련 재료가 확인됐어. 아래 뉴스와 하단 데이터를 함께 확인해봐.`);
            }
        } else if (macroEventText && !isCryptoMarket) {
            parts.push(`${macroEventText}가 여러 독립 출처에서 확인됐어. 시장 흐름과 하단 데이터를 함께 확인해봐.`);
        } else if (marketRate !== null && !isCryptoMarket) {
            parts.push(
                `특이한 개별 수급 근거가 확인되지 않는 가운데 ${selectedMarketName} 지수는 ${marketRate >= 0 ? '+' : ''}${marketRate.toFixed(2)}% ${upDown(marketRate)} 중이야. 시장 흐름과 하단 수급 데이터를 함께 확인해봐.`
            );
        } else {
            parts.push('현재 확인된 데이터만으로는 특정 원인을 단정하기 어려워. 하단 수급·퀀트 지표를 함께 확인해봐.');
        }

        parts.push('🐜 오늘도 개미들 힘내자! 💪');
        const gaemiComment = parts.join('\n\n');

        mrCard.innerHTML = `
            <div style="font-size:0.86rem;color:var(--text-sub);line-height:1.72;white-space:pre-line;word-break:keep-all;">
                ${gaemiComment}
            </div>
        `;

    }

    }

    window.GaemiWidgets = window.GaemiWidgets || {};
    window.GaemiWidgets.whyMovement = { render };
})();
