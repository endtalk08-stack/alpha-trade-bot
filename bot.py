// ============================================================
// 개미들아 코멘트
// 기존 데이터만 사용
// 말투: 쉽고 자연스럽게 / 초보자용 / 센스 있게
// ============================================================

function buildGaemiComment(mr, data) {

    if (!mr) {
        return "아직 확인할 수 있는 데이터가 충분하지 않아.";
    }

    const reasons = Array.isArray(mr.reasons)
        ? mr.reasons
        : [];

    const getReason = (key) => {
        return reasons.find(r => r.key === key) || null;
    };

    const market = getReason("market");
    const theme = getReason("theme");
    const material = getReason("material");
    const flow = getReason("flow");
    const technical = getReason("technical");

    const positiveReasons = reasons.filter(
        r => Number(r.score || 0) > 0
    );

    const negativeReasons = reasons.filter(
        r => Number(r.score || 0) < 0
    );

    const comments = [];

    // --------------------------------------------------------
    // 숫자 변환
    // --------------------------------------------------------

    function num(value) {

        if (
            value === null ||
            value === undefined ||
            value === ""
        ) {
            return null;
        }

        const n = Number(
            String(value)
                .replace(/,/g, "")
                .replace(/%/g, "")
                .replace(/원/g, "")
                .trim()
        );

        return Number.isFinite(n) ? n : null;
    }


    // --------------------------------------------------------
    // 뉴스
    // --------------------------------------------------------

    function getNewsTitle() {

        const news = Array.isArray(data.stock_news)
            ? data.stock_news
            : [];

        for (const item of news) {

            if (item && item.title) {
                return String(item.title).trim();
            }
        }

        return "";
    }


    // --------------------------------------------------------
    // 주가 변화
    // --------------------------------------------------------

    let priceChange = null;

    try {

        const candles = Array.isArray(data.candles)
            ? data.candles
            : [];

        if (candles.length >= 2) {

            const today = num(
                candles[candles.length - 1].close
            );

            const yesterday = num(
                candles[candles.length - 2].close
            );

            if (
                today !== null &&
                yesterday !== null &&
                yesterday !== 0
            ) {

                priceChange =
                    ((today - yesterday) / yesterday) * 100;
            }
        }

    } catch (e) {
        priceChange = null;
    }


    // --------------------------------------------------------
    // 1. 첫 문장
    // 관심을 끄는 방식
    // --------------------------------------------------------

    if (mr.direction === "상승") {

        if (positiveReasons.length >= 3) {

            comments.push(
                `<strong>개미들아, 오늘은 그냥 오른 게 아니야.</strong>`
            );

        } else {

            comments.push(
                `<strong>개미들아, 오늘 왜 올랐는지 하나씩 볼게.</strong>`
            );
        }

    } else if (mr.direction === "하락") {

        comments.push(
            `<strong>개미들아, 오늘은 그냥 빠진 게 아니야.</strong>`
        );

    } else {

        comments.push(
            `<strong>개미들아, 오늘 움직임은 조금 복잡해.</strong>`
        );
    }


    // --------------------------------------------------------
    // 2. 시장
    // --------------------------------------------------------

    if (market) {

        const score = Number(market.score || 0);
        const detail = String(
            market.detail || ""
        ).trim();

        if (score > 0) {

            comments.push(
                `시장 분위기가 좋았고, ${detail || "전체 시장도 상승 흐름을 보였어"}. ` +
                `시장이 좋으면 투자자들의 분위기도 함께 좋아지면서 ` +
                `개별 종목에도 매수세가 들어오는 영향을 줄 수 있어.`
            );

        } else if (score < 0) {

            comments.push(
                `오늘 시장 분위기는 좋지 않았어. ` +
                `${detail || "전체 시장도 약한 모습을 보였어"}. ` +
                `이런 상황에서도 이 종목이 움직였다면 ` +
                `시장 전체보다 개별 종목의 재료가 더 강했는지 확인해볼 필요가 있어.`
            );
        }
    }


    // --------------------------------------------------------
    // 3. 테마
    // --------------------------------------------------------

    if (theme) {

        const detail = String(
            theme.detail || ""
        ).trim();

        let themeName = "";

        if (/반도체/i.test(detail)) {
            themeName = "반도체";
        } else if (/AI|인공지능/i.test(detail)) {
            themeName = "AI";
        } else if (/로봇/i.test(detail)) {
            themeName = "로봇";
        } else if (/바이오|제약/i.test(detail)) {
            themeName = "바이오";
        } else if (/2차전지|배터리/i.test(detail)) {
            themeName = "2차전지";
        } else if (/원전|원자력/i.test(detail)) {
            themeName = "원전";
        } else if (/방산|국방/i.test(detail)) {
            themeName = "방산";
        } else if (/우주|위성/i.test(detail)) {
            themeName = "우주";
        }


        if (themeName === "AI") {

            comments.push(
                `<strong>관련 업종도 같이 올라왔어.</strong><br>` +
                `지금 잡힌 테마는 AI야. ` +
                `AI 테마는 인공지능 서비스를 만드는 데 필요한 ` +
                `반도체·서버·장비·소프트웨어 등을 말해. ` +
                `AI 투자가 늘어나면 이런 제품과 서비스를 필요로 하는 곳도 많아질 수 있어. ` +
                `그래서 AI 관련 투자 기대가 커지면 관련 기업들이 함께 움직이는 경우가 있어. ` +
                `이 종목도 그중 <strong>실제 사업과 연결된 분야</strong>가 있어서 ` +
                `관련 움직임의 영향을 받고 있는지 확인해볼 필요가 있어.`
            );

        } else if (themeName === "반도체") {

            comments.push(
                `<strong>관련 반도체 업종도 같이 움직이고 있어.</strong><br>` +
                `반도체는 컴퓨터와 스마트폰, 자동차, 데이터센터 등에 들어가는 핵심 부품이야. ` +
                `특히 AI와 데이터센터 투자가 늘어나면 고성능 반도체 수요가 커질 수 있어. ` +
                `그래서 반도체 업황이 좋아질 것이라는 기대가 생기면 ` +
                `관련 기업들이 함께 주목받을 수 있어. ` +
                `이 종목도 반도체 산업과 연결된 사업을 하고 있기 때문에 ` +
                `이런 업종 흐름이 주가에 영향을 주고 있는지 확인해볼 필요가 있어.`
            );

        } else if (themeName === "로봇") {

            comments.push(
                `<strong>로봇 관련 종목들도 같이 움직이고 있어.</strong><br>` +
                `로봇 테마는 산업용 로봇부터 협동로봇, 서비스 로봇, 휴머노이드까지 ` +
                `로봇과 관련된 산업을 말해. ` +
                `기업들이 자동화에 돈을 많이 투자하거나 로봇 시장이 커질 것이라는 기대가 생기면 ` +
                `관련 기업들이 함께 주목받을 수 있어. ` +
                `이 종목도 로봇 산업과 연결된 사업이 있기 때문에 ` +
                `이번 움직임이 테마 전체의 흐름과 연결되는지 확인할 필요가 있어.`
            );

        } else if (themeName === "2차전지") {

            comments.push(
                `<strong>2차전지 관련 종목들도 같이 움직이고 있어.</strong><br>` +
                `2차전지는 전기차와 에너지저장장치 등에 사용하는 충전식 배터리야. ` +
                `전기차 판매가 늘거나 배터리 투자가 확대되고 신규 수주가 나오면 ` +
                `관련 기업들의 성장 기대가 커질 수 있어. ` +
                `이 종목도 배터리 산업과 연결된 사업이 있기 때문에 ` +
                `업종 전체의 움직임이 주가에 영향을 주고 있는지 확인해볼 필요가 있어.`
            );

        } else if (themeName === "바이오") {

            comments.push(
                `<strong>바이오 관련 종목들도 같이 움직이고 있어.</strong><br>` +
                `바이오는 신약 개발과 의약품, 임상시험 같은 생명과학 산업을 말해. ` +
                `임상 결과나 신약 승인, 기술이전 같은 소식 하나가 ` +
                `기업의 미래 가치에 큰 영향을 줄 수 있는 분야야. ` +
                `이 종목도 바이오 산업과 연결돼 있기 때문에 ` +
                `관련 재료가 실제 주가에 영향을 주고 있는지 살펴볼 필요가 있어.`
            );

        } else if (themeName === "원전") {

            comments.push(
                `<strong>원전 관련 종목들도 같이 움직이고 있어.</strong><br>` +
                `원전 테마는 원자력발전소와 원자로, 관련 부품과 장비를 만드는 산업이야. ` +
                `원전 건설 확대나 해외 수주, 정책 변화가 나오면 ` +
                `관련 기업들의 신규 매출 기대가 커질 수 있어. ` +
                `이 종목도 원전 사업과 연결돼 있다면 이런 기대가 주가에 반영되는지 확인할 필요가 있어.`
            );

        } else if (themeName === "방산") {

            comments.push(
                `<strong>방산 관련 종목들도 같이 움직이고 있어.</strong><br>` +
                `방산은 전투기와 함정, 레이더 등 군사 장비를 개발하고 생산하는 산업이야. ` +
                `국방비 증가나 해외 무기 수출, 대형 수주가 나오면 ` +
                `관련 기업의 매출 증가 기대가 커질 수 있어. ` +
                `이 종목도 방산 사업과 연결돼 있다면 이런 재료가 주가에 영향을 주는지 확인해볼 필요가 있어.`
            );

        } else if (detail) {

            comments.push(
                `<strong>관련 업종도 같이 움직이고 있어.</strong><br>` +
                `${detail}. ` +
                `이런 테마는 관련 산업에 새로운 재료가 생기면 ` +
                `여러 종목이 함께 움직일 수 있어.`
            );
        }
    }


    // --------------------------------------------------------
    // 4. 기업 재료
    // --------------------------------------------------------

    if (material) {

        const score =
            Number(material.score || 0);

        const newsTitle =
            getNewsTitle();

        if (score > 0) {

            if (newsTitle) {

                comments.push(
                    `<strong>여기에 기업 재료까지 확인됐어.</strong><br>` +
                    `오늘 확인된 주요 뉴스는 <strong>'${newsTitle}'</strong>이야.`
                );

            } else {

                comments.push(
                    `<strong>여기에 기업과 관련된 긍정적인 재료도 확인됐어.</strong>`
                );
            }

            comments.push(
                `이게 중요한 이유는 단순히 뉴스가 나왔다는 게 아니야. ` +
                `실제 매출이나 실적, 신규 사업과 연결될 가능성이 있는 재료라면 ` +
                `회사의 미래에 대한 투자자들의 기대가 달라질 수 있기 때문이야.`
            );

        } else if (score < 0) {

            if (newsTitle) {

                comments.push(
                    `<strong>반대로 주가에 부담이 될 수 있는 소식도 있어.</strong><br>` +
                    `'${newsTitle}'`
                );

            } else {

                comments.push(
                    `<strong>기업 쪽에서는 주가에 부담이 될 수 있는 재료가 확인됐어.</strong>`
                );
            }

            comments.push(
                `중요한 건 뉴스 제목만 보고 판단하지 않는 거야. ` +
                `실제로 회사의 매출이나 실적에 얼마나 영향을 줄 수 있는지 봐야 해.`
            );
        }
    }


    // --------------------------------------------------------
    // 5. 거래량
    // --------------------------------------------------------

    if (volumeRatio !== null) {

        if (volumeRatio >= 200) {

            comments.push(
                `<strong>그리고 오늘 거래량이 확 달라졌어.</strong> ` +
                `최근 5거래일 평균보다 약 ${volumeRatio.toFixed(0)}% 수준이야. ` +
                `쉽게 말하면 평소보다 훨씬 많은 사람들이 이 종목을 사고팔고 있다는 뜻이야. ` +
                `특히 주가가 오르면서 거래량까지 크게 늘었다면 ` +
                `가격만 오른 게 아니라 실제 매매 참여도 크게 늘었다는 의미라서 ` +
                `상승세가 얼마나 강한지 확인하는 데 중요한 자료가 될 수 있어.`
            );

        } else if (volumeRatio >= 120) {

            comments.push(
                `<strong>거래량도 평소보다 늘었어.</strong> ` +
                `최근 5거래일 평균의 약 ${volumeRatio.toFixed(0)}% 수준이야. ` +
                `쉽게 말하면 평소보다 많은 투자자가 이 종목의 매매에 참여하고 있다는 뜻이야. ` +
                `특히 주가가 오르면서 거래량도 같이 늘었다면 ` +
                `상승에 실제 매수세가 붙고 있는지 확인해볼 필요가 있어.`
            );

        } else if (volumeRatio <= 70) {

            comments.push(
                `<strong>그런데 거래량은 오히려 줄었어.</strong> ` +
                `최근 5거래일 평균의 약 ${volumeRatio.toFixed(0)}% 수준이야. ` +
                `가격은 움직이고 있지만 실제 매매 참여가 충분히 따라오고 있는지는 ` +
                `조금 더 확인해볼 필요가 있어.`
            );

        } else {

            comments.push(
                `거래량은 최근 5거래일 평균의 약 ${volumeRatio.toFixed(0)}% 수준이야. ` +
                `현재 가격 움직임에 거래 참여가 충분히 따라오고 있는지도 같이 봐야 해.`
            );
        }
    }


    // --------------------------------------------------------
    // 6. 기존 수급/거래 흐름
    // --------------------------------------------------------

    if (flow) {

        const score =
            Number(flow.score || 0);

        const detail =
            String(flow.detail || "").trim();

        if (score > 0) {

            comments.push(
                `<strong>거래 흐름도 같은 방향을 보여주고 있어.</strong> ` +
                `${detail}. ` +
                `가격이 움직이는 것과 실제 거래 흐름이 같이 움직이고 있다는 점이 중요해.`
            );

        } else if (score < 0) {

            comments.push(
                `<strong>다만 거래 흐름에서는 조금 다른 모습이 보여.</strong> ` +
                `${detail}. ` +
                `주가가 오르더라도 거래 흐름이 따라오지 않는다면 ` +
                `현재 상승이 얼마나 이어질지는 조금 더 확인해야 해.`
            );
        }
    }


    // --------------------------------------------------------
    // 7. 기술적 흐름
    // --------------------------------------------------------

    if (technical) {

        const score =
            Number(technical.score || 0);

        const detail =
            String(technical.detail || "").trim();

        if (score > 0) {

            comments.push(
                `<strong>가격 흐름도 상승 쪽이야.</strong> ` +
                `${detail}. ` +
                `20일선은 최근 약 한 달 동안의 평균 가격을 보는 기준이라고 생각하면 쉬워. ` +
                `현재 가격이 이 선 위에 있다는 건 최근 평균 가격보다 높은 수준에서 ` +
                `거래되고 있다는 뜻이야.`
            );

        } else if (score < 0) {

            comments.push(
                `<strong>그런데 가격 흐름에서는 조심할 부분도 있어.</strong> ` +
                `${detail}. ` +
                `주요 평균선 아래에서 움직인다면 단기적인 상승 흐름이 약해졌는지 ` +
                `확인할 필요가 있어.`
            );
        }
    }


    // --------------------------------------------------------
    // 8. 마지막 종합
    // --------------------------------------------------------

    if (mr.direction === "상승") {

        if (positiveReasons.length >= 3) {

            comments.push(
                `<strong>정리하면 오늘은 시장 + 테마 + 재료 + 거래량 + 기술적 흐름이 ` +
                `같은 방향으로 겹친 날이야.</strong> ` +
                `여러 데이터가 동시에 좋아지고 있다는 점에서 오늘 상승을 설명할 수 있는 ` +
                `근거가 비교적 많은 상황이야. ` +
                `다만 이것만으로 앞으로 계속 오른다고 단정할 수는 없어. ` +
                `다음 거래에서도 거래량과 수급, 가격 흐름이 계속 이어지는지 확인하는 게 중요해.`
            );

        } else {

            comments.push(
                `<strong>정리하면 오늘 상승을 설명할 수 있는 근거는 일부 확인돼.</strong> ` +
                `하지만 모든 데이터가 같은 방향으로 움직이는 건 아니야. ` +
                `그래서 지금 당장 상승이 계속된다고 단정하기보다는 ` +
                `다음 거래에서 거래량과 수급이 이어지는지 확인하는 게 좋아.`
            );
        }

    } else if (mr.direction === "하락") {

        comments.push(
            `<strong>정리하면 오늘 하락과 연결되는 신호가 확인되고 있어.</strong> ` +
            `하지만 하나의 데이터만 보고 이유를 정하기보다는 ` +
            `뉴스와 거래량, 시장 분위기, 가격 흐름을 함께 확인하는 게 중요해.`
        );

    } else {

        comments.push(
            `<strong>정리하면 오늘은 좋은 신호와 부담스러운 신호가 섞여 있어.</strong> ` +
            `지금 데이터만으로 주가가 어느 방향으로 계속 움직일지 단정하기는 어려워. ` +
            `새로운 뉴스가 나오는지, 거래량과 수급이 어느 방향으로 움직이는지를 ` +
            `계속 확인하는 게 좋아.`
        );
    }


    // --------------------------------------------------------
    // 출력
    // --------------------------------------------------------

    mrCard.innerHTML = `
        <div style="
            font-size:0.88rem;
            color:var(--text-sub);
            line-height:1.85;
            letter-spacing:-0.02em;
            word-break:keep-all;
        ">
            ${comments.map(text => `
                <div style="
                    margin-bottom:13px;
                ">
                    ${text}
                </div>
            `).join("")}
        </div>
    `;
}
