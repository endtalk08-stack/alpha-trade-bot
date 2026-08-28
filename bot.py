function buildGaemiComment(mr, data) {
    if (!mr) {
        return "현재 확인할 수 있는 데이터가 충분하지 않아요.";
    }

    const reasons = Array.isArray(mr.reasons) ? mr.reasons : [];

    const getReason = (key) => {
        return reasons.find(r => r.key === key) || null;
    };

    const material = getReason("material");
    const flow = getReason("flow");
    const technical = getReason("technical");
    const market = getReason("market");

    const paragraphs = [];

    // ------------------------------------------------
    // 1. 오늘 주가
    // ------------------------------------------------
    let priceText = "";

    try {
        const candles = data.candles || [];

        if (candles.length >= 2) {
            const today = Number(candles[candles.length - 1].close);
            const yesterday = Number(candles[candles.length - 2].close);

            if (yesterday > 0) {
                const change =
                    ((today - yesterday) / yesterday) * 100;

                if (change > 0) {
                    priceText =
                        `${data.name}은 오늘 ${change.toFixed(1)}% 오르고 있어요.`;
                } else if (change < 0) {
                    priceText =
                        `${data.name}은 오늘 ${change.toFixed(1)}% 하락하고 있어요.`;
                } else {
                    priceText =
                        `${data.name}은 오늘 가격 변화가 크지 않아요.`;
                }
            }
        }
    } catch (e) {
        priceText = "";
    }

    if (priceText) {
        paragraphs.push(
            priceText +
            " 지금부터 확인되는 데이터를 하나씩 쉽게 풀어볼게요."
        );
    }

    // ------------------------------------------------
    // 2. 기업 재료
    // ------------------------------------------------
    if (material) {
        const score = Number(material.score || 0);

        if (score > 0) {

            let newsText = "";

            if (
                Array.isArray(mr.news_evidence) &&
                mr.news_evidence.length > 0
            ) {
                const news = mr.news_evidence[0];

                if (news && news.title) {
                    newsText =
                        `현재 확인된 주요 뉴스는 '${news.title}'입니다.`;
                }
            }

            if (newsText) {
                paragraphs.push(
                    newsText
                );
            }

            paragraphs.push(
                "이런 소식이 중요한 이유는 단순히 뉴스가 하나 나왔다는 뜻이 아니라, " +
                "회사의 매출이나 실적, 신규 사업에 영향을 줄 가능성이 있기 때문이에요. " +
                "다만 뉴스가 나왔다고 주가가 반드시 계속 오르는 것은 아니기 때문에 " +
                "실제로 시장에서 어떻게 반응하고 있는지도 함께 확인해야 합니다."
            );

        } else if (score < 0) {

            paragraphs.push(
                "오늘 기업과 관련해서 주가에 부담이 될 수 있는 재료도 확인되고 있어요. " +
                "중요한 것은 뉴스의 제목만 보는 것이 아니라 실제로 회사의 실적이나 사업에 " +
                "어떤 영향을 줄 수 있는지를 확인하는 거예요."
            );

        } else {

            paragraphs.push(
                "현재 확인되는 뉴스와 공시만으로는 오늘 움직임을 직접 설명할 만큼 " +
                "뚜렷한 기업 재료는 충분하지 않아요."
            );
        }
    }

    // ------------------------------------------------
    // 3. 테마
    // ------------------------------------------------
    try {
        if (typeof data.theme === "string" && data.theme.trim()) {

            const theme = data.theme.trim();

            const themeExplain = {
                "반도체":
                    "컴퓨터와 스마트폰, 자동차, 데이터센터 등에 들어가는 반도체와 관련된 산업이에요. AI와 데이터센터 투자가 늘어나면 고성능 반도체 수요가 증가할 수 있어 관련 기업들이 함께 주목받을 수 있어요.",

                "AI":
                    "인공지능 서비스를 만들고 실행하는 데 필요한 기술과 장비, 소프트웨어와 관련된 산업이에요. AI 사용이 늘어나면 GPU와 메모리, 서버 같은 관련 장비에 대한 투자도 커질 수 있어요.",

                "로봇":
                    "산업용 로봇과 협동로봇, 서비스 로봇, 휴머노이드 등 로봇과 관련된 산업이에요. 기업의 자동화 투자가 늘어나면 로봇과 부품, 장비를 공급하는 기업도 관심을 받을 수 있어요.",

                "바이오":
                    "신약과 의약품, 임상시험 등 생명과학 기술과 관련된 산업이에요. 임상 결과나 신약 승인, 기술이전 같은 소식이 기업가치에 큰 영향을 줄 수 있어요.",

                "2차전지":
                    "전기차와 에너지저장장치 등에 사용되는 충전식 배터리 산업이에요. 전기차 판매와 배터리 투자, 신규 수주 등이 관련 기업의 성장 기대에 영향을 줄 수 있어요.",

                "방산":
                    "전투기와 함정, 레이더 등 군사 장비를 개발하고 생산하는 산업이에요. 국방비 증가나 해외 수주가 발생하면 관련 기업의 매출 증가 기대가 커질 수 있어요.",

                "원전":
                    "원자력발전소와 원전 부품, 장비 등에 관련된 산업이에요. 원전 건설 확대나 해외 수주 같은 재료가 나오면 관련 기업들이 함께 주목받을 수 있어요."
            };

            paragraphs.push(
                `이 종목은 현재 '${theme}' 테마와 연결해서 볼 수 있어요. ` +
                (
                    themeExplain[theme] ||
                    `${theme}와 관련된 기업들이 함께 움직이는 산업군이에요. ` +
                    "관련 뉴스나 정책, 투자 확대 같은 새로운 재료가 나오면 " +
                    "관련 종목들이 함께 움직일 수 있어요."
                )
            );
        }
    } catch (e) {}

    // ------------------------------------------------
    // 4. 거래량
    // ------------------------------------------------
    try {
        const candles = data.candles || [];

        if (candles.length >= 6) {

            const volumes = candles
                .slice(-6, -1)
                .map(x => Number(x.volume || 0))
                .filter(x => x > 0);

            const todayVolume =
                Number(candles[candles.length - 1].volume || 0);

            if (volumes.length > 0 && todayVolume > 0) {

                const avg =
                    volumes.reduce((a, b) => a + b, 0)
                    / volumes.length;

                const ratio =
                    (todayVolume / avg) * 100;

                if (ratio >= 150) {

                    paragraphs.push(
                        `오늘 거래량은 최근 5거래일 평균의 약 ${ratio.toFixed(0)}% 수준이에요. ` +
                        "쉽게 말하면 평소보다 훨씬 많은 사람들이 이 종목을 사고팔고 있다는 뜻이에요. " +
                        "특히 주가가 상승하면서 거래량까지 크게 증가했다면 " +
                        "가격 상승에 실제 매수 참여가 함께 늘고 있는지 확인할 필요가 있어요."
                    );

                } else if (ratio >= 120) {

                    paragraphs.push(
                        `오늘 거래량은 최근 5거래일 평균보다 늘어나 약 ${ratio.toFixed(0)}% 수준이에요. ` +
                        "거래량이 늘었다는 것은 이 종목에 대한 시장의 관심과 매매 참여가 " +
                        "평소보다 커졌다는 의미로 볼 수 있어요. " +
                        "다만 거래량 증가만으로 주가가 계속 오른다고 판단할 수는 없기 때문에 " +
                        "가격 방향과 함께 확인하는 게 중요합니다."
                    );

                } else if (ratio <= 70) {

                    paragraphs.push(
                        `오늘 거래량은 최근 5거래일 평균의 약 ${ratio.toFixed(0)}% 수준으로 줄었어요. ` +
                        "가격이 움직이더라도 거래 참여가 충분히 따라오고 있는지는 " +
                        "조금 더 확인할 필요가 있어요."
                    );

                } else {

                    paragraphs.push(
                        `오늘 거래량은 최근 5거래일 평균의 약 ${ratio.toFixed(0)}% 수준이에요. ` +
                        "평소와 비교해 거래 참여가 크게 달라졌는지는 조금 더 지켜볼 필요가 있어요."
                    );
                }
            }
        }
    } catch (e) {}

    // ------------------------------------------------
    // 5. 거래량·수급 분석
    // ------------------------------------------------
    if (flow) {

        const score = Number(flow.score || 0);
        const detail = flow.detail || "";

        if (score > 0) {

            paragraphs.push(
                `거래 흐름에서도 긍정적인 모습이 확인돼요. ${detail}. ` +
                "주가가 오르는 과정에서 거래가 함께 증가했다면 " +
                "단순히 가격만 올라간 것인지 실제 매수 참여가 늘어난 것인지 " +
                "확인하는 데 도움이 됩니다."
            );

        } else if (score < 0) {

            paragraphs.push(
                `거래 흐름에서는 부담스러운 모습도 확인돼요. ${detail}. ` +
                "가격이 하락하면서 거래까지 크게 늘었다면 " +
                "매도 참여가 강해지고 있는지 확인할 필요가 있어요."
            );
        }
    }

    // ------------------------------------------------
    // 6. 기술적 흐름
    // ------------------------------------------------
    if (technical) {

        const score = Number(technical.score || 0);
        const detail = technical.detail || "";

        if (score > 0) {

            paragraphs.push(
                `가격 흐름도 단기적으로 긍정적인 모습이에요. ${detail}. ` +
                "20일 이동평균선은 최근 약 한 달 동안의 평균 가격 흐름을 보는 기준이에요. " +
                "현재 가격이 이 선 위에 있다는 것은 최근 평균 가격보다 높은 수준에서 " +
                "거래되고 있다는 뜻입니다."
            );

        } else if (score < 0) {

            paragraphs.push(
                `가격 흐름에서는 부담스러운 모습도 보여요. ${detail}. ` +
                "20일 이동평균선 아래에서 움직이고 있다면 단기적인 상승 흐름이 약해졌는지 " +
                "확인할 필요가 있어요."
            );
        }
    }

    // ------------------------------------------------
    // 7. 시장 분위기
    // ------------------------------------------------
    if (market) {

        const score = Number(market.score || 0);
        const detail = market.detail || "";

        if (score > 0) {

            paragraphs.push(
                `시장 분위기도 우호적인 편이에요. ${detail}. ` +
                "시장 전체가 좋아지면 개별 종목에도 투자심리가 좋아지는 영향을 줄 수 있어요. " +
                "하지만 시장이 좋다는 이유만으로 이 종목의 상승을 설명할 수는 없기 때문에 " +
                "개별 재료와 함께 봐야 합니다."
            );

        } else if (score < 0) {

            paragraphs.push(
                `시장 전체 분위기는 부담스러운 편이에요. ${detail}. ` +
                "이런 상황에서도 이 종목이 상승한다면 시장 전체 흐름보다 " +
                "개별 종목의 재료가 더 강하게 작용하고 있는지 확인해볼 수 있어요."
            );
        }
    }

    // ------------------------------------------------
    // 8. 최종 판단
    // ------------------------------------------------
    const direction = mr.direction || "혼조";

    const positive =
        reasons.filter(r => Number(r.score || 0) > 0).length;

    const negative =
        reasons.filter(r => Number(r.score || 0) < 0).length;

    if (direction === "상승") {

        if (positive >= 3) {

            paragraphs.push(
                "정리하면 오늘 상승은 한 가지 이유만으로 보기보다 " +
                "기업 재료와 거래량, 가격 흐름, 시장 분위기 등 여러 신호가 " +
                "같은 방향으로 나타나는지 확인하는 게 중요해요. " +
                "여러 데이터가 동시에 같은 방향을 가리키고 있다는 점은 " +
                "오늘 움직임을 이해하는 데 중요한 근거가 됩니다. " +
                "다만 이것만으로 앞으로 계속 오른다고 단정할 수는 없어요."
            );

        } else {

            paragraphs.push(
                "정리하면 상승을 설명할 수 있는 근거가 일부 확인되고 있지만 " +
                "모든 데이터가 같은 방향으로 움직이는 것은 아니에요. " +
                "지금 확인된 정보만으로 상승 원인을 하나로 단정하기보다는 " +
                "추가 뉴스와 거래량, 가격 흐름이 계속 이어지는지 확인하는 게 좋아요."
            );
        }

    } else if (direction === "하락") {

        paragraphs.push(
            "정리하면 현재 하락과 연결되는 신호가 확인되고 있어요. " +
            "다만 하나의 데이터만으로 하락 원인을 단정하지 말고 " +
            "뉴스와 거래량, 시장 분위기가 같은 방향으로 움직이는지 " +
            "함께 확인하는 것이 중요합니다."
        );

    } else {

        paragraphs.push(
            "정리하면 현재 데이터에서는 상승과 하락을 설명하는 신호가 " +
            "서로 섞여 있어요. " +
            "지금 확인되는 정보만으로 특정 원인을 단정하기보다는 " +
            "새로운 뉴스와 거래량, 가격 흐름이 어떻게 이어지는지 확인하는 게 좋아요."
        );
    }

    return paragraphs
        .filter(Boolean)
        .join("\n\n");
}
