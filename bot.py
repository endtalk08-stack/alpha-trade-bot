# ----------------------------------------------------
# 움직임 원인 카드 - 초보자용 설명 코멘트
# 기존 데이터만 사용
# ----------------------------------------------------

def get_movement_comment(
    change_pct,
    volume_ratio=None,
    stock_news=None,
    dart_disclosures=None
):
    """
    기존에 수집된 데이터만 이용해서
    초보자가 이해하기 쉬운 설명형 코멘트를 만든다.

    새로운 데이터 수집 없음.
    고정된 가짜 숫자 없음.
    """

    comments = []

    # ------------------------------------------------
    # 1. 주가
    # ------------------------------------------------

    try:
        change = float(change_pct)
    except (TypeError, ValueError):
        change = None

    if change is None:

        comments.append(
            "오늘 주가 움직임을 정확하게 확인하기는 어려워."
        )

    elif change > 0.5:

        comments.append(
            f"개미들아, 오늘은 그냥 오른 게 아니야.\n\n"
            f"오늘 주가가 {change:+.2f}% 상승하고 있어. "
            f"이제부터 왜 올랐는지 확인되는 데이터 하나씩 볼게."
        )

    elif change < -0.5:

        comments.append(
            f"개미들아, 오늘은 그냥 빠진 게 아니야.\n\n"
            f"오늘 주가가 {change:+.2f}% 하락하고 있어. "
            f"현재 확인되는 데이터를 기준으로 왜 움직였는지 볼게."
        )

    else:

        comments.append(
            "개미들아, 오늘은 주가 움직임이 크지 않아.\n\n"
            "지금 확인되는 데이터에서 특별한 변화가 있는지 하나씩 볼게."
        )


    # ------------------------------------------------
    # 2. 뉴스 / 기업 재료
    # ------------------------------------------------

    news_list = stock_news or []

    valid_news = []

    for item in news_list:

        if not isinstance(item, dict):
            continue

        title = str(
            item.get("title")
            or item.get("headline")
            or ""
        ).strip()

        if title:
            valid_news.append(title)

    if valid_news:

        news_title = valid_news[0]

        if change is not None and change > 0.5:

            comments.append(
                "여기에 기업 관련 뉴스도 확인됐어.\n\n"
                f"오늘 확인된 주요 내용은 '{news_title}'이야.\n\n"
                "뉴스가 중요한 이유는 단순히 기사 하나가 나왔다는 게 아니야. "
                "회사의 매출이나 실적, 신규 사업, 수주 같은 실제 사업과 연결되는 "
                "내용이라면 투자자들이 앞으로 벌어들일 돈에 대한 기대를 바꿀 수 있어.\n\n"
                "다만 뉴스가 좋다고 무조건 주가가 계속 오르는 건 아니야. "
                "이미 시장에 알려진 내용인지, 실제 거래량과 주가가 어떻게 반응했는지도 "
                "같이 봐야 해."
            )

        elif change is not None and change < -0.5:

            comments.append(
                "기업 쪽에서도 오늘 주가에 영향을 줄 수 있는 뉴스가 확인됐어.\n\n"
                f"확인된 주요 내용은 '{news_title}'이야.\n\n"
                "중요한 건 뉴스 제목만 보고 판단하는 게 아니야. "
                "이 내용이 실제 매출이나 실적, 사업에 어떤 영향을 줄 수 있는지 "
                "확인해야 오늘 하락과 연결해서 볼 수 있어."
            )

        else:

            comments.append(
                "기업 관련 뉴스도 확인됐어.\n\n"
                f"주요 내용은 '{news_title}'이야.\n\n"
                "다만 현재 주가 움직임과 이 뉴스가 직접 연결됐다고 "
                "단정하기보다는 거래량과 다른 데이터도 함께 확인해야 해."
            )

    else:

        comments.append(
            "그런데 현재 확인되는 종목 뉴스에서는 "
            "오늘 움직임을 바로 설명할 만한 뚜렷한 내용이 충분하지 않아.\n\n"
            "확인되지 않은 재료를 억지로 연결하기보다는 "
            "실제로 확인되는 데이터만 가지고 보는 게 좋아."
        )


    # ------------------------------------------------
    # 3. 거래량
    # ------------------------------------------------

    if volume_ratio is not None:

        try:
            ratio = float(volume_ratio)
        except (TypeError, ValueError):
            ratio = None

        if ratio is not None:

            if ratio >= 200:

                comments.append(
                    "그리고 오늘 거래량이 확 달라졌어.\n\n"
                    f"최근 5거래일 평균을 100으로 보면 "
                    f"오늘 거래량은 약 {ratio:.0f}% 수준이야.\n\n"
                    "쉽게 말하면 평소보다 훨씬 많은 사람들이 "
                    "이 종목을 사고팔고 있다는 뜻이야.\n\n"
                    "특히 주가가 오르면서 거래량까지 크게 늘었다면 "
                    "가격만 올라간 게 아니라 실제 매매 참여도 함께 늘어난 거라서 "
                    "상승세가 얼마나 강한지 확인하는 데 중요한 자료가 될 수 있어."

                )

            elif ratio >= 120:

                comments.append(
                    "거래량도 평소보다 늘었어.\n\n"
                    f"최근 5거래일 평균 대비 약 {ratio:.0f}% 수준이야.\n\n"
                    "쉽게 말하면 평소보다 많은 투자자가 "
                    "이 종목의 매매에 참여하고 있다는 뜻이야.\n\n"
                    "특히 주가가 상승하면서 거래량도 같이 증가했다면 "
                    "실제 매수 참여가 커지고 있는지 확인해볼 필요가 있어."
                )

            elif ratio <= 70:

                comments.append(
                    "그런데 거래량은 오히려 줄었어.\n\n"
                    f"최근 5거래일 평균 대비 약 {ratio:.0f}% 수준이야.\n\n"
                    "가격은 움직이고 있지만 거래에 참여하는 사람이 "
                    "평소보다 적다는 뜻이야.\n\n"
                    "따라서 현재 움직임이 강하게 이어질지는 "
                    "추가 거래량이 붙는지 확인할 필요가 있어."
                )

            else:

                comments.append(
                    f"거래량은 최근 5거래일 평균 대비 "
                    f"약 {ratio:.0f}% 수준이야.\n\n"
                    "평소와 비교해 거래 참여가 크게 달라진 상황은 아니어서 "
                    "현재 주가 움직임이 거래량을 동반하고 있는지는 "
                    "조금 더 지켜볼 필요가 있어."
                )


    # ------------------------------------------------
    # 4. 공시
    # ------------------------------------------------

    disclosure_list = dart_disclosures or []

    valid_disclosures = []

    for item in disclosure_list:

        if not isinstance(item, dict):
            continue

        title = str(
            item.get("title")
            or item.get("reportTitle")
            or ""
        ).strip()

        if title:
            valid_disclosures.append(title)

    if valid_disclosures:

        disclosure_title = valid_disclosures[0]

        comments.append(
            "공시에서도 확인할 내용이 있어.\n\n"
            f"최근 확인된 공시는 '{disclosure_title}'이야.\n\n"
            "공시는 회사가 투자자에게 공식적으로 알리는 내용이기 때문에 "
            "단순한 소문보다 실제 사업이나 재무 상황을 확인하는 데 도움이 돼.\n\n"
            "다만 공시가 있다는 사실만으로 주가가 오르거나 내린다고 "
            "판단하면 안 되고 내용이 실제 기업가치에 어떤 영향을 주는지 봐야 해."
        )


    # ------------------------------------------------
    # 5. 최종 종합
    # ------------------------------------------------

    if change is not None and change > 0.5:

        if ratio is not None and ratio >= 120 and valid_news:

            comments.append(
                "정리하면 오늘은 그냥 가격만 오른 게 아니야.\n\n"
                "기업 관련 뉴스가 확인되고 거래량도 평소보다 크게 늘면서 "
                "실제 시장 참여가 함께 증가한 모습이야.\n\n"
                "즉, 오늘 상승을 설명할 수 있는 재료와 거래 움직임이 "
                "같은 방향으로 겹쳤다는 점이 중요해.\n\n"
                "다만 여기서 바로 '계속 오른다'고 생각하면 안 돼. "
                "다음 거래에서도 거래량과 가격 흐름이 이어지는지 확인하는 게 중요해."
            )

        elif ratio is not None and ratio >= 120:

            comments.append(
                "정리하면 오늘은 주가 상승과 거래량 증가가 함께 나타났어.\n\n"
                "가격이 오르면서 거래 참여까지 늘었다는 점은 긍정적으로 볼 수 있지만 "
                "기업 재료가 뚜렷하게 확인되는지는 별도로 확인해야 해.\n\n"
                "내일도 거래량이 유지되면서 가격이 같은 방향으로 움직이는지 "
                "확인하는 게 중요해."
            )

        elif valid_news:

            comments.append(
                "정리하면 오늘 상승에는 기업 관련 뉴스가 눈에 들어와.\n\n"
                "다만 거래량이 강하게 증가한 상황까지 확인되는 것은 아니기 때문에 "
                "뉴스 하나만 가지고 상승이 계속될 거라고 판단하면 안 돼.\n\n"
                "다음 거래에서 거래량과 가격 흐름이 어떻게 이어지는지 확인해보자."
            )

        else:

            comments.append(
                "정리하면 오늘 주가는 상승했지만 "
                "현재 확인된 데이터만으로 확실한 상승 원인을 하나로 정하기는 어려워.\n\n"
                "괜히 이유를 만들어내기보다는 "
                "추가 뉴스가 나오는지와 거래량이 붙는지를 확인하는 게 좋아."
            )


    elif change is not None and change < -0.5:

        if ratio is not None and ratio >= 120:

            comments.append(
                "정리하면 오늘은 주가가 하락하는 가운데 거래량도 크게 늘었어.\n\n"
                "쉽게 말하면 가격이 내려가는 과정에서 매매 참여까지 많아졌다는 뜻이야.\n\n"
                "이 경우에는 단순한 조정인지 실제 매도세가 강해진 건지 "
                "다음 거래에서도 거래량과 가격 흐름이 이어지는지 확인하는 게 중요해."
            )

        else:

            comments.append(
                "정리하면 오늘 하락은 확인되는 뉴스와 거래 흐름을 함께 봐야 해.\n\n"
                "현재 데이터만으로 하나의 원인을 확정하기보다는 "
                "추가 뉴스와 거래량 변화가 어떻게 이어지는지 확인하는 게 좋아."
            )


    else:

        comments.append(
            "정리하면 오늘은 주가 움직임이 크지 않은 만큼 "
            "지금 당장 하나의 방향을 정하기는 어려워.\n\n"
            "새로운 뉴스가 나오는지, 거래량이 갑자기 늘어나는지 "
            "확인하면서 다음 움직임을 보는 게 좋아."
        )


    return "\n\n".join(
        comment.strip()
        for comment in comments
        if comment and comment.strip()
    )


def build_movement_confirmation(
    raw_price,
    df,
    stock_news,
    dart_disclosures
):
    """
    기존 데이터만 사용한다.
    기존 데이터 수집 로직은 변경하지 않는다.
    """

    change_pct = None
    prev_close = None

    # ------------------------------------------------
    # 현재가 대비 전일 종가
    # ------------------------------------------------

    try:

        if df is not None and len(df) >= 2:

            prev_close = float(
                df["Close"].iloc[-2]
            )

            if raw_price is not None and prev_close:

                change_pct = (
                    (float(raw_price) - prev_close)
                    / prev_close
                ) * 100

    except Exception:
        pass


    # ------------------------------------------------
    # 거래량
    # 기존 방식 그대로
    # ------------------------------------------------

    volume_text = "-"
    volume_sub = "거래량 데이터를 확인할 수 없습니다."

    volume_ratio = None

    try:

        if (
            df is not None
            and len(df) >= 6
            and "Volume" in df.columns
        ):

            vols = df["Volume"].astype(float)

            current_volume = float(
                vols.iloc[-1]
            )

            avg_volume = float(
                vols.tail(6).iloc[:-1].mean()
            )

            if avg_volume > 0:

                volume_ratio = (
                    current_volume
                    / avg_volume
                ) * 100

                volume_text = (
                    f"평균 대비 "
                    f"{volume_ratio - 100:+.0f}%"
                )

                volume_sub = (
                    f"최근 5거래일 평균 대비 · "
                    f"현재 비율 {volume_ratio:.0f}%"
                )

    except Exception:
        pass


    # ------------------------------------------------
    # 설명형 코멘트
    # ------------------------------------------------

    comment = get_movement_comment(
        change_pct=change_pct,
        volume_ratio=volume_ratio,
        stock_news=stock_news,
        dart_disclosures=dart_disclosures
    )


    # ------------------------------------------------
    # 기존 화면 데이터는 그대로 반환
    # ------------------------------------------------

    return {
        "change_pct": change_pct,
        "volume_text": volume_text,
        "volume_sub": volume_sub,
        "news_count": len(stock_news or []),
        "disclosure_count": len(dart_disclosures or []),
        "comment": comment,
    }
