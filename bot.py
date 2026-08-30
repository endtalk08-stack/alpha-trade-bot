def fetch_all_real_earnings():
    """
    실적 캘린더 실제 데이터 수집기

    핵심 변경사항
    1. Finnhub에서 받은 실적 일정을 고정 종목 목록으로 제한하지 않는다.
    2. 앞으로 30일 실적을 중심으로 수집한다.
    3. 주요 종목은 기존 MAJOR_US_TECH_TICKERS 기준으로 별표를 부여한다.
    4. 그 외 기업도 실적 일정이 있으면 ★☆☆로 표시한다.
    5. Yahoo는 Finnhub 누락 종목의 보조 수단으로만 사용한다.
    6. 한국 기업은 현재 API에서 실제 일정이 확인되는 경우에만 표시한다.
    7. 같은 종목/날짜/시간 중복은 마지막에 제거한다.
    """

    events = []

    # ----------------------------------------------------
    # 기본 설정
    # ----------------------------------------------------
    now_kst = datetime.now(timezone(timedelta(hours=9)))
    today_kst = now_kst.date()

    # 화면에서 필요한 기간:
    # 오늘 포함 앞으로 30일
    future_end_date = today_kst + timedelta(days=30)

    # API는 약간 넓게 조회한다.
    # 오늘보다 하루 앞선 일정도 발표 완료 상태 확인을 위해 확보한다.
    api_from_date = today_kst - timedelta(days=7)
    api_to_date = future_end_date

    # 기존 주요 종목 한글명
    known_names = {
        sym.upper(): ko
        for sym, ko in MAJOR_US_TECH_TICKERS
    }

    # 국내 주요 종목명
    for k_code, k_name in MAJOR_KR_EARNINGS.items():
        known_names[str(k_code).upper()] = k_name

    # ----------------------------------------------------
    # 중요도 판정
    # ----------------------------------------------------
    tier3_symbols = {
        "NVDA", "AVGO", "AAPL", "MSFT", "AMZN",
        "GOOGL", "META", "TSLA", "AMD", "ORCL",
        "MU", "COST"
    }

    tier2_symbols = {
        "ADBE", "CRM", "NFLX", "INTC", "QCOM",
        "AMAT", "CSCO", "PANW", "SNOW", "DELL",
        "HPE", "FDX", "NKE", "ACN", "CTAS",
        "CIEN", "ADSK", "MRVL"
    }

    def get_star_info(sym):
        sym = str(sym or "").upper().strip()

        if sym in tier3_symbols:
            return 3, "★★★"

        if sym in tier2_symbols:
            return 2, "★★☆"

        return 1, "★☆☆"

    # ----------------------------------------------------
    # 공통 이벤트 생성
    # ----------------------------------------------------
    def add_event(
        sym,
        raw_date,
        hour="amc",
        eps_est=None,
        rev_est=None,
        eps_actual=None,
        rev_actual=None,
        source="Finnhub"
    ):
        sym = str(sym or "").upper().strip()
        raw_date = str(raw_date or "").strip()

        if not sym or not raw_date:
            return

        try:
            us_date = datetime.strptime(
                raw_date,
                "%Y-%m-%d"
            ).date()
        except Exception:
            return

        # ------------------------------------------------
        # 화면에 필요한 기간만 유지
        #
        # 과거 일정은 너무 많이 넣지 않는다.
        # 오늘 이전 7일까지는 발표완료 확인용으로 허용.
        # ------------------------------------------------
        if us_date < api_from_date or us_date > future_end_date:
            return

        hour = str(hour or "amc").lower().strip()

        # ------------------------------------------------
        # 발표 시간 결정
        #
        # Finnhub은 날짜 + amc/bmo/dmh 형태로 주는 경우가
        # 많기 때문에 미국 동부시간 기준으로 변환한다.
        # ------------------------------------------------
        dt_utc = None

        if ZoneInfo is not None:
            try:
                et_zone = ZoneInfo("America/New_York")

                if hour == "bmo":
                    local_time = datetime.strptime(
                        raw_date + " 06:00",
                        "%Y-%m-%d %H:%M"
                    ).replace(tzinfo=et_zone)

                elif hour == "amc":
                    local_time = datetime.strptime(
                        raw_date + " 16:00",
                        "%Y-%m-%d %H:%M"
                    ).replace(tzinfo=et_zone)

                else:
                    # 장중 발표는 임의의 09:30을 그대로
                    # 사용자에게 보여주지 않고 16:00 기준으로 처리
                    local_time = datetime.strptime(
                        raw_date + " 16:00",
                        "%Y-%m-%d %H:%M"
                    ).replace(tzinfo=et_zone)

                dt_utc = local_time.astimezone(timezone.utc)

            except Exception:
                dt_utc = None

        # ------------------------------------------------
        # 최종 fallback
        # ------------------------------------------------
        if dt_utc is None:
            base_utc = datetime.strptime(
                raw_date,
                "%Y-%m-%d"
            ).replace(tzinfo=timezone.utc)

            if hour == "bmo":
                dt_utc = base_utc + timedelta(hours=10)

            elif hour == "amc":
                dt_utc = base_utc + timedelta(hours=20)

            else:
                dt_utc = base_utc + timedelta(hours=20)

        # ------------------------------------------------
        # 주요 종목은 Yahoo 실제 timestamp가 있으면
        # 해당 timestamp를 사용한다.
        #
        # 단, Finnhub 날짜와 하루 이상 차이나면 사용하지 않는다.
        # ------------------------------------------------
        yahoo_dt = yahoo_times.get(sym) if "yahoo_times" in locals() else None

        if yahoo_dt is not None:
            try:
                if abs((yahoo_dt.date() - us_date).days) <= 1:
                    normalized_dt, normalized_hour = \
                        _normalize_earnings_timestamp(sym, yahoo_dt)

                    # Yahoo 시간이 정상적으로 확인된 경우만 사용
                    if normalized_dt is not None:
                        dt_utc = normalized_dt
                        hour = normalized_hour
            except Exception:
                pass

        # ------------------------------------------------
        # 이벤트 생성
        # ------------------------------------------------
        stars, star_str = get_star_info(sym)

        ko_name = known_names.get(sym, sym)

        # EPS
        if eps_est is not None:
            try:
                eps_str = f"예상 EPS ${float(eps_est):.2f}"
            except Exception:
                eps_str = "예상치 집계중"
        else:
            eps_str = "예상치 집계중"

        # 매출
        if rev_est is not None:
            try:
                rev_str = f"예상 매출 ${float(rev_est) / 1e9:.1f}B"
            except Exception:
                rev_str = "-"
        else:
            rev_str = "-"

        # 발표 완료 여부
        has_actual = (
            eps_actual is not None or
            rev_actual is not None
        )

        if has_actual:
            val_str = "발표완료"
        else:
            val_str = "-"

        # 발표시간 표기
        hour_label = (
            "(美장마감후)"
            if hour == "amc"
            else "(美장시작전)"
            if hour == "bmo"
            else ""
        )

        # ------------------------------------------------
        # 기존 build_earnings_event 사용
        # ------------------------------------------------
        try:
            built = build_earnings_event(
                sym,
                ko_name,
                dt_utc,
                eps_str,
                rev_str,
                hour,
                actual_eps=eps_actual,
                actual_rev=rev_actual,
                expected_eps=eps_est,
                expected_rev=rev_est
            )

            # build 함수에서 만든 이벤트에 중요도 정보 보강
            for ev in built:
                ev["stars"] = stars
                ev["star_str"] = star_str
                ev["source"] = source

                # 기존 함수에서 이미 처리하지만
                # 명시적으로 예상 매출을 보존
                ev["expected_revenue"] = rev_est
                ev["expected_revenue_str"] = rev_str

                events.append(ev)

        except Exception as exc:
            print(
                f"[CALENDAR][EARN] 이벤트 생성 실패 "
                f"{sym} {raw_date}: {type(exc).__name__}: {exc}"
            )

    # ----------------------------------------------------
    # Yahoo timestamp 확보
    #
    # 기존 주요 종목 목록은 시간 보정용으로만 사용한다.
    # '조회 대상 제한' 용도가 아니다.
    # ----------------------------------------------------
    us_symbols = [
        item[0]
        for item in MAJOR_US_TECH_TICKERS
    ]

    yahoo_times = _fetch_yahoo_earnings_timestamps(us_symbols)

    # ----------------------------------------------------
    # Tier 1
    # Finnhub 전체 실적 일정
    # ----------------------------------------------------
    if FINNHUB_API_KEY:
        try:
            from_d = api_from_date.strftime("%Y-%m-%d")
            to_d = api_to_date.strftime("%Y-%m-%d")

            url = (
                "https://finnhub.io/api/v1/calendar/earnings"
                f"?from={from_d}"
                f"&to={to_d}"
                f"&token={FINNHUB_API_KEY}"
            )

            res = HTTP_SESSION.get(
                url,
                timeout=5.0
            ).json()

            items = res.get(
                "earningsCalendar",
                []
            )

            print(
                f"[EARNINGS DEBUG] Finnhub 조회기간: "
                f"{from_d} ~ {to_d}"
            )

            print(
                f"[EARNINGS DEBUG] Finnhub 전체 원본: "
                f"{len(items)}건"
            )

            captured_symbols = set()

            for it in items:
                sym = str(
                    it.get("symbol", "")
                ).upper().strip()

                if not sym:
                    continue

                raw_date = str(
                    it.get("date", "")
                ).strip()

                if not raw_date:
                    continue

                add_event(
                    sym=sym,
                    raw_date=raw_date,
                    hour=it.get("hour", "amc"),
                    eps_est=it.get("epsEstimate"),
                    rev_est=it.get("revenueEstimate"),
                    eps_actual=it.get("epsActual"),
                    rev_actual=it.get("revenueActual"),
                    source="Finnhub"
                )

                captured_symbols.add(sym)

            print(
                f"[EARNINGS DEBUG] Finnhub 반영: "
                f"{len(events)}건"
            )

        except Exception as exc:
            print(
                "[CALENDAR][EARN] Finnhub 오류:",
                repr(exc)
            )

    # ----------------------------------------------------
    # Tier 2
    # Yahoo 보조 수집
    #
    # Finnhub에 없는 '주요 종목'의 다음 실적 일정 보완
    # ----------------------------------------------------
    try:
        captured_symbols = {
            str(ev.get("ticker", "")).upper()
            for ev in events
            if ev.get("ticker")
        }

        missing = [
            s for s in us_symbols
            if s.upper() not in captured_symbols
        ]

        if missing:
            url = (
                "https://query1.finance.yahoo.com/v7/finance/quote"
                f"?symbols={','.join(missing)}"
            )

            res = HTTP_SESSION.get(
                url,
                timeout=3.0
            ).json()

            items = res.get(
                "quoteResponse",
                {}
            ).get(
                "result",
                []
            )

            print(
                f"[YAHOO DEBUG] 보조 조회: "
                f"{len(items)}건"
            )

            for it in items:
                sym = str(
                    it.get("symbol", "")
                ).upper().strip()

                raw_ts = (
                    it.get("earningsTimestamp")
                    or
                    it.get("earningsTimestampStart")
                )

                if not sym or not raw_ts:
                    continue

                try:
                    dt_utc = datetime.fromtimestamp(
                        int(raw_ts),
                        tz=timezone.utc
                    )
                except Exception:
                    continue

                # Yahoo 실제 timestamp를 KST 날짜로 변환하기 전에
                # 앞으로 30일 범위인지 확인
                try:
                    kst_date = dt_utc.astimezone(
                        timezone(timedelta(hours=9))
                    ).date()

                    if (
                        kst_date < today_kst
                        or
                        kst_date > future_end_date
                    ):
                        continue

                except Exception:
                    continue

                dt_utc, hour = _normalize_earnings_timestamp(
                    sym,
                    dt_utc
                )

                eps_f = it.get("epsForward")

                eps_str = (
                    f"예상 EPS ${float(eps_f):.2f}"
                    if eps_f is not None
                    else "예상치 집계중"
                )

                try:
                    built = build_earnings_event(
                        sym,
                        known_names.get(sym, sym),
                        dt_utc,
                        eps_str,
                        "-",
                        hour
                    )

                    stars, star_str = get_star_info(sym)

                    for ev in built:
                        ev["stars"] = stars
                        ev["star_str"] = star_str
                        ev["source"] = "Yahoo"

                        events.append(ev)

                except Exception as exc:
                    print(
                        f"[CALENDAR][EARN] Yahoo 이벤트 실패 "
                        f"{sym}: {repr(exc)}"
                    )

    except Exception as exc:
        print(
            "[CALENDAR][EARN] Yahoo 보조 조회 오류:",
            repr(exc)
        )

    # ----------------------------------------------------
    # 중복 제거
    #
    # 같은 기업 + 같은 날짜 + 같은 시간이면 1개만 유지
    # ----------------------------------------------------
    unique = {}

    for ev in events:
        ticker = str(
            ev.get("ticker", "")
        ).upper().strip()

        date_key = str(
            ev.get("date_key", "")
        ).strip()

        event_time = str(
            ev.get("time", "")
        ).strip()

        if not ticker or not date_key:
            continue

        key = (
            ticker,
            date_key,
            event_time
        )

        # Finnhub 데이터를 우선한다.
        # 이미 Finnhub이 있으면 Yahoo가 덮어쓰지 않는다.
        if key not in unique:
            unique[key] = ev

        else:
            old_source = str(
                unique[key].get("source", "")
            )

            new_source = str(
                ev.get("source", "")
            )

            if old_source != "Finnhub" and new_source == "Finnhub":
                unique[key] = ev

    # ----------------------------------------------------
    # 최종 정렬
    # ----------------------------------------------------
    final_events = sorted(
        unique.values(),
        key=lambda x: (
            str(x.get("date_key", "")),
            str(x.get("time", "")),
            str(x.get("ticker", ""))
        )
    )

    # ----------------------------------------------------
    # 디버그
    # ----------------------------------------------------
    future_events = []

    for ev in final_events:
        try:
            date_key = str(
                ev.get("date_key", "")
            )

            y, m, d = map(
                int,
                date_key.split(".")
            )

            event_date = date(
                y,
                m,
                d
            )

            if today_kst <= event_date <= future_end_date:
                future_events.append(ev)

        except Exception:
            continue

    print(
        f"[EARNINGS FINAL] "
        f"전체={len(final_events)}건 / "
        f"앞으로30일={len(future_events)}건"
    )

    # 날짜별 건수 확인
    try:
        date_counts = {}

        for ev in future_events:
            dk = ev.get("date_key")

            if dk:
                date_counts[dk] = (
                    date_counts.get(dk, 0) + 1
                )

        print(
            "[EARNINGS FINAL] 앞으로30일 날짜별:",
            date_counts
        )

    except Exception:
        pass

    return final_events
