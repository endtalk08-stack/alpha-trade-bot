def fetch_naver_domestic_flow_metrics(code):
    """
    국내주식 실데이터: 당일 거래대금 + 최근 5거래일 외국인/기관 순매수.
    
    STEP 17 + 수정:
    - Naver integration API에서 거래대금을 못 찾으면 키움 ka10007/ka10005로 fallback.
    - 응답 구조가 바뀌어도 최소한 키움 데이터는 사용 가능.
    """
    result = {
        "trading_value": None,
        "foreigner_latest": None,
        "institution_latest": None,
        "foreigner_5d": None,
        "institution_5d": None,
        "trading_value_detail": "거래대금 데이터 확인 필요",
        "foreigner_detail": "외국인 수급 데이터 확인 필요",
        "institution_detail": "기관 수급 데이터 확인 필요",
        "source": "Naver",
    }
    if not code:
        return result

    def num(v):
        if v is None:
            return None
        if isinstance(v, (int, float)) and not isinstance(v, bool):
            return float(v)
        x = str(v).strip().replace(",", "").replace(" ", "")
        try:
            if any(u in x for u in ('조', '억', '만')):
                total = 0.0
                found = False
                for u, mul in [('조', 10**12), ('억', 10**8), ('만', 10**4)]:
                    m = re.search(r'([+-]?\d+(?:\.\d+)?)' + u, x)
                    if m:
                        total += float(m.group(1)) * mul
                        found = True
                return total if found else None
            return float(x.replace('%', ''))
        except Exception:
            return None

    def norm_key(v):
        return re.sub(r'[^0-9a-z가-힣]', '', str(v).lower())

    def walk_dicts(obj):
        if isinstance(obj, dict):
            yield obj
            for v in obj.values():
                yield from walk_dicts(v)
        elif isinstance(obj, list):
            for v in obj:
                yield from walk_dicts(v)

    # ===== 1) Naver integration =====
    try:
        r = HTTP_SESSION.get(f"https://m.stock.naver.com/api/stock/{code}/integration", timeout=3.0)
        if r.status_code == 200:
            data = r.json()

            # 1-1) 거래대금
            priority_groups = [
                {'accumulatedtradingvalue', 'accumulatedtradingamount',
                 'accumulatedtradingamountvalue', 'accumulatedamountvalue',
                 '누적거래대금', '누적거래대금액'},
                {'tradingvalue', 'tradingvalueamount', 'tradingamount',
                 '거래대금', '거래대금액'},
                {'amount', 'accumulatedamount'},
            ]
            candidates_by_group = [[] for _ in priority_groups]
            for obj in walk_dicts(data):
                for raw_k, v in obj.items():
                    k = norm_key(raw_k)
                    for idx, keys in enumerate(priority_groups):
                        if k in keys:
                            n = num(v)
                            if n is not None and n >= 0:
                                candidates_by_group[idx].append(n)
                            break

            for candidates in candidates_by_group:
                if candidates:
                    result['trading_value'] = max(candidates)
                    break

            if result['trading_value'] is not None:
                tv = result['trading_value']
                if tv >= 10**12:
                    result['trading_value_detail'] = f"당일 거래대금 {tv/10**12:.2f}조원"
                elif tv >= 10**8:
                    result['trading_value_detail'] = f"당일 거래대금 {tv/10**8:.0f}억원"
                else:
                    result['trading_value_detail'] = f"당일 거래대금 {tv:,.0f}원"

            # 1-2) 외국인/기관
            fv = []
            iv = []
            for it in (data.get('dealTrendInfos') or [])[-5:]:
                if not isinstance(it, dict):
                    continue
                for k in ('foreignerPureBuyQuant', 'foreignerNetBuyQuantity', 'foreignPureBuyQuant'):
                    v = num(it.get(k))
                    if v is not None:
                        fv.append(v)
                        break
                for k in ('organPureBuyQuant', 'institutionPureBuyQuant', 'institutionNetBuyQuantity'):
                    v = num(it.get(k))
                    if v is not None:
                        iv.append(v)
                        break

            if fv:
                total = sum(fv)
                result['foreigner_latest'] = fv[-1]
                result['foreigner_5d'] = total
                result['foreigner_detail'] = (
                    f"최근 5거래일 외국인 "
                    f"{'순매수' if total > 0 else '순매도' if total < 0 else '순매수/순매도 없음'} "
                    f"{abs(total):,.0f}주"
                )
            if iv:
                total = sum(iv)
                result['institution_latest'] = iv[-1]
                result['institution_5d'] = total
                result['institution_detail'] = (
                    f"최근 5거래일 기관 "
                    f"{'순매수' if total > 0 else '순매도' if total < 0 else '순매수/순매도 없음'} "
                    f"{abs(total):,.0f}주"
                )

            if result['trading_value'] is None:
                print(f"[NAVER FLOW] {code}: 거래대금 필드 미검출 → 키움 fallback 시도")
        else:
            print(f"[NAVER FLOW] {code} HTTP {r.status_code}")
    except Exception as exc:
        print(f"[NAVER FLOW] {code} Naver fetch failed: {type(exc).__name__}: {exc}")

    # ===== 2) ✅ [수정] 키움 fallback (거래대금이 없을 때만) =====
    if result['trading_value'] is None:
        try:
            kiwoom_tv = fetch_kiwoom_trading_value_evidence(code)
            if kiwoom_tv.get("available"):
                # 키움은 백만원 단위로 반환 (예: 12345 = 123.45억원)
                latest = kiwoom_tv.get("latest")
                if latest is not None:
                    # 백만원 → 원
                    result['trading_value'] = float(latest) * 1_000_000
                    tv = result['trading_value']
                    if tv >= 10**12:
                        result['trading_value_detail'] = f"당일 거래대금 {tv/10**12:.2f}조원 (키움)"
                    elif tv >= 10**8:
                        result['trading_value_detail'] = f"당일 거래대금 {tv/10**8:.0f}억원 (키움)"
                    else:
                        result['trading_value_detail'] = f"당일 거래대금 {tv:,.0f}원 (키움)"
                    result['source'] = 'Kiwoom'
                    print(f"[NAVER FLOW] {code}: 키움 fallback 성공 → {result['trading_value_detail']}")
        except Exception as exc:
            print(f"[NAVER FLOW] {code} Kiwoom fallback failed: {type(exc).__name__}: {exc}")

    return result
