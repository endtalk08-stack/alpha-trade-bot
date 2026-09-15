"""시장 애널리스트 컨센서스 수집 모듈."""

import time
import threading
import re
import json
import requests

FINNHUB_API_KEY = ""
ALPHA_VANTAGE_API_KEY = ""
FINNHUB_CONSENSUS_CACHE = {}
FINNHUB_CONSENSUS_CACHE_TTL = 1800
CONSENSUS_RESULT_CACHE = {}
CONSENSUS_RESULT_CACHE_TTL = 900
CONSENSUS_CACHE_LOCK = threading.Lock()

from numeric_utils import _safe_float

def _fetch_market_consensus_unified_uncached(symbol, market_name, raw_price, formal_name=""):
    """실제 확인 가능한 국내/미국 애널리스트 컨센서스를 수집한다.

    이 함수는 외부 API를 실제 호출하는 원본 수집기다.
    반복 검색 방지는 바깥의 fetch_market_consensus_unified()가 담당한다.
    """
    try:
        def _walk_objects(obj, depth=0):
            if depth > 14:
                return
            if isinstance(obj, dict):
                yield obj
                for v in obj.values():
                    yield from _walk_objects(v, depth + 1)
            elif isinstance(obj, list):
                for v in obj[:500]:
                    yield from _walk_objects(v, depth + 1)

        def _safe_num(v):
            try:
                if isinstance(v, dict):
                    for k in ("raw", "value", "amount", "targetPrice", "price", "val"):
                        if k in v:
                            n = _safe_num(v[k])
                            if n is not None:
                                return n
                    return None
                if v is None or v == "":
                    return None
                s = str(v).replace(",", "").replace("원", "").replace("KRW", "").strip()
                n = float(s)
                return n if n > 0 else None
            except Exception:
                return None

        def _value(node, keys):
            if not isinstance(node, dict):
                return None
            wanted = {str(k).lower() for k in keys}
            for k, v in node.items():
                if str(k).lower() in wanted and v not in (None, "", [], {}):
                    return v
            return None

        def _number(node, keys):
            return _safe_num(_value(node, keys))

        # Naver 응답은 API 버전에 따라 키 이름/중첩 구조가 조금씩 달라진다.
        # "리포트 객체 자체"에 증권사/제목 + 날짜/의견/목표가 중 하나가 있으면 채택한다.
        def _collect_report_dicts(payload):
            broker_keys = {
                "writecorpname","writecorp","brokercode","brokername","brokeragename","brokerage",
                "firmname","firm_name","publishername","publisher","securitiesname","securities",
                "writername","writer","corpname","corp_name","companyname","company_name","broker"
            }
            opinion_keys = {
                "investmentopinion","investmentopinionname","investment_opinion","opinion",
                "recommendation","rating","investopinion","opinionname","opinion_name"
            }
            target_keys = {
                "targetprice","targetpricevalue","target_price","pricetarget","price_target",
                "targetmeanprice","targetmean","target_price_mean","targetprice_mean",
                "averagetargetprice","average_target_price","avgtargetprice","avg_target_price",
                "target"
            }
            date_keys = {
                "writedate","researchdate","publishdate","publishedat","published_at",
                "reportdate","regdate","createdat","created_at","createdate","date"
            }
            title_keys = {
                "title","reporttitle","report_title","researchtitle","research_title","subject","name"
            }
            id_keys = {"researchid","research_id","researchno","researchnumber","reportid","report_id","id"}

            out, seen = [], set()

            for node in _walk_objects(payload):
                if not isinstance(node, dict):
                    continue
                keys = {str(k).lower() for k in node.keys()}
                broker_hit = bool(keys & broker_keys)
                content_hit = bool(keys & (opinion_keys | target_keys | date_keys | title_keys | id_keys))
                # 리포트 객체의 직접 필드가 충분한 경우만 채택한다.
                if not (broker_hit and content_hit):
                    continue

                marker = json.dumps(node, ensure_ascii=False, sort_keys=True, default=str)
                if marker not in seen:
                    seen.add(marker)
                    out.append(node)

            return out

        def _parse_report(node):
            if not isinstance(node, dict):
                return None

            broker = _value(node, [
                "writeCorpName","writeCorp","brokerName","brokerageName","brokerage",
                "companyName","company_name","firmName","firm_name","publisherName","publisher",
                "broker","securitiesName","securities","writerName","writer","corpName","corp_name"
            ])
            title = _value(node, [
                "title","reportTitle","report_title","subject","researchTitle","research_title","name"
            ])
            opinion = _value(node, [
                "investmentOpinion","investmentOpinionName","investment_opinion",
                "opinion","recommendation","rating","investOpinion","opinionName","opinion_name"
            ])
            date_val = _value(node, [
                "writeDate","researchDate","publishedAt","published_at","publishDate",
                "reportDate","date","regDate","createdAt","created_at","createDate"
            ])
            tp = _number(node, [
                "targetPrice","targetPriceValue","target","priceTarget","price_target",
                "targetMean","targetMeanPrice","target_price_mean","targetprice_mean",
                "averageTargetPrice","average_target_price","avgTargetPrice","avg_target_price"
            ])
            rid = _value(node, [
                "researchId","research_id","researchNo","researchNumber","reportId","report_id","id"
            ])

            # 제목/증권사/의견/날짜/목표가 중 실제 내용이 하나도 없으면 제외
            if not any([broker, title, opinion, date_val, tp, rid]):
                return None

            date_txt = str(date_val)[:10] if date_val else ""
            sub = []
            if opinion:
                sub.append(str(opinion))
            if date_txt:
                sub.append(date_txt)
            if not sub and title:
                sub.append(str(title)[:100])

            current = _safe_num(raw_price)
            upside = ((tp-current)/current*100) if tp and current else None

            return {
                "main": str(broker or "증권사 리포트"),
                "sub": " · ".join(sub) or "실제 리포트",
                "val": f"{int(tp):,}원" if tp else "목표가 미확인",
                "sub_val": f"{upside:+.1f}%" if upside is not None else "",
                "_id": str(rid or ""),
                "_title": str(title or "")
            }

        def _parse_naver_pc_research_html(html_text):
            """Naver PC 리서치 종목분석 HTML fallback.
            API가 200이지만 JSON 구조가 바뀌었을 때 실제 공개 리포트 목록을 복구한다.
            """
            try:
                from html.parser import HTMLParser
                from html import unescape
                import re as _re

                class _RowParser(HTMLParser):
                    def __init__(self):
                        super().__init__(convert_charrefs=True)
                        self.rows = []
                        self.in_tr = False
                        self.in_td = False
                        self.in_a = False
                        self.current = []
                        self.cell = []
                        self.href = ""

                    def handle_starttag(self, tag, attrs):
                        attrs = dict(attrs)
                        if tag == "tr":
                            self.in_tr = True
                            self.current = []
                        elif self.in_tr and tag in ("td", "th"):
                            self.in_td = True
                            self.cell = []
                        elif self.in_tr and self.in_td and tag == "a":
                            self.in_a = True
                            self.href = attrs.get("href", "")

                    def handle_data(self, data):
                        if self.in_tr and self.in_td:
                            self.cell.append(data)

                    def handle_endtag(self, tag):
                        if tag in ("td", "th") and self.in_td:
                            txt = " ".join("".join(self.cell).split())
                            self.current.append(txt)
                            self.cell = []
                            self.in_td = False
                        elif tag == "a" and self.in_a:
                            self.in_a = False
                        elif tag == "tr" and self.in_tr:
                            if self.current:
                                self.rows.append(self.current)
                            self.current = []
                            self.in_tr = False

                parser = _RowParser()
                parser.feed(html_text)

                results = []
                date_re = _re.compile(r"20\d{2}[./-]\d{1,2}[./-]\d{1,2}")
                price_re = _re.compile(r"(?<!\d)\d{2,3}(?:,\d{3})+(?!\d)|(?<!\d)\d{5,8}(?!\d)")

                for cells in parser.rows:
                    cells = [unescape(c).strip() for c in cells if c.strip()]
                    if len(cells) < 3:
                        continue

                    joined = " ".join(cells)
                    if formal_name and formal_name not in joined and symbol not in joined:
                        continue
                    if "작성일" in joined or "종목명" in joined:
                        continue

                    date_txt = ""
                    for c in cells:
                        md = date_re.search(c)
                        if md:
                            date_txt = md.group(0)
                            break

                    # 실제 리포트 행은 보통 제목/증권사/의견/목표가/작성일 중 여러 칸을 가진다.
                    price = None
                    for c in cells:
                        # 날짜가 포함된 셀은 가격 후보에서 제외
                        if date_re.search(c):
                            continue
                        nums = price_re.findall(c.replace("원", ""))
                        for raw in nums:
                            n = _safe_num(raw)
                            if n and n >= 1000:
                                price = n
                                break
                        if price:
                            break

                    opinion = ""
                    for c in cells:
                        if any(x in c.lower() for x in (
                            "buy","hold","sell","strong buy","strong sell",
                            "매수","중립","매도","강력매수","강력 매수"
                        )):
                            opinion = c
                            break

                    # 네이버 종목분석 목록의 일반적인 순서를 활용하되,
                    # 고정 인덱스에 의존하지 않고 텍스트 특성으로 선택한다.
                    broker = ""
                    for c in cells:
                        if any(x in c for x in ("증권", "투자", "자산운용", "리서치")):
                            broker = c
                            break
                    if not broker and len(cells) >= 4:
                        broker = cells[-2]

                    title = ""
                    for c in cells:
                        if c != broker and c != date_txt and c != opinion and c:
                            if len(c) >= 5 and not price_re.fullmatch(c.replace("원","").strip()):
                                title = c
                                break

                    if not (broker or title or price or opinion or date_txt):
                        continue

                    results.append({
                        "main": broker or "증권사 리포트",
                        "sub": " · ".join(x for x in (opinion, date_txt) if x) or (title[:100] if title else "실제 리포트"),
                        "val": f"{int(price):,}원" if price else "목표가 미확인",
                        "sub_val": "",
                        "_id": "",
                        "_title": title
                    })

                return results[:20]
            except Exception as exc:
                print(f"[CONSENSUS][KR] PC research HTML parser failed: {type(exc).__name__}: {exc}")
                return []

        def _parse_wisereport_consensus_html(html_text):
            """WiseReport의 '제공처별 투자의견 및 목표주가' 표를 열 기준으로 정확히 읽는다.

            핵심: HTML 전체에서 숫자를 추측하지 않고, 실제 컨센서스 표의 헤더를
            먼저 찾은 뒤 같은 열의 값을 사용한다.
            제공처 | 최종일자 | 목표가 | 직전목표가 | 변동률(%) | 투자의견 | 직전투자의견
            """
            try:
                from html.parser import HTMLParser
                from html import unescape
                import re as _re

                class _TableParser(HTMLParser):
                    def __init__(self):
                        super().__init__(convert_charrefs=True)
                        self.tables = []
                        self.table_depth = 0
                        self.current_table = None
                        self.in_tr = False
                        self.in_cell = False
                        self.current_row = []
                        self.cell = []

                    def handle_starttag(self, tag, attrs):
                        if tag == "table":
                            if self.table_depth == 0:
                                self.current_table = []
                            self.table_depth += 1
                        elif self.table_depth == 1 and tag == "tr":
                            self.in_tr = True
                            self.current_row = []
                        elif self.table_depth == 1 and self.in_tr and tag in ("td", "th"):
                            self.in_cell = True
                            self.cell = []

                    def handle_data(self, data):
                        if self.table_depth == 1 and self.in_tr and self.in_cell:
                            self.cell.append(data)

                    def handle_endtag(self, tag):
                        if self.table_depth == 1 and tag in ("td", "th") and self.in_cell:
                            txt = " ".join("".join(self.cell).split())
                            self.current_row.append(txt)
                            self.cell = []
                            self.in_cell = False
                        elif self.table_depth == 1 and tag == "tr" and self.in_tr:
                            if self.current_row:
                                self.current_table.append(self.current_row)
                            self.current_row = []
                            self.in_tr = False
                        elif tag == "table" and self.table_depth > 0:
                            self.table_depth -= 1
                            if self.table_depth == 0:
                                if self.current_table:
                                    self.tables.append(self.current_table)
                                self.current_table = None

                parser = _TableParser()
                parser.feed(html_text or "")

                def clean(v):
                    return unescape(" ".join(str(v).split())).strip()

                def num(v):
                    try:
                        x = str(v).replace(",", "").replace("원", "").strip()
                        n = float(x)
                        return n if n > 0 else None
                    except Exception:
                        return None

                date_re = _re.compile(r"^(?:20\d{2}[./-]\d{1,2}[./-]\d{1,2}|\d{2}[./-]\d{1,2}[./-]\d{1,2})$")
                price_re = _re.compile(r"^\d{2,3}(?:,\d{3})+$|^\d{5,9}$")
                percent_re = _re.compile(r"^[+-]?\d+(?:\.\d+)?%?$")

                # 평균 목표가는 별도의 컨센서스 영역에서만 읽는다.
                plain = _re.sub(r"<[^>]+>", " ", html_text or "")
                plain = unescape(" ".join(plain.split()))
                target_p = None
                for pat in (
                    r"목표주가\s*\(원\).*?(\d{1,3}(?:,\d{3})+)",
                    r"목표주가.*?(\d{1,3}(?:,\d{3})+).*?EPS",
                ):
                    m = _re.search(pat, plain, flags=_re.I | _re.S)
                    if m:
                        target_p = num(m.group(1))
                        if target_p:
                            break

                # 반드시 '제공처별 투자의견 및 목표주가' 헤더가 있는 표만 선택한다.
                target_table = None
                header = None
                for table in parser.tables:
                    for row in table:
                        cells = [clean(x) for x in row]
                        joined = " | ".join(cells)
                        if (
                            "제공처" in joined and
                            "최종일자" in joined and
                            "목표가" in joined and
                            "투자의견" in joined
                        ):
                            target_table = table
                            header = cells
                            break
                    if target_table is not None:
                        break

                reports = []
                if target_table is None:
                    print(f"[CONSENSUS][KR] WiseReport target table NOT FOUND tables={len(parser.tables)}")
                    return target_p, []

                # 헤더의 실제 열 위치를 사용한다. 고정 인덱스가 아니라 헤더명으로 매핑한다.
                def col_index(names):
                    for i, h in enumerate(header):
                        h2 = h.replace(" ", "")
                        if any(name.replace(" ", "") in h2 for name in names):
                            return i
                    return -1

                broker_i = col_index(["제공처"])
                date_i = col_index(["최종일자"])
                target_i = col_index(["목표가"])
                opinion_i = col_index(["투자의견"])

                if min(broker_i, date_i, target_i, opinion_i) < 0:
                    print(f"[CONSENSUS][KR] WiseReport header mapping failed header={header}")
                    return target_p, []

                # 헤더 행 다음의 실제 데이터 행만 처리한다.
                header_pos = -1
                for i, row in enumerate(target_table):
                    if [clean(x) for x in row] == header:
                        header_pos = i
                        break
                if header_pos < 0:
                    # 같은 헤더를 포함하는 첫 행을 다시 찾는다.
                    for i, row in enumerate(target_table):
                        c = [clean(x) for x in row]
                        if "제공처" in " | ".join(c) and "목표가" in " | ".join(c):
                            header_pos = i
                            break

                seen = set()
                current = _safe_num(raw_price)
                for raw_row in target_table[header_pos + 1:]:
                    cells = [clean(x) for x in raw_row]
                    if len(cells) <= max(broker_i, date_i, target_i, opinion_i):
                        continue

                    broker = cells[broker_i]
                    date_txt = cells[date_i]
                    target_txt = cells[target_i]
                    opinion = cells[opinion_i]

                    # 실제 데이터 행만 통과시킨다.
                    if not broker or not date_re.fullmatch(date_txt):
                        continue
                    if not target_txt or percent_re.fullmatch(target_txt):
                        continue

                    # 목표가 열에서만 숫자를 읽는다. 직전 목표가/변동률은 절대 사용하지 않는다.
                    m_price = _re.search(r"\d{1,3}(?:,\d{3})+|\d{5,9}", target_txt.replace("원", ""))
                    if not m_price:
                        continue
                    target = num(m_price.group(0))
                    if target is None or target < 1000:
                        continue

                    key = (broker, date_txt, int(target), opinion)
                    if key in seen:
                        continue
                    seen.add(key)

                    upside = ((target - current) / current * 100.0) if target and current else None
                    reports.append({
                        "main": broker,
                        "sub": " · ".join(x for x in (opinion, date_txt) if x) or "실제 리포트",
                        "val": f"{int(target):,}원",
                        "sub_val": f"{upside:+.1f}%" if upside is not None else "",
                        "_id": f"wise:{broker}:{date_txt}:{int(target)}:{opinion}",
                        "_title": ""
                    })

                print(f"[CONSENSUS][KR] WiseReport tables={len(parser.tables)} target_table_rows={len(target_table)} header={header} reports={len(reports)}")
                return target_p, reports[:20]

            except Exception as exc:
                print(f"[CONSENSUS][KR] WiseReport parser failed: {type(exc).__name__}: {exc}")
                return None, []

        # 국내주식
        if market_name in ["국내주식", "국내증시"]:
            code = str(symbol).replace("A", "").zfill(6)
            current = _safe_num(raw_price)
            target_p = None
            target_source = None
            reports = []
            report_source = None

            # 0) 국내 컨센서스의 기준 경로: Naver 기업정보(WiseReport)
            # 실제 제공처/최종일자/목표가/투자의견 표를 직접 읽어 매핑한다.
            try:
                wise_url = f"https://navercomp.wisereport.co.kr/v2/company/c1010001.aspx?cmp_cd={code}"
                wr = HTTP_SESSION.get(
                    wise_url, timeout=8.0,
                    headers={
                        "User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/151 Safari/537.36",
                        "Accept":"text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                        "Accept-Language":"ko-KR,ko;q=0.9,en-US;q=0.8",
                        "Referer":"https://finance.naver.com/"
                    }
                )
                print(f"[CONSENSUS][KR] WiseReport {code} HTTP {wr.status_code}")
                if wr.ok:
                    wise_target, wise_reports = _parse_wisereport_consensus_html(wr.text)
                    if wise_target:
                        target_p = wise_target
                        target_source = "Naver WiseReport 실제 컨센서스"
                    if wise_reports:
                        reports = wise_reports
                        report_source = "Naver WiseReport 실제 증권사 리포트"
                    print(f"[CONSENSUS][KR] WiseReport {code} target={wise_target} reports={len(wise_reports)}")
                else:
                    print(f"[CONSENSUS][KR] WiseReport {code} HTTP {wr.status_code}")
            except Exception as e:
                print(f"[CONSENSUS][KR] WiseReport failed {code}: {type(e).__name__}: {e}")

            # 1) 기존 mobile integration
            try:
                url = f"https://m.stock.naver.com/api/stock/{code}/integration"
                res = HTTP_SESSION.get(
                    url, timeout=5.0,
                    headers={
                        "User-Agent":"Mozilla/5.0",
                        "Accept":"application/json, text/plain, */*",
                        "Accept-Language":"ko-KR,ko;q=0.9",
                        "Referer":f"https://m.stock.naver.com/domestic/stock/{code}/total"
                    }
                )
                print(f"[CONSENSUS][KR] integration {code} HTTP {res.status_code}")
                if res.ok:
                    data = res.json()
                    ci = data.get("consensusInfo") if isinstance(data, dict) else None
                    for obj in (ci, data):
                        if target_p is None and obj is not None:
                            target_p = _number(obj, [
                                "priceTargetMean","consensusTargetPrice","targetPriceMean",
                                "averageTargetPrice","avgTargetPrice","targetPrice"
                            ])
                            if target_p:
                                target_source = "Naver 실제 컨센서스"

                    # integration.researches가 직접 존재하는 현재 구조를 우선 처리
                    direct_researches = []
                    if isinstance(data, dict):
                        for k in ("researches", "research", "researchList", "researchesList"):
                            v = data.get(k)
                            if isinstance(v, list):
                                direct_researches.extend(v)
                            elif isinstance(v, dict):
                                direct_researches.extend(list(_walk_objects(v)))

                    # WiseReport가 실제 제공처/목표가를 매핑했다면
                    # Naver integration의 불완전한 중첩 객체를 섞지 않는다.
                    if not reports:
                        candidates = direct_researches or _collect_report_dicts(data)
                        for it in candidates:
                            row = _parse_report(it)
                            if row:
                                reports.append(row)

                        if reports:
                            report_source = "Naver 통합 리서치"
                    print(f"[CONSENSUS][KR] integration {code} target={target_p} reports={len(reports)}")
            except Exception as e:
                print(f"[CONSENSUS][KR] integration failed {code}: {type(e).__name__}: {e}")

            # 2) 현재 관찰되는 Naver front-api integration
            if not reports:
                try:
                    url = f"https://m.stock.naver.com/front-api/stock/domestic/integration?code={code}&endType=stock"
                    res = HTTP_SESSION.get(
                        url, timeout=5.0,
                        headers={
                            "User-Agent":"Mozilla/5.0",
                            "Accept":"application/json, text/plain, */*",
                            "Accept-Language":"ko-KR,ko;q=0.9",
                            "Referer":f"https://m.stock.naver.com/domestic/stock/{code}/total"
                        }
                    )
                    print(f"[CONSENSUS][KR] front-api integration {code} HTTP {res.status_code}")
                    if res.ok:
                        data = res.json()
                        for obj in (
                            data.get("consensusInfo") if isinstance(data, dict) else None,
                            data
                        ):
                            if target_p is None and obj is not None:
                                target_p = _number(obj, [
                                    "priceTargetMean","consensusTargetPrice","targetPriceMean",
                                    "averageTargetPrice","avgTargetPrice","targetPrice"
                                ])
                                if target_p:
                                    target_source = "Naver 실제 컨센서스"

                        direct = []
                        if isinstance(data, dict):
                            for k in ("researches","research","researchList","researchesList"):
                                v = data.get(k)
                                if isinstance(v, list):
                                    direct.extend(v)
                                elif isinstance(v, dict):
                                    direct.extend(list(_walk_objects(v)))

                        for it in direct or _collect_report_dicts(data):
                            row = _parse_report(it)
                            if row:
                                reports.append(row)

                        if reports:
                            report_source = "Naver front-api 리서치"
                        print(f"[CONSENSUS][KR] front-api integration {code} target={target_p} reports={len(reports)}")
                except Exception as e:
                    print(f"[CONSENSUS][KR] front-api integration failed {code}: {type(e).__name__}: {e}")

            # 3) 최신 v2 company / by-items
            v2_urls = [
                f"https://m.stock.naver.com/api/stockSecurity/researches/v2/company?itemCodes={urllib.parse.quote(code)}&index=0&size=20",
                f"https://m.stock.naver.com/api/stockSecurity/researches/v2/company/by-items?itemCodes={urllib.parse.quote(code)}&size=20",
            ]
            for v2_url in v2_urls:
                if reports:
                    break
                try:
                    rr = HTTP_SESSION.get(
                        v2_url, timeout=5.0,
                        headers={
                            "User-Agent":"Mozilla/5.0",
                            "Accept":"application/json, text/plain, */*",
                            "Accept-Language":"ko-KR,ko;q=0.9",
                            "Referer":f"https://m.stock.naver.com/domestic/stock/{code}/research"
                        }
                    )
                    print(f"[CONSENSUS][KR] research-v2 {code} HTTP {rr.status_code} path={'by-items' if 'by-items' in v2_url else 'company'}")
                    if not rr.ok:
                        continue
                    payload = rr.json()
                    parsed = []
                    for it in _collect_report_dicts(payload):
                        row = _parse_report(it)
                        if row:
                            parsed.append(row)
                    if parsed:
                        reports = parsed[:20]
                        report_source = "Naver 최신 리서치 v2"
                    print(f"[CONSENSUS][KR] research-v2 {code} reports={len(parsed)}")
                except Exception as e:
                    print(f"[CONSENSUS][KR] research-v2 failed {code}: {type(e).__name__}: {e}")

            # 4) v1 호환
            if not reports:
                for v1_url in [
                    f"https://m.stock.naver.com/api/stockSecurity/researches/v1/company/by-items?itemCodes={urllib.parse.quote(code)}&size=20",
                    f"https://m.stock.naver.com/api/stockSecurity/researches/v1/company?index=0&size=20",
                ]:
                    try:
                        rr = HTTP_SESSION.get(
                            v1_url, timeout=5.0,
                            headers={"User-Agent":"Mozilla/5.0","Accept":"application/json, text/plain, */*"}
                        )
                        print(f"[CONSENSUS][KR] research-v1 {code} HTTP {rr.status_code}")
                        if not rr.ok:
                            continue
                        parsed = []
                        for it in _collect_report_dicts(rr.json()):
                            row = _parse_report(it)
                            if row:
                                parsed.append(row)
                        if parsed:
                            reports = parsed[:20]
                            report_source = "Naver 리서치 v1 호환"
                            break
                    except Exception as e:
                        print(f"[CONSENSUS][KR] research-v1 failed {code}: {type(e).__name__}: {e}")

            # 5) legacy JSON
            if not reports:
                try:
                    legacy = f"https://m.stock.naver.com/api/stock/{code}/research?page=1&pageSize=20"
                    rr = HTTP_SESSION.get(
                        legacy, timeout=4.0,
                        headers={"User-Agent":"Mozilla/5.0","Accept":"application/json, text/plain, */*"}
                    )
                    print(f"[CONSENSUS][KR] legacy research {code} HTTP {rr.status_code}")
                    if rr.ok:
                        parsed = []
                        for it in _collect_report_dicts(rr.json()):
                            row = _parse_report(it)
                            if row:
                                parsed.append(row)
                        if parsed:
                            reports = parsed[:20]
                            report_source = "Naver 리서치 호환 경로"
                except Exception as e:
                    print(f"[CONSENSUS][KR] legacy research failed {code}: {type(e).__name__}: {e}")

            # 6) 최후 fallback: 공개 PC 종목분석 리서치 목록 HTML
            # 검색 URL은 실제 Naver research/company_list.naver의 itemCode 필터 형식을 사용한다.
            if not reports:
                try:
                    params = {
                        "keyword": "",
                        "brokerCode": "",
                        "writeFromDate": "",
                        "writeToDate": "",
                        "searchType": "itemCode",
                        "itemName": formal_name or "",
                        "itemCode": code,
                        "x": "55",
                        "y": "24",
                    }
                    url = "https://finance.naver.com/research/company_list.naver"
                    rr = HTTP_SESSION.get(
                        url, params=params, timeout=7.0,
                        headers={
                            "User-Agent":"Mozilla/5.0",
                            "Accept":"text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                            "Accept-Language":"ko-KR,ko;q=0.9",
                            "Referer":"https://finance.naver.com/research/",
                        }
                    )
                    print(f"[CONSENSUS][KR] PC research HTML {code} HTTP {rr.status_code}")
                    if rr.ok:
                        parsed = _parse_naver_pc_research_html(rr.text)
                        if parsed:
                            reports = parsed[:20]
                            report_source = "Naver 종목분석 리포트"
                        print(f"[CONSENSUS][KR] PC research HTML {code} reports={len(parsed)}")
                except Exception as e:
                    print(f"[CONSENSUS][KR] PC research HTML failed {code}: {type(e).__name__}: {e}")

            # 중복 제거
            unique = []
            seen = set()
            for row in reports:
                key = (
                    row.get("_id") or "",
                    row.get("main"),
                    row.get("sub"),
                    row.get("val"),
                    row.get("_title")
                )
                if key in seen:
                    continue
                seen.add(key)
                row.pop("_id", None)
                row.pop("_title", None)
                unique.append(row)
            reports = unique[:20]

            # 평균 목표가는 실제 Naver/WiseReport 또는 기존 Naver 컨센서스만 사용
            if target_p and current:
                low_p, high_p = min(current, target_p), max(current, target_p)
                if high_p <= low_p:
                    high_p = low_p + 1
                fill_pct = min(92, max(8, int(((target_p-low_p)/(high_p-low_p))*84+8)))
                upside = (target_p-current)/current*100
                primary_label = "평균 목표가"
                primary_val = f"{int(target_p):,} 원"
                slider_mid = f"목표가 {int(target_p):,}원 ({upside:+.1f}%)"
            else:
                low_p = high_p = current or 0
                fill_pct = 50
                primary_label = "평균 목표가"
                primary_val = "실제 목표가 데이터 없음"
                slider_mid = "실제 목표가 확인 필요"

            if reports:
                source_tag = f"출처: {report_source or 'Naver / 실제 증권사 리포트'}"
            elif target_p:
                source_tag = f"출처: {target_source or 'Naver / 실제 컨센서스'}"
            else:
                source_tag = "출처: Naver / 확인 가능한 컨센서스 데이터 없음"

            print(
                f"[CONSENSUS][KR] FINAL {code} "
                f"target={target_p} reports={len(reports)} "
                f"report_source={report_source or '-'}"
            )

            return {
                "title":"애널리스트 컨센서스",
                "gauge_val":str(len(reports)),
                "gauge_label":"확인 리포트",
                "status_label":"검증 데이터",
                "badge_text":"실제 데이터만 표시",
                "dots":f"확인 리포트 {len(reports)}건" if reports else "",
                "primary_label":primary_label,
                "primary_val":primary_val,
                "secondary_label":"현재가",
                "secondary_val":f"{int(current):,} 원" if current else "-",
                "slider_left":f"{int(low_p):,}원" if low_p else "-",
                "slider_mid":slider_mid,
                "slider_right":f"{int(high_p):,}원" if high_p else "-",
                "fill_pct":fill_pct,
                "list_title":"실제 증권사 리포트",
                "source_tag":source_tag,
                "items":reports
            }

        # 비트코인: 주식의 Buy/Hold/Sell 컨센서스가 아니라 실제 기관별 BTC 가격 전망을 사용한다.
        if market_name == "암호화폐" and str(symbol or "").upper().startswith("BTC"):
            current = _safe_float(raw_price)

            # 2026년 공개 리서치에서 확인된 기관 전망 스냅샷.
            # 서로 다른 목표 시점/방법론을 가진 전망이므로 '공식 컨센서스'라고 부르지 않는다.
            # 평균값은 명시된 Base/Target 전망만 계산하고, 시나리오 자료는 평균에서 제외한다.
            btc_forecasts = [
                {"firm":"Citi", "target":82000, "horizon":"12개월 · 2027년 중반", "stance":"Bear/Base 범위 중 Base", "date":"2026-07", "include_avg":True},
                {"firm":"Standard Chartered", "target":100000, "horizon":"2026년 말", "stance":"Bullish", "date":"2026-07", "include_avg":True},
                {"firm":"Bernstein", "target":150000, "horizon":"2026년 말", "stance":"Bullish", "date":"2026-07", "include_avg":True},
                {"firm":"JPMorgan", "target":160000, "horizon":"2026년", "stance":"Bullish · $150K~$170K 범위 중간값", "date":"2026-07", "include_avg":True},
                {"firm":"Tiger Research", "target":143000, "horizon":"12개월", "stance":"Bullish", "date":"2026-04", "include_avg":True},
                {"firm":"NYDIG", "target":38500, "horizon":"2026년 10월경", "stance":"Bearish 시나리오", "date":"2026-07", "include_avg":False},
            ]

            valid = [x for x in btc_forecasts if x.get("include_avg") and x.get("target")]
            avg_target = (sum(x["target"] for x in valid) / len(valid)) if valid else None
            targets = [x["target"] for x in valid]
            low_target = min(targets) if targets else None
            high_target = max(targets) if targets else None

            items = []
            for item in btc_forecasts:
                tp = item["target"]
                upside = ((tp-current)/current*100) if current else None
                items.append({
                    "main": item["firm"],
                    "sub": f"{item['horizon']} · {item['date']} · {item['stance']}",
                    "val": f"${tp:,.0f}",
                    "sub_val": f"{upside:+.1f}%" if upside is not None else ""
                })

            # 평균 목표가 대비 현재가의 상승/하락 여력.
            avg_upside = ((avg_target-current)/current*100) if avg_target and current else None
            if avg_target and current:
                primary_val = f"${avg_target:,.0f}"
                fill_pct = max(8, min(92, int(((current-low_target) / max(1, high_target-low_target))*100))) if high_target and low_target and high_target > low_target else 50
                slider_mid = f"평균 ${avg_target:,.0f}"
            else:
                primary_val = "기관 전망 확인 필요"
                fill_pct = 50
                slider_mid = "기관 전망 확인 필요"

            return {
                "title":"기관 BTC 전망",
                "gauge_val":str(len(btc_forecasts)),
                "gauge_label":"확인 전망",
                "status_label":"검증 데이터",
                "badge_text":"실제 기관 전망만 표시",
                "dots":f"확인 전망 {len(btc_forecasts)}건",
                "primary_label":"기관 평균 전망",
                "primary_val":primary_val,
                "secondary_label":"현재가",
                "secondary_val":f"${current:,.2f}" if current else "-",
                "slider_left":f"${low_target:,.0f}" if low_target else "-",
                "slider_mid":slider_mid,
                "slider_right":f"${high_target:,.0f}" if high_target else "-",
                "fill_pct":fill_pct,
                "list_title":"실제 기관 전망",
                "source_tag":"출처: Citi · Standard Chartered · Bernstein · JPMorgan · Tiger Research · NYDIG / 2026 공개 전망",
                "items":items
            }

        # 이더리움: 기관별 실제 가격 전망이 충분히 확인되는 경우에만 BTC와 같은 기관 전망 카드로 표시한다.
        # SOL/XRP 및 기타 코인은 현재 이 분기에 포함하지 않아 기존 동작(컨센서스 미표시)을 유지한다.
        if market_name == "암호화폐" and str(symbol or "").upper().startswith("ETH"):
            current = _safe_float(raw_price)

            eth_forecasts = [
                {"firm":"Citi", "target":2240, "horizon":"12개월", "stance":"Bear/Base 범위 중 Base", "date":"2026-07", "include_avg":True},
                {"firm":"Standard Chartered", "target":7500, "horizon":"2026년 말", "stance":"Bullish", "date":"2026-01", "include_avg":True},
                {"firm":"Fundstrat · Tom Lee", "target":12000, "horizon":"2026년 전망", "stance":"Bullish", "date":"2026-08", "include_avg":True},
            ]

            valid = [x for x in eth_forecasts if x.get("include_avg") and x.get("target")]
            avg_target = (sum(x["target"] for x in valid) / len(valid)) if valid else None
            targets = [x["target"] for x in valid]
            low_target = min(targets) if targets else None
            high_target = max(targets) if targets else None

            items = []
            for item in eth_forecasts:
                tp = item["target"]
                upside = ((tp-current)/current*100) if current else None
                items.append({
                    "main": item["firm"],
                    "sub": f"{item['horizon']} · {item['date']} · {item['stance']}",
                    "val": f"${tp:,.0f}",
                    "sub_val": f"{upside:+.1f}%" if upside is not None else ""
                })

            if avg_target and current:
                primary_val = f"${avg_target:,.0f}"
                avg_upside = (avg_target-current)/current*100
                fill_pct = max(8, min(92, int(((current-low_target) / max(1, high_target-low_target))*100))) if high_target and low_target and high_target > low_target else 50
                slider_mid = f"평균 ${avg_target:,.0f} ({avg_upside:+.1f}%)"
            else:
                primary_val = "기관 전망 확인 필요"
                fill_pct = 50
                slider_mid = "기관 전망 확인 필요"

            return {
                "title":"기관 ETH 전망",
                "gauge_val":str(len(eth_forecasts)),
                "gauge_label":"확인 전망",
                "status_label":"검증 데이터",
                "badge_text":"실제 기관 전망만 표시",
                "dots":f"확인 전망 {len(eth_forecasts)}건",
                "primary_label":"기관 평균 전망",
                "primary_val":primary_val,
                "secondary_label":"현재가",
                "secondary_val":f"${current:,.2f}" if current else "-",
                "slider_left":f"${low_target:,.0f}" if low_target else "-",
                "slider_mid":slider_mid,
                "slider_right":f"${high_target:,.0f}" if high_target else "-",
                "fill_pct":fill_pct,
                "list_title":"실제 기관 전망",
                "source_tag":"출처: Citi · Standard Chartered · Fundstrat / Tom Lee · 2026 공개 전망",
                "items":items
            }

        # 미국/글로벌
        if market_name in ["미국주식", "글로벌증시", "원자재"]:
            key = str(symbol or "").upper(); current = _safe_float(raw_price)
            counts = {"Buy":0,"Hold":0,"Sell":0}; rec_items=[]
            target_p=target_low=target_high=None; target_source=None; target_error=None

            if FINNHUB_API_KEY and key:
                try:
                    ck=f"recommendation:{key}"; cached=_cache_get(FINNHUB_CONSENSUS_CACHE,ck,FINNHUB_CONSENSUS_CACHE_TTL)
                    if cached is None:
                        rr=HTTP_SESSION.get("https://finnhub.io/api/v1/stock/recommendation",params={"symbol":key,"token":FINNHUB_API_KEY},timeout=5.0)
                        print(f"[CONSENSUS][US] recommendation {key} HTTP {rr.status_code}")
                        cached=(rr.json()[0] if rr.ok and isinstance(rr.json(),list) and rr.json() else None)
                        if isinstance(cached,dict): _cache_set(FINNHUB_CONSENSUS_CACHE,ck,cached)
                    if isinstance(cached,dict):
                        counts["Buy"]=int(cached.get("strongBuy") or 0)+int(cached.get("buy") or 0)
                        counts["Hold"]=int(cached.get("hold") or 0)
                        counts["Sell"]=int(cached.get("sell") or 0)+int(cached.get("strongSell") or 0)
                    for label in ("Buy","Hold","Sell"):
                        if counts[label]: rec_items.append({"main":label,"sub":"Finnhub 최근 추천의견 집계","val":str(counts[label]),"sub_val":"실제 집계"})
                except Exception as e: print(f"[CONSENSUS][US] recommendation failed {key}: {type(e).__name__}: {e}")

            if FINNHUB_API_KEY and key:
                try:
                    tr=HTTP_SESSION.get("https://finnhub.io/api/v1/stock/price-target",params={"symbol":key,"token":FINNHUB_API_KEY},timeout=5.0)
                    print(f"[CONSENSUS][US] Finnhub price-target {key} HTTP {tr.status_code}")
                    if tr.ok:
                        p=tr.json(); target_p=_safe_float(p.get("targetMean")); target_low=_safe_float(p.get("targetLow")); target_high=_safe_float(p.get("targetHigh")); target_source="Finnhub" if target_p else None
                    else: target_error=f"Finnhub HTTP {tr.status_code}"
                except Exception as e: target_error=f"Finnhub {type(e).__name__}: {e}"

            # Yahoo quoteSummary
            if target_p is None and key:
                for yurl in [f"https://query1.finance.yahoo.com/v10/finance/quoteSummary/{urllib.parse.quote(key)}?modules=financialData",f"https://query2.finance.yahoo.com/v10/finance/quoteSummary/{urllib.parse.quote(key)}?modules=financialData"]:
                    try:
                        yr=HTTP_SESSION.get(yurl,timeout=6.0,headers={"User-Agent":"Mozilla/5.0","Accept":"application/json,text/plain,*/*"})
                        print(f"[CONSENSUS][US] Yahoo target {key} HTTP {yr.status_code}")
                        if not yr.ok: continue
                        result=(yr.json().get("quoteSummary",{}).get("result",[]) or [])
                        if result:
                            fd=result[0].get("financialData",{}) or {}
                            def yv(k):
                                v=fd.get(k); return v.get("raw") if isinstance(v,dict) else v
                            target_p=_safe_float(yv("targetMeanPrice")); target_low=_safe_float(yv("targetLowPrice")); target_high=_safe_float(yv("targetHighPrice"))
                            if target_p: target_source="Yahoo Finance"; break
                    except Exception as e: print(f"[CONSENSUS][US] Yahoo target failed {key}: {type(e).__name__}: {e}")

            # StockAnalysis 공개 forecast fallback
            if target_p is None and key:
                try:
                    sr=HTTP_SESSION.get(f"https://stockanalysis.com/stocks/{urllib.parse.quote(key.lower())}/forecast/",timeout=8.0,headers={"User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/151 Safari/537.36","Accept-Language":"en-US,en;q=0.9"})
                    print(f"[CONSENSUS][US] StockAnalysis forecast {key} HTTP {sr.status_code}")
                    if sr.ok:
                        html=sr.text
                        for pat in [r"average price target[^$]{0,160}\$([0-9][0-9,]*(?:\.[0-9]+)?)",r"Price Target:\s*\$([0-9][0-9,]*(?:\.[0-9]+)?)",r'"targetMeanPrice"\s*:\s*([0-9.]+)']:
                            m=re.search(pat,html,flags=re.I|re.S)
                            if m:
                                target_p=_safe_float(m.group(1).replace(",",""))
                                if target_p: target_source="StockAnalysis / S&P Global"; break
                        if target_p:
                            ml=re.search(r'lowest[^$]{0,120}\$([0-9][0-9,]*(?:\.[0-9]+)?)',html,flags=re.I|re.S); mh=re.search(r'highest[^$]{0,120}\$([0-9][0-9,]*(?:\.[0-9]+)?)',html,flags=re.I|re.S)
                            if ml: target_low=_safe_float(ml.group(1).replace(",",""))
                            if mh: target_high=_safe_float(mh.group(1).replace(",",""))
                    else: target_error=target_error or f"StockAnalysis HTTP {sr.status_code}"
                except Exception as e: print(f"[CONSENSUS][US] StockAnalysis failed {key}: {type(e).__name__}: {e}")

            # Alpha Vantage OVERVIEW fallback
            if target_p is None and ALPHA_VANTAGE_API_KEY and key:
                try:
                    ar=HTTP_SESSION.get("https://www.alphavantage.co/query",params={"function":"OVERVIEW","symbol":key,"apikey":ALPHA_VANTAGE_API_KEY},timeout=8.0)
                    print(f"[CONSENSUS][US] Alpha Vantage OVERVIEW {key} HTTP {ar.status_code}")
                    if ar.ok:
                        p=ar.json(); candidate=_safe_float(p.get("AnalystTargetPrice")) if isinstance(p,dict) else None
                        if candidate: target_p=candidate; target_source="Alpha Vantage OVERVIEW"
                except Exception as e: print(f"[CONSENSUS][US] Alpha Vantage failed {key}: {type(e).__name__}: {e}")

            total=sum(counts.values())
            if target_p and current:
                upside=((target_p-current)/current)*100; primary_val=f"${target_p:,.2f}"; secondary_val=f"${current:,.2f}"
                if target_low and target_high and target_high>target_low:
                    low_p,high_p=target_low,target_high; fill_pct=min(92,max(8,int(((target_p-low_p)/(high_p-low_p))*100))); slider_left=f"${low_p:,.2f}"; slider_mid=f"평균 ${target_p:,.2f} ({upside:+.1f}%)"; slider_right=f"${high_p:,.2f}"
                else:
                    fill_pct=min(92,max(8,50+int(max(-40,min(40,upside))/2))); slider_left=f"현재가 ${current:,.2f}"; slider_mid=f"평균 ${target_p:,.2f} ({upside:+.1f}%)"; slider_right="목표범위 미확인"
            else:
                primary_val="실제 목표가 데이터 없음"; secondary_val=f"${current:,.2f}" if current else "-"; fill_pct=50; slider_left=f"${current:,.2f}" if current else "-"; slider_mid="실제 목표가 확인 필요"; slider_right="-"

            if target_p: source_tag=f"출처: {target_source} / 실제 목표주가" + (" + Finnhub 추천의견" if total else "")
            elif total: source_tag="출처: Finnhub / 추천의견 실제 데이터" + (f" · {target_error}" if target_error else "")
            else: source_tag="확인 가능한 컨센서스 데이터 없음"
            print(f"[CONSENSUS][US] FINAL {key} target={target_p} opinions={total} source={target_source or '-'}")
            return {"title":"애널리스트 컨센서스","gauge_val":str(total),"gauge_label":"확인 의견","status_label":"검증 데이터","badge_text":"실제 데이터만 표시","dots":f"Buy {counts['Buy']} · Hold {counts['Hold']} · Sell {counts['Sell']}","primary_label":"평균 목표가","primary_val":primary_val,"secondary_label":"현재가","secondary_val":secondary_val,"slider_left":slider_left,"slider_mid":slider_mid,"slider_right":slider_right,"fill_pct":fill_pct,"list_title":"실제 애널리스트 의견","source_tag":source_tag,"items":rec_items}

        return None
    except Exception as exc:
        print(f"[CONSENSUS] {symbol} failed: {type(exc).__name__}: {exc}")
        return None

def fetch_market_consensus_unified(symbol, market_name, raw_price, formal_name=""):
    """컨센서스 통합 조회의 캐시 래퍼.

    Step 1 속도 개선에서는 기존 Naver/Finnhub/Yahoo fallback 로직을
    제거하지 않고, 동일 종목의 반복 검색에 대한 외부 API 재호출을 막는다.

    - TTL: 15분
    - 캐시 키: 시장 + 종목코드
    - None도 캐시하여 일시적인 외부 API 장애가 반복 호출로 증폭되는 것을 방지
    """
    key = f"{str(market_name or '').strip()}:{str(symbol or '').strip().upper()}"
    now = time.time()

    with CONSENSUS_CACHE_LOCK:
        cached = CONSENSUS_RESULT_CACHE.get(key)
        if cached is not None:
            cached_time, cached_result = cached
            if now - cached_time < CONSENSUS_RESULT_CACHE_TTL:
                return dict(cached_result) if isinstance(cached_result, dict) else cached_result
            CONSENSUS_RESULT_CACHE.pop(key, None)

    result = _fetch_market_consensus_unified_uncached(
        symbol=symbol,
        market_name=market_name,
        raw_price=raw_price,
        formal_name=formal_name,
    )

    with CONSENSUS_CACHE_LOCK:
        CONSENSUS_RESULT_CACHE[key] = (now, result)

    return dict(result) if isinstance(result, dict) else result
