def build_kiwoom_deep_briefing(formal_name, price_str, current_change_pct, volume_str, market_index_name, world_board, domestic_flow, dart_disclosures):
    """키움 정량 데이터만으로 가장 길고 분석적인 종합 브리핑을 조립합니다."""
    paragraphs = []
    pct = float(current_change_pct) if current_change_pct is not None else 0.0

    # 1. 글로벌 매크로 & 섹터 환경 분석
    wb = world_board or []
    def extract_rate(target_name):
        for item in wb:
            if str(item.get("name", "")).upper() == target_name.upper():
                try:
                    return float(str(item.get("rate", "")).replace("%", "").replace("+", "").strip())
                except:
                    pass
        return None

    m_rate = extract_rate(market_index_name or "코스피") or extract_rate("QQQ")
    soxx_rate = extract_rate("SOXX")
    
    macro_parts = []
    if m_rate is not None:
        m_dir = "상승 흐름을 타며 우호적인 투자 심리를 형성하고 있고" if m_rate > 0 else ("조정을 받으며 시장 전반에 관망세가 짙은 가운데" if m_rate < 0 else "보합권에서 방향성을 탐색 중이며")
        macro_parts.append(f"현재 국내 시장({market_index_name or '지수'})은 {m_rate:+.2f}% {m_dir}")
    if soxx_rate is not None:
        s_dir = "동반 강세를 보이며 섹터 모멘텀을 지지하고 있습니다." if soxx_rate > 0 else "약세를 나타내며 업종 전반에 부담 요인으로 작용하고 있습니다."
        macro_parts.append(f"글로벌 반도체·기술주 벤치마크인 SOXX 지수가 {soxx_rate:+.2f}% {s_dir}")
    
    if macro_parts:
        paragraphs.append("🌐 [시장 및 업종 배경]\n" + " ".join(macro_parts))

    # 2. 당일 주가 탄력성 및 가격대 진단
    if pct >= 5.0:
        move_desc = f"오늘 {pct:+.2f}% 급등세를 기록하며 시장의 핵심 주도주로 거래대금이 집중되고 있습니다."
    elif pct > 0:
        move_desc = f"오늘 {pct:+.2f}% 상승 흐름을 이어가며 견조한 주가 방어력을 증명하고 있습니다."
    elif pct <= -5.0:
        move_desc = f"오늘 {pct:+.2f}% 급락세를 보이며 단기 투매 및 차익 매물이 집중 출회되고 있습니다."
    elif pct < 0:
        move_desc = f"오늘 {pct:+.2f}% 하락세를 나타내며 매수세 유입이 다소 지연되는 양상입니다."
    else:
        move_desc = "오늘 0.00% 보합권에서 매수와 매도 공방이 팽팽하게 맞서고 있습니다."

    p_info = f"현재가는 {price_str or '확인 중'}"
    v_info = f", 거래량은 {volume_str} 수준" if volume_str else ""
    paragraphs.append(f"📈 [가격 및 변동성]\n{formal_name}은(는) {p_info}{v_info}에서 {move_desc}")

    # 3. 메이저 수급 주체 (5거래일 외인/기관 분석)
    f_net = domestic_flow.get("foreigner_5d") if domestic_flow else None
    i_net = domestic_flow.get("institution_5d") if domestic_flow else None
    
    flow_desc = []
    if f_net is not None and f_net != 0:
        flow_desc.append(f"외국인이 누적 {abs(int(f_net)):,}주를 {'순매수' if f_net > 0 else '순매도'}")
    if i_net is not None and i_net != 0:
        flow_desc.append(f"기관이 누적 {abs(int(i_net)):,}주를 {'순매수' if i_net > 0 else '순매도'}")

    if flow_desc:
        if f_net and f_net > 0 and i_net and i_net > 0:
            conclusion = "외인과 기관의 강력한 '쌍끌이 매수세'가 주가 상승의 핵심 동력으로 작용 중입니다."
        elif f_net and f_net < 0 and i_net and i_net < 0:
            conclusion = "메이저 양대 주체의 동반 매도세가 출회되며 수급상 하방 압력이 가중되고 있습니다."
        elif f_net and f_net > 0:
            conclusion = "기관의 매도 물량을 외국인의 공격적인 매수세가 흡수하며 주가를 방어하고 있습니다."
        elif i_net and i_net > 0:
            conclusion = "외국인의 수급 이탈에도 불구하고 기관의 든든한 저가 매수세가 유입되는 구조입니다."
        else:
            conclusion = "뚜렷한 수급 주체의 쏠림 없이 개인 간의 손바뀜 중심 장세가 이어지고 있습니다."
        paragraphs.append(f"📊 [수급 분석 (최근 5일)]\n최근 5거래일 동안 {' 및 '.join(flow_desc)}했습니다. {conclusion}")
    else:
        paragraphs.append("📊 [수급 분석 (최근 5일)]\n현재 집계 기준 뚜렷한 메이저 수급 이탈이나 대량 쏠림 없이 단기 호가 중심의 공방이 전개 중입니다.")

    # 4. 공시 및 기업 재료 체크
    if dart_disclosures:
        pos_list = [d.get("title", "") for d in dart_disclosures if d.get("tag_type") == "positive"]
        neg_list = [d.get("title", "") for d in dart_disclosures if d.get("tag_type") == "negative"]
        
        if pos_list:
            paragraphs.append(f"📋 [핵심 공시 모멘텀]\n최근 기업 가치에 긍정적인 주요 공시('{pos_list[0]}')가 확인되어 재료 측면에서 매수 심리를 자극하고 있습니다. 세부 내역은 하단 DART 위젯을 참조하세요.")
        elif neg_list:
            paragraphs.append(f"⚠️ [주의 공시 체크]\n최근 주가 변동성을 야기할 수 있는 공시('{neg_list[0]}')가 보고되었으니 오버나잇 전 반드시 세부 위험 조건을 검토해야 합니다.")
        else:
            paragraphs.append(f"📋 [공시 동향]\n최근 접수된 공시: '{dart_disclosures[0].get('title', '일반 공시')}' 외 특이 변동 공시는 확인되지 않았습니다.")
    else:
        paragraphs.append("📋 [기업 재료]\n현재 주가를 뒤흔들 만한 돌발 악재나 단독 대형 공시는 확인되지 않으며, 시장 업황과 자체 펀더멘털에 수렴하는 흐름입니다.")

    # 5. 행동 전략 및 마무리
    paragraphs.append("💡 [개미 가이드]\n상단 요약 브리핑과 함께 하단의 퀀트 점수, 일봉 지지선, 기관 수급 추이를 종합 대조하여 진입 타점을 검증하세요. 오늘도 원칙 매매로 성투합시다! 🐜💪")

    return "\n\n".join(paragraphs)
