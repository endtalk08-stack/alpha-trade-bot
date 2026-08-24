async def analyze(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.message.text
    status_msg = await update.message.reply_text(f"{query} 실시간 차트 렌더링 및 퀀트 연산 중입니다")

    try:
        current_price, symbol, formal_name, quant_data = fetch_realtime_price_fallback(query)

        if not current_price:
            await status_msg.edit_text(f"❌ {query}의 실시간 가격을 가져오지 못했습니다. 종목명을 다시 확인해 주세요.")
            return

        summary_text, detail_text = generate_full_quant_report(formal_name, symbol, current_price, quant_data)

        report_id = str(uuid.uuid4())[:8]
        REPORT_CACHE[report_id] = {
            "summary": summary_text,
            "detail": detail_text
        }

        keyboard = [[InlineKeyboardButton("심층 분석 근거 보기", callback_data=f"show_detail:{report_id}")]]
        reply_markup = InlineKeyboardMarkup(keyboard)

        # 1. 차트 이미지 생성
        chart_buffer = None
        if quant_data and quant_data.get('chart_df') is not None:
            chart_buffer = generate_candlestick_chart(quant_data['chart_df'], formal_name)

        # 2. 로딩 메시지 삭제 후 차트 + 타점 카드 분리 전송 (1,024자 제한 에러 방지)
        await status_msg.delete()

        if chart_buffer:
            await update.message.reply_photo(photo=chart_buffer)
        
        await update.message.reply_text(
            text=summary_text,
            reply_markup=reply_markup
        )

    except Exception as e:
        err_msg = str(e)
        if "429" in err_msg or "quota" in err_msg.lower():
            await update.message.reply_text("일일 API 할당량이 소진되었습니다 잠시 후 다시 시도해 주세요")
        else:
            await update.message.reply_text(f"분석 오류: {err_msg}")
