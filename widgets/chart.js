/* gaemiGTP Widget: Chart
 * 1차 교체: Apache ECharts
 * 표시 범위: 캔들 + 거래량만
 * 계약은 기존과 동일하게 window.initTvChart(candles)를 유지한다.
 * 종목 전환 시 차트 인스턴스를 삭제하지 않고 데이터만 교체한다.
 */
(function () {
    'use strict';

    let resizeObserver = null;

    function asNumber(value) {
        const n = Number(value);
        return Number.isFinite(n) ? n : null;
    }

    function normalizeCandles(candles) {
        if (!Array.isArray(candles)) return [];
        return candles.map(function (row) {
            const open = asNumber(row && row.open);
            const high = asNumber(row && row.high);
            const low = asNumber(row && row.low);
            const close = asNumber(row && row.close);
            const volume = asNumber(row && row.volume);
            if (!row || !row.time || [open, high, low, close].some(v => v === null)) return null;
            return {
                time: String(row.time),
                open: open,
                high: high,
                low: low,
                close: close,
                volume: volume === null ? 0 : Math.max(0, volume)
            };
        }).filter(Boolean);
    }

    function themeColors() {
        const isLight = document.body.classList.contains('light-theme');
        return {
            isLight: isLight,
            text: isLight ? '#5f6368' : '#8e918f',
            border: isLight ? '#dadce0' : '#303236',
            split: isLight ? '#eeeeee' : '#292b2e',
            tooltipBg: isLight ? '#ffffff' : '#1e1f20',
            tooltipBorder: isLight ? '#dadce0' : '#3a3d40',
            up: '#ef4444',
            down: '#38bdf8'
        };
    }

    function buildOption(rows) {
        const theme = themeColors();
        const dates = rows.map(r => r.time);
        const candleData = rows.map(r => [r.open, r.close, r.low, r.high]);
        const volumeData = rows.map(function (r) {
            return {
                value: r.volume,
                itemStyle: {
                    color: r.close >= r.open ? theme.up : theme.down,
                    opacity: 0.55
                }
            };
        });

        return {
            animation: false,
            useUTC: false,
            backgroundColor: 'transparent',
            grid: [
                { left: 8, right: 58, top: 10, height: '60%' },
                { left: 8, right: 58, top: '76%', height: '17%' }
            ],
            tooltip: {
                trigger: 'axis',
                axisPointer: { type: 'cross' },
                backgroundColor: theme.tooltipBg,
                borderColor: theme.tooltipBorder,
                textStyle: { color: theme.text },
                formatter: function (params) {
                    if (!Array.isArray(params) || !params.length) return '';
                    const idx = params[0].dataIndex;
                    const row = rows[idx];
                    if (!row) return '';
                    const volume = Number(row.volume || 0).toLocaleString('ko-KR');
                    return [
                        '<div style="font-weight:600;margin-bottom:4px;">' + row.time + '</div>',
                        '시가&nbsp;&nbsp;' + Number(row.open).toLocaleString('ko-KR'),
                        '<br>고가&nbsp;&nbsp;' + Number(row.high).toLocaleString('ko-KR'),
                        '<br>저가&nbsp;&nbsp;' + Number(row.low).toLocaleString('ko-KR'),
                        '<br>종가&nbsp;&nbsp;' + Number(row.close).toLocaleString('ko-KR'),
                        '<br>거래량&nbsp;' + volume
                    ].join('');
                }
            },
            xAxis: [
                {
                    type: 'category',
                    data: dates,
                    gridIndex: 0,
                    boundaryGap: true,
                    axisLine: { lineStyle: { color: theme.border } },
                    axisTick: { show: false },
                    axisLabel: { show: false },
                    splitLine: { show: false }
                },
                {
                    type: 'category',
                    data: dates,
                    gridIndex: 1,
                    boundaryGap: true,
                    axisLine: { lineStyle: { color: theme.border } },
                    axisTick: { show: false },
                    axisLabel: { color: theme.text, fontSize: 10, hideOverlap: true },
                    splitLine: { show: false }
                }
            ],
            yAxis: [
                {
                    type: 'value',
                    scale: true,
                    gridIndex: 0,
                    axisLabel: { color: theme.text, fontSize: 10 },
                    axisLine: { lineStyle: { color: theme.border } },
                    axisTick: { show: false },
                    splitLine: { lineStyle: { color: theme.split } }
                },
                {
                    type: 'value',
                    gridIndex: 1,
                    axisLabel: {
                        color: theme.text,
                        fontSize: 9,
                        formatter: function (value) {
                            const n = Number(value);
                            if (Math.abs(n) >= 100000000) return (n / 100000000).toFixed(1) + '억';
                            if (Math.abs(n) >= 10000) return (n / 10000).toFixed(0) + '만';
                            return n.toLocaleString('ko-KR');
                        }
                    },
                    axisLine: { lineStyle: { color: theme.border } },
                    axisTick: { show: false },
                    splitLine: { show: false }
                }
            ],
            series: [
                {
                    name: '주가',
                    type: 'candlestick',
                    xAxisIndex: 0,
                    yAxisIndex: 0,
                    data: candleData,
                    itemStyle: {
                        color: theme.up,
                        color0: theme.down,
                        borderColor: theme.up,
                        borderColor0: theme.down
                    },
                    barMaxWidth: 10
                },
                {
                    name: '거래량',
                    type: 'bar',
                    xAxisIndex: 1,
                    yAxisIndex: 1,
                    data: volumeData,
                    barMaxWidth: 10
                }
            ]
        };
    }

    function ensureChart(container) {
        if (!window.echarts) throw new Error('Apache ECharts가 로드되지 않았습니다.');
        if (window.gaemiChart && !window.gaemiChart.isDisposed()) return window.gaemiChart;

        const chart = window.echarts.init(container, null, {
            renderer: 'canvas',
            useDirtyRect: true
        });
        window.gaemiChart = chart;

        if (resizeObserver) {
            try { resizeObserver.disconnect(); } catch (_) {}
        }
        resizeObserver = new ResizeObserver(function () {
            try { chart.resize(); } catch (_) {}
        });
        resizeObserver.observe(container);

        return chart;
    }

    function initTvChart(candles) {
        const container = document.getElementById('tvChart');
        if (!container) return;

        const rows = normalizeCandles(candles);
        const chart = ensureChart(container);
        if (!rows.length) {
            chart.clear();
            return;
        }

        chart.setOption(buildOption(rows), {
            notMerge: false,
            lazyUpdate: false
        });
    }

    function updateTheme() {
        const chart = window.gaemiChart;
        if (!chart || chart.isDisposed()) return;
        // 현재 데이터는 유지한 채 축/툴팁 색상만 갱신하기 위해 기존 옵션을 읽어
        // candles를 다시 재구성하지 않고 테마성 옵션만 최소 변경한다.
        const theme = themeColors();
        chart.setOption({
            tooltip: {
                backgroundColor: theme.tooltipBg,
                borderColor: theme.tooltipBorder,
                textStyle: { color: theme.text }
            },
            xAxis: [
                { axisLine: { lineStyle: { color: theme.border } }, axisLabel: { show: false } },
                { axisLine: { lineStyle: { color: theme.border } }, axisLabel: { color: theme.text } }
            ],
            yAxis: [
                { axisLine: { lineStyle: { color: theme.border } }, axisLabel: { color: theme.text }, splitLine: { lineStyle: { color: theme.split } } },
                { axisLine: { lineStyle: { color: theme.border } }, axisLabel: { color: theme.text } }
            ]
        }, { lazyUpdate: true });
        chart.resize();
    }

    window.initTvChart = initTvChart;
    window.updateGaemiChartTheme = updateTheme;
    window.gaemiChartWidget = Object.freeze({ init: initTvChart, updateTheme: updateTheme });
})();
