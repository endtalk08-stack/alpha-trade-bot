/* gaemiGTP Widget: Chart
 * 독립 위젯 계약: window.initTvChart(candles)
 * 데이터는 기존 /api/analyze 또는 /api/chart에서 전달받는다.
 */
(function () {
    'use strict';

    function initTvChart(candles) {
        const container = document.getElementById('tvChart');
        if (!container) return;

        if (window.gaemiChart && typeof window.gaemiChart.remove === 'function') {
            try { window.gaemiChart.remove(); } catch (_) {}
        }

        container.innerHTML = '';
        const isLight = document.body.classList.contains('light-theme');

        const nextChart = LightweightCharts.createChart(container, {
            width: container.clientWidth || 700,
            height: 300,
            layout: {
                background: { color: isLight ? '#ffffff' : '#1e1f20' },
                textColor: isLight ? '#5f6368' : '#8e918f'
            },
            grid: {
                vertLines: { color: 'rgba(0,0,0,0)' },
                horzLines: { color: 'rgba(0,0,0,0)' }
            },
            crosshair: {
                vertLine: { visible: false },
                horzLine: { visible: false }
            },
            timeScale: { borderColor: isLight ? '#dadce0' : '#282a2c' },
            rightPriceScale: { borderColor: isLight ? '#dadce0' : '#282a2c' }
        });

        const candleSeries = nextChart.addCandlestickSeries({
            upColor: '#ef4444',
            downColor: '#38bdf8',
            borderDownColor: '#38bdf8',
            borderUpColor: '#ef4444',
            wickDownColor: '#38bdf8',
            wickUpColor: '#ef4444',
            priceLineVisible: false,
            lastValueVisible: true
        });
        candleSeries.setData(Array.isArray(candles) ? candles : []);
        nextChart.timeScale().fitContent();
        window.gaemiChart = nextChart;
    }

    window.initTvChart = initTvChart;
    window.gaemiChartWidget = Object.freeze({ init: initTvChart });
})();
