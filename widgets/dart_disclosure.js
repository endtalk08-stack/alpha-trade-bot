(function () {
    window.GaemiWidgets = window.GaemiWidgets || {};

    function escapeHtml(value) {
        return String(value ?? '')
            .replace(/&/g, '&amp;')
            .replace(/</g, '&lt;')
            .replace(/>/g, '&gt;')
            .replace(/"/g, '&quot;')
            .replace(/'/g, '&#39;');
    }

    window.GaemiWidgets.renderDartDisclosure = function (items) {
        const card = document.getElementById('rowDart');
        const list = document.getElementById('dartList');
        if (!card || !list) return;

        if (!Array.isArray(items) || items.length === 0) {
            list.innerHTML = '';
            card.style.display = 'none';
            return;
        }

        list.innerHTML = items.map(function (d) {
            const link = escapeHtml(d && d.link ? d.link : '#');
            const tagType = escapeHtml(d && d.tag_type ? d.tag_type : 'neutral');
            const tagName = escapeHtml(d && d.tag_name ? d.tag_name : '공시');
            const title = escapeHtml(d && d.title ? d.title : '');
            const date = escapeHtml(d && d.date ? d.date : '');
            return '<a href="' + link + '" target="_blank" rel="noopener noreferrer" class="dart-item">' +
                '<span class="dart-tag tag-' + tagType + '">' + tagName + '</span>' +
                '<span class="dart-title">' + title + '</span>' +
                '<span style="color:var(--text-muted); font-size:0.72rem; flex-shrink:0;">' + date + '</span>' +
                '</a>';
        }).join('');

        card.style.display = 'block';
    };
})();
