# Shared HTTP/performance infrastructure extracted from the verified 1site baseline.
import time
import threading
import urllib.parse
import requests

# Shared persistent HTTP session (moved with the performance/network layer).
HTTP_SESSION = requests.Session()
HTTP_SESSION.headers.update({
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
    "Connection": "keep-alive",
})

_PERF_LOCAL = threading.local()
_PERF_REQUESTS = {}
_PERF_REQUESTS_LOCK = threading.Lock()
_PERF_HISTORY = []
_PERF_HISTORY_MAX = 100

def _perf_record_http(url, elapsed_ms):
    request_id = getattr(_PERF_LOCAL, "request_id", None)
    if not request_id:
        return
    try:
        parsed_url = urllib.parse.urlparse(str(url))
        host = parsed_url.netloc or parsed_url.path
        path = parsed_url.path or "/"
        call = {"host": host, "path": path[:160], "ms": round(elapsed_ms, 1)}
        with _PERF_REQUESTS_LOCK:
            record = _PERF_REQUESTS.get(request_id)
            if record is not None:
                record["http_calls"].append(call)
    except Exception:
        pass


def _perf_run_with_context(func, request_id, *args, **kwargs):
    # ThreadPoolExecutor worker에서도 동일 request_id를 전달해 실제 외부 HTTP 시간을 집계한다.
    previous = getattr(_PERF_LOCAL, "request_id", None)
    _PERF_LOCAL.request_id = request_id
    try:
        return func(*args, **kwargs)
    finally:
        if previous is None:
            try:
                del _PERF_LOCAL.request_id
            except AttributeError:
                pass
        else:
            _PERF_LOCAL.request_id = previous


def _perf_submit(executor, func, *args, **kwargs):
    """현재 분석 요청의 request_id를 worker thread로 전달한다."""
    request_id = getattr(_PERF_LOCAL, "request_id", None)
    if request_id:
        return executor.submit(_perf_run_with_context, func, request_id, *args, **kwargs)
    return executor.submit(func, *args, **kwargs)


def _perf_stage(name):
    request_id = getattr(_PERF_LOCAL, "request_id", None)
    if not request_id:
        return
    now = time.perf_counter()
    with _PERF_REQUESTS_LOCK:
        record = _PERF_REQUESTS.get(request_id)
        if record is None:
            return
        last = record.get("last_stage_ts", record["started"])
        record["stages"][str(name)] = round((now - last) * 1000.0, 1)
        record["last_stage_ts"] = now


def _perf_store_history(snapshot):
    if not snapshot:
        return
    with _PERF_REQUESTS_LOCK:
        _PERF_HISTORY.append(dict(snapshot))
        if len(_PERF_HISTORY) > _PERF_HISTORY_MAX:
            del _PERF_HISTORY[:-_PERF_HISTORY_MAX]


def _perf_history_snapshot(limit=20):
    with _PERF_REQUESTS_LOCK:
        rows = list(_PERF_HISTORY[-max(1, min(int(limit), _PERF_HISTORY_MAX)):])
    analyze_rows = [r for r in rows if r.get("kind") == "analyze"]
    chart_rows = [r for r in rows if r.get("kind") == "chart"]

    def _summary(items):
        vals = sorted(float(x.get("total_ms") or 0) for x in items)
        if not vals:
            return {"count": 0, "avg_ms": 0, "p50_ms": 0, "p95_ms": 0, "max_ms": 0}
        def pct(p):
            idx = min(len(vals) - 1, max(0, int(round((len(vals) - 1) * p))))
            return round(vals[idx], 1)
        return {
            "count": len(vals),
            "avg_ms": round(sum(vals) / len(vals), 1),
            "p50_ms": pct(0.50),
            "p95_ms": pct(0.95),
            "max_ms": round(vals[-1], 1),
        }

    return {
        "recent": rows,
        "analyze": _summary(analyze_rows),
        "chart": _summary(chart_rows),
        "history_limit": _PERF_HISTORY_MAX,
    }


_original_http_get = HTTP_SESSION.get

def _timed_http_get(url, *args, **kwargs):
    started = time.perf_counter()
    try:
        return _original_http_get(url, *args, **kwargs)
    finally:
        elapsed_ms = (time.perf_counter() - started) * 1000.0
        _perf_record_http(url, elapsed_ms)

HTTP_SESSION.get = _timed_http_get

def _perf_begin(kind, query):
    request_id = f"{threading.get_ident()}-{time.time_ns()}"
    started = time.perf_counter()
    _PERF_LOCAL.kind = kind
    _PERF_LOCAL.query = query
    _PERF_LOCAL.started = started
    _PERF_LOCAL.request_id = request_id
    _PERF_LOCAL.http_calls = []
    with _PERF_REQUESTS_LOCK:
        _PERF_REQUESTS[request_id] = {
            "kind": kind,
            "query": str(query),
            "started": started,
            "last_stage_ts": started,
            "http_calls": [],
            "stages": {},
        }
    return request_id


def _perf_snapshot():
    request_id = getattr(_PERF_LOCAL, "request_id", None)
    started = getattr(_PERF_LOCAL, "started", None)
    with _PERF_REQUESTS_LOCK:
        record = dict(_PERF_REQUESTS.get(request_id, {}) or {}) if request_id else {}
        calls = list(record.get("http_calls", []) or [])
        stages = dict(record.get("stages", {}) or {})
    total_ms = ((time.perf_counter() - started) * 1000.0) if started else None
    calls.sort(key=lambda x: x.get("ms", 0), reverse=True)
    snapshot = {
        "request_id": request_id,
        "kind": getattr(_PERF_LOCAL, "kind", None),
        "query": getattr(_PERF_LOCAL, "query", None),
        "total_ms": round(total_ms, 1) if total_ms is not None else None,
        "http_call_count": len(calls),
        "slowest_http": calls[:10],
        "stages": stages,
    }
    _perf_store_history(snapshot)
    if request_id:
        with _PERF_REQUESTS_LOCK:
            _PERF_REQUESTS.pop(request_id, None)
    return snapshot
