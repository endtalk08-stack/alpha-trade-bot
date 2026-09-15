import threading
import time

EXTERNAL_CACHE_LOCK = threading.Lock()

FRED_LAST_REQUEST_TS = 0.0

FRED_MIN_INTERVAL = 0.55  # FRED 2 req/s 제한에 안전하게 대응

API_LOCK = threading.Lock()

def _rate_limit_wait(min_interval):
    global FRED_LAST_REQUEST_TS
    with API_LOCK:
        now = time.time()
        wait = min_interval - (now - FRED_LAST_REQUEST_TS)
        if wait > 0:
            time.sleep(wait)
        FRED_LAST_REQUEST_TS = time.time()

def _cache_get(cache, key, ttl):
    with EXTERNAL_CACHE_LOCK:
        item = cache.get(key)
        if not item:
            return None
        ts, value = item
        if time.time() - ts > ttl:
            cache.pop(key, None)
            return None
        return value

def _cache_set(cache, key, value):
    with EXTERNAL_CACHE_LOCK:
        cache[key] = (time.time(), value)
        # 외부 API 캐시는 장시간 서버가 살아 있어도 무한히 커지지 않도록 제한한다.
        if len(cache) > 1000:
            oldest = sorted(cache.items(), key=lambda item: item[1][0])[:200]
            for old_key, _ in oldest:
                cache.pop(old_key, None)
    return value

