"""Same-process bridge to the original gaemiGTP Flask app.

Only routes under /gaemi-api/ are delegated, and Flask's existing
authentication and private-access checks remain in force.
"""
from werkzeug.test import Client
from werkzeug.wrappers import Response

_app = None

def _client():
    global _app
    if _app is None:
        from gaemi_app import app
        _app = Client(app, Response)
    return _app

def serve(handler, method="GET"):
    from urllib.parse import urlsplit
    parsed = urlsplit(handler.path)
    prefix = "/gaemi-api"
    if not (parsed.path == prefix or parsed.path.startswith(prefix + "/")):
        handler._json(404, {"ok": False, "error": "unknown_path"})
        return
    length = int(handler.headers.get("Content-Length") or 0)
    if length > 65536:
        handler._json(413, {"ok": False, "error": "request_too_large"})
        return
    payload = handler.rfile.read(length) if method == "POST" and length else None
    path = parsed.path[len(prefix):] or "/"
    path += ("?" + parsed.query) if parsed.query else ""
    forwarded_headers = {}
    for name in ("Content-Type", "Authorization", "Cookie"):
        value = handler.headers.get(name)
        if value:
            forwarded_headers[name] = value
    try:
        response = _client().open(path=path, method=method, data=payload,
                                  headers=forwarded_headers,
                                  follow_redirects=False)
    except Exception as exc:
        print(f"[gaemi gateway] error: {type(exc).__name__}: {exc}")
        handler._json(502, {"ok": False, "error": "gaemi_backend_unavailable"})
        return
    body = response.get_data()
    handler.send_response(response.status_code)
    for header in ("Content-Type", "Set-Cookie", "Location"):
        value = response.headers.get(header)
        if value:
            # Cookies on the integration host must not reuse a domain from elsewhere.
            handler.send_header(header, value)
    handler.send_header("Cache-Control", "no-store")
    handler.send_header("Content-Length", str(len(body)))
    handler.end_headers()
    handler.wfile.write(body)
