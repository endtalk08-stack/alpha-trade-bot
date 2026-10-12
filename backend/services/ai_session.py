"""Protected AI chat session, independent of the site's private preview gate."""
import hmac
import os
import threading
import time
from collections import defaultdict
from flask import Blueprint, jsonify, request
from itsdangerous import URLSafeTimedSerializer, BadSignature, SignatureExpired

ai_session_bp = Blueprint("gaemi_ai_session", __name__)
COOKIE = "gaemi_ai_session"
TTL = 12 * 3600
_failures = defaultdict(list)
_lock = threading.Lock()

def _password():
    return os.getenv("AI_CHAT_ACCESS_PASSWORD", "").strip()

def _serializer():
    key = _password()
    return URLSafeTimedSerializer(key, salt="gaemi-ai-session-v1") if len(key) >= 16 else None

def authorized():
    signer = _serializer()
    if signer is None:
        return False
    try:
        return signer.loads(request.cookies.get(COOKIE, ""), max_age=TTL) == "ai"
    except (BadSignature, SignatureExpired, ValueError, TypeError):
        return False

@ai_session_bp.get("/api/ai-session/status")
def status():
    return jsonify({"configured": _serializer() is not None, "authenticated": authorized()})

@ai_session_bp.post("/api/ai-session/login")
def login():
    password = _password()
    signer = _serializer()
    if signer is None:
        return jsonify({"ok": False, "error": "ai_login_not_configured"}), 503
    origin = request.headers.get("Origin", "")
    if origin and origin != request.host_url.rstrip("/"):
        return jsonify({"ok": False, "error": "origin_mismatch"}), 403
    ip = request.remote_addr or "unknown"
    now = time.monotonic()
    with _lock:
        previous = [t for t in _failures[ip] if now - t < 600]
        _failures[ip] = previous
        if len(previous) >= 8:
            return jsonify({"ok": False, "error": "too_many_attempts"}), 429
    data = request.get_json(silent=True)
    received = data.get("password") if isinstance(data, dict) else None
    if not isinstance(received, str) or not hmac.compare_digest(received, password):
        with _lock:
            _failures[ip].append(now)
        return jsonify({"ok": False, "error": "invalid_password"}), 403
    response = jsonify({"ok": True})
    response.set_cookie(COOKIE, signer.dumps("ai"), max_age=TTL, httponly=True, secure=True, samesite="Lax", path="/")
    response.headers["Cache-Control"] = "no-store"
    return response
