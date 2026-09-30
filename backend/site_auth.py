"""Site-wide password gate. Empty SITE_PASSWORD disables it (local/dev)."""
from __future__ import annotations

import hashlib
import hmac
import os
import time
from collections import defaultdict

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse, Response
from starlette.middleware.base import BaseHTTPMiddleware

COOKIE = "wb_dash"
TTL_SEC = 30 * 24 * 3600
FAIL_WINDOW = 15 * 60
FAIL_MAX = 8
PUBLIC = {"/api/login", "/api/logout"}

_fails: dict[str, list[float]] = defaultdict(list)


def site_password() -> str:
    return (os.getenv("SITE_PASSWORD") or "").strip()


def _secret() -> bytes:
    extra = (os.getenv("SITE_AUTH_SECRET") or "").strip()
    raw = extra or (site_password() + "|wb-dashboard-gate")
    return hashlib.sha256(raw.encode()).digest()


def _sign(exp: int) -> str:
    mac = hmac.new(_secret(), str(exp).encode(), hashlib.sha256).hexdigest()
    return f"{exp}.{mac}"


def cookie_valid(token: str) -> bool:
    if not token or "." not in token:
        return False
    exp_s, mac = token.split(".", 1)
    try:
        exp = int(exp_s)
    except ValueError:
        return False
    if exp < int(time.time()):
        return False
    expect = hmac.new(_secret(), exp_s.encode(), hashlib.sha256).hexdigest()
    return hmac.compare_digest(expect, mac)


def password_ok(got: str) -> bool:
    want = site_password()
    if not want:
        return False
    a = hashlib.sha256((got or "").encode()).digest()
    b = hashlib.sha256(want.encode()).digest()
    return hmac.compare_digest(a, b)


def _client_ip(request: Request) -> str:
    fwd = (request.headers.get("x-forwarded-for") or "").split(",")[0].strip()
    return fwd or (request.client.host if request.client else "unknown")


def _too_many(ip: str) -> bool:
    now = time.time()
    rec = [t for t in _fails[ip] if now - t < FAIL_WINDOW]
    _fails[ip] = rec
    return len(rec) >= FAIL_MAX


def _mark_fail(ip: str) -> None:
    _fails[ip].append(time.time())


def _secure_cookie(request: Request) -> bool:
    proto = (request.headers.get("x-forwarded-proto") or request.url.scheme or "").lower()
    return proto == "https"


def _set_auth_cookie(response: Response, request: Request) -> None:
    exp = int(time.time()) + TTL_SEC
    response.set_cookie(
        COOKIE,
        _sign(exp),
        max_age=TTL_SEC,
        httponly=True,
        samesite="lax",
        secure=_secure_cookie(request),
        path="/",
    )


LOGIN_HTML = """<!DOCTYPE html>
<html lang="ru">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Вход — WB Partners</title>
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600&display=swap" rel="stylesheet">
<style>
*{box-sizing:border-box;margin:0;padding:0}
body{min-height:100vh;display:flex;align-items:center;justify-content:center;
  font-family:Inter,sans-serif;background:#F5F3F0;color:#1A1816}
.card{width:min(400px,calc(100vw - 32px));background:#fff;border:1px solid #E2DDD8;
  border-radius:12px;padding:28px 26px 24px;box-shadow:0 12px 40px rgba(74,69,64,.08)}
.brand{display:flex;align-items:center;gap:10px;margin-bottom:18px}
.mark{width:10px;height:10px;border-radius:50%;background:#D4622A}
h1{font-size:16px;font-weight:600;letter-spacing:.02em}
.sub{font-size:13px;color:#6B6560;margin:0 0 20px}
label{display:block;font-size:12px;color:#6B6560;margin-bottom:6px}
input{width:100%;border:1px solid #E2DDD8;border-radius:8px;padding:11px 12px;
  font:15px Inter,sans-serif;color:#1A1816;background:#fff}
input:focus{outline:none;border-color:#D4622A;box-shadow:0 0 0 3px rgba(212,98,42,.12)}
button{width:100%;margin-top:14px;border:0;border-radius:8px;padding:11px 14px;
  font:14px/1 Inter,sans-serif;font-weight:600;color:#fff;background:#D4622A;cursor:pointer}
button:hover{background:#4A4540}
button:disabled{opacity:.65;cursor:default}
.err{min-height:18px;margin-top:12px;font-size:13px;color:#F04438}
</style>
</head>
<body>
<form class="card" id="f" autocomplete="current-password">
  <div class="brand"><span class="mark"></span><h1>WB Partners</h1></div>
  <p class="sub">Введи пароль, чтобы открыть кабинет.</p>
  <label for="p">Пароль</label>
  <input id="p" name="password" type="password" autofocus required>
  <button type="submit" id="b">Войти</button>
  <div class="err" id="e"></div>
</form>
<script>
const f=document.getElementById('f'), p=document.getElementById('p');
const b=document.getElementById('b'), e=document.getElementById('e');
f.addEventListener('submit', async ev => {
  ev.preventDefault();
  e.textContent='';
  b.disabled=true;
  try {
    const r = await fetch('/api/login', {
      method:'POST',
      headers:{'Content-Type':'application/json'},
      body: JSON.stringify({password: p.value})
    });
    const d = await r.json().catch(() => ({}));
    if (!r.ok) { e.textContent = d.error || 'Неверный пароль'; b.disabled=false; return; }
    location.href = '/';
  } catch (err) {
    e.textContent = 'Сеть недоступна';
    b.disabled=false;
  }
});
</script>
</body>
</html>
"""


class SiteAuthMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        if not site_password():
            return await call_next(request)
        path = request.url.path
        if path in PUBLIC:
            return await call_next(request)
        if cookie_valid(request.cookies.get(COOKIE, "")):
            return await call_next(request)
        if path.startswith("/api/"):
            return JSONResponse({"error": "unauthorized"}, status_code=401)
        return HTMLResponse(
            LOGIN_HTML,
            status_code=200,
            headers={"Cache-Control": "no-store, no-cache, must-revalidate", "Pragma": "no-cache"},
        )


def register_site_auth(app: FastAPI) -> None:
    app.add_middleware(SiteAuthMiddleware)

    @app.post("/api/login")
    async def site_login(request: Request):
        if not site_password():
            return {"ok": True, "disabled": True}
        ip = _client_ip(request)
        if _too_many(ip):
            return JSONResponse({"error": "Слишком много попыток. Подожди пару минут."}, status_code=429)
        try:
            body = await request.json()
        except Exception:
            body = {}
        if not isinstance(body, dict) or not password_ok(str(body.get("password") or "")):
            _mark_fail(ip)
            return JSONResponse({"error": "Неверный пароль"}, status_code=401)
        resp = JSONResponse({"ok": True})
        _set_auth_cookie(resp, request)
        return resp

    @app.post("/api/logout")
    async def site_logout():
        resp = JSONResponse({"ok": True})
        resp.delete_cookie(COOKIE, path="/")
        return resp
