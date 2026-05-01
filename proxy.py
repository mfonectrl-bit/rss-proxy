import urllib.request
import urllib.parse
import urllib.error
import json
import threading
import time
import re
import os
import asyncio
import queue as _queue
import base64
import hashlib
import struct
import socket
import xml.etree.ElementTree as ET
from http.server import HTTPServer, BaseHTTPRequestHandler
from socketserver import ThreadingMixIn
from urllib.parse import urlparse, parse_qs, urlunparse
from concurrent.futures import ThreadPoolExecutor
from difflib import SequenceMatcher

FEEDS_FILE = 'feeds.json'

# --- GitHub Persistence ---
# Đặt các biến này trong Render Environment Variables:
#   GITHUB_TOKEN  : Personal Access Token (scope: repo)
#   GITHUB_REPO   : tên repo dạng "username/repo-name"
#   GITHUB_BRANCH : nhánh chứa code, thường là "main" hoặc "master"
GITHUB_TOKEN  = os.environ.get('GITHUB_TOKEN', '').strip()
GITHUB_REPO   = os.environ.get('GITHUB_REPO', '').strip()
GITHUB_BRANCH = os.environ.get('GITHUB_BRANCH', 'main').strip()
GITHUB_PATH   = 'feeds.json'   # đường dẫn file trong repo

_github_save_lock = threading.Lock()

def _github_api(method, path, payload=None):
    """Gọi GitHub API, trả về (status_code, response_dict)"""
    url = f'https://api.github.com/repos/{GITHUB_REPO}/contents/{path}'
    data = json.dumps(payload).encode('utf-8') if payload else None
    req = urllib.request.Request(url, data=data, method=method, headers={
        'Authorization': f'token {GITHUB_TOKEN}',
        'Accept': 'application/vnd.github.v3+json',
        'Content-Type': 'application/json',
        'User-Agent': 'RSSReader/1.0',
    })
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            return resp.status, json.loads(resp.read())
    except urllib.error.HTTPError as e:
        return e.code, {}
    except Exception as e:
        return 0, {'error': str(e)}

def save_feeds_to_file(urls):
    """Lưu feeds.json lên GitHub repo (ghi đè) + backup local"""
    # Gói toàn bộ config vào 1 object
    config = {
        'feeds': urls,
        'auto_fwd': auto_fwd_enabled,
        'tg_channels': tg_channels,
        'translate_engine': translate_engine,
    }
    config_str = json.dumps(config, ensure_ascii=False, indent=2)

    # Luôn lưu local làm backup
    try:
        with open(FEEDS_FILE, 'w', encoding='utf-8') as f:
            f.write(config_str)
    except Exception:
        pass

    if not GITHUB_TOKEN or not GITHUB_REPO:
        print('[CONFIG] Chưa cấu hình GITHUB_TOKEN/GITHUB_REPO — chỉ lưu local')
        return True

    with _github_save_lock:
        try:
            content_b64 = base64.b64encode(config_str.encode('utf-8')).decode('ascii')

            # Lấy SHA hiện tại (cần để update file)
            status, existing = _github_api('GET', GITHUB_PATH)
            sha = existing.get('sha') if status == 200 else None

            payload = {
                'message': f'[auto][skip ci] update feeds.json ({len(urls)} feeds)',
                'content': content_b64,
                'branch': GITHUB_BRANCH,
            }
            if sha:
                payload['sha'] = sha

            status, resp = _github_api('PUT', GITHUB_PATH, payload)
            if status in (200, 201):
                print(f'[CONFIG] ✅ Đã lưu {len(urls)} feeds + settings lên GitHub')
                return True
            else:
                print(f'[CONFIG] GitHub lưu lỗi HTTP {status}: {resp}')
                return False
        except Exception as e:
            print(f'[CONFIG] GitHub lưu lỗi: {e}')
            return False

def load_feeds_from_file():
    """Load feeds.json từ GitHub repo, fallback về local nếu lỗi"""
    if GITHUB_TOKEN and GITHUB_REPO:
        try:
            status, resp = _github_api('GET', GITHUB_PATH)
            if status == 200 and resp.get('content'):
                raw = base64.b64decode(resp['content']).decode('utf-8')
                data = json.loads(raw)
                # Hỗ trợ cả format cũ (list) và mới (dict)
                feeds_list = data.get('feeds', data) if isinstance(data, dict) else data
                print(f'[CONFIG] ✅ Load thành công {len(feeds_list)} feeds từ GitHub')
                try:
                    with open(FEEDS_FILE, 'w', encoding='utf-8') as f:
                        f.write(raw)
                except Exception:
                    pass
                return data  # trả về toàn bộ để __main__ xử lý
            elif status == 404:
                print('[CONFIG] feeds.json chưa có trên GitHub — sẽ tạo khi lưu lần đầu')
            else:
                print(f'[CONFIG] GitHub load lỗi HTTP {status} — thử đọc local')
        except Exception as e:
            print(f'[CONFIG] GitHub load lỗi: {e} — thử đọc local')

    # Fallback: đọc local
    if os.path.exists(FEEDS_FILE):
        try:
            with open(FEEDS_FILE, 'r', encoding='utf-8') as f:
                data = json.load(f)
            feeds_list = data.get('feeds', data) if isinstance(data, dict) else data
            print(f'[CONFIG] ✅ Load thành công {len(feeds_list)} feeds từ local')
            return data
        except Exception as e:
            print(f'[CONFIG] Đọc local lỗi: {e}')

    print('[CONFIG] Chưa có feeds — mở web để thêm feed lần đầu')
    return []


# --- Redis / Dedup ---
try:
    import redis as _redis_lib
    REDIS_AVAILABLE = True
except ImportError:
    REDIS_AVAILABLE = False
    print('[!] Thư viện redis chưa cài — pip install redis')


class RSSDeduplicator:
    """
    Chống trùng tin 3 lớp: time filter → exact hash → fuzzy title.
    Dùng Redis để persist qua restart.
    """
    def __init__(self, redis_client,
                 key_prefix='rss_dedupe',
                 max_items=5000,
                 similarity_threshold=0.9,
                 time_window_hours=24):
        self.r = redis_client
        self.key_items  = f'{key_prefix}:items'
        self.key_titles = f'{key_prefix}:titles'
        self.max_items  = max_items
        self.similarity_threshold = similarity_threshold
        self.time_window = time_window_hours * 3600

    def _normalize_url(self, url):
        try:
            parsed = urlparse(url)
            return urlunparse((parsed.scheme, parsed.netloc, parsed.path, '', '', ''))
        except Exception:
            return url or ''

    def _clean_title(self, title):
        if not title:
            return ''
        title = title.lower()
        title = re.sub(r'\s+', ' ', title)
        title = re.sub(r'[^\w\s]', '', title)
        return title.strip()

    def _is_similar(self, t1, t2):
        return SequenceMatcher(None, t1, t2).ratio() >= self.similarity_threshold

    def _make_id(self, item):
        guid  = item.get('guid', '') or ''
        link  = self._normalize_url(item.get('link', ''))
        title = self._clean_title(item.get('title', ''))
        raw = f'{guid}|{link}|{title}'
        # Nếu tất cả đều rỗng → thêm timestamp để tránh hash trùng
        if not guid and not link and not title:
            raw = f'empty|{time.time()}|{id(item)}'
        return hashlib.md5(raw.encode()).hexdigest()

    def _is_old(self, item):
        ts = item.get('timestamp')
        if not ts:
            return False
        return (time.time() - ts) > self.time_window

    def is_duplicate(self, item):
        try:
            fid   = self._make_id(item)
            title = self._clean_title(item.get('title', ''))
            if self._is_old(item):
                return True
            if self.r.sismember(self.key_items, fid):
                return True
            # Chỉ check fuzzy title nếu title không rỗng
            # Item không có title (media-only) → bỏ qua fuzzy check, luôn cho qua
            if title:
                recent = self.r.lrange(self.key_titles, 0, 50)
                for t in recent:
                    t_decoded = t.decode()
                    if t_decoded and self._is_similar(title, t_decoded):
                        return True
            pipe = self.r.pipeline()
            pipe.sadd(self.key_items, fid)
            if title:  # Chỉ lưu title nếu có — tránh lưu string rỗng
                pipe.lpush(self.key_titles, title)
                pipe.ltrim(self.key_titles, 0, self.max_items)
            pipe.expire(self.key_items, int(self.time_window))
            pipe.expire(self.key_titles, int(self.time_window))
            pipe.execute()
            return False
        except Exception as e:
            print(f'[Dedup] Redis lỗi (bỏ qua): {e}')
            return False


# Khởi tạo deduplicator — None nếu không có Redis
_deduplicator = None

# --- State "đọc toàn bộ nguồn" ---
# Set chứa URL của các job cần dừng (thay vì 1 flag global duy nhất)
_read_all_stop_urls = set()
# Job chạy ngầm: None hoặc dict {url, name, done, total, status, error}
# status: 'running' | 'done' | 'error' | 'stopped'
_read_all_jobs = {}  # {url: {status, done, total, name, current}} — multi-job support
_read_all_job_lock = threading.Lock()

# Forward count per feed (in-memory, reset khi restart)
_fwd_counts    = {}   # {url: int}
_fwd_count_lock = threading.Lock()

# Dedup per-send: tránh gửi trùng khi nhiều job song song cùng forward 1 guid tới cùng dest
_fwd_sent_keys = set()   # {guid|dest|topic_id}
_fwd_sent_lock = threading.Lock()

# --- CẤU HÌNH ---
POLL_INTERVAL    = 90
HTTP_PORT = int(os.environ.get("PORT", 8765))
TRANSLATE_ENABLE = True
SESSION_FILE     = 'tg_session'
TG_CONFIG_FILE   = 'tg_config.json'

# --- BẢO MẬT ---
# Đặt APP_USERNAME, APP_PASSWORD, APP_API_KEY trong Render Environment Variables
# Nếu không đặt → app chạy không cần login (chỉ dùng local)
APP_USERNAME = os.environ.get('APP_USERNAME', '').strip()
APP_PASSWORD = os.environ.get('APP_PASSWORD', '').strip()
APP_API_KEY  = os.environ.get('APP_API_KEY', '').strip()   # dùng cho API calls / UptimeRobot

# Chat ID nhận thông báo lỗi (mày, không phải kênh đích)
# Đặt ERROR_NOTIFY_CHAT trong Render env — ví dụ: '123456789' (user ID của mày)
ERROR_NOTIFY_CHAT = os.environ.get('ERROR_NOTIFY_CHAT', '').strip()

# Bot Telegram riêng để nhận cảnh báo (có sound notification)
# Tạo bot qua @BotFather → lấy token → khai báo ERROR_BOT_TOKEN trong Render env
ERROR_BOT_TOKEN = os.environ.get('ERROR_BOT_TOKEN', '').strip()

# Thời điểm khởi động server — dùng cho uptime
_start_time = time.time()

# Tracking lỗi feed — tránh spam notification
_feed_errors      = {}   # {url: {'count': int, 'last_notify': float}}
_feed_error_lock  = threading.Lock()
_ERROR_THRESHOLD  = 3    # Báo sau 3 lần lỗi liên tiếp
_NOTIFY_COOLDOWN  = 3600 # Không spam — tối thiểu 1 tiếng/lần

# ===================== TELEGRAM BOT (Error Notification + Check System) =====================

def _bot_send(text: str, chat_id: str = None, reply_markup: dict = None, parse_mode: str = 'HTML'):
    """Gửi tin qua Bot API — không dùng Telethon, dùng HTTP thuần."""
    cid = chat_id or ERROR_NOTIFY_CHAT
    if not ERROR_BOT_TOKEN or not cid:
        return False
    try:
        payload = {'chat_id': cid, 'text': text, 'parse_mode': parse_mode}
        if reply_markup:
            payload['reply_markup'] = json.dumps(reply_markup)
        data = json.dumps(payload).encode('utf-8')
        req  = urllib.request.Request(
            f'https://api.telegram.org/bot{ERROR_BOT_TOKEN}/sendMessage',
            data=data,
            headers={'Content-Type': 'application/json'},
            method='POST'
        )
        with urllib.request.urlopen(req, timeout=10) as resp:
            result = json.loads(resp.read())
            return result.get('ok', False)
    except Exception as e:
        print(f'[Bot] Gửi thất bại: {e}')
        return False

def _bot_answer_callback(callback_query_id: str, text: str = ''):
    """Answer callback query để Telegram biết bot đã xử lý."""
    if not ERROR_BOT_TOKEN:
        return
    try:
        payload = json.dumps({'callback_query_id': callback_query_id, 'text': text}).encode('utf-8')
        req = urllib.request.Request(
            f'https://api.telegram.org/bot{ERROR_BOT_TOKEN}/answerCallbackQuery',
            data=payload, headers={'Content-Type': 'application/json'}, method='POST'
        )
        urllib.request.urlopen(req, timeout=5)
    except Exception:
        pass

def _bot_get_system_status() -> str:
    """Tạo message status hệ thống đầy đủ."""
    now = time.time()
    uptime_sec = int(now - _start_time)
    h, m = divmod(uptime_sec // 60, 60)
    d, h = divmod(h, 24)
    uptime_str = f'{d}d {h}h {m}m' if d else f'{h}h {m}m'

    # Feeds
    with lock:
        total_feeds = len(watched_urls)
        tg_feeds  = sum(1 for u in watched_urls if is_tg_source(u.get('url', '')))
        rss_feeds = total_feeds - tg_feeds

    # Jobs đang chạy
    with _read_all_job_lock:
        running_jobs = [(v['name'], v['done'], v['total'])
                        for v in _read_all_jobs.values() if v.get('status') == 'running']

    # Redis
    redis_ok = False
    try:
        if _deduplicator:
            _deduplicator.r.ping()
            redis_ok = True
    except Exception:
        pass

    # Forward counts
    with _fwd_count_lock:
        total_fwd = sum(_fwd_counts.values())

    lines = [
        '📊 <b>RSS Reader — System Status</b>',
        '',
        f'🕐 <b>Uptime:</b> {uptime_str}',
        f'🔗 <b>Telethon:</b> {"✅ Đã kết nối" if (TELETHON_AVAILABLE and tg_client is not None) else "❌ Chưa kết nối"}',
        f'🗄 <b>Redis (dedup):</b> {"✅ OK" if redis_ok else "❌ Không kết nối được"}',
        f'🔄 <b>Auto-forward:</b> {"✅ Bật" if auto_fwd_enabled else "⏸ Tắt"}',
        '',
        f'📋 <b>Feeds:</b> {total_feeds} tổng ({rss_feeds} RSS + {tg_feeds} TG)',
        f'📤 <b>Đã forward:</b> {total_fwd} bài (từ khi khởi động)',
        f'📡 <b>Kênh đích:</b> {len(tg_channels)} kênh',
    ]

    if running_jobs:
        lines.append('')
        lines.append(f'⚙️ <b>Đang chạy {len(running_jobs)} job:</b>')
        for name, done, total in running_jobs:
            pct = f'{round(done/total*100)}%' if total else '?%'
            lines.append(f'  • {name}: {done}/{total} ({pct})')

    # Feeds có lỗi gần đây
    with _feed_error_lock:
        err_feeds = [(url, rec) for url, rec in _feed_errors.items()
                     if rec.get('count', 0) > 0 or rec.get('last_notify', 0) > 0]
    if err_feeds:
        lines.append('')
        lines.append(f'⚠️ <b>Feeds có lỗi gần đây:</b>')
        with lock:
            url_to_name = {u['url']: u.get('name', u['url']) for u in watched_urls}
        for url, rec in err_feeds[:5]:
            name = url_to_name.get(url, url)[:30]
            cnt  = rec.get('count', 0)
            last = rec.get('last_notify', 0)
            last_str = f'{int((now-last)//60)}p trước' if last else 'chưa báo'
            lines.append(f'  • {name} (lỗi: {cnt}, báo: {last_str})')

    return '\n'.join(lines)

_CHECK_KEYBOARD = {
    'inline_keyboard': [[
        {'text': '🔍 Check system', 'callback_data': 'check_system'},
        {'text': '🔕 Tắt cảnh báo tạm', 'callback_data': 'mute_1h'},
    ]]
}

# Mute notification tạm thời
_bot_muted_until = 0.0
_bot_mute_lock   = threading.Lock()

def _bot_is_muted() -> bool:
    with _bot_mute_lock:
        return time.time() < _bot_muted_until

def _bot_poll_loop():
    """Long-polling Bot API — chạy trên thread riêng, nhận lệnh từ Telegram."""
    if not ERROR_BOT_TOKEN:
        return
    print('[Bot] Bắt đầu polling Telegram Bot API...')
    offset = 0
    while True:
        try:
            url = (f'https://api.telegram.org/bot{ERROR_BOT_TOKEN}/getUpdates'
                   f'?offset={offset}&timeout=30&allowed_updates=["message","callback_query"]')
            req = urllib.request.Request(url, method='GET')
            with urllib.request.urlopen(req, timeout=35) as resp:
                data = json.loads(resp.read())
            if not data.get('ok'):
                time.sleep(5); continue

            for upd in data.get('result', []):
                offset = upd['update_id'] + 1
                # Xử lý callback_query (nút bấm)
                if 'callback_query' in upd:
                    cq     = upd['callback_query']
                    cid    = str(cq['message']['chat']['id'])
                    cqid   = cq['id']
                    action = cq.get('data', '')
                    if action == 'check_system':
                        _bot_answer_callback(cqid, '🔍 Đang kiểm tra...')
                        _bot_send(_bot_get_system_status(), chat_id=cid,
                                  reply_markup=_CHECK_KEYBOARD)
                    elif action == 'mute_1h':
                        with _bot_mute_lock:
                            global _bot_muted_until
                            _bot_muted_until = time.time() + 3600
                        _bot_answer_callback(cqid, '🔕 Đã tắt cảnh báo 1 tiếng')
                        _bot_send('🔕 Cảnh báo lỗi đã tắt trong <b>1 tiếng</b>.\nDùng /start để bật lại sớm hơn.', chat_id=cid, reply_markup=_CHECK_KEYBOARD)




                # Xử lý message text
                elif 'message' in upd:
                    msg  = upd['message']
                    cid  = str(msg['chat']['id'])
                    text = msg.get('text', '')
                    if text in ('/start', '/status', '/check'):
                        # Reset mute nếu có
                        with _bot_mute_lock:
                            _bot_muted_until = 0.0
                        _bot_send('👋 <b>RSS Reader Bot</b> sẵn sàng! ' 'Tao sẽ thông báo khi có feed lỗi hoặc forward thất bại. ' 'Nhấn nút bên dưới để kiểm tra trạng thái hệ thống:', chat_id=cid, reply_markup=_CHECK_KEYBOARD)
        except urllib.error.HTTPError as e:
            if e.code == 409:
                # 409 Conflict: 2 instance đang poll cùng lúc (xảy ra khi Render deploy)
                # Đợi lâu hơn để instance cũ tắt trước
                print('[Bot] 409 Conflict — đang chờ instance cũ tắt...')
                time.sleep(30)
            elif e.code == 429:
                # Rate limit
                print('[Bot] 429 Too Many Requests — chờ 60s')
                time.sleep(60)
            else:
                print(f'[Bot] HTTP lỗi {e.code}: {e}')
                time.sleep(10)
        except Exception as e:
            print(f'[Bot] Poll lỗi: {e}')
            time.sleep(10)

def _notify_error(msg: str, feed_url: str = '', is_test: bool = False):
    """
    Gửi thông báo lỗi qua 2 kênh:
    1. Broadcast WS → UI banner
    2. Telegram message → ERROR_NOTIFY_CHAT (nếu đã cấu hình)
    """
    # 1. UI banner — luôn broadcast
    broadcast({'type': 'error_alert', 'msg': msg, 'url': feed_url, 'ts': time.time()})

    # 2. Telegram — ưu tiên Bot API, fallback Telethon
    if is_test:
        test_text = '🔔 <b>Test thông báo RSS Reader</b>\n\nHệ thống hoạt động bình thường ✅'
        if ERROR_BOT_TOKEN and ERROR_NOTIFY_CHAT:
            ok = _bot_send(test_text, reply_markup=_CHECK_KEYBOARD)
        else:
            ok = False
        if not ok:
            _tg_notify_send(f'🔔 Test thông báo\n{msg}')
        return

    if not ERROR_NOTIFY_CHAT:
        return
    if _bot_is_muted():
        print(f'[Notify] Muted — bỏ qua: {msg[:60]}')
        return

    now = time.time()
    with _feed_error_lock:
        rec = _feed_errors.get(feed_url, {'count': 0, 'last_notify': 0})
        rec['count'] += 1
        _feed_errors[feed_url] = rec
        if rec['count'] < _ERROR_THRESHOLD:
            return   # chưa đủ ngưỡng
        if now - rec['last_notify'] < _NOTIFY_COOLDOWN:
            return   # cooldown chưa hết
        rec['last_notify'] = now
        rec['count'] = 0

    # Gửi qua Bot API (có sound) — fallback Telethon nếu chưa cấu hình bot
    def _do_send():
        err_text = f'⚠️ <b>RSS Reader — Cảnh báo lỗi</b>\n\n{msg}'
        sent = False
        if ERROR_BOT_TOKEN:
            sent = _bot_send(err_text, reply_markup=_CHECK_KEYBOARD)
        if not sent:
            _tg_notify_send(msg)
    _thread_pool.submit(_do_send)

def _notify_recover(feed_url: str, feed_name: str):
    """Báo khi feed hoạt động trở lại sau khi đã có lỗi."""
    with _feed_error_lock:
        rec = _feed_errors.get(feed_url, {})
        if rec.get('last_notify', 0) == 0:
            return  # chưa bao giờ báo lỗi → không cần báo recover
        _feed_errors[feed_url] = {'count': 0, 'last_notify': 0}
    broadcast({'type': 'error_recover', 'url': feed_url})
    def _do_recover():
        text = f'✅ <b>Feed hoạt động trở lại</b>\n\n{feed_name}'
        sent = False
        if ERROR_BOT_TOKEN:
            sent = _bot_send(text, reply_markup=_CHECK_KEYBOARD)
        if not sent:
            _tg_notify_send(f'✅ Feed hoạt động trở lại: {feed_name}')
    _thread_pool.submit(_do_recover)

def _tg_notify_send(text: str):
    """Gửi text tới ERROR_NOTIFY_CHAT — chạy trên thread riêng, không block poller."""
    if not ERROR_NOTIFY_CHAT or not TELETHON_AVAILABLE or tg_client is None:
        return
    def _do():
        async def _send():
            try:
                await tg_client.send_message(int(ERROR_NOTIFY_CHAT), text)
                print(f'[Notify] ✅ Đã gửi thông báo tới {ERROR_NOTIFY_CHAT}')
            except Exception as e:
                print(f'[Notify] Gửi thất bại: {e}')
        try:
            tg_run(_send())
        except Exception as e:
            print(f'[Notify] tg_run lỗi: {e}')
    _thread_pool.submit(_do)

def _auth_enabled():
    return bool(APP_USERNAME and APP_PASSWORD)

def _make_session_token():
    """Tạo session token ngẫu nhiên."""
    return hashlib.sha256(os.urandom(32)).hexdigest()

def _check_session(cookie_header):
    """Kiểm tra session cookie hợp lệ."""
    if not cookie_header:
        return False
    for part in cookie_header.split(';'):
        part = part.strip()
        if part.startswith('session='):
            token = part[8:]
            with _session_lock:
                return token in _active_sessions
    return False

def _check_api_key(headers):
    """Kiểm tra X-API-Key header."""
    if not APP_API_KEY:
        return False
    return headers.get('X-API-Key', '') == APP_API_KEY

def _is_authed(handler):
    """True nếu request đã xác thực (session cookie hoặc API key)."""
    if not _auth_enabled():
        return True   # chưa cấu hình → luôn cho qua (local dev)
    if _check_api_key(handler.headers):
        return True
    return _check_session(handler.headers.get('Cookie', ''))

_session_lock   = threading.Lock()
_active_sessions = set()   # set các token hợp lệ

# Pool cho poll/fetch/setup — giới hạn 40 threads
_thread_pool = ThreadPoolExecutor(max_workers=20)
# Pool riêng cho forward — tách biệt, không bị poll/fetch chiếm chỗ
_forward_pool = ThreadPoolExecutor(max_workers=5)

# --- Engine dịch ---
DEEPL_API_KEY  = os.environ.get('DEEPL_API_KEY', '').strip()

# Multi-key support: GEMINI_API_KEYS=key1,key2,key3 (ưu tiên)
# Backward compatible: GEMINI_API_KEY=key1 (vẫn hoạt động)
def _load_gemini_keys():
    raw = os.environ.get('GEMINI_API_KEYS', '').strip()
    if raw:
        keys = [k.strip() for k in raw.split(',') if k.strip()]
        if keys:
            return keys
    single = os.environ.get('GEMINI_API_KEY', '').strip()
    return [single] if single else []

GEMINI_API_KEYS = _load_gemini_keys()   # list các key, có thể 1 hoặc nhiều
GEMINI_API_KEY  = GEMINI_API_KEYS[0] if GEMINI_API_KEYS else ''  # backward compat

def _default_engine():
    if GEMINI_API_KEY:
        return 'gemini'
    if DEEPL_API_KEY:
        return 'deepl'
    return 'google'

translate_engine      = _default_engine()
translate_target_lang = 'vi'   # ngôn ngữ đích dịch — có thể đổi qua UI
import threading as _threading
_tl_engine = _threading.local()  # thread-local: lưu engine vừa dùng để gán vào item
# Map code ngôn ngữ → tên để dùng trong Gemini prompt
_LANG_NAME = {
    'vi': 'tiếng Việt', 'en': 'English', 'zh-CN': 'Chinese', 'ja': 'Japanese',
    'ko': 'Korean', 'fr': 'French', 'de': 'German', 'es': 'Spanish',
    'pt': 'Portuguese', 'it': 'Italian', 'ru': 'Russian', 'ar': 'Arabic',
    'hi': 'Hindi', 'th': 'Thai', 'id': 'Indonesian', 'ms': 'Malay',
    'nl': 'Dutch', 'pl': 'Polish', 'sv': 'Swedish', 'tr': 'Turkish',
    'uk': 'Ukrainian', 'cs': 'Czech', 'ro': 'Romanian', 'hu': 'Hungarian',
    'el': 'Greek', 'da': 'Danish', 'fi': 'Finnish', 'no': 'Norwegian',
    'sk': 'Slovak', 'bg': 'Bulgarian', 'hr': 'Croatian', 'lt': 'Lithuanian',
    'lv': 'Latvian', 'et': 'Estonian', 'sl': 'Slovenian',
}
# Map code UI → code deep_translator (chỉ những code không khớp)
_GOOGLE_LANG_CODE = {
    'zh-CN': 'zh-CN',  # explicit — deep_translator dùng zh-CN không phải zh
}


from gemini_pool_sync import GeminiPool, GEMINI_MODELS as _GPOOL_MODELS, GEMINI_PREFIX as _GPOOL_PREFIX

_gemini_pool = GeminiPool(
    keys=GEMINI_API_KEYS,
    system_loaded_fn=lambda: _system_fully_loaded,
)
# Alias backward-compat cho các caller chưa đổi tên
_engine_dispatcher = _gemini_pool


# Import dịch — cần trước _dispatcher_translate (resolve at call-time, nhưng explicit hơn)
try:
    from deep_translator import GoogleTranslator as _GoogleTranslator
except ImportError:
    _GoogleTranslator = None

def _gemini_translate_inner(text, is_html=False):
    """
    Core retry loop dùng GeminiPool — dùng chung cho plain text và HTML.
    Trả về (result, alias) hoặc raise RuntimeError khi hết slot.
    """
    tried = set()
    while True:
        slot = _gemini_pool.pick_slot()
        if not slot:
            raise RuntimeError('GeminiPool: tất cả slots exhausted')
        alias, ki, api_key = slot
        slot_key = (alias, ki)
        if slot_key in tried:
            raise RuntimeError('GeminiPool: đã thử hết slots')
        tried.add(slot_key)
        model_name = _GPOOL_MODELS[alias]
        try:
            if is_html:
                result = _fix_html_spacing(_translate_gemini_html(text, model=model_name, api_key=api_key))
            else:
                result = _translate_gemini(text, model=model_name, api_key=api_key)
            _gemini_pool.record_success(alias, ki)
            return result, alias
        except Exception as e:
            is_rl = '429' in str(e) or 'quota' in str(e).lower()
            _gemini_pool.record_failure(alias, ki, is_rate_limit=is_rl)
            print(f'[Translate] {alias}[key{ki}] loi: {e} — thu slot tiep')


def _dispatcher_translate(text, preferred=None):
    """
    Drop-in replacement cho _engine_dispatcher.translate() cũ.
    Dùng trong _fast_translate(). Trả về (result, engine_label).
    """
    if GEMINI_API_KEYS and _system_fully_loaded:
        try:
            return _gemini_translate_inner(text, is_html=False)
        except RuntimeError:
            pass

    if DEEPL_API_KEY:
        try:
            return _translate_deepl(text), 'deepl'
        except Exception:
            pass

    try:
        _gtarget = _GOOGLE_LANG_CODE.get(translate_target_lang, translate_target_lang)
        if _GoogleTranslator:
            return _GoogleTranslator(source='auto', target=_gtarget).translate(text) or text, 'google'
    except Exception:
        pass
    return text, 'error'


# ================= AI SERVICE (Multi-AI Rotation) =================

class BatchPipeline:
    """
    Pipeline xử lý tin với nhiều worker thread song song.

    Thiết kế cho 45+ feed:
    - MAX_QUEUE_SIZE: nếu queue đầy → bỏ tin cũ nhất (tránh tắc nghẽn RAM)
    - NUM_WORKERS: nhiều worker xử lý song song, mỗi worker 1 batch
    - FLUSH_TIMEOUT: flush batch sau 0.5s dù chưa đủ BATCH_SIZE
    - Mỗi worker tự chờ queue độc lập → không tranh nhau lock
    """
    BATCH_SIZE     = 3
    FLUSH_TIMEOUT  = 0.5   # giây: flush nhanh để UI cập nhật ngay
    NUM_WORKERS    = 2
    MAX_QUEUE_SIZE = 100

    def __init__(self):
        self._q = _queue.Queue(maxsize=self.MAX_QUEUE_SIZE)
        self._threads = []

    def start(self):
        for i in range(self.NUM_WORKERS):
            t = threading.Thread(target=self._worker_loop, args=(i,), daemon=True)
            t.start()
            self._threads.append(t)
        print(f'[Pipeline] {self.NUM_WORKERS} workers khởi động, queue max={self.MAX_QUEUE_SIZE}')

    def put(self, item):
        """Non-blocking put — nếu queue đầy thì drop item cũ nhất để nhường chỗ item mới"""
        try:
            self._q.put_nowait(item)
        except _queue.Full:
            # Queue đầy → drop item cũ nhất (get + put_nowait)
            try:
                dropped = self._q.get_nowait()
                print(f'[Pipeline] Queue đầy — drop tin cũ: {dropped.get("guid","?")}')
                self._q.put_nowait(item)
            except _queue.Empty:
                pass  # race condition hiếm gặp, bỏ qua

    def _worker_loop(self, worker_id):
        while True:
            batch = []
            deadline = time.time() + self.FLUSH_TIMEOUT
            try:
                first = self._q.get(timeout=self.FLUSH_TIMEOUT)
                batch.append(first)
            except _queue.Empty:
                continue

            # Drain thêm không block cho đến hết BATCH_SIZE hoặc deadline
            while len(batch) < self.BATCH_SIZE:
                remaining = deadline - time.time()
                if remaining <= 0:
                    break
                try:
                    item = self._q.get(timeout=min(remaining, 0.1))
                    batch.append(item)
                except _queue.Empty:
                    break

            if batch:
                try:
                    self._process_batch(batch, worker_id)
                except Exception as e:
                    print(f'[Pipeline W{worker_id}] Batch lỗi: {e}')

    def _process_batch(self, batch, worker_id=0):
        """
        Xử lý một batch: AI translate → broadcast UI → forward Telethon.
        Chạy trên worker thread riêng — nhiều batch song song.
        """
        by_feed = {}
        for item in batch:
            fu = item.get('feed_url', '')
            by_feed.setdefault(fu, []).append(item)

        for feed_url, items in by_feed.items():
            processed = []
            for item in items:
                text = item.get('text', '')
                should_translate = item.get('do_translate', True)
                # Auto-skip nếu text đã là ngôn ngữ đích — tránh dịch thừa
                if should_translate and text and is_same_as_target(text[:200]):
                    should_translate = False
                if should_translate:
                    has_hidden_link = item.get('_tg_has_hidden_link', False)
                    has_format      = item.get('_tg_has_format', False)
                    html_text = item.get('_tg_html_text', '')
                    if html_text and (has_hidden_link or has_format):
                        # Bản tin có link ẩn / format HTML → dịch HTML để giữ nguyên thẻ
                        translated, _eng, _ = _translate_with_hidden_links(html_text)
                        # Nếu kết quả rỗng → fallback Google plain text
                        if not translated or not translated.strip():
                            _plain = re.sub(r'<[^>]+>', '', html_text).strip()
                            try:
                                _gtarget = _GOOGLE_LANG_CODE.get(translate_target_lang, translate_target_lang)
                                translated = GoogleTranslator(source='auto', target=_gtarget).translate(_plain) or _plain
                            except Exception:
                                translated = _plain or text
                            _eng = 'google'
                        # Chỉ update nếu có kết quả hợp lệ
                        if translated and translated.strip():
                            item['_tg_html_text'] = translated
                        item['_translate_engine_used'] = _eng
                    else:
                        # Bản tin plain text (không có hidden link / format):
                        # Ưu tiên Gemini cho TẤT CẢ bản tin, chỉ fallback Google khi Gemini hết quota/lỗi
                        translated = None
                        _eng = ''
                        if GEMINI_API_KEYS and _system_fully_loaded:
                            try:
                                translated, _eng = _gemini_translate_inner(text, is_html=False)
                            except RuntimeError:
                                pass  # hết slot → fallback bên dưới
                        # Fallback: _fast_translate (sẽ dùng dispatcher → DeepL → Google)
                        if not translated or not translated.strip():
                            translated = _fast_translate(text)
                            _eng = getattr(_tl_engine, 'used', 'google')
                        # Nếu vẫn rỗng → Google trực tiếp
                        if not translated or not translated.strip():
                            try:
                                _gtarget = _GOOGLE_LANG_CODE.get(translate_target_lang, translate_target_lang)
                                translated = GoogleTranslator(source='auto', target=_gtarget).translate(text) or text
                            except Exception:
                                translated = text
                            _eng = 'google'
                        # Xóa _tg_html_text để _do_forward không dùng text gốc chưa dịch
                        item['_tg_html_text'] = ''
                        item['_translate_engine_used'] = _eng
                else:
                    translated = text  # Không dịch — giữ nguyên bản gốc
                    item['_translate_engine_used'] = ''  # Không gán prefix engine
                if not item.get('category'):
                    item['category'] = item.get('category') or 'Khác'
                item['text_translated'] = translated
                # Giữ \n thật — không convert thành <br>, _do_forward xử lý plain text
                title     = (translated[:80] + '...') if len(translated) > 80 else translated
                item['desc']       = translated
                item['title']      = title
                item['translated'] = should_translate
                item['show_link']  = item.get('show_link', True)
                processed.append(item)

            if not processed:
                continue

            # 1. Broadcast lên UI ngay
            ws_items = [{k: v for k, v in it.items()
                         if k not in ('_tg_has_media', 'text', 'text_translated')}
                        for it in processed]
            broadcast({'type': 'new_items', 'url': feed_url, 'items': ws_items})

            # 2. Forward — dùng pool có giới hạn, không spawn thread mới mỗi batch
            feed_category = processed[0].get('category', '')
            _forward_pool.submit(_do_forward, processed, feed_category, feed_url)

    def qsize(self):
        return self._q.qsize()


# Khởi động pipeline — sẽ gọi sau khi các hàm broadcast/forward được định nghĩa
_pipeline = BatchPipeline()

try:
    from deep_translator import GoogleTranslator
    from langdetect import detect
    TRANSLATE_AVAILABLE = True
    print('[i] deep-translator + langdetect: OK')
except ImportError as e:
    TRANSLATE_AVAILABLE = False
    print(f'[!] Thư viện dịch chưa cài: {e}')

_active_engine = _default_engine()
_engines_ready = []
if GEMINI_API_KEY: _engines_ready.append('Gemini ✅')
if DEEPL_API_KEY:  _engines_ready.append('DeepL ✅')
_engines_ready.append('Google ✅')
print(f'[i] Engine dịch: {" | ".join(_engines_ready)} — ưu tiên: {_active_engine}')
from gemini_pool_sync import QUOTA as _GPOOL_QUOTA
_g25_lim = _GPOOL_QUOTA['gemini-2.5']['rpm'];      _g25_iv = int(_GPOOL_QUOTA['gemini-2.5']['interval'])
_g20_lim = _GPOOL_QUOTA['gemini-2.5-lite']['rpm']; _g30_lim = _GPOOL_QUOTA['gemini-3.0']['rpm']; _g15_lim = _GPOOL_QUOTA['gemini-3.1']['rpm']
_d_lim = 40  # DeepL limit giữ nguyên
print(f'[i] GeminiPool: GM2.5={_g25_lim}/min({_g25_iv}s) | GM2.5L={_g20_lim}/min | GM3.0={_g30_lim}/min | GM3.1={_g15_lim}/min | DeepL={_d_lim}/min | Google=unlimited')

try:
    from telethon import TelegramClient, events
    from telethon.sessions import StringSession
    from telethon.tl.types import MessageMediaPhoto, MessageMediaDocument
    from telethon.tl.types import MessageEntityTextUrl
    from telethon.extensions import html as tg_html
    import telethon.errors
    TELETHON_AVAILABLE = True
    print('[i] Telethon: OK')
except ImportError:
    TELETHON_AVAILABLE = False
    print('[!] Telethon chưa cài: pip install telethon')

translate_cache = {}
translate_lock  = threading.Lock()

# Telethon state
tg_client        = None
tg_api_id        = None
tg_api_hash      = None
tg_phone         = None
tg_loop          = None
tg_loop_thread   = None
tg_auth_state    = 'idle'   # idle | waiting_code | waiting_2fa | connected | error
tg_auth_msg      = ''
tg_phone_code_hash = None   # lưu phone_code_hash khi gửi OTP

# --- HÀM TIỆN ÍCH ---
def _translate_with_engine(text, engine=None):
    """Wrapper dùng GeminiPool — backward compatible."""
    result, used = _dispatcher_translate(text, preferred=engine or translate_engine)
    return result


def _translate_deepl(text):
    """Dịch bằng DeepL Free API"""
    payload = urllib.parse.urlencode({
        'auth_key': DEEPL_API_KEY,
        'text': text[:4000],
        'target_lang': translate_target_lang.upper(),
        'source_lang': 'auto',
    }).encode('utf-8')
    base = 'api-free.deepl.com' if DEEPL_API_KEY.endswith(':fx') else 'api.deepl.com'
    req = urllib.request.Request(
        f'https://{base}/v2/translate', data=payload,
        headers={'Content-Type': 'application/x-www-form-urlencoded'}
    )
    resp = urllib.request.urlopen(req, timeout=10)
    data = json.loads(resp.read())
    return data['translations'][0]['text'].strip()

def _translate_google(text):
    """Dịch bằng Google Translate (deep_translator) — fallback"""
    try:
        _gtarget = _GOOGLE_LANG_CODE.get(translate_target_lang, translate_target_lang)
        # Mask URLs để Google không bỏ qua dịch text xung quanh
        _url_ph = {}
        def _mask(m):
            t = f'__URL{len(_url_ph)}__'
            _url_ph[t] = m.group(0)
            return t
        masked = re.sub(r'https?://\S+', _mask, text)
        translated = GoogleTranslator(source='auto', target=_gtarget).translate(masked)
        if translated:
            for tok, url in _url_ph.items():
                translated = translated.replace(tok, url)
            return translated
        return text
    except Exception as e:
        err_str = str(e)
        if 'SSL' in err_str or 'EOF' in err_str or 'Max retries' in err_str:
            raise
        raise

def _translate_gemini(text, model='gemini-2.0-flash', api_key=None):
    """
    Dịch text bằng Gemini. api_key: key cụ thể (multi-key), None = dùng GEMINI_API_KEY.
    """
    _key = api_key or GEMINI_API_KEY
    if not _key:
        raise ValueError('GEMINI_API_KEY chưa set')
    _lang = _LANG_NAME.get(translate_target_lang, translate_target_lang)
    prompt = (
        f'Dịch đoạn văn bản sau sang {_lang} tự nhiên, giữ nguyên format xuống dòng, '
        'emoji và các ký tự đặc biệt. Chỉ trả về bản dịch, không giải thích thêm:\n\n' + text[:15000]
    )
    payload = json.dumps({
        'contents': [{'parts': [{'text': prompt}]}],
        'generationConfig': {'temperature': 0.1, 'maxOutputTokens': 8192},
    }).encode('utf-8')
    url = f'https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent?key={_key}'
    req = urllib.request.Request(url, data=payload, headers={'Content-Type': 'application/json'})
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read())
        result = data['candidates'][0]['content']['parts'][0]['text'].strip()
        if _is_error_result(result):
            raise RuntimeError(f'Gemini trả về error text: {result[:100]}')
        return result
    except urllib.error.HTTPError as e:
        body = e.read().decode('utf-8', errors='ignore')
        # Parse Retry-After nếu có để log chính xác
        retry_after = e.headers.get('Retry-After', '') if hasattr(e, 'headers') else ''
        ra_str = f' retry-after={retry_after}s' if retry_after else ''
        raise RuntimeError(f'Gemini HTTP {e.code}{ra_str}: {body[:200]}')

def _translate_gemini_html(html_text, model='gemini-2.0-flash', api_key=None):
    """
    Dịch HTML có link ẩn bằng Gemini — giữ nguyên tất cả thẻ HTML.
    api_key: key cụ thể (multi-key), None = dùng GEMINI_API_KEY.
    """
    _key = api_key or GEMINI_API_KEY
    if not _key:
        raise ValueError('GEMINI_API_KEY chưa set')
    _lang = _LANG_NAME.get(translate_target_lang, translate_target_lang)
    prompt = (
        f'Dịch đoạn HTML sau sang {_lang} tự nhiên, giữ nguyên format xuống dòng, '
        'emoji và các ký tự đặc biệt. '
        'QUAN TRỌNG: Giữ nguyên TOÀN BỘ các thẻ HTML sau đây — '
        'chỉ dịch nội dung text thuần, KHÔNG xóa, KHÔNG sửa, KHÔNG thêm bất kỳ thẻ nào: '
        '<a href="...">, <b>, </b>, <i>, </i>, <u>, </u>, <s>, </s>, <code>, </code>. '
        'Giữ nguyên dấu cách trước và sau mỗi thẻ HTML. '
        'Chỉ trả về bản dịch HTML, không giải thích thêm:\n\n' + html_text[:15000]
    )
    payload = json.dumps({
        'contents': [{'parts': [{'text': prompt}]}],
        'generationConfig': {'temperature': 0.1, 'maxOutputTokens': 8192},
    }).encode('utf-8')
    url = f'https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent?key={_key}'
    req = urllib.request.Request(url, data=payload, headers={'Content-Type': 'application/json'})
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read())
        result = data['candidates'][0]['content']['parts'][0]['text'].strip()
        # Bỏ markdown fence nếu model trả về ```html ... ```
        result = re.sub(r'^```html\s*', '', result)
        result = re.sub(r'^```\s*', '', result)
        result = re.sub(r'\s*```$', '', result).strip()
        # Nếu kết quả trông như error message thì raise để caller fallback
        if _is_error_result(result):
            raise RuntimeError(f'Gemini trả về error text: {result[:100]}')
        return result
    except urllib.error.HTTPError as e:
        body = e.read().decode('utf-8', errors='ignore')
        retry_after = e.headers.get('Retry-After', '') if hasattr(e, 'headers') else ''
        ra_str = f' retry-after={retry_after}s' if retry_after else ''
        raise RuntimeError(f'Gemini HTML HTTP {e.code}{ra_str}: {body[:200]}')


def _translate_deepl_html(html_text):
    """
    Dịch HTML có link ẩn bằng DeepL — dùng tag_handling=html để bảo toàn thẻ <a>.
    Chỉ gọi khi bản tin có MessageEntityTextUrl.
    """
    payload = urllib.parse.urlencode({
        'auth_key': DEEPL_API_KEY,
        'text': html_text[:4000],
        'target_lang': translate_target_lang.upper(),
        'source_lang': 'auto',
        'tag_handling': 'html',
    }).encode('utf-8')
    base = 'api-free.deepl.com' if DEEPL_API_KEY.endswith(':fx') else 'api.deepl.com'
    req = urllib.request.Request(
        f'https://{base}/v2/translate', data=payload,
        headers={'Content-Type': 'application/x-www-form-urlencoded'}
    )
    resp = urllib.request.urlopen(req, timeout=10)
    data = json.loads(resp.read())
    return data['translations'][0]['text'].strip()


def _fix_html_spacing(html):
    """
    Post-process HTML sau khi dịch (Gemini/DeepL) — đảm bảo có space
    trước opening tag và sau closing tag, tránh text bị dính vào tag.
    """
    if not html:
        return html
    # Space trước opening tag nếu ký tự trước không phải space/newline
    html = re.sub(
        r'([^ \n])(<(?!/)(?:b|i|u|s|a|code)(?:[\s>]))',
        r'\1 \2', html, flags=re.I
    )
    # Space sau closing tag nếu ký tự sau không phải space/newline/dấu câu
    html = re.sub(
        r'(</(?:b|i|u|s|a|code)>)([^ \n.,!?:;)\]])',
        r'\1 \2', html, flags=re.I
    )
    return html


def _translate_with_hidden_links(html_text):
    """
    Dịch bản tin có link ẩn / format HTML (bold, italic...).
    Dùng weighted round-robin multi-key qua _engine_dispatcher.
    Trả về (translated_text, engine_used, is_html).
    """
    if GEMINI_API_KEYS and _system_fully_loaded:
        try:
            result, alias = _gemini_translate_inner(html_text, is_html=True)
            return result, alias, True
        except RuntimeError as e:
            print(f'[Translate] Gemini HTML hết slot: {e}')

    # Thử DeepL
    if DEEPL_API_KEY:
        try:
            result = _fix_html_spacing(_translate_deepl_html(html_text))
            return result, 'deepl', True
        except Exception as e:
            print(f'[Translate] DeepL HTML lỗi: {e} — fallback Google HTML')

    # Fallback Google: dịch từng text segment, giữ nguyên tag HTML
    try:
        parts = re.split(r'(<[^>]+>)', html_text)
        result = []
        for i, part in enumerate(parts):
            if re.match(r'<[^>]+>', part):
                is_opening = not part.startswith('</')
                if is_opening and result and result[-1] and result[-1][-1] not in (' ', '\n'):
                    result.append(' ')
                result.append(part)
            elif part.strip():
                leading  = part[:len(part) - len(part.lstrip('\n'))]
                trailing = part[len(part.rstrip('\n')):]
                inner    = part.strip()
                import html as _html_mod
                inner = _html_mod.unescape(inner)
                _url_placeholders = {}
                def _mask_url(m):
                    token = f'__URL{len(_url_placeholders)}__'
                    _url_placeholders[token] = m.group(0)
                    return token
                inner_masked = re.sub(r'https?://\S+', _mask_url, inner)
                if result and re.match(r'</', result[-1]) and inner_masked and inner_masked[0] not in (' ', '\n') \
                        and inner_masked[0] not in '.,!?:;)】」』"\'':
                    inner_masked = ' ' + inner_masked
                try:
                    _gtarget = _GOOGLE_LANG_CODE.get(translate_target_lang, translate_target_lang)
                    translated_masked = GoogleTranslator(source='auto', target=_gtarget).translate(inner_masked)
                    translated_inner = translated_masked or inner_masked
                    for token, url in _url_placeholders.items():
                        translated_inner = translated_inner.replace(token, url)
                    result.append(leading + translated_inner + trailing)
                except Exception:
                    result.append(part)
            else:
                result.append(part)
        return _fix_html_spacing(''.join(result)), 'google', True
    except Exception as e:
        print(f'[Translate] Google HTML lỗi: {e} — fallback plain text')
        try:
            _plain = re.sub(r'<[^>]+>', '', html_text).strip()
            _translated_plain = _fast_translate(_plain) if _plain else _plain
            return _translated_plain, 'google', False
        except Exception as e2:
            print(f'[Translate] Fallback plain text lỗi: {e2} — giữ nguyên bản gốc (strip HTML)')
            _plain = re.sub(r'<[^>]+>', '', html_text).strip()
            return _plain if _plain else html_text, 'none', False


def strip_html(text):
    return re.sub(r'<[^>]+>', ' ', text).strip()

# Patterns lỗi HTTP/server thường bị lẫn vào kết quả dịch
_ERROR_RESULT_RE = re.compile(
    r'(Error\s*\d{3}|Server Error|Internal Server Error|'
    r'That\'s an error|Please try again later|'
    r'HTTP\s+\d{3}|\{"error":|"code":\s*[45]\d\d|'
    r'!+1[45]\d\d|ServiceUnavailable|Bad Gateway|Gateway Timeout)',
    re.I
)

def _is_error_result(text):
    """Trả về True nếu text trông như error message từ server, không phải bản dịch."""
    if not text:
        return False
    return bool(_ERROR_RESULT_RE.search(text[:400]))


def _expand_hidden_links_to_text(item):
    """
    Trả về HTML với <a href> nguyên vẹn (Telegram Bot API hỗ trợ parse_mode=HTML).
    Giữ các tag Telegram hỗ trợ: <a href>, <b>, <i>, <s>, <u>, <code>, <pre>.
    Strip các tag khác. Không dùng BS4, không đụng \\n.
    Trả về HTML string, hoặc None nếu item không có _tg_html_text.
    """
    html_src = item.get('_tg_html_text', '') or ''
    if not html_src:
        return None

    _allowed = re.compile(
        r'^</?(?:b|i|s|u|code|pre|a)(?:\s+href=(?:"[^"]*"|\'[^\']*\'))?>$',
        re.I
    )

    def _keep_or_strip(m):
        return m.group(0) if _allowed.match(m.group(0)) else ''

    result = re.sub(r'<[^>]+>', _keep_or_strip, html_src)
    # Unescape HTML entities: &amp; → &, &lt; → <, etc.
    import html as _html_mod
    result = _html_mod.unescape(result)
    return result.strip() or None

def is_same_as_target(text):
    """Kiểm tra xem text đã là ngôn ngữ đích chưa — nếu rồi thì skip dịch"""
    clean = strip_html(text)
    clean = re.sub(r'[^\w\s\u00C0-\u024F\u1E00-\u1EFF]', ' ', clean)[:400].strip()
    if not clean or len(clean) < 5:
        return True  # text quá ngắn, không cần dịch
    # Nếu target là tiếng Việt: dùng fast-check dấu thanh điệu
    if translate_target_lang == 'vi':
        viet_chars = set('àáâãèéêìíòóôõùúýăđơưạảấầẩẫậắằẳẵặẹẻẽếềểễệỉịọỏốồổỗộớờởỡợụủứừửữựỳỷỹỵ'
                         'ÀÁÂÃÈÉÊÌÍÒÓÔÕÙÚÝĂĐƠƯẠẢẤẦẨẪẬẮẰẲẴẶẸẺẼẾỀỂỄỆỈỊỌỎỐỒỔỖỘỚỜỞỠỢỤỦỨỪỬỮỰỲỶỸỴ')
        if sum(1 for c in clean if c in viet_chars) >= 3:
            return True
    # Fallback langdetect cho mọi ngôn ngữ
    try:
        detected = detect(clean)
        # Normalize: zh-cn/zh-tw → zh-CN; nb/nn → no
        target = translate_target_lang.lower()
        if target in ('zh-cn', 'zh-tw'):
            target = 'zh-cn'
        if detected in ('zh-cn', 'zh-tw'):
            detected = 'zh-cn'
        if target == 'no' and detected in ('no', 'nb', 'nn'):
            return True
        return detected == target
    except:
        return False  # không detect được → thử dịch

def translate_text(text):
    if not text or not TRANSLATE_AVAILABLE or not TRANSLATE_ENABLE:
        return text
    key = (hash(text), translate_engine)
    with translate_lock:
        if key in translate_cache:
            return translate_cache[key]
    try:
        # Tách HTML tags, dịch phần text, ghép lại
        parts = re.split(r'(<[^>]+>)', text)
        out, buf = [], []
        for part in parts:
            if re.match(r'<[^>]+>', part):
                if buf:
                    chunk = ' '.join(buf).strip()
                    if chunk and len(chunk) > 3:
                        out.append(_translate_with_engine(chunk))
                    elif chunk:
                        out.append(chunk)
                    buf = []
                out.append(part)
            else:
                if part.strip():
                    buf.append(part)
                else:
                    out.append(part)
        if buf:
            chunk = ' '.join(buf).strip()
            if chunk and len(chunk) > 3:
                out.append(_translate_with_engine(chunk))
            elif chunk:
                out.append(chunk)
        result = ''.join(out)
    except Exception as e:
        print(f'[!] Dịch lỗi: {e}')
        result = text
    with translate_lock:
        if len(translate_cache) > 150:
            keys = list(translate_cache.keys())
            for k in keys[:100]:
                del translate_cache[k]
        translate_cache[key] = result
    return result

def maybe_translate(title, desc):
    sample = strip_html(title + ' ' + desc)
    if is_same_as_target(sample):
        return title, desc, False
    return (translate_text(title) if title else title), (translate_text(desc) if desc else desc), True

def _fast_translate(text):
    """
    Dịch nhanh cho pipeline — không classification, không AI phức tạp.
    Dùng engine được chọn trong UI, fallback Google nếu lỗi.
    Bỏ qua nếu text đã là tiếng Việt.
    URLs và newlines được bảo toàn trước khi dịch.
    """
    if not text or len(text.strip()) < 4:
        return text
    if is_same_as_target(text[:400]):
        return text

    # Bước 1: Tách URL ra, thay bằng placeholder
    url_pattern = re.compile(r'https?://[^\s\)<>]+')
    urls = url_pattern.findall(text)
    placeholders = {}
    masked = text
    for i, url in enumerate(urls):
        placeholder = f'URLTOKEN{i}URLTOKEN'
        placeholders[placeholder] = url
        masked = masked.replace(url, placeholder, 1)

    # Bước 2: Bảo toàn newlines bằng NLTOKEN trước khi dịch
    NL_TOKEN = '⏎'   # dùng ký tự Unicode đặc biệt — engine không dịch ký tự này
    masked = masked.replace('\n', NL_TOKEN)

    translated, _used_engine = _dispatcher_translate(masked)
    _tl_engine.used = _used_engine  # lưu để caller có thể lấy

    # Bước 3: Khôi phục newlines
    translated = translated.replace(NL_TOKEN, '\n')

    # Bước 4: Khôi phục URLs
    for placeholder, url in placeholders.items():
        translated = translated.replace(placeholder, url)
        translated = translated.replace(placeholder.replace('URLTOKEN', ' URLTOKEN ').strip(), url)

    return translated

def _fast_translate_with_key(text, model_alias, api_key):
    """
    Dịch plain text bằng Gemini key cụ thể — dùng cho _process_batch.
    Tái dụng logic URL masking / NL_TOKEN từ _fast_translate.
    Raise exception để caller xử lý retry / fallback.
    """
    if not text or len(text.strip()) < 4:
        return text
    if is_same_as_target(text[:200]):
        return text

    url_pattern = re.compile(r'https?://[^\s\)<>]+')
    urls = url_pattern.findall(text)
    placeholders = {}
    masked = text
    for i, url in enumerate(urls):
        ph = f'URLTOKEN{i}URLTOKEN'
        placeholders[ph] = url
        masked = masked.replace(url, ph, 1)

    NL_TOKEN = '⏎'
    masked = masked.replace('\n', NL_TOKEN)

    model_name = _GPOOL_MODELS.get(model_alias, model_alias)
    result = _translate_gemini(masked, model=model_name, api_key=api_key)

    result = result.replace(NL_TOKEN, '\n')
    for ph, url in placeholders.items():
        result = result.replace(ph, url)

    return result


def extract_media(desc_html):
    if not desc_html:
        return [], []
    imgs = re.findall(r'<img[^>]+src=["\']([^"\']+)["\']', desc_html, re.I)
    links = re.findall(r'href=["\']([^"\']+)["\']', desc_html, re.I)
    videos = []
    for link in links:
        l = link.lower()
        if any(l.endswith(ext) for ext in ('.mp4', '.webm', '.mov', '.mkv')):
            videos.append(link)
    videos += re.findall(r'<(?:video|source)[^>]+src=["\']([^"\']+)["\']', desc_html, re.I)
    imgs = list(dict.fromkeys(imgs))
    videos = list(dict.fromkeys(videos))
    return imgs, videos

def run_tg_loop():
    global tg_loop, _tg_history_semaphore, tg_semaphore
    tg_loop = asyncio.new_event_loop()
    asyncio.set_event_loop(tg_loop)
    _tg_history_semaphore = asyncio.Semaphore(2)  # tối đa 2 history loads song song
    tg_semaphore = asyncio.Semaphore(1)            # serialize Telethon get_messages calls
    tg_loop.run_forever()

def tg_run(coro):
    """Chạy coroutine trên tg_loop từ thread khác, trả về kết quả"""
    global tg_loop, tg_loop_thread
    if tg_loop is None or not tg_loop.is_running():
        raise RuntimeError('Telethon loop chưa khởi động hoặc đã chết')
    future = asyncio.run_coroutine_threadsafe(coro, tg_loop)
    try:
        return future.result(timeout=30)
    except TimeoutError:
        future.cancel()
        raise RuntimeError('Telethon tg_run timeout sau 30s')

def save_tg_config(api_id, api_hash, phone):
    try:
        with open(TG_CONFIG_FILE, 'w') as f:
            json.dump({'api_id': api_id, 'api_hash': api_hash, 'phone': phone}, f)
    except:
        pass

def load_tg_config():
    try:
        if os.path.exists(TG_CONFIG_FILE):
            with open(TG_CONFIG_FILE) as f:
                return json.load(f)
    except:
        pass
    return None

async def _tg_send_code(api_id, api_hash, phone):
    global tg_client, tg_api_id, tg_api_hash, tg_phone, tg_auth_state, tg_auth_msg, tg_phone_code_hash
    tg_api_id   = int(api_id)
    tg_api_hash = api_hash
    tg_phone    = phone
    # Ưu tiên dùng StringSession từ env var (ổn định trên cloud, không bị mất khi restart)
    session_str = os.environ.get('TG_SESSION_STRING', '').strip()
    if session_str and TELETHON_AVAILABLE:
        session = StringSession(session_str)
    else:
        session = SESSION_FILE
    tg_client   = TelegramClient(session, tg_api_id, tg_api_hash)
    await tg_client.connect()
    if await tg_client.is_user_authorized():
        tg_auth_state = 'connected'
        tg_auth_msg   = 'Đã đăng nhập'
        save_tg_config(api_id, api_hash, phone)
        return {'ok': True, 'state': 'connected', 'msg': 'Đã đăng nhập sẵn'}
    result = await tg_client.send_code_request(phone)
    tg_phone_code_hash = result.phone_code_hash
    tg_auth_state = 'waiting_code'
    tg_auth_msg   = f'Đã gửi OTP đến {phone}'
    save_tg_config(api_id, api_hash, phone)
    return {'ok': True, 'state': 'waiting_code', 'msg': tg_auth_msg}

async def _tg_sign_in(code, password=None):
    global tg_auth_state, tg_auth_msg
    try:
        await tg_client.sign_in(tg_phone, code, phone_code_hash=tg_phone_code_hash)
        tg_auth_state = 'connected'
        tg_auth_msg   = 'Đăng nhập thành công'
        tg_urls = [u['url'] for u in watched_urls if is_tg_source(u['url'])]
        if tg_urls:
            await _tg_setup_realtime(tg_urls)
        return {'ok': True, 'state': 'connected', 'msg': tg_auth_msg}
    except telethon.errors.SessionPasswordNeededError:
        if password:
            await tg_client.sign_in(password=password)
            tg_auth_state = 'connected'
            tg_auth_msg   = 'Đăng nhập thành công (2FA)'
            tg_urls = [u['url'] for u in watched_urls if is_tg_source(u['url'])]
            if tg_urls:
                await _tg_setup_realtime(tg_urls)
            return {'ok': True, 'state': 'connected', 'msg': tg_auth_msg}
        else:
            tg_auth_state = 'waiting_2fa'
            tg_auth_msg   = 'Cần mật khẩu 2FA'
            return {'ok': True, 'state': 'waiting_2fa', 'msg': tg_auth_msg}
    except Exception as e:
        tg_auth_state = 'error'
        tg_auth_msg   = str(e)
        return {'ok': False, 'state': 'error', 'msg': str(e)}

async def _tg_sign_in_2fa(password):
    global tg_auth_state, tg_auth_msg
    try:
        await tg_client.sign_in(password=password)
        tg_auth_state = 'connected'
        tg_auth_msg   = 'Đăng nhập thành công (2FA)'
        tg_urls = [u['url'] for u in watched_urls if is_tg_source(u['url'])]
        if tg_urls:
            await _tg_setup_realtime(tg_urls)
        return {'ok': True, 'state': 'connected', 'msg': tg_auth_msg}
    except Exception as e:
        tg_auth_state = 'error'
        tg_auth_msg   = str(e)
        return {'ok': False, 'state': 'error', 'msg': str(e)}

async def _tg_check_auth():
    if tg_client and await tg_client.is_user_authorized():
        return True
    return False

# --- Hàng đợi tin mới từ Telethon event handler ---
tg_new_items_queue = []
tg_new_items_lock  = threading.Lock()

async def _tg_load_history(channel, limit=20):
    """Chỉ gọi 1 lần khi thêm kênh mới — load tin cũ để init known_guids"""
    async with _tg_history_semaphore:
        msgs = await tg_client.get_messages(channel, limit=limit)
    chat_username = channel.lstrip('@')
    items = []
    for msg in msgs:
        if not msg or not msg.message:
            continue
        guid = f'tg_{channel}_{msg.id}'
        link = f'https://t.me/{channel.lstrip("@")}/{msg.id}'
        desc = msg.message.replace('\n', '<br>')
        title = (msg.message[:80] + '...') if len(msg.message) > 80 else msg.message
        pub  = msg.date.isoformat() if msg.date else ''
        items.append({
            'guid': guid, 'title': title, 'desc': desc, 'link': link,
            'pubDate': pub, 'translated': False, 'category': '',
            '_tg_media_bytes': None, '_tg_msg_id': msg.id, '_tg_chat': chat_username,
            '_source': 'telethon',
        })
    return items

# Lưu handler để có thể remove sau
_tg_new_message_handler = None

# Cache các channel đã resolve thành công để không resolve lại mỗi lần WS reconnect
_resolved_channels_cache = set()

_tg_setup_running = threading.Event()  # Tránh concurrent setup gây duplicate handler (thread-safe)

async def _tg_setup_realtime(feed_urls):
    """
    Setup real-time listener cho danh sách feed URLs.
    - Resolve channel mới (chưa có trong cache) tuần tự với delay nhỏ
    - Luôn đăng ký handler với TOÀN BỘ channels (cached + newly resolved)
    - Lock để tránh 2 lần gọi song song gây duplicate handler
    """
    global _tg_new_message_handler, _resolved_channels_cache

    if not tg_client or not await tg_client.is_user_authorized():
        return

    # threading.Event — thread-safe, hoạt động cross-loop (asyncio.Lock không dùng được vì tg_loop riêng)
    if _tg_setup_running.is_set():
        print('[TG] Setup đang chạy, bỏ qua lần gọi này')
        return
    _tg_setup_running.set()
    try:
        # Xóa event handlers cũ
        try:
            for callback, event in tg_client.list_event_handlers():
                tg_client.remove_event_handler(callback, event)
        except Exception:
            pass
        _tg_new_message_handler = None

        if not feed_urls:
            return

        def _to_channel_id(u):
            n = normalize_tg_channel(u)
            stripped = n.lstrip('-')
            if stripped.isdigit() and len(stripped) >= 5:
                return int(n)  # numeric ID → int cho Telethon
            return n.lstrip('@')
        raw_channels = [_to_channel_id(u) for u in feed_urls]

        # Resolve các channel chưa có trong cache
        need_resolve = [ch for ch in raw_channels if ch not in _resolved_channels_cache]
        if need_resolve:
            print(f'[TG] Resolve {len(need_resolve)} channels mới...')
            for ch in need_resolve:
                try:
                    await tg_client.get_input_entity(ch)
                    _resolved_channels_cache.add(ch)
                    await asyncio.sleep(2.0)  # 2s/channel tránh FloodWait ResolveUsername
                except Exception as e:
                    err_str = str(e)
                    if 'FloodWait' in err_str or 'flood' in err_str.lower():
                        # Bị rate limit resolve → dừng lại, không resolve tiếp
                        print(f'[TG] FloodWait khi resolve — dùng cache hiện có: {e}')
                        break
                    print(f'[TG] Bỏ qua @{ch}: {e}')
                    await asyncio.sleep(2.0)

        # Luôn đăng ký với TẤT CẢ channels đã resolve thành công
        all_channels = [ch for ch in raw_channels if ch in _resolved_channels_cache]
        if all_channels:
            print(f'[TG] Đăng ký real-time đầy đủ: {len(all_channels)} channels')
            global _system_fully_loaded
            _system_fully_loaded = True
            print('[System] ✅ Fully loaded — Gemini enabled')
            await _register_handler(all_channels)
        else:
            print('[TG] Không có channel nào resolve được')
    finally:
        _tg_setup_running.clear()  # luôn release dù có exception

async def _register_handler(channels_list):
    """Đăng ký event handler cho danh sách channels"""
    global _tg_new_message_handler

    # Xóa handler cũ
    try:
        for callback, event in tg_client.list_event_handlers():
            tg_client.remove_event_handler(callback, event)
    except Exception:
        pass

    handled_groups = set()
    HANDLED_GROUPS_MAX = 500  # tránh leak bộ nhớ vô tận

    async def handler(event):
        msg = event.message
        # Bỏ qua nếu không có cả text lẫn media
        if not msg:
            return
        has_text  = bool(msg.message)
        has_media = bool(msg.media)
        if not has_text and not has_media:
            return
        chat_username = getattr(event.chat, 'username', None)
        if not chat_username:
            return
        if msg.grouped_id and msg.grouped_id in handled_groups:
            return

        feed_url = None
        category = ''
        with lock:
            for u in watched_urls:
                if is_tg_source(u['url']) and \
                   normalize_tg_channel(u['url']).lstrip('@').lower() == chat_username.lower():
                    feed_url = u['url']
                    category = u.get('category', '')
                    break
        if not feed_url:
            return

        if msg.grouped_id:
            if len(handled_groups) >= HANDLED_GROUPS_MAX:
                handled_groups.clear()
            handled_groups.add(msg.grouped_id)

        guid     = f'tg_@{chat_username}_{msg.id}'
        link     = f'https://t.me/{chat_username}/{msg.id}'
        msg_text = msg.message or ''  # an toàn khi tin chỉ có media, không có text
        desc     = msg_text  # giữ \n thật — nhất quán với read_all_bg
        title    = (msg_text[:80] + '...') if len(msg_text) > 80 else (msg_text or f'[Media] @{chat_username}')
        pub      = msg.date.isoformat() if msg.date else ''
        has_media = bool(msg.media and isinstance(msg.media, (MessageMediaPhoto, MessageMediaDocument)))

        # Detect link ẩn hoặc format entities (bold/italic/underline...)
        # → lưu HTML để dịch bảo toàn format
        from telethon.tl.types import (
            MessageEntityBold, MessageEntityItalic, MessageEntityUnderline,
            MessageEntityStrike, MessageEntityCode, MessageEntityPre,
        )
        _FORMAT_ENTITIES = (
            MessageEntityTextUrl, MessageEntityBold, MessageEntityItalic,
            MessageEntityUnderline, MessageEntityStrike, MessageEntityCode, MessageEntityPre,
        )
        _has_hidden_link = bool(
            msg_text and msg.entities and
            any(isinstance(e, MessageEntityTextUrl) for e in msg.entities)
        )
        _has_format = bool(
            msg_text and msg.entities and
            any(isinstance(e, _FORMAT_ENTITIES) for e in msg.entities)
        )
        _html_text = tg_html.unparse(msg_text, msg.entities) if (_has_hidden_link or _has_format) else ''

        item = {
            'guid': guid, 'title': title, 'desc': desc, 'link': link,
            'pubDate': pub, 'translated': False, 'category': category,
            '_tg_media_bytes': None, '_tg_has_media': has_media,
            '_tg_msg_id': msg.id, '_tg_chat': chat_username,
            '_tg_grouped_id': msg.grouped_id,
            '_source': 'telethon', '_feed_url': feed_url,
            '_tg_has_hidden_link': _has_hidden_link,
            '_tg_has_format': _has_format,
            '_tg_html_text': _html_text,
        }
        with tg_new_items_lock:
            tg_new_items_queue.append(item)
        print(f'[TG] Tin mới real-time: @{chat_username} #{msg.id}')
        # Xử lý ngay — không chờ poller 0.5s
        threading.Thread(target=_process_tg_queue, daemon=True).start()

    tg_client.add_event_handler(handler, events.NewMessage(chats=channels_list))
    _tg_new_message_handler = handler
    print(f'[TG] Đăng ký real-time cho: {channels_list}')

_tg_realtime_last_setup = 0.0

_tg_realtime_last_urls = set()  # track URLs đã setup để tránh re-register không cần thiết

def tg_setup_realtime_sync(feed_urls):
    """Wrapper đồng bộ để gọi từ thread thường — có debounce 120s + URL change check"""
    global _tg_realtime_last_setup, _tg_realtime_last_urls
    # Chặn ngay tại đây nếu đang có setup chạy — tránh queue 2 lần gọi
    if _tg_setup_running.is_set():
        print('[TG] tg_setup_realtime_sync: setup đang chạy, bỏ qua')
        return
    now = time.time()
    new_url_set = set(feed_urls)
    if now - _tg_realtime_last_setup < 300 and new_url_set == _tg_realtime_last_urls:
        return  # channels không thay đổi, bỏ qua (300s — đủ để lần đầu resolve 104 channels xong)
    if now - _tg_realtime_last_setup < 60:
        return  # hard minimum 60s
    _tg_realtime_last_urls = new_url_set
    _tg_realtime_last_setup = now
    if tg_loop and TELETHON_AVAILABLE:
        try:
            tg_run_long(_tg_setup_realtime(feed_urls), timeout=600)
        except Exception as e:
            print(f'[!] Setup real-time lỗi: {e}')

def tg_run_long(coro, timeout=60):
    """tg_run với timeout dài hơn — dùng cho history loading và realtime setup"""
    global tg_loop
    if tg_loop is None or not tg_loop.is_running():
        raise RuntimeError('Telethon loop chưa khởi động')
    future = asyncio.run_coroutine_threadsafe(coro, tg_loop)
    try:
        return future.result(timeout=timeout)
    except TimeoutError:
        future.cancel()
        raise RuntimeError(f'tg_run_long timeout sau {timeout}s')

def tg_load_history_sync(channel, limit=20):
    """Load lịch sử kênh TG — dùng cho poller, /tl_fetch endpoint và init feed mới"""
    return tg_run_long(_tg_load_history(channel, limit), timeout=60)

# Alias để không phải đổi các chỗ đang gọi tg_fetch_channel
tg_fetch_channel = tg_load_history_sync

# --- STATE ---
lock = threading.Lock()
watched_urls = []
known_guids = {}
ws_clients = set()
translate_enabled = TRANSLATE_ENABLE
auto_fwd_enabled = False
tg_channels = []
poll_next_time = time.time() + POLL_INTERVAL

# Dict track trạng thái cleanup topic
_cleanup_status   = {}  # key: channel_str → {status, deleted, total, error}
# Schedule tự động: key = "channel|topic_id" → {channel, topic_id, count, hour, minute, enabled}
_cleanup_schedules = {}  # lưu trong memory, persist qua Redis nếu có

def _persist_cleanup_schedules():
    """Lưu _cleanup_schedules vào Redis nếu có"""
    if _deduplicator:
        try:
            _deduplicator.r.set('cleanup_schedules',
                                json.dumps(list(_cleanup_schedules.values())).encode('utf-8'))
        except Exception as e:
            print(f'[Cleanup] Lưu schedule lỗi: {e}')

def _load_cleanup_schedules():
    """Load _cleanup_schedules từ Redis khi khởi động"""
    global _cleanup_schedules
    if _deduplicator:
        try:
            raw = _deduplicator.r.get('cleanup_schedules')
            if raw:
                items = json.loads(raw.decode('utf-8'))
                for s in items:
                    key = f'{s["channel"]}|{s["topic_id"]}'
                    _cleanup_schedules[key] = s
                print(f'[Cleanup] Load {len(_cleanup_schedules)} schedule(s) từ Redis')
        except Exception as e:
            print(f'[Cleanup] Load schedule lỗi: {e}')

def _run_cleanup_scheduler():
    """Background thread kiểm tra và chạy cleanup schedule hàng ngày"""
    import datetime
    print('[Cleanup] Scheduler thread started')
    while True:
        try:
            now = datetime.datetime.now()
            for key, s in list(_cleanup_schedules.items()):
                if not s.get('enabled'):
                    continue
                if now.hour == s.get('hour', 3) and now.minute == s.get('minute', 0):
                    last = s.get('last_run', '')
                    today = now.strftime('%Y-%m-%d')
                    if last == today:
                        continue  # đã chạy hôm nay rồi
                    print(f'[Cleanup] Auto-run: {s["channel"]} topic={s["topic_id"]} count={s["count"]}')
                    _cleanup_schedules[key]['last_run'] = today
                    _persist_cleanup_schedules()
                    # Tái dụng _run_cleanup_bg đã có sẵn
                    ch  = s['channel']
                    tid = s['topic_id']
                    cnt = s['count']
                    _cleanup_status[ch] = {'status': 'running', 'deleted': 0, 'total': 0, 'error': ''}

                    def _run_bg(ch_=ch, tid_=tid, cnt_=cnt):
                        async def _do():
                            try:
                                from telethon.tl.functions.channels import DeleteMessagesRequest as ChDel
                                from telethon.tl.functions.messages import DeleteMessagesRequest as MDel
                                dest         = await _resolve_dest(ch_)
                                entity       = await tg_client.get_entity(dest)
                                input_entity = await tg_client.get_input_entity(dest)
                                is_ch        = hasattr(entity, 'broadcast') or hasattr(entity, 'megagroup')
                                tid_int      = int(tid_)

                                all_ids = await _collect_topic_ids(entity, input_entity, tid_int)
                                all_ids.sort()
                                msg_ids = all_ids[:max(0, int(cnt_))]
                                assert len(msg_ids) <= int(cnt_), f'Safety check fail: {len(msg_ids)} > {cnt_}'
                                _cleanup_status[ch_]['total'] = len(msg_ids)
                                deleted = 0
                                for i in range(0, len(msg_ids), 100):
                                    batch = msg_ids[i:i+100]
                                    try:
                                        if is_ch:
                                            res = await tg_client(ChDel(channel=input_entity, id=batch))
                                            deleted += getattr(res, 'pts_count', len(batch)) or len(batch)
                                        else:
                                            await tg_client(MDel(id=batch, revoke=True))
                                            deleted += len(batch)
                                    except Exception as del_err:
                                        err_str = str(del_err)
                                        if 'FLOOD_WAIT' in err_str:
                                            import re as _re
                                            m = _re.search(r'FLOOD_WAIT_(\d+)', err_str)
                                            wait_sec = int(m.group(1)) if m else 30
                                            await asyncio.sleep(wait_sec + 2)
                                            if is_ch:
                                                res = await tg_client(ChDel(channel=input_entity, id=batch))
                                                deleted += getattr(res, 'pts_count', len(batch)) or len(batch)
                                            else:
                                                await tg_client(MDel(id=batch, revoke=True))
                                                deleted += len(batch)
                                        else:
                                            raise
                                    _cleanup_status[ch_]['deleted'] = deleted
                                    await asyncio.sleep(0.5)
                                remaining = max(0, len(all_ids) - deleted)
                                _cleanup_status[ch_].update({'status': 'done', 'deleted': deleted, 'remaining': remaining})
                                print(f'[Cleanup] Auto ✅ {ch_} topic={tid_}: xóa {deleted} tin, còn lại {remaining} tin')

                                # Force refresh Telegram UI sau khi xóa (cả toàn bộ lẫn một phần)
                                if deleted > 0:
                                    try:
                                        from telethon.tl.types import InputReplyToMessage
                                        sent = await tg_client.send_message(
                                            entity,
                                            '.',
                                            reply_to=InputReplyToMessage(
                                                reply_to_msg_id=tid_int,
                                                top_msg_id=tid_int,
                                            ),
                                            silent=True
                                        )
                                        await asyncio.sleep(0.5)
                                        await tg_client.delete_messages(entity, [sent.id], revoke=True)
                                        print(f'[Cleanup] 🔄 Auto force UI refresh topic={tid_} OK')
                                    except Exception as rf_err:
                                        print(f'[Cleanup] Auto force refresh warning: {rf_err}')
                            except Exception as e:
                                _cleanup_status[ch_].update({'status': 'error', 'error': str(e)})
                                print(f'[Cleanup] Auto ❌ {ch_} topic={tid_}: {e}')
                        try:
                            tg_run_long(_do(), timeout=600)
                        except Exception as e:
                            _cleanup_status[ch_].update({'status': 'error', 'error': str(e)})

                    threading.Thread(target=_run_bg, daemon=True, name='CleanupAutoRun').start()
        except Exception as e:
            print(f'[Cleanup] Scheduler lỗi: {e}')
        time.sleep(60)  # kiểm tra mỗi phút

threading.Thread(target=_run_cleanup_scheduler, daemon=True, name='CleanupScheduler').start()

# Set chứa các feed URL đã init xong known_guids — chỉ feed nào có trong set mới được forward
_forward_ready_feeds = set()
_system_fully_loaded = False   # True sau khi toàn bộ TG history load xong
_watchdog_running = False  # Tránh watchdog chạy trùng

# --- WebSocket thuần RFC 6455 (không cần thư viện websockets) ---

class WsConn:
    """WebSocket connection wrapper dùng raw socket, tương thích với ws_handler cũ."""
    def __init__(self, sock):
        self._sock = sock
        self._send_lock = threading.Lock()
        self._closed = False

    def send(self, text):
        if self._closed:
            raise ConnectionError('closed')
        payload = text.encode('utf-8')
        n = len(payload)
        with self._send_lock:
            try:
                if n < 126:
                    header = bytes([0x81, n])
                elif n < 65536:
                    header = bytes([0x81, 126]) + struct.pack('>H', n)
                else:
                    header = bytes([0x81, 127]) + struct.pack('>Q', n)
                self._sock.sendall(header + payload)
            except Exception:
                self._closed = True
                raise

    def _recvexact(self, n):
        buf = b''
        while len(buf) < n:
            chunk = self._sock.recv(n - len(buf))
            if not chunk:
                raise ConnectionError('disconnected')
            buf += chunk
        return buf

    def recv(self):
        """Đọc một WebSocket frame, trả về string hoặc None nếu đóng."""
        try:
            # Timeout 120s — đủ lâu để server bận init 45 feeds mà không bị cắt
            self._sock.settimeout(120)
            
            header = self._recvexact(2)
            opcode = header[0] & 0x0f
            masked = bool(header[1] & 0x80)
            length = header[1] & 0x7f
            if length == 126:
                length = struct.unpack('>H', self._recvexact(2))[0]
            elif length == 127:
                length = struct.unpack('>Q', self._recvexact(8))[0]
            mask_key = self._recvexact(4) if masked else b'\x00\x00\x00\x00'
            data = bytearray(self._recvexact(length))
            if masked:
                for i in range(length):
                    data[i] ^= mask_key[i % 4]
            
            # Handle close frame
            if opcode == 0x8:
                print(f'[WS] Frame close nhận được')
                return None
            # Handle ping → reply pong
            if opcode == 0x9:
                with self._send_lock:
                    try:
                        self._sock.sendall(bytes([0x8a, len(data)]) + bytes(data))
                    except Exception:
                        pass
                return self.recv()
            return data.decode('utf-8', errors='replace')
        except socket.timeout:
            # Timeout không phải disconnect — server đang bận, thử lại
            return ''   # trả về chuỗi rỗng, ws_handler sẽ bỏ qua
        except (ConnectionError, OSError, BrokenPipeError):
            print(f'[WS] recv disconnected')
            return None
        except Exception as e:
            print(f'[WS] recv lỗi: {type(e).__name__}: {e}')
            return None

    def close(self):
        self._closed = True
        try:
            self._sock.close()
        except Exception:
            pass

    def __iter__(self):
        while True:
            msg = self.recv()
            if msg is None:
                break
            if msg == '':
                continue  # socket.timeout — không phải disconnect, chờ tiếp
            yield msg


def _ws_handshake(request_handler):
    """
    Thực hiện WebSocket upgrade từ BaseHTTPRequestHandler.
    Trả về WsConn nếu thành công, None nếu thất bại.
    """
    key = request_handler.headers.get('Sec-WebSocket-Key', '').strip()
    if not key:
        print('[WS] Handshake thất bại: không có Sec-WebSocket-Key')
        return None
    accept = base64.b64encode(
        hashlib.sha1((key + '258EAFA5-E914-47DA-95CA-C5AB0DC85B11').encode()).digest()
    ).decode()
    response = (
        'HTTP/1.1 101 Switching Protocols\r\n'
        'Upgrade: websocket\r\n'
        'Connection: Upgrade\r\n'
        f'Sec-WebSocket-Accept: {accept}\r\n\r\n'
    )
    try:
        orig_sock = request_handler.connection
        orig_sock.sendall(response.encode())
        # Duplicate socket — WS thread dùng bản copy, HTTP handler dùng bản gốc
        # HTTP handler sẽ tự close bản gốc khi xong, WS vẫn có bản copy
        ws_sock = orig_sock.dup()
        ws_sock.settimeout(None)
        print('[WS] Handshake thành công')
        return WsConn(ws_sock)
    except Exception as e:
        print(f'[WS] Handshake lỗi: {e}')
        return None



# --- LOGIN PAGE ---
_LOGIN_HTML = """<!DOCTYPE html>
<html lang="vi">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Đăng nhập — RSS Reader</title>
<style>
*{box-sizing:border-box;margin:0;padding:0}
body{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;background:#f5f5f0;min-height:100vh;display:flex;align-items:center;justify-content:center}
.card{background:#fff;border-radius:12px;box-shadow:0 4px 24px rgba(0,0,0,.10);padding:40px 36px;width:100%;max-width:360px}
h2{font-size:20px;font-weight:700;color:#1a1a1a;margin-bottom:6px;text-align:center}
.sub{font-size:13px;color:#888;text-align:center;margin-bottom:28px}
label{display:block;font-size:12px;font-weight:600;color:#555;margin-bottom:5px}
input{width:100%;padding:10px 12px;border:1.5px solid #e0e0d8;border-radius:7px;font-size:14px;outline:none;transition:border .15s}
input:focus{border-color:#2563eb}
.field{margin-bottom:16px}
.btn{width:100%;padding:11px;background:#2563eb;color:#fff;border:none;border-radius:7px;font-size:15px;font-weight:600;cursor:pointer;margin-top:8px;transition:background .15s}
.btn:hover{background:#1d4ed8}
.btn:disabled{background:#93c5fd;cursor:not-allowed}
.err{color:#dc2626;font-size:13px;margin-top:12px;text-align:center;min-height:20px}
.logo{text-align:center;font-size:32px;margin-bottom:12px}
</style>
</head>
<body>
<div class="card">
  <div class="logo">📡</div>
  <h2>RSS Reader</h2>
  <p class="sub">Vui lòng đăng nhập để tiếp tục</p>
  <div class="field">
    <label>Tên đăng nhập</label>
    <input type="text" id="usr" autocomplete="username" placeholder="Username" onkeydown="if(event.key==='Enter')doLogin()">
  </div>
  <div class="field">
    <label>Mật khẩu</label>
    <input type="password" id="pwd" autocomplete="current-password" placeholder="Password" onkeydown="if(event.key==='Enter')doLogin()">
  </div>
  <button class="btn" id="btn" onclick="doLogin()">Đăng nhập</button>
  <div class="err" id="err"></div>
</div>
<script>
async function doLogin(){
  const btn=document.getElementById('btn');
  const err=document.getElementById('err');
  const usr=document.getElementById('usr').value.trim();
  const pwd=document.getElementById('pwd').value;
  if(!usr||!pwd){err.textContent='Vui lòng nhập đầy đủ';return;}
  btn.disabled=true; btn.textContent='Đang đăng nhập...'; err.textContent='';
  try{
    const r=await fetch('/login',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({username:usr,password:pwd})});
    const d=await r.json();
    if(d.ok){window.location.reload();}
    else{err.textContent=d.error||'Sai thông tin đăng nhập';btn.disabled=false;btn.textContent='Đăng nhập';}
  }catch(e){err.textContent='Lỗi kết nối';btn.disabled=false;btn.textContent='Đăng nhập';}
}
document.getElementById('usr').focus();
</script>
<!-- Error notification banner -->
<div class="err-banner" id="err-banner">
  <span class="eb-close" onclick="closeErrBanner()">×</span>
  <div class="eb-title"><span>⚠️</span><span id="eb-title-text">Lỗi hệ thống</span></div>
  <div class="eb-msg" id="eb-msg"></div>
  <div class="eb-actions">
    <button class="eb-btn" onclick="closeErrBanner()">Đóng</button>
    <button class="eb-btn" id="eb-del-btn" style="display:none" onclick="deleteErrFeed()">Xóa feed này</button>
  </div>
</div>
</body>
</html>"""

HTML = r"""<!DOCTYPE html>
<html lang="vi">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>RSS Reader</title>
<style>
*{box-sizing:border-box;margin:0;padding:0}
body{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;background:#f5f5f0;color:#1a1a1a;min-height:100vh;display:flex;flex-direction:column}
.top-fixed{position:sticky;top:0;z-index:20;display:flex;flex-direction:column}
header{background:#fff;border-bottom:1px solid #e0e0d8;padding:.7rem 1.5rem;display:flex;align-items:center;gap:10px;flex-wrap:wrap}
header h1{font-size:1rem;font-weight:700;white-space:nowrap;margin-right:4px}
.tg-bar{background:#fffbeb;border-bottom:1px solid #fde68a;padding:.55rem 1.5rem;display:flex;align-items:center;gap:8px;flex-wrap:wrap}
.tg-bar label{font-size:12px;color:#92400e;font-weight:700;white-space:nowrap}
.tg-bar input,.tg-bar select{padding:5px 9px;border:1px solid #fcd34d;border-radius:7px;font-size:12px;outline:none;background:#fff;color:#1a1a1a}
.tg-save{padding:5px 12px;background:#f59e0b;color:#fff;border:none;border-radius:7px;font-size:12px;cursor:pointer;font-weight:600}
.auto-fwd{display:flex;align-items:center;gap:6px;padding:4px 10px;border:1px solid #e0e0d8;border-radius:7px;background:#fff;cursor:pointer;font-size:12px;color:#555}
.auto-fwd:hover{background:#f5f5f0}
.auto-fwd.active{border-color:#22c55e;color:#166534;background:#f0fdf4}
.adot{width:8px;height:8px;border-radius:50%;background:#ccc}
.adot.on{background:#22c55e}
.ws-status{display:flex;align-items:center;gap:6px;font-size:12px;color:#aaa}
.ws-dot{width:7px;height:7px;border-radius:50%;background:#ccc}
.ws-dot.on{background:#22c55e}
.fwd-bar{background:#eff6ff;border:1px solid #bfdbfe;border-radius:8px;padding:6px 12px;display:flex;align-items:center;gap:10px}
.fwd-bar.hidden{display:none}
.fwd-count{font-size:13px;font-weight:600;color:#1d4ed8}
.btn-fwd{padding:6px 16px;background:#2563eb;color:#fff;border:none;border-radius:8px;font-size:13px;cursor:pointer}
.layout{display:flex;flex:1;min-height:0;background:#fff}
aside{width:240px;flex-shrink:0;background:#fff;border-right:1px solid #e0e0d8;display:flex;flex-direction:column;overflow:hidden;min-height:0}
.feed-list{flex:1;overflow-y:auto;background:#fff;min-height:0;padding-bottom:100px}
.feed-row{display:flex;align-items:center;padding:7px 10px 7px 14px;cursor:pointer;gap:8px;font-size:12px;background:#fff;min-width:0}
.sb-section{padding:.35rem .75rem .15rem;font-size:11px;font-weight:600;color:#bbb;text-transform:uppercase;letter-spacing:.5px;background:#fff}
.feed-row:hover{background:#f8f8f4}
.feed-row.active{background:#f0f0e8;font-weight:600}
.feed-row .fname{font-size:12px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;min-width:0}
.err-banner{position:fixed;bottom:16px;right:16px;max-width:340px;background:#fef2f2;border:1.5px solid #fca5a5;border-radius:10px;padding:12px 14px;box-shadow:0 4px 16px rgba(0,0,0,.12);z-index:9999;font-size:13px;color:#991b1b;display:none;animation:slideUp .25s ease}
.err-banner .eb-title{font-weight:700;margin-bottom:4px;display:flex;align-items:center;gap:6px}
.err-banner .eb-msg{color:#7f1d1d;font-size:12px;word-break:break-word}
.err-banner .eb-close{position:absolute;top:8px;right:10px;cursor:pointer;color:#aaa;font-size:16px;line-height:1}
.err-banner .eb-actions{margin-top:8px;display:flex;gap:8px}
.err-banner .eb-btn{font-size:11px;padding:3px 10px;border-radius:5px;border:1px solid #fca5a5;background:#fff;cursor:pointer;color:#991b1b}
.err-banner .eb-btn:hover{background:#fef2f2}
@keyframes slideUp{from{transform:translateY(20px);opacity:0}to{transform:translateY(0);opacity:1}}
.fdel{color:#ddd;font-size:14px}.fdel:hover{color:#e74c3c}
.fedit{color:#ddd;font-size:14px}.fedit:hover{color:#2563eb}
.item{background:#fff;border:1px solid #e8e8e0;border-radius:10px;margin-bottom:.6rem}
.item.selected{border-color:#2563eb}
.item-header{padding:.85rem;cursor:pointer;display:flex;justify-content:space-between}
.item-title{font-size:.88rem;font-weight:600}
.item-title a{color:inherit;text-decoration:none}
.item-title a:hover{color:#555}
.item-body{display:none;padding:0 1.1rem .9rem}
.item-body.open{display:block}
.item-footer{padding:0 1.1rem .75rem}
.item-footer a{font-size:12px;color:#2563eb;text-decoration:none}
.item-footer a:hover{text-decoration:underline}
.modal-bg{display:none;position:fixed;inset:0;background:rgba(0,0,0,.3);z-index:100;align-items:center;justify-content:center}
.modal-bg.open{display:flex}
.modal{background:#fff;border-radius:14px;padding:1.5rem;width:460px;max-width:92vw}
.modal h2{font-size:1rem;font-weight:600;margin-bottom:1rem}
.modal input,.modal select{width:100%;padding:9px;border:1px solid #d0d0c8;border-radius:8px;margin-bottom:.6rem;font-size:14px}
.modal-btns{display:flex;gap:8px;justify-content:flex-end;margin-top:.5rem}
.modal-btns button{padding:8px 18px;border-radius:8px;cursor:pointer;border:1px solid #d0d0c8;background:#fff;font-size:13px}
.modal-btns .btn-ok{background:#1a1a1a;color:#fff;border-color:#1a1a1a}
#toast{position:fixed;bottom:1.5rem;right:1.5rem;z-index:200}
.toast-item{background:#1a1a1a;color:#fff;padding:10px 16px;border-radius:10px;margin-top:8px;font-size:13px}
.toolbar{padding:.6rem 1.5rem;background:#fff;border-bottom:1px solid #e0e0d8;display:flex;align-items:center;gap:10px;flex-wrap:wrap}
.toolbar-left{display:flex;align-items:center;gap:10px;flex:1}
.toolbar-right{display:flex;align-items:center;gap:10px}
#view-label{font-size:13px;font-weight:700}
.btn-sm{padding:5px 12px;border:1px solid #d0d0c8;border-radius:7px;background:#fff;cursor:pointer;font-size:12px;color:#555}
.btn-sm:hover{background:#f5f5f0}
.category-item{display:flex;align-items:center;gap:6px;padding:4px 8px;background:#f9f9f9;border-radius:6px;margin-bottom:4px}
.category-item span{flex:1;font-size:12px}
.category-item button{padding:2px 8px;font-size:11px;border:1px solid #ddd;background:#fff;border-radius:4px;cursor:pointer}
.category-item button:hover{background:#f0f0f0}
#item-count{font-size:12px;color:#aaa}
#poll-timer{font-size:11px;color:#bbb;margin-left:8px}
/* Ảnh trong item body luôn xuống hàng */
[id^="b"] img{display:block;max-width:100%;height:auto;margin:8px 0;border-radius:6px}
[id^="b"] br{display:block;content:"";margin:2px 0}
.item-body-div img{display:block;max-width:100%;height:auto;margin:8px 0;border-radius:6px}
.item-body-div br{display:block;content:"";margin:2px 0}
.item-body-div p{margin:4px 0}
/* Telethon auth */
.tg-auth-box{background:#f0fdf4;border:1px solid #bbf7d0;border-radius:10px;padding:14px 18px;margin-bottom:10px}
.tg-auth-box.error{background:#fef2f2;border-color:#fecaca}
.tg-auth-box.warn{background:#fffbeb;border-color:#fde68a}
.auth-step{display:none}.auth-step.active{display:block}
.feed-badge{display:inline-block;padding:1px 5px;border-radius:6px;font-size:10px;font-weight:600;margin-left:4px;vertical-align:middle}
.badge-tg{background:#dbeafe;color:#1d4ed8}
.badge-rss{background:#dcfce7;color:#166534}
@keyframes spin{to{transform:rotate(360deg)}}
</style>
</head>
<body>
<div class="top-fixed" id="top-fixed">
<header>
<h1>RSS Reader</h1>
<div class="auto-fwd" id="auto-fwd-btn" onclick="toggleAutoFwd()">
<div class="adot" id="afwd-dot"></div><span>Auto-forward</span>
</div>
<div class="ws-status"><div class="ws-dot wait" id="ws-dot"></div><span id="ws-lbl">Đang kết nối...</span><span id="poll-timer"></span></div>
<button id="logout-btn" onclick="doLogout()" title="Đăng xuất" style="display:none;background:none;border:1px solid #ddd;border-radius:6px;padding:4px 10px;font-size:12px;cursor:pointer;color:#666;margin-left:8px">⏻ Đăng xuất</button>
</header>
<div class="tg-bar">
<label>Telegram</label>
<button class="tg-save" onclick="openChannelManager()">Quản lý kênh</button>
<button class="tg-save" style="background:#6366f1" onclick="openTelethonModal()">⚡ Telethon</button>
<span id="tg-saved" style="display:none;color:#16a34a;font-size:11px">✓ Đã lưu</span>
<span id="tg-auth-badge" style="font-size:11px;color:#aaa">Telethon: chưa kết nối</span>
<span style="display:flex;align-items:center;gap:6px;font-size:12px;color:#374151;font-weight:600">
  🗣 Ngôn ngữ:
  <select id="lang-select" onchange="setTranslateLang(this.value)"
    style="padding:4px 8px;border:1px solid #d1d5db;border-radius:7px;font-size:12px;background:#fff;cursor:pointer">
    <option value="vi">🇻🇳 Tiếng Việt</option>
    <option value="en">🇬🇧 English</option>
    <option value="zh-CN">🇨🇳 中文简体 (Chinese Simplified)</option>
    <option value="ja">🇯🇵 日本語 (Japanese)</option>
    <option value="ko">🇰🇷 한국어 (Korean)</option>
    <option value="fr">🇫🇷 Français (French)</option>
    <option value="de">🇩🇪 Deutsch (German)</option>
    <option value="es">🇪🇸 Español (Spanish)</option>
    <option value="pt">🇵🇹 Português (Portuguese)</option>
    <option value="it">🇮🇹 Italiano (Italian)</option>
    <option value="ru">🇷🇺 Русский (Russian)</option>
    <option value="ar">🇸🇦 العربية (Arabic)</option>
    <option value="hi">🇮🇳 हिन्दी (Hindi)</option>
    <option value="th">🇹🇭 ภาษาไทย (Thai)</option>
    <option value="id">🇮🇩 Bahasa Indonesia</option>
    <option value="ms">🇲🇾 Bahasa Melayu</option>
    <option value="nl">🇳🇱 Nederlands (Dutch)</option>
    <option value="pl">🇵🇱 Polski (Polish)</option>
    <option value="sv">🇸🇪 Svenska (Swedish)</option>
    <option value="tr">🇹🇷 Türkçe (Turkish)</option>
    <option value="uk">🇺🇦 Українська (Ukrainian)</option>
    <option value="cs">🇨🇿 Čeština (Czech)</option>
    <option value="ro">🇷🇴 Română (Romanian)</option>
    <option value="hu">🇭🇺 Magyar (Hungarian)</option>
    <option value="el">🇬🇷 Ελληνικά (Greek)</option>
    <option value="da">🇩🇰 Dansk (Danish)</option>
    <option value="fi">🇫🇮 Suomi (Finnish)</option>
    <option value="no">🇳🇴 Norsk (Norwegian)</option>
    <option value="sk">🇸🇰 Slovenčina (Slovak)</option>
    <option value="bg">🇧🇬 Български (Bulgarian)</option>
    <option value="hr">🇭🇷 Hrvatski (Croatian)</option>
    <option value="lt">🇱🇹 Lietuvių (Lithuanian)</option>
    <option value="lv">🇱🇻 Latviešu (Latvian)</option>
    <option value="et">🇪🇪 Eesti (Estonian)</option>
    <option value="sl">🇸🇮 Slovenščina (Slovenian)</option>
  </select>
</span>
<span style="margin-left:auto;display:flex;align-items:center;gap:6px;font-size:12px;color:#92400e;font-weight:600">
  🌐 Dịch:
  <select id="engine-select" onchange="setTranslateEngine(this.value)"
    style="padding:4px 8px;border:1px solid #fcd34d;border-radius:7px;font-size:12px;background:#fff;cursor:pointer">
    <option value="gemini">✨ Gemini 2.0 Flash</option>
    <option value="deepl">DeepL</option>
    <option value="google">Google Translate</option>
  </select>
  <span id="notify-bell-btn" title="Test gửi thông báo lỗi tới Telegram" onclick="testNotify()"
    style="cursor:pointer;font-size:17px;line-height:1;padding:3px 5px;border-radius:6px;transition:background .15s;user-select:none"
    onmouseover="this.style.background='#fef3c7'" onmouseout="this.style.background='transparent'">🔔</span>
</span>
</div>
</div>
<div class="layout">
<aside id="aside">
<div style="padding:.75rem;border-bottom:1px solid #f0f0e8">
<button onclick="openModal()" style="width:100%;padding:7px;background:#1a1a1a;color:#fff;border:none;border-radius:8px;cursor:pointer;font-size:13px">+ Thêm feed</button>
<input type="text" id="search" placeholder="Tìm kiếm..." oninput="applyFilter()" style="width:100%;padding:6px;margin-top:6px;border:1px solid #e0e0d8;border-radius:8px;font-size:13px">
</div>
<div style="padding:.35rem .75rem .2rem;background:#fff;border-bottom:1px solid #f0f0e8;display:flex;align-items:center;gap:6px">
  <span style="font-size:11px;color:#bbb;flex:1">Nguồn</span>
  <select id="feed-sort" onchange="renderSidebar()" title="Sắp xếp" style="font-size:11px;border:1px solid #e0e0d8;border-radius:5px;padding:2px 4px;color:#666;background:#fff;cursor:pointer">
    <option value="default">Mặc định</option>
    <option value="activity">Hoạt động</option>
    <option value="name">Tên A-Z</option>
    <option value="stale">Feed chết trước</option>
  </select>
</div>
<input type="text" id="feed-search" placeholder="🔍 Tìm nguồn..." oninput="renderSidebar()"
  style="width:calc(100% - 16px);margin:6px 8px 4px;padding:5px 8px;border:1px solid #e0e0d8;border-radius:7px;font-size:12px;outline:none;box-sizing:border-box">
<div id="feed-list" class="feed-list"></div>
</aside>
<main style="flex:1;min-width:0;display:flex;flex-direction:column">
<div class="toolbar">
<div class="toolbar-left">
<span id="view-label">Tất cả nguồn</span>
</div>
<div class="toolbar-right">
<div class="fwd-bar hidden" id="fwd-bar">
<span id="fwd-count">0 tin</span>
<button class="btn-fwd" onclick="openForwardModal()">Gửi Telegram</button>
<button onclick="clearSelection()" class="btn-sm">Bỏ chọn</button>
</div>
<button onclick="manualRefreshAll()" class="btn-sm">Refresh</button>
<button onclick="openCleanupModal()" title="Dọn bài trong topic" class="btn-sm" style="padding:4px 8px;font-size:15px;background:none;border:1px solid #e0e0d8;border-radius:7px;cursor:pointer;line-height:1">🗑️</button>
<span id="item-count"></span>
</div>
</div>
<div id="stream" style="flex:1;overflow-y:auto;padding:1rem 1.5rem;background:#fff"></div>
</main>
</div>

<!-- Modal thêm/sửa feed -->
<div class="modal-bg" id="modal">
<div class="modal" style="max-height:90vh;overflow-y:auto">
<div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:1rem">
  <h2 id="modal-title" style="margin-bottom:0">Thêm feed</h2>
  <label style="display:flex;align-items:center;gap:6px;font-size:12px;cursor:pointer;background:#f0fdf4;border:1px solid #bbf7d0;padding:4px 10px;border-radius:8px" id="auto-fwd-toggle-wrap">
    <input type="checkbox" id="new-auto-fwd" style="width:14px;height:14px;cursor:pointer">
    <span>Auto-forward</span>
  </label>
</div>
<input type="text" id="new-name" placeholder="Tên feed">
<input type="text" id="new-url" placeholder="URL RSS hoặc @username / t.me/channel">
<div id="url-duplicate-warn" style="display:none;color:#dc2626;font-size:12px;margin-bottom:6px;padding:6px 10px;background:#fef2f2;border-radius:6px;border:1px solid #fecaca">
  ⚠️ Feed này đã tồn tại trong danh sách: <span id="url-dup-name"></span>
</div>
<div id="feed-type-hint" style="font-size:11px;color:#6366f1;margin-bottom:8px;display:none">⚡ Nguồn Telegram — sẽ dùng Telethon</div>
<!-- Checkboxes hàng ngang -->
<div style="display:flex;gap:16px;margin-bottom:10px;flex-wrap:wrap">
  <label style="display:flex;align-items:center;gap:6px;font-size:13px;cursor:pointer">
    <input type="checkbox" id="new-show-link" checked style="width:15px;height:15px;cursor:pointer">
    Hiển thị link gốc
  </label>
  <label style="display:flex;align-items:center;gap:6px;font-size:13px;cursor:pointer">
    <input type="checkbox" id="new-translate" checked style="width:15px;height:15px;cursor:pointer">
    Dịch nội dung
  </label>
</div>
<div id="read-all-wrap" style="display:none;margin-bottom:10px;padding:8px 12px;background:#fffbeb;border:1px solid #fde68a;border-radius:8px">
  <label style="display:flex;align-items:center;gap:8px;font-size:13px;cursor:pointer">
    <input type="checkbox" id="new-read-all" style="width:15px;height:15px;cursor:pointer">
    <div>
      <div style="font-weight:600;color:#92400e">📚 Đọc toàn bộ nguồn</div>
      <div style="font-size:11px;color:#b45309;margin-top:2px">Lấy tất cả tin từ cũ đến mới, delay random 5-20s/tin, forward tới kênh đích</div>
    </div>
  </label>
</div>
<div style="margin-bottom:10px">
  <label style="font-size:12px;color:#555;font-weight:600;display:block;margin-bottom:4px">Số bài lịch sử lấy về khi khởi động:</label>
  <input type="number" id="new-history-limit" min="0" max="200" value="20"
    placeholder="0 = không lấy lịch sử"
    style="width:100%;padding:7px;border:1px solid #d0d0c8;border-radius:8px;font-size:13px">
  <div style="font-size:11px;color:#aaa;margin-top:3px">Nhập 0 để không lấy lịch sử. Tối đa 200 bài.</div>
</div>
<!-- Đích forward — hiển thị khi có kênh -->
<div id="feed-dest-wrap" style="display:none;margin-bottom:10px">
  <div style="font-size:12px;color:#555;font-weight:600;margin-bottom:6px">Đích forward:</div>
  <div id="feed-dest-list" style="border:1px solid #e0e0d8;border-radius:8px;background:#fff;padding:4px 0"></div>
</div>
<div class="modal-btns">
<button onclick="closeModal()">Hủy</button>
<button class="btn-ok" id="btn-add-feed" onclick="addFeed()">Thêm</button>
</div>
</div>
</div>

<!-- Modal quản lý kênh Telegram -->
<div class="modal-bg" id="channel-modal">
<div class="modal" style="max-height:90vh;overflow-y:auto;display:flex;flex-direction:column">
<h2>Quản lý Kênh / Group Telegram</h2>
<div style="background:#eff6ff;border:1px solid #bfdbfe;border-radius:8px;padding:8px 12px;margin-bottom:10px;font-size:12px;color:#1e40af">
  ⚡ Dùng <b>Telethon</b> để gửi — nhập <b>@username</b> hoặc <b>-100xxx</b>. Topic IDs được cấu hình trong từng feed.
</div>
<select id="ch-select-modal" onchange="loadChannelToForm(this.value)"></select>
<input type="text" id="ch-username" placeholder="@kênh_đích hoặc -100xxxxxxxxx (Group ID)">
<input type="text" id="ch-name" placeholder="Tên hiển thị">
<label style="display:flex;align-items:center;gap:8px;font-size:13px;cursor:pointer;margin-bottom:10px">
  <input type="checkbox" id="ch-is-group" style="width:15px;height:15px;cursor:pointer" onchange="toggleTopicSection()">
  Là Group/Supergroup (có Topic IDs)
</label>

<!-- Quản lý Topic IDs — chỉ hiện khi là Group -->
<div id="ch-topic-section" style="display:none;border:1px solid #e0e0d8;border-radius:8px;padding:10px;margin-bottom:12px;background:#f8f8f4">
  <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:8px">
    <span style="font-size:12px;font-weight:600;color:#555">📋 Danh sách Topics</span>
    <button type="button" onclick="chAddTopicRow()" style="font-size:12px;padding:3px 10px;border:1px solid #2563eb;border-radius:6px;background:#eff6ff;color:#2563eb;cursor:pointer">+ Thêm topic</button>
  </div>
  <div id="ch-topic-list" style="display:flex;flex-direction:column;gap:6px"></div>
</div>

<div class="modal-btns">
<button id="btn-ch-del" onclick="deleteChannel()" style="display:none">Xóa</button>
<button class="btn-ok" onclick="saveTgSettings()">Lưu</button>
<button onclick="closeChannelManager()">Đóng</button>
</div>
</div>
</div>

<!-- Modal Telethon Login -->
<div class="modal-bg" id="telethon-modal">
<div class="modal" style="width:500px">
<h2>⚡ Kết nối Telethon</h2>
<div id="tg-status-box" class="tg-auth-box warn" style="display:none"></div>

<!-- Bước 1: nhập API credentials + số điện thoại -->
<div class="auth-step active" id="step-creds">
<input type="text" id="tl-api-id" placeholder="API ID (từ my.telegram.org)">
<input type="text" id="tl-api-hash" placeholder="API Hash">
<input type="text" id="tl-phone" placeholder="Số điện thoại (+84...)">
<div class="modal-btns">
<button onclick="closeTelethonModal()">Hủy</button>
<button class="btn-ok" onclick="tlSendCode()">Gửi OTP</button>
</div>
</div>

<!-- Bước 2: nhập OTP -->
<div class="auth-step" id="step-otp">
<p style="font-size:13px;color:#555;margin-bottom:10px">Nhập mã OTP đã gửi đến Telegram/SMS của bạn:</p>
<input type="text" id="tl-otp" placeholder="Mã OTP">
<div class="modal-btns">
<button onclick="showStep('step-creds')">Quay lại</button>
<button class="btn-ok" onclick="tlSignIn()">Xác nhận</button>
</div>
</div>

<!-- Bước 3: 2FA -->
<div class="auth-step" id="step-2fa">
<p style="font-size:13px;color:#555;margin-bottom:10px">Tài khoản bật xác thực 2 bước, nhập mật khẩu:</p>
<input type="password" id="tl-2fa" placeholder="Mật khẩu 2FA">
<div class="modal-btns">
<button onclick="closeTelethonModal()">Hủy</button>
<button class="btn-ok" onclick="tlSignIn2fa()">Xác nhận</button>
</div>
</div>

<!-- Đã kết nối -->
<div class="auth-step" id="step-connected">
<div class="tg-auth-box" style="text-align:center">
<div style="font-size:24px;margin-bottom:8px">✅</div>
<div style="font-weight:600">Đã kết nối Telethon!</div>
<div style="font-size:12px;color:#555;margin-top:4px">Có thể lấy tin từ kênh Telegram trực tiếp</div>
</div>
<div class="modal-btns">
<button class="btn-ok" onclick="closeTelethonModal()">Đóng</button>
</div>
</div>
</div>
</div>

<!-- Modal forward -->
<div class="modal-bg" id="fwd-modal">
<div class="modal" style="max-height:90vh;overflow-y:auto">
<h2>Chọn đích gửi</h2>
<div style="font-size:11px;color:#888;margin-bottom:8px">Group: nhập Topic IDs phân cách bằng dấu phẩy (để trống = gửi vào group không qua topic)</div>
<div id="fwd-ch-list" style="max-height:320px;overflow-y:auto;margin:8px 0"></div>
<div class="modal-btns">
<button onclick="document.getElementById('fwd-modal').classList.remove('open')">Hủy</button>
<button class="btn-ok" onclick="forwardSelected()">Gửi</button>
</div>
</div>
</div>

<!-- Modal kết quả -->
<div class="modal-bg" id="result-modal">
<div class="modal">
<h2 id="result-title">Kết quả</h2>
<div id="result-list" style="max-height:260px;overflow-y:auto"></div>
<div class="modal-btns"><button class="btn-ok" onclick="document.getElementById('result-modal').classList.remove('open')">Đóng</button></div>
</div>
</div>

<div id="toast"></div>

<!-- Modal tiến trình Đọc toàn bộ nguồn -->
<!-- Modal dọn bài trong topic -->
<div class="modal-bg" id="cleanup-modal" style="z-index:300">
<div class="modal" style="width:560px;max-height:90vh;overflow-y:auto">
  <h2 style="margin-bottom:1rem">🗑️ Dọn bài trong topic</h2>

  <!-- Chọn group -->
  <div style="margin-bottom:.75rem">
    <label style="font-size:12px;color:#666;display:block;margin-bottom:4px">Group</label>
    <select id="cu-channel"
      style="width:100%;padding:8px;border:1px solid #d0d0c8;border-radius:8px;font-size:13px;background:#fff"
      onchange="cuLoadTopics()">
      <option value="">— Chọn group —</option>
    </select>
  </div>

  <!-- Danh sách topics với số bài -->
  <div id="cu-topics-wrap" style="display:none;margin-bottom:.75rem">
    <label style="font-size:12px;color:#666;display:block;margin-bottom:6px">
      Chọn topic &amp; số bài cần xóa
    </label>
    <div id="cu-topics-list" style="border:1px solid #e0e0d8;border-radius:8px;overflow:hidden"></div>
  </div>

  <!-- Lịch tự động -->
  <div id="cu-schedule-wrap" style="display:none;margin-bottom:.75rem;padding:10px 12px;background:#f5f5f0;border-radius:8px">
    <div style="font-size:12px;color:#666;margin-bottom:8px;font-weight:600">⏰ Lịch tự động (hàng ngày)</div>
    <div style="display:flex;align-items:center;gap:8px">
      <label style="font-size:13px">Lúc</label>
      <input id="cu-hour" type="number" min="0" max="23" value="3"
        style="width:60px;padding:6px;border:1px solid #d0d0c8;border-radius:6px;font-size:13px;text-align:center">
      <span style="font-size:13px">:</span>
      <input id="cu-minute" type="number" min="0" max="59" value="0"
        style="width:60px;padding:6px;border:1px solid #d0d0c8;border-radius:6px;font-size:13px;text-align:center">
      <span style="font-size:12px;color:#888">giờ mỗi ngày</span>
    </div>
  </div>

  <!-- Danh sách schedules đã lưu -->
  <div id="cu-saved-wrap" style="margin-bottom:.75rem;display:none">
    <div style="font-size:12px;color:#666;margin-bottom:6px;font-weight:600">📋 Schedules đã cài</div>
    <div id="cu-saved-list" style="border:1px solid #e0e0d8;border-radius:8px;overflow:hidden;font-size:12px"></div>
  </div>

  <div id="cu-status" style="font-size:13px;color:#555;min-height:20px;margin-bottom:.5rem"></div>
  <div style="background:#e0e0d8;border-radius:8px;height:6px;margin-bottom:.75rem;overflow:hidden;display:none" id="cu-bar-wrap">
    <div id="cu-bar" style="background:#dc2626;height:100%;width:0%;transition:width 0.3s;border-radius:8px"></div>
  </div>

  <div class="modal-btns">
    <button onclick="closeCleanupModal()">Đóng</button>
    <button id="cu-auto-btn" class="btn-ok"
      style="background:#2563eb;border-color:#2563eb;display:none"
      onclick="cuSaveSchedules()">⏰ Bật Auto</button>
    <button id="cu-run-btn" class="btn-ok"
      style="background:#dc2626;border-color:#dc2626;display:none"
      onclick="runCleanup()">🗑️ Dọn ngay</button>
  </div>
</div>
</div>
<div class="modal-bg" id="read-all-modal" style="z-index:300">
<div class="modal" style="width:460px;text-align:center">
  <h2 id="ra-title" style="margin-bottom:12px">📚 Đang đọc toàn bộ nguồn...</h2>
  <div style="background:#f5f5f0;border-radius:8px;padding:8px 12px;margin-bottom:12px;font-size:13px;color:#555" id="ra-current">Đang khởi động...</div>
  <div style="background:#e0e0d8;border-radius:8px;height:8px;margin-bottom:8px;overflow:hidden">
    <div id="ra-bar" style="background:#2563eb;height:100%;width:0%;transition:width 0.3s;border-radius:8px"></div>
  </div>
  <div style="font-size:12px;color:#aaa;margin-bottom:16px" id="ra-progress">0 / 0 tin</div>
  <div style="display:flex;gap:8px;justify-content:center" id="ra-btn-row">
    <button id="ra-bg-btn" onclick="runReadAllBg()" style="padding:7px 18px;border:1px solid #d0d0c8;border-radius:8px;background:#fff;cursor:pointer;font-size:13px;color:#2563eb">⬇ Chạy ngầm</button>
    <button id="ra-stop-btn" onclick="stopReadAll()" style="padding:7px 18px;border:1px solid #e0e0d8;border-radius:8px;background:#fff;cursor:pointer;font-size:13px;color:#dc2626">⏹ Dừng lại</button>
  </div>
</div>
</div>

<!-- Indicator job chạy ngầm -->
<div id="ra-bg-indicator" onclick="openBgJobPopup()" title="Đang có job chạy ngầm — nhấn để xem tiến độ"
  style="display:none;position:fixed;bottom:18px;right:18px;z-index:500;background:#2563eb;color:#fff;border-radius:24px;padding:8px 16px;font-size:13px;cursor:pointer;box-shadow:0 2px 8px rgba(0,0,0,.2);align-items:center;gap:8px">
  <span style="display:inline-block;width:10px;height:10px;border-radius:50%;border:2px solid #fff;border-top-color:transparent;animation:spin .8s linear infinite"></span>
  <span id="ra-bg-indicator-text">Đang chạy ngầm...</span>
</div>

<script>
const WS_URL=(location.protocol==='https:'?'wss://':'ws://')+location.host+'/ws';
const DEFAULT_FEEDS=[{name:'VN Wall Street',url:'https://tg.i-c-a.su/rss/vnwallstreet'}];
const PAGE_SIZE = 30;

let feeds=JSON.parse(localStorage.getItem('rss_feeds')||'null')||DEFAULT_FEEDS;
let tgChannels=JSON.parse(localStorage.getItem('tg_channels')||'null')||[];
let allItems=[],newBadges={},filterUrl=null,searchQ='',ws=null,wsReady=false,shownCount=PAGE_SIZE;
let translateOn=JSON.parse(localStorage.getItem('translate_on')??'true');
let translateEngine=localStorage.getItem('translate_engine')||'deepl';
let translateLang=localStorage.getItem('translate_lang')||'vi';
let selected=new Set(),autoFwd=JSON.parse(localStorage.getItem('auto_fwd')??'false');
let editFeedIndex=-1,selectedChannelIndex=-1;
let pollInterval=60,pollNextIn=0;
let telethonConnected=false;
let feedsSynced=false;  // global — không reset khi WS reconnect
let _wsRetryDelay=3000;  // backoff delay, reset về 3s khi connect thành công
let settingsSent=false; // global — chỉ gửi WS settings 1 lần

function saveFeeds(){localStorage.setItem('rss_feeds',JSON.stringify(feeds));}
function saveTgChannels(){localStorage.setItem('tg_channels',JSON.stringify(tgChannels));}

function setTranslateEngine(val){
    translateEngine=val;
    localStorage.setItem('translate_engine',val);
    wsSend({type:'translate_engine',engine:val});
}
function setTranslateLang(val){
    translateLang=val;
    localStorage.setItem('translate_lang',val);
    wsSend({type:'translate_lang',lang:val});
}
function adjustLayout(){
    const h=document.getElementById('top-fixed').offsetHeight;
    const remaining = `calc(100vh - ${h}px)`;
    document.getElementById('aside').style.height = remaining;
    document.getElementById('aside').style.maxHeight = remaining;
    // Đảm bảo main area cũng đúng chiều cao
    document.querySelector('main').style.height = remaining;
    document.querySelector('main').style.maxHeight = remaining;
}
new ResizeObserver(adjustLayout).observe(document.getElementById('top-fixed'));
adjustLayout();

// --- Telethon UI ---
function isTgSource(url){
    if(!url) return false;
    url=url.trim();
    if(url.startsWith('@')||url.includes('t.me/')) return true;
    // Numeric ID: -100xxxxxxxxxx
    const stripped=url.replace(/^-/,'');
    return /^\d{5,}$/.test(stripped);
}

function openTelethonModal(){
    document.getElementById('telethon-modal').classList.add('open');
    checkTelethonStatus();
}
function closeTelethonModal(){document.getElementById('telethon-modal').classList.remove('open');}

function showStep(stepId){
    document.querySelectorAll('.auth-step').forEach(el=>el.classList.remove('active'));
    document.getElementById(stepId).classList.add('active');
}

function showStatusBox(msg,type='info'){
    const box=document.getElementById('tg-status-box');
    box.style.display='block';
    box.className='tg-auth-box'+(type==='error'?' error':type==='warn'?' warn':'');
    box.textContent=msg;
}

async function checkTelethonStatus(){
    try{
        const r=await fetch('/tl_status');
        const d=await r.json();
        if(d.connected){
            telethonConnected=true;
            showStep('step-connected');
            updateTelethonBadge(true);
        } else {
            showStep('step-creds');
            updateTelethonBadge(false);
        }
    }catch(e){showStep('step-creds');}
}

async function tlSendCode(){
    const apiId=document.getElementById('tl-api-id').value.trim();
    const apiHash=document.getElementById('tl-api-hash').value.trim();
    const phone=document.getElementById('tl-phone').value.trim();
    if(!apiId||!apiHash||!phone){alert('Nhập đủ thông tin');return;}
    showStatusBox('Đang gửi OTP...','warn');
    try{
        const r=await fetch('/tl_send_code',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({api_id:apiId,api_hash:apiHash,phone})});
        const d=await r.json();
        if(d.state==='connected'){
            showStep('step-connected');telethonConnected=true;updateTelethonBadge(true);
            document.getElementById('tg-status-box').style.display='none';
        } else if(d.state==='waiting_code'){
            showStep('step-otp');showStatusBox(d.msg,'info');
        } else {
            showStatusBox(d.msg||'Lỗi','error');
        }
    }catch(e){showStatusBox('Lỗi kết nối: '+e.message,'error');}
}

async function tlSignIn(){
    const code=document.getElementById('tl-otp').value.trim();
    if(!code){alert('Nhập mã OTP');return;}
    showStatusBox('Đang xác thực...','warn');
    try{
        const r=await fetch('/tl_sign_in',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({code})});
        const d=await r.json();
        if(d.state==='connected'){
            showStep('step-connected');telethonConnected=true;updateTelethonBadge(true);
            document.getElementById('tg-status-box').style.display='none';
        } else if(d.state==='waiting_2fa'){
            showStep('step-2fa');showStatusBox(d.msg,'warn');
        } else {
            showStatusBox(d.msg||'Lỗi','error');
        }
    }catch(e){showStatusBox('Lỗi: '+e.message,'error');}
}

async function tlSignIn2fa(){
    const pw=document.getElementById('tl-2fa').value;
    if(!pw){alert('Nhập mật khẩu 2FA');return;}
    showStatusBox('Đang xác thực 2FA...','warn');
    try{
        const r=await fetch('/tl_sign_in_2fa',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({password:pw})});
        const d=await r.json();
        if(d.state==='connected'){
            showStep('step-connected');telethonConnected=true;updateTelethonBadge(true);
            document.getElementById('tg-status-box').style.display='none';
        } else {
            showStatusBox(d.msg||'Lỗi','error');
        }
    }catch(e){showStatusBox('Lỗi: '+e.message,'error');}
}

function updateTelethonBadge(connected){
    const badge=document.getElementById('tg-auth-badge');
    if(connected){
        badge.innerHTML='⚡ Telethon: đã kết nối <span style="color:#aaa;font-weight:400">(Tổng số feeds: '+feeds.length+')</span>';
        badge.style.color='#16a34a';
    } else {
        badge.textContent='Telethon: chưa kết nối';badge.style.color='#aaa';
    }
    telethonConnected=connected;
}

// Cập nhật badge mỗi khi feeds thay đổi
function refreshTelethonBadge(){
    if(telethonConnected) updateTelethonBadge(true);
}

// --- Feed type hint ---
document.getElementById('new-url').addEventListener('input',function(){
    const hint=document.getElementById('feed-type-hint');
    hint.style.display=isTgSource(this.value.trim())?'block':'none';

    // Hiện/ẩn "Đọc toàn bộ nguồn" chỉ khi là TG feed
    document.getElementById('read-all-wrap').style.display=isTgSource(this.value.trim())?'block':'none';

    // Kiểm tra URL trùng realtime
    const val=this.value.trim().toLowerCase();
    const warn=document.getElementById('url-duplicate-warn');
    const warnName=document.getElementById('url-dup-name');
    if(val.length > 5){
        const dup=feeds.find((f,i)=>f.url.toLowerCase()===val && i!==editFeedIndex);
        if(dup){
            warnName.textContent=dup.name||dup.url;
            warn.style.display='block';
            document.getElementById('btn-add-feed').disabled=true;
            document.getElementById('btn-add-feed').style.opacity='0.5';
        } else {
            warn.style.display='none';
            document.getElementById('btn-add-feed').disabled=false;
            document.getElementById('btn-add-feed').style.opacity='1';
        }
    } else {
        warn.style.display='none';
        document.getElementById('btn-add-feed').disabled=false;
        document.getElementById('btn-add-feed').style.opacity='1';
    }
});

// --- Render destination list inside feed modal ---
// destinations = [{ch_idx, topic_ids_str}] — lưu trong feed.destinations
function renderFeedDestList(savedDests){
    const wrap=document.getElementById('feed-dest-wrap');
    const list=document.getElementById('feed-dest-list');
    if(!tgChannels.length){wrap.style.display='none';return;}
    wrap.style.display='block';
    const destMap={};
    if(savedDests){
        savedDests.forEach(d=>{
            if(typeof d==='number') destMap[d]={checked:true,topic_ids:''};
            else destMap[d.ch_idx]={checked:true,topic_ids:(d.topic_ids||'')};
        });
    }
    list.innerHTML=tgChannels.map((ch,i)=>{
        const d=destMap[i]||{checked:false,topic_ids:''};
        const isGroup=ch.is_group;
        const topics=ch.topics||[];
        const hasTopics=topics.length>0;
        const topicOpts=topics.map(t=>'<option value="'+t.id+'">'+t.name+' ('+t.id+')</option>').join('');
        let topicSection='';
        if(isGroup){
            topicSection='<div style="margin-top:6px;padding-left:23px">'
                +'<div style="display:flex;align-items:center;gap:6px;margin-bottom:4px">'
                +'<span style="font-size:11px;color:#555;white-space:nowrap;flex-shrink:0">Topics đã chọn:</span>'
                +'<input type="text" class="feed-dest-topics" data-idx="'+i+'" value="'+(d.topic_ids||'')+'"'
                +' placeholder="Để trống = gửi vào group (không topic)"'
                +' style="flex:1;padding:4px 7px;border:1px solid #d0d0c8;border-radius:6px;font-size:12px">'
                +'</div>';
            if(hasTopics){
                topicSection+='<div style="display:flex;align-items:center;gap:6px">'
                    +'<select class="feed-dest-dd" data-idx="'+i+'"'
                    +' style="flex:1;padding:4px 7px;border:1px solid #d0d0c8;border-radius:6px;font-size:12px;color:#555">'
                    +'<option value="">— Chọn topic để thêm —</option>'+topicOpts
                    +'</select>'
                    +'<button type="button" onclick="feedAddTopicFromDd('+i+')"'
                    +' style="padding:4px 12px;border:1px solid #2563eb;border-radius:6px;background:#eff6ff;color:#2563eb;font-size:15px;font-weight:700;cursor:pointer;flex-shrink:0"'
                    +' title="Thêm vào danh sách">+</button>'
                    +'</div>';
            } else {
                topicSection+='<span style="font-size:11px;color:#aaa">Chưa có topic — vào Quản lý Kênh để thêm</span>';
            }
            topicSection+='</div>';
        }
        return '<div style="padding:7px 10px;border-bottom:1px solid #f0f0e8">'
            +'<label style="display:flex;align-items:center;gap:8px;cursor:pointer;font-size:13px">'
            +'<input type="checkbox" class="feed-dest-cb" data-idx="'+i+'" '+(d.checked?'checked':'')+' style="width:15px;height:15px">'
            +'<span style="font-weight:500">'+ch.name+'</span>'
            +'<span style="color:#aaa;font-size:11px">'+(isGroup?'Group':'Channel')+' · '+ch.username+'</span>'
            +'</label>'
            +topicSection
            +'</div>';
    }).join('');
}

function fwdAddTopicFromDd(chIdx){
    const dd=document.querySelector('.fwd-topic-dd[data-idx="'+chIdx+'"]');
    const input=document.querySelector('.fwd-topic-inp[data-idx="'+chIdx+'"]');
    if(!dd||!input||!dd.value) return;
    const cur=input.value.trim();
    const parts=cur?cur.split(',').map(s=>s.trim()).filter(Boolean):[];
    if(!parts.includes(dd.value)){parts.push(dd.value);}
    input.value=parts.join(', ');
    dd.value='';
}

function feedAddTopicFromDd(chIdx){
    const dd=document.querySelector('.feed-dest-dd[data-idx="'+chIdx+'"]');
    const input=document.querySelector('.feed-dest-topics[data-idx="'+chIdx+'"]');
    if(!dd||!input||!dd.value) return;
    const cur=input.value.trim();
    const parts=cur?cur.split(',').map(s=>s.trim()).filter(Boolean):[];
    if(!parts.includes(dd.value)){parts.push(dd.value);}
    input.value=parts.join(', ');
    dd.value='';
}
function getFeedDests(){
    const results=[];
    document.querySelectorAll('.feed-dest-cb:checked').forEach(cb=>{
        const idx=parseInt(cb.dataset.idx);
        const topicInput=document.querySelector(`.feed-dest-topics[data-idx="${idx}"]`);
        const topic_ids=topicInput?topicInput.value.trim():'';
        results.push({ch_idx:idx,topic_ids});
    });
    return results;
}

// --- Channel Manager ---
function renderChannelManager(){
    const select=document.getElementById('ch-select-modal');
    select.innerHTML='<option value="-1">-- Tạo mới --</option>';
    tgChannels.forEach((ch,idx)=>{
        const opt=document.createElement('option');
        opt.value=idx;opt.textContent=`${idx+1}. ${ch.name} (${ch.is_group?'Group':'Channel'})`;
        select.appendChild(opt);
    });
    if(tgChannels.length>0) loadChannelToForm(0);
    else loadChannelToForm(-1);
}

function toggleTopicSection(){
    const isGroup=document.getElementById('ch-is-group').checked;
    document.getElementById('ch-topic-section').style.display=isGroup?'':'none';
}

function chAddTopicRow(topicId='', topicName=''){
    const list=document.getElementById('ch-topic-list');
    const row=document.createElement('div');
    row.className='ch-topic-row';
    row.style.cssText='display:flex;align-items:center;gap:6px';
    row.innerHTML=`
        <input type="text" placeholder="Topic ID" value="${topicId}"
            style="width:90px;padding:5px 7px;border:1px solid #d0d0c8;border-radius:6px;font-size:12px" class="ch-topic-id">
        <input type="text" placeholder="Tên topic (VD: Âm nhạc)" value="${topicName}"
            style="flex:1;padding:5px 7px;border:1px solid #d0d0c8;border-radius:6px;font-size:12px" class="ch-topic-name">
        <span onclick="chDelTopicRow(this)" style="color:#ef4444;cursor:pointer;font-size:18px;line-height:1;flex-shrink:0" title="Xóa">×</span>`;
    list.appendChild(row);
}

function chDelTopicRow(btn){
    const row=btn.closest('.ch-topic-row');
    const name=row.querySelector('.ch-topic-name').value||row.querySelector('.ch-topic-id').value||'topic này';
    if(!confirm('Xóa "'+name+'" khỏi danh sách?')) return;
    row.remove();
}

function chGetTopics(){
    const rows=document.querySelectorAll('.ch-topic-row');
    const result=[];
    rows.forEach(r=>{
        const id=r.querySelector('.ch-topic-id').value.trim();
        const name=r.querySelector('.ch-topic-name').value.trim();
        if(id) result.push({id,name:name||id});
    });
    return result;
}

function loadChannelToForm(idx){
    selectedChannelIndex=parseInt(idx);
    if(selectedChannelIndex>=0&&tgChannels[selectedChannelIndex]){
        const ch=tgChannels[selectedChannelIndex];
        document.getElementById('ch-username').value=ch.username||'';
        document.getElementById('ch-name').value=ch.name||'';
        document.getElementById('ch-is-group').checked=ch.is_group===true;
        document.getElementById('btn-ch-del').style.display='inline-block';
        // Load topics
        document.getElementById('ch-topic-list').innerHTML='';
        (ch.topics||[]).forEach(t=>chAddTopicRow(t.id,t.name));
        toggleTopicSection();
    } else {
        document.getElementById('ch-username').value='';
        document.getElementById('ch-name').value='';
        document.getElementById('ch-is-group').checked=false;
        document.getElementById('btn-ch-del').style.display='none';
        document.getElementById('ch-topic-list').innerHTML='';
        toggleTopicSection();
    }
}

function saveTgSettings(){
    const username=document.getElementById('ch-username').value.trim();
    const name=document.getElementById('ch-name').value.trim()||username;
    if(!username){alert('Nhập @username hoặc channel ID');return;}
    const normUsername=(!username.startsWith('@')&&!username.startsWith('-')&&!/^\d/.test(username))?'@'+username:username;
    // Dùng checkbox thay vì auto-detect — cho phép @username cũng là group
    const is_group=document.getElementById('ch-is-group').checked;
    const topics=is_group?chGetTopics():[];
    const chObj={username:normUsername,name,is_group,topics};
    if(selectedChannelIndex>=0){
        tgChannels[selectedChannelIndex]=chObj;
    } else {
        tgChannels.push(chObj);
    }
    saveTgChannels();renderChannelManager();
    wsSend({type:'tg_settings',channels:tgChannels});
    closeChannelManager();
    document.getElementById('tg-saved').style.display='inline';
    setTimeout(()=>document.getElementById('tg-saved').style.display='none',2000);
}

function deleteChannel(){
    if(selectedChannelIndex<0)return;
    if(!confirm('Xóa kênh?'))return;
    tgChannels.splice(selectedChannelIndex,1);saveTgChannels();renderChannelManager();
    loadChannelToForm(-1);wsSend({type:'tg_settings',channels:tgChannels});
}

function openChannelManager(){document.getElementById('channel-modal').classList.add('open');renderChannelManager();}
function closeChannelManager(){document.getElementById('channel-modal').classList.remove('open');}

function toggleAutoFwd(){
    autoFwd=!autoFwd;localStorage.setItem('auto_fwd',JSON.stringify(autoFwd));
    document.getElementById('afwd-dot').className='adot'+(autoFwd?' on':'');
    document.getElementById('auto-fwd-btn').className='auto-fwd'+(autoFwd?' active':'');
    wsSend({type:'auto_fwd',enabled:autoFwd,channels:tgChannels});
}
document.getElementById('afwd-dot').className='adot'+(autoFwd?' on':'');
document.getElementById('auto-fwd-btn').className='auto-fwd'+(autoFwd?' active':'');

function updateFwdBar(){
    const bar=document.getElementById('fwd-bar');
    document.getElementById('fwd-count').textContent=selected.size+' tin';
    bar.classList.toggle('hidden',selected.size===0);
}

function openForwardModal(){
    // Lấy feed configs của các tin đang được chọn để pre-check đúng đích
    const selectedItems=allItems.filter(it=>selected.has(it.guid));
    // Gom tất cả destinations đã config trong các feed được chọn
    const preDestMap={};  // ch_idx -> topic_ids (từ config feed)
    selectedItems.forEach(it=>{
        const feedCfg=feeds.find(f=>f.url===it.feedUrl);
        if(feedCfg&&feedCfg.destinations){
            feedCfg.destinations.forEach(d=>{
                const idx=typeof d==='number'?d:d.ch_idx;
                const topics=typeof d==='number'?'':(d.topic_ids||'');
                if(!preDestMap[idx]) preDestMap[idx]={checked:true,topic_ids:topics};
            });
        }
    });
    const list=document.getElementById('fwd-ch-list');
    list.innerHTML=tgChannels.map((ch,i)=>{
        const pre=preDestMap[i]||{checked:false,topic_ids:''};
        const topics=ch.topics||[];
        const hasTopics=topics.length>0;
        const topicOpts=topics.map(t=>'<option value="'+t.id+'">'+t.name+' ('+t.id+')</option>').join('');
        let topicSection='';
        if(ch.is_group){
            topicSection='<div style="margin-top:6px;padding-left:23px">'
                +'<div style="display:flex;align-items:center;gap:6px;margin-bottom:4px">'
                +'<span style="font-size:11px;color:#555;white-space:nowrap;flex-shrink:0">Topics đã chọn:</span>'
                +'<input type="text" class="fwd-topic-inp" data-idx="'+i+'" value="'+(pre.topic_ids||'')+'"'
                +' placeholder="Để trống = không topic"'
                +' style="flex:1;padding:4px 7px;border:1px solid #d0d0c8;border-radius:6px;font-size:12px">'
                +'</div>';
            if(hasTopics){
                topicSection+='<div style="display:flex;align-items:center;gap:6px">'
                    +'<select class="fwd-topic-dd" data-idx="'+i+'"'
                    +' style="flex:1;padding:4px 7px;border:1px solid #d0d0c8;border-radius:6px;font-size:12px;color:#555">'
                    +'<option value="">— Chọn topic để thêm —</option>'+topicOpts
                    +'</select>'
                    +'<button type="button" onclick="fwdAddTopicFromDd('+i+')"'
                    +' style="padding:4px 12px;border:1px solid #2563eb;border-radius:6px;background:#eff6ff;color:#2563eb;font-size:15px;font-weight:700;cursor:pointer;flex-shrink:0"'
                    +' title="Thêm vào danh sách">+</button>'
                    +'</div>';
            } else {
                topicSection+='<span style="font-size:11px;color:#aaa">Chưa có topic — vào Quản lý Kênh để thêm</span>';
            }
            topicSection+='</div>';
        }
        return '<div style="padding:7px 10px;border-bottom:1px solid #f0f0e8">'
            +'<label style="display:flex;align-items:center;gap:8px;cursor:pointer;font-size:13px">'
            +'<input type="checkbox" class="fwd-ch-cb" data-idx="'+i+'" '+(pre.checked?'checked':'')+' style="width:15px;height:15px">'
            +'<span style="font-weight:500">'+ch.name+'</span>'
            +'<span style="color:#aaa;font-size:11px">'+(ch.is_group?'Group':'Channel')+' · '+ch.username+'</span>'
            +'</label>'
            +topicSection
            +'</div>';
    }).join('');
        document.getElementById('fwd-modal').classList.add('open');
}

async function forwardSelected(){
    const checkboxes=document.querySelectorAll('#fwd-ch-list .fwd-ch-cb:checked');
    if(checkboxes.length===0){alert('Chọn ít nhất 1 đích gửi');return;}
    // Build destinations list
    const destinations=Array.from(checkboxes).map(cb=>{
        const idx=parseInt(cb.dataset.idx);
        const ch=tgChannels[idx];
        const topicInp=document.querySelector(`.fwd-topic-inp[data-idx="${idx}"]`);
        const topic_ids=topicInp?topicInp.value.trim():'';
        return {username:ch.username,name:ch.name,is_group:ch.is_group,topic_ids};
    });
    const items=allItems.filter(it=>selected.has(it.guid)).map(it=>{
        const feedCfg=feeds.find(f=>f.url===it.feedUrl)||{};
        return {guid:it.guid,title:it.title,desc:it.desc,link:it.link,
                category:it.category,feedUrl:it.feedUrl,
                show_link:feedCfg.show_link!==false};
    });
    try{
        const r=await fetch('/tg_forward',{method:'POST',headers:{'Content-Type':'application/json'},
            body:JSON.stringify({destinations,items})});
        const data=await r.json();
        document.getElementById('result-list').innerHTML=data.results.map(r=>
            `<div style="padding:7px;background:${r.ok?'#f0fdf4':'#fef2f2'}">${r.ok?'✓':'✗'} ${r.title||'Gửi'}${r.error?' — '+r.error:''}</div>`
        ).join('');
        document.getElementById('result-modal').classList.add('open');
        selected.clear();updateFwdBar();
        document.querySelectorAll('.item.selected').forEach(el=>{
            el.classList.remove('selected');
            const cb=el.querySelector('input[type=checkbox]');
            if(cb) cb.checked=false;
        });
    }catch(e){alert('Lỗi: '+e.message);}
    document.getElementById('fwd-modal').classList.remove('open');
}

// --- Sidebar ---
// Trả về timestamp (ms) của bài mới nhất trong feed, hoặc 0 nếu chưa có
function _feedLastTs(url) {
    let max = 0;
    for (const it of allItems) {
        if (it.feedUrl === url && it.ts > max) max = it.ts;
    }
    return max;
}

// Forward counts từ server (load 1 lần lúc init, cập nhật qua WS)
let _fwdCounts = {};
fetch('/fwd_counts')
    .then(r=>r.json()).then(d=>{ _fwdCounts=d; renderSidebar(); }).catch(()=>{});

function renderSidebar(){
    const list   = document.getElementById('feed-list');
    const totalNew = Object.values(newBadges).reduce((a,b)=>a+b,0);
    const now    = Date.now();
    const STALE_MS = 30 * 24 * 60 * 60 * 1000;
    const sortBy = (document.getElementById('feed-sort')||{}).value || 'default';
    const searchQ = ((document.getElementById('feed-search')||{}).value||'').trim().toLowerCase();

    // Lọc theo search
    let visible = feeds.filter((f,i) => {
        f._idx = i; // giữ index gốc
        return !searchQ || f.name.toLowerCase().includes(searchQ) || f.url.toLowerCase().includes(searchQ);
    });

    // Sort
    if (sortBy === 'activity') {
        visible.sort((a,b) => _feedLastTs(b.url) - _feedLastTs(a.url));
    } else if (sortBy === 'name') {
        visible.sort((a,b) => a.name.localeCompare(b.name, 'vi'));
    } else if (sortBy === 'stale') {
        visible.sort((a,b) => {
            const sa = _feedLastTs(a.url), sb = _feedLastTs(b.url);
            const aStale = sa > 0 && (now-sa) > STALE_MS;
            const bStale = sb > 0 && (now-sb) > STALE_MS;
            if (aStale !== bStale) return aStale ? -1 : 1;
            return sa - sb;
        });
    }

    // Group by category
    const groups = {};
    for (const f of visible) {
        const cat = f.category || 'Chưa phân loại';
        if (!groups[cat]) groups[cat] = [];
        groups[cat].push(f);
    }
    const catOrder = Object.keys(groups).sort((a,b) => {
        if (a === 'Chưa phân loại') return 1;
        if (b === 'Chưa phân loại') return -1;
        return a.localeCompare(b, 'vi');
    });

    let html = `<div class="feed-row${filterUrl===null?' active':''}" onclick="setFilter(null)">
        <span style="flex:1;min-width:0" class="fname">Tất cả nguồn</span>
        ${totalNew>0?`<span style="background:#dbeafe;color:#1d4ed8;padding:1px 5px;border-radius:8px;font-size:10px">+${totalNew}</span>`:''}</div>`;

    const multiCat = catOrder.length > 1;

    for (const cat of catOrder) {
        if (multiCat) {
            html += `<div style="padding:4px 10px 2px;font-size:10px;font-weight:700;color:#94a3b8;text-transform:uppercase;letter-spacing:.5px;background:#f8f8f4;border-top:1px solid #f0f0e8">${cat}</div>`;
        }
        for (const f of groups[cat]) {
            const i   = f._idx;
            const n   = newBadges[f.url]||0;
            const active = filterUrl===f.url;
            const isTg   = isTgSource(f.url);
            const badge  = `<span class="feed-badge ${isTg?'badge-tg':'badge-rss'}">${isTg?'TG':'RSS'}</span>`;
            const readAllBtn = f.read_all&&isTg
                ? `<span title="Đọc toàn bộ nguồn" onclick="startReadAll(${i})" style="cursor:pointer;color:#f59e0b;font-size:13px;padding:0 2px" class="fedit">▶</span>`
                : '';
            const lastTs = _feedLastTs(f.url);
            const isStale = lastTs > 0 && (now - lastTs) > STALE_MS;
            const staleDot = isStale
                ? `<span title="Không có bài mới trong 30 ngày" style="color:#ef4444;font-size:10px;margin-right:3px;flex-shrink:0">●</span>`
                : '';
            const fwdN = _fwdCounts[f.url] || 0;
            const fwdBadge = fwdN > 0
                ? `<span title="Đã forward ${fwdN} bài" style="color:#10b981;font-size:10px;flex-shrink:0;margin-right:2px">↗${fwdN}</span>`
                : '';
            html += `<div class="feed-row${active?' active':''}">
                ${staleDot}<span class="fname" style="flex:1;min-width:0" onclick="setFilter('${f.url}')">${f.name}${badge}</span>
                ${fwdBadge}
                ${n>0&&!active?`<span style="background:#dbeafe;color:#1d4ed8;padding:1px 5px;border-radius:8px;font-size:10px">+${n}</span>`:''}
                ${readAllBtn}<span class="fedit" style="flex-shrink:0" onclick="openEditFeed(${i})">✎</span><span class="fdel" style="flex-shrink:0" onclick="deleteFeed(${i})">×</span></div>`;
        }
    }
    list.innerHTML=html;
}

function setFilter(url){
    filterUrl=url;
    if(url) newBadges[url]=0; else Object.keys(newBadges).forEach(k=>newBadges[k]=0);
    document.getElementById('view-label').textContent=url?(feeds.find(f=>f.url===url)?.name||url):'Tất cả nguồn';
    shownCount=PAGE_SIZE;renderSidebar();renderStream();
}

function syncFeedsHttp(){
    const payload=feeds.map(f=>({url:f.url,name:f.name,
        show_link:f.show_link!==false,
        auto_fwd:f.auto_fwd===true,
        destinations:f.destinations||[],
        history_limit:f.history_limit!=null?f.history_limit:20}));
    fetch('/sync_feeds',{method:'POST',headers:{'Content-Type':'application/json'},
        body:JSON.stringify({feeds:payload})})
    .then(r=>r.json()).then(d=>console.log('[HTTP] sync_feeds OK, feeds='+d.count))
    .catch(e=>console.warn('[HTTP] sync_feeds lỗi:',e));
}

function deleteFeed(i){
    if(!confirm('Xóa feed "'+feeds[i].name+'"?'))return;
    const url=feeds[i].url;feeds.splice(i,1);saveFeeds();
    allItems=allItems.filter(it=>it.feedUrl!==url);
    if(filterUrl===url)filterUrl=null;
    syncFeedsHttp();
    refreshTelethonBadge();
    renderSidebar();renderStream();
}

function openEditFeed(i){
    editFeedIndex=i;const f=feeds[i];
    document.getElementById('new-name').value=f.name;
    document.getElementById('new-url').value=f.url;
    document.getElementById('new-show-link').checked=f.show_link!==false;
    document.getElementById('new-auto-fwd').checked=f.auto_fwd===true;
    document.getElementById('new-translate').checked=f.do_translate!==false;
    document.getElementById('new-read-all').checked=f.read_all===true;
    document.getElementById('read-all-wrap').style.display=isTgSource(f.url)?'block':'none';
    document.getElementById('new-history-limit').value=String(f.history_limit!=null?f.history_limit:20);
    document.getElementById('modal-title').textContent='Sửa feed';
    document.getElementById('btn-add-feed').textContent='Cập nhật';
    const hint=document.getElementById('feed-type-hint');
    hint.style.display=isTgSource(f.url)?'block':'none';
    renderFeedDestList(f.destinations||[]);
    document.getElementById('modal').classList.add('open');
}

function openModal(){
    editFeedIndex=-1;
    document.getElementById('new-name').value='';
    document.getElementById('new-url').value='';
    document.getElementById('url-duplicate-warn').style.display='none';
    document.getElementById('btn-add-feed').disabled=false;
    document.getElementById('btn-add-feed').style.opacity='1';
    document.getElementById('new-show-link').checked=true;
    document.getElementById('new-auto-fwd').checked=false;
    document.getElementById('new-translate').checked=true;
    document.getElementById('new-read-all').checked=false;
    document.getElementById('read-all-wrap').style.display='none';
    document.getElementById('new-history-limit').value='20';
    document.getElementById('modal-title').textContent='Thêm feed';
    document.getElementById('btn-add-feed').textContent='Thêm';
    document.getElementById('feed-type-hint').style.display='none';
    renderFeedDestList([]);
    document.getElementById('modal').classList.add('open');
}
function closeModal(){document.getElementById('modal').classList.remove('open');}

function addFeed(){
    const name=document.getElementById('new-name').value.trim(),
          url=document.getElementById('new-url').value.trim(),
          show_link=document.getElementById('new-show-link').checked,
          auto_fwd=document.getElementById('new-auto-fwd').checked,
          do_translate=document.getElementById('new-translate').checked,
          read_all=document.getElementById('new-read-all').checked,
          history_limit=parseInt(document.getElementById('new-history-limit').value)||20,
          destinations=getFeedDests();
    if(!name||!url){alert('Nhập đủ tên và URL');return;}
    // Kiểm tra trùng URL (backup check)
    const dupFeed=feeds.find((f,i)=>f.url.toLowerCase()===url.toLowerCase() && i!==editFeedIndex);
    if(dupFeed){alert('Feed "'+( dupFeed.name||dupFeed.url)+'" đã tồn tại trong danh sách!');return;}
    const feedObj={name,url,show_link,auto_fwd,do_translate,read_all,destinations,history_limit};
    if(editFeedIndex>=0){feeds[editFeedIndex]=feedObj;}
    else{feeds.push(feedObj);}
    saveFeeds();
    syncFeedsHttp();
    closeModal();
    if(editFeedIndex<0) fetchAndMerge(url,name,'',true);
    else{allItems=allItems.filter(it=>it.feedUrl!==url);fetchAndMerge(url,name,'',false);}
    refreshTelethonBadge();
    renderSidebar();renderStream();
}

function applyFilter(){searchQ=document.getElementById('search').value.trim().toLowerCase();shownCount=PAGE_SIZE;renderStream();}

// --- Đọc toàn bộ nguồn ---

// ===================== READ ALL - ĐỌC TOÀN BỘ NGUỒN =====================
// Flow đơn giản: ▶ → start bg job → poll status → hiện popup
// Không dùng SSE, không race condition, nhiều job song song OK

let _raCurrentUrl  = null;  // URL đang hiển thị trong popup
let _raPollTimer   = null;  // timer poll status cho popup đang mở
let _raBgPollTimer = null;  // timer poll indicator góc phải

function _raSetBtns(mode) {
    const stopBtn = document.getElementById('ra-stop-btn');
    const bgBtn   = document.getElementById('ra-bg-btn');
    if (mode === 'running') {
        stopBtn.textContent = '⏹ Dừng'; stopBtn.style.color = '#dc2626';
        stopBtn.onclick = stopReadAll; stopBtn.disabled = false;
        bgBtn.style.display = ''; bgBtn.textContent = '⬇ Chạy ngầm'; bgBtn.disabled = false;
    } else if (mode === 'done') {
        stopBtn.textContent = '✓ Đóng'; stopBtn.style.color = '#16a34a';
        stopBtn.onclick = closeReadAllModal;
        stopBtn.disabled = false; bgBtn.style.display = 'none';
    }
}


function _raUpdateProgress(done, total, current) {
    const pct = total > 0 ? Math.round(done / total * 100) : 0;
    document.getElementById('ra-bar').style.width = pct + '%';
    document.getElementById('ra-progress').textContent = done + ' / ' + (total || '?') + ' tin';
    if (current !== undefined) document.getElementById('ra-current').textContent = current;
}

// Poll status cho popup đang mở — chỉ poll khi popup open
function _startRaPoll(url) {
    _stopRaPoll();
    _raCurrentUrl = url;
    _raPollTimer = setInterval(() => _raPollOnce(url), 2000);
    _raPollOnce(url); // poll ngay lập tức
}

function _stopRaPoll() {
    if (_raPollTimer) { clearInterval(_raPollTimer); _raPollTimer = null; }
}

async function _raPollOnce(url) {
    // Không poll nếu popup đóng
    if (!document.getElementById('read-all-modal').classList.contains('open')) {
        _stopRaPoll(); return;
    }
    // Guard: nếu popup đã chuyển sang job khác thì bỏ qua kết quả của URL này
    if (url !== _raCurrentUrl) { _stopRaPoll(); return; }
    try {
        const r = await fetch('/tg_read_all_status', {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({url})
        });
        const d = await r.json();
        const job = d.job;
        if (!job) return;
        // Double-check sau khi await — tránh race condition async
        if (url !== _raCurrentUrl) return;
        _raUpdateProgress(job.done, job.total, job.current);
        if (job.status !== 'running') {
            _stopRaPoll();
            _raSetBtns('done');
            _updateBgIndicator(); // cập nhật indicator
        }
    } catch(e) {}
}

// Bấm ▶ trên feed
async function startReadAll(feedIdx) {
    const f = feeds[feedIdx];
    if (!f || !isTgSource(f.url)) return;
    const url = f.url;

    // Kiểm tra job hiện tại của URL này
    try {
        const sr = await fetch('/tg_read_all_status', {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({url})
        });
        const sd = await sr.json();
        const job = sd.job;

        if (job && job.status === 'running') {
            // Đang chạy → mở popup xem tiến độ, KHÔNG start lại
            _openRaPopup(url, job);
            return;
        }
        if (job && (job.status === 'done' || job.status === 'stopped' || job.status === 'error')) {
            const msg = job.status === 'done'
                ? `Đã hoàn thành (${job.done} tin). Chạy lại từ đầu?`
                : `Job trước bị ${job.status}. Chạy lại?`;
            if (!confirm(msg)) return;
        }
    } catch(e) {}

    // Start job mới
    _raCurrentUrl = url;
    _openRaPopup(url, {done: 0, total: 0, status: 'running', current: 'Đang khởi động...'});

    try {
        // Dùng /tg_read_all_bg — endpoint này không reset job đang chạy
        const resp = await fetch('/tg_read_all_bg', {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({url})
        });
        const d = await resp.json();
        if (d.error === 'blocked') {
            showToastMsg('⚠️ ' + d.msg);
            // Blocked vì đang chạy → mở popup xem
            _startRaPoll(url);
            return;
        }
        if (!d.ok) {
            document.getElementById('ra-current').textContent = '❌ ' + (d.error || d.msg || 'Lỗi');
            _raSetBtns('done'); return;
        }
        // Job started → bắt đầu poll
        _startRaPoll(url);
    } catch(e) {
        document.getElementById('ra-current').textContent = '❌ Lỗi: ' + e.message;
        _raSetBtns('done');
    }
}

// Mở popup với data hiện tại
function _openRaPopup(url, job) {
    _raCurrentUrl = url;
    const f = feeds.find(x => x.url === url);
    document.getElementById('ra-title').textContent = '📚 ' + (f ? f.name : url);
    _raUpdateProgress(job.done || 0, job.total || 0, job.current || 'Đang khởi động...');
    _raSetBtns(job.status === 'running' ? 'running' : 'done');
    document.getElementById('ra-bar').style.width =
        (job.total > 0 ? Math.round((job.done||0)/job.total*100) : 0) + '%';
    document.getElementById('read-all-modal').classList.add('open');
    if (job.status === 'running') _startRaPoll(url);
}

// Nút "Chạy ngầm" — chỉ đóng popup, job vẫn chạy trên server
function runReadAllBg() {
    _stopRaPoll(); // dừng poll của popup
    document.getElementById('read-all-modal').classList.remove('open');
    if (_raCurrentUrl) {
        const f = feeds.find(x => x.url === _raCurrentUrl);
        showToastMsg('⬇ Chạy ngầm: ' + (f ? f.name : _raCurrentUrl));
        _startBgPoll(); // bắt đầu poll indicator
    }
}

// Nút "Dừng"
function stopReadAll() {
    const url = _raCurrentUrl;
    fetch('/tg_read_all_stop', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({url: url || ''})
    }).catch(() => {});
    _stopRaPoll();
    _stopBgPoll();
    document.getElementById('read-all-modal').classList.remove('open');
    // Ẩn indicator ngay lập tức — không đợi server confirm
    const bgEl = document.getElementById('ra-bg-indicator');
    if (bgEl) bgEl.style.display = 'none';
    // Poll lại sau 3s để đồng bộ trạng thái thực từ server
    setTimeout(_updateBgIndicator, 3000);
}

// Indicator góc phải — poll tất cả jobs running
function _startBgPoll() {
    if (_raBgPollTimer) return;
    _raBgPollTimer = setInterval(_updateBgIndicator, 5000);
    _updateBgIndicator();
}

function _stopBgPoll() {
    if (_raBgPollTimer) { clearInterval(_raBgPollTimer); _raBgPollTimer = null; }
}

async function _updateBgIndicator() {
    try {
        const r = await fetch('/tg_read_all_status', {
            method: 'POST', headers: {'Content-Type': 'application/json'}, body: '{}'
        });
        const d = await r.json();
        const running = Object.values(d.jobs || {}).filter(j => j.status === 'running');
        const el  = document.getElementById('ra-bg-indicator');
        const txt = document.getElementById('ra-bg-indicator-text');
        if (running.length === 0) {
            el.style.display = 'none';
            _stopBgPoll();
        } else {
            el.style.display = 'flex';
            const first = running[0];
            const extra = running.length > 1 ? ` (+${running.length - 1})` : '';
            txt.textContent = (first.name || '') + ': ' + first.done + '/' + (first.total || '?') + extra;
        }
    } catch(e) {}
}

// Nhấn indicator → mở popup job đầu tiên đang running
async function openBgJobPopup() {
    try {
        const r = await fetch('/tg_read_all_status', {
            method: 'POST', headers: {'Content-Type': 'application/json'}, body: '{}'
        });
        const d = await r.json();
        const running = Object.values(d.jobs || {}).filter(j => j.status === 'running');
        if (running.length === 0) { showToastMsg('Không có job nào đang chạy'); return; }
        // Ưu tiên mở job của _raCurrentUrl nếu còn running, không thì mở job đầu tiên
        const job = running.find(j => j.url === _raCurrentUrl) || running[0];
        _openRaPopup(job.url, job);
    } catch(e) {}
}

// ===================== CLEANUP TOPIC =====================
let _cuRunning = false;

function openCleanupModal() {
    const chSel = document.getElementById('cu-channel');
    const prevCh = chSel.value;
    chSel.innerHTML = '<option value="">— Chọn group —</option>'
        + tgChannels.filter(ch=>ch.is_group).map(ch=>{
            return '<option value="'+ch.username+'">'+ch.name+' ('+ch.username+')</option>';
        }).join('');
    if(prevCh) chSel.value = prevCh;
    document.getElementById('cu-topics-wrap').style.display = 'none';
    document.getElementById('cu-schedule-wrap').style.display = 'none';
    document.getElementById('cu-run-btn').style.display = 'none';
    document.getElementById('cu-auto-btn').style.display = 'none';
    document.getElementById('cu-status').textContent = '';
    document.getElementById('cu-bar-wrap').style.display = 'none';
    document.getElementById('cu-bar').style.width = '0%';
    cuLoadSavedSchedules();
    document.getElementById('cleanup-modal').classList.add('open');
}

function cuLoadTopics() {
    const chSel  = document.getElementById('cu-channel');
    const ch     = tgChannels.find(c=>c.username===chSel.value);
    const topics = (ch && ch.topics) ? ch.topics : [];
    const wrap   = document.getElementById('cu-topics-wrap');
    const list   = document.getElementById('cu-topics-list');
    if(!ch || topics.length === 0) {
        wrap.style.display = 'none';
        document.getElementById('cu-schedule-wrap').style.display = 'none';
        document.getElementById('cu-run-btn').style.display = 'none';
        document.getElementById('cu-auto-btn').style.display = 'none';
        return;
    }
    list.innerHTML = topics.map((t,i)=>`
      <div style="display:flex;align-items:center;gap:10px;padding:8px 12px;
        ${i>0?'border-top:1px solid #f0f0e8':''}">
        <input type="checkbox" id="cu-chk-${t.id}" value="${t.id}"
          data-name="${t.name}" style="width:15px;height:15px;cursor:pointer"
          onchange="cuUpdateButtons()">
        <label for="cu-chk-${t.id}" style="flex:1;font-size:13px;cursor:pointer">
          ${t.name} <span style="color:#aaa;font-size:11px">(${t.id})</span>
        </label>
        <input type="number" id="cu-cnt-${t.id}" min="1" value="100"
          placeholder="Số bài xóa"
          style="width:90px;padding:5px 8px;border:1px solid #d0d0c8;border-radius:6px;font-size:12px;text-align:center">
      </div>
    `).join('');
    wrap.style.display = '';
    document.getElementById('cu-schedule-wrap').style.display = '';
    cuUpdateButtons();
}

function cuUpdateButtons() {
    const anyChecked = document.querySelectorAll('#cu-topics-list input[type=checkbox]:checked').length > 0;
    document.getElementById('cu-run-btn').style.display  = anyChecked ? '' : 'none';
    document.getElementById('cu-auto-btn').style.display = anyChecked ? '' : 'none';
}

async function cuSaveSchedules() {
    const channel = document.getElementById('cu-channel').value.trim();
    const hour    = parseInt(document.getElementById('cu-hour').value) || 3;
    const minute  = parseInt(document.getElementById('cu-minute').value) || 0;
    const checked = [...document.querySelectorAll('#cu-topics-list input[type=checkbox]:checked')];
    if(!checked.length) { showToastMsg('⚠️ Chọn ít nhất 1 topic'); return; }
    let saved = 0;
    for(const chk of checked) {
        const tid   = chk.value;
        const tname = chk.dataset.name || tid;
        const count = parseInt(document.getElementById('cu-cnt-'+tid)?.value) || 100;
        await fetch('/tg_cleanup_schedule_save', {
            method:'POST', headers:{'Content-Type':'application/json'},
            body: JSON.stringify({channel, topic_id: parseInt(tid), topic_name: tname,
                                  count, hour, minute, enabled: true})
        });
        saved++;
    }
    showToastMsg(`✅ Đã lưu ${saved} schedule — chạy lúc ${String(hour).padStart(2,'0')}:${String(minute).padStart(2,'0')} hàng ngày`);
    cuLoadSavedSchedules();
}

async function cuLoadSavedSchedules() {
    try {
        const r = await fetch('/tg_cleanup_schedule_list', {method:'POST',
            headers:{'Content-Type':'application/json'}, body:'{}'});
        const d = await r.json();
        const wrap = document.getElementById('cu-saved-wrap');
        const list = document.getElementById('cu-saved-list');
        if(!d.schedules || d.schedules.length === 0) { wrap.style.display='none'; return; }
        wrap.style.display = '';
        list.innerHTML = d.schedules.map((s,i)=>`
          <div style="display:flex;align-items:center;gap:8px;padding:7px 12px;
            ${i>0?'border-top:1px solid #f0f0e8':''};background:${s.enabled?'#fff':'#fafaf8'}">
            <span style="flex:1;font-size:12px;color:${s.enabled?'#333':'#aaa'}">
              <b>${s.channel}</b> / ${s.topic_name||s.topic_id}
              — xóa ${s.count} tin lúc
              ${String(s.hour).padStart(2,'0')}:${String(s.minute).padStart(2,'0')}
              ${s.last_run?'<span style="color:#aaa">(chạy: '+s.last_run+')</span>':''}
            </span>
            <button onclick="cuToggleSchedule('${s.channel}','${s.topic_id}',${!s.enabled})"
              style="padding:3px 8px;border:1px solid #d0d0c8;border-radius:5px;font-size:11px;cursor:pointer;background:#fff;color:${s.enabled?'#2563eb':'#aaa'}">
              ${s.enabled?'✅ Bật':'⏸ Tắt'}
            </button>
            <button onclick="cuDeleteSchedule('${s.channel}','${s.topic_id}')"
              style="padding:3px 8px;border:1px solid #fca5a5;border-radius:5px;font-size:11px;cursor:pointer;background:#fff;color:#dc2626">🗑</button>
          </div>
        `).join('');
    } catch(e) {}
}

async function cuToggleSchedule(channel, topic_id, enabled) {
    const r = await fetch('/tg_cleanup_schedule_list', {method:'POST',
        headers:{'Content-Type':'application/json'}, body:'{}'});
    const d = await r.json();
    const s = d.schedules.find(x=>x.channel===channel && String(x.topic_id)===String(topic_id));
    if(!s) return;
    await fetch('/tg_cleanup_schedule_save', {
        method:'POST', headers:{'Content-Type':'application/json'},
        body: JSON.stringify({...s, enabled})
    });
    cuLoadSavedSchedules();
}

async function cuDeleteSchedule(channel, topic_id) {
    if(!confirm(`Xóa schedule cho topic ${topic_id} của ${channel}?`)) return;
    await fetch('/tg_cleanup_schedule_delete', {
        method:'POST', headers:{'Content-Type':'application/json'},
        body: JSON.stringify({channel, topic_id: parseInt(topic_id)})
    });
    cuLoadSavedSchedules();
}

function closeCleanupModal() {
    document.getElementById('cleanup-modal').classList.remove('open');
}

let _cuPollTimer = null;

async function runCleanup() {
    if (_cuRunning) return;
    const channel = document.getElementById('cu-channel').value.trim();
    const checked = [...document.querySelectorAll('#cu-topics-list input[type=checkbox]:checked')];
    if(!channel) { showToastMsg('⚠️ Chọn group'); return; }
    if(!checked.length) { showToastMsg('⚠️ Chọn ít nhất 1 topic'); return; }
    const topics = checked.map(chk=>({
        id: chk.value, name: chk.dataset.name || chk.value,
        count: parseInt(document.getElementById('cu-cnt-'+chk.value)?.value) || 100
    }));
    const summary = topics.map(t=>`${t.name}: ${t.count} bài`).join(', ');
    if(!confirm(`Xóa ${topics.length} topic(s):\n${summary}\n\nHành động này không thể hoàn tác!`)) return;
    _cuRunning = true;
    document.getElementById('cu-run-btn').disabled = true;
    document.getElementById('cu-auto-btn').disabled = true;
    document.getElementById('cu-bar-wrap').style.display = '';
    document.getElementById('cu-bar').style.width = '0%';
    let totalDone = 0;
    let lastRemaining = null;
    for(let i=0; i<topics.length; i++) {
        const t = topics[i];
        document.getElementById('cu-status').textContent =
            `⏳ [${i+1}/${topics.length}] Đang xóa topic "${t.name}"...`;
        try {
            const resp = await fetch('/tg_cleanup_topic', {
                method:'POST', headers:{'Content-Type':'application/json'},
                body: JSON.stringify({channel, topic_id: parseInt(t.id), count: t.count})
            });
            const d = await resp.json();
            if(!d.ok) { document.getElementById('cu-status').textContent = '❌ '+(d.error||'Lỗi'); break; }
            await new Promise(resolve=>{
                const poll = setInterval(async()=>{
                    try {
                        const r2 = await fetch('/tg_cleanup_status', {method:'POST',
                            headers:{'Content-Type':'application/json'},
                            body: JSON.stringify({channel})});
                        const st = await r2.json();
                        const pct = st.total>0 ? Math.round(st.deleted/st.total*100) : 0;
                        document.getElementById('cu-bar').style.width =
                            Math.round((i/topics.length + pct/100/topics.length)*100)+'%';
                        if(st.status==='done') {
                            totalDone += st.deleted;
                            if(st.remaining != null) lastRemaining = st.remaining;
                            clearInterval(poll); resolve();
                        }
                        else if(st.status==='error') {
                            document.getElementById('cu-status').textContent = `❌ "${t.name}": ${st.error}`;
                            clearInterval(poll); resolve();
                        }
                    } catch(e) { clearInterval(poll); resolve(); }
                }, 2000);
            });
        } catch(e) {
            document.getElementById('cu-status').textContent = '❌ Lỗi kết nối: '+e.message; break;
        }
    }
    document.getElementById('cu-bar').style.width = '100%';
    const remainingTxt = lastRemaining != null
        ? ` — còn lại <b>~${lastRemaining} tin</b> <span style="color:#999;font-size:11px">(Telegram app cache, restart để thấy đúng)</span>`
        : '';
    document.getElementById('cu-status').innerHTML = `✅ Hoàn thành — đã xóa <b>${totalDone} tin</b>${remainingTxt}`;
    showToastMsg(`✅ Đã dọn ${totalDone} tin`);
    _cuRunning = false;
    document.getElementById('cu-run-btn').disabled = false;
    document.getElementById('cu-auto-btn').disabled = false;
}
// ===================== END CLEANUP TOPIC =====================

function getVisible(){
    let items=filterUrl?allItems.filter(it=>it.feedUrl===filterUrl):[...allItems];
    if(searchQ) items=items.filter(it=>(it.title+it.desc).toLowerCase().includes(searchQ));
    return items;
}

// --- State mở/đóng tin — dùng guid thay vì index để không bị reset khi re-render ---
const openBodies = new Set();  // guid của các tin đang mở

function renderStream(){
    const visible=getVisible(),stream=document.getElementById('stream');
    document.getElementById('item-count').textContent=visible.length+' bài';
    if(!visible.length){stream.innerHTML='<div style="color:#aaa;padding:3rem;text-align:center">Không có bài</div>';return;}

    const slice=visible.slice(0,shownCount);

    // Smart merge: chỉ thêm DOM node mới, không xoá node cũ còn dùng
    const existingGuids=new Set(
        Array.from(stream.querySelectorAll('.item[data-guid]')).map(el=>el.dataset.guid)
    );
    const newGuids=new Set(slice.map(it=>it.guid||''));

    // Xoá node không còn trong danh sách hiển thị
    stream.querySelectorAll('.item[data-guid]').forEach(el=>{
        if(!newGuids.has(el.dataset.guid)) el.remove();
    });

    // Rebuild theo đúng thứ tự — dùng DocumentFragment để tránh reflow nhiều lần
    const frag=document.createDocumentFragment();
    slice.forEach((it,i)=>{
        const guid=it.guid||('tmp_'+i);
        let el=stream.querySelector(`.item[data-guid="${CSS.escape(guid)}"]`);
        if(!el){
            // Tin mới — tạo node
            el=_buildItemEl(it,guid);
            // Nếu trước đó đang mở → restore
            if(openBodies.has(guid)){
                const body=el.querySelector('.item-body-div');
                if(body) body.style.display='block';
            }
        } else {
            // Tin cũ — cập nhật selected state nếu cần, giữ nguyên open/close
            el.classList.toggle('selected',selected.has(guid));
        }
        frag.appendChild(el);
    });
    stream.innerHTML='';
    stream.appendChild(frag);
}

function _buildItemEl(it,guid){
    const isSel=selected.has(guid);
    const titleText=it.title||(it.category?`(${it.category})`:'(không tiêu đề)');
    const safeGuid=String(guid).replace(/"/g,'&quot;').replace(/'/g,'&#39;');
    const sourceBadge=isTgSource(it.feedUrl||'')?'<span class="feed-badge badge-tg">TG</span>':'';
    const div=document.createElement('div');
    div.className='item'+(isSel?' selected':'');
    div.dataset.guid=guid;
    // Web preview: chỉ hiển thị khi có link (không phải TG source)
    const hasLink=it.link&&it.link.trim()&&!isTgSource(it.feedUrl||'');
    const previewHtml=hasLink?`<div style="margin:8px 0 4px;border:1px solid #e8e8e0;border-radius:8px;overflow:hidden;background:#fafaf8">
        <iframe src="${it.link}" style="width:100%;height:200px;border:none;display:block"
            loading="lazy" sandbox="allow-scripts allow-same-origin allow-popups"
            onerror="this.parentElement.style.display='none'"></iframe>
    </div>`:'';
    div.innerHTML=`
        <div style="display:flex;gap:8px;padding:.85rem">
            <input type="checkbox"${isSel?' checked':''} onchange="toggleSelect('${safeGuid}',this.checked)">
            <div style="flex:1">
                <div style="font-weight:600;font-size:.88rem;color:#1a1a1a">${titleText}${sourceBadge}</div>
                <div style="font-size:11px;color:#aaa;margin-top:3px">${it.feedName||''}</div>
            </div>
            <span onclick="toggleBody('${safeGuid}',this)" style="cursor:pointer;color:#ccc;user-select:none">+</span>
        </div>
        <div class="item-body-div" style="display:none;padding:0 1.1rem .9rem;border-top:1px solid #f4f4f0;line-height:1.6">${it.desc||''}${previewHtml}</div>
        ${it.link?`<div class="item-footer"><a href="${it.link}" target="_blank">Xem bài gốc →</a></div>`:''}`;
    return div;
}

function toggleBody(guid, btn){
    const el=document.querySelector(`.item[data-guid="${CSS.escape(guid)}"]`);
    if(!el) return;
    const body=el.querySelector('.item-body-div');
    if(!body) return;
    const isOpen=body.style.display!=='none';
    body.style.display=isOpen?'none':'block';
    if(btn) btn.textContent=isOpen?'+':'−';
    if(isOpen) openBodies.delete(guid); else openBodies.add(guid);
}

function toggleSelect(guid,checked){
    if(checked) selected.add(guid); else selected.delete(guid);
    updateFwdBar();
    const el=document.querySelector(`[data-guid="${CSS.escape(guid)}"]`);
    if(el) el.classList.toggle('selected',checked);
}
function clearSelection(){
    selected.clear();updateFwdBar();
    document.querySelectorAll('.item.selected').forEach(el=>el.classList.remove('selected'));
    document.querySelectorAll('input[type=checkbox]').forEach(cb=>cb.checked=false);
}

// --- Fetch & Poll ---
async function fetchAndMerge(url,feedName,category,markNew){
    try{
        const feedCfg=feeds.find(f=>f.url===url)||{};
        const historyLimit=feedCfg.history_limit!=null?feedCfg.history_limit:20;
        let items;
        if(isTgSource(url)){
            const r=await fetch('/tl_fetch?url='+encodeURIComponent(url)+'&translate='+(translateOn?'1':'0')+'&category='+encodeURIComponent(category)+'&history_limit='+historyLimit);
            if(!r.ok) throw new Error('HTTP '+r.status);
            const data=await r.json();
            items=data.items||[];
        } else {
            const r=await fetch('/fetch?url='+encodeURIComponent(url)+'&translate='+(translateOn?'1':'0')+'&category='+encodeURIComponent(category)+'&history_limit='+historyLimit);
            if(!r.ok) throw new Error('HTTP '+r.status);
            const data=await r.json();
            items=data.items||[];
        }
        if(!items.length) return;
        const existing=new Set(allItems.map(it=>it.guid));
        items.forEach(it=>{
            if(!existing.has(it.guid)){
                it.feedUrl=url;it.feedName=feedName;it.category=it.category||category;
                it.ts=it.pubDate?new Date(it.pubDate).getTime():Date.now();
                it.isNew=markNew;allItems.push(it);
            }
        });
        allItems.sort((a,b)=>b.ts-a.ts);
        // Giới hạn 50 bài/feed
        const MAX_PER_FEED=50;const fc={};
        allItems=allItems.filter(it=>{const u=it.feedUrl||'';fc[u]=(fc[u]||0)+1;return fc[u]<=MAX_PER_FEED;});
        renderStream();
        renderSidebar(); // cập nhật chấm đỏ sau khi items load
    }catch(e){console.warn('fetch error',url,e);}
}

async function manualRefreshAll(){
    // RSS song song, TG tuần tự với delay để không flood Telethon
    const rssFeed=feeds.filter(f=>!isTgSource(f.url));
    const tgFeed=feeds.filter(f=>isTgSource(f.url));
    await Promise.all(rssFeed.map(f=>fetchAndMerge(f.url,f.name,f.category,false)));
    for(const f of tgFeed){
        await fetchAndMerge(f.url,f.name,f.category,false);
        await new Promise(r=>setTimeout(r,800)); // 800ms delay giữa các TG feed
    }
}

// --- Poll timer ---
// pollNextIn được server reset khi có i-c-a feeds, đếm ngược mỗi giây
function updatePollTimer(){
    const el = document.getElementById('poll-timer');
    const hasIca = feeds.some(f => f.url.includes('tg.i-c-a.su'));
    if(!hasIca){ el.textContent=''; return; }
    if(pollNextIn > 0){
        el.textContent = '· ' + pollNextIn + 's';
    } else {
        el.textContent = '· đang cập nhật...';
    }
}

setInterval(()=>{
    if(pollNextIn > 0) pollNextIn--;
    updatePollTimer();
}, 1000);

// --- WebSocket ---
let wsReconnectCount=0;
let wsHeartbeatTimer=null;
function connectWS(){
    ws=new WebSocket(WS_URL);
    let feedsAckReceived=false;
    let feedsAckTimer=null;
    let feedsPayload=null;

    ws.onopen=()=>{
        wsReady=true;
        document.getElementById('ws-dot').className='ws-dot on';
        document.getElementById('ws-lbl').textContent='Đang kết nối...';
        feedsPayload=feeds.map(f=>({url:f.url,name:f.name,
            show_link:f.show_link!==false,
            auto_fwd:f.auto_fwd===true,
            destinations:f.destinations||[],
            history_limit:f.history_limit!=null?f.history_limit:20}));
        wsReconnectCount++;
        if(wsHeartbeatTimer) clearInterval(wsHeartbeatTimer);
        wsHeartbeatTimer=setInterval(()=>{ if(wsReady) wsSend({type:'heartbeat'}); }, 10000);
        // Gửi feeds qua HTTP ngay khi WS open (không chờ 'connected' frame)
        _sendInitMessages();
    };

    function _sendInitMessages(){
        if(!feedsPayload) return;
        // Gửi settings qua WS chỉ lần đầu
        if(wsReady && !settingsSent){
            settingsSent = true;
            setTimeout(()=>{ if(wsReady) wsSend({type:'tg_settings',channels:tgChannels}); }, 100);
            setTimeout(()=>{ if(wsReady) wsSend({type:'auto_fwd',enabled:autoFwd,channels:tgChannels}); }, 200);
            setTimeout(()=>{ if(wsReady) wsSend({type:'translate_engine',engine:translateEngine}); }, 300);
            setTimeout(()=>{ if(wsReady) wsSend({type:'translate_lang',lang:translateLang}); }, 400);
        }
        // Gửi feeds qua HTTP chỉ 1 lần — không lặp lại mỗi khi WS reconnect
        if(feedsSynced) return;
        feedsSynced = true;  // set ngay để tránh gọi đôi nếu onopen fire 2 lần
        fetch('/sync_feeds', {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({
                feeds: feedsPayload,
                auto_fwd: autoFwd,
                tg_channels: tgChannels,
                translate_engine: translateEngine,
                translate_lang: translateLang
            })
        }).then(r=>r.json()).then(d=>{
            if(d.ok){
                feedsAckReceived=true;
                if(feedsAckTimer){clearTimeout(feedsAckTimer);feedsAckTimer=null;}
                document.getElementById('ws-lbl').textContent='Đang theo dõi';
                console.log('[HTTP] sync_feeds OK, feeds='+d.count);
            }
        }).catch(e=>{
            console.warn('[HTTP] sync_feeds lỗi:',e);
            // Reset để retry sau 5s nếu lỗi mạng thực sự
            feedsSynced=false;
            if(feedsAckTimer) clearTimeout(feedsAckTimer);
            feedsAckTimer=setTimeout(()=>{ _sendInitMessages(); }, 5000);
        });
        // Fetch RSS sau khi init (song song ok vì không dùng Telethon)
        if(wsReconnectCount>1){
            setTimeout(()=>{
                feeds.filter(f=>!isTgSource(f.url)).forEach(f=>fetchAndMerge(f.url,f.name,f.category,false));
            }, 1000);
        }
        // Lần đầu: fetch TG history TUẦN TỰ với delay — tránh flood Telethon loop
        if(wsReconnectCount===1){
            setTimeout(async()=>{
                const tgFeeds=feeds.filter(f=>isTgSource(f.url));
                for(const f of tgFeeds){
                    await fetchAndMerge(f.url,f.name,f.category,false);
                    await new Promise(r=>setTimeout(r,500)); // 500ms delay giữa các feed
                }
            }, 10000);
        }
    }

    ws.onmessage=e=>{
        const msg=JSON.parse(e.data);
        if(msg.type==='heartbeat') return;
        if(msg.type==='connected'){
            // Server sẵn sàng — gửi init messages ngay
            console.log('[WS] Server ready, gửi feeds...');
            _sendInitMessages();
            return;
        }
        if(msg.type==='feeds_ack'){
            feedsAckReceived=true;
            if(feedsAckTimer){clearTimeout(feedsAckTimer);feedsAckTimer=null;}
            document.getElementById('ws-lbl').textContent='Đang theo dõi';
            console.log('[WS] feeds_ack OK, feeds='+msg.count);
            return;
        }
        if(msg.type==='new_items'){
            const feedName=feeds.find(f=>f.url===msg.url)?.name||msg.url;
            const parsed=msg.items.map(it=>({...it,feedUrl:msg.url,feedName,ts:it.pubDate?new Date(it.pubDate).getTime():0,isNew:true}));
            if(filterUrl!==null&&filterUrl!==msg.url) newBadges[msg.url]=(newBadges[msg.url]||0)+parsed.length;
            const existing=new Set(allItems.map(it=>it.guid));
            parsed.forEach(it=>{if(!existing.has(it.guid)) allItems.push(it);});
            allItems.sort((a,b)=>b.ts-a.ts);

            // Giới hạn tối đa 50 bài/feed — xóa bài cũ nhất khi vượt ngưỡng
            const MAX_PER_FEED=50;
            const feedCounts={};
            allItems=allItems.filter(it=>{
                const url=it.feedUrl||'';
                feedCounts[url]=(feedCounts[url]||0)+1;
                return feedCounts[url]<=MAX_PER_FEED;
            });

            renderSidebar();renderStream();
        }
        if(msg.type==='poll_status'){pollInterval=msg.interval;pollNextIn=msg.next_in;}
        if(msg.type==='auto_fwd_sent'){
            showToastMsg(`Auto-forward: đã gửi ${msg.count} tin mới lên Telegram`);
            if(msg.url && msg.total_fwd!=null) {
                _fwdCounts[msg.url] = msg.total_fwd;
                renderSidebar(); // cập nhật badge forward count
            }
        }
        if(msg.type==='error_alert'){
            showErrBanner(msg.msg, msg.url||'');
        }
        if(msg.type==='error_recover'){
            // Feed hoạt động trở lại — tự đóng banner nếu đang hiện lỗi feed đó
            const banner=document.getElementById('err-banner');
            if(banner._errUrl===msg.url) closeErrBanner();
        }
        if(msg.type==='read_all_status'){
            _updateBgIndicator(); // luôn fetch lại toàn bộ jobs từ server
            if(msg.status==='done') showToastMsg('✅ Đọc xong ' + (msg.done||0) + ' tin từ ' + (msg.name||''));
            if(msg.status==='error') showToastMsg('❌ Lỗi job ' + (msg.name||'') + ': ' + (msg.error||''));
        }
    };
    ws.onclose=()=>{
        wsReady=false;
        if(wsHeartbeatTimer){clearInterval(wsHeartbeatTimer);wsHeartbeatTimer=null;}
        if(feedsAckTimer){clearTimeout(feedsAckTimer);feedsAckTimer=null;}
        document.getElementById('ws-dot').className='ws-dot wait';
        document.getElementById('ws-lbl').textContent='Mất kết nối...';
        // Exponential backoff: 3s → 6s → 12s → 24s → 60s (tránh reconnect loop)
        _wsRetryDelay = Math.min((_wsRetryDelay||3000) * 2, 60000);
        setTimeout(connectWS, _wsRetryDelay);
    };
    ws.onopen=ws.onopen||(()=>{});
    const _origOnOpen = ws.onopen;
    ws.onopen = function(e){ _wsRetryDelay=3000; if(_origOnOpen) _origOnOpen(e); };
}
function wsSend(obj){if(ws&&wsReady)ws.send(JSON.stringify(obj));}

function showToastMsg(msg){
    const t=document.getElementById('toast');
    const el=document.createElement('div');
    el.className='toast-item';el.textContent=msg;t.appendChild(el);
    setTimeout(()=>el.remove(),4000);
}

(function(){
    renderSidebar();
    // Kiểm tra job ngầm đang chạy khi mở lại trang
    // Kiểm tra jobs đang chạy ngầm khi mở lại trang
    _updateBgIndicator().then(() => {
        // Nếu có job running thì bật poll indicator
        fetch('/tg_read_all_status', {method:'POST',headers:{'Content-Type':'application/json'},body:'{}'})
            .then(r=>r.json()).then(d=>{
                const running = Object.values(d.jobs||{}).filter(j=>j.status==='running');
                if(running.length > 0) _startBgPoll();
            }).catch(()=>{});
    }).catch(()=>{});
    checkTelethonStatus();
    // Khôi phục engine và ngôn ngữ đã chọn từ localStorage
    const sel=document.getElementById('engine-select');
    if(sel) sel.value=translateEngine;
    const langSel=document.getElementById('lang-select');
    if(langSel) langSel.value=translateLang;
    connectWS();
    // Chỉ load RSS feeds ngay lập tức
    // TG feeds: không gọi /tl_fetch lúc init vì server đang bận setup realtime listeners
    // TG feed history sẽ đến qua WS broadcast từ _init_tg_feed
    const rssFeeds=feeds.filter(f=>!isTgSource(f.url));
    rssFeeds.forEach(f=>fetchAndMerge(f.url,f.name,f.category,false));
    // Hiện nút logout nếu server có bật auth
    fetch('/auth_status').then(r=>r.json()).then(d=>{
        if(d.auth_enabled) document.getElementById('logout-btn').style.display='';
    }).catch(()=>{});
})();

// ===================== ERROR BANNER =====================
let _errBannerTimer = null;
let _errBannerUrl   = '';

function showErrBanner(msg, feedUrl) {
    const banner = document.getElementById('err-banner');
    const msgEl  = document.getElementById('eb-msg');
    const delBtn = document.getElementById('eb-del-btn');
    const title  = document.getElementById('eb-title-text');
    if (!banner) return;
    // Nếu đang hiện lỗi khác → stack (ghi đè)
    banner._errUrl = feedUrl;
    _errBannerUrl  = feedUrl;
    title.textContent = feedUrl
        ? (feeds.find(f=>f.url===feedUrl)?.name || feedUrl)
        : 'Lỗi hệ thống';
    msgEl.textContent = msg;
    delBtn.style.display = feedUrl ? '' : 'none';
    banner.style.display = 'block';
    // Tự đóng sau 30 giây
    if (_errBannerTimer) clearTimeout(_errBannerTimer);
    _errBannerTimer = setTimeout(closeErrBanner, 30000);
}

function closeErrBanner() {
    const banner = document.getElementById('err-banner');
    if (banner) banner.style.display = 'none';
    if (_errBannerTimer) { clearTimeout(_errBannerTimer); _errBannerTimer = null; }
}

function deleteErrFeed() {
    const url = _errBannerUrl;
    if (!url) return;
    const idx = feeds.findIndex(f => f.url === url);
    if (idx < 0) return;
    if (!confirm(`Xóa feed "${feeds[idx].name}"?`)) return;
    feeds.splice(idx, 1);
    allItems = allItems.filter(it => it.feedUrl !== url);
    if (filterUrl === url) filterUrl = null;
    saveFeeds(); syncFeedsHttp();
    renderSidebar(); renderStream();
    closeErrBanner();
    showToastMsg('🗑 Đã xóa feed lỗi');
}

async function testNotify() {
    const bell = document.getElementById('notify-bell-btn');
    if (bell) bell.textContent = '⏳';
    try {
        const r = await fetch('/test_notify', {method:'POST', headers:{'Content-Type':'application/json'}, body:'{}'});
        const d = await r.json();
        if (bell) bell.textContent = '🔔';
        if (d.ok) {
            showToastMsg('✅ Đã gửi thông báo test tới Telegram thành công!');
        } else {
            showToastMsg('❌ Gửi thất bại: ' + (d.error || 'Lỗi không xác định'));
        }
    } catch(e) {
        if (bell) bell.textContent = '🔔';
        showToastMsg('❌ Lỗi kết nối: ' + e.message);
    }
}

async function doLogout(){
    if(!confirm('Đăng xuất?')) return;
    await fetch('/logout',{method:'POST',headers:{'Content-Type':'application/json'},body:'{}'});
    window.location.reload();
}
</script>
</body>
</html>"""

# --- SERVER LOGIC ---

def fetch_feed(url):
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Accept': 'application/rss+xml, application/xml, text/xml, */*'
    }
    req = urllib.request.Request(url, headers=headers)
    return urllib.request.urlopen(req, timeout=8).read()

def parse_items(xml_bytes, category_hint='', limit=None):
    root = ET.fromstring(xml_bytes)
    is_atom = 'feed' in root.tag
    ns = '{http://www.w3.org/2005/Atom}'
    # Namespaces cho MediaRSS (VnExpress và các feed chuẩn MediaRSS)
    _NS_MEDIA = ['{http://search.yahoo.com/mrss/}', '{http://www.rssboard.org/media-rss}']
    entries = (root.findall(f'.//{ns}entry') or root.findall('.//entry')) if is_atom else root.findall('.//item')
    if limit and limit > 0:
        entries = entries[:limit]
    items = []
    for e in entries:
        def g(*tags):
            for t in tags:
                el = e.find(t)
                if el is not None and el.text:
                    return el.text.strip()
            return ''
        if is_atom:
            le = e.find(f'{ns}link') or e.find('link')
            link = (le.get('href') or le.text or '').strip() if le is not None else ''
            guid = g(f'{ns}id', 'id') or link
            title = g(f'{ns}title', 'title')
            pub = g(f'{ns}published', f'{ns}updated', 'published', 'updated')
            de = e.find(f'{ns}content') or e.find(f'{ns}summary') or e.find('content') or e.find('summary')
            desc = de.text.strip() if de is not None and de.text else ''
        else:
            link = g('link')
            guid = g('guid') or link
            title = g('title')
            pub = g('pubDate')
            d = e.find('description')
            desc = d.text.strip() if d is not None and d.text else ''

        # --- Trích xuất media URL từ enclosure / media:content / media:thumbnail ---
        # VnExpress RSS để ảnh trong các tag này thay vì <img> trong <description>
        media_url = None

        # 1. <enclosure url="..." type="image/...">
        enc = e.find('enclosure')
        if enc is not None:
            enc_url  = enc.get('url', '')
            enc_type = enc.get('type', '')
            if enc_url and ('image' in enc_type or enc_url.lower().split('?')[0].endswith(('.jpg', '.jpeg', '.png', '.webp', '.gif'))):
                media_url = enc_url

        # 2. <media:content url="..."> (thử tất cả namespace variants)
        if not media_url:
            for _ns_m in _NS_MEDIA:
                mc = e.find(f'{_ns_m}content')
                if mc is not None:
                    mc_url    = mc.get('url', '')
                    mc_medium = mc.get('medium', '')
                    if mc_url and ('image' in mc_medium or not mc_medium):
                        media_url = mc_url
                        break

        # 3. <media:thumbnail url="...">
        if not media_url:
            for _ns_m in _NS_MEDIA:
                mt = e.find(f'{_ns_m}thumbnail')
                if mt is not None:
                    mt_url = mt.get('url', '')
                    if mt_url:
                        media_url = mt_url
                        break

        # 4. Nhúng media_url vào desc để extract_media() hoạt động bình thường
        if media_url and f'src="{media_url}"' not in desc and f"src='{media_url}'" not in desc:
            desc = f'<img src="{media_url}"/>' + ('\n' if desc else '') + desc

        items.append({'guid': guid, 'title': title, 'link': link, 'pubDate': pub, 'desc': desc, 'translated': False, 'category': category_hint})
    return items

def process_items(items):
    if not translate_enabled or not TRANSLATE_AVAILABLE:
        return items
    results = [None] * len(items)
    def do(i, it):
        t, d, was = maybe_translate(it['title'], it['desc'])
        results[i] = {**it, 'title': t, 'desc': d, 'translated': was}
    futures = [_thread_pool.submit(do, i, it) for i, it in enumerate(items)]
    for f in futures:
        try:
            f.result(timeout=15)
        except Exception:
            pass
    return [r for r in results if r is not None]

def process_tg_items(items):
    """Dịch items từ Telethon — dịch desc trước, sau đó derive title từ desc đã dịch"""
    if not translate_enabled or not TRANSLATE_AVAILABLE:
        return items
    results = [None] * len(items)
    def do(i, it):
        desc_plain = strip_html(it.get('desc', '').replace('<br>', '\n'))

        if is_same_as_target(desc_plain):
            results[i] = {**it, 'translated': False}
            return

        # Dịch toàn bộ text gốc (plain text, không có HTML)
        translated_desc_plain = _translate_with_engine(desc_plain[:4000]) or desc_plain

        # Re-derive title từ bản dịch
        title_translated = (translated_desc_plain[:80] + '...') if len(translated_desc_plain) > 80 else translated_desc_plain
        # Convert desc plain đã dịch về dạng HTML với <br>
        desc_translated = translated_desc_plain.replace('\n', '<br>')

        results[i] = {**it, 'title': title_translated, 'desc': desc_translated, 'translated': True}

    futures = [_thread_pool.submit(do, i, it) for i, it in enumerate(items)]
    for f in futures:
        try:
            f.result(timeout=15)
        except Exception:
            pass
    return [r for r in results if r is not None]

# Semaphore để serialize Telethon calls — tránh conflict khi nhiều kênh poll cùng lúc
# Dùng asyncio.Semaphore vì được dùng bên trong async functions với await
# Sẽ được khởi tạo lại trong run_tg_loop() sau khi asyncio loop chạy
tg_semaphore = None
# Semaphore async giới hạn concurrent history loading — tránh block forward
_tg_history_semaphore = None  # khởi tạo sau khi asyncio loop chạy
# Semaphore HTTP giới hạn concurrent /tl_fetch từ browser — tối đa 2 cùng lúc
_tl_fetch_semaphore = threading.Semaphore(2)

def broadcast(msg):
    data = json.dumps(msg, ensure_ascii=False)
    dead = set()
    with lock:
        clients = list(ws_clients)
    for c in clients:
        try:
            c.send(data)
        except:
            dead.add(c)
    with lock:
        ws_clients.difference_update(dead)

def _split_text(text, max_len):
    """Chia text dài thành các chunk không quá max_len ký tự, ưu tiên cắt theo dòng"""
    if len(text) <= max_len:
        return [text]
    chunks = []
    while text:
        if len(text) <= max_len:
            chunks.append(text)
            break
        # Tìm điểm cắt tốt nhất: ưu tiên newline, rồi khoảng trắng
        cut = text.rfind('\n', 0, max_len)
        if cut < max_len // 2:
            cut = text.rfind(' ', 0, max_len)
        if cut < 0:
            cut = max_len
        chunks.append(text[:cut].strip())
        text = text[cut:].strip()
    return [c for c in chunks if c]


async def _resolve_dest(dest_channel):
    """
    Resolve kênh đích về Telethon entity.
    - @username → dùng trực tiếp (Telethon tự resolve)
    - ID số (-100xxx hoặc xxx) → dùng PeerChannel để Telethon nhận ra
    Trả về entity đã resolve, hoặc raise nếu không tìm được.
    """
    dest = str(dest_channel).strip()
    # Nếu là ID số (có thể có dấu -)
    if dest.lstrip('-').isdigit():
        channel_id = int(dest)
        # Telethon dùng PeerChannel với ID không có prefix -100
        if str(channel_id).startswith('-100'):
            channel_id = int(str(channel_id)[4:])  # bỏ prefix -100
        from telethon.tl.types import PeerChannel
        try:
            entity = await tg_client.get_entity(PeerChannel(channel_id))
            return entity
        except Exception:
            # Fallback: thử get_input_entity với ID gốc
            entity = await tg_client.get_input_entity(int(dest))
            return entity
    # Username (@channel hoặc channel)
    return dest


async def _collect_topic_ids(entity, input_entity, tid_int):
    """
    Thu thap TẤT CA msg_id trong forum topic dung GetRepliesRequest.
    Tra ve list[int] sort tang dan (cu -> moi).
    """
    from telethon.tl.functions.messages import GetRepliesRequest
    seen    = set()
    all_ids = []
    offset_id = 0
    limit     = 100

    while True:
        result = await tg_client(GetRepliesRequest(
            peer        = input_entity,
            msg_id      = tid_int,
            offset_id   = offset_id,
            offset_date = 0,
            add_offset  = 0,
            limit       = limit,
            max_id      = 0,
            min_id      = 0,
            hash        = 0,
        ))
        msgs = getattr(result, 'messages', [])
        if not msgs:
            break
        for msg in msgs:
            if msg is not None and msg.id not in seen:
                seen.add(msg.id)
                all_ids.append(msg.id)
        if len(all_ids) % 500 == 0 and len(all_ids) > 0:
            print(f'[Cleanup] Dang thu thap... {len(all_ids)} msgs topic={tid_int}')
        if len(msgs) < limit:
            break
        offset_id = min(m.id for m in msgs if m is not None)
        await asyncio.sleep(0.3)

    all_ids.sort()
    print(f'[Cleanup] Tong thu thap: {len(all_ids)} msgs trong topic={tid_int}')
    return all_ids

async def _tg_send_item(dest_channel, item, caption, topic_id=None, desc_has_link=False):
    """
    Gửi 1 item lên kênh/group đích qua Telethon (không dùng Bot API).

    Logic:
    - Nếu là tin TG nguồn có media → dùng msg.media object gốc (forward không download)
      * grouped media (album) → gom tất cả msg trong group, send_file với list media
      * single media → send_file(media)
    - Nếu là tin RSS có media URL → send_file(url) — Telegram tự fetch
    - Nếu không có media → send_message(text)

    topic_id: int hoặc None — nếu là Group có Topics, truyền message_thread_id để gửi vào đúng topic.
    Không bao giờ download bytes về RAM.
    Hỗ trợ cả @username lẫn ID số (-100xxx).
    """
    if not tg_client:
        return False
    try:
        # Resolve entity trước — xử lý cả @username lẫn -100xxx
        dest      = await _resolve_dest(dest_channel)
        source    = item.get('_source', '')
        msg_id    = item.get('_tg_msg_id')
        chat      = item.get('_tg_chat')
        grouped   = item.get('_tg_grouped_id')
        has_media = item.get('_tg_has_media', False)
        rss_media = item.get('_rss_media_url')
        # Web preview chỉ bật khi: có URL thật trong nội dung VÀ tin không có media
        # Tin có hình/video/audio → tắt preview (Telegram sẽ tách media nếu bật)
        show_preview = desc_has_link and not has_media and not rss_media

        # reply_to = topic_id nếu gửi vào topic của group, None nếu gửi bình thường
        thread_reply = int(topic_id) if topic_id else None

        if source == 'telethon' and has_media and msg_id and chat:
            # --- TG nguồn có media: dùng msg.media object trực tiếp ---
            from telethon.tl.types import MessageMediaWebPage, MessageMediaDocument
            from telethon.tl.types import DocumentAttributeAudio, DocumentAttributeVideo
            async with tg_semaphore:
                if grouped:
                    # Mở rộng range tìm kiếm: album có thể span rộng quanh msg_id
                    # msg_id là ID của msg đầu tiên (nhỏ nhất) trong album
                    id_range = list(range(max(1, msg_id - 2), msg_id + 30))
                    all_msgs = await tg_client.get_messages(chat, ids=id_range)
                    if not isinstance(all_msgs, list):
                        all_msgs = [all_msgs] if all_msgs else []
                    # Lọc đúng grouped_id, bỏ WebPage, sắp xếp theo msg.id tăng dần
                    group_msgs = sorted(
                        [m for m in all_msgs
                         if m and m.grouped_id == grouped and m.media
                         and not isinstance(m.media, MessageMediaWebPage)],
                        key=lambda m: m.id
                    )
                    if not group_msgs:
                        # Fallback: chỉ lấy msg_id gốc
                        single = await tg_client.get_messages(chat, ids=msg_id)
                        is_real = single and single.media and not isinstance(single.media, MessageMediaWebPage)
                        group_msgs = [single] if is_real else []
                else:
                    msgs = await tg_client.get_messages(chat, ids=msg_id)
                    msg  = msgs if not isinstance(msgs, list) else (msgs[0] if msgs else None)
                    if msg and msg.media and isinstance(msg.media, MessageMediaWebPage):
                        msg = None
                    group_msgs = [msg] if msg and msg.media else []

            if group_msgs:
                media_list = [m.media for m in group_msgs[:10]]

                # Nếu caption truyền vào rỗng (media-only), thử lấy text từ msg nguồn
                if not caption.strip():
                    for gm in group_msgs:
                        if gm and gm.message and gm.message.strip():
                            caption = gm.message
                            break

                # Chỉ treat là audio group khi TẤT CẢ file đều là pure audio (DocumentAttributeAudio).
                # Video album và photo album Telegram hỗ trợ caption bình thường → không tách.
                def _is_pure_audio(media):
                    if not isinstance(media, MessageMediaDocument):
                        return False
                    doc = getattr(media, 'document', None)
                    if not doc:
                        return False
                    attrs = getattr(doc, 'attributes', [])
                    has_audio = any(isinstance(a, DocumentAttributeAudio) for a in attrs)
                    has_video = any(isinstance(a, DocumentAttributeVideo) for a in attrs)
                    return has_audio and not has_video

                is_audio_group = len(media_list) > 1 and all(_is_pure_audio(m) for m in media_list)

                if len(media_list) == 1:
                    # Tin đơn: gắn caption vào media
                    caption_plain_len = len(re.sub(r'<[^>]+>', '', caption))
                    caption_val = caption if caption.strip() else None
                    if caption_plain_len <= 1024:
                        await tg_client.send_file(dest, media_list[0], caption=caption_val,
                                                  parse_mode='html', reply_to=thread_reply,
                                                  link_preview=show_preview)
                    else:
                        sent = await tg_client.send_file(dest, media_list[0], caption=None,
                                                         reply_to=thread_reply)
                        reply_to_id = sent.id if not isinstance(sent, list) else sent[0].id
                        for chunk in _split_text(caption, 4096):
                            await tg_client.send_message(dest, chunk, parse_mode='html',
                                                         reply_to=reply_to_id, link_preview=show_preview)
                elif is_audio_group:
                    # Audio/document group: gửi từng file riêng không caption,
                    # sau đó gửi caption thành tin nhắn text riêng
                    for audio_media in media_list:
                        await tg_client.send_file(dest, audio_media, caption=None,
                                                  reply_to=thread_reply)
                    if caption.strip():
                        for chunk in _split_text(caption, 4096):
                            await tg_client.send_message(dest, chunk, parse_mode='html',
                                                         reply_to=thread_reply,
                                                         link_preview=show_preview)
                else:
                    # Photo/video album: Telegram hỗ trợ caption trên album
                    caption_plain_len = len(re.sub(r'<[^>]+>', '', caption))
                    caption_val = caption if caption.strip() else None
                    if caption_plain_len <= 1024:
                        await tg_client.send_file(dest, media_list, caption=caption_val,
                                                  parse_mode='html', reply_to=thread_reply,
                                                  link_preview=show_preview)
                    else:
                        # Caption dài: gửi album trước, rồi text sau
                        await tg_client.send_file(dest, media_list, caption=None,
                                                  reply_to=thread_reply)
                        for chunk in _split_text(caption, 4096):
                            await tg_client.send_message(dest, chunk, parse_mode='html',
                                                         reply_to=thread_reply,
                                                         link_preview=show_preview)
                return True
            # Không lấy được media → fallback text
            for chunk in _split_text(caption, 4096):
                await tg_client.send_message(dest, chunk, parse_mode='html',
                                             reply_to=thread_reply, link_preview=show_preview)
            return True

        elif rss_media:
            # Telegram giới hạn caption kèm media tối đa 1024 ký tự hiển thị (sau strip HTML)
            caption_plain_len = len(re.sub(r'<[^>]+>', '', caption))

            # Download ảnh về bytes trước — tránh Telegram không fetch được URL từ CDN
            # (VnExpress và một số CDN block Telegram server IP)
            _media_to_send = rss_media  # default: dùng URL trực tiếp
            if rss_media.lower().split('?')[0].endswith(('.jpg', '.jpeg', '.png', '.webp', '.gif')):
                try:
                    import io
                    _img_req = urllib.request.Request(
                        rss_media,
                        headers={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
                    )
                    with urllib.request.urlopen(_img_req, timeout=10) as _img_resp:
                        _img_bytes = _img_resp.read()
                    if _img_bytes:
                        _media_to_send = io.BytesIO(_img_bytes)
                        _media_to_send.name = rss_media.split('/')[-1].split('?')[0] or 'image.jpg'
                except Exception as _dl_err:
                    print(f'[TG Send] Download ảnh lỗi ({_dl_err}) — thử URL trực tiếp')
                    _media_to_send = rss_media

            try:
                if caption_plain_len <= 1024:
                    await tg_client.send_file(dest, _media_to_send, caption=caption,
                                              parse_mode='html', reply_to=thread_reply,
                                              link_preview=show_preview)
                else:
                    # Caption quá dài — gửi media không caption, reply text riêng
                    sent = await tg_client.send_file(dest, _media_to_send, caption='',
                                                      parse_mode='html', reply_to=thread_reply)
                    reply_to = sent.id if not isinstance(sent, list) else sent[0].id
                    for chunk in _split_text(caption, 4096):
                        await tg_client.send_message(dest, chunk, parse_mode='html',
                                                     reply_to=reply_to, link_preview=show_preview)
            except Exception as media_err:
                err_str = str(media_err)
                if 'Webpage media empty' in err_str or 'fetching the webpage' in err_str or 'WEBPAGE_MEDIA_EMPTY' in err_str:
                    # URL preview lỗi → fallback gửi text + link, không kèm media
                    print(f'[TG Send] RSS media preview lỗi, fallback text: {media_err}')
                    for chunk in _split_text(caption, 4096):
                        await tg_client.send_message(dest, chunk, parse_mode='html',
                                                     reply_to=thread_reply, link_preview=False)
                else:
                    raise
            return True

        else:
            for chunk in _split_text(caption, 4096):
                await tg_client.send_message(dest, chunk, parse_mode='html',
                                             reply_to=thread_reply, link_preview=show_preview)
            return True

    except Exception as e:
        print(f'[TG Send] Lỗi gửi {dest_channel} topic={topic_id}: {e}')
        return False


# Semaphore giới hạn số lượng Telethon send đồng thời — tránh flood MTProto
_send_semaphore = threading.Semaphore(3)  # tối đa 3 send song song

def _do_forward(processed, category, url):
    """
    Gửi tin lên các kênh đích qua Telethon.
    Đọc feed.destinations = [{ch_idx, topic_ids}] — topic_ids là string phân cách bằng dấu phẩy.
    Tương thích ngược với target_channels cũ (array of int).
    """
    with lock:
        do_fwd   = auto_fwd_enabled
        cfgs     = list(tg_channels)
        feed_cfg = next((u for u in watched_urls if u['url'] == url), {})
    if not do_fwd:
        return
    if not cfgs:
        return
    if not feed_cfg.get('auto_fwd', True):
        return
    if not TELETHON_AVAILABLE or tg_client is None:
        print('[Forward] Telethon chưa kết nối — bỏ qua forward')
        return
    if url not in _forward_ready_feeds:
        return  # Feed này chưa init xong — bỏ qua tin cũ lúc khởi động

    show_link = feed_cfg.get('show_link', True)

    # --- Resolve destinations từ feed config ---
    # Format mới: destinations = [{ch_idx, topic_ids}, ...]
    # Format cũ:  target_channels = [0, 1, 2, ...]  (chỉ index, không topic)
    destinations = feed_cfg.get('destinations') or []
    if not destinations:
        # fallback tương thích ngược
        old = feed_cfg.get('target_channels', [])
        destinations = [{'ch_idx': i, 'topic_ids': ''} for i in old] if old else []

    # Nếu feed không cấu hình đích nào → gửi tới tất cả kênh (hành vi cũ)
    if not destinations:
        destinations = [{'ch_idx': i, 'topic_ids': ''} for i in range(len(cfgs))]

    total_sent = 0

    for dest_cfg in destinations:
        ch_idx    = dest_cfg.get('ch_idx', dest_cfg) if isinstance(dest_cfg, dict) else dest_cfg
        topic_ids_str = dest_cfg.get('topic_ids', '') if isinstance(dest_cfg, dict) else ''

        if ch_idx < 0 or ch_idx >= len(cfgs):
            continue
        ch   = cfgs[ch_idx]
        dest = ch.get('username') or ch.get('channel_id', '')
        if not dest:
            print(f'[Forward] Kênh "{ch.get("name")}" thiếu username — bỏ qua')
            continue

        channel_name = ch.get('name', dest)

        # Parse topic IDs: "123, 456" → [123, 456]; "" → [None]
        raw_topics = [t.strip() for t in topic_ids_str.split(',') if t.strip()] if topic_ids_str else []
        topic_list = [int(t) for t in raw_topics if t.isdigit()] or [None]

        for topic_id in topic_list:
            for it in reversed(processed):
                # Chuyển <br> → \n trước, rồi strip các HTML tag còn lại
                # (strip_html thay tag bằng space nên phải xử lý <br> riêng trước)
                desc_raw = it.get('desc', '') or ''
                # desc luôn là plain text với \n thật (cả TG realtime lẫn read_all)
                # Chỉ convert <br> nếu là RSS feed còn sót lại
                if '<br' in desc_raw.lower():
                    desc_plain = re.sub(r'(<br\s*/?>){2,}', '\n\n', desc_raw, flags=re.I)
                    desc_plain = re.sub(r'<br\s*/?>', '\n', desc_plain, flags=re.I)
                    desc_plain = re.sub(r'<[^>]+>', '', desc_plain)
                else:
                    desc_plain = re.sub(r'<[^>]+>', '', desc_raw)
                # Nếu có link ẩn → dùng text đã expand (link ẩn → URL nổi ngay tại chỗ)
                _expanded = _expand_hidden_links_to_text(it)
                if _expanded:
                    caption = _expanded
                else:
                    caption = desc_plain.rstrip()
                # desc_has_link: chỉ check https link trong nội dung gốc, bỏ qua t.me
                # Bật web preview chỉ khi nội dung GỐC có external link
                # Check từ desc_plain (content gốc, không tính Xem bài gốc + channel name)
                # t.me links không cần preview, nhưng bbc.com, youtube.com... thì có
                # Check link trong tất cả các field có thể chứa URL gốc
                _raw_for_check = (it.get('text', '') or it.get('_tg_html_text', '') or it.get('desc', '') or '')
                # Bài có link ẩn (MessageEntityTextUrl) luôn coi là có link → bật web preview
                desc_has_link = it.get('_tg_has_hidden_link', False) or bool(re.search(r'https?://', _raw_for_check))

                # Thêm prefix engine dịch vào đầu caption
                _eng_used = it.get('_translate_engine_used', '')
                _eng_prefix = {
                    'gemini-2.5': 'GM2.5', 'gemini-2.5-lite': 'GM2.5L', 'gemini-3.0': 'GM3.0', 'gemini-3.1': 'GM3.1',
                    'gemini': 'GM', 'deepl': 'DL', 'google': 'GT',
                }.get(_eng_used, '')
                if _eng_prefix and caption.strip():
                    caption = f'<b>{_eng_prefix}:</b> ' + caption

                if show_link and it.get('link'):
                    caption += f'\n\n<a href="{it["link"]}">Xem bài gốc →</a>'
                if channel_name:
                    caption += f'\n\n<i>{channel_name}</i>'

                # Dedup per guid+dest+topic — tránh gửi trùng khi nhiều job song song
                _fwd_key = f'{it.get("guid","")}|{dest}|{topic_id}'
                with _fwd_sent_lock:
                    if _fwd_key in _fwd_sent_keys:
                        print(f'[Forward] Skip trùng: {_fwd_key[:80]}')
                        continue
                    _fwd_sent_keys.add(_fwd_key)

                acquired = _send_semaphore.acquire(timeout=10)
                if not acquired:
                    print(f'[Forward] Bỏ qua tin — _send_semaphore timeout')
                    continue
                try:
                    ok = tg_run(_tg_send_item(dest, it, caption, topic_id=topic_id, desc_has_link=desc_has_link))
                except Exception as e:
                    print(f'[Forward] tg_run lỗi: {e}')
                    ok = False
                    _notify_error(f'❌ Forward thất bại → {channel_name}\n{e}', feed_url=url)
                finally:
                    _send_semaphore.release()
                if ok:
                    total_sent += 1
                time.sleep(0.3)

    if total_sent > 0:
        # Cập nhật forward count
        with _fwd_count_lock:
            _fwd_counts[url] = _fwd_counts.get(url, 0) + total_sent
        broadcast({'type': 'auto_fwd_sent', 'count': total_sent, 'url': url,
                   'total_fwd': _fwd_counts.get(url, 0)})


def _run_read_all_bg(url, feed_cfg):
    """
    Chạy "đọc toàn bộ nguồn" trên background thread — không cần browser mở.
    Cập nhật _read_all_job để UI có thể poll tiến độ bất cứ lúc nào.
    """
    import random as _random

    channel      = normalize_tg_channel(url)
    show_link    = feed_cfg.get("show_link", True)
    category     = feed_cfg.get("category", "")
    do_translate = feed_cfg.get("do_translate", True)
    feed_name    = feed_cfg.get("name", url)

    def _upd(**kw):
        with _read_all_job_lock:
            if url in _read_all_jobs:
                _read_all_jobs[url].update(kw)

    # KHÔNG reset lại job ở đây — server handler đã khởi tạo job rồi.
    # Reset ở đây sẽ làm mất trạng thái và gây đếm sai.
    _upd(current='Đang lấy danh sách tin từ kênh...')

    try:
        async def _fetch_all():
            msgs = []
            async for msg in tg_client.iter_messages(channel, reverse=True):
                msgs.append(msg)
            return msgs

        all_msgs = tg_run_long(_fetch_all(), timeout=300)
        total = len(all_msgs)

        # Load checkpoint từ Redis nếu có (resume sau crash)
        if _deduplicator:
            try:
                ck_key = f'readall_ckpt:{url}'
                ck_data = _deduplicator.r.get(ck_key)
                if ck_data:
                    ck_guids = set(json.loads(ck_data.decode('utf-8')))
                    with lock:
                        if url not in known_guids:
                            known_guids[url] = set()
                        before = len(known_guids[url])
                        known_guids[url].update(ck_guids)
                        after = len(known_guids[url])
                    if after > before:
                        print(f'[ReadAll] Resume từ checkpoint: +{after-before} guids đã có')
            except Exception as _ck_e:
                print(f'[ReadAll] Load checkpoint lỗi (bỏ qua): {_ck_e}')

        # Snapshot known_guids tại thời điểm bắt đầu để xác định tin nào đã được xử lý
        with lock:
            already_forwarded = frozenset(known_guids.get(url, set()))

        # Đếm chính xác: số tin đã có trong known_guids TRƯỚC khi job này bắt đầu
        already_done = sum(1 for msg in all_msgs if msg and
                           f'tg_@{channel.lstrip("@")}_{msg.id}' in already_forwarded)

        # Tổng bản tin cần xử lý = tổng - đã xử lý trước
        remaining = total - already_done
        _upd(done=already_done, total=total,
             current=f'Tìm thấy {total} tin — đã forward {already_done}, còn {remaining} tin cần xử lý...')

        done = already_done  # counter bắt đầu từ số đã forward thực sự
        _sent_groups = set()  # grouped_id đã gửi — tránh gửi album nhiều lần
        for msg in all_msgs:
            if url in _read_all_stop_urls:
                break
            if not msg:
                continue

            msg_text      = msg.message or ""
            chat_username = channel.lstrip("@")
            guid  = f"tg_@{chat_username}_{msg.id}"
            link  = f"https://t.me/{chat_username}/{msg.id}"
            from telethon.tl.types import (
                MessageEntityTextUrl, MessageEntityBold, MessageEntityItalic,
                MessageEntityUnderline, MessageEntityStrike, MessageEntityCode, MessageEntityPre,
            )
            desc  = msg_text  # giữ nguyên newline, KHÔNG replace thành <br>
            title = (msg_text[:80] + "...") if len(msg_text) > 80 else (msg_text or f"[Media] @{chat_username}")
            has_media = bool(msg.media)
            grouped   = getattr(msg, "grouped_id", None)

            # Detect link ẩn và format entities (bold/italic/underline...)
            _has_hidden_link = bool(
                msg_text and msg.entities and
                any(isinstance(e, MessageEntityTextUrl) for e in msg.entities)
            )
            _has_format = bool(
                msg_text and msg.entities and
                any(isinstance(e, (MessageEntityBold, MessageEntityItalic, MessageEntityUnderline,
                                   MessageEntityStrike, MessageEntityCode, MessageEntityPre)) for e in msg.entities)
            )
            _html_text = tg_html.unparse(msg_text, msg.entities) if (_has_hidden_link or _has_format) else ''

            # Atomic check+add: tránh race condition giữa các job song song
            with lock:
                if url not in known_guids:
                    known_guids[url] = set()
                if guid in known_guids[url]:
                    continue  # đã forward, bỏ qua
                # Claim ngay trong lock — job khác sẽ thấy và skip
                known_guids[url].add(guid)

            # Skip nếu là msg phụ trong album đã được gửi qua msg đầu tiên
            if grouped and grouped in _sent_groups:
                # guid đã ghi ở trên, chỉ tăng counter
                done += 1
                _upd(done=done, current=title)
                continue

            item = {
                "guid": guid, "title": title, "desc": desc, "link": link,
                "pubDate": msg.date.isoformat() if msg.date else "",
                "translated": False, "category": category,
                "show_link": show_link, "feed_url": url,
                "_tg_has_media": has_media,
                "_tg_msg_id": msg.id, "_tg_chat": chat_username,
                "_tg_grouped_id": grouped,
                "_source": "telethon",
                "do_translate": do_translate,
                "_tg_has_hidden_link": _has_hidden_link,
                "_tg_has_format": _has_format,
                "_tg_html_text": _html_text,
            }

            # Auto-skip nếu text đã là ngôn ngữ đích — tránh dịch thừa cho feed tiếng Việt
            _effective_translate = do_translate and msg_text and not is_same_as_target(msg_text[:200])

            if _effective_translate:
                item["text"] = msg_text
                if _has_hidden_link or _has_format:
                    translated, _eng, _ = _translate_with_hidden_links(_html_text)
                    # Nếu kết quả rỗng → fallback Google plain text
                    if not translated or not translated.strip():
                        _plain = re.sub(r'<[^>]+>', '', _html_text).strip()
                        translated = _fast_translate(_plain) if _plain else msg_text
                        _eng = 'google'
                    item["_tg_html_text"] = translated
                    item["_translate_engine_used"] = _eng
                else:
                    translated = _fast_translate(msg_text)
                    # Nếu kết quả rỗng → fallback Google trực tiếp
                    if not translated or not translated.strip():
                        try:
                            _gtarget = _GOOGLE_LANG_CODE.get(translate_target_lang, translate_target_lang)
                            translated = GoogleTranslator(source='auto', target=_gtarget).translate(msg_text) or msg_text
                        except Exception:
                            translated = msg_text
                        _tl_engine.used = 'google'
                    item["_translate_engine_used"] = getattr(_tl_engine, 'used', '')
                item["desc"] = translated
                item["title"] = (translated[:80] + "...") if len(translated) > 80 else translated
                item["translated"] = True
            else:
                item["_translate_engine_used"] = ''  # Không gán prefix engine

            # Đánh dấu grouped_id đã xử lý (guid đã được ghi atomic ở trên)
            if grouped:
                _sent_groups.add(grouped)

            if auto_fwd_enabled and tg_channels:
                # Đảm bảo feed được đánh dấu ready trước khi _do_forward check
                _forward_ready_feeds.add(url)
                _do_forward([item], category, url)

            done += 1
            _upd(done=done, current=title)

            # Checkpoint mỗi 20 tin — lưu progress vào Redis để resume sau crash
            if done % 20 == 0 and _deduplicator:
                try:
                    ck_key = f'readall_ckpt:{url}'
                    with lock:
                        guids_snapshot = list(known_guids.get(url, set()))
                    # Lưu dạng JSON vào Redis, TTL 7 ngày
                    _deduplicator.r.set(ck_key,
                                        json.dumps(guids_snapshot).encode('utf-8'),
                                        ex=7*24*3600)
                except Exception as _ck_e:
                    pass  # Redis lỗi → bỏ qua, không crash job

            waited = 0
            delay = _random.uniform(5, 20)
            while waited < delay and url not in _read_all_stop_urls:
                time.sleep(0.5)
                waited += 0.5

        if url in _read_all_stop_urls:
            _upd(status="stopped", current="⏹ Đã dừng.")
            broadcast({"type": "read_all_status", "status": "stopped", "done": done, "total": total, "name": feed_name})
        else:
            _upd(status="done", current=f"✅ Hoàn thành! Đã xử lý {done} tin.")
            broadcast({"type": "read_all_status", "status": "done", "done": done, "total": total, "name": feed_name})

    except Exception as e:
        _upd(status="error", current=f"❌ Lỗi: {e}")
        broadcast({"type": "read_all_status", "status": "error", "done": 0, "total": 0, "name": feed_name, "error": str(e)})
        print(f"[ReadAllBG] Lỗi: {e}")


def _cleanup_known_guids():
    """
    Dọn dẹp known_guids khi vượt ngưỡng 2x history_limit.
    GUIDs dạng 'tg_@channel_12345' hoặc RSS guid — sort theo phần số cuối để giữ mới nhất.
    """
    with lock:
        urls_snapshot = list(watched_urls)
        guids_snapshot = {k: set(v) for k, v in known_guids.items()}

    for u in urls_snapshot:
        url = u.get('url')
        limit = int(u.get('history_limit', 20))
        max_keep = max(limit, 10)
        guids = guids_snapshot.get(url)
        if not guids or len(guids) <= max_keep:
            continue

        # Sort theo phần số cuối GUID để giữ lại bài MỚI nhất
        # VD: "tg_@channel_12345" → key=12345, RSS guid thường có timestamp
        def _guid_key(g):
            parts = re.split(r'[_\-]', g)
            for p in reversed(parts):
                if p.isdigit():
                    return int(p)
            return 0

        sorted_guids = sorted(guids, key=_guid_key)
        trimmed = set(sorted_guids[-max_keep:])  # giữ max_keep bài mới nhất
        with lock:
            known_guids[url] = trimmed
        print(f'[CLEANUP] {url}: {len(guids)} → {len(trimmed)} GUIDs')

def _memory_cleanup():
    """
    Dọn dẹp RAM định kỳ: translate_cache, Telethon session, gc.
    Gọi mỗi 20 phút từ poller.
    """
    import gc

    # Trim translate_cache xuống còn tối đa 50 entries
    with translate_lock:
        if len(translate_cache) > 50:
            keys = list(translate_cache.keys())
            for k in keys[:-50]:
                del translate_cache[k]
            print(f'[MEM] translate_cache trimmed → {len(translate_cache)} entries')

    # Flush Telethon session cache — giải phóng entity cache trong RAM
    try:
        if tg_client and tg_loop and tg_loop.is_running():
            asyncio.run_coroutine_threadsafe(
                tg_client.session.save() if hasattr(tg_client.session, 'save') else asyncio.sleep(0),
                tg_loop
            )
    except Exception:
        pass

    # Force GC
    collected = gc.collect()
    print(f'[MEM] GC collected {collected} objects')

    # Log RAM usage nếu có psutil
    try:
        import psutil, os as _os
        proc = psutil.Process(_os.getpid())
        mb = proc.memory_info().rss / 1024 / 1024
        print(f'[MEM] RAM hiện tại: {mb:.1f} MB')
    except ImportError:
        pass


def _poll_one(url_obj):
    """
    Poll một feed, trả về True nếu thành công.
    """
    url           = url_obj['url']
    category      = url_obj.get('category', '')
    show_link     = url_obj.get('show_link', True)
    history_limit = int(url_obj.get('history_limit', 20))
    try:
        if is_tg_source(url):
            if not TELETHON_AVAILABLE or tg_client is None:
                return False
            channel = normalize_tg_channel(url)
            items = tg_fetch_channel(channel, limit=history_limit)
            for it in items:
                if not it.get('category'):
                    it['category'] = category
        else:
            xml   = fetch_feed(url)
            items = parse_items(xml, category, limit=history_limit)

        if not items:
            return True

        with lock:
            prev = known_guids.get(url)

        # ==================== DEDUP ====================
        if prev is None or len(prev) == 0:
            # Lần đầu hoặc bị reset → init known_guids
            with lock:
                known_guids[url] = {it['guid'] for it in items if it['guid']}
            _forward_ready_feeds.add(url)
            print(f'[INIT] known_guids khởi tạo lần đầu cho: {url} ({len(items)} items)')
            # Broadcast lên UI để hiển thị ngay — không dịch, không forward
            ws_items = []
            for it in items:
                ws_it = {k: v for k, v in it.items() if k not in ('_tg_media_bytes', 'text', 'text_translated')}
                ws_it['feed_url'] = url
                ws_it['show_link'] = show_link
                ws_items.append(ws_it)
            if ws_items:
                broadcast({'type': 'new_items', 'url': url, 'items': ws_items})
            return True   # Không forward tin cũ

        # Bình thường: tìm tin mới
        new_items = [it for it in items if it['guid'] and it['guid'] not in prev]

        # Lớp 2: Redis dedup (fuzzy title + persist qua restart)
        if new_items and _deduplicator:
            before = len(new_items)
            new_items = [it for it in new_items if not _deduplicator.is_duplicate(it)]
            skipped = before - len(new_items)
            if skipped:
                print(f'[Dedup] Lọc {skipped} tin trùng (Redis): {url}')

        if new_items:
            # Cập nhật known_guids — thêm GUID mới, giữ tối đa history_limit*2
            with lock:
                current = known_guids.get(url, set())
                for it in items:
                    if it['guid']:
                        current.add(it['guid'])
                # Giới hạn kích thước để tránh phình RAM
                if len(current) > history_limit * 2:
                    guids_list = sorted(current)
                    current = set(guids_list[-(history_limit * 2):])
                known_guids[url] = current

            print(f'[+] {len(new_items)} bài mới → pipeline: {url}')
            
            for it in new_items:
                it['show_link']    = show_link
                it['feed_url']     = url
                it['do_translate'] = url_obj.get('do_translate', True)
                # Khôi phục \n từ <br> trước khi strip tag — strip_html thay <br> bằng space
                _rss_desc = it.get('desc', '') or ''
                _rss_desc_plain = re.sub(r'<br\s*/?>', '\n', _rss_desc, flags=re.I)
                _rss_desc_plain = re.sub(r'<[^>]+>', ' ', _rss_desc_plain).strip()
                raw_text = it.get('title', '') + '\n' + _rss_desc_plain
                it['text']    = raw_text.strip()
                it['_source'] = 'rss'

                imgs, videos = extract_media(it.get('desc', ''))
                if videos:
                    v = videos[0].lower()
                    it['_rss_media_url'] = videos[0] if any(v.endswith(e) for e in ('.mp4','.webm','.mov','.mkv')) else None
                elif imgs:
                    it['_rss_media_url'] = imgs[0]
                else:
                    it['_rss_media_url'] = None

                _pipeline.put(it)

        # Feed poll thành công → báo recover nếu trước đó đã có lỗi
        _notify_recover(url, url_obj.get('name', url))
        return True

    except Exception as ex:
        print(f'[!] Poll lỗi {url}: {ex}')
        import traceback
        traceback.print_exc()
        feed_name = url_obj.get('name', url)
        _notify_error(f'⚠️ Feed lỗi: {feed_name}\n{ex}', feed_url=url)
        return False

def _is_ica_source(url):
    return 'tg.i-c-a.su' in url

def _init_tg_feed(url_obj):
    """
    Load lịch sử 1 lần khi thêm kênh TG mới — chỉ để hiển thị UI.
    KHÔNG dịch ở đây (tránh chiếm thread pool, block forward).
    KHÔNG forward (đây là tin cũ).
    """
    url           = url_obj['url']
    category      = url_obj.get('category', '')
    history_limit = int(url_obj.get('history_limit', 20))
    channel       = normalize_tg_channel(url)

    if history_limit == 0:
        with lock:
            known_guids[url] = set()
        print(f'[TG] Bỏ qua lịch sử {channel} (history_limit=0)')
        return

    try:
        items = tg_load_history_sync(channel, limit=history_limit)
        for it in items:
            if not it.get('category'):
                it['category'] = category

        # Lưu known_guids để dedup tin mới sau này
        with lock:
            known_guids[url] = {it['guid'] for it in items if it['guid']}

        # Broadcast thẳng lên UI không qua dịch — nhanh, không tốn pool
        ws_items = [{k: v for k, v in it.items() if k != '_tg_media_bytes'} for it in items]
        if ws_items:
            broadcast({'type': 'new_items', 'url': url, 'items': ws_items})
        _forward_ready_feeds.add(url)
        print(f'[TG] Load lịch sử {channel}: {len(items)} tin (history_limit={history_limit})')
    except Exception as e:
        print(f'[!] Load lịch sử lỗi {channel}: {e}')

def _process_tg_queue():
    """
    Xử lý tin mới từ Telethon real-time queue.
    Kiến trúc mới: dedup → đẩy vào BatchPipeline (async, non-blocking).
    Pipeline sẽ gom batch, dịch, broadcast + forward.
    """
    with tg_new_items_lock:
        if not tg_new_items_queue:
            return
        items_to_process = list(tg_new_items_queue)
        tg_new_items_queue.clear()

    # Nhóm theo feed_url
    by_feed = {}
    for item in items_to_process:
        fu = item.get('_feed_url', '')
        by_feed.setdefault(fu, []).append(item)

    for feed_url, items in by_feed.items():
        category     = ''
        show_link    = True
        do_translate = True
        with lock:
            for u in watched_urls:
                if u['url'] == feed_url:
                    category     = u.get('category', '')
                    show_link    = u.get('show_link', True)
                    do_translate = u.get('do_translate', True)
                    break
            prev = known_guids.get(feed_url)

        # Dedup — lọc trùng trước khi vào pipeline
        new_items = [it for it in items if not prev or it['guid'] not in prev]
        if not new_items:
            continue

        # Lớp 2: Redis dedup
        if _deduplicator:
            before = len(new_items)
            new_items = [it for it in new_items if not _deduplicator.is_duplicate(it)]
            skipped = before - len(new_items)
            if skipped:
                print(f'[Dedup] Lọc {skipped} tin trùng TG (Redis): {feed_url}')

        # Cập nhật known_guids ngay để poller không xử lý lại
        with lock:
            if known_guids.get(feed_url) is None:
                known_guids[feed_url] = set()
            for it in new_items:
                known_guids[feed_url].add(it['guid'])

        print(f'[+] {len(new_items)} bài mới (real-time → pipeline): {feed_url}')

        # Đẩy vào BatchPipeline thay vì xử lý sync
        for it in new_items:
            # Khôi phục newline từ <br> trước khi dịch — strip_html thay <br> bằng space
            _raw_desc = it.get('desc', '') or ''
            if '<br' in _raw_desc.lower():
                _raw_text = re.sub(r'<br\s*/?>', '\n', _raw_desc, flags=re.I)
                _raw_text = re.sub(r'<[^>]+>', '', _raw_text)
            else:
                _raw_text = _raw_desc
            _raw_text = _raw_text or it.get('title', '')
            pipeline_item = {
                'text': _raw_text,
                'feed_url': feed_url,
                'category': category,
                'show_link': show_link,
                'do_translate': do_translate,  # lấy từ lock ở trên, không lookup lại
                'guid': it.get('guid', ''),
                'link': it.get('link', ''),
                'pubDate': it.get('pubDate', ''),
                '_tg_has_media': it.get('_tg_has_media', False),
                '_tg_msg_id': it.get('_tg_msg_id'),
                '_tg_chat': it.get('_tg_chat'),
                '_tg_grouped_id': it.get('_tg_grouped_id'),
                '_source': 'telethon',
                '_tg_has_hidden_link': it.get('_tg_has_hidden_link', False),
                '_tg_has_format': it.get('_tg_has_format', False),
                '_tg_html_text': it.get('_tg_html_text', ''),
            }
            _pipeline.put(pipeline_item)

def poller():
    """
    Poller background chính
    """
    global poll_next_time
    FAST_INTERVAL    = 60
    fast_next: dict  = {}
    tg_inited: set   = set()
    tg_init_pending: set = set()
    _last_cleanup: float = 0.0
    _last_debug_sec: int = -1
    _last_tg_watchdog: float = 0   # kiểm tra TG connection mỗi 30 phút

    print("[POLL] ✅ Background poller STARTED successfully")

    while True:
        try:
            now = time.time()
            with lock:
                urls = list(watched_urls)
        
            if len(urls) == 0:
                if int(now) % 60 == 0:
                    print("[WARN] watched_urls đang rỗng — chưa có feed nào hoặc GitHub chưa load xong")

            # --- Xử lý queue real-time từ Telethon ---
            _process_tg_queue()

            # --- Watchdog: kiểm tra TG connection mỗi 10 phút, tự reconnect nếu mất ---
            if TELETHON_AVAILABLE and tg_client is not None and now - _last_tg_watchdog > 1800:
                _last_tg_watchdog = now
                def _watchdog():
                    global _tg_realtime_last_setup, tg_loop, tg_loop_thread, _watchdog_running
                    if _watchdog_running:
                        return
                    _watchdog_running = True
                    try:
                        # Kiểm tra loop còn sống không
                        if tg_loop is None or not tg_loop.is_running():
                            print('[TG Watchdog] ⚠️ Telethon loop chết — đang restart...')
                            new_loop = asyncio.new_event_loop()
                            t = threading.Thread(target=lambda l=new_loop: (
                                asyncio.set_event_loop(l), l.run_forever()
                            ), daemon=True)
                            t.start()
                            tg_loop = new_loop
                            tg_loop_thread = t
                            time.sleep(1)

                        ok = tg_run(_tg_check_auth())
                        if ok:
                            # Chỉ re-register nếu handler bị mất (không còn handler nào)
                            handlers = tg_client.list_event_handlers()
                            if not handlers:
                                tg_urls_all = [u['url'] for u in watched_urls if is_tg_source(u.get('url', ''))]
                                if tg_urls_all:
                                    # Reset debounce timestamp để force re-register
                                    _tg_realtime_last_setup = 0.0
                                    _tg_realtime_last_urls.clear()
                                    tg_run_long(_tg_setup_realtime(tg_urls_all), timeout=300)
                                    _tg_realtime_last_setup = time.time()
                                    _tg_realtime_last_urls.update(tg_urls_all)
                                    print(f'[TG Watchdog] ✅ Re-register {len(tg_urls_all)} channels (handler bị mất)')
                            else:
                                print(f'[TG Watchdog] ✅ Handler OK ({len(handlers)} handlers) — không cần re-register')
                        else:
                            print('[TG Watchdog] ⚠️ Mất xác thực Telethon')
                    except Exception as e:
                        print(f'[TG Watchdog] Lỗi: {type(e).__name__}: {e}')
                    finally:
                        _watchdog_running = False
                threading.Thread(target=_watchdog, daemon=True).start()

            # --- Init history cho TG feed mới ---
            tg_urls = [u for u in urls if is_tg_source(u['url'])]
            new_tg = [u for u in tg_urls
                      if u['url'] not in tg_inited and u['url'] not in tg_init_pending
                      and TELETHON_AVAILABLE and tg_client is not None]

            for idx, url_obj in enumerate(new_tg):
                tg_inited.add(url_obj['url'])
                tg_init_pending.add(url_obj['url'])
                delay = idx * 2.0
                def _run(u=url_obj, d=delay):
                    time.sleep(d)
                    _init_tg_feed(u)
                    tg_init_pending.discard(u['url'])
                threading.Thread(target=_run, daemon=True).start()

            # --- RSS thường (parallel) ---
            rss_urls = [u for u in urls if not is_tg_source(u['url']) and not _is_ica_source(u['url'])]
            rss_due  = [u for u in rss_urls if now >= fast_next.get(u['url'], 0)]

            if rss_due:
                # Non-blocking: submit rồi để chạy background, không đợi result
                # Đợi f.result() sẽ block poller thread và chặn _process_tg_queue()
                for u in rss_due:
                    _thread_pool.submit(_poll_one, u)
                    fast_next[u['url']] = time.time() + FAST_INTERVAL

            # --- tg.i-c-a.su ---
            if now >= poll_next_time:
                ica_urls = [u for u in urls if _is_ica_source(u['url'])]
                for url_obj in ica_urls:
                    _poll_one(url_obj)
                poll_next_time = time.time() + POLL_INTERVAL
                broadcast({'type': 'poll_status', 'interval': POLL_INTERVAL, 'next_in': POLL_INTERVAL})

            # Debug định kỳ — in đúng 1 lần mỗi phút
            cur_sec = int(now)
            if cur_sec % 60 == 0 and cur_sec != _last_debug_sec:
                _last_debug_sec = cur_sec
                with lock:
                    total_feeds = len(watched_urls)
                try:
                    import psutil as _psutil, os as _os2
                    _mb = _psutil.Process(_os2.getpid()).memory_info().rss / 1024 / 1024
                    _ram_str = f' | RAM: {_mb:.0f}MB'
                except ImportError:
                    _ram_str = ''
                print(f"[POLL] Đang chạy | Feeds: {total_feeds} | Queue: {_pipeline.qsize()} | Known GUIDs: {len(known_guids)} | TG inited: {len(tg_inited)}{_ram_str}")


            # Dọn dẹp known_guids + RAM mỗi 20 phút
            if now - _last_cleanup >= 1200:
                _last_cleanup = now
                _cleanup_known_guids()
                _memory_cleanup()
                # Trim fast_next — xóa URLs không còn trong watched_urls
                with lock:
                    active_urls = {u['url'] for u in watched_urls}
                stale = [k for k in list(fast_next.keys()) if k not in active_urls]
                for k in stale:
                    del fast_next[k]
                if stale:
                    print(f'[MEM] fast_next trimmed {len(stale)} stale entries')

            time.sleep(0.5)

        except Exception as e:
            print(f"[POLL ERROR] Lỗi lớn trong poller(): {e}")
            import traceback
            traceback.print_exc()
            time.sleep(5)   # tránh cpu 100% nếu lỗi liên tục

def ws_handler(ws):
    global translate_enabled, auto_fwd_enabled, tg_channels, translate_engine, translate_target_lang
    
    with lock:
        ws_clients.add(ws)
    
    print(f'[WS] Client kết nối, tổng={len(ws_clients)}')

    try:
        # Gửi connected ngay
        ws.send(json.dumps({'type': 'connected', 'ts': time.time()}, ensure_ascii=False))

        # Gửi heartbeat định kỳ trong thread riêng để giữ Render proxy không đóng connection
        def keepalive():
            while not getattr(ws, '_closed', False):
                time.sleep(5)
                if getattr(ws, '_closed', False):
                    break
                try:
                    ws.send(json.dumps({'type': 'heartbeat', 'ts': time.time()}, ensure_ascii=False))
                except:
                    break
        threading.Thread(target=keepalive, daemon=True).start()

        # Main message loop
        while True:
            try:
                raw = ws.recv()
            except Exception:
                break
            if raw is None:
                break
            try:
                msg = json.loads(raw)
            except:
                continue

            t = msg.get('type')
            if t == 'heartbeat':
                continue

            if t == 'feeds':
                print(f'[WS] msg: type={t} size={len(raw)}b feeds={len(msg.get("feeds",[]))}')
            else:
                print(f'[WS] msg: type={t} size={len(raw)}b')

            if t == 'feeds':
                urls = msg.get('feeds', [])
                print(f'[WS] ✅ Nhận được {len(urls)} feeds từ client')

                with lock:
                    watched_urls.clear()
                    watched_urls.extend(urls)
                    init_count = 0
                    for u in urls:
                        url = u.get('url')
                        if url and (url not in known_guids or not known_guids[url]):
                            known_guids[url] = set()
                            init_count += 1
                    print(f'[INIT] Đã init {init_count} feeds mới')

                # Gửi ack NGAY
                ws.send(json.dumps({'type': 'feeds_ack', 'count': len(urls)}, ensure_ascii=False))
                # Lưu GitHub ở background (nếu WS vẫn gửi feeds)
                threading.Thread(target=save_feeds_to_file, args=(urls,), daemon=True).start()

            elif t == 'translate_engine':
                translate_engine = msg.get('engine', 'google')
                st = _gemini_pool.status()
                eng_st = st.get(translate_engine, {})
                if translate_engine == 'gemini' and not GEMINI_API_KEY:
                    print(f'[WS] Engine: gemini — ⚠️ GEMINI_API_KEY chưa set, dispatcher sẽ fallback')
                elif translate_engine == 'deepl' and not DEEPL_API_KEY:
                    print(f'[WS] Engine: deepl — ⚠️ DEEPL_API_KEY chưa set, dispatcher sẽ fallback')
                else:
                    print(f'[WS] Engine ưu tiên → {translate_engine} | '
                          f'dispatcher status: {st}')

            elif t == 'translate_lang':
                lang = msg.get('lang', 'vi').strip().lower()
                if lang:
                    translate_target_lang = lang
                    print(f'[WS] Ngôn ngữ đích → {translate_target_lang}')

            elif t == 'auto_fwd':
                auto_fwd_enabled = msg.get('enabled', False)
                with lock:
                    tg_channels = msg.get('channels', tg_channels)
                print(f'[WS] auto_fwd={auto_fwd_enabled}, channels={len(tg_channels)}')

            elif t == 'tg_settings':
                pass

    except BrokenPipeError:
        pass  # client ngắt kết nối bình thường
    except Exception as e:
        print(f'[WS] Lỗi xử lý WebSocket: {e}')
    finally:
        ws._closed = True
        try:
            ws.close()
        except:
            pass
        with lock:
            ws_clients.discard(ws)
        print(f'[WS] Client ngắt, tổng còn lại: {len(ws_clients)}')

def is_tg_source(url):
    if not url:
        return False
    url = url.strip()
    # @username, t.me/ link, hoặc numeric ID (-100xxxxxxxxxx hoặc số thuần)
    if url.startswith('@') or 't.me/' in url:
        return True
    # Numeric ID: -100xxxxxxxxxx hoặc chỉ số âm/dương
    stripped = url.lstrip('-')
    return stripped.isdigit() and len(stripped) >= 5

def normalize_tg_channel(url):
    url = url.strip()
    # Numeric ID — giữ nguyên, không thêm @
    stripped = url.lstrip('-')
    if stripped.isdigit() and len(stripped) >= 5:
        return url  # trả về dạng -1001419735783 hoặc 1001419735783
    if url.startswith('https://t.me/'):
        url = url[len('https://t.me/'):]
    elif url.startswith('t.me/'):
        url = url[len('t.me/'):]
    if not url.startswith('@'):
        url = '@' + url
    return url

class HttpHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        p = urlparse(self.path)

        # --- Các path public không cần auth ---
        _public_paths = {'/healthz', '/login', '/auth_status', '/logout'}
        if p.path not in _public_paths and p.path != '/ws':
            if not _is_authed(self):
                # Nếu là browser request → redirect về trang login
                if 'text/html' in self.headers.get('Accept', ''):
                    data = _LOGIN_HTML.encode('utf-8')
                    self.send_response(200)
                    self.send_header('Content-Type', 'text/html; charset=utf-8')
                    self.send_header('Content-Length', str(len(data)))
                    self.end_headers()
                    self.wfile.write(data)
                else:
                    self._json({'error': 'Unauthorized'}, 401)
                return

        # --- WebSocket upgrade ---
        if p.path == '/ws' and self.headers.get('Upgrade', '').lower() == 'websocket':
            if not _is_authed(self):
                self.send_error(401); return
            print(f'[WS] Upgrade request nhận được, key={self.headers.get("Sec-WebSocket-Key","?")}')
            ws = _ws_handshake(self)
            if ws:
                threading.Thread(target=ws_handler, args=(ws,), daemon=True).start()
            return

        if p.path == '/healthz':
            self.send_response(200)
            self.send_header('Content-Type', 'text/plain')
            self.end_headers()
            self.wfile.write(b'OK')
            return

        if p.path == '/auth_status':
            self._json({'auth_enabled': _auth_enabled(), 'logged_in': _is_authed(self)})
            return

        if p.path == '/fwd_counts':
            with _fwd_count_lock:
                self._json(dict(_fwd_counts))
            return

        if p.path == '/fetch':
            qs = parse_qs(p.query)
            url = qs.get('url', [None])[0]
            do_tl = qs.get('translate', ['1'])[0] == '1'
            category = qs.get('category', [''])[0]
            if not url:
                self.send_error(400); return
            try:
                xml = fetch_feed(url)
                _hlimit = int(qs.get('history_limit', ['20'])[0])
                items = parse_items(xml, category, limit=_hlimit)
                if do_tl and translate_enabled and TRANSLATE_AVAILABLE:
                    items = process_items(items)
                resp = json.dumps({'items': items}, ensure_ascii=False).encode('utf-8')
                self.send_response(200)
                self.send_header('Content-Type', 'application/json; charset=utf-8')
                self.send_header('Access-Control-Allow-Origin', '*')
                self.end_headers()
                self.wfile.write(resp)
            except Exception as e:
                self.send_response(500)
                self.send_header('Content-Type', 'text/plain')
                self.end_headers()
                self.wfile.write(str(e).encode())

        elif p.path == '/tl_fetch':
            qs = parse_qs(p.query)
            url = qs.get('url', [None])[0]
            do_tl = qs.get('translate', ['1'])[0] == '1'
            category = qs.get('category', [''])[0]
            history_limit = int(qs.get('history_limit', ['20'])[0])
            if not url:
                self.send_error(400); return
            if not TELETHON_AVAILABLE or tg_client is None:
                self._json({'error': 'Telethon chưa kết nối'}, 503); return
            # Giới hạn concurrent tl_fetch — tránh flood Telethon loop
            if not _tl_fetch_semaphore.acquire(blocking=True, timeout=5):
                self.send_response(503)
                self.send_header('Content-Type', 'text/plain')
                self.end_headers()
                self.wfile.write(b'Server busy, retry later')
                return
            try:
                channel = normalize_tg_channel(url)
                items = tg_fetch_channel(channel, limit=history_limit)
                for it in items:
                    if not it.get('category'):
                        it['category'] = category
                if do_tl and translate_enabled and TRANSLATE_AVAILABLE:
                    items = process_tg_items(items)
                safe_items = [{k:v for k,v in it.items() if k != '_tg_media_bytes'} for it in items]
                resp = json.dumps({'items': safe_items}, ensure_ascii=False).encode('utf-8')
                self.send_response(200)
                self.send_header('Content-Type', 'application/json; charset=utf-8')
                self.send_header('Access-Control-Allow-Origin', '*')
                self.end_headers()
                self.wfile.write(resp)
            except Exception as e:
                self.send_response(500)
                self.send_header('Content-Type', 'text/plain')
                self.end_headers()
                self.wfile.write(str(e).encode())
            finally:
                _tl_fetch_semaphore.release()

        elif p.path == '/tl_status':
            connected = False
            if TELETHON_AVAILABLE and tg_client is not None and tg_loop is not None:
                try:
                    connected = tg_run(_tg_check_auth())
                except:
                    pass
            self._json({'connected': connected, 'state': tg_auth_state, 'msg': tg_auth_msg})

        elif p.path == '/proxy':
            qs = parse_qs(p.query)
            url = qs.get('url', [None])[0]
            if not url or not url.startswith('http'):
                self.send_error(400); return
            try:
                req = urllib.request.Request(url, headers={
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                    'Referer': 'https://t.me/',
                    'Accept': 'image/avif,image/webp,image/apng,image/svg+xml,image/*,*/*;q=0.8'
                })
                with urllib.request.urlopen(req, timeout=12) as response:
                    content = response.read()
                    ct = response.headers.get('Content-Type', 'application/octet-stream')
                    self.send_response(200)
                    self.send_header('Content-Type', ct)
                    self.send_header('Content-Length', str(len(content)))
                    self.send_header('Cache-Control', 'public, max-age=86400')
                    self.send_header('Access-Control-Allow-Origin', '*')
                    self.end_headers()
                    self.wfile.write(content)
            except:
                self.send_error(404)
        else:
            data = HTML.encode('utf-8')
            self.send_response(200)
            self.send_header('Content-Type', 'text/html; charset=utf-8')
            self.send_header('Content-Length', str(len(data)))
            self.end_headers()
            self.wfile.write(data)

    def do_POST(self):
        p = urlparse(self.path)

        # --- Login / Logout (không cần auth) ---
        if p.path == '/login':
            body = self._read_json()
            username = body.get('username', '').strip()
            password = body.get('password', '').strip()
            if not _auth_enabled():
                self._json({'ok': True, 'msg': 'Auth không bật'}); return
            if username == APP_USERNAME and password == APP_PASSWORD:
                token = _make_session_token()
                with _session_lock:
                    _active_sessions.add(token)
                resp_data = json.dumps({'ok': True}).encode('utf-8')
                self.send_response(200)
                self.send_header('Content-Type', 'application/json; charset=utf-8')
                self.send_header('Set-Cookie', f'session={token}; Path=/; HttpOnly; SameSite=Strict; Max-Age=2592000')
                self.send_header('Content-Length', str(len(resp_data)))
                self.end_headers()
                self.wfile.write(resp_data)
                print(f'[Auth] Login thành công: {username}')
            else:
                self._json({'ok': False, 'error': 'Sai tên đăng nhập hoặc mật khẩu'}, 401)
                print(f'[Auth] Login thất bại: {username}')
            return

        if p.path == '/logout':
            cookie = self.headers.get('Cookie', '')
            for part in cookie.split(';'):
                part = part.strip()
                if part.startswith('session='):
                    token = part[8:]
                    with _session_lock:
                        _active_sessions.discard(token)
            resp_data = json.dumps({'ok': True}).encode('utf-8')
            self.send_response(200)
            self.send_header('Content-Type', 'application/json; charset=utf-8')
            self.send_header('Set-Cookie', 'session=; Path=/; HttpOnly; Max-Age=0')
            self.send_header('Content-Length', str(len(resp_data)))
            self.end_headers()
            self.wfile.write(resp_data)
            return

        # --- Auth check cho tất cả endpoint còn lại ---
        if not _is_authed(self):
            self._json({'error': 'Unauthorized'}, 401); return

        if p.path == '/test_notify':
            _notify_error('🔔 Test thông báo lỗi từ RSS Reader — mọi thứ hoạt động bình thường!',
                          is_test=True)
            self._json({'ok': True, 'msg': 'Đã gửi test notification'})
            return

        if p.path == '/sync_feeds':
            body = self._read_json()
            urls = body.get('feeds', [])
            if not urls:
                self._json({'ok': False, 'error': 'missing feeds'}); return

            # Set global state
            if 'auto_fwd' in body:
                global auto_fwd_enabled
                auto_fwd_enabled = bool(body['auto_fwd'])
            if 'tg_channels' in body:
                global tg_channels
                with lock:
                    tg_channels = body['tg_channels']
            if 'translate_engine' in body:
                global translate_engine
                translate_engine = body['translate_engine']
                print(f'[CONFIG] translate_engine = {translate_engine}')
            if 'translate_lang' in body:
                global translate_target_lang
                translate_target_lang = body['translate_lang'].strip().lower()
                print(f'[CONFIG] translate_target_lang = {translate_target_lang}')

            with lock:
                watched_urls.clear()
                watched_urls.extend(urls)
                init_count = 0
                for u in urls:
                    url_key = u.get('url')
                    if url_key and (url_key not in known_guids or not known_guids[url_key]):
                        known_guids[url_key] = set()
                        init_count += 1
            print(f'[HTTP] ✅ sync_feeds: {len(urls)} feeds, {init_count} mới | auto_fwd={auto_fwd_enabled} | channels={len(tg_channels)}')
            tg_urls = [u['url'] for u in urls if is_tg_source(u.get('url', ''))]
            if tg_urls and TELETHON_AVAILABLE and tg_client:
                threading.Thread(target=tg_setup_realtime_sync, args=(tg_urls,), daemon=True).start()
            threading.Thread(target=save_feeds_to_file, args=(urls,), daemon=True).start()
            self._json({'ok': True, 'count': len(urls)})
            return

        elif p.path == '/tl_send_code':
            body = self._read_json()
            if not body:
                self.send_error(400); return
            if not TELETHON_AVAILABLE:
                self._json({'ok': False, 'state': 'error', 'msg': 'Telethon chưa cài'}); return
            try:
                result = tg_run(_tg_send_code(body['api_id'], body['api_hash'], body['phone']))
                self._json(result)
            except Exception as e:
                self._json({'ok': False, 'state': 'error', 'msg': str(e)})

        elif p.path == '/tl_sign_in':
            body = self._read_json()
            if not body:
                self.send_error(400); return
            try:
                result = tg_run(_tg_sign_in(body['code']))
                self._json(result)
            except Exception as e:
                self._json({'ok': False, 'state': 'error', 'msg': str(e)})

        elif p.path == '/tl_sign_in_2fa':
            body = self._read_json()
            if not body:
                self.send_error(400); return
            try:
                result = tg_run(_tg_sign_in_2fa(body['password']))
                self._json(result)
            except Exception as e:
                self._json({'ok': False, 'state': 'error', 'msg': str(e)})

        elif p.path == '/tg_forward':
            # Forward thủ công từ UI — dùng Telethon, không Bot API, không download
            body = self._read_json()
            # Format mới: destinations = [{username, name, is_group, topic_ids}, ...]
            # Format cũ:  channels = [{username, name, type, topic_id}, ...]
            destinations = body.get('destinations') or body.get('channels', [])
            items_raw    = body.get('items', [])
            if not destinations:
                self._json({'error': 'missing destinations'}, 400); return
            if not TELETHON_AVAILABLE or tg_client is None:
                self._json({'error': 'Telethon chưa kết nối'}, 503); return

            all_results = []
            for dest_cfg in destinations:
                dest = dest_cfg.get('username') or dest_cfg.get('channel_id', '')
                if not dest:
                    all_results.append({'title': dest_cfg.get('name','?'), 'ok': False, 'error': 'Thiếu username kênh đích'})
                    continue
                channel_name = dest_cfg.get('name', dest)

                # Hỗ trợ cả topic_ids string mới lẫn topic_id int cũ
                topic_ids_str = dest_cfg.get('topic_ids', '') or ''
                raw_topics = [t.strip() for t in topic_ids_str.split(',') if t.strip()] if topic_ids_str else []
                topic_list = [int(t) for t in raw_topics if t.isdigit()]
                if not topic_list:
                    # fallback topic_id cũ (int)
                    old_tid = dest_cfg.get('topic_id')
                    topic_list = [int(old_tid)] if old_tid else [None]

                for topic_id in topic_list:
                    for it in items_raw:
                        feed_url  = it.get('feedUrl', '')
                        show_link = it.get('show_link', True)

                        desc_raw   = it.get('desc', '') or ''
                        desc_plain = re.sub(r'<br\s*/?>', '\n', desc_raw, flags=re.I)
                        desc_plain = re.sub(r'<[^>]+>', '', desc_plain).strip()
                        # Nếu có link ẩn → dùng text đã expand
                        _expanded = _expand_hidden_links_to_text(it)
                        caption   = _expanded if _expanded else desc_plain
                        if show_link and it.get('link'):
                            caption += f'\n\n<a href="{it["link"]}">Xem bài gốc →</a>'
                        if channel_name:
                            caption += f'\n\n<i>{channel_name}</i>'

                        send_item = {**it, '_source': 'telethon' if is_tg_source(feed_url) else 'rss'}
                        if is_tg_source(feed_url):
                            try:
                                link   = it.get('link','')
                                msg_id = int(link.rstrip('/').split('/')[-1])
                                chat   = normalize_tg_channel(feed_url).lstrip('@')
                                send_item['_tg_msg_id']    = msg_id
                                send_item['_tg_chat']      = chat
                                send_item['_tg_has_media'] = True
                            except Exception:
                                send_item['_tg_has_media'] = False
                        else:
                            imgs, _ = extract_media(it.get('desc',''))
                            send_item['_rss_media_url'] = imgs[0] if imgs else None
                        # desc_has_link: check link thật trong desc, bỏ t.me
                        desc_has_link = bool(re.search(r'https?://', it.get('desc', '') or ''))
                        try:
                            ok = tg_run(_tg_send_item(dest, send_item, caption, topic_id=topic_id, desc_has_link=desc_has_link))
                            all_results.append({'title': it.get('title',''), 'ok': ok, 'error': '' if ok else 'Gửi thất bại'})
                        except RuntimeError as e:
                            all_results.append({'title': it.get('title',''), 'ok': False, 'error': str(e)})
                        except Exception as e:
                            all_results.append({'title': it.get('title',''), 'ok': False, 'error': str(e)})
                        time.sleep(0.3)

            resp = json.dumps({'results': all_results}, ensure_ascii=False).encode('utf-8')
            self.send_response(200)
            self.send_header('Content-Type', 'application/json; charset=utf-8')
            self.end_headers()
            self.wfile.write(resp)

        elif p.path == '/tg_cleanup_topic':
            if not TELETHON_AVAILABLE or tg_client is None:
                self._json({'error': 'Telethon chưa kết nối'}, 503); return
            body = self._read_json()
            channel_str = body.get('channel', '').strip()
            topic_id    = body.get('topic_id')
            raw_count   = body.get('count', 100)
            # Handle "All" / "all" → xóa toàn bộ (999999)
            if str(raw_count).strip().lower() in ('all', ''):
                count = 999999
            else:
                count = max(1, int(raw_count))
            if not channel_str or not topic_id:
                self._json({'error': 'Thiếu channel hoặc topic_id'}, 400); return

            # Chạy trên background thread — không block HTTP, tránh timeout
            _cleanup_status[channel_str] = {'status': 'running', 'deleted': 0, 'total': 0, 'error': ''}

            def _run_cleanup_bg(ch_str, tid, cnt):
                async def _do():
                    try:
                        from telethon.tl.functions.channels import DeleteMessagesRequest as ChDeleteMsg
                        from telethon.tl.functions.messages import DeleteMessagesRequest as MsgDeleteMsg
                        dest         = await _resolve_dest(ch_str)
                        entity       = await tg_client.get_entity(dest)
                        input_entity = await tg_client.get_input_entity(dest)
                        is_channel   = hasattr(entity, 'broadcast') or hasattr(entity, 'megagroup')
                        tid_int      = int(tid)

                        # Kiểm tra quyền admin trước khi xóa
                        try:
                            me = await tg_client.get_me()
                            from telethon.tl.functions.channels import GetParticipantRequest
                            part = await tg_client(GetParticipantRequest(channel=input_entity, participant=me.id))
                            from telethon.tl.types import ChannelParticipantCreator, ChannelParticipantAdmin
                            p = part.participant
                            is_admin = isinstance(p, (ChannelParticipantCreator, ChannelParticipantAdmin))
                            can_delete = is_admin and (
                                isinstance(p, ChannelParticipantCreator) or
                                getattr(getattr(p, 'admin_rights', None), 'delete_messages', False)
                            )
                            print(f'[Cleanup] Account is_admin={is_admin}, can_delete_messages={can_delete}')
                            if not can_delete:
                                raise PermissionError('Account không có quyền delete_messages trong channel này')
                        except PermissionError:
                            raise
                        except Exception as pe:
                            print(f'[Cleanup] Không kiểm tra được quyền: {pe} — tiếp tục thử xóa')

                        # Thu thap tin trong topic
                        all_ids = await _collect_topic_ids(entity, input_entity, tid_int)
                        print(f'[Cleanup] Thu thap {len(all_ids)} msgs trong topic={tid}')

                        # GUARD: đảm bảo không xóa quá cnt tin
                        msg_ids = all_ids[:max(0, int(cnt))]
                        print(f'[Cleanup] Sẽ xóa {len(msg_ids)} msgs (cnt={cnt}), is_channel={is_channel}')
                        print(f'[Cleanup] IDs mẫu (10 đầu): {msg_ids[:10]}')
                        assert len(msg_ids) <= int(cnt), f'Safety check fail: {len(msg_ids)} > {cnt}'
                        _cleanup_status[ch_str]['total'] = len(msg_ids)

                        # Xoa bang Telethon high-level API - tu xu ly peer dung
                        deleted = 0
                        for i in range(0, len(msg_ids), 100):
                            batch = msg_ids[i:i+100]
                            try:
                                result = await tg_client.delete_messages(entity, batch, revoke=True)
                                n = sum(getattr(r, 'pts_count', 0) or 0 for r in (result if isinstance(result, list) else [result]))
                                deleted += n if n > 0 else len(batch)
                                print(f'[Cleanup] delete_messages batch={len(batch)} pts_count={n}')
                            except Exception as del_err:
                                err_str = str(del_err)
                                if 'FLOOD_WAIT' in err_str:
                                    import re as _re
                                    m = _re.search(r'FLOOD_WAIT_(\d+)', err_str)
                                    wait_sec = int(m.group(1)) if m else 30
                                    print(f'[Cleanup] FloodWait {wait_sec}s - doi...')                                    
                                    await asyncio.sleep(wait_sec + 2)
                                    result = await tg_client.delete_messages(entity, batch, revoke=True)
                                    deleted += len(batch)
                                else:
                                    raise
                            _cleanup_status[ch_str]['deleted'] = deleted
                            await asyncio.sleep(0.5)

                        remaining = max(0, len(all_ids) - deleted)
                        _cleanup_status[ch_str].update({'status': 'done', 'deleted': deleted, 'remaining': remaining})
                        print(f'[Cleanup] ✅ {ch_str} topic={tid}: xóa {deleted} tin, còn lại {remaining} tin')

                        # Force refresh Telegram UI: gửi + xóa ngay 1 tin dummy
                        # Chạy mọi lúc khi deleted > 0 — topic forum không tự refresh dù xóa toàn bộ
                        if deleted > 0:
                            try:
                                from telethon.tl.types import InputReplyToMessage
                                sent = await tg_client.send_message(
                                    entity,
                                    '.',
                                    reply_to=InputReplyToMessage(
                                        reply_to_msg_id=tid_int,
                                        top_msg_id=tid_int,
                                    ),
                                    silent=True
                                )
                                await asyncio.sleep(0.5)
                                await tg_client.delete_messages(entity, [sent.id], revoke=True)
                                print(f'[Cleanup] 🔄 Force UI refresh topic={tid} OK')
                            except Exception as rf_err:
                                print(f'[Cleanup] Force refresh warning (không ảnh hưởng xóa): {rf_err}')
                    except Exception as e:
                        _cleanup_status[ch_str].update({'status': 'error', 'error': str(e)})
                        print(f'[Cleanup] ❌ {ch_str} topic={tid}: {e}')

                try:
                    tg_run_long(_do(), timeout=600)
                except Exception as e:
                    _cleanup_status[ch_str].update({'status': 'error', 'error': str(e)})

            threading.Thread(target=_run_cleanup_bg, args=(channel_str, topic_id, count),
                             daemon=True, name='CleanupBG').start()
            self._json({'ok': True, 'msg': 'Đang xóa...', 'channel': channel_str})

        elif p.path == '/tg_cleanup_status':
            body = self._read_json()
            ch   = body.get('channel', '').strip()
            self._json(_cleanup_status.get(ch, {'status': 'unknown'}))

        elif p.path == '/tg_cleanup_schedule_save':
            # Lưu hoặc cập nhật 1 schedule: {channel, topic_id, count, hour, minute, enabled}
            body     = self._read_json()
            channel  = body.get('channel', '').strip()
            topic_id = str(body.get('topic_id', '')).strip()
            count    = int(body.get('count', 100))
            hour     = int(body.get('hour', 3))
            minute   = int(body.get('minute', 0))
            enabled  = bool(body.get('enabled', True))
            topic_name = str(body.get('topic_name', topic_id))
            if not channel or not topic_id:
                self._json({'error': 'Thiếu channel hoặc topic_id'}, 400); return
            key = f'{channel}|{topic_id}'
            _cleanup_schedules[key] = {
                'channel': channel, 'topic_id': topic_id, 'topic_name': topic_name,
                'count': count, 'hour': hour, 'minute': minute,
                'enabled': enabled, 'last_run': _cleanup_schedules.get(key, {}).get('last_run', ''),
            }
            _persist_cleanup_schedules()
            self._json({'ok': True})

        elif p.path == '/tg_cleanup_schedule_delete':
            body     = self._read_json()
            channel  = body.get('channel', '').strip()
            topic_id = str(body.get('topic_id', '')).strip()
            key = f'{channel}|{topic_id}'
            _cleanup_schedules.pop(key, None)
            _persist_cleanup_schedules()
            self._json({'ok': True})

        elif p.path == '/tg_cleanup_schedule_list':
            self._json({'schedules': list(_cleanup_schedules.values())})

        elif p.path == '/tg_read_all_stop':
            body_stop = self._read_json()
            stop_url = body_stop.get('url', '')
            if stop_url:
                _read_all_stop_urls.add(stop_url)
            else:
                # fallback: dừng tất cả job đang chạy
                with _read_all_job_lock:
                    # Dừng tất cả job đang running
                    for _jurl, _jstate in _read_all_jobs.items():
                        if _jstate.get('status') == 'running':
                            _read_all_stop_urls.add(_jurl)
            self._json({'ok': True})

        elif p.path == '/tg_read_all_status':
            body_st = self._read_json()
            req_url = body_st.get('url', '') if body_st else ''
            with _read_all_job_lock:
                if req_url and req_url in _read_all_jobs:
                    job = dict(_read_all_jobs[req_url])
                elif req_url:
                    job = None
                else:
                    # Không có url → trả về job running đầu tiên (backward compat)
                    running = [v for v in _read_all_jobs.values() if v.get('status') == 'running']
                    job = dict(running[0]) if running else (dict(list(_read_all_jobs.values())[-1]) if _read_all_jobs else None)
            self._json({'job': job, 'jobs': {k: dict(v) for k, v in _read_all_jobs.items()}})

        elif p.path == '/tg_read_all_bg':
            body = self._read_json()
            url = body.get('url', '')
            if not url or not is_tg_source(url):
                self._json({'error': 'URL không hợp lệ'}, 400); return
            if not TELETHON_AVAILABLE or tg_client is None:
                self._json({'error': 'Telethon chưa kết nối'}, 503); return
            with lock:
                feed_cfg = next((u for u in watched_urls if u['url'] == url), None)
            if not feed_cfg:
                self._json({'error': 'Feed không tồn tại'}, 404); return
            with _read_all_job_lock:
                existing = _read_all_jobs.get(url)
                if existing and existing.get('status') == 'running' and url not in _read_all_stop_urls:
                    done_n = existing['done']; total_n = existing['total']
                    self._json({'error': 'blocked', 'msg': f'Đang có job chạy ngầm cho kênh này ({done_n}/{total_n} tin)'}); return
                # Job mới (hoặc restart sau khi done/error/stopped):
                # Reset known_guids[url] để job chạy lại từ đầu và đếm đúng
                if existing and existing.get('status') in ('done', 'error', 'stopped'):
                    with lock:
                        known_guids.pop(url, None)
                _read_all_jobs[url] = {'url': url, 'name': feed_cfg.get('name', url), 'done': 0, 'total': 0, 'status': 'running', 'current': 'Đang khởi động...'}
            _read_all_stop_urls.discard(url)  # reset stop flag cho url này
            threading.Thread(target=_run_read_all_bg, args=(url, feed_cfg), daemon=True, name='ReadAllBG').start()
            self._json({'ok': True, 'msg': 'Job chạy ngầm đã bắt đầu'})


        else:
            self.send_error(404)

    def _read_json(self):
        try:
            length = int(self.headers.get('Content-Length', 0))
            return json.loads(self.rfile.read(length))
        except:
            return {}

    def _json(self, obj, status=200):
        data = json.dumps(obj, ensure_ascii=False).encode('utf-8')
        self.send_response(status)
        self.send_header('Content-Type', 'application/json; charset=utf-8')
        self.send_header('Access-Control-Allow-Origin', '*')
        self.end_headers()
        self.wfile.write(data)

    def do_HEAD(self):
        """Render health check dùng HEAD — trả 200 ngay, không gửi body"""
        self.send_response(200)
        self.send_header('Content-Type', 'text/html')
        self.end_headers()

    def handle_error(self, request, client_address):
        """Suppress BrokenPipeError từ Render load balancer"""
        import sys
        exc = sys.exc_info()[1]
        if isinstance(exc, BrokenPipeError):
            return
        super().handle_error(request, client_address)

    def log_message(self, *a):
        pass

class ThreadingHTTPServer(ThreadingMixIn, HTTPServer):
    daemon_threads = True

    def handle_error(self, request, client_address):
        """Suppress BrokenPipeError ở cấp server"""
        import sys
        exc = sys.exc_info()[1]
        if isinstance(exc, BrokenPipeError):
            return
        super().handle_error(request, client_address)

if __name__ == '__main__':
    # === REDIS DEDUPLICATOR ===
    _redis_url = os.environ.get('REDIS_URL', '').strip()
    if REDIS_AVAILABLE and _redis_url:
        try:
            _redis_client = _redis_lib.from_url(_redis_url, decode_responses=False)
            _redis_client.ping()
            _deduplicator = RSSDeduplicator(_redis_client)
            print('[i] Redis Deduplicator: OK ✅')
            _load_cleanup_schedules()
        except Exception as e:
            print(f'[!] Redis kết nối lỗi — dedup tắt: {e}')
    else:
        if not _redis_url:
            print('[i] REDIS_URL chưa cấu hình — dedup Redis tắt')
        elif not REDIS_AVAILABLE:
            print('[i] Thư viện redis chưa cài — dedup Redis tắt')

    # === PERSIST FEEDS - TỰ ĐỘNG LOAD KHI RESTART ===
    default_feeds = load_feeds_from_file()
    if default_feeds:
        # Hỗ trợ format mới {feeds:[], auto_fwd:bool, tg_channels:[]}
        if isinstance(default_feeds, dict):
            _cfg = default_feeds
            default_feeds = _cfg.get('feeds', [])
            auto_fwd_enabled = _cfg.get('auto_fwd', False)
            tg_channels      = _cfg.get('tg_channels', [])
            translate_engine = _cfg.get('translate_engine', translate_engine)
            print(f'[CONFIG] auto_fwd={auto_fwd_enabled} | channels={len(tg_channels)}')

        with lock:
            watched_urls.clear()
            watched_urls.extend(default_feeds)
            for u in default_feeds:
                url = u.get('url')
                if url and url not in known_guids:
                    known_guids[url] = set()
        print(f'[CONFIG] ✅ ĐÃ TỰ ĐỘNG KÍCH HOẠT {len(default_feeds)} FEEDS TỪ FILE')

    _pipeline.start()
    print('[i] BatchPipeline: OK')

    # Khởi động Telegram Bot polling thread
    if ERROR_BOT_TOKEN:
        threading.Thread(target=_bot_poll_loop, daemon=True, name='TGBot-Poll').start()
        print(f'[Bot] ✅ Telegram Bot polling bắt đầu')
    else:
        print('[Bot] ERROR_BOT_TOKEN chưa cấu hình — bot notification tắt')

    # Khởi động asyncio loop cho Telethon trên thread riêng
    if TELETHON_AVAILABLE:
        tg_loop_thread = threading.Thread(target=run_tg_loop, daemon=True)
        tg_loop_thread.start()
        time.sleep(0.2)
        print('[i] Telethon loop: OK')

        session_str = os.environ.get('TG_SESSION_STRING', '').strip()
        env_api_id   = os.environ.get('TG_API_ID', '').strip()
        env_api_hash = os.environ.get('TG_API_HASH', '').strip()
        env_phone    = os.environ.get('TG_PHONE', '').strip()

        if env_api_id and env_api_hash and env_phone:
            cfg = {'api_id': env_api_id, 'api_hash': env_api_hash, 'phone': env_phone}
            print('[i] Dùng Telethon config từ env vars')
        else:
            cfg = load_tg_config()

        if cfg:
            try:
                result = tg_run(_tg_send_code(cfg['api_id'], cfg['api_hash'], cfg['phone']))
                if result.get('state') == 'connected':
                    src = 'session string' if session_str else 'session file'
                    print(f'[i] Telethon: đăng nhập thành công ({src})')
                    # Setup realtime cho TG feeds đã load từ file
                    tg_urls = [u['url'] for u in watched_urls if is_tg_source(u.get('url', ''))]
                    if tg_urls:
                        _thread_pool.submit(tg_setup_realtime_sync, tg_urls)
                else:
                    print(f'[!] Telethon auto-login: {result.get("msg")}')
            except Exception as e:
                print(f'[!] Telethon auto-login lỗi: {e}')
        else:
            print('[i] Chưa có config — cần đăng nhập qua giao diện web')

    # Khởi động poller (1 lần duy nhất)
    threading.Thread(target=poller, daemon=True, name="RSS-Poller").start()
    print("[MAIN] Poller thread đã được start")

    print(f'=== RSS + Telegram Reader === http://localhost:{HTTP_PORT}')
    print(f'Dịch: {"bật" if TRANSLATE_AVAILABLE else "chưa cài thư viện"}')
    print(f'Telethon: {"OK" if TELETHON_AVAILABLE else "chưa cài — pip install telethon"}')
    print(f'Poll mỗi {POLL_INTERVAL}s | Ctrl+C để dừng')
    # Chỉ mở browser khi chạy local (không phải trên cloud)
    if not os.environ.get('PORT'):
        import webbrowser
        webbrowser.open(f'http://localhost:{HTTP_PORT}')
    print(f'[i] Khởi động HTTP server trên port {HTTP_PORT}...')
    try:
        server = ThreadingHTTPServer(('', HTTP_PORT), HttpHandler)
        print(f'[i] HTTP server đang lắng nghe port {HTTP_PORT}')
        server.serve_forever()
    except KeyboardInterrupt:
        print('Dừng.')
    except OSError as e:
        # Address already in use — đợi rồi thử lại
        print(f'[!] Port {HTTP_PORT} bị chiếm: {e} — thử lại sau 3s')
        time.sleep(3)
        server = ThreadingHTTPServer(('', HTTP_PORT), HttpHandler)
        print(f'[i] HTTP server khởi động lại thành công')
        server.serve_forever()
    except Exception as e:
        print(f'[!] HTTP server lỗi nghiêm trọng: {type(e).__name__}: {e}')
        raise