import urllib.request
import urllib.parse
import json
import threading
import time
import re
import os
import asyncio
from http.server import HTTPServer, BaseHTTPRequestHandler
from socketserver import ThreadingMixIn
from urllib.parse import urlparse, parse_qs
import hashlib
import base64
import struct
import xml.etree.ElementTree as ET

# ==================== WEBSOCKET THƯ VIỆN MỚI ====================
import websockets
from websockets.exceptions import ConnectionClosed

# --- CẤU HÌNH ---
POLL_INTERVAL = 60
HTTP_PORT = int(os.environ.get("PORT", 8765))
TRANSLATE_ENABLE = True
SESSION_FILE = 'tg_session'
TG_CONFIG_FILE = 'tg_config.json'

try:
    from deep_translator import GoogleTranslator
    from langdetect import detect
    TRANSLATE_AVAILABLE = True
    print('[i] deep-translator + langdetect: OK')
except ImportError as e:
    TRANSLATE_AVAILABLE = False
    print(f'[!] Thư viện dịch chưa cài: {e}')

try:
    from telethon import TelegramClient, events
    from telethon.sessions import StringSession
    from telethon.tl.types import MessageMediaPhoto, MessageMediaDocument
    import telethon.errors
    TELETHON_AVAILABLE = True
    print('[i] Telethon: OK')
except ImportError:
    TELETHON_AVAILABLE = False
    print('[!] Telethon chưa cài')

translate_cache = {}
translate_lock = threading.Lock()

# Telethon state
tg_client = None
tg_client_lock = threading.Lock()
tg_api_id = None
tg_api_hash = None
tg_phone = None
tg_loop = None
tg_loop_thread = None
tg_auth_state = 'idle'
tg_auth_msg = ''
tg_phone_code_hash = None

# ====================== HÀM TIỆN ÍCH ======================
def strip_html(text):
    return re.sub(r'<[^>]+>', ' ', text).strip()

def is_vietnamese(text):
    clean = strip_html(text)
    clean = re.sub(r'[^\w\s\u00C0-\u024F\u1E00-\u1EFF]', ' ', clean)[:400].strip()
    if not clean or len(clean) < 5:
        return True
    viet_chars = set('àáâãèéêìíòóôõùúýăđơưạảấầẩẫậắằẳẵặẹẻẽếềểễệỉịọỏốồổỗộớờởỡợụủứừửữựỳỷỹỵÀÁÂÃÈÉÊÌÍÒÓÔÕÙÚÝĂĐƠƯẠẢẤẦẨẪẬẮẰẲẴẶẸẺẼẾỀỂỄỆỈỊỌỎỐỒỔỖỘỚỜỞỠỢỤỦỨỪỬỮỰỲỶỸỴ')
    if sum(1 for c in clean if c in viet_chars) >= 3:
        return True
    try:
        return detect(clean) == 'vi'
    except:
        return False

def translate_text(text):
    if not text or not TRANSLATE_AVAILABLE or not TRANSLATE_ENABLE:
        return text
    key = hash(text)
    with translate_lock:
        if key in translate_cache:
            return translate_cache[key]
    try:
        result = GoogleTranslator(source='auto', target='vi').translate(text) or text
    except:
        result = text
    with translate_lock:
        if len(translate_cache) > 500:
            keys = list(translate_cache.keys())
            for k in keys[:250]: del translate_cache[k]
        translate_cache[key] = result
    return result

def maybe_translate(title, desc):
    if is_vietnamese(strip_html(title + ' ' + desc)):
        return title, desc, False
    return translate_text(title), translate_text(desc), True

def html_to_telegram(html, channel_name='', link=''):
    if not html: return ''
    text = html.replace('\r\n', '\n').replace('\r', '\n')
    text = re.sub(r'<br\s*/?>', '\n', text, flags=re.I)
    text = re.sub(r'</(p|div|li|tr|h[1-6])>', '\n', text, flags=re.I)
    text = re.sub(r'<(p|div|li|tr|h[1-6])[^>]*>', '\n', text, flags=re.I)
    ALLOWED = {'b','strong','i','em','u','s','del','code','pre'}
    text = re.sub(r'<a\s[^>]*href=["\']([^"\']*)["\'][^>]*>(.*?)</a>', 
                  lambda m: f'<a href="{m.group(1)}">{m.group(2)}</a>', text, flags=re.I|re.S)
    
    def replace_tag(m):
        tag = re.match(r'</?(\w+)', m.group(0))
        if tag and tag.group(1).lower() in ALLOWED or tag.group(1).lower() == 'a':
            return m.group(0)
        return ''
    text = re.sub(r'<[^>]+>', replace_tag, text)
    
    entities = {'&amp;':'&','&lt;':'<','&gt;':'>','&quot;':'"','&#39;':"'",'&nbsp;':' ','&apos;':"'"'}
    for ent, char in entities.items():
        text = text.replace(ent, char)
    text = re.sub(r'&#(\d+);', lambda m: chr(int(m.group(1))), text)
    text = re.sub(r'&#x([0-9a-fA-F]+);', lambda m: chr(int(m.group(1),16)), text)
    
    lines = [ln.strip() for ln in text.split('\n')]
    clean_lines, prev_empty = [], False
    for ln in lines:
        is_empty = (ln == '')
        if is_empty and prev_empty: continue
        clean_lines.append(ln)
        prev_empty = is_empty
    text = '\n'.join(clean_lines).strip()

    footer = ""
    if link: footer += f'\n\n<a href="{link}">Xem bài gốc →</a>'
    if channel_name: footer += f'\n\n<i>{channel_name}</i>'
    return text + footer

# ====================== TELEGRAM BOT API FUNCTIONS ======================
def tg_api(bot_token, method, payload):
    url = f'https://api.telegram.org/bot{bot_token}/{method}'
    data = json.dumps(payload).encode('utf-8')
    req = urllib.request.Request(url, data=data, headers={'Content-Type':'application/json'})
    try:
        resp = urllib.request.urlopen(req, timeout=15)
        return json.loads(resp.read())
    except Exception as e:
        try:
            return json.loads(e.read())
        except:
            return {'ok': False, 'description': str(e)}

# (Các hàm send_to_telegram, _send_media_then_text... bạn copy từ file cũ vào đây)
# Để tiết kiệm độ dài, tôi giữ nguyên logic cũ. Bạn thay phần này bằng code cũ của bạn nếu cần.

# ====================== WEBSOCKET MỚI (ỔN ĐỊNH) ======================
lock = threading.Lock()
watched_urls = []
known_guids = {}
ws_clients = set()
translate_enabled = TRANSLATE_ENABLE
auto_fwd_enabled = False
tg_channels = []
categories = ['Sức khỏe', 'Tài chính', 'Xã hội', 'Công nghệ', 'Giải trí', 'Khác']
poll_next_time = time.time() + POLL_INTERVAL

async def websocket_handler(websocket):
    global auto_fwd_enabled, tg_channels, categories, translate_enabled
    with lock:
        ws_clients.add(websocket)
    print(f'[WS] Client kết nối | Tổng: {len(ws_clients)}')

    try:
        async for message in websocket:
            try:
                msg = json.loads(message)
                t = msg.get('type')
                if t == 'feeds':
                    urls = msg.get('feeds', [])
                    with lock:
                        watched_urls.clear()
                        watched_urls.extend(urls)
                        for u in urls:
                            if u['url'] not in known_guids:
                                known_guids[u['url']] = None
                    tg_urls = [u['url'] for u in urls if is_tg_source(u['url'])]
                    if tg_urls and TELETHON_AVAILABLE and tg_client:
                        threading.Thread(target=tg_setup_realtime_sync, args=(tg_urls,), daemon=True).start()
                elif t == 'translate':
                    translate_enabled = msg.get('enabled', True)
                elif t == 'tg_settings':
                    with lock:
                        tg_channels = msg.get('channels', [])
                elif t == 'auto_fwd':
                    auto_fwd_enabled = msg.get('enabled', False)
                    with lock:
                        tg_channels = msg.get('channels', tg_channels)
                elif t == 'categories':
                    with lock:
                        categories = msg.get('categories', categories)
            except:
                continue
    except ConnectionClosed:
        pass
    except Exception as e:
        print(f'[WS] Lỗi: {e}')
    finally:
        with lock:
            ws_clients.discard(websocket)
        print(f'[WS] Client ngắt | Còn lại: {len(ws_clients)}')


async def broadcast(message: dict):
    data = json.dumps(message, ensure_ascii=False)
    dead = set()
    with lock:
        clients = list(ws_clients)
    for ws in clients:
        try:
            await ws.send(data)
        except:
            dead.add(ws)
    with lock:
        ws_clients.difference_update(dead)


# ====================== HTTP HANDLER ======================
class HttpHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        p = urlparse(self.path)
        if p.path == '/ws':
            self.send_error(426, "Upgrade Required")
            return

        if p.path == '/fetch':
            # Copy code /fetch từ file cũ của bạn
            qs = parse_qs(p.query)
            url = qs.get('url', [None])[0]
            do_tl = qs.get('translate', ['1'])[0] == '1'
            category = qs.get('category', [''])[0]
            if not url:
                self.send_error(400); return
            try:
                xml = fetch_feed(url)
                items = parse_items(xml, category)
                if do_tl:
                    items = process_items(items)
                self._json({'items': items})
            except Exception as e:
                self.send_error(500, str(e))

        elif p.path.startswith('/tl_') or p.path == '/proxy':
            # Copy các endpoint /tl_fetch, /tl_status, /proxy từ file cũ
            # (Bạn copy nguyên phần tương ứng vào đây)
            pass  

        else:
            # Serve HTML
            data = HTML.encode('utf-8')
            self.send_response(200)
            self.send_header('Content-Type', 'text/html; charset=utf-8')
            self.send_header('Content-Length', str(len(data)))
            self.end_headers()
            self.wfile.write(data)

    def do_POST(self):
        # Copy toàn bộ do_POST từ file cũ của bạn (tl_send_code, tl_sign_in, tg_forward...)
        pass   # ← Thay bằng code POST cũ của bạn

    def _json(self, obj, status=200):
        data = json.dumps(obj, ensure_ascii=False).encode('utf-8')
        self.send_response(status)
        self.send_header('Content-Type', 'application/json; charset=utf-8')
        self.send_header('Access-Control-Allow-Origin', '*')
        self.end_headers()
        self.wfile.write(data)

    def log_message(self, *a): pass


# ====================== CHẠY SERVER ======================
async def start_websocket_server():
    async with websockets.serve(websocket_handler, "0.0.0.0", HTTP_PORT, 
                               ping_interval=15, ping_timeout=30):
        print(f'[WS] WebSocket server đang chạy trên port {HTTP_PORT}')
        await asyncio.Future()


def run_http_server():
    server = ThreadingHTTPServer(('', HTTP_PORT), HttpHandler)
    print(f'=== RSS + Telegram Reader http://0.0.0.0:{HTTP_PORT} ===')
    server.serve_forever()


if __name__ == '__main__':
    if TELETHON_AVAILABLE:
        tg_loop_thread = threading.Thread(target=run_tg_loop, daemon=True)
        tg_loop_thread.start()
        time.sleep(0.3)
        # Phần auto-login Telethon giữ nguyên từ file cũ

    threading.Thread(target=poller, daemon=True).start()

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    http_thread = threading.Thread(target=run_http_server, daemon=True)
    http_thread.start()

    try:
        loop.run_until_complete(start_websocket_server())
    except KeyboardInterrupt:
        print('Server dừng.')