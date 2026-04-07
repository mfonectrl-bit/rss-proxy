import urllib.request
import urllib.parse
import json
import threading
import time
import re
import os
import asyncio
import queue as _queue
from http.server import HTTPServer, BaseHTTPRequestHandler
from socketserver import ThreadingMixIn
from urllib.parse import urlparse, parse_qs
import hashlib
import base64
import struct
import socket
import xml.etree.ElementTree as ET

# --- CẤU HÌNH ---
POLL_INTERVAL    = 60
HTTP_PORT = int(os.environ.get("PORT", 8765))
TRANSLATE_ENABLE = True
SESSION_FILE     = 'tg_session'
TG_CONFIG_FILE   = 'tg_config.json'

# --- Engine dịch ---
# Đọc API keys từ env var
GEMINI_API_KEY = os.environ.get('GEMINI_API_KEY', '').strip()
DEEPL_API_KEY  = os.environ.get('DEEPL_API_KEY', '').strip()

# Engine mặc định: gemini nếu có key, deepl nếu có key, google nếu không có gì
def _default_engine():
    if GEMINI_API_KEY:
        return 'gemini'
    if DEEPL_API_KEY:
        return 'deepl'
    return 'google'

translate_engine = _default_engine()  # có thể thay đổi qua WS message

# ================= AI SERVICE (Multi-AI Rotation) =================
# Kiến trúc mới: xoay vòng Gemini → DeepL → Google
# Không còn phụ thuộc 1 API, tránh rate limit, classify category luôn khi dùng Gemini
class AIService:
    """
    Multi-AI rotation engine với Circuit Breaker + Exponential Backoff.

    Luồng xử lý:
      process(text) → (translated_text, category)

    Circuit Breaker (per provider):
      - Nếu provider bị lỗi liên tiếp >= FAIL_THRESHOLD lần → "mở mạch" (skip) trong COOLDOWN giây
      - Sau COOLDOWN → thử lại 1 lần (half-open), nếu OK → đóng mạch, nếu vẫn lỗi → gia hạn thêm COOLDOWN

    Exponential Backoff (chỉ cho Gemini 429):
      - Lần 1: chờ 2s → retry
      - Lần 2: chờ 4s → retry
      - Lần 3: mở circuit, fallback sang provider tiếp theo
    """
    FAIL_THRESHOLD = 3        # số lần lỗi liên tiếp để mở circuit
    COOLDOWN       = 60.0     # giây circuit ở trạng thái mở trước khi thử lại
    MAX_RETRY_429  = 2        # số lần retry Gemini khi gặp 429 trước khi fallback
    BACKOFF_BASE   = 2.0      # giây backoff cơ bản (nhân đôi mỗi lần)

    def __init__(self):
        self._providers = ['gemini', 'deepl', 'google']
        # Circuit breaker state per provider
        self._fail_count  = {p: 0   for p in self._providers}
        self._open_until  = {p: 0.0 for p in self._providers}  # timestamp circuit mở đến khi nào
        self._lock = threading.Lock()

    def _is_open(self, provider):
        """Trả về True nếu circuit đang mở (provider bị skip)"""
        with self._lock:
            if time.time() < self._open_until[provider]:
                return True
            return False

    def _record_success(self, provider):
        with self._lock:
            self._fail_count[provider] = 0
            self._open_until[provider] = 0.0

    def _record_failure(self, provider, is_rate_limit=False):
        with self._lock:
            self._fail_count[provider] += 1
            if self._fail_count[provider] >= self.FAIL_THRESHOLD:
                cooldown = self.COOLDOWN * (2 if is_rate_limit else 1)
                self._open_until[provider] = time.time() + cooldown
                print(f'[AI] Circuit OPEN: {provider} — nghỉ {cooldown:.0f}s')

    def _run_gemini(self, text):
        """Dịch + classify category bằng Gemini với retry/backoff khi 429"""
        if not GEMINI_API_KEY:
            return None
        prompt = (
            'Translate the following text to Vietnamese. '
            'Also classify it into one of: politics, health, tech, finance, entertainment, default. '
            'Return ONLY valid JSON, no markdown, no explanation: '
            '{"text":"<translated>","category":"<category>"}.\n\nTEXT: ' + text[:3000]
        )
        payload = json.dumps({
            'contents': [{'parts': [{'text': prompt}]}],
            'generationConfig': {'temperature': 0.1, 'maxOutputTokens': 2048}
        }).encode('utf-8')
        url = (f'https://generativelanguage.googleapis.com/v1beta/models/'
               f'gemini-2.0-flash:generateContent?key={GEMINI_API_KEY}')

        for attempt in range(self.MAX_RETRY_429 + 1):
            try:
                req  = urllib.request.Request(url, data=payload, headers={'Content-Type': 'application/json'})
                resp = urllib.request.urlopen(req, timeout=15)
                data = json.loads(resp.read())
                raw  = data['candidates'][0]['content']['parts'][0]['text'].strip()
                raw  = re.sub(r'^```[a-z]*\n?', '', raw).rstrip('`').strip()
                result = json.loads(raw)
                self._record_success('gemini')
                return result.get('text', ''), result.get('category', 'default')
            except urllib.error.HTTPError as e:
                if e.code == 429:
                    if attempt < self.MAX_RETRY_429:
                        wait = self.BACKOFF_BASE * (2 ** attempt)
                        print(f'[AI] Gemini 429 — backoff {wait:.0f}s (lần {attempt+1}/{self.MAX_RETRY_429})')
                        time.sleep(wait)
                        continue
                    else:
                        print(f'[AI] Gemini 429 — đã retry {self.MAX_RETRY_429} lần, mở circuit')
                        self._record_failure('gemini', is_rate_limit=True)
                        return None
                else:
                    print(f'[AI] Gemini HTTP {e.code}: {e}')
                    self._record_failure('gemini')
                    return None
            except Exception as e:
                print(f'[AI] Gemini lỗi: {e}')
                self._record_failure('gemini')
                return None
        return None

    def _run_deepl(self, text):
        """Dịch bằng DeepL"""
        if not DEEPL_API_KEY:
            return None
        try:
            base = 'api-free.deepl.com' if DEEPL_API_KEY.endswith(':fx') else 'api.deepl.com'
            payload = urllib.parse.urlencode({
                'auth_key': DEEPL_API_KEY,
                'text': text[:3000],
                'target_lang': 'VI',
            }).encode('utf-8')
            req = urllib.request.Request(
                f'https://{base}/v2/translate', data=payload,
                headers={'Content-Type': 'application/x-www-form-urlencoded'}
            )
            resp = urllib.request.urlopen(req, timeout=15)
            data = json.loads(resp.read())
            translated = data['translations'][0]['text']
            self._record_success('deepl')
            return translated, 'default'
        except urllib.error.HTTPError as e:
            print(f'[AI] DeepL HTTP {e.code}: {e}')
            self._record_failure('deepl', is_rate_limit=(e.code == 429))
            return None
        except Exception as e:
            print(f'[AI] DeepL lỗi: {e}')
            self._record_failure('deepl')
            return None

    def _run_google(self, text):
        """Google Translate — fallback cuối, luôn thành công"""
        try:
            result = _translate_google(text)
            self._record_success('google')
            return result, 'default'
        except Exception as e:
            print(f'[AI] Google lỗi: {e}')
            self._record_failure('google')
            return text, 'default'

    def process(self, text):
        """
        Xử lý text qua AI rotation với circuit breaker.
        Trả về (translated_text, category).
        Provider bị open circuit → bị skip, thử provider tiếp theo.
        """
        if not text or len(text.strip()) < 4:
            return text, 'default'
        if is_vietnamese(text[:400]):
            return text, 'default'

        for provider in self._providers:
            if self._is_open(provider):
                remaining = max(0, self._open_until[provider] - time.time())
                print(f'[AI] Skip {provider} (circuit open, còn {remaining:.0f}s)')
                continue
            result = None
            if provider == 'gemini':
                result = self._run_gemini(text)
            elif provider == 'deepl':
                result = self._run_deepl(text)
            elif provider == 'google':
                result = self._run_google(text)
            if result:
                translated, category = result
                if translated:
                    return translated, category
        # Tất cả fail → trả về text gốc
        print('[AI] Tất cả provider thất bại, giữ nguyên text gốc')
        return text, 'default'

    def status(self):
        """Trả về dict trạng thái circuit breaker để debug/monitoring"""
        now = time.time()
        with self._lock:
            return {
                p: {
                    'fail_count': self._fail_count[p],
                    'open': now < self._open_until[p],
                    'cooldown_remaining': max(0, round(self._open_until[p] - now))
                }
                for p in self._providers
            }

# Singleton AI service — dùng chung toàn bộ app
ai_service = AIService()

# ================= ASYNC PIPELINE (Batch Queue) =================
# Hàng đợi async cho tin Telethon real-time + RSS
# Thay thế process_items / process_tg_items dùng sync threads đơn thuần
class BatchPipeline:
    """
    Pipeline async xử lý tin theo batch với timeout flush.
    - put(item): đẩy item vào queue (non-blocking)
    - Consumer loop: gom tối đa BATCH_SIZE item, hoặc flush sau FLUSH_TIMEOUT giây
      → tránh delay khi traffic thấp (không cần đủ 5 item mới xử lý)
    """
    BATCH_SIZE    = 5
    FLUSH_TIMEOUT = 3.0   # giây: flush batch dù chưa đủ BATCH_SIZE

    def __init__(self):
        self._q = _queue.Queue()
        self._thread = None

    def start(self):
        self._thread = threading.Thread(target=self._consumer_loop, daemon=True)
        self._thread.start()

    def put(self, item):
        self._q.put(item)

    def _consumer_loop(self):
        while True:
            batch = []
            deadline = time.time() + self.FLUSH_TIMEOUT
            # Chờ item đầu tiên (blocking, timeout=FLUSH_TIMEOUT)
            try:
                first = self._q.get(timeout=self.FLUSH_TIMEOUT)
                batch.append(first)
            except _queue.Empty:
                continue  # không có gì trong FLUSH_TIMEOUT → loop tiếp

            # Drain thêm không block cho đến hết BATCH_SIZE hoặc hết deadline
            while len(batch) < self.BATCH_SIZE:
                remaining = deadline - time.time()
                if remaining <= 0:
                    break  # hết timeout → flush ngay dù chưa đủ batch
                try:
                    item = self._q.get(timeout=min(remaining, 0.1))
                    batch.append(item)
                except _queue.Empty:
                    break  # queue trống → flush luôn

            if batch:
                try:
                    self._process_batch(batch)
                except Exception as e:
                    print(f'[Pipeline] Batch lỗi: {e}')

    def _process_batch(self, batch):
        """
        Xử lý một batch: AI translate → route → broadcast UI → forward qua Telethon.
        Không còn download bytes — Telethon gửi thẳng msg.media object hoặc URL.
        """
        by_feed = {}
        for item in batch:
            fu = item.get('feed_url', '')
            by_feed.setdefault(fu, []).append(item)

        for feed_url, items in by_feed.items():
            processed = []
            for item in items:
                text = item.get('text', '')
                translated, ai_category = ai_service.process(text)
                if not item.get('category'):
                    item['category'] = _map_category(ai_category)
                item['text_translated'] = translated
                desc_html = translated.replace('\n', '<br>')
                title     = (translated[:80] + '...') if len(translated) > 80 else translated
                item['desc']      = desc_html
                item['title']     = title
                item['translated'] = True
                item['show_link']  = item.get('show_link', True)
                processed.append(item)

            if not processed:
                continue

            # 1. Broadcast lên UI ngay (không chờ forward)
            ws_items = [{k: v for k, v in it.items()
                         if k not in ('_tg_has_media', 'text', 'text_translated')}
                        for it in processed]
            broadcast({'type': 'new_items', 'url': feed_url, 'items': ws_items})

            # 2. Forward qua Telethon trên thread riêng
            feed_category = processed[0].get('category', '')
            threading.Thread(
                target=_do_forward,
                args=(processed, feed_category, feed_url),
                daemon=True
            ).start()

def _map_category(ai_category):
    """
    Map AI category về category hệ thống.
    AI trả về: politics, health, tech, finance, entertainment, default
    Hệ thống dùng: Sức khỏe, Tài chính, Công nghệ, Giải trí, Xã hội, Khác
    """
    mapping = {
        'politics':      'Xã hội',
        'health':        'Sức khỏe',
        'tech':          'Công nghệ',
        'finance':       'Tài chính',
        'entertainment': 'Giải trí',
        'default':       'Khác',
    }
    return mapping.get(ai_category, 'Khác')

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

print(f'[i] Engine dịch mặc định: {_default_engine()} | Gemini={"có" if GEMINI_API_KEY else "không"} | DeepL={"có" if DEEPL_API_KEY else "không"}')

try:
    from telethon import TelegramClient, events
    from telethon.sessions import StringSession
    from telethon.tl.types import MessageMediaPhoto, MessageMediaDocument
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
tg_client_lock   = threading.Lock()
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
    """
    Dịch text sang tiếng Việt.
    - Nếu engine chỉ định → dùng engine đó (override thủ công từ UI dropdown).
    - Nếu engine lỗi → fallback về AIService rotation thay vì chỉ Google.
    """
    if not text or len(text.strip()) < 4:
        return text
    eng = engine or translate_engine
    try:
        if eng == 'gemini' and GEMINI_API_KEY:
            return _translate_gemini(text)
        elif eng == 'deepl' and DEEPL_API_KEY:
            return _translate_deepl(text)
        else:
            return _translate_google(text)
    except Exception as e:
        print(f'[!] Dịch [{eng}] lỗi: {e}, fallback AI rotation')
        # Fallback về AIService rotation (Gemini→DeepL→Google) thay vì chỉ Google
        translated, _ = ai_service.process(text)
        return translated or text

def _translate_gemini(text):
    """Dịch bằng Gemini API (gemini-2.0-flash — nhanh và miễn phí)"""
    prompt = (
        'Dịch đoạn văn bản sau sang tiếng Việt. '
        'Giữ nguyên tên riêng, thuật ngữ chuyên ngành, số liệu và ký hiệu. '
        'Chỉ trả về bản dịch, không giải thích thêm.\n\n' + text[:4000]
    )
    payload = json.dumps({
        'contents': [{'parts': [{'text': prompt}]}],
        'generationConfig': {'temperature': 0.1, 'maxOutputTokens': 2048}
    }).encode('utf-8')
    url = f'https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash:generateContent?key={GEMINI_API_KEY}'
    req = urllib.request.Request(url, data=payload, headers={'Content-Type': 'application/json'})
    resp = urllib.request.urlopen(req, timeout=15)
    data = json.loads(resp.read())
    return data['candidates'][0]['content']['parts'][0]['text'].strip()

def _translate_deepl(text):
    """Dịch bằng DeepL Free API"""
    payload = urllib.parse.urlencode({
        'auth_key': DEEPL_API_KEY,
        'text': text[:4000],
        'target_lang': 'VI',
        'source_lang': 'EN',
    }).encode('utf-8')
    # DeepL Free dùng api-free.deepl.com, Pro dùng api.deepl.com
    base = 'api-free.deepl.com' if DEEPL_API_KEY.endswith(':fx') else 'api.deepl.com'
    req = urllib.request.Request(
        f'https://{base}/v2/translate', data=payload,
        headers={'Content-Type': 'application/x-www-form-urlencoded'}
    )
    resp = urllib.request.urlopen(req, timeout=15)
    data = json.loads(resp.read())
    return data['translations'][0]['text'].strip()

def _translate_google(text):
    """Dịch bằng Google Translate (deep_translator) — fallback"""
    return GoogleTranslator(source='auto', target='vi').translate(text) or text

def strip_html(text):
    return re.sub(r'<[^>]+>', ' ', text).strip()

def is_vietnamese(text):
    """Kiểm tra xem text có phải tiếng Việt không — dùng nhiều tín hiệu để tránh false positive"""
    clean = strip_html(text)
    # Lấy tối đa 400 ký tự, bỏ emoji và ký tự đặc biệt
    clean = re.sub(r'[^\w\s\u00C0-\u024F\u1E00-\u1EFF]', ' ', clean)[:400].strip()
    if not clean or len(clean) < 5:
        return True  # text quá ngắn, không cần dịch
    # Dấu hiệu rõ ràng là tiếng Việt: có dấu thanh điệu đặc trưng
    viet_chars = set('àáâãèéêìíòóôõùúýăđơưạảấầẩẫậắằẳẵặẹẻẽếềểễệỉịọỏốồổỗộớờởỡợụủứừửữựỳỷỹỵ'
                     'ÀÁÂÃÈÉÊÌÍÒÓÔÕÙÚÝĂĐƠƯẠẢẤẦẨẪẬẮẰẲẴẶẸẺẼẾỀỂỄỆỈỊỌỎỐỒỔỖỘỚỜỞỠỢỤỦỨỪỬỮỰỲỶỸỴ')
    viet_count = sum(1 for c in clean if c in viet_chars)
    if viet_count >= 3:
        return True
    # Fallback langdetect
    try:
        lang = detect(clean)
        return lang == 'vi'
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
        if len(translate_cache) > 500:
            keys = list(translate_cache.keys())
            for k in keys[:250]:
                del translate_cache[k]
        translate_cache[key] = result
    return result

def maybe_translate(title, desc):
    sample = strip_html(title + ' ' + desc)
    if is_vietnamese(sample):
        return title, desc, False
    return (translate_text(title) if title else title), (translate_text(desc) if desc else desc), True

def html_to_telegram(html, channel_name='', link=''):
    if not html:
        return ''
    text = html.replace('\r\n', '\n').replace('\r', '\n')
    text = re.sub(r'<br\s*/?>', '\n', text, flags=re.I)
    text = re.sub(r'</(p|div|li|tr|h[1-6])>', '\n', text, flags=re.I)
    text = re.sub(r'<(p|div|li|tr|h[1-6])[^>]*>', '\n', text, flags=re.I)
    ALLOWED = {'b','strong','i','em','u','s','del','code','pre'}

    text = re.sub(r'<a\s[^>]*href=["\']([^"\']*)["\'][^>]*>(.*?)</a>',
                  lambda m: f'<a href="{m.group(1)}">{m.group(2)}</a>', text, flags=re.I|re.S)

    def replace_tag(m):
        tag = re.match(r'</?(\w+)', m.group(0))
        if tag and tag.group(1).lower() in ALLOWED:
            return m.group(0)
        if tag and tag.group(1).lower() == 'a':
            return m.group(0)
        return ''

    text = re.sub(r'<[^>]+>', replace_tag, text)

    entities = {'&amp;':'&','&lt;':'<','&gt;':'>','&quot;':'"','&#39;':"'",'&nbsp;':' ','&apos;':"'"}
    for ent, char in entities.items():
        text = text.replace(ent, char)
    text = re.sub(r'&#(\d+);', lambda m: chr(int(m.group(1))), text)
    text = re.sub(r'&#x([0-9a-fA-F]+);', lambda m: chr(int(m.group(1),16)), text)

    lines = [ln.strip() for ln in text.split('\n')]
    clean_lines, prev_empty = [], False
    for ln in lines:
        is_empty = (ln == '')
        if is_empty and prev_empty:
            continue
        clean_lines.append(ln)
        prev_empty = is_empty
    text = '\n'.join(clean_lines).strip()

    footer = ""
    if link:
        footer += f'\n\n<a href="{link}">Xem bài gốc →</a>'
    if channel_name:
        footer += f'\n\n<i>{channel_name}</i>'

    return text + footer


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
    global tg_loop
    tg_loop = asyncio.new_event_loop()
    asyncio.set_event_loop(tg_loop)
    tg_loop.run_forever()

def tg_run(coro):
    """Chạy coroutine trên tg_loop từ thread khác, trả về kết quả"""
    if tg_loop is None:
        raise RuntimeError('Telethon loop chưa khởi động')
    future = asyncio.run_coroutine_threadsafe(coro, tg_loop)
    return future.result(timeout=120)

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

# Hàm cũ giữ lại để không break _tg_fetch messages (dùng trong /tl_fetch endpoint)
async def _tg_fetch_messages(channel, limit=20):
    msgs = await tg_client.get_messages(channel, limit=limit)
    items = []
    for msg in msgs:
        if not msg or not msg.message:
            continue
        guid  = f'tg_{channel}_{msg.id}'
        title = (msg.message[:80] + '...') if len(msg.message) > 80 else msg.message
        desc  = msg.message.replace('\n', '<br>')
        pub   = msg.date.isoformat() if msg.date else ''
        link  = f'https://t.me/{channel.lstrip("@")}/{msg.id}'
        # Không download media ở đây — chỉ download khi forward (/tg_forward sẽ fetch lại)
        item = {
            'guid': guid, 'title': title, 'desc': desc, 'link': link,
            'pubDate': pub, 'translated': False, 'category': '',
            '_tg_media_bytes': None, '_source': 'telethon',
        }
        items.append(item)
    return items

async def _tg_check_auth():
    if tg_client and await tg_client.is_user_authorized():
        return True
    return False

# --- Hàng đợi tin mới từ Telethon event handler ---
tg_new_items_queue = []
tg_new_items_lock  = threading.Lock()

async def _tg_load_history(channel, limit=20):
    """Chỉ gọi 1 lần khi thêm kênh mới — load tin cũ để init known_guids"""
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

async def _tg_setup_realtime(feed_urls):
    """Setup real-time listener cho danh sách feed URLs"""
    global _tg_new_message_handler, _resolved_channels_cache

    if not tg_client or not await tg_client.is_user_authorized():
        return

    # Xóa TẤT CẢ event handlers hiện tại để tránh duplicate
    try:
        for callback, event in tg_client.list_event_handlers():
            tg_client.remove_event_handler(callback, event)
    except Exception:
        pass
    _tg_new_message_handler = None

    if not feed_urls:
        return

    raw_channels = [normalize_tg_channel(u).lstrip('@') for u in feed_urls]

    # Resolve channel — dùng cache để tránh gọi API lặp lại mỗi lần WS reconnect
    channels_list = []
    for ch in raw_channels:
        if ch in _resolved_channels_cache:
            channels_list.append(ch)
            continue
        try:
            await tg_client.get_input_entity(ch)
            _resolved_channels_cache.add(ch)
            channels_list.append(ch)
        except Exception as e:
            print(f'[TG] Bỏ qua channel không hợp lệ @{ch}: {e}')

    if not channels_list:
        print('[TG] Không có channel hợp lệ nào để đăng ký real-time')
        return

    print(f'[TG] Đăng ký real-time cho: {channels_list}')

    # Set lưu grouped_id đã xử lý để không tạo duplicate item
    handled_groups = set()

    async def handler(event):
        msg = event.message
        if not msg or not msg.message:
            return
        chat_username = getattr(event.chat, 'username', None)
        if not chat_username:
            return

        # Bỏ qua nếu là tin phụ trong group (đã có tin chính)
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
            handled_groups.add(msg.grouped_id)

        guid  = f'tg_@{chat_username}_{msg.id}'
        link  = f'https://t.me/{chat_username}/{msg.id}'
        desc  = msg.message.replace('\n', '<br>')
        title = (msg.message[:80] + '...') if len(msg.message) > 80 else msg.message
        pub   = msg.date.isoformat() if msg.date else ''
        has_media = bool(msg.media and isinstance(msg.media, (MessageMediaPhoto, MessageMediaDocument)))

        item = {
            'guid': guid, 'title': title, 'desc': desc, 'link': link,
            'pubDate': pub, 'translated': False, 'category': category,
            '_tg_media_bytes': None, '_tg_has_media': has_media,
            '_tg_msg_id': msg.id, '_tg_chat': chat_username,
            '_tg_grouped_id': msg.grouped_id,  # None nếu không phải group
            '_source': 'telethon', '_feed_url': feed_url,
        }
        with tg_new_items_lock:
            tg_new_items_queue.append(item)
        print(f'[TG] Tin mới real-time: @{chat_username} #{msg.id}')

    tg_client.add_event_handler(handler, events.NewMessage(chats=channels_list))
    _tg_new_message_handler = handler

def tg_setup_realtime_sync(feed_urls):
    """Wrapper đồng bộ để gọi từ thread thường"""
    if tg_loop and TELETHON_AVAILABLE:
        try:
            tg_run(_tg_setup_realtime(feed_urls))
        except Exception as e:
            print(f'[!] Setup real-time lỗi: {e}')

def tg_load_history_sync(channel, limit=20):
    """Load lịch sử — chỉ gọi 1 lần khi thêm kênh mới"""
    return tg_run(_tg_load_history(channel, limit))

def tg_fetch_channel(channel, limit=20):
    """Fetch tin từ một kênh TG — dùng cho poller và /tl_fetch endpoint"""
    return tg_run(_tg_load_history(channel, limit))

# --- STATE ---
lock = threading.Lock()
watched_urls = []
known_guids = {}
ws_clients = set()
translate_enabled = TRANSLATE_ENABLE
auto_fwd_enabled = False
tg_channels = []
categories = ['Sức khỏe', 'Tài chính', 'Xã hội', 'Công nghệ', 'Giải trí', 'Khác']
poll_next_time = time.time() + POLL_INTERVAL

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
            # ⚠️ Set timeout 30s để phát hiện disconnect nhanh
            self._sock.settimeout(30)
            
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
        except (ConnectionError, OSError, socket.timeout, BrokenPipeError):
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
.feed-row{display:flex;align-items:center;padding:7px 10px 7px 14px;cursor:pointer;gap:8px;font-size:12px;background:#fff}
.sb-section{padding:.35rem .75rem .15rem;font-size:11px;font-weight:600;color:#bbb;text-transform:uppercase;letter-spacing:.5px;background:#fff}
.feed-row:hover{background:#f8f8f4}
.feed-row.active{background:#f0f0e8;font-weight:600}
.feed-row .fname{font-size:12px}
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
/* Telethon auth */
.tg-auth-box{background:#f0fdf4;border:1px solid #bbf7d0;border-radius:10px;padding:14px 18px;margin-bottom:10px}
.tg-auth-box.error{background:#fef2f2;border-color:#fecaca}
.tg-auth-box.warn{background:#fffbeb;border-color:#fde68a}
.auth-step{display:none}.auth-step.active{display:block}
.feed-badge{display:inline-block;padding:1px 5px;border-radius:6px;font-size:10px;font-weight:600;margin-left:4px;vertical-align:middle}
.badge-tg{background:#dbeafe;color:#1d4ed8}
.badge-rss{background:#dcfce7;color:#166534}
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
</header>
<div class="tg-bar">
<label>Telegram</label>
<button class="tg-save" onclick="openChannelManager()">Quản lý kênh</button>
<button class="tg-save" style="background:#6366f1" onclick="openTelethonModal()">⚡ Telethon</button>
<span id="tg-saved" style="display:none;color:#16a34a;font-size:11px">✓ Đã lưu</span>
<span id="tg-auth-badge" style="font-size:11px;color:#aaa">Telethon: chưa kết nối</span>
<span style="margin-left:auto;display:flex;align-items:center;gap:6px;font-size:12px;color:#92400e;font-weight:600">
  🌐 Dịch:
  <select id="engine-select" onchange="setTranslateEngine(this.value)"
    style="padding:4px 8px;border:1px solid #fcd34d;border-radius:7px;font-size:12px;background:#fff;cursor:pointer">
    <option value="gemini">Gemini (tốt nhất)</option>
    <option value="deepl">DeepL</option>
    <option value="google">Google Translate</option>
  </select>
</span>
</div>
</div>
<div class="layout">
<aside id="aside">
<div style="padding:.75rem;border-bottom:1px solid #f0f0e8">
<button onclick="openModal()" style="width:100%;padding:7px;background:#1a1a1a;color:#fff;border:none;border-radius:8px;cursor:pointer;font-size:13px">+ Thêm feed</button>
<input type="text" id="search" placeholder="Tìm kiếm..." oninput="applyFilter()" style="width:100%;padding:6px;margin-top:6px;border:1px solid #e0e0d8;border-radius:8px;font-size:13px">
</div>
<div style="padding:.35rem .75rem;font-size:11px;color:#bbb;background:#fff">Nguồn</div>
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
<span id="item-count"></span>
</div>
</div>
<div id="stream" style="flex:1;overflow-y:auto;padding:1rem 1.5rem;background:#fff"></div>
</main>
</div>

<!-- Modal thêm/sửa feed -->
<div class="modal-bg" id="modal">
<div class="modal">
<div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:1rem">
  <h2 id="modal-title" style="margin-bottom:0">Thêm feed</h2>
  <label style="display:flex;align-items:center;gap:6px;font-size:12px;cursor:pointer;background:#f0fdf4;border:1px solid #bbf7d0;padding:4px 10px;border-radius:8px" id="auto-fwd-toggle-wrap">
    <input type="checkbox" id="new-auto-fwd" style="width:14px;height:14px;cursor:pointer">
    <span>Auto-forward</span>
  </label>
</div>
<input type="text" id="new-name" placeholder="Tên feed">
<input type="text" id="new-url" placeholder="URL RSS hoặc @username / t.me/channel">
<select id="new-category"></select>
<div id="feed-type-hint" style="font-size:11px;color:#6366f1;margin-bottom:8px;display:none">⚡ Nguồn Telegram — sẽ dùng Telethon</div>
<label style="display:flex;align-items:center;gap:8px;font-size:13px;margin-bottom:10px;cursor:pointer">
  <input type="checkbox" id="new-show-link" checked style="width:16px;height:16px;cursor:pointer">
  Hiển thị link "Xem bài gốc →" khi forward
</label>
<!-- Dropdown chọn kênh đích -->
<div id="feed-channel-wrap" style="margin-bottom:10px;display:none">
  <div style="font-size:12px;color:#555;margin-bottom:6px;font-weight:600">Forward tới kênh:</div>
  <div id="feed-channel-list" style="border:1px solid #e0e0d8;border-radius:8px;max-height:140px;overflow-y:auto;padding:4px 0;background:#fff"></div>
</div>
<div class="modal-btns">
<button onclick="closeModal()">Hủy</button>
<button class="btn-ok" id="btn-add-feed" onclick="addFeed()">Thêm</button>
</div>
</div>
</div>

<!-- Modal quản lý kênh Telegram -->
<div class="modal-bg" id="channel-modal">
<div class="modal">
<h2>Quản lý Kênh Telegram</h2>
<div style="background:#eff6ff;border:1px solid #bfdbfe;border-radius:8px;padding:8px 12px;margin-bottom:10px;font-size:12px;color:#1e40af">
  ⚡ Hệ thống dùng <b>Telethon</b> để gửi — nhập <b>@username</b> hoặc <b>-100xxx</b> của kênh đích. Không cần Bot Token.
</div>
<select id="ch-select-modal" onchange="loadChannelToForm(this.value)"></select>
<input type="text" id="ch-username" placeholder="@kênh_đích hoặc -100xxxxxxxxx">
<input type="text" id="ch-name" placeholder="Tên hiển thị (tuỳ chọn)">
<select id="ch-type">
<option value="master">Master (Tất cả tin)</option>
<option value="category">Category (theo chủ đề)</option>
<option value="group">Group</option>
</select>
<select id="ch-category"></select>
<div style="margin:10px 0;padding:10px;background:#f9f9f9;border-radius:8px">
<div style="font-size:12px;font-weight:600;margin-bottom:8px">Quản lý chủ đề:</div>
<div id="category-list"></div>
<div style="display:flex;gap:6px;margin-top:8px">
<input type="text" id="new-category-input" placeholder="Tên chủ đề mới" style="flex:1;padding:6px;border:1px solid #ddd;border-radius:6px;font-size:12px">
<button onclick="addCategory()" class="btn-sm" style="padding:6px 12px">+ Thêm</button>
</div>
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
<div class="modal">
<h2>Chọn kênh gửi</h2>
<div id="fwd-ch-list" style="max-height:200px;overflow-y:auto;margin:10px 0"></div>
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

<script>
const WS_URL=(location.protocol==='https:'?'wss://':'ws://')+location.host+'/ws';
const DEFAULT_FEEDS=[{name:'VN Wall Street',url:'https://tg.i-c-a.su/rss/vnwallstreet',category:'Tài chính'}];
const DEFAULT_CATEGORIES=['Sức khỏe','Tài chính','Xã hội','Công nghệ','Giải trí','Khác'];
const PAGE_SIZE = 30;

let feeds=JSON.parse(localStorage.getItem('rss_feeds')||'null')||DEFAULT_FEEDS;
let tgChannels=JSON.parse(localStorage.getItem('tg_channels')||'null')||[];
let categories=JSON.parse(localStorage.getItem('categories')||'null')||DEFAULT_CATEGORIES;
let allItems=[],newBadges={},filterUrl=null,searchQ='',ws=null,wsReady=false,shownCount=PAGE_SIZE;
let translateOn=JSON.parse(localStorage.getItem('translate_on')??'true');
let translateEngine=localStorage.getItem('translate_engine')||'gemini';
let selected=new Set(),autoFwd=JSON.parse(localStorage.getItem('auto_fwd')??'false');
let editFeedIndex=-1,selectedChannelIndex=-1;
let pollInterval=60,pollNextIn=0;
let telethonConnected=false;

function saveFeeds(){localStorage.setItem('rss_feeds',JSON.stringify(feeds));}
function saveTgChannels(){localStorage.setItem('tg_channels',JSON.stringify(tgChannels));}
function saveCategories(){localStorage.setItem('categories',JSON.stringify(categories));}

function setTranslateEngine(val){
    translateEngine=val;
    localStorage.setItem('translate_engine',val);
    wsSend({type:'translate_engine',engine:val});
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
    return url.startsWith('@')||url.includes('t.me/');
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
        badge.textContent='⚡ Telethon: đã kết nối';badge.style.color='#16a34a';
    } else {
        badge.textContent='Telethon: chưa kết nối';badge.style.color='#aaa';
    }
}

// --- Feed type hint ---
document.getElementById('new-url').addEventListener('input',function(){
    const hint=document.getElementById('feed-type-hint');
    hint.style.display=isTgSource(this.value.trim())?'block':'none';
});

// --- Category ---
function renderCategoryList(){
    const list=document.getElementById('category-list');
    list.innerHTML=categories.map((cat,idx)=>`
        <div class="category-item">
            <span>${cat}</span>
            ${categories.length>1?`<button onclick="deleteCategory(${idx})">Xóa</button>`:''}
        </div>
    `).join('');
    const selects=['new-category','ch-category'];
    selects.forEach(id=>{
        const sel=document.getElementById(id);
        if(sel) sel.innerHTML=categories.map(c=>`<option value="${c}">${c}</option>`).join('');
    });
}

function addCategory(){
    const input=document.getElementById('new-category-input');
    const name=input.value.trim();
    if(!name){alert('Nhập tên chủ đề');return;}
    if(categories.includes(name)){alert('Chủ đề đã tồn tại');return;}
    categories.push(name);saveCategories();renderCategoryList();input.value='';
    wsSend({type:'categories',categories});
}

function deleteCategory(idx){
    if(categories.length<=1){alert('Phải có ít nhất 1 chủ đề');return;}
    if(!confirm('Xóa chủ đề "'+categories[idx]+'"?'))return;
    categories.splice(idx,1);saveCategories();renderCategoryList();
    wsSend({type:'categories',categories});
}

// --- Channel Manager ---
function renderChannelManager(){
    const select=document.getElementById('ch-select-modal');
    select.innerHTML='<option value="-1">-- Tạo mới --</option>';
    tgChannels.forEach((ch,idx)=>{
        const opt=document.createElement('option');
        opt.value=idx;opt.textContent=`${idx+1}. ${ch.name} (${ch.type})`;
        select.appendChild(opt);
    });
    if(tgChannels.length>0) loadChannelToForm(0);
    renderCategoryList();
}

function loadChannelToForm(idx){
    selectedChannelIndex=parseInt(idx);
    const form={username:document.getElementById('ch-username'),
        name:document.getElementById('ch-name'),type:document.getElementById('ch-type'),category:document.getElementById('ch-category')};
    if(selectedChannelIndex>=0&&tgChannels[selectedChannelIndex]){
        const ch=tgChannels[selectedChannelIndex];
        form.username.value=ch.username||ch.channel_id||'';
        form.name.value=ch.name;form.type.value=ch.type;
        form.category.value=ch.category_filter||'';
        document.getElementById('btn-ch-del').style.display='inline-block';
    } else {
        form.username.value='';form.name.value='';
        form.type.value='master';form.category.value='';
        document.getElementById('btn-ch-del').style.display='none';
    }
}

function saveTgSettings(){
    const username=document.getElementById('ch-username').value.trim();
    const name=document.getElementById('ch-name').value.trim()||username;
    const type=document.getElementById('ch-type').value;
    const category=document.getElementById('ch-category').value;
    if(!username){alert('Nhập @username hoặc channel ID');return;}
    // Chuẩn hoá: thêm @ nếu chưa có và không phải ID số
    const normUsername = (!username.startsWith('@') && !username.startsWith('-') && !/^\d/.test(username))
        ? '@'+username : username;
    const chObj={username:normUsername,name,type,category_filter:category};
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
    const list=document.getElementById('fwd-ch-list');list.innerHTML='';
    tgChannels.forEach((ch,idx)=>{
        const div=document.createElement('div');
        div.innerHTML=`<input type="checkbox" value="${idx}" checked> ${ch.name} (${ch.type})`;
        div.style.padding='5px';list.appendChild(div);
    });
    document.getElementById('fwd-modal').classList.add('open');
}

async function forwardSelected(){
    const checkboxes=document.querySelectorAll('#fwd-ch-list input:checked');
    const channelIndices=Array.from(checkboxes).map(cb=>parseInt(cb.value));
    if(channelIndices.length===0){alert('Chọn ít nhất 1 kênh');return;}
    const items=allItems.filter(it=>selected.has(it.guid)).map(it=>({guid:it.guid,title:it.title,desc:it.desc,link:it.link,category:it.category,feedUrl:it.feedUrl}));
    const channelsToSend=channelIndices.map(i=>tgChannels[i]);
    try{
        const r=await fetch('/tg_forward',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({channels:channelsToSend,items})});
        const data=await r.json();
        document.getElementById('result-list').innerHTML=data.results.map(r=>`<div style="padding:7px;background:${r.ok?'#f0fdf4':'#fef2f2'}">${r.ok?'✓':'✗'} ${r.title||'Gửi'}${r.error?' - '+r.error:''}</div>`).join('');
        document.getElementById('result-modal').classList.add('open');
        selected.clear();updateFwdBar();
        document.querySelectorAll('input[type=checkbox]').forEach(cb=>cb.checked=false);
        document.querySelectorAll('.item.selected').forEach(el=>el.classList.remove('selected'));
    }catch(e){alert('Lỗi: '+e.message);}
    document.getElementById('fwd-modal').classList.remove('open');
}

// --- Sidebar ---
function renderSidebar(){
    const list=document.getElementById('feed-list');
    const totalNew=Object.values(newBadges).reduce((a,b)=>a+b,0);
    let html=`<div class="feed-row${filterUrl===null?' active':''}" onclick="setFilter(null)">
        <span style="flex:1">Tất cả nguồn</span>${totalNew>0?`<span style="background:#dbeafe;color:#1d4ed8;padding:1px 5px;border-radius:8px;font-size:10px">+${totalNew}</span>`:''}</div>`;
    feeds.forEach((f,i)=>{
        const n=newBadges[f.url]||0,active=filterUrl===f.url;
        const isTg=isTgSource(f.url);
        const badge=`<span class="feed-badge ${isTg?'badge-tg':'badge-rss'}">${isTg?'TG':'RSS'}</span>`;
        html+=`<div class="feed-row${active?' active':''}">
            <span class="fname" style="flex:1" onclick="setFilter('${f.url}')">${f.name}${badge}</span>
            ${n>0&&!active?`<span style="background:#dbeafe;color:#1d4ed8;padding:1px 5px;border-radius:8px;font-size:10px">+${n}</span>`:''}
            <span class="fedit" onclick="openEditFeed(${i})">✎</span><span class="fdel" onclick="deleteFeed(${i})">×</span></div>`;
    });
    list.innerHTML=html;
}

function setFilter(url){
    filterUrl=url;
    if(url) newBadges[url]=0; else Object.keys(newBadges).forEach(k=>newBadges[k]=0);
    document.getElementById('view-label').textContent=url?(feeds.find(f=>f.url===url)?.name||url):'Tất cả nguồn';
    shownCount=PAGE_SIZE;renderSidebar();renderStream();
}

function deleteFeed(i){
    if(!confirm('Xóa feed "'+feeds[i].name+'"?'))return;
    const url=feeds[i].url;feeds.splice(i,1);saveFeeds();
    allItems=allItems.filter(it=>it.feedUrl!==url);
    if(filterUrl===url)filterUrl=null;
    wsSend({type:'feeds',feeds:feeds.map(f=>({url:f.url,name:f.name,category:f.category,show_link:f.show_link!==false,auto_fwd:f.auto_fwd===true,target_channels:f.target_channels||[]}))});
    renderSidebar();renderStream();
}

function renderFeedChannelList(selectedIds){
    const wrap=document.getElementById('feed-channel-wrap');
    const list=document.getElementById('feed-channel-list');
    if(!tgChannels.length){wrap.style.display='none';return;}
    wrap.style.display='block';
    list.innerHTML=tgChannels.map((ch,idx)=>`
        <label style="display:flex;align-items:center;gap:8px;padding:6px 10px;cursor:pointer;font-size:13px;hover:background:#f5f5f0">
            <input type="checkbox" class="feed-ch-cb" value="${idx}" ${selectedIds&&selectedIds.includes(idx)?'checked':''} style="width:14px;height:14px">
            <span>${ch.name} <span style="color:#aaa;font-size:11px">(${ch.type})</span></span>
        </label>
    `).join('');
}

function getSelectedFeedChannels(){
    return Array.from(document.querySelectorAll('.feed-ch-cb:checked')).map(el=>parseInt(el.value));
}

function openEditFeed(i){
    editFeedIndex=i;const f=feeds[i];
    document.getElementById('new-name').value=f.name;
    document.getElementById('new-url').value=f.url;
    document.getElementById('new-category').value=f.category||'Khác';
    document.getElementById('new-show-link').checked=f.show_link!==false;
    document.getElementById('new-auto-fwd').checked=f.auto_fwd===true;
    document.getElementById('modal-title').textContent='Sửa feed';
    document.getElementById('btn-add-feed').textContent='Cập nhật';
    const hint=document.getElementById('feed-type-hint');
    hint.style.display=isTgSource(f.url)?'block':'none';
    renderFeedChannelList(f.target_channels||[]);
    document.getElementById('modal').classList.add('open');
}

function openModal(){
    editFeedIndex=-1;
    document.getElementById('new-name').value='';
    document.getElementById('new-url').value='';
    document.getElementById('new-category').value=categories[0]||'Khác';
    document.getElementById('new-show-link').checked=true;
    document.getElementById('new-auto-fwd').checked=false;
    document.getElementById('modal-title').textContent='Thêm feed';
    document.getElementById('btn-add-feed').textContent='Thêm';
    document.getElementById('feed-type-hint').style.display='none';
    renderFeedChannelList([]);
    document.getElementById('modal').classList.add('open');
}
function closeModal(){document.getElementById('modal').classList.remove('open');}

function addFeed(){
    const name=document.getElementById('new-name').value.trim(),
          url=document.getElementById('new-url').value.trim(),
          cat=document.getElementById('new-category').value,
          show_link=document.getElementById('new-show-link').checked,
          auto_fwd=document.getElementById('new-auto-fwd').checked,
          target_channels=getSelectedFeedChannels();
    if(!name||!url){alert('Nhập đủ tên và URL');return;}
    const feedObj={name,url,category:cat,show_link,auto_fwd,target_channels};
    if(editFeedIndex>=0){feeds[editFeedIndex]=feedObj;}
    else{feeds.push(feedObj);}
    saveFeeds();
    wsSend({type:'feeds',feeds:feeds.map(f=>({url:f.url,name:f.name,category:f.category,show_link:f.show_link!==false,auto_fwd:f.auto_fwd===true,target_channels:f.target_channels||[]}))});
    closeModal();
    if(editFeedIndex<0) fetchAndMerge(url,name,cat,true);
    else{allItems=allItems.filter(it=>it.feedUrl!==url);fetchAndMerge(url,name,cat,false);}
    renderSidebar();renderStream();
}

function applyFilter(){searchQ=document.getElementById('search').value.trim().toLowerCase();shownCount=PAGE_SIZE;renderStream();}
function getVisible(){
    let items=filterUrl?allItems.filter(it=>it.feedUrl===filterUrl):[...allItems];
    if(searchQ) items=items.filter(it=>(it.title+it.desc).toLowerCase().includes(searchQ));
    return items;
}

function renderStream(){
    const visible=getVisible(),stream=document.getElementById('stream');
    document.getElementById('item-count').textContent=visible.length+' bài';
    if(!visible.length){stream.innerHTML='<div style="color:#aaa;padding:3rem;text-align:center">Không có bài</div>';return;}
    stream.innerHTML=visible.slice(0,shownCount).map((it,i)=>itemHTML(it,i)).join('');
}

function itemHTML(it,i){
    const isSel=selected.has(it.guid),guid=it.guid||('tmp_'+i+'_'+Date.now());
    const titleText=it.title||(it.category?`(${it.category})`:'(không tiêu đề)');
    const safeGuid=String(guid).replace(/"/g,'&quot;').replace(/'/g,'&#39;');
    const sourceBadge=isTgSource(it.feedUrl||'')?'<span class="feed-badge badge-tg">TG</span>':'';
    return `<div class="item${isSel?' selected':''}" data-guid="${safeGuid}">
        <div style="display:flex;gap:8px;padding:.85rem">
            <input type="checkbox"${isSel?' checked':''} onchange="toggleSelect('${safeGuid}',this.checked)">
            <div style="flex:1">
                <div style="font-weight:600;font-size:.88rem;color:#1a1a1a">${titleText}${sourceBadge}</div>
                <div style="font-size:11px;color:#aaa;margin-top:3px">${it.feedName||''}</div>
            </div>
            <span onclick="toggleBody(${i})" style="cursor:pointer;color:#ccc">+</span>
        </div>
        <div id="b${i}" style="display:none;padding:0 1.1rem .9rem;border-top:1px solid #f4f4f0;line-height:1.6">${it.desc||''}</div>
        ${it.link?`<div class="item-footer"><a href="${it.link}" target="_blank">Xem bài gốc →</a></div>`:''}
    </div>`;
}

function toggleBody(i){
    const b=document.getElementById('b'+i);
    if(b) b.style.display=b.style.display==='none'?'block':'none';
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
        let endpoint,items;
        if(isTgSource(url)){
            const r=await fetch('/tl_fetch?url='+encodeURIComponent(url)+'&translate='+(translateOn?'1':'0')+'&category='+encodeURIComponent(category));
            if(!r.ok) throw new Error('HTTP '+r.status);
            const data=await r.json();
            items=data.items||[];
        } else {
            const r=await fetch('/fetch?url='+encodeURIComponent(url)+'&translate='+(translateOn?'1':'0')+'&category='+encodeURIComponent(category));
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
        allItems.sort((a,b)=>b.ts-a.ts);renderStream();
    }catch(e){console.warn('fetch error',url,e);}
}

async function manualRefreshAll(){
    await Promise.all(feeds.map(f=>fetchAndMerge(f.url,f.name,f.category,false)));
}

// --- Poll timer ---
// pollNextIn được server reset khi có i-c-a feeds, đếm ngược mỗi giây
let timerRunning = false;
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
    ws.onopen=()=>{
        wsReady=true;document.getElementById('ws-dot').className='ws-dot on';
        document.getElementById('ws-lbl').textContent='Đang theo dõi';
        wsSend({type:'feeds',feeds:feeds.map(f=>({url:f.url,name:f.name,category:f.category,show_link:f.show_link!==false,auto_fwd:f.auto_fwd===true,target_channels:f.target_channels||[]}))});
        wsSend({type:'tg_settings',channels:tgChannels});
        wsSend({type:'auto_fwd',enabled:autoFwd,channels:tgChannels});
        wsSend({type:'categories',categories});
        wsSend({type:'translate_engine',engine:translateEngine});
        // Khi reconnect: chỉ fetch lại RSS feeds (nhanh), TG feeds sẽ nhận qua WS broadcast
        if(wsReconnectCount>0){
            setTimeout(()=>{
                feeds.filter(f=>!isTgSource(f.url)).forEach(f=>fetchAndMerge(f.url,f.name,f.category,false));
            }, 1000);
        }
        wsReconnectCount++;
        // Heartbeat từ client mỗi 10s để tránh Render LB cắt connection
        if(wsHeartbeatTimer) clearInterval(wsHeartbeatTimer);
        wsHeartbeatTimer=setInterval(()=>{
            if(wsReady) wsSend({type:'heartbeat'});
        }, 10000);
    };
    ws.onmessage=e=>{
        const msg=JSON.parse(e.data);
        if(msg.type==='heartbeat') return;
        if(msg.type==='new_items'){
            const feedName=feeds.find(f=>f.url===msg.url)?.name||msg.url;
            const parsed=msg.items.map(it=>({...it,feedUrl:msg.url,feedName,ts:it.pubDate?new Date(it.pubDate).getTime():0,isNew:true}));
            if(filterUrl!==null&&filterUrl!==msg.url) newBadges[msg.url]=(newBadges[msg.url]||0)+parsed.length;
            const existing=new Set(allItems.map(it=>it.guid));
            parsed.forEach(it=>{if(!existing.has(it.guid)) allItems.push(it);});
            allItems.sort((a,b)=>b.ts-a.ts);renderSidebar();renderStream();
        }
        if(msg.type==='poll_status'){
            pollInterval=msg.interval;
            pollNextIn=msg.next_in;
        }
        if(msg.type==='auto_fwd_sent'){
            showToastMsg(`Auto-forward: đã gửi ${msg.count} tin mới lên Telegram`);
        }
    };
    ws.onclose=()=>{
        wsReady=false;
        if(wsHeartbeatTimer){clearInterval(wsHeartbeatTimer);wsHeartbeatTimer=null;}
        document.getElementById('ws-dot').className='ws-dot wait';
        document.getElementById('ws-lbl').textContent='Mất kết nối...';
        setTimeout(connectWS,3000);
    };
}
function wsSend(obj){if(ws&&wsReady)ws.send(JSON.stringify(obj));}

function showToastMsg(msg){
    const t=document.getElementById('toast');
    const el=document.createElement('div');
    el.className='toast-item';el.textContent=msg;t.appendChild(el);
    setTimeout(()=>el.remove(),4000);
}

(function(){
    renderCategoryList();
    renderSidebar();
    checkTelethonStatus();
    // Khôi phục engine đã chọn từ localStorage
    const sel=document.getElementById('engine-select');
    if(sel) sel.value=translateEngine;
    connectWS();
    feeds.forEach(f=>fetchAndMerge(f.url,f.name,f.category,false));
})();
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
    return urllib.request.urlopen(req, timeout=10).read()

def parse_items(xml_bytes, category_hint=''):
    root = ET.fromstring(xml_bytes)
    is_atom = 'feed' in root.tag
    ns = '{http://www.w3.org/2005/Atom}'
    entries = (root.findall(f'.//{ns}entry') or root.findall('.//entry')) if is_atom else root.findall('.//item')
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
        items.append({'guid': guid, 'title': title, 'link': link, 'pubDate': pub, 'desc': desc, 'translated': False, 'category': category_hint})
    return items

def process_items(items):
    if not translate_enabled or not TRANSLATE_AVAILABLE:
        return items
    results = [None] * len(items)
    def do(i, it):
        t, d, was = maybe_translate(it['title'], it['desc'])
        results[i] = {**it, 'title': t, 'desc': d, 'translated': was}
    threads = [threading.Thread(target=do, args=(i, it)) for i, it in enumerate(items)]
    for t in threads: t.start()
    for t in threads: t.join()
    return results

def process_tg_items(items):
    """Dịch items từ Telethon — dịch desc trước, sau đó derive title từ desc đã dịch"""
    if not translate_enabled or not TRANSLATE_AVAILABLE:
        return items
    results = [None] * len(items)
    def do(i, it):
        # Với Telethon, title chỉ là 80 ký tự đầu của desc
        # → dịch desc trước, rồi re-derive title từ desc đã dịch
        desc_plain = strip_html(it.get('desc', '').replace('<br>', '\n'))
        title_orig = it.get('title', '')
        desc_orig  = it.get('desc', '')

        if is_vietnamese(desc_plain):
            results[i] = {**it, 'translated': False}
            return

        # Dịch toàn bộ text gốc (plain text, không có HTML)
        translated_desc_plain = _translate_with_engine(desc_plain[:4000]) or desc_plain

        # Re-derive title từ bản dịch
        title_translated = (translated_desc_plain[:80] + '...') if len(translated_desc_plain) > 80 else translated_desc_plain
        # Convert desc plain đã dịch về dạng HTML với <br>
        desc_translated = translated_desc_plain.replace('\n', '<br>')

        results[i] = {**it, 'title': title_translated, 'desc': desc_translated, 'translated': True}

    threads = [threading.Thread(target=do, args=(i, it)) for i, it in enumerate(items)]
    for t in threads: t.start()
    for t in threads: t.join()
    return results

# Semaphore để serialize Telethon calls — tránh conflict khi nhiều kênh poll cùng lúc
tg_semaphore = threading.Semaphore(1)

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


async def _tg_send_item(dest_channel, item, caption):
    """
    Gửi 1 item lên kênh đích qua Telethon (không dùng Bot API).

    Logic:
    - Nếu là tin TG nguồn có media → dùng msg.media object gốc (forward không download)
      * grouped media (album) → gom tất cả msg trong group, send_file với list media
      * single media → send_file(media)
    - Nếu là tin RSS có media URL → send_file(url) — Telegram tự fetch
    - Nếu không có media → send_message(text)

    Không bao giờ download bytes về RAM.
    """
    if not tg_client:
        return False
    try:
        source   = item.get('_source', '')
        msg_id   = item.get('_tg_msg_id')
        chat     = item.get('_tg_chat')
        grouped  = item.get('_tg_grouped_id')
        has_media = item.get('_tg_has_media', False)
        rss_media = item.get('_rss_media_url')   # URL ảnh/video từ RSS

        if source == 'telethon' and has_media and msg_id and chat:
            # --- TG nguồn có media: dùng msg.media object trực tiếp ---
            with tg_semaphore:
                if grouped:
                    # Album: lấy các msg trong group (range quét)
                    all_msgs = await tg_client.get_messages(chat, ids=list(range(msg_id, msg_id + 10)))
                    if not isinstance(all_msgs, list):
                        all_msgs = [all_msgs] if all_msgs else []
                    group_msgs = [m for m in all_msgs
                                  if m and m.grouped_id == grouped and m.media]
                    if not group_msgs:
                        # fallback: chỉ lấy msg chính
                        single = await tg_client.get_messages(chat, ids=msg_id)
                        group_msgs = [single] if single and single.media else []
                else:
                    msgs = await tg_client.get_messages(chat, ids=msg_id)
                    msg  = msgs if not isinstance(msgs, list) else (msgs[0] if msgs else None)
                    group_msgs = [msg] if msg and msg.media else []

            if group_msgs:
                media_list = [m.media for m in group_msgs[:10]]
                if len(media_list) == 1:
                    await tg_client.send_file(dest_channel, media_list[0], caption=caption, parse_mode='html')
                else:
                    # Album: caption chỉ gắn vào media đầu tiên
                    await tg_client.send_file(dest_channel, media_list,
                                              caption=caption, parse_mode='html')
                return True
            # Không lấy được media → fallback text
            await tg_client.send_message(dest_channel, caption, parse_mode='html',
                                         link_preview=False)
            return True

        elif rss_media:
            # --- RSS có URL media: để Telegram tự fetch ---
            await tg_client.send_file(dest_channel, rss_media,
                                      caption=caption, parse_mode='html')
            return True

        else:
            # --- Chỉ text ---
            # Chia nhỏ nếu dài hơn 4096 ký tự
            chunks = _split_text(caption, 4096)
            for chunk in chunks:
                await tg_client.send_message(dest_channel, chunk, parse_mode='html',
                                             link_preview=False)
            return True

    except Exception as e:
        print(f'[TG Send] Lỗi gửi {dest_channel}: {e}')
        return False


def _do_forward(processed, category, url):
    """
    Gửi tin lên các kênh đích qua Telethon (thay thế Bot API hoàn toàn).
    Routing: master → tất cả, category/group → chỉ kênh khớp category.
    """
    with lock:
        do_fwd   = auto_fwd_enabled
        cfgs     = list(tg_channels)
        feed_cfg = next((u for u in watched_urls if u['url'] == url), {})
    if not do_fwd or not cfgs:
        return
    if not feed_cfg.get('auto_fwd', True):
        return
    if not TELETHON_AVAILABLE or tg_client is None:
        print('[Forward] Telethon chưa kết nối — bỏ qua forward')
        return

    target_channels = feed_cfg.get('target_channels', [])
    total_sent = 0

    for ch_idx, ch in enumerate(cfgs):
        if target_channels and ch_idx not in target_channels:
            continue
        should_send = False
        if ch['type'] == 'master':
            should_send = True
        elif ch['type'] in ['category', 'group']:
            if ch.get('category_filter') == category:
                should_send = True
        if not should_send:
            continue

        # dest_channel: dùng field 'username' (dạng @channel hoặc -100xxx)
        dest = ch.get('username') or ch.get('channel_id', '')
        if not dest:
            print(f'[Forward] Kênh "{ch.get("name")}" thiếu username — bỏ qua')
            continue

        channel_name = ch.get('name', dest)
        show_link    = feed_cfg.get('show_link', True)

        for it in reversed(processed):  # gửi theo thứ tự cũ → mới
            # Build caption
            desc_plain = strip_html(it.get('desc', '').replace('<br>', '\n')).strip()
            caption    = desc_plain
            if show_link and it.get('link'):
                caption += f'\n\n<a href="{it["link"]}">Xem bài gốc →</a>'
            if channel_name:
                caption += f'\n\n<i>{channel_name}</i>'

            ok = tg_run(_tg_send_item(dest, it, caption))
            if ok:
                total_sent += 1
            # Delay nhỏ giữa các tin để tránh flood
            time.sleep(0.5)

    if total_sent > 0:
        broadcast({'type': 'auto_fwd_sent', 'count': total_sent, 'url': url})

def _poll_one(url_obj):
    """
    Poll một feed, trả về True nếu thành công.
    Kiến trúc mới:
    - Lấy items từ nguồn (RSS XML hoặc Telethon history poll)
    - Dedup bằng known_guids
    - Đẩy NEW items vào BatchPipeline → AI rotate → broadcast → forward
    Lưu ý: Telethon real-time đã xử lý qua _process_tg_queue/pipeline riêng.
    _poll_one chỉ dùng cho: RSS thường + tg.i-c-a.su (không có real-time listener).
    """
    url       = url_obj['url']
    category  = url_obj.get('category', '')
    show_link = url_obj.get('show_link', True)
    try:
        if is_tg_source(url):
            if not TELETHON_AVAILABLE or tg_client is None:
                return False
            channel = normalize_tg_channel(url)
            items = tg_fetch_channel(channel, limit=20)
            for it in items:
                if not it.get('category'):
                    it['category'] = category
        else:
            xml   = fetch_feed(url)
            items = parse_items(xml, category)

        with lock:
            prev = known_guids.get(url)
        if prev is None:
            # Lần đầu — chỉ init known_guids, không broadcast (tránh spam items cũ)
            with lock:
                known_guids[url] = {it['guid'] for it in items if it['guid']}
        else:
            new_items = [it for it in items if it['guid'] and it['guid'] not in prev]
            if new_items:
                # Cập nhật known_guids ngay để không xử lý lại
                with lock:
                    known_guids[url] = {it['guid'] for it in items if it['guid']}
                print(f'[+] {len(new_items)} bài mới → pipeline: {url}')
                # Đẩy vào BatchPipeline thay vì xử lý sync
                for it in new_items:
                    it['show_link'] = show_link
                    it['feed_url']  = url
                    raw_text = it.get('title', '') + '\n' + strip_html(it.get('desc', ''))
                    it['text']    = raw_text.strip()
                    it['_source'] = 'rss'
                    # Trích URL media từ desc để Telethon send_file (không download)
                    imgs, videos = extract_media(it.get('desc', ''))
                    if videos:
                        v = videos[0].lower()
                        it['_rss_media_url'] = videos[0] if any(v.endswith(e) for e in ('.mp4','.webm','.mov','.mkv')) else None
                    elif imgs:
                        it['_rss_media_url'] = imgs[0]
                    else:
                        it['_rss_media_url'] = None
                    _pipeline.put(it)
        return True
    except Exception as ex:
        print(f'[!] Poll lỗi {url}: {ex}')
        return False

def _is_ica_source(url):
    return 'tg.i-c-a.su' in url

def _init_tg_feed(url_obj):
    """
    Load lịch sử 1 lần khi thêm kênh TG mới.
    Chỉ dùng để hiển thị trên UI (không forward).
    Dịch bằng process_tg_items (sync) vì đây là init one-shot, không cần pipeline.
    """
    url      = url_obj['url']
    category = url_obj.get('category', '')
    channel  = normalize_tg_channel(url)
    try:
        items = tg_load_history_sync(channel, limit=20)
        for it in items:
            if not it.get('category'):
                it['category'] = category
        with lock:
            known_guids[url] = {it['guid'] for it in items if it['guid']}
        # Dịch để hiển thị UI (không forward — chỉ lịch sử)
        if translate_enabled and TRANSLATE_AVAILABLE:
            items = process_tg_items(items)
        ws_items = [{k: v for k, v in it.items() if k != '_tg_media_bytes'} for it in items]
        if ws_items:
            broadcast({'type': 'new_items', 'url': url, 'items': ws_items})
        print(f'[TG] Load lịch sử {channel}: {len(items)} tin')
    except Exception as e:
        print(f'[!] Load lịch sử lỗi {channel}: {e}')

def _process_tg_queue():
    """
    Xử lý tin mới từ Telethon real-time queue.
    Kiến trúc mới: dedup → đẩy vào BatchPipeline (async, non-blocking).
    Pipeline sẽ gom batch, gọi AIService rotation, broadcast + forward.
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
        category  = ''
        show_link = True
        with lock:
            for u in watched_urls:
                if u['url'] == feed_url:
                    category  = u.get('category', '')
                    show_link = u.get('show_link', True)
                    break
            prev = known_guids.get(feed_url)

        # Dedup — lọc trùng trước khi vào pipeline
        new_items = [it for it in items if not prev or it['guid'] not in prev]
        if not new_items:
            continue

        # Cập nhật known_guids ngay để poller không xử lý lại
        with lock:
            if known_guids.get(feed_url) is None:
                known_guids[feed_url] = set()
            for it in new_items:
                known_guids[feed_url].add(it['guid'])

        print(f'[+] {len(new_items)} bài mới (real-time → pipeline): {feed_url}')

        # Đẩy vào BatchPipeline thay vì xử lý sync
        for it in new_items:
            pipeline_item = {
                'text': strip_html(it.get('desc', '').replace('<br>', '\n')) or it.get('title', ''),
                'feed_url': feed_url,
                'category': category,
                'show_link': show_link,
                'guid': it.get('guid', ''),
                'link': it.get('link', ''),
                'pubDate': it.get('pubDate', ''),
                '_tg_has_media': it.get('_tg_has_media', False),
                '_tg_msg_id': it.get('_tg_msg_id'),
                '_tg_chat': it.get('_tg_chat'),
                '_tg_grouped_id': it.get('_tg_grouped_id'),
                '_source': 'telethon',
            }
            _pipeline.put(pipeline_item)

def poller():
    """
    Poller:
    - Telethon: event-driven (real-time), poller chỉ xử lý queue + init history
    - RSS thường: parallel threads, interval 30s
    - tg.i-c-a.su: sequential, interval POLL_INTERVAL (60s)
    """
    global poll_next_time
    FAST_INTERVAL   = 30
    fast_next: dict = {}
    tg_inited: set  = set()   # các TG feed đã được load history

    while True:
        now = time.time()
        with lock:
            urls = list(watched_urls)

        # --- Xử lý queue real-time từ Telethon ---
        _process_tg_queue()

        # --- Init history cho TG feed mới ---
        tg_urls = [u for u in urls if is_tg_source(u['url'])]
        new_tg = [u for u in tg_urls if u['url'] not in tg_inited and TELETHON_AVAILABLE and tg_client is not None]
        # Khởi tạo từng feed tuần tự với delay — tránh flood API khi có nhiều feed
        for url_obj in new_tg:
            tg_inited.add(url_obj['url'])
            threading.Thread(target=_init_tg_feed, args=(url_obj,), daemon=True).start()
            time.sleep(1.0)   # delay 1s/feed — 45 feed = 45s khởi động, chấp nhận được

        # --- RSS thường (parallel) ---
        rss_urls = [u for u in urls if not is_tg_source(u['url']) and not _is_ica_source(u['url'])]
        rss_due  = [u for u in rss_urls if now >= fast_next.get(u['url'], 0)]
        if rss_due:
            threads = [threading.Thread(target=_poll_one, args=(u,), daemon=True) for u in rss_due]
            for t in threads: t.start()
            for t in threads: t.join(timeout=20)
            for u in rss_due:
                fast_next[u['url']] = time.time() + FAST_INTERVAL

        # --- tg.i-c-a.su (interval 60s) ---
        if now >= poll_next_time:
            ica_urls = [u for u in urls if _is_ica_source(u['url'])]
            for url_obj in ica_urls:
                _poll_one(url_obj)
            poll_next_time = time.time() + POLL_INTERVAL
            broadcast({'type': 'poll_status', 'interval': POLL_INTERVAL, 'next_in': POLL_INTERVAL})

        time.sleep(0.5)   # check queue thường xuyên hơn

def ws_handler(ws):
    global translate_enabled, auto_fwd_enabled, tg_channels, categories, translate_engine
    with lock:
        ws_clients.add(ws)
    print(f'[WS] Client kết nối, tổng={len(ws_clients)}')
    msg_count = 0

    # Heartbeat thread: gửi JSON mỗi 15s để giữ connection sống qua Render LB
    def keepalive():
        """Gửi heartbeat JSON mỗi 15s để giữ connection qua Render LB"""
        while True:
            time.sleep(15)  # Render LB timeout ~55s → gửi mỗi 15s là an toàn
            if getattr(ws, '_closed', False):
                break
            try:
                # Gửi JSON qua DATA frame (0x81) thay vì PING frame (0x89)
                hb = json.dumps({'type': 'heartbeat', 'ts': time.time()}, ensure_ascii=False)
                ws.send(hb)  # Dùng WsConn.send() đã có sẵn
            except Exception:
                break  # Thoát thread khi connection mất
    threading.Thread(target=keepalive, daemon=True).start()  # ← đúng chỗ: ngoài hàm

    try:
        for raw in ws:
            msg_count += 1
            try:
                msg = json.loads(raw)
            except Exception:
                continue

            t = msg.get('type')
            if t == 'heartbeat':
                continue  # Bỏ qua heartbeat client gửi lên (nếu có)

            if t == 'feeds':
                urls = msg.get('feeds', [])
                tg_count = sum(1 for u in urls if is_tg_source(u['url']))
                print(f'[WS] feeds: {len(urls)} feeds ({tg_count} TG)')
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
            elif t == 'translate_engine':
                new_engine = msg.get('engine', 'gemini')
                if new_engine in ('gemini', 'deepl', 'google'):
                    translate_engine = new_engine
                    print(f'[WS] Engine dịch: {translate_engine}')
            elif t == 'tg_settings':
                with lock:
                    tg_channels = msg.get('channels', [])
            elif t == 'auto_fwd':
                auto_fwd_enabled = msg.get('enabled', False)
                with lock:
                    tg_channels = msg.get('channels', tg_channels)
                print(f'[WS] auto_fwd={auto_fwd_enabled}, channels={len(tg_channels)}')
            elif t == 'categories':
                with lock:
                    categories = msg.get('categories', categories)
    except Exception as e:
        print(f'[WS] Lỗi xử lý tin nhắn: {e}')
    finally:
        # Đánh dấu đóng để stop keepalive thread
        ws._closed = True
        try:
            ws.close()
        except Exception as e:
            print(f'[WS] close error: {e}')
        with lock:
            ws_clients.discard(ws)
        print(f'[WS] Client ngắt, nhận {msg_count} messages')

def is_tg_source(url):
    return url.startswith('@') or 't.me/' in url

def normalize_tg_channel(url):
    url = url.strip()
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

        # --- WebSocket upgrade ---
        if p.path == '/ws' and self.headers.get('Upgrade', '').lower() == 'websocket':
            print(f'[WS] Upgrade request nhận được, key={self.headers.get("Sec-WebSocket-Key","?")}')
            ws = _ws_handshake(self)
            if ws:
                threading.Thread(target=ws_handler, args=(ws,), daemon=True).start()
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
                items = parse_items(xml, category)
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
            if not url:
                self.send_error(400); return
            if not TELETHON_AVAILABLE or tg_client is None:
                self._json({'error': 'Telethon chưa kết nối'}, 503); return
            try:
                channel = normalize_tg_channel(url)
                items = tg_fetch_channel(channel, limit=20)
                for it in items:
                    if not it.get('category'):
                        it['category'] = category
                if do_tl and translate_enabled and TRANSLATE_AVAILABLE:
                    items = process_tg_items(items)
                # Loại bỏ media bytes khi trả về JSON (không serialize được)
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

        if p.path == '/tl_send_code':
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
            channels  = body.get('channels', [])
            items_raw = body.get('items', [])
            if not channels:
                self._json({'error': 'missing channels'}, 400); return
            if not TELETHON_AVAILABLE or tg_client is None:
                self._json({'error': 'Telethon chưa kết nối'}, 503); return

            all_results = []
            for ch in channels:
                dest = ch.get('username') or ch.get('channel_id', '')
                if not dest:
                    all_results.append({'title': ch.get('name','?'), 'ok': False, 'error': 'Thiếu username kênh đích'})
                    continue
                channel_name = ch.get('name', dest)
                for it in items_raw:
                    feed_url = it.get('feedUrl', '')
                    with lock:
                        show_link = next((u.get('show_link', True) for u in watched_urls if u['url'] == feed_url), True)
                    # Build caption
                    desc_plain = strip_html((it.get('desc','') or '').replace('<br>','\n')).strip()
                    caption    = desc_plain
                    if show_link and it.get('link'):
                        caption += f'\n\n<a href="{it["link"]}">Xem bài gốc →</a>'
                    if channel_name:
                        caption += f'\n\n<i>{channel_name}</i>'
                    # Reconstruct item để _tg_send_item nhận diện được
                    send_item = {**it, '_source': 'telethon' if is_tg_source(feed_url) else 'rss'}
                    if is_tg_source(feed_url):
                        try:
                            link   = it.get('link','')
                            msg_id = int(link.rstrip('/').split('/')[-1])
                            chat   = normalize_tg_channel(feed_url).lstrip('@')
                            send_item['_tg_msg_id']   = msg_id
                            send_item['_tg_chat']     = chat
                            send_item['_tg_has_media'] = True  # thử gửi media, send_item sẽ fallback text nếu không có
                        except Exception:
                            send_item['_tg_has_media'] = False
                    else:
                        imgs, _ = extract_media(it.get('desc',''))
                        send_item['_rss_media_url'] = imgs[0] if imgs else None
                    try:
                        ok = tg_run(_tg_send_item(dest, send_item, caption))
                        all_results.append({'title': it.get('title',''), 'ok': ok, 'error': '' if ok else 'Gửi thất bại'})
                    except Exception as e:
                        all_results.append({'title': it.get('title',''), 'ok': False, 'error': str(e)})
                    time.sleep(0.3)  # delay nhỏ giữa các tin

            resp = json.dumps({'results': all_results}, ensure_ascii=False).encode('utf-8')
            self.send_response(200)
            self.send_header('Content-Type', 'application/json; charset=utf-8')
            self.end_headers()
            self.wfile.write(resp)

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
    # Khởi động BatchPipeline (async AI rotation + batch processing)
    _pipeline.start()
    print('[i] BatchPipeline: OK (AI rotation + batch processing)')

    # Khởi động asyncio loop cho Telethon trên thread riêng
    if TELETHON_AVAILABLE:
        tg_loop_thread = threading.Thread(target=run_tg_loop, daemon=True)
        tg_loop_thread.start()
        time.sleep(0.2)
        print('[i] Telethon loop: OK')

        session_str = os.environ.get('TG_SESSION_STRING', '').strip()

        # Đọc config từ env vars trước, fallback về file
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
                else:
                    print(f'[!] Telethon auto-login: {result.get("msg")}')
            except Exception as e:
                print(f'[!] Telethon auto-login lỗi: {e}')
        else:
            print('[i] Chưa có config — cần đăng nhập qua giao diện web')

    threading.Thread(target=poller, daemon=True).start()

    print(f'=== RSS + Telegram Reader === http://localhost:{HTTP_PORT}')
    print(f'Dịch: {"bật" if TRANSLATE_AVAILABLE else "chưa cài thư viện"}')
    print(f'Telethon: {"OK" if TELETHON_AVAILABLE else "chưa cài — pip install telethon"}')
    print(f'Poll mỗi {POLL_INTERVAL}s | Ctrl+C để dừng')
    # Chỉ mở browser khi chạy local (không phải trên cloud)
    if not os.environ.get('PORT'):
        import webbrowser
        webbrowser.open(f'http://localhost:{HTTP_PORT}')
    try:
        ThreadingHTTPServer(('', HTTP_PORT), HttpHandler).serve_forever()
    except KeyboardInterrupt:
        print('Dừng.')