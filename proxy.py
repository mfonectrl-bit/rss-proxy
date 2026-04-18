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
    Dùng Redis để persist qua restart. Nếu Redis lỗi → bỏ qua silently.
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
        return hashlib.md5(f'{guid}|{link}|{title}'.encode()).hexdigest()

    def _is_old(self, item):
        ts = item.get('timestamp')
        if not ts:
            return False
        return (time.time() - ts) > self.time_window

    def is_duplicate(self, item):
        try:
            fid   = self._make_id(item)
            title = self._clean_title(item.get('title', ''))

            # 1. Lọc tin quá cũ
            if self._is_old(item):
                return True

            # 2. Exact hash check
            if self.r.sismember(self.key_items, fid):
                return True

            # 3. Fuzzy title check (50 titles gần nhất)
            recent = self.r.lrange(self.key_titles, 0, 50)
            for t in recent:
                if self._is_similar(title, t.decode()):
                    return True

            # 4. Lưu vào Redis
            pipe = self.r.pipeline()
            pipe.sadd(self.key_items, fid)
            pipe.lpush(self.key_titles, title)
            pipe.ltrim(self.key_titles, 0, self.max_items)
            pipe.expire(self.key_items, int(self.time_window))
            pipe.expire(self.key_titles, int(self.time_window))
            pipe.execute()

            return False
        except Exception as e:
            # Redis lỗi → không block pipeline, coi như không trùng
            print(f'[Dedup] Redis lỗi (bỏ qua): {e}')
            return False


# Khởi tạo deduplicator — None nếu không có Redis
_deduplicator = None

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



# --- CẤU HÌNH ---
POLL_INTERVAL    = 60
HTTP_PORT = int(os.environ.get("PORT", 8765))
TRANSLATE_ENABLE = True
SESSION_FILE     = 'tg_session'
TG_CONFIG_FILE   = 'tg_config.json'

# Pool cho poll/fetch/setup — giới hạn 40 threads
_thread_pool = ThreadPoolExecutor(max_workers=40)
# Pool riêng cho forward — tách biệt, không bị poll/fetch chiếm chỗ
_forward_pool = ThreadPoolExecutor(max_workers=10)

# --- Engine dịch ---
# Gemini đã bị loại bỏ — 429 rate limit quá thấp (15 req/phút), không dùng được với nhiều feed
DEEPL_API_KEY  = os.environ.get('DEEPL_API_KEY', '').strip()

def _default_engine():
    if DEEPL_API_KEY:
        return 'deepl'
    return 'google'

translate_engine = _default_engine()

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
    NUM_WORKERS    = 4
    MAX_QUEUE_SIZE = 200

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
                translated = _fast_translate(text)
                if not item.get('category'):
                    item['category'] = item.get('category') or 'Khác' 
                item['text_translated'] = translated
                desc_html = translated.replace('\n', '<br>')
                title     = (translated[:80] + '...') if len(translated) > 80 else translated
                item['desc']       = desc_html
                item['title']      = title
                item['translated'] = True
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

print(f'[i] Engine dịch mặc định: {_default_engine()} | DeepL={"có" if DEEPL_API_KEY else "không"}')

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
    """Dịch text — DeepL nếu có key, Google làm fallback."""
    if not text or len(text.strip()) < 4:
        return text
    eng = engine or translate_engine
    try:
        if eng == 'deepl' and DEEPL_API_KEY:
            return _translate_deepl(text)
        else:
            return _translate_google(text)
    except Exception as e:
        err_str = str(e)
        # SSL/network lỗi thoáng qua — thử Google 1 lần, không print log dài
        if 'SSL' in err_str or 'Max retries' in err_str or 'EOF' in err_str:
            try:
                return _translate_google(text)
            except Exception:
                return text  # cả hai đều lỗi → trả text gốc, không log
        print(f'[!] Dịch [{eng}] lỗi: {e}')
        try:
            return _translate_google(text)
        except Exception:
            return text


def _translate_deepl(text):
    """Dịch bằng DeepL Free API"""
    payload = urllib.parse.urlencode({
        'auth_key': DEEPL_API_KEY,
        'text': text[:4000],
        'target_lang': 'VI',
        'source_lang': 'EN',
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
        result = GoogleTranslator(source='auto', target='vi').translate(text)
        return result or text
    except Exception as e:
        err_str = str(e)
        if 'SSL' in err_str or 'EOF' in err_str or 'Max retries' in err_str:
            raise  # ném lại để caller xử lý yên lặng
        raise

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

def _fast_translate(text):
    """
    Dịch nhanh cho pipeline — không classification, không AI phức tạp.
    Dùng engine được chọn trong UI, fallback Google nếu lỗi.
    Bỏ qua nếu text đã là tiếng Việt.
    """
    if not text or len(text.strip()) < 4:
        return text
    if is_vietnamese(text[:400]):
        return text
    return _translate_with_engine(text)

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

async def _tg_setup_realtime(feed_urls):
    """
    Setup real-time listener cho danh sách feed URLs.
    - Resolve channel mới (chưa có trong cache) tuần tự với delay nhỏ
    - Luôn đăng ký handler với TOÀN BỘ channels (cached + newly resolved)
    """
    global _tg_new_message_handler, _resolved_channels_cache

    if not tg_client or not await tg_client.is_user_authorized():
        return

    # Xóa event handlers cũ
    try:
        for callback, event in tg_client.list_event_handlers():
            tg_client.remove_event_handler(callback, event)
    except Exception:
        pass
    _tg_new_message_handler = None

    if not feed_urls:
        return

    raw_channels = [normalize_tg_channel(u).lstrip('@') for u in feed_urls]

    # Resolve các channel chưa có trong cache
    need_resolve = [ch for ch in raw_channels if ch not in _resolved_channels_cache]
    if need_resolve:
        print(f'[TG] Resolve {len(need_resolve)} channels mới...')
        for ch in need_resolve:
            try:
                await tg_client.get_input_entity(ch)
                _resolved_channels_cache.add(ch)
            except Exception as e:
                print(f'[TG] Bỏ qua @{ch}: {e}')
            await asyncio.sleep(0.5)

    # Luôn đăng ký với TẤT CẢ channels đã resolve thành công
    all_channels = [ch for ch in raw_channels if ch in _resolved_channels_cache]
    if all_channels:
        print(f'[TG] Đăng ký real-time đầy đủ: {len(all_channels)} channels')
        await _register_handler(all_channels)
    else:
        print('[TG] Không có channel nào resolve được')

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
        if not msg or not msg.message:
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
            '_tg_grouped_id': msg.grouped_id,
            '_source': 'telethon', '_feed_url': feed_url,
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

def tg_setup_realtime_sync(feed_urls):
    """Wrapper đồng bộ để gọi từ thread thường — có debounce 60s"""
    global _tg_realtime_last_setup
    now = time.time()
    if now - _tg_realtime_last_setup < 60:
        return  # bỏ qua nếu vừa setup xong trong vòng 60 giây
    _tg_realtime_last_setup = now
    if tg_loop and TELETHON_AVAILABLE:
        try:
            tg_run_long(_tg_setup_realtime(feed_urls), timeout=120)
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
    <option value="deepl">DeepL (tốt nhất)</option>
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
<div id="feed-type-hint" style="font-size:11px;color:#6366f1;margin-bottom:8px;display:none">⚡ Nguồn Telegram — sẽ dùng Telethon</div>
<!-- Checkboxes hàng ngang -->
<div style="display:flex;gap:16px;margin-bottom:10px;flex-wrap:wrap">
  <label style="display:flex;align-items:center;gap:6px;font-size:13px;cursor:pointer">
    <input type="checkbox" id="new-show-link" checked style="width:15px;height:15px;cursor:pointer">
    Hiển thị link gốc
  </label>
</div>
<div style="margin-bottom:10px">
  <label style="font-size:12px;color:#555;font-weight:600;display:block;margin-bottom:4px">Số bài lịch sử lấy về khi khởi động:</label>
  <select id="new-history-limit" style="width:100%;padding:7px;border:1px solid #d0d0c8;border-radius:8px;font-size:13px">
    <option value="20">20 bài (mặc định)</option>
    <option value="10">10 bài</option>
    <option value="5">5 bài</option>
    <option value="0">Không lấy lịch sử</option>
  </select>
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
<div class="modal">
<h2>Quản lý Kênh / Group Telegram</h2>
<div style="background:#eff6ff;border:1px solid #bfdbfe;border-radius:8px;padding:8px 12px;margin-bottom:10px;font-size:12px;color:#1e40af">
  ⚡ Dùng <b>Telethon</b> để gửi — nhập <b>@username</b> hoặc <b>-100xxx</b>. Topic IDs được cấu hình trong từng feed.
</div>
<select id="ch-select-modal" onchange="loadChannelToForm(this.value)"></select>
<input type="text" id="ch-username" placeholder="@kênh_đích hoặc -100xxxxxxxxx (Group ID)">
<input type="text" id="ch-name" placeholder="Tên hiển thị">
<label style="display:flex;align-items:center;gap:8px;font-size:13px;cursor:pointer;margin-bottom:10px">
  <input type="checkbox" id="ch-is-group" style="width:15px;height:15px;cursor:pointer">
  Là Group/Supergroup (có Topic IDs)
</label>
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

<script>
const WS_URL=(location.protocol==='https:'?'wss://':'ws://')+location.host+'/ws';
const DEFAULT_FEEDS=[{name:'VN Wall Street',url:'https://tg.i-c-a.su/rss/vnwallstreet'}];
const PAGE_SIZE = 30;

let feeds=JSON.parse(localStorage.getItem('rss_feeds')||'null')||DEFAULT_FEEDS;
let tgChannels=JSON.parse(localStorage.getItem('tg_channels')||'null')||[];
let allItems=[],newBadges={},filterUrl=null,searchQ='',ws=null,wsReady=false,shownCount=PAGE_SIZE;
let translateOn=JSON.parse(localStorage.getItem('translate_on')??'true');
let translateEngine=localStorage.getItem('translate_engine')||'deepl';
let selected=new Set(),autoFwd=JSON.parse(localStorage.getItem('auto_fwd')??'false');
let editFeedIndex=-1,selectedChannelIndex=-1;
let pollInterval=60,pollNextIn=0;
let telethonConnected=false;
let feedsSynced=false;  // global — không reset khi WS reconnect
let settingsSent=false; // global — chỉ gửi WS settings 1 lần

function saveFeeds(){localStorage.setItem('rss_feeds',JSON.stringify(feeds));}
function saveTgChannels(){localStorage.setItem('tg_channels',JSON.stringify(tgChannels));}

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
});

// --- Render destination list inside feed modal ---
// destinations = [{ch_idx, topic_ids_str}] — lưu trong feed.destinations
function renderFeedDestList(savedDests){
    const wrap=document.getElementById('feed-dest-wrap');
    const list=document.getElementById('feed-dest-list');
    if(!tgChannels.length){wrap.style.display='none';return;}
    wrap.style.display='block';
    // savedDests là array {ch_idx, topic_ids} hoặc legacy array of numbers
    const destMap={};
    if(savedDests){
        savedDests.forEach(d=>{
            if(typeof d==='number') destMap[d]={checked:true,topic_ids:''};
            else destMap[d.ch_idx]={checked:true,topic_ids:(d.topic_ids||'')};
        });
    }
    list.innerHTML=tgChannels.map((ch,idx)=>{
        const d=destMap[idx]||{checked:false,topic_ids:''};
        const isGroup=ch.is_group;
        return `<div style="padding:7px 10px;border-bottom:1px solid #f0f0e8">
            <label style="display:flex;align-items:center;gap:8px;cursor:pointer;font-size:13px">
                <input type="checkbox" class="feed-dest-cb" data-idx="${idx}" ${d.checked?'checked':''} style="width:15px;height:15px">
                <span style="font-weight:500">${ch.name}</span>
                <span style="color:#aaa;font-size:11px">${isGroup?'Group':'Channel'} · ${ch.username}</span>
            </label>
            ${isGroup?`<div style="margin-top:5px;padding-left:23px;display:flex;align-items:center;gap:6px">
                <span style="font-size:11px;color:#555;white-space:nowrap">Topic IDs:</span>
                <input type="text" class="feed-dest-topics" data-idx="${idx}" value="${d.topic_ids||''}"
                    placeholder="VD: 123, 456 — để trống = gửi vào group (không topic)"
                    style="flex:1;padding:4px 7px;border:1px solid #d0d0c8;border-radius:6px;font-size:12px">
            </div>`:''}
        </div>`;
    }).join('');
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

function loadChannelToForm(idx){
    selectedChannelIndex=parseInt(idx);
    if(selectedChannelIndex>=0&&tgChannels[selectedChannelIndex]){
        const ch=tgChannels[selectedChannelIndex];
        document.getElementById('ch-username').value=ch.username||'';
        document.getElementById('ch-name').value=ch.name||'';
        document.getElementById('ch-is-group').checked=ch.is_group===true;
        document.getElementById('btn-ch-del').style.display='inline-block';
    } else {
        document.getElementById('ch-username').value='';
        document.getElementById('ch-name').value='';
        document.getElementById('ch-is-group').checked=false;
        document.getElementById('btn-ch-del').style.display='none';
    }
}

function saveTgSettings(){
    const username=document.getElementById('ch-username').value.trim();
    const name=document.getElementById('ch-name').value.trim()||username;
    if(!username){alert('Nhập @username hoặc channel ID');return;}
    const normUsername=(!username.startsWith('@')&&!username.startsWith('-')&&!/^\d/.test(username))?'@'+username:username;
    // Dùng checkbox thay vì auto-detect — cho phép @username cũng là group
    const is_group=document.getElementById('ch-is-group').checked;
    const chObj={username:normUsername,name,is_group};
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
    list.innerHTML=tgChannels.map((ch,idx)=>{
        const pre=preDestMap[idx]||{checked:false,topic_ids:''};
        return `<div style="padding:7px 10px;border-bottom:1px solid #f0f0e8">
            <label style="display:flex;align-items:center;gap:8px;cursor:pointer;font-size:13px">
                <input type="checkbox" class="fwd-ch-cb" data-idx="${idx}" ${pre.checked?'checked':''} style="width:15px;height:15px">
                <span style="font-weight:500">${ch.name}</span>
                <span style="color:#aaa;font-size:11px">${ch.is_group?'Group':'Channel'} · ${ch.username}</span>
            </label>
            ${ch.is_group?`<div style="margin-top:5px;padding-left:23px;display:flex;align-items:center;gap:6px">
                <span style="font-size:11px;color:#555;white-space:nowrap">Topic IDs:</span>
                <input type="text" class="fwd-topic-inp" data-idx="${idx}" value="${pre.topic_ids||''}"
                    placeholder="VD: 123, 456 — để trống = không topic"
                    style="flex:1;padding:4px 7px;border:1px solid #d0d0c8;border-radius:6px;font-size:12px">
            </div>`:''}
        </div>`;
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
    document.getElementById('new-show-link').checked=true;
    document.getElementById('new-auto-fwd').checked=false;
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
          history_limit=parseInt(document.getElementById('new-history-limit').value)||20,
          destinations=getFeedDests();
    if(!name||!url){alert('Nhập đủ tên và URL');return;}
    const feedObj={name,url,show_link,auto_fwd,destinations,history_limit};
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
        allItems.sort((a,b)=>b.ts-a.ts);
        // Giới hạn 50 bài/feed
        const MAX_PER_FEED=50;const fc={};
        allItems=allItems.filter(it=>{const u=it.feedUrl||'';fc[u]=(fc[u]||0)+1;return fc[u]<=MAX_PER_FEED;});
        renderStream();
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
        }
        // Gửi feeds qua HTTP chỉ 1 lần — không lặp lại mỗi khi WS reconnect
        if(feedsSynced) return;
        fetch('/sync_feeds', {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({
                feeds: feedsPayload,
                auto_fwd: autoFwd,
                tg_channels: tgChannels,
                translate_engine: translateEngine
            })
        }).then(r=>r.json()).then(d=>{
            if(d.ok){
                feedsSynced=true;
                feedsAckReceived=true;
                if(feedsAckTimer){clearTimeout(feedsAckTimer);feedsAckTimer=null;}
                document.getElementById('ws-lbl').textContent='Đang theo dõi';
                console.log('[HTTP] sync_feeds OK, feeds='+d.count);
            }
        }).catch(e=>{
            console.warn('[HTTP] sync_feeds lỗi:',e);
            // Retry sau 5s nếu lỗi mạng
            if(feedsAckTimer) clearTimeout(feedsAckTimer);
            feedsAckTimer=setTimeout(()=>{ feedsSynced=false; _sendInitMessages(); }, 5000);
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
        if(msg.type==='auto_fwd_sent'){showToastMsg(`Auto-forward: đã gửi ${msg.count} tin mới lên Telegram`);}
    };
    ws.onclose=()=>{
        wsReady=false;
        if(wsHeartbeatTimer){clearInterval(wsHeartbeatTimer);wsHeartbeatTimer=null;}
        if(feedsAckTimer){clearTimeout(feedsAckTimer);feedsAckTimer=null;}
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
    renderSidebar();
    checkTelethonStatus();
    // Khôi phục engine đã chọn từ localStorage
    const sel=document.getElementById('engine-select');
    if(sel) sel.value=translateEngine;
    connectWS();
    // Chỉ load RSS feeds ngay lập tức
    // TG feeds: không gọi /tl_fetch lúc init vì server đang bận setup realtime listeners
    // TG feed history sẽ đến qua WS broadcast từ _init_tg_feed
    const rssFeeds=feeds.filter(f=>!isTgSource(f.url));
    rssFeeds.forEach(f=>fetchAndMerge(f.url,f.name,f.category,false));
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
    return urllib.request.urlopen(req, timeout=8).read()

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
        # link_preview chỉ bật khi desc gốc có URL — không tính t.me link trong 'Xem bài gốc'
        show_preview = desc_has_link

        # reply_to = topic_id nếu gửi vào topic của group, None nếu gửi bình thường
        thread_reply = int(topic_id) if topic_id else None

        if source == 'telethon' and has_media and msg_id and chat:
            # --- TG nguồn có media: dùng msg.media object trực tiếp ---
            from telethon.tl.types import MessageMediaWebPage
            async with tg_semaphore:
                if grouped:
                    all_msgs = await tg_client.get_messages(chat, ids=list(range(msg_id, msg_id + 10)))
                    if not isinstance(all_msgs, list):
                        all_msgs = [all_msgs] if all_msgs else []
                    group_msgs = [m for m in all_msgs
                                  if m and m.grouped_id == grouped and m.media
                                  and not isinstance(m.media, MessageMediaWebPage)]
                    if not group_msgs:
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
                if len(caption) <= 4096:
                    # Caption trong giới hạn Telegram (4096 ký tự) — gửi 1 tin gồm media + caption
                    if len(media_list) == 1:
                        await tg_client.send_file(dest, media_list[0], caption=caption,
                                                  parse_mode='html', reply_to=thread_reply,
                                                  link_preview=show_preview)
                    else:
                        await tg_client.send_file(dest, media_list, caption=caption,
                                                  parse_mode='html', reply_to=thread_reply,
                                                  link_preview=show_preview)
                else:
                    # Caption quá dài (>4096) — gửi media trống, reply text riêng
                    if len(media_list) == 1:
                        sent = await tg_client.send_file(dest, media_list[0], caption='',
                                                         parse_mode='html', reply_to=thread_reply)
                    else:
                        sent = await tg_client.send_file(dest, media_list, caption='',
                                                         parse_mode='html', reply_to=thread_reply)
                    reply_to = sent.id if not isinstance(sent, list) else sent[0].id
                    for chunk in _split_text(caption, 4096):
                        await tg_client.send_message(dest, chunk, parse_mode='html',
                                                     reply_to=reply_to, link_preview=show_preview)
                return True
            # Không lấy được media → fallback text
            for chunk in _split_text(caption, 4096):
                await tg_client.send_message(dest, chunk, parse_mode='html',
                                             reply_to=thread_reply, link_preview=show_preview)
            return True

        elif rss_media:
            if len(caption) <= 4096:
                # Caption trong giới hạn Telegram (4096 ký tự) — gửi 1 tin gồm media + caption
                await tg_client.send_file(dest, rss_media, caption=caption,
                                          parse_mode='html', reply_to=thread_reply,
                                          link_preview=show_preview)
            else:
                # Caption quá dài (>4096) — gửi media trống, reply text riêng
                sent = await tg_client.send_file(dest, rss_media, caption='',
                                                  parse_mode='html', reply_to=thread_reply)
                reply_to = sent.id if not isinstance(sent, list) else sent[0].id
                for chunk in _split_text(caption, 4096):
                    await tg_client.send_message(dest, chunk, parse_mode='html',
                                                 reply_to=reply_to, link_preview=show_preview)
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
                desc_plain = strip_html(it.get('desc', '').replace('<br>', '\n')).strip()
                caption    = desc_plain
                # desc_has_link: chỉ check https link trong nội dung gốc, bỏ qua t.me
                desc_has_link = bool(re.search(r'https?://(?!t\.me)', it.get('desc', '') or ''))

                if show_link and it.get('link'):
                    caption += f'\n\n<a href="{it["link"]}">Xem bài gốc →</a>'
                if channel_name:
                    caption += f'\n\n<i>{channel_name}</i>'

                acquired = _send_semaphore.acquire(timeout=10)
                if not acquired:
                    print(f'[Forward] Bỏ qua tin — _send_semaphore timeout')
                    continue
                try:
                    ok = tg_run(_tg_send_item(dest, it, caption, topic_id=topic_id, desc_has_link=desc_has_link))
                except Exception as e:
                    print(f'[Forward] tg_run lỗi: {e}')
                    ok = False
                finally:
                    _send_semaphore.release()
                if ok:
                    total_sent += 1
                time.sleep(0.3)

    if total_sent > 0:
        broadcast({'type': 'auto_fwd_sent', 'count': total_sent, 'url': url})

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
        max_keep = limit * 2
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
            items = parse_items(xml, category)

        if not items:
            return True

        with lock:
            prev = known_guids.get(url)

        # ==================== FIX Ở ĐÂY ====================
        if prev is None or len(prev) == 0:
            # Lần đầu hoặc bị reset → init known_guids
            with lock:
                known_guids[url] = {it['guid'] for it in items if it['guid']}
            print(f'[INIT] known_guids khởi tạo lần đầu cho: {url} ({len(items)} items)')
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
            with lock:
                known_guids[url] = {it['guid'] for it in items if it['guid']}
            
            print(f'[+] {len(new_items)} bài mới → pipeline: {url}')
            
            for it in new_items:
                it['show_link'] = show_link
                it['feed_url']  = url
                raw_text = it.get('title', '') + '\n' + strip_html(it.get('desc', ''))
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

        return True

    except Exception as ex:
        print(f'[!] Poll lỗi {url}: {ex}')
        import traceback
        traceback.print_exc()
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

        # Lớp 2: Redis dedup (fuzzy title + persist qua restart)
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
    Poller background chính
    """
    global poll_next_time
    FAST_INTERVAL    = 30
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
            if TELETHON_AVAILABLE and tg_client is not None and now - _last_tg_watchdog > 600:
                _last_tg_watchdog = now
                def _watchdog():
                    global _tg_realtime_last_setup, tg_loop, tg_loop_thread
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
                            tg_urls_all = [u['url'] for u in watched_urls if is_tg_source(u.get('url', ''))]
                            if tg_urls_all:
                                tg_run_long(_tg_setup_realtime(tg_urls_all), timeout=120)
                                _tg_realtime_last_setup = time.time()
                                print(f'[TG Watchdog] ✅ Đã re-register {len(tg_urls_all)} channels')
                        else:
                            print('[TG Watchdog] ⚠️ Mất xác thực Telethon')
                    except Exception as e:
                        print(f'[TG Watchdog] Lỗi: {type(e).__name__}: {e}')
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
                print(f"[POLL] Đang chạy | Feeds: {total_feeds} | Queue: {_pipeline.qsize()} | Known GUIDs: {len(known_guids)} | TG inited: {len(tg_inited)}")

            # Dọn dẹp known_guids mỗi giờ — dùng timer thay vì % 3600 tránh bị miss
            if now - _last_cleanup >= 3600:
                _last_cleanup = now
                _cleanup_known_guids()

            time.sleep(0.5)

        except Exception as e:
            print(f"[POLL ERROR] Lỗi lớn trong poller(): {e}")
            import traceback
            traceback.print_exc()
            time.sleep(5)   # tránh cpu 100% nếu lỗi liên tục

def ws_handler(ws):
    global translate_enabled, auto_fwd_enabled, tg_channels, translate_engine
    
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
                print(f'[WS] Engine dịch: {translate_engine}')

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

        if p.path == '/healthz':
            self.send_response(200)
            self.send_header('Content-Type', 'text/plain')
            self.end_headers()
            self.wfile.write(b'OK')
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

                        desc_plain = strip_html((it.get('desc','') or '').replace('<br>','\n')).strip()
                        caption    = desc_plain
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
                        desc_has_link = bool(re.search(r'https?://(?!t\.me)', it.get('desc', '') or ''))
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
        except Exception as e:
            print(f'[!] Redis kết nối lỗi — dedup tắt: {e}')
    else:
        if not _redis_url:
            print('[i] REDIS_URL chưa cấu hình — dedup Redis tắt, dùng known_guids')
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