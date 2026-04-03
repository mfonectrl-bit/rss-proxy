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

# --- CẤU HÌNH ---
POLL_INTERVAL    = 60
HTTP_PORT = int(os.environ.get("PORT", 8765))
TRANSLATE_ENABLE = True
SESSION_FILE     = 'tg_session'
TG_CONFIG_FILE   = 'tg_config.json'

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
    key = hash(text)
    with translate_lock:
        if key in translate_cache:
            return translate_cache[key]
    try:
        parts = re.split(r'(<[^>]+>)', text)
        out, buf = [], []
        for part in parts:
            if re.match(r'<[^>]+>', part):
                if buf:
                    chunk = ' '.join(buf).strip()
                    if chunk and len(chunk) > 3:
                        try:
                            out.append(GoogleTranslator(source='auto', target='vi').translate(chunk) or chunk)
                        except:
                            out.append(chunk)
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
                try:
                    out.append(GoogleTranslator(source='auto', target='vi').translate(chunk) or chunk)
                except:
                    out.append(chunk)
            elif chunk:
                out.append(chunk)
        result = ''.join(out)
    except Exception as e:
        print(f'[!] Dịch lỗi: {e}')
        result = text
    with translate_lock:
        if len(translate_cache) > 500:
            # Xóa bớt nửa cache khi quá lớn
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

def tg_api(bot_token, method, payload):
    url = f'https://api.telegram.org/bot{bot_token}/{method}'
    data = json.dumps(payload).encode('utf-8')
    req = urllib.request.Request(url, data=data, headers={'Content-Type':'application/json'})
    try:
        resp = urllib.request.urlopen(req, timeout=15)
        return json.loads(resp.read())
    except urllib.error.HTTPError as e:
        return json.loads(e.read())
    except Exception as e:
        return {'ok': False, 'description': str(e)}

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

def tg_api_multipart(bot_token, method, fields, file_field, file_data, file_name, file_mime):
    """Gửi multipart/form-data qua Bot API"""
    boundary = b'----TGBoundary7x'
    parts = []
    for k, v in fields.items():
        parts.append(
            b'--' + boundary + b'\r\n' +
            f'Content-Disposition: form-data; name="{k}"\r\n\r\n'.encode() +
            str(v).encode('utf-8') + b'\r\n'
        )
    parts.append(
        b'--' + boundary + b'\r\n' +
        f'Content-Disposition: form-data; name="{file_field}"; filename="{file_name}"\r\n'.encode() +
        f'Content-Type: {file_mime}\r\n\r\n'.encode() +
        file_data + b'\r\n'
    )
    parts.append(b'--' + boundary + b'--\r\n')
    body = b''.join(parts)
    url = f'https://api.telegram.org/bot{bot_token}/{method}'
    req = urllib.request.Request(url, data=body,
        headers={'Content-Type': f'multipart/form-data; boundary={boundary.decode()}'})
    try:
        resp = urllib.request.urlopen(req, timeout=30)
        return json.loads(resp.read())
    except urllib.error.HTTPError as e:
        try:
            return json.loads(e.read())
        except:
            return {'ok': False, 'description': str(e)}
    except Exception as e:
        return {'ok': False, 'description': str(e)}

def send_to_telegram(bot_token, channel_id, channel_name, items, category_filter=None, channel_type='channel'):
    results = []
    for item in items:
        item_cat = item.get('category', '')
        if channel_type != 'master' and category_filter and item_cat != category_filter:
            continue

        title          = item.get('title', '').strip()
        desc           = item.get('desc', '')
        link           = item.get('link', '')
        tg_media_bytes = item.get('_tg_media_bytes')   # list of (type, bytes, mime)

        body    = html_to_telegram(desc, channel_name, link)
        caption = (body or '').strip()
        MAX_CAP  = 1024
        MAX_TEXT = 4096

        try:
            if tg_media_bytes:
                # --- Telethon media: gửi media trước, reply text nếu caption quá dài ---
                r = _send_media_then_text(bot_token, channel_id, tg_media_bytes, caption, MAX_CAP, MAX_TEXT, title, use_bytes=True)
                results.append(r)
            else:
                imgs, videos = extract_media(desc)
                if videos:
                    media_list = [('video', u) for u in videos]
                else:
                    media_list = [('photo', u) for u in imgs]

                if not media_list:
                    # Chỉ text — chia nhỏ nếu quá dài
                    chunks = _split_text(caption, MAX_TEXT)
                    sent_id = None
                    ok = True
                    for chunk in chunks:
                        payload = {'chat_id': channel_id, 'text': chunk, 'parse_mode': 'HTML', 'disable_web_page_preview': True}
                        if sent_id:
                            payload['reply_to_message_id'] = sent_id
                        resp = tg_api(bot_token, 'sendMessage', payload)
                        if resp.get('ok') and not sent_id:
                            sent_id = resp.get('result', {}).get('message_id')
                        if not resp.get('ok'):
                            ok = False
                    results.append({'title': title, 'ok': ok, 'error': '' if ok else 'Lỗi gửi'})
                else:
                    r = _send_media_then_text(bot_token, channel_id, media_list, caption, MAX_CAP, MAX_TEXT, title, use_bytes=False)
                    results.append(r)
        except Exception as e:
            results.append({'title': title, 'ok': False, 'error': str(e)})
    return results

def _split_text(text, max_len):
    """Chia text thành các chunk <= max_len, cắt ở ranh giới dòng nếu được"""
    if len(text) <= max_len:
        return [text]
    chunks = []
    while text:
        if len(text) <= max_len:
            chunks.append(text)
            break
        # Tìm điểm cắt gần nhất là dòng mới
        cut = text.rfind('\n', 0, max_len)
        if cut < max_len // 2:
            cut = max_len - 3
            chunks.append(text[:cut] + '…')
        else:
            chunks.append(text[:cut])
        text = text[cut:].lstrip('\n')
    return chunks

def _send_media_then_text(bot_token, channel_id, media_list, caption, max_cap, max_text, title, use_bytes=False):
    """
    Gửi media (1 hoặc nhiều), caption ngắn đi kèm media.
    Nếu caption quá dài: gửi media với CHỈ footer (link + tên kênh),
    sau đó reply toàn bộ nội dung đầy đủ.
    """
    if len(caption) <= max_cap:
        short_cap = caption
        need_reply = False
    else:
        # Tách footer: 2 dòng cuối (link gốc + tên kênh)
        lines = caption.split('\n')
        footer_lines = []
        for l in reversed(lines):
            if l.strip():
                footer_lines.insert(0, l)
                if len(footer_lines) >= 2:
                    break
        footer = '\n'.join(footer_lines)
        # Media chỉ kèm footer, không kèm body bị cắt
        short_cap = footer if len(footer) <= max_cap else ''
        need_reply = True

    # Gửi media
    media_msg_id = None
    ok = True
    err = ''

    if use_bytes:
        # Telethon media bytes
        for idx, (mtype, data, mime) in enumerate(media_list[:1]):  # 1 media đại diện
            method = 'sendPhoto' if mtype == 'photo' else 'sendVideo'
            field  = 'photo'    if mtype == 'photo' else 'video'
            ext    = 'jpg'      if mtype == 'photo' else 'mp4'
            resp = tg_api_multipart(bot_token, method,
                {'chat_id': channel_id, 'caption': short_cap, 'parse_mode': 'HTML'},
                field, data, f'media.{ext}', mime)
            ok = resp.get('ok', False)
            err = resp.get('description', '')
            if ok:
                media_msg_id = resp.get('result', {}).get('message_id')
        # Nếu có nhiều media, gửi tiếp các ảnh còn lại không caption
        for idx, (mtype, data, mime) in enumerate(media_list[1:10]):
            method = 'sendPhoto' if mtype == 'photo' else 'sendVideo'
            field  = 'photo'    if mtype == 'photo' else 'video'
            ext    = 'jpg'      if mtype == 'photo' else 'mp4'
            tg_api_multipart(bot_token, method,
                {'chat_id': channel_id, 'reply_to_message_id': media_msg_id} if media_msg_id else {'chat_id': channel_id},
                field, data, f'media.{ext}', mime)
    else:
        # URL media
        if len(media_list) == 1:
            mtype, murl = media_list[0]
            method = 'sendPhoto' if mtype == 'photo' else 'sendVideo'
            resp = tg_api(bot_token, method, {
                'chat_id': channel_id, mtype: murl,
                'caption': short_cap, 'parse_mode': 'HTML'
            })
            ok = resp.get('ok', False)
            err = resp.get('description', '')
            if ok:
                media_msg_id = resp.get('result', {}).get('message_id')
        else:
            media_group = []
            for idx, (mtype, murl) in enumerate(media_list[:10]):
                entry = {'type': 'photo' if mtype == 'photo' else 'video', 'media': murl}
                if idx == 0:
                    entry['caption'] = short_cap
                    entry['parse_mode'] = 'HTML'
                media_group.append(entry)
            resp = tg_api(bot_token, 'sendMediaGroup', {'chat_id': channel_id, 'media': media_group})
            ok = resp.get('ok') if isinstance(resp, dict) else bool(resp)
            err = resp.get('description', '') if isinstance(resp, dict) else ''
            if ok and isinstance(resp.get('result'), list) and resp['result']:
                media_msg_id = resp['result'][0].get('message_id')

    # Nếu caption bị cắt, reply text đầy đủ
    if ok and need_reply and media_msg_id:
        chunks = _split_text(caption, max_text)
        reply_id = media_msg_id
        for chunk in chunks:
            payload = {'chat_id': channel_id, 'text': chunk, 'parse_mode': 'HTML',
                       'disable_web_page_preview': True, 'reply_to_message_id': reply_id}
            resp2 = tg_api(bot_token, 'sendMessage', payload)
            if resp2.get('ok'):
                reply_id = resp2.get('result', {}).get('message_id', reply_id)

    return {'title': title, 'ok': ok, 'error': err}

# --- TELETHON FUNCTIONS ---

def is_tg_source(url):
    """Kiểm tra xem URL có phải nguồn Telegram không"""
    return url.startswith('@') or 't.me/' in url or url.startswith('https://t.me/')

def normalize_tg_channel(url):
    """Chuẩn hóa về dạng username, vd: @channel hoặc channel"""
    url = url.strip()
    if url.startswith('https://t.me/'):
        url = url[len('https://t.me/'):]
    elif url.startswith('t.me/'):
        url = url[len('t.me/'):]
    if not url.startswith('@'):
        url = '@' + url
    return url

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
    items = []
    for msg in msgs:
        if not msg or not msg.message:
            continue
        guid = f'tg_{channel}_{msg.id}'
        link = f'https://t.me/{channel.lstrip("@")}/{msg.id}'
        desc = msg.message.replace('\n', '<br>')
        title = (msg.message[:80] + '...') if len(msg.message) > 80 else msg.message
        pub  = msg.date.isoformat() if msg.date else ''
        # Không download media ở đây — chỉ download khi forward
        items.append({
            'guid': guid, 'title': title, 'desc': desc, 'link': link,
            'pubDate': pub, 'translated': False, 'category': '',
            '_tg_media_bytes': None, '_source': 'telethon',
        })
    return items

async def _tg_register_handlers():
    """Đăng ký event handler real-time cho tất cả TG feeds hiện tại"""
    global tg_client
    # Xóa handler cũ nếu có
    tg_client.remove_event_handler(_tg_new_message_handler)

    with lock:
        tg_feed_urls = [u['url'] for u in watched_urls if is_tg_source(u['url'])]

    if not tg_feed_urls:
        return

    channels = [normalize_tg_channel(u).lstrip('@') for u in tg_feed_urls]

    @tg_client.on(events.NewMessage(chats=channels))
    async def _tg_new_message_handler(event):
        msg = event.message
        if not msg or not msg.message:
            return
        # Tìm feed_url tương ứng
        chat_username = getattr(event.chat, 'username', None)
        if not chat_username:
            return
        feed_url = None
        with lock:
            for u in watched_urls:
                if is_tg_source(u['url']) and normalize_tg_channel(u['url']).lstrip('@').lower() == chat_username.lower():
                    feed_url = u['url']
                    category = u.get('category', '')
                    break
        if not feed_url:
            return

        guid  = f'tg_@{chat_username}_{msg.id}'
        link  = f'https://t.me/{chat_username}/{msg.id}'
        desc  = msg.message.replace('\n', '<br>')
        title = (msg.message[:80] + '...') if len(msg.message) > 80 else msg.message
        pub   = msg.date.isoformat() if msg.date else ''

        item = {
            'guid': guid, 'title': title, 'desc': desc, 'link': link,
            'pubDate': pub, 'translated': False, 'category': category,
            '_tg_media_bytes': None, '_source': 'telethon', '_feed_url': feed_url,
        }
        with tg_new_items_lock:
            tg_new_items_queue.append(item)
        print(f'[TG] Tin mới real-time: @{chat_username} #{msg.id}')

# Lưu handler để có thể remove sau
_tg_new_message_handler = None

async def _tg_setup_realtime(feed_urls):
    """Setup real-time listener cho danh sách feed URLs"""
    global _tg_new_message_handler

    if not tg_client or not await tg_client.is_user_authorized():
        return

    # Remove old handler
    if _tg_new_message_handler:
        try:
            tg_client.remove_event_handler(_tg_new_message_handler)
        except:
            pass
        _tg_new_message_handler = None

    if not feed_urls:
        return

    raw_channels = [normalize_tg_channel(u).lstrip('@') for u in feed_urls]

    # Resolve từng channel để loại bỏ username không hợp lệ/không tồn tại
    # Nếu bỏ qua bước này, một channel lỗi sẽ khiến toàn bộ handler crash
    channels_list = []
    for ch in raw_channels:
        try:
            await tg_client.get_input_entity(ch)
            channels_list.append(ch)
        except Exception as e:
            print(f'[TG] Bỏ qua channel không hợp lệ @{ch}: {e}')

    if not channels_list:
        print('[TG] Không có channel hợp lệ nào để đăng ký real-time')
        return

    print(f'[TG] Đăng ký real-time cho: {channels_list}')

    async def handler(event):
        msg = event.message
        if not msg or not msg.message:
            return
        chat_username = getattr(event.chat, 'username', None)
        if not chat_username:
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

        guid  = f'tg_@{chat_username}_{msg.id}'
        link  = f'https://t.me/{chat_username}/{msg.id}'
        desc  = msg.message.replace('\n', '<br>')
        title = (msg.message[:80] + '...') if len(msg.message) > 80 else msg.message
        pub   = msg.date.isoformat() if msg.date else ''

        item = {
            'guid': guid, 'title': title, 'desc': desc, 'link': link,
            'pubDate': pub, 'translated': False, 'category': category,
            '_tg_media_bytes': None, '_source': 'telethon', '_feed_url': feed_url,
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
            if opcode == 0x8:   # close
                print(f'[WS] Frame close nhận được (len={length})')
                return None
            if opcode == 0x9:   # ping → pong
                with self._send_lock:
                    try:
                        self._sock.sendall(bytes([0x8a, len(data)]) + bytes(data))
                    except Exception:
                        pass
                return self.recv()
            return data.decode('utf-8', errors='replace')
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
<h2 id="modal-title">Thêm feed</h2>
<input type="text" id="new-name" placeholder="Tên feed">
<input type="text" id="new-url" placeholder="URL RSS hoặc @username / t.me/channel">
<select id="new-category"></select>
<div id="feed-type-hint" style="font-size:11px;color:#6366f1;margin-bottom:8px;display:none">⚡ Nguồn Telegram — sẽ dùng Telethon</div>
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
<select id="ch-select-modal" onchange="loadChannelToForm(this.value)"></select>
<input type="text" id="ch-token" placeholder="Bot API Token">
<input type="text" id="ch-channel" placeholder="Channel ID">
<input type="text" id="ch-name" placeholder="Tên kênh">
<select id="ch-type">
<option value="master">Master (Tất cả)</option>
<option value="category">Category</option>
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
let selected=new Set(),autoFwd=JSON.parse(localStorage.getItem('auto_fwd')??'false');
let editFeedIndex=-1,selectedChannelIndex=-1;
let pollInterval=60,pollNextIn=0;
let telethonConnected=false;

function saveFeeds(){localStorage.setItem('rss_feeds',JSON.stringify(feeds));}
function saveTgChannels(){localStorage.setItem('tg_channels',JSON.stringify(tgChannels));}
function saveCategories(){localStorage.setItem('categories',JSON.stringify(categories));}

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
    const form={token:document.getElementById('ch-token'),channel:document.getElementById('ch-channel'),
        name:document.getElementById('ch-name'),type:document.getElementById('ch-type'),category:document.getElementById('ch-category')};
    if(selectedChannelIndex>=0&&tgChannels[selectedChannelIndex]){
        const ch=tgChannels[selectedChannelIndex];
        form.token.value=ch.token;form.channel.value=ch.channel_id;form.name.value=ch.name;
        form.type.value=ch.type;form.category.value=ch.category_filter||'';
        document.getElementById('btn-ch-del').style.display='inline-block';
    } else {
        form.token.value='';form.channel.value='';form.name.value='';
        form.type.value='master';form.category.value='';
        document.getElementById('btn-ch-del').style.display='none';
    }
}

function saveTgSettings(){
    const form={token:document.getElementById('ch-token').value.trim(),channel:document.getElementById('ch-channel').value.trim(),
        name:document.getElementById('ch-name').value.trim(),type:document.getElementById('ch-type').value,
        category:document.getElementById('ch-category').value};
    if(!form.token||!form.channel){alert('Thiếu Token hoặc Channel ID');return;}
    if(selectedChannelIndex>=0){
        tgChannels[selectedChannelIndex]={token:form.token,channel_id:form.channel,name:form.name,type:form.type,category_filter:form.category};
    } else {
        tgChannels.push({token:form.token,channel_id:form.channel,name:form.name,type:form.type,category_filter:form.category});
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
    wsSend({type:'feeds',feeds:feeds.map(f=>({url:f.url,name:f.name,category:f.category}))});
    renderSidebar();renderStream();
}

function openEditFeed(i){
    editFeedIndex=i;const f=feeds[i];
    document.getElementById('new-name').value=f.name;document.getElementById('new-url').value=f.url;
    document.getElementById('new-category').value=f.category||'Khác';
    document.getElementById('modal-title').textContent='Sửa feed';
    document.getElementById('btn-add-feed').textContent='Cập nhật';
    const hint=document.getElementById('feed-type-hint');
    hint.style.display=isTgSource(f.url)?'block':'none';
    document.getElementById('modal').classList.add('open');
}

function openModal(){
    editFeedIndex=-1;document.getElementById('new-name').value='';document.getElementById('new-url').value='';
    document.getElementById('new-category').value=categories[0]||'Khác';
    document.getElementById('modal-title').textContent='Thêm feed';
    document.getElementById('btn-add-feed').textContent='Thêm';
    document.getElementById('feed-type-hint').style.display='none';
    document.getElementById('modal').classList.add('open');
}
function closeModal(){document.getElementById('modal').classList.remove('open');}

function addFeed(){
    const name=document.getElementById('new-name').value.trim(),url=document.getElementById('new-url').value.trim(),
        cat=document.getElementById('new-category').value;
    if(!name||!url){alert('Nhập đủ tên và URL');return;}
    if(editFeedIndex>=0){feeds[editFeedIndex]={name,url,category:cat};}
    else{feeds.push({name,url,category:cat});}
    saveFeeds();wsSend({type:'feeds',feeds:feeds.map(f=>({url:f.url,name:f.name,category:f.category}))});
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
function connectWS(){
    ws=new WebSocket(WS_URL);
    ws.onopen=()=>{
        wsReady=true;document.getElementById('ws-dot').className='ws-dot on';
        document.getElementById('ws-lbl').textContent='Đang theo dõi';
        wsSend({type:'feeds',feeds:feeds.map(f=>({url:f.url,name:f.name,category:f.category}))});
        wsSend({type:'tg_settings',channels:tgChannels});
        wsSend({type:'auto_fwd',enabled:autoFwd,channels:tgChannels});
        wsSend({type:'categories',categories});
        // Khi reconnect: chỉ fetch lại RSS feeds (nhanh), TG feeds sẽ nhận qua WS broadcast
        if(wsReconnectCount>0){
            setTimeout(()=>{
                feeds.filter(f=>!isTgSource(f.url)).forEach(f=>fetchAndMerge(f.url,f.name,f.category,false));
            }, 1000);
        }
        wsReconnectCount++;
    };
    ws.onmessage=e=>{
        const msg=JSON.parse(e.data);
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
    ws.onclose=()=>{wsReady=false;document.getElementById('ws-dot').className='ws-dot wait';document.getElementById('ws-lbl').textContent='Mất kết nối...';setTimeout(connectWS,3000);};
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
        try:
            translated_desc_plain = GoogleTranslator(source='auto', target='vi').translate(desc_plain[:4000]) or desc_plain
        except:
            translated_desc_plain = desc_plain

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

def _do_forward(processed, category, url):
    """Gửi tin lên Telegram nếu auto-forward bật"""
    with lock:
        do_fwd = auto_fwd_enabled
        cfgs = list(tg_channels)
    if not do_fwd or not cfgs:
        return
    total_sent = 0
    for ch in cfgs:
        should_send = False
        if ch['type'] == 'master':
            should_send = True
        elif ch['type'] in ['category', 'group']:
            if ch.get('category_filter') == category:
                should_send = True
        if should_send:
            r = send_to_telegram(ch['token'], ch['channel_id'], ch['name'],
                                 list(reversed(processed)),
                                 category_filter=ch.get('category_filter'),
                                 channel_type=ch['type'])
            total_sent += sum(1 for x in r if x['ok'])
    if total_sent > 0:
        broadcast({'type': 'auto_fwd_sent', 'count': total_sent, 'url': url})

def _poll_one(url_obj):
    """Poll một feed, trả về True nếu thành công"""
    url      = url_obj['url']
    category = url_obj.get('category', '')
    try:
        if is_tg_source(url):
            if not TELETHON_AVAILABLE or tg_client is None:
                return False
            channel = normalize_tg_channel(url)
            # KHÔNG dùng semaphore ở đây — poller đã gọi TG feeds tuần tự
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
            with lock:
                known_guids[url] = {it['guid'] for it in items if it['guid']}
        else:
            new_items = [it for it in items if it['guid'] and it['guid'] not in prev]
            if new_items:
                processed = process_tg_items(new_items) if is_tg_source(url) else process_items(new_items)
                with lock:
                    known_guids[url] = {it['guid'] for it in items if it['guid']}
                ws_items = [{k: v for k, v in it.items() if k != '_tg_media_bytes'} for it in processed]
                broadcast({'type': 'new_items', 'url': url, 'items': ws_items})
                print(f'[+] {len(new_items)} bài mới: {url}')
                _do_forward(processed, category, url)
        return True
    except Exception as ex:
        print(f'[!] Poll lỗi {url}: {ex}')
        return False

def _is_ica_source(url):
    return 'tg.i-c-a.su' in url

def _init_tg_feed(url_obj):
    """Load lịch sử 1 lần khi feed TG mới được thêm vào"""
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
        # Broadcast lịch sử để hiển thị trên web (không forward)
        ws_items = [{k: v for k, v in it.items() if k != '_tg_media_bytes'} for it in items]
        if ws_items:
            broadcast({'type': 'new_items', 'url': url, 'items': ws_items})
        print(f'[TG] Load lịch sử {channel}: {len(items)} tin')
    except Exception as e:
        print(f'[!] Load lịch sử lỗi {channel}: {e}')

def _process_tg_queue():
    """Xử lý tin mới từ Telethon real-time queue"""
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
        category = ''
        with lock:
            for u in watched_urls:
                if u['url'] == feed_url:
                    category = u.get('category', '')
                    break
            prev = known_guids.get(feed_url)

        # Lọc trùng
        new_items = [it for it in items if not prev or it['guid'] not in prev]
        if not new_items:
            continue

        processed = process_tg_items(new_items)
        with lock:
            if known_guids.get(feed_url) is None:
                known_guids[feed_url] = set()
            for it in new_items:
                known_guids[feed_url].add(it['guid'])

        ws_items = [{k: v for k, v in it.items() if k != '_tg_media_bytes'} for it in processed]
        broadcast({'type': 'new_items', 'url': feed_url, 'items': ws_items})
        print(f'[+] {len(new_items)} bài mới (real-time): {feed_url}')
        _do_forward(processed, category, feed_url)

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
        for url_obj in tg_urls:
            url = url_obj['url']
            if url not in tg_inited and TELETHON_AVAILABLE and tg_client is not None:
                tg_inited.add(url)
                threading.Thread(target=_init_tg_feed, args=(url_obj,), daemon=True).start()
                time.sleep(0.3)   # delay nhỏ giữa các feed để tránh flood Telegram API

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
    global translate_enabled, auto_fwd_enabled, tg_channels, categories
    with lock:
        ws_clients.add(ws)
    print(f'[WS] Client kết nối, tổng={len(ws_clients)}')
    msg_count = 0
    try:
        for raw in ws:
            msg_count += 1
            try:
                msg = json.loads(raw)
            except:
                continue
            t = msg.get('type')
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
        print(f'[WS] Lỗi: {e}')
    finally:
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
            body = self._read_json()
            channels = body.get('channels', [])
            items_raw = body.get('items', [])
            if not channels:
                self.send_response(400)
                self.send_header('Content-Type', 'application/json')
                self.end_headers()
                self.wfile.write(b'{"error": "missing channels"}')
                return

            # Với item từ Telethon, fetch lại media bytes (serialize qua semaphore)
            items = []
            for it in items_raw:
                feed_url = it.get('feedUrl', '')
                if is_tg_source(feed_url) and TELETHON_AVAILABLE and tg_client is not None:
                    try:
                        link = it.get('link', '')
                        msg_id = int(link.rstrip('/').split('/')[-1])
                        channel = normalize_tg_channel(feed_url)
                        with tg_semaphore:
                            msgs = tg_run(tg_client.get_messages(channel, ids=msg_id))
                        msg = msgs if not isinstance(msgs, list) else (msgs[0] if msgs else None)
                        media_bytes = []
                        if msg and msg.media:
                            try:
                                with tg_semaphore:
                                    data = tg_run(tg_client.download_media(msg.media, file=bytes))
                                if data:
                                    if isinstance(msg.media, MessageMediaPhoto):
                                        media_bytes.append(('photo', data, 'image/jpeg'))
                                    elif isinstance(msg.media, MessageMediaDocument):
                                        mime = getattr(msg.media.document, 'mime_type', 'application/octet-stream')
                                        if mime.startswith('video') or mime.startswith('image'):
                                            media_bytes.append(('video' if mime.startswith('video') else 'photo', data, mime))
                            except Exception as me:
                                print(f'[!] Download media lỗi: {me}')
                        it = {**it, '_tg_media_bytes': media_bytes if media_bytes else None}
                    except Exception as e:
                        print(f'[!] Re-fetch media lỗi: {e}')
                items.append(it)

            all_results = []
            for ch in channels:
                r = send_to_telegram(ch['token'], ch['channel_id'], ch['name'], items,
                                     category_filter=ch.get('category_filter'), channel_type=ch['type'])
                all_results.extend(r)
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
    import webbrowser
    webbrowser.open(f'http://localhost:{HTTP_PORT}')
    try:
        ThreadingHTTPServer(('', HTTP_PORT), HttpHandler).serve_forever()
    except KeyboardInterrupt:
        print('Dừng.')