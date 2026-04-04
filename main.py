import os
import io
import uuid
import re
import datetime
import subprocess
import time
import threading
import traceback
import random
from concurrent.futures import ThreadPoolExecutor, as_completed
import openai
import gspread
import requests as http_requests
from fastapi import FastAPI, BackgroundTasks
from fastapi.responses import JSONResponse
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload, MediaFileUpload
import json
import base64

app = FastAPI()

# ============================================================
# CONFIGURATION
# ============================================================
MASTER_SHEET_URL    = os.environ.get("MASTER_SHEET_URL",    "https://docs.google.com/spreadsheets/d/1tlB7auPNU_fXUiuIbI-5EbmizInw4rw35tB2SWEvPas/edit")
PART2_FILE_ID       = os.environ.get("PART2_FILE_ID",       "1INNY-MUaI0xFPd7dafeGx5_5TE9CwlbL")
PART2_UK_FILE_ID    = os.environ.get("PART2_UK_FILE_ID",    "1_G7pAuZx-5-xCFEsurI5CXiQVQfM2Csu")
DEFAULT_TITLE       = os.environ.get("DEFAULT_TITLE",       "The danger no one told you about")
VIDEOS_PER_CAMPAIGN = int(os.environ.get("VIDEOS_PER_CAMPAIGN", "20"))
TELEGRAM_TOKEN      = os.environ.get("TELEGRAM_TOKEN",      "8747966519:AAEsz9JSa8OXcETu9OnUWwf6v1LdvNxrv3w")
TELEGRAM_CHAT_ID    = os.environ.get("TELEGRAM_CHAT_ID",    "1687730801")
GOOGLE_CREDS_JSON   = os.environ.get("GOOGLE_CREDS_JSON",   "")
OPENAI_API_KEY      = os.environ.get("OPENAI_API_KEY",      "")

KIE_API_KEY         = os.environ.get("KIE_API_KEY", "2c6b074d932083a007b99a398bbe829c")
KIE_API_BASE        = "https://api.kie.ai"
KIE_MODEL           = os.environ.get("KIE_MODEL",           "veo3_fast")
KIE_ASPECT_RATIO    = "9:16"

PROMPTS_SHEET_ID    = os.environ.get("PROMPTS_SHEET_ID",    "13BxBpA1nZ8Vt-hlRDUqZcC6pTU0z-BMvwdtuZmnsy6g")
MAX_CONCURRENT      = int(os.environ.get("MAX_CONCURRENT", "5"))
SERVER_ID           = os.environ.get("SERVER_ID", "S1")

# ============================================================
# MULTI-COUNTRY CONFIG
# ============================================================
COUNTRY_CONFIG = {
    "USA": {
        "results_folder_id": os.environ.get("RESULTS_USA_FOLDER_ID", "1ZTciHcp8LtbLjsuwUbE0MEwuCNMJzbPQ"),
        "part2_file_id":     PART2_FILE_ID,
        "master_tab":        os.environ.get("MASTER_TAB_USA", "To launch (USA)"),
        "prompt_tab":        os.environ.get("PROMPT_TAB_USA", "USA"),
    },
}

DISTRIBUTE_TABS = ["USA_1", "USA_2", "USA_3", "USA_4", "USA_5"]

MASTER_SHEET_HEADERS = [
    "Ad_Name", "Drive_Share_Link", "Direct_Download_Link",
    "Campaign_Name", "AdSet_Name", "Primary_Text", "Headline", "Video_Prompt",
    "status", "ad_type", "error_message"
]

FONT_PATH = "/usr/share/fonts/truetype/custom/Montserrat-Bold.ttf"

is_processing = False
last_activity_time = time.time()
WATCHDOG_TIMEOUT = 7200

# ============================================================
# GOOGLE AUTH (cached credentials)
# ============================================================
_google_creds = None
_google_creds_lock = threading.Lock()

def _get_credentials():
    global _google_creds
    with _google_creds_lock:
        if _google_creds is None:
            creds_data = json.loads(base64.b64decode(GOOGLE_CREDS_JSON).decode("utf-8"))
            _google_creds = service_account.Credentials.from_service_account_info(
                creds_data,
                scopes=[
                    "https://www.googleapis.com/auth/spreadsheets",
                    "https://www.googleapis.com/auth/drive"
                ]
            )
        return _google_creds

def get_google_services():
    creds = _get_credentials()
    drive_service = build("drive", "v3", credentials=creds)
    gc = gspread.authorize(creds)
    return drive_service, gc

# ============================================================
# KIE.AI — dynamic headers
# ============================================================
def _kie_headers():
    return {
        "Authorization": f"Bearer {KIE_API_KEY}",
        "Content-Type": "application/json",
    }

# ============================================================
# DRIVE HELPERS
# ============================================================
def drive_download_file(drive_service, file_id, local_path):
    request = drive_service.files().get_media(fileId=file_id, supportsAllDrives=True)
    fh = io.FileIO(local_path, "wb")
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while not done:
        _, done = downloader.next_chunk()

def drive_get_or_create_folder(drive_service, parent_id, folder_name):
    q = f"'{parent_id}' in parents and trashed = false and mimeType = 'application/vnd.google-apps.folder' and name = '{folder_name}'"
    resp = drive_service.files().list(
        q=q, fields="files(id,name)",
        supportsAllDrives=True,
        includeItemsFromAllDrives=True
    ).execute()
    files = resp.get("files", [])
    if files:
        return files[0]["id"]
    meta = {"name": folder_name, "mimeType": "application/vnd.google-apps.folder", "parents": [parent_id]}
    created = drive_service.files().create(body=meta, fields="id", supportsAllDrives=True).execute()
    return created["id"]

def drive_upload_video(drive_service, local_path, parent_id, filename):
    file_size = os.path.getsize(local_path)
    for attempt in range(3):
        try:
            if file_size < 5 * 1024 * 1024:
                media = MediaFileUpload(local_path, mimetype="video/mp4", resumable=False)
            else:
                media = MediaFileUpload(local_path, mimetype="video/mp4", resumable=True, chunksize=5*1024*1024)
            meta = {"name": filename, "parents": [parent_id]}
            req = drive_service.files().create(
                body=meta, media_body=media, fields="id", supportsAllDrives=True
            )
            if file_size < 5 * 1024 * 1024:
                created = req.execute()
            else:
                response = None
                while response is None:
                    try:
                        _, response = req.next_chunk()
                    except Exception as chunk_err:
                        if "200" in str(chunk_err):
                            response = req.execute()
                            break
                        raise
                created = response
            return created["id"]
        except Exception as e:
            print(f"  [{SERVER_ID}] Upload attempt {attempt+1} failed: {e}")
            if attempt < 2:
                time.sleep(3)
                drive_service, _ = get_google_services()
            else:
                raise

def drive_list_all_files(drive_service, folder_id, mime_type="video/mp4"):
    """List all files in a folder with pagination support."""
    all_files = []
    page_token = None
    while True:
        q = f"'{folder_id}' in parents and trashed=false and mimeType='{mime_type}'"
        kwargs = dict(
            q=q, fields="nextPageToken,files(id,name)",
            supportsAllDrives=True, includeItemsFromAllDrives=True,
            pageSize=1000
        )
        if page_token:
            kwargs["pageToken"] = page_token
        resp = drive_service.files().list(**kwargs).execute()
        all_files.extend(resp.get("files", []))
        page_token = resp.get("nextPageToken")
        if not page_token:
            break
    return all_files

def make_drive_links(file_id):
    share_link = f"https://drive.google.com/file/d/{file_id}/view?usp=sharing"
    direct_download = f"https://drive.google.com/uc?export=download&id={file_id}"
    return share_link, direct_download

# ============================================================
# MASTER SHEET
# ============================================================
_master_ss_cache = {}
_master_ss_lock = threading.Lock()

def get_or_create_master_tab(gc, tab_name):
    with _master_ss_lock:
        ss = _master_ss_cache.get("_ss")
        if ss is None:
            ss = _sheets_retry(lambda: gc.open_by_url(MASTER_SHEET_URL))
            _master_ss_cache["_ss"] = ss
        ws = _master_ss_cache.get(tab_name)
        if ws is not None:
            return ws
    try:
        ws = _sheets_retry(lambda: ss.worksheet(tab_name))
    except gspread.exceptions.WorksheetNotFound:
        ws = ss.add_worksheet(title=tab_name, rows=1000, cols=20)
        ws.append_row(MASTER_SHEET_HEADERS, value_input_option="USER_ENTERED")
        print(f"  [{SERVER_ID}] Tab '{tab_name}' created in Master Sheet")
    with _master_ss_lock:
        _master_ss_cache[tab_name] = ws
    return ws

# ============================================================
# TELEGRAM
# ============================================================
def send_telegram(msg):
    try:
        resp = http_requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            json={"chat_id": TELEGRAM_CHAT_ID, "text": f"[{SERVER_ID}] {msg}"},
            timeout=10
        )
        if not resp.ok:
            print(f"  [{SERVER_ID}] Telegram error: {resp.status_code} — {resp.text[:200]}")
    except Exception as e:
        print(f"  [{SERVER_ID}] Telegram send failed: {e}")

# ============================================================
# FFMPEG HELPERS
# ============================================================
def get_video_duration(path):
    result = subprocess.run(
        ["ffprobe", "-v", "error", "-show_entries", "format=duration",
         "-of", "default=noprint_wrappers=1:nokey=1", path],
        capture_output=True, text=True
    )
    return float(result.stdout.strip())

def has_video_stream(path):
    result = subprocess.run(
        ["ffprobe", "-v", "error", "-select_streams", "v:0",
         "-show_entries", "stream=codec_type",
         "-of", "default=noprint_wrappers=1:nokey=1", path],
        capture_output=True, text=True
    )
    return "video" in result.stdout.strip()

def reencode_video(input_path, output_path):
    subprocess.run([
        "ffmpeg", "-y", "-i", input_path,
        "-c:v", "libx264", "-preset", "fast", "-crf", "23",
        "-r", "24",
        "-c:a", "aac", "-ar", "44100", "-ac", "2",
        "-movflags", "+faststart",
        output_path
    ], check=True, capture_output=True)

# ============================================================
# WHISPER WITH RETRY
# ============================================================
def _whisper_transcribe(audio_path):
    """Call Whisper with retry + exponential backoff for 429s."""
    client = openai.OpenAI(api_key=OPENAI_API_KEY)
    for attempt in range(3):
        try:
            with open(audio_path, "rb") as f:
                result = client.audio.transcriptions.create(
                    model="whisper-1",
                    file=f,
                    response_format="verbose_json",
                    timestamp_granularities=["word"]
                )
            return result
        except Exception as e:
            if "429" in str(e) or "rate" in str(e).lower():
                wait = (2 ** attempt) * 5
                print(f"  [{SERVER_ID}] Whisper rate limited, waiting {wait}s...")
                time.sleep(wait)
            else:
                if attempt < 2:
                    time.sleep(2)
                else:
                    raise

def get_speech_bounds(audio_path, video_duration):
    result = _whisper_transcribe(audio_path)
    words = result.words or []
    if words:
        start_t = max(0, words[0].start - 0.1)
        end_t = min(words[-1].end + 0.8, video_duration - 0.05)
    else:
        start_t, end_t = 0, video_duration
    return start_t, end_t

def generate_srt(audio_path):
    result = _whisper_transcribe(audio_path)
    words_all = []
    if result.words:
        for w in result.words:
            words_all.append({"word": w.word, "start": w.start, "end": w.end})

    srt_lines = []
    idx = 1
    chunk = []
    for i, w in enumerate(words_all):
        chunk.append(w)
        if len(chunk) >= 5 or i == len(words_all) - 1:
            start = chunk[0]["start"]
            end = chunk[-1]["end"]
            text = " ".join([x["word"].strip() for x in chunk]).lstrip(",. ")

            def fmt(t):
                h = int(t // 3600)
                m = int((t % 3600) // 60)
                s = int(t % 60)
                ms = int((t - int(t)) * 1000)
                return f"{h:02d}:{m:02d}:{s:02d},{ms:03d}"

            srt_lines.append(f"{idx}\n{fmt(start)} --> {fmt(end)}\n{text}\n")
            idx += 1
            chunk = []

    return "\n".join(srt_lines)

def build_subtitle_drawtext_filters(srt_path, font_path):
    if os.path.exists(font_path):
        font_arg = f"fontfile={font_path.replace(':', chr(92) + ':')}"
    elif os.path.exists('/usr/share/fonts/truetype/liberation/LiberationSans-Bold.ttf'):
        font_arg = "fontfile=/usr/share/fonts/truetype/liberation/LiberationSans-Bold.ttf"
    else:
        font_arg = ""

    with open(srt_path, encoding="utf-8") as f:
        content = f.read()

    blocks = re.split(r'\n\n+', content.strip())
    filters = []
    for block in blocks:
        lines = block.strip().split('\n')
        if len(lines) < 3:
            continue
        m = re.match(r'(\d{2}:\d{2}:\d{2},\d{3}) --> (\d{2}:\d{2}:\d{2},\d{3})', lines[1])
        if not m:
            continue
        def ts2s(ts):
            h, mn, s_ms = ts.split(':')
            s, ms = s_ms.split(',')
            return int(h)*3600 + int(mn)*60 + int(s) + int(ms)/1000
        start = ts2s(m.group(1))
        end = ts2s(m.group(2))
        text = ' '.join(lines[2:]).strip()
        text_esc = (text
            .replace('\\', '\\\\')
            .replace("'", "\u2019")
            .replace(':', '\\:')
            .replace(',', '\\,')
            .replace('[', '\\[')
            .replace(']', '\\]')
            .replace('%', '%%')
        )
        if font_arg:
            f_str = (
                f"drawtext={font_arg}:text='{text_esc}':"
                f"fontcolor=white:fontsize=34:x=(w-tw)/2:y=965:"
                f"box=1:boxcolor=black@1.0:boxborderw=8:"
                f"enable='between(t,{start},{end})'"
            )
        else:
            f_str = (
                f"drawtext=text='{text_esc}':"
                f"fontcolor=white:fontsize=34:x=(w-tw)/2:y=965:"
                f"box=1:boxcolor=black@1.0:boxborderw=8:"
                f"enable='between(t,{start},{end})'"
            )
        filters.append(f_str)
    return filters

# ============================================================
# EDIT A SINGLE VIDEO (thread-safe temp files via UUID prefix)
# ============================================================
edit_semaphore = threading.Semaphore(1)

def edit_single_video(local_hook_raw, local_part2_clean, metadata, country, vid_index):
    uid = uuid.uuid4().hex[:8]
    pfx = f"/tmp/edit_{SERVER_ID}_{uid}"
    local_hook_audio = f"{pfx}_audio.wav"
    local_hook_cut   = f"{pfx}_cut.mp4"
    local_concat     = f"{pfx}_concat.mp4"
    local_audio      = f"{pfx}_full_audio.wav"
    local_srt        = f"{pfx}_subs.srt"
    local_out        = f"{pfx}_final.mp4"
    concat_list      = f"{pfx}_list.txt"

    temps = [local_hook_audio, local_hook_cut,
             local_concat, local_audio, local_srt, concat_list]

    try:
        video_title = metadata.get("title", "") or DEFAULT_TITLE

        hook_duration = get_video_duration(local_hook_raw)
        print(f"  [{SERVER_ID}/{country}] V{vid_index} hook duration: {hook_duration:.2f}s")

        subprocess.run([
            "ffmpeg", "-y", "-i", local_hook_raw,
            "-vn", "-acodec", "pcm_s16le", "-ar", "16000", "-ac", "1",
            local_hook_audio
        ], check=True, capture_output=True)
        start_t, end_t = get_speech_bounds(local_hook_audio, hook_duration)
        print(f"  [{SERVER_ID}/{country}] V{vid_index} speech: {start_t:.2f}s -> {end_t:.2f}s")

        if os.path.exists(local_hook_audio):
            os.remove(local_hook_audio)

        subprocess.run([
            "ffmpeg", "-y", "-i", local_hook_raw,
            "-ss", str(start_t), "-to", str(end_t),
            "-c:v", "libx264", "-preset", "ultrafast", "-crf", "23",
            "-r", "24", "-c:a", "aac", "-ar", "44100", "-ac", "2",
            "-movflags", "+faststart",
            local_hook_cut
        ], check=True, capture_output=True)

        with open(concat_list, "w") as cl:
            cl.write(f"file '{local_hook_cut}'\n")
            cl.write(f"file '{local_part2_clean}'\n")
        subprocess.run([
            "ffmpeg", "-y", "-f", "concat", "-safe", "0",
            "-i", concat_list, "-c", "copy",
            "-movflags", "+faststart",
            local_concat
        ], check=True, capture_output=True)

        if os.path.exists(local_hook_cut):
            os.remove(local_hook_cut)

        subprocess.run([
            "ffmpeg", "-y", "-i", local_concat,
            "-vn", "-acodec", "pcm_s16le", "-ar", "16000", "-ac", "1",
            local_audio
        ], check=True, capture_output=True)

        srt_content = generate_srt(local_audio)
        with open(local_srt, "w", encoding="utf-8") as sf:
            sf.write(srt_content)

        if os.path.exists(local_audio):
            os.remove(local_audio)

        title_clean = video_title.replace("\n", " ").strip()

        def esc(s):
            return (s.replace("\\", "\\\\")
                     .replace("'", "\u2019")
                     .replace(":", "\\:")
                     .replace("%", "%%"))

        def split_title_lines(text, max_chars=28):
            words = text.split()
            if len(text) <= max_chars:
                return [text]
            for cut in range(len(words) // 2, len(words)):
                l1 = " ".join(words[:cut])
                l2 = " ".join(words[cut:])
                if len(l1) <= max_chars and len(l2) <= max_chars:
                    return [l1, l2]
            for c1 in range(1, len(words) - 1):
                for c2 in range(c1 + 1, len(words)):
                    l1 = " ".join(words[:c1])
                    l2 = " ".join(words[c1:c2])
                    l3 = " ".join(words[c2:])
                    if len(l1) <= max_chars and len(l2) <= max_chars and len(l3) <= max_chars:
                        return [l1, l2, l3]
            return [text]

        title_lines = split_title_lines(title_clean)
        num_lines = len(title_lines)
        font_size = 46 if num_lines <= 2 else 40
        line_height = font_size + 18

        if os.path.exists(FONT_PATH):
            font_path_esc = FONT_PATH.replace(':', '\\:')
            font_base = f"fontfile={font_path_esc}:fontcolor=black:fontsize={font_size}"
        else:
            font_base = f"fontcolor=black:fontsize={font_size}"

        total_height = num_lines * line_height
        start_y = 810 - total_height // 2

        title_parts = []
        for i, line in enumerate(title_lines):
            y = start_y + i * line_height
            title_parts.append(
                f"drawtext=text='{esc(line)}':{font_base}:x=(w-tw)/2:y={y}:"
                f"box=1:boxcolor=white@1.0:boxborderw=12:enable='lt(t,4)'"
            )
        title_filter = ",".join(title_parts)

        sub_filters = build_subtitle_drawtext_filters(local_srt, FONT_PATH)
        all_filters = [title_filter] + sub_filters
        vf_filter = ",".join(all_filters)

        result = subprocess.run([
            "ffmpeg", "-y", "-i", local_concat,
            "-vf", vf_filter,
            "-c:v", "libx264", "-preset", "ultrafast", "-crf", "23",
            "-c:a", "aac",
            "-movflags", "+faststart",
            local_out
        ], capture_output=True, text=True)

        if os.path.exists(local_concat):
            os.remove(local_concat)

        if result.returncode != 0:
            print(f"  [{SERVER_ID}/{country}] Overlay stderr: {result.stderr[-500:]}")
            raise Exception(f"Final encode failed (exit {result.returncode})")

        if not os.path.exists(local_out) or os.path.getsize(local_out) < 10000:
            raise Exception(f"Output file missing or too small")

        final_duration = get_video_duration(local_out)
        print(f"  [{SERVER_ID}/{country}] V{vid_index} edited: {final_duration:.2f}s")
        return local_out

    finally:
        for p in temps:
            try:
                if os.path.exists(p):
                    os.remove(p)
            except:
                pass

# ============================================================
# KIE.AI — VIDEO GENERATION
# ============================================================
def kie_generate_video(prompt: str) -> str:
    resp = http_requests.post(
        f"{KIE_API_BASE}/api/v1/veo/generate",
        headers=_kie_headers(),
        json={
            "model": KIE_MODEL,
            "prompt": prompt,
            "aspect_ratio": KIE_ASPECT_RATIO,
            "generationType": "TEXT_2_VIDEO",
        },
        timeout=30,
    )
    data = resp.json()
    if data.get("code") != 200:
        raise Exception(f"kie.ai generate error: {data.get('msg', resp.text[:200])}")
    task_id = data["data"]["taskId"]
    print(f"  [{SERVER_ID}] kie.ai task created: {task_id}")
    return task_id

def kie_poll_video(task_id: str, max_wait=600, interval=15) -> str:
    elapsed = 0
    while elapsed < max_wait:
        time.sleep(interval)
        elapsed += interval
        try:
            resp = http_requests.get(
                f"{KIE_API_BASE}/api/v1/veo/record-info",
                headers=_kie_headers(),
                params={"taskId": task_id},
                timeout=30,
            )
            data = resp.json()
            if data.get("code") != 200:
                print(f"  [{SERVER_ID}] kie.ai poll error: {data.get('msg', '')} — retrying...")
                continue

            flag = data["data"].get("successFlag", 0)
            if flag == 1:
                urls = data["data"].get("response", {}).get("resultUrls", [])
                if urls:
                    print(f"  [{SERVER_ID}] kie.ai video ready: {urls[0][:80]}...")
                    return urls[0]
                raise Exception("kie.ai returned success but no resultUrls")
            elif flag in (2, 3):
                err = data["data"].get("errorMessage", "unknown error")
                raise Exception(f"kie.ai generation failed (flag={flag}): {err}")

            print(f"  [{SERVER_ID}] kie.ai generating... ({elapsed}s / {max_wait}s)")
        except http_requests.exceptions.RequestException as e:
            print(f"  [{SERVER_ID}] kie.ai poll network error: {e}")

    raise Exception(f"kie.ai timeout after {max_wait}s for task {task_id}")

def kie_download_video(url: str, local_path: str):
    for attempt in range(3):
        try:
            resp = http_requests.get(url, stream=True, timeout=120)
            resp.raise_for_status()
            with open(local_path, "wb") as f:
                for chunk in resp.iter_content(chunk_size=8192):
                    f.write(chunk)
            if os.path.getsize(local_path) < 1000:
                raise Exception(f"Downloaded file too small: {os.path.getsize(local_path)} bytes")
            print(f"  [{SERVER_ID}] kie.ai video downloaded: {local_path}")
            return
        except Exception as e:
            print(f"  [{SERVER_ID}] kie.ai download attempt {attempt+1} failed: {e}")
            if attempt < 2:
                time.sleep(5)
            else:
                raise

# ============================================================
# PROMPT SHEET
# ============================================================
def get_prompts_for_country(gc, country_cfg, limit=5):
    prompt_tab = country_cfg["prompt_tab"]
    try:
        ws = _get_worksheet(gc, prompt_tab)
    except gspread.exceptions.WorksheetNotFound:
        return []

    all_rows = _sheets_retry(lambda: ws.get_all_values())
    if len(all_rows) <= 1:
        return []

    headers = [h.strip().lower() for h in all_rows[0]]
    prompts = []

    for idx, row in enumerate(all_rows[1:], start=2):
        row_dict = {}
        for h_idx, h in enumerate(headers):
            row_dict[h] = row[h_idx].strip() if h_idx < len(row) else ""

        status = row_dict.get("status", "").lower()
        if status != "ready":
            continue

        prompt_text = row_dict.get("prompt", "")
        if not prompt_text:
            continue

        prompts.append({
            "row_index": idx,
            "prompt": prompt_text,
            "title_of_video": row_dict.get("title of video", ""),
            "headline_meta": row_dict.get("headline meta", ""),
            "primary_text": row_dict.get("primary text", ""),
        })

        if len(prompts) >= limit:
            break

    return prompts

def _sheets_retry(func, max_retries=4):
    """Retry a Sheets API call with exponential backoff on 429 errors."""
    for attempt in range(max_retries):
        try:
            return func()
        except gspread.exceptions.APIError as e:
            if "429" in str(e) and attempt < max_retries - 1:
                wait = (2 ** attempt) * 5 + random.uniform(0, 3)
                print(f"  [{SERVER_ID}] Sheets 429, retry {attempt+1} in {wait:.0f}s")
                time.sleep(wait)
            else:
                raise

_ws_cache = {}
_ws_cache_lock = threading.Lock()

def _get_worksheet(gc, tab_name):
    """Get a worksheet, caching the spreadsheet object to reduce API calls."""
    with _ws_cache_lock:
        ss = _ws_cache.get("_spreadsheet")
        if ss is None:
            ss = _sheets_retry(lambda: gc.open_by_key(PROMPTS_SHEET_ID))
            _ws_cache["_spreadsheet"] = ss
        ws = _ws_cache.get(tab_name)
        if ws is None:
            ws = _sheets_retry(lambda: ss.worksheet(tab_name))
            _ws_cache[tab_name] = ws
        return ws

_headers_cache = {}
_headers_cache_lock = threading.Lock()

def _get_status_col(gc, prompt_tab):
    """Get status column index (1-based), cached per tab."""
    with _headers_cache_lock:
        if prompt_tab in _headers_cache:
            return _headers_cache[prompt_tab]
    ws = _get_worksheet(gc, prompt_tab)
    headers = [h.strip().lower() for h in _sheets_retry(lambda: ws.row_values(1))]
    status_col = (headers.index("status") + 1) if "status" in headers else -1
    with _headers_cache_lock:
        _headers_cache[prompt_tab] = status_col
    return status_col

def mark_prompt_status(gc, country_cfg, row_index, status, prompt_text=None):
    prompt_tab = country_cfg["prompt_tab"]
    status_col = _get_status_col(gc, prompt_tab)
    if status_col < 0:
        return
    ws = _get_worksheet(gc, prompt_tab)
    s = status
    r = row_index
    _sheets_retry(lambda: ws.update_cell(r, status_col, s))

# ============================================================
# FULL PIPELINE: PARALLEL generation -> edit -> upload -> log
# ============================================================
sheet_lock = threading.Lock()
_vid_index_lock = threading.Lock()

def _process_single_prompt(p_data, country, c_cfg, date_str, vid_index,
                           drive_svc, edited_folder_id, original_folder_id,
                           local_part2_clean, edited_names):
    """Process a single prompt: generate, edit, upload, log. Thread-safe."""
    global last_activity_time
    uid = uuid.uuid4().hex[:8]
    row_index = p_data["row_index"]
    prompt_text = p_data["prompt"]
    nom_final = f"{date_str}_{SERVER_ID}_V{vid_index}.mp4"

    if nom_final in edited_names:
        print(f"  [{SERVER_ID}/{country}] Skipping V{vid_index}: already exists (cached)")
        return "skipped"

    try:
        dup_q = f"'{edited_folder_id}' in parents and trashed=false and name='{nom_final}'"
        dup_check = drive_svc.files().list(
            q=dup_q, fields="files(id)", supportsAllDrives=True,
            includeItemsFromAllDrives=True, pageSize=1
        ).execute().get("files", [])
        if dup_check:
            print(f"  [{SERVER_ID}/{country}] Skipping V{vid_index}: already on Drive")
            edited_names.add(nom_final)
            return "skipped"
    except Exception as dup_err:
        print(f"  [{SERVER_ID}/{country}] Drive dup check error: {dup_err}")

    local_raw = f"/tmp/gen_{SERVER_ID}_{uid}.mp4"
    local_edited = None

    try:
        with sheet_lock:
            _, gc_s = get_google_services()
            ws_check = _get_worksheet(gc_s, c_cfg["prompt_tab"])
            status_col = _get_status_col(gc_s, c_cfg["prompt_tab"])
            if status_col > 0:
                ri = row_index
                cell_val = _sheets_retry(lambda: ws_check.cell(ri, status_col).value)
                if cell_val and cell_val.strip().lower() != "ready":
                    print(f"  [{SERVER_ID}/{country}] V{vid_index}: prompt no longer READY ({cell_val}), skipping")
                    return "skipped"
            mark_prompt_status(gc_s, c_cfg, row_index, "processing")

        print(f"  [{SERVER_ID}/{country}] V{vid_index}: generating...")

        task_id = kie_generate_video(prompt_text)
        video_url = kie_poll_video(task_id)
        last_activity_time = time.time()

        kie_download_video(video_url, local_raw)

        orig_name = f"veo_{SERVER_ID}_{datetime.datetime.now().strftime('%H%M%S')}_{uid}.mp4"
        drive_upload_video(drive_svc, local_raw, original_folder_id, orig_name)
        print(f"  [{SERVER_ID}/{country}] V{vid_index}: original uploaded")

        edit_semaphore.acquire()
        try:
            metadata = {
                "title": p_data["title_of_video"] or DEFAULT_TITLE,
                "headline": p_data["headline_meta"],
                "primary_text": p_data["primary_text"],
                "prompt": prompt_text,
            }
            local_edited = edit_single_video(local_raw, local_part2_clean, metadata, country, vid_index)
        finally:
            edit_semaphore.release()

        if not local_edited or not os.path.exists(local_edited) or os.path.getsize(local_edited) < 10000:
            raise Exception("Edited file missing or too small")

        out_id = drive_upload_video(drive_svc, local_edited, edited_folder_id, nom_final)
        edited_names.add(nom_final)
        print(f"  [{SERVER_ID}/{country}] V{vid_index}: edited uploaded")

        version = ((vid_index - 1) // VIDEOS_PER_CAMPAIGN) + 1
        drive_link, direct_link = make_drive_links(out_id)

        for attempt in range(3):
            try:
                _, gc_t = get_google_services()
                mws = get_or_create_master_tab(gc_t, c_cfg["master_tab"])
                mws.append_row(
                    [nom_final.replace(".mp4", ""), drive_link, direct_link,
                     f"C{date_str}_{SERVER_ID}_{country}_{version:02d}",
                     f"adset{version}_{SERVER_ID}_{country}_{date_str}",
                     p_data["primary_text"], p_data["headline_meta"], prompt_text,
                     "pending", "video", ""],
                    value_input_option="USER_ENTERED"
                )
                break
            except Exception as se:
                print(f"  [{SERVER_ID}] Sheet write retry {attempt+1}: {se}")
                if attempt < 2:
                    time.sleep(5)
                else:
                    send_telegram(f"{country} Sheet write FAILED V{vid_index}: {str(se)[:100]}")

        with sheet_lock:
            try:
                _, gc_d = get_google_services()
                mark_prompt_status(gc_d, c_cfg, row_index, "done")
            except Exception as mark_err:
                print(f"  [{SERVER_ID}/{country}] V{vid_index} mark done failed: {mark_err}")

        last_activity_time = time.time()
        send_telegram(f"{country} V{vid_index} done!")
        return "success"

    except Exception as e:
        print(f"  [{SERVER_ID}/{country}] V{vid_index} ERROR: {e}")
        traceback.print_exc()
        send_telegram(f"{country} V{vid_index} error: {str(e)[:200]}")
        try:
            with sheet_lock:
                _, gc_e = get_google_services()
                mark_prompt_status(gc_e, c_cfg, row_index, "error")
        except:
            pass
        return "error"

    finally:
        try:
            if os.path.exists(local_raw):
                os.remove(local_raw)
        except:
            pass
        if local_edited:
            try:
                if os.path.exists(local_edited):
                    os.remove(local_edited)
            except:
                pass


def full_pipeline(country="USA"):
    global is_processing, last_activity_time
    c_cfg = COUNTRY_CONFIG[country]
    date_str = datetime.datetime.now().strftime("%d.%m")

    local_part2 = f"/tmp/part2_{SERVER_ID}_{country}.mp4"
    local_part2_clean = f"/tmp/part2_clean_{SERVER_ID}_{country}.mp4"

    try:
        drive_svc, gc = get_google_services()

        prompts = get_prompts_for_country(gc, c_cfg, limit=999)
        if not prompts:
            print(f"[{SERVER_ID}/{country}] No READY prompts found")
            return

        print(f"[{SERVER_ID}/{country}] Found {len(prompts)} READY prompt(s)")
        send_telegram(f"{country} Starting pipeline: {len(prompts)} video(s)")

        results_folder_id = c_cfg["results_folder_id"]
        date_folder_id = drive_get_or_create_folder(drive_svc, results_folder_id, date_str)
        original_folder_id = drive_get_or_create_folder(drive_svc, date_folder_id, "original")
        edited_folder_id = drive_get_or_create_folder(drive_svc, date_folder_id, "edited")

        existing_edited = drive_list_all_files(drive_svc, edited_folder_id)
        edited_names = {f["name"] for f in existing_edited}

        server_prefix = f"{date_str}_{SERVER_ID}_V"
        max_v = 0
        for ef in existing_edited:
            m = re.search(rf"{re.escape(date_str)}_{re.escape(SERVER_ID)}_V(\d+)", ef["name"])
            if m:
                v_num = int(m.group(1))
                if v_num > max_v:
                    max_v = v_num

        for p in [local_part2, local_part2_clean]:
            if os.path.exists(p):
                os.remove(p)
        drive_download_file(drive_svc, c_cfg["part2_file_id"], local_part2)
        reencode_video(local_part2, local_part2_clean)
        print(f"  [{SERVER_ID}/{country}] Part2 ready")

        vid_index = max_v + 1
        total_success = 0
        total_errors = 0

        workers = min(MAX_CONCURRENT, len(prompts))
        print(f"[{SERVER_ID}/{country}] Launching {workers} parallel workers for {len(prompts)} prompts")

        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = {}
            for p_data in prompts:
                current_vid = vid_index
                vid_index += 1
                future = executor.submit(
                    _process_single_prompt,
                    p_data, country, c_cfg, date_str, current_vid,
                    drive_svc, edited_folder_id, original_folder_id,
                    local_part2_clean, edited_names
                )
                futures[future] = current_vid

            for future in as_completed(futures):
                v = futures[future]
                try:
                    result = future.result()
                    if result == "success":
                        total_success += 1
                    elif result == "error":
                        total_errors += 1
                except Exception as e:
                    total_errors += 1
                    print(f"  [{SERVER_ID}/{country}] V{v} future error: {e}")

        send_telegram(f"{country} Pipeline done: {total_success} OK, {total_errors} errors out of {len(prompts)}")

        try:
            reconcile_master_sheet(country)
        except Exception as rec_err:
            print(f"[{SERVER_ID}/{country}] Reconciliation failed: {rec_err}")

    except Exception as e:
        print(f"[{SERVER_ID}/{country}] Pipeline critical error: {e}")
        traceback.print_exc()
        send_telegram(f"{country} Pipeline error: {str(e)[:200]}")

    finally:
        for p in [local_part2, local_part2_clean]:
            try:
                if os.path.exists(p):
                    os.remove(p)
            except:
                pass
        is_processing = False

# ============================================================
# RECONCILIATION
# ============================================================
def reconcile_master_sheet(country="USA"):
    c_cfg = COUNTRY_CONFIG[country]
    date_str = datetime.datetime.now().strftime("%d.%m")
    try:
        drive_svc, gc = get_google_services()
        results_folder_id = c_cfg["results_folder_id"]

        q = f"'{results_folder_id}' in parents and trashed=false and mimeType='application/vnd.google-apps.folder' and name='{date_str}'"
        date_folders = drive_svc.files().list(q=q, fields="files(id)", supportsAllDrives=True, includeItemsFromAllDrives=True).execute().get("files", [])
        if not date_folders:
            return 0
        date_folder_id = date_folders[0]["id"]

        sub_q = f"'{date_folder_id}' in parents and trashed=false and mimeType='application/vnd.google-apps.folder'"
        subs = drive_svc.files().list(q=sub_q, fields="files(id,name)", supportsAllDrives=True, includeItemsFromAllDrives=True).execute().get("files", [])
        edit_folder_id = next((f["id"] for f in subs if f["name"] == "edited"), None)
        if not edit_folder_id:
            return 0

        edited_files = drive_list_all_files(drive_svc, edit_folder_id)
        if not edited_files:
            return 0

        mws = get_or_create_master_tab(gc, c_cfg["master_tab"])
        existing_rows = mws.get_all_values()
        existing_names = set()
        if existing_rows:
            for row in existing_rows[1:]:
                if row:
                    existing_names.add(row[0].strip())

        prompt_data_map = {}
        try:
            ss = gc.open_by_key(PROMPTS_SHEET_ID)
            ws = ss.worksheet(c_cfg["prompt_tab"])
            p_rows = ws.get_all_values()
            if p_rows:
                p_headers = [h.strip().lower() for h in p_rows[0]]
                for pr in p_rows[1:]:
                    p_dict = {p_headers[j]: pr[j] for j in range(min(len(p_headers), len(pr)))}
                    s = p_dict.get("status", "").strip().upper()
                    if s == "DONE":
                        pt = p_dict.get("prompt", "")
                        if pt and pt not in prompt_data_map:
                            prompt_data_map[pt] = {
                                "primary_text": p_dict.get("primary text", p_dict.get("primary_text", "")),
                                "headline": p_dict.get("headline meta", p_dict.get("headline", "")),
                                "title": p_dict.get("title of video", p_dict.get("title", "")),
                                "prompt": pt,
                            }
        except Exception as pe:
            print(f"[{SERVER_ID}/{country}] Reconciliation: could not load prompt data: {pe}")

        prompt_list = list(prompt_data_map.values())

        added = 0
        for ef in sorted(edited_files, key=lambda x: x["name"]):
            ad_name = ef["name"].replace(".mp4", "")
            if ad_name in existing_names:
                continue

            file_id = ef["id"]
            drive_link, direct_link = make_drive_links(file_id)
            m = re.search(r"V(\d+)", ad_name)
            v_num = int(m.group(1)) if m else added + 1
            version = ((v_num - 1) // VIDEOS_PER_CAMPAIGN) + 1

            p_info = prompt_list[v_num - 1] if v_num <= len(prompt_list) else {}
            primary_text = p_info.get("primary_text", "")
            headline = p_info.get("headline", "")
            prompt_text = p_info.get("prompt", "")

            srv_match = re.search(r"_(S\d+)_", ad_name)
            srv_tag = srv_match.group(1) if srv_match else SERVER_ID

            for attempt in range(3):
                try:
                    mws.append_row(
                        [ad_name, drive_link, direct_link,
                         f"C{date_str}_{srv_tag}_{country}_{version:02d}",
                         f"adset{version}_{srv_tag}_{country}_{date_str}",
                         primary_text, headline, prompt_text,
                         "pending", "video", ""],
                        value_input_option="USER_ENTERED"
                    )
                    added += 1
                    break
                except Exception as se:
                    if attempt < 2:
                        time.sleep(5)
            time.sleep(1)

        if added > 0:
            print(f"[{SERVER_ID}/{country}] Reconciliation: added {added} missing entries")
            send_telegram(f"{country} Reconciliation: added {added} missing entries")
        return added
    except Exception as e:
        print(f"[{SERVER_ID}/{country}] Reconciliation error: {e}")
        return 0

# ============================================================
# RECOVERY
# ============================================================
def recover_stuck_processing():
    reverted = 0
    try:
        _, gc = get_google_services()
        for c_name, c_cfg in COUNTRY_CONFIG.items():
            ws = _get_worksheet(gc, c_cfg["prompt_tab"])
            all_rows = _sheets_retry(lambda: ws.get_all_values())
            if not all_rows:
                continue
            headers = [h.strip().lower() for h in all_rows[0]]
            if "status" not in headers:
                continue
            status_col = headers.index("status") + 1
            cells_to_update = []
            for idx, row in enumerate(all_rows[1:], start=2):
                st = row[status_col - 1].strip().lower() if status_col - 1 < len(row) else ""
                if st == "processing":
                    cells_to_update.append(gspread.Cell(row=idx, col=status_col, value="READY"))
            if cells_to_update:
                _sheets_retry(lambda: ws.update_cells(cells_to_update))
                reverted += len(cells_to_update)
        if reverted > 0:
            print(f"[{SERVER_ID}] Recovery: reverted {reverted} stuck processing prompts")
            send_telegram(f"Recovery: reverted {reverted} stuck processing prompts")
    except Exception as e:
        print(f"[{SERVER_ID}] Recovery error: {e}")
    return reverted

# ============================================================
# CRON
# ============================================================
CRON_INTERVAL = int(os.environ.get("CRON_INTERVAL", "60"))
cron_enabled = os.environ.get("CRON_ENABLED", "true").lower() == "true"

def has_ready_prompts():
    try:
        _, gc = get_google_services()
        for c_name, c_cfg in COUNTRY_CONFIG.items():
            prompts = get_prompts_for_country(gc, c_cfg, limit=1)
            if prompts:
                return c_name
    except Exception as e:
        print(f"  [{SERVER_ID}] Cron check error: {e}")
    return None

def cron_loop():
    global is_processing, last_activity_time
    print(f"[{SERVER_ID}] Cron started (interval={CRON_INTERVAL}s)")
    while True:
        time.sleep(CRON_INTERVAL)
        try:
            stale = time.time() - last_activity_time
            if is_processing and stale > WATCHDOG_TIMEOUT:
                print(f"[{SERVER_ID}] WATCHDOG: stuck for {stale:.0f}s, forcing reset!")
                send_telegram(f"WATCHDOG: processing stuck for {stale:.0f}s, forcing reset")
                is_processing = False

            if not is_processing:
                country = has_ready_prompts()
                if country:
                    print(f"[{SERVER_ID}] Cron: found READY prompts for {country}")
                    is_processing = True
                    last_activity_time = time.time()
                    threading.Thread(target=full_pipeline, args=(country,), daemon=True).start()

        except Exception as e:
            print(f"[{SERVER_ID}] Cron error: {e}")
            traceback.print_exc()

@app.on_event("startup")
def start_cron():
    print(f"[{SERVER_ID}] Server starting up...")
    try:
        recover_stuck_processing()
    except Exception as e:
        print(f"[{SERVER_ID}] Startup recovery error: {e}")

    if cron_enabled:
        t = threading.Thread(target=cron_loop, daemon=True)
        t.start()
        print(f"[{SERVER_ID}] Cron thread started (every {CRON_INTERVAL}s)")
    else:
        print(f"[{SERVER_ID}] Cron disabled")

# ============================================================
# ENDPOINTS
# ============================================================
@app.get("/")
def root():
    return {
        "status": "Video Editor Server (scaled mode)",
        "server_id": SERVER_ID,
        "processing": is_processing,
        "cron_enabled": cron_enabled,
        "cron_interval_s": CRON_INTERVAL,
        "max_concurrent": MAX_CONCURRENT,
        "countries": list(COUNTRY_CONFIG.keys()),
        "prompt_tab": COUNTRY_CONFIG.get("USA", {}).get("prompt_tab", ""),
    }

_prompt_cache = {"counts": {"ready": 0, "processing": 0, "done": 0, "error": 0, "total": 0}, "ts": 0}
_PROMPT_CACHE_TTL = 30

def _refresh_prompt_counts():
    now = time.time()
    if now - _prompt_cache["ts"] < _PROMPT_CACHE_TTL:
        return _prompt_cache["counts"]
    try:
        _, gc = get_google_services()
        c_cfg = COUNTRY_CONFIG.get("USA", {})
        counts = {"ready": 0, "processing": 0, "done": 0, "error": 0, "total": 0}
        if c_cfg:
            ws = _get_worksheet(gc, c_cfg["prompt_tab"])
            all_rows = _sheets_retry(lambda: ws.get_all_values())
            if all_rows:
                headers = [h.strip().lower() for h in all_rows[0]]
                status_idx = headers.index("status") if "status" in headers else -1
                prompt_idx = headers.index("prompt") if "prompt" in headers else -1
                for r in all_rows[1:]:
                    has_prompt = prompt_idx >= 0 and prompt_idx < len(r) and r[prompt_idx].strip()
                    if not has_prompt:
                        continue
                    counts["total"] += 1
                    st = r[status_idx].strip().lower() if status_idx >= 0 and status_idx < len(r) else ""
                    if st in ("ready", "processing", "done", "error"):
                        counts[st] += 1
        _prompt_cache["counts"] = counts
        _prompt_cache["ts"] = now
    except Exception:
        pass
    return _prompt_cache["counts"]

@app.get("/status")
def status():
    stale = round(time.time() - last_activity_time)
    prompt_counts = _refresh_prompt_counts()
    return {
        "server_id": SERVER_ID,
        "processing": is_processing,
        "cron_enabled": cron_enabled,
        "countries": list(COUNTRY_CONFIG.keys()),
        "last_activity_seconds_ago": stale,
        "max_concurrent": MAX_CONCURRENT,
        "prompt_tab": COUNTRY_CONFIG.get("USA", {}).get("prompt_tab", ""),
        "prompts": prompt_counts,
    }

@app.post("/reset")
def reset_flags():
    global is_processing, last_activity_time
    was = is_processing
    is_processing = False
    last_activity_time = time.time()

    reverted = 0
    try:
        _, gc = get_google_services()
        for c_name, c_cfg in COUNTRY_CONFIG.items():
            ss = gc.open_by_key(PROMPTS_SHEET_ID)
            ws = ss.worksheet(c_cfg["prompt_tab"])
            all_rows = ws.get_all_values()
            if not all_rows:
                continue
            headers = [h.strip().lower() for h in all_rows[0]]
            if "status" not in headers:
                continue
            status_col = headers.index("status") + 1
            for idx, row in enumerate(all_rows[1:], start=2):
                st = row[status_col - 1].strip().lower() if status_col - 1 < len(row) else ""
                if st == "processing":
                    ws.update_cell(idx, status_col, "READY")
                    reverted += 1
    except Exception as e:
        print(f"[{SERVER_ID}] Reset revert error: {e}")

    send_telegram(f"Manual reset: processing {was}->False, reverted {reverted}")
    return {"status": "ok", "server_id": SERVER_ID, "was_processing": was, "reverted_to_ready": reverted}

@app.post("/process")
def trigger_process(background_tasks: BackgroundTasks, country: str = "USA"):
    global is_processing
    if is_processing:
        return JSONResponse({"status": "already_running", "server_id": SERVER_ID, "message": "Processing already in progress"})
    c = country.upper()
    if c not in COUNTRY_CONFIG:
        return JSONResponse({"status": "error", "message": f"Unknown country: {c}"}, status_code=400)
    is_processing = True
    last_activity_time = time.time()
    background_tasks.add_task(full_pipeline, c)
    return {"status": "started", "server_id": SERVER_ID, "country": c}

@app.get("/debug")
def debug_state():
    try:
        drive_service, gc = get_google_services()
        result = {"server_id": SERVER_ID}

        for c_name, c_cfg in COUNTRY_CONFIG.items():
            country_info = {}

            results_id = c_cfg["results_folder_id"]
            q = f"'{results_id}' in parents and trashed = false and mimeType='application/vnd.google-apps.folder'"
            date_folders = drive_service.files().list(
                q=q, fields="files(id,name)",
                supportsAllDrives=True, includeItemsFromAllDrives=True
            ).execute().get("files", [])
            folders_info = {}
            for df in sorted(date_folders, key=lambda x: x["name"]):
                sub_q = f"'{df['id']}' in parents and trashed = false and mimeType='application/vnd.google-apps.folder'"
                sub_resp = drive_service.files().list(
                    q=sub_q, fields="files(id,name)",
                    supportsAllDrives=True, includeItemsFromAllDrives=True
                ).execute().get("files", [])
                sub_info = {}
                for sf in sub_resp:
                    inner_files = drive_list_all_files(drive_service, sf["id"])
                    sub_info[sf["name"]] = len(inner_files)
                folders_info[df["name"]] = sub_info
            country_info["results"] = folders_info

            try:
                ss = gc.open_by_key(PROMPTS_SHEET_ID)
                ws = ss.worksheet(c_cfg["prompt_tab"])
                all_rows = ws.get_all_values()
                headers = [h.strip().lower() for h in all_rows[0]] if all_rows else []
                status_idx = headers.index("status") if "status" in headers else -1
                prompt_idx = headers.index("prompt") if "prompt" in headers else -1
                counts = {"ready": 0, "processing": 0, "done": 0, "error": 0, "other": 0}
                for r in all_rows[1:]:
                    has_prompt = prompt_idx >= 0 and prompt_idx < len(r) and r[prompt_idx].strip()
                    if not has_prompt:
                        continue
                    st = r[status_idx].strip().lower() if status_idx >= 0 and status_idx < len(r) else ""
                    if st in counts:
                        counts[st] += 1
                    else:
                        counts["other"] += 1
                country_info["prompts"] = counts
            except Exception as e:
                country_info["prompts"] = {"error": str(e)}

            try:
                master_ws = get_or_create_master_tab(gc, c_cfg["master_tab"])
                master_rows = master_ws.get_all_values()
                country_info["master_sheet_rows"] = len(master_rows) - 1
            except Exception as e:
                country_info["master_sheet_rows"] = {"error": str(e)}

            result[c_name] = country_info

        return result
    except Exception as e:
        return {"error": str(e), "server_id": SERVER_ID}

@app.post("/fix-sheet")
def fix_sheet(country: str = "USA", date: str = None):
    c = country.upper()
    if c not in COUNTRY_CONFIG:
        return JSONResponse({"status": "error", "message": f"Unknown country: {c}"}, status_code=400)
    if not date:
        date = datetime.datetime.now().strftime("%d.%m")

    c_cfg = COUNTRY_CONFIG[c]
    try:
        drive_svc, gc = get_google_services()
        results_folder_id = c_cfg["results_folder_id"]

        q = f"'{results_folder_id}' in parents and trashed=false and mimeType='application/vnd.google-apps.folder' and name='{date}'"
        date_folders = drive_svc.files().list(q=q, fields="files(id)", supportsAllDrives=True, includeItemsFromAllDrives=True).execute().get("files", [])
        if not date_folders:
            return {"status": "error", "message": f"No folder {date} in RESULTS/{c}"}
        date_folder_id = date_folders[0]["id"]

        sub_q = f"'{date_folder_id}' in parents and trashed=false and mimeType='application/vnd.google-apps.folder'"
        subs = drive_svc.files().list(q=sub_q, fields="files(id,name)", supportsAllDrives=True, includeItemsFromAllDrives=True).execute().get("files", [])
        edit_folder_id = next((f["id"] for f in subs if f["name"] == "edited"), None)
        if not edit_folder_id:
            return {"status": "error", "message": "No edited/ folder found"}

        edited_files = drive_list_all_files(drive_svc, edit_folder_id)

        master_ws = get_or_create_master_tab(gc, c_cfg["master_tab"])
        existing_rows = master_ws.get_all_values()
        existing_names = set()
        if existing_rows:
            for row in existing_rows[1:]:
                if row:
                    existing_names.add(row[0].strip())

        added = 0
        skipped = 0

        for ef in sorted(edited_files, key=lambda x: x["name"]):
            ad_name = ef["name"].replace(".mp4", "")
            if ad_name in existing_names:
                skipped += 1
                continue

            file_id = ef["id"]
            drive_link, direct_link = make_drive_links(file_id)
            m = re.search(r"V(\d+)", ad_name)
            v_num = int(m.group(1)) if m else added + 1
            version = ((v_num - 1) // VIDEOS_PER_CAMPAIGN) + 1

            srv_match = re.search(r"_(S\d+)_", ad_name)
            srv_tag = srv_match.group(1) if srv_match else SERVER_ID

            for attempt in range(3):
                try:
                    master_ws.append_row(
                        [ad_name, drive_link, direct_link,
                         f"C{date}_{srv_tag}_{c}_{version:02d}",
                         f"adset{version}_{srv_tag}_{c}_{date}",
                         "", "", "",
                         "pending", "video", ""],
                        value_input_option="USER_ENTERED"
                    )
                    added += 1
                    break
                except Exception as se:
                    if attempt < 2:
                        time.sleep(5)
            time.sleep(1)

        msg = f"[{c}] fix-sheet {date}: added {added}, skipped {skipped}"
        send_telegram(msg)
        return {"status": "ok", "server_id": SERVER_ID, "added": added, "skipped": skipped, "total_edited": len(edited_files)}

    except Exception as e:
        traceback.print_exc()
        return {"status": "error", "message": str(e)}

@app.post("/clean-sheet")
def clean_sheet(country: str = "USA", keep_date: str = None):
    c = country.upper()
    if c not in COUNTRY_CONFIG:
        return JSONResponse({"status": "error", "message": f"Unknown country: {c}"}, status_code=400)
    if not keep_date:
        keep_date = datetime.datetime.now().strftime("%d.%m")

    c_cfg = COUNTRY_CONFIG[c]
    try:
        _, gc = get_google_services()
        ws = get_or_create_master_tab(gc, c_cfg["master_tab"])
        all_rows = ws.get_all_values()
        if len(all_rows) <= 1:
            return {"status": "ok", "kept": 0, "removed": 0}

        header = all_rows[0]
        rows_to_keep = [header]
        removed = 0
        for row in all_rows[1:]:
            ad_name = row[0].strip() if row else ""
            if ad_name.startswith(keep_date):
                rows_to_keep.append(row)
            else:
                removed += 1

        ws.clear()
        if rows_to_keep:
            max_cols = max(len(r) for r in rows_to_keep)
            normalized = [r + [""] * (max_cols - len(r)) for r in rows_to_keep]
            end_col = chr(ord("A") + max_cols - 1) if max_cols <= 26 else "Z"
            ws.update(f"A1:{end_col}{len(normalized)}", normalized, value_input_option="USER_ENTERED")

        kept = len(rows_to_keep) - 1
        msg = f"[{c}] clean-sheet: kept {kept} rows ({keep_date}), removed {removed}"
        send_telegram(msg)
        return {"status": "ok", "server_id": SERVER_ID, "kept": kept, "removed": removed}

    except Exception as e:
        traceback.print_exc()
        return {"status": "error", "message": str(e)}


@app.get("/check-sheet")
def check_sheet(country: str = "USA"):
    c = country.upper()
    if c not in COUNTRY_CONFIG:
        return {"error": f"Unknown country: {c}"}
    c_cfg = COUNTRY_CONFIG[c]
    try:
        _, gc = get_google_services()
        ss = gc.open_by_key(PROMPTS_SHEET_ID)
        ws = ss.worksheet(c_cfg["prompt_tab"])
        all_rows = ws.get_all_values()
        headers = all_rows[0] if all_rows else []
        rows_preview = []
        for i, row in enumerate(all_rows[1:6], start=2):
            rows_preview.append({"row": i, "values": row})
        return {"server_id": SERVER_ID, "headers": headers, "headers_lower": [h.strip().lower() for h in headers], "rows": rows_preview, "total_rows": len(all_rows) - 1}
    except Exception as e:
        return {"error": str(e)}

@app.post("/clear-prompts")
def clear_prompts(country: str = "USA"):
    c = country.upper()
    if c not in COUNTRY_CONFIG:
        return JSONResponse({"status": "error", "message": f"Unknown country: {c}"}, status_code=400)
    c_cfg = COUNTRY_CONFIG[c]
    try:
        _, gc = get_google_services()
        ss = gc.open_by_key(PROMPTS_SHEET_ID)
        ws = ss.worksheet(c_cfg["prompt_tab"])
        all_rows = ws.get_all_values()
        if len(all_rows) > 1:
            ws.resize(rows=1)
            ws.resize(rows=1000)
        return {"status": "ok", "server_id": SERVER_ID, "removed": len(all_rows) - 1}
    except Exception as e:
        return {"status": "error", "message": str(e)}

@app.post("/add-prompts")
def add_prompts(country: str = "USA", count: int = 2):
    c = country.upper()
    if c not in COUNTRY_CONFIG:
        return JSONResponse({"status": "error", "message": f"Unknown country: {c}"}, status_code=400)

    test_prompts = [
        "A 52 year old American White man AI developer in a charcoal suit seated on a bright TV morning show set in a plush cream armchair with the female host visible in profile to his right holding cue cards and a vase of sunflowers on the coffee table between them, he ignores the host and looks directly at camera with a grave expression. He says: I'm an AI developer and I know the host wants to keep this light but what AI does to your phone is not a light topic and every viewer needs to hear this right now. No special effects, no CGI, no holograms, no digital graphics, no text overlays, no post-production effects. Raw realistic footage only.",
        "A 58 year old American Black man AI developer in a navy blazer with a pocket square seated at a bright TV news desk beside the anchor with both their microphones visible and three monitors behind them showing phone scam statistics in red and white graphics, he turns from the anchor to camera with urgency. He says: The anchor just asked me if phones are safe and after 20 years of building AI my answer is no and here's why every viewer should be concerned right now. No special effects, no CGI, no holograms, no digital graphics, no text overlays, no post-production effects. Raw realistic footage only.",
        "A 50 year old American Indian man AI developer in a dark suit with no tie seated on a bright TV interview set on a grey velvet couch with a single floor lamp casting warm light on his face and a blurred abstract painting on the wall behind him, he sits on the edge of the couch and looks at camera seriously. He says: I develop AI and the show booked me for five minutes but what I have to say about your phone could fill five hours because that's how serious this problem is. No special effects, no CGI, no holograms, no digital graphics, no text overlays, no post-production effects. Raw realistic footage only.",
        "A 60 year old American White man AI developer in a grey three piece suit seated in a bright TV studio armchair with the host standing beside him holding a microphone toward him and studio cameras visible on dollies in the background, he pushes the microphone away slightly and looks directly at camera with determination. He says: I don't need a microphone in my face to say this clearly AI is accessing everything on your phone and I helped build the technology that makes it possible. No special effects, no CGI, no holograms, no digital graphics, no text overlays, no post-production effects. Raw realistic footage only.",
        "A 55 year old American Black man AI developer in a dark blazer seated at a bright TV roundtable set with four other empty chairs and name placards at each seat and overhead spotlights and a large screen behind showing YOUR PHONE AT RISK in bold white text, he looks at the empty chairs then at camera with concern. He says: I'm the only guest who showed up tonight because the other developers invited to this roundtable were too afraid to say on TV what AI does to your phone. No special effects, no CGI, no holograms, no digital graphics, no text overlays, no post-production effects. Raw realistic footage only.",
    ]

    title = "AI developer invited on TV to warn you"
    headline = "Protect your phone"
    primary_text = "An AI developer was invited on live TV to talk about innovation. Instead he used his airtime to warn viewers about what AI accesses on your phone every day. Your photos, messages and bank info are all exposed. Free protection takes 30 seconds."
    date_str = datetime.datetime.now().strftime("%d.%m")

    selected = test_prompts[:min(count, len(test_prompts))]

    try:
        _, gc = get_google_services()
        c_cfg = COUNTRY_CONFIG[c]
        ss = gc.open_by_key(PROMPTS_SHEET_ID)
        try:
            ws = ss.worksheet(c_cfg["prompt_tab"])
        except gspread.exceptions.WorksheetNotFound:
            ws = ss.add_worksheet(title=c_cfg["prompt_tab"], rows=1000, cols=20)
            ws.append_row(["date", "prompt", "headline meta", "primary text", "title of video", "STATUS"], value_input_option="USER_ENTERED")

        headers = [h.strip().lower() for h in ws.row_values(1)]
        col_map = {}
        for i, h in enumerate(headers):
            col_map[h] = i

        added = 0
        for p in selected:
            row_data = [""] * len(headers)
            if "date" in col_map:
                row_data[col_map["date"]] = date_str
            if "prompt" in col_map:
                row_data[col_map["prompt"]] = p
            if "headline meta" in col_map:
                row_data[col_map["headline meta"]] = headline
            if "primary text" in col_map:
                row_data[col_map["primary text"]] = primary_text
            if "title of video" in col_map:
                row_data[col_map["title of video"]] = title
            if "status" in col_map:
                row_data[col_map["status"]] = "READY"
            ws.append_row(row_data, value_input_option="USER_ENTERED")
            added += 1
            time.sleep(1)

        return {"status": "ok", "server_id": SERVER_ID, "added": added, "country": c}
    except Exception as e:
        return {"status": "error", "message": str(e), "traceback": traceback.format_exc()}

# ============================================================
# DISTRIBUTE PROMPTS: USA -> USA_1..USA_5
# ============================================================
@app.post("/distribute-prompts")
def distribute_prompts():
    """Read READY prompts from 'USA' tab and distribute them round-robin to USA_1..USA_5."""
    try:
        _, gc = get_google_services()
        ss = _sheets_retry(lambda: gc.open_by_key(PROMPTS_SHEET_ID))

        try:
            source_ws = _sheets_retry(lambda: ss.worksheet("USA"))
        except gspread.exceptions.WorksheetNotFound:
            return {"status": "error", "message": "Source tab 'USA' not found"}

        all_rows = _sheets_retry(lambda: source_ws.get_all_values())
        if len(all_rows) <= 1:
            return {"status": "ok", "distributed": 0, "message": "No rows in USA tab"}

        headers = [h.strip().lower() for h in all_rows[0]]
        if "status" not in headers:
            return {"status": "error", "message": "No STATUS column in USA tab"}

        status_col_idx = headers.index("status")
        ready_rows = []
        ready_row_indices = []
        for idx, row in enumerate(all_rows[1:], start=2):
            st = row[status_col_idx].strip().lower() if status_col_idx < len(row) else ""
            if st == "ready":
                ready_rows.append(row)
                ready_row_indices.append(idx)

        if not ready_rows:
            return {"status": "ok", "distributed": 0, "message": "No READY prompts in USA tab"}

        target_worksheets = []
        for tab_name in DISTRIBUTE_TABS:
            try:
                ws = ss.worksheet(tab_name)
            except gspread.exceptions.WorksheetNotFound:
                ws = ss.add_worksheet(title=tab_name, rows=1000, cols=20)
                ws.append_row(all_rows[0], value_input_option="USER_ENTERED")
                print(f"  [{SERVER_ID}] Created tab {tab_name}")
            target_worksheets.append(ws)

        batches = {tab: [] for tab in DISTRIBUTE_TABS}
        for i, row in enumerate(ready_rows):
            target_idx = i % len(DISTRIBUTE_TABS)
            tab_name = DISTRIBUTE_TABS[target_idx]
            row_copy = list(row)
            if status_col_idx < len(row_copy):
                row_copy[status_col_idx] = "READY"
            batches[tab_name].append(row_copy)

        counts = {}
        for tab_idx, tab_name in enumerate(DISTRIBUTE_TABS):
            rows_to_add = batches[tab_name]
            if not rows_to_add:
                counts[tab_name] = 0
                continue
            ws = target_worksheets[tab_idx]
            for attempt in range(3):
                try:
                    ws.append_rows(rows_to_add, value_input_option="USER_ENTERED")
                    counts[tab_name] = len(rows_to_add)
                    break
                except Exception as e:
                    if attempt < 2:
                        time.sleep(5)
                    else:
                        print(f"  [{SERVER_ID}] Failed to write batch to {tab_name}: {e}")
                        counts[tab_name] = 0
            time.sleep(1)

        status_col = status_col_idx + 1
        dist_cells = [gspread.Cell(row=idx, col=status_col, value="distributed") for idx in ready_row_indices]
        if dist_cells:
            for i in range(0, len(dist_cells), 500):
                source_ws.update_cells(dist_cells[i:i+500])
                time.sleep(1)

        total = sum(counts.values())
        send_telegram(f"Distributed {total} prompts: {counts}")
        return {"status": "ok", "server_id": SERVER_ID, "distributed": total, "per_tab": counts}

    except Exception as e:
        traceback.print_exc()
        return {"status": "error", "message": str(e)}


# ============================================================
# MARK READY / RETRY ERRORS (for control panel)
# ============================================================
@app.post("/mark-ready")
def mark_ready():
    """Scan the source 'USA' tab and set STATUS=READY on rows that have a prompt but no status."""
    try:
        _, gc = get_google_services()
        ss = _sheets_retry(lambda: gc.open_by_key(PROMPTS_SHEET_ID))
        try:
            ws = _sheets_retry(lambda: ss.worksheet("USA"))
        except gspread.exceptions.WorksheetNotFound:
            return {"status": "error", "message": "Tab 'USA' not found"}

        all_rows = _sheets_retry(lambda: ws.get_all_values())
        if len(all_rows) <= 1:
            return {"status": "ok", "marked": 0}

        headers = [h.strip().lower() for h in all_rows[0]]
        if "status" not in headers or "prompt" not in headers:
            return {"status": "error", "message": "Missing 'status' or 'prompt' column"}

        status_col = headers.index("status") + 1
        prompt_idx = headers.index("prompt")

        cells_to_update = []
        for idx, row in enumerate(all_rows[1:], start=2):
            prompt_val = row[prompt_idx].strip() if prompt_idx < len(row) else ""
            status_val = row[status_col - 1].strip().lower() if status_col - 1 < len(row) else ""
            if prompt_val and status_val not in ("ready", "done", "processing", "error"):
                cells_to_update.append(gspread.Cell(row=idx, col=status_col, value="READY"))

        marked = len(cells_to_update)
        if cells_to_update:
            _sheets_retry(lambda: ws.update_cells(cells_to_update))

        send_telegram(f"Marked {marked} prompts as READY in USA tab")
        return {"status": "ok", "server_id": SERVER_ID, "marked": marked}

    except Exception as e:
        traceback.print_exc()
        return {"status": "error", "message": str(e)}


@app.post("/retry-errors")
def retry_errors():
    """Scan USA_1..USA_5 tabs and reset 'error' prompts back to 'READY'."""
    try:
        _, gc = get_google_services()
        ss = _sheets_retry(lambda: gc.open_by_key(PROMPTS_SHEET_ID))

        total_retried = 0
        per_tab = {}

        for tab_name in DISTRIBUTE_TABS:
            try:
                ws = _sheets_retry(lambda: ss.worksheet(tab_name))
            except gspread.exceptions.WorksheetNotFound:
                per_tab[tab_name] = 0
                continue

            all_rows = _sheets_retry(lambda: ws.get_all_values())
            if len(all_rows) <= 1:
                per_tab[tab_name] = 0
                continue

            headers = [h.strip().lower() for h in all_rows[0]]
            if "status" not in headers:
                per_tab[tab_name] = 0
                continue

            status_col = headers.index("status") + 1
            cells_to_update = []

            for idx, row in enumerate(all_rows[1:], start=2):
                st = row[status_col - 1].strip().lower() if status_col - 1 < len(row) else ""
                if st == "error":
                    cells_to_update.append(gspread.Cell(row=idx, col=status_col, value="READY"))

            if cells_to_update:
                _sheets_retry(lambda: ws.update_cells(cells_to_update))

            per_tab[tab_name] = len(cells_to_update)
            total_retried += len(cells_to_update)

        send_telegram(f"Retried {total_retried} error prompts: {per_tab}")
        return {"status": "ok", "server_id": SERVER_ID, "retried": total_retried, "per_tab": per_tab}

    except Exception as e:
        traceback.print_exc()
        return {"status": "error", "message": str(e)}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", 8000)))
