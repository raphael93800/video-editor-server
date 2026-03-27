import os
import io
import datetime
import subprocess
import time
import threading
from collections import deque
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

# kie.ai (Veo 3.1) video generation
KIE_API_KEY         = os.environ.get("KIE_API_KEY",         "be30649990b3e9ee1d9644afeeddccf0")
KIE_API_BASE        = "https://api.kie.ai"
KIE_MODEL           = os.environ.get("KIE_MODEL",           "veo3_fast")
KIE_ASPECT_RATIO    = "9:16"

# Prompt sheet
PROMPTS_SHEET_ID    = os.environ.get("PROMPTS_SHEET_ID",    "13BxBpA1nZ8Vt-hlRDUqZcC6pTU0z-BMvwdtuZmnsy6g")
MAX_VIDEOS_PER_RUN  = int(os.environ.get("MAX_VIDEOS_PER_RUN", "5"))
MAX_CONCURRENT      = int(os.environ.get("MAX_CONCURRENT", "5"))

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
    "UK": {
        "results_folder_id": os.environ.get("RESULTS_UK_FOLDER_ID", "1SeDVgbd1Fo3SyYdxRbmP4QeIpuqyAoni"),
        "part2_file_id":     PART2_UK_FILE_ID,
        "master_tab":        os.environ.get("MASTER_TAB_UK", "To launch (UK)"),
        "prompt_tab":        os.environ.get("PROMPT_TAB_UK", "UK"),
    },
}

MASTER_SHEET_HEADERS = [
    "Ad_Name", "Drive_Share_Link", "Direct_Download_Link",
    "Campaign_Name", "AdSet_Name", "Primary_Text", "Headline", "Video_Prompt"
]

FONT_PATH = "/usr/share/fonts/truetype/custom/Montserrat-Bold.ttf"

is_generating = False
is_reediting = False
last_activity_time = time.time()
WATCHDOG_TIMEOUT = 1800

# ============================================================
# GOOGLE AUTH
# ============================================================
def get_google_services():
    creds_data = json.loads(base64.b64decode(GOOGLE_CREDS_JSON).decode("utf-8"))
    creds = service_account.Credentials.from_service_account_info(
        creds_data,
        scopes=[
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive"
        ]
    )
    drive_service = build("drive", "v3", credentials=creds)
    gc = gspread.authorize(creds)
    return drive_service, gc

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
    media = MediaFileUpload(local_path, mimetype="video/mp4", resumable=True)
    meta = {"name": filename, "parents": [parent_id]}
    created = drive_service.files().create(
        body=meta, media_body=media, fields="id", supportsAllDrives=True
    ).execute()
    return created["id"]

def make_drive_links(file_id):
    share_link = f"https://drive.google.com/file/d/{file_id}/view?usp=sharing"
    direct_download = f"https://drive.google.com/uc?export=download&id={file_id}"
    return share_link, direct_download

# ============================================================
# MASTER SHEET
# ============================================================
def get_or_create_master_tab(gc, tab_name):
    spreadsheet = gc.open_by_url(MASTER_SHEET_URL)
    try:
        ws = spreadsheet.worksheet(tab_name)
    except gspread.exceptions.WorksheetNotFound:
        ws = spreadsheet.add_worksheet(title=tab_name, rows=1000, cols=20)
        ws.append_row(MASTER_SHEET_HEADERS, value_input_option="USER_ENTERED")
        print(f"  Tab '{tab_name}' created in Master Sheet")
    return ws

# ============================================================
# TELEGRAM
# ============================================================
def send_telegram(msg):
    try:
        resp = http_requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            json={"chat_id": TELEGRAM_CHAT_ID, "text": msg},
            timeout=10
        )
        if not resp.ok:
            print(f"  Telegram error: {resp.status_code} — {resp.text[:200]}")
    except Exception as e:
        print(f"  Telegram send failed: {e}")

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

def get_speech_bounds(audio_path, video_duration):
    client = openai.OpenAI(api_key=OPENAI_API_KEY)
    with open(audio_path, "rb") as f:
        result = client.audio.transcriptions.create(
            model="whisper-1",
            file=f,
            response_format="verbose_json",
            timestamp_granularities=["word"]
        )
    words = result.words or []
    if words:
        start_t = max(0, words[0].start - 0.1)
        end_t = min(words[-1].end + 0.8, video_duration - 0.05)
    else:
        start_t, end_t = 0, video_duration
    return start_t, end_t

def generate_srt(audio_path):
    client = openai.OpenAI(api_key=OPENAI_API_KEY)
    with open(audio_path, "rb") as f:
        result = client.audio.transcriptions.create(
            model="whisper-1",
            file=f,
            response_format="verbose_json",
            timestamp_granularities=["word"]
        )
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
    import re as _re

    if os.path.exists(font_path):
        font_arg = f"fontfile={font_path.replace(':', chr(92) + ':')}"
    elif os.path.exists('/usr/share/fonts/truetype/liberation/LiberationSans-Bold.ttf'):
        font_arg = "fontfile=/usr/share/fonts/truetype/liberation/LiberationSans-Bold.ttf"
    else:
        font_arg = ""

    with open(srt_path, encoding="utf-8") as f:
        content = f.read()

    blocks = _re.split(r'\n\n+', content.strip())
    filters = []
    for block in blocks:
        lines = block.strip().split('\n')
        if len(lines) < 3:
            continue
        m = _re.match(r'(\d{2}:\d{2}:\d{2},\d{3}) --> (\d{2}:\d{2}:\d{2},\d{3})', lines[1])
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
# EDIT A SINGLE VIDEO (local file + in-memory metadata)
# ============================================================
def edit_single_video(local_hook_raw, local_part2_clean, metadata, country, vid_index):
    """
    Low-memory FFmpeg pipeline: only 1 full encode (the final render).
    All intermediate steps use stream copy or audio-only extraction.
    """
    pfx = f"/tmp/edit_{country}_{vid_index}"
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

        # 1. Get duration + extract audio for speech detection (no video encode)
        hook_duration = get_video_duration(local_hook_raw)
        print(f"  [{country}] Hook duration: {hook_duration:.2f}s")

        subprocess.run([
            "ffmpeg", "-y", "-i", local_hook_raw,
            "-vn", "-acodec", "pcm_s16le", "-ar", "16000", "-ac", "1",
            local_hook_audio
        ], check=True, capture_output=True)
        start_t, end_t = get_speech_bounds(local_hook_audio, hook_duration)
        print(f"  [{country}] Speech: {start_t:.2f}s -> {end_t:.2f}s")

        # Clean up audio early to free memory
        if os.path.exists(local_hook_audio):
            os.remove(local_hook_audio)

        # 2. Cut hook with re-encode (needed for clean concat with Part2)
        subprocess.run([
            "ffmpeg", "-y", "-i", local_hook_raw,
            "-ss", str(start_t), "-to", str(end_t),
            "-c:v", "libx264", "-preset", "ultrafast", "-crf", "23",
            "-r", "24", "-c:a", "aac", "-ar", "44100", "-ac", "2",
            "-movflags", "+faststart",
            local_hook_cut
        ], check=True, capture_output=True)

        # 3. Concat cut + Part2 using stream copy (zero RAM for video)
        with open(concat_list, "w") as cl:
            cl.write(f"file '{local_hook_cut}'\n")
            cl.write(f"file '{local_part2_clean}'\n")
        subprocess.run([
            "ffmpeg", "-y", "-f", "concat", "-safe", "0",
            "-i", concat_list, "-c", "copy",
            "-movflags", "+faststart",
            local_concat
        ], check=True, capture_output=True)

        # Clean up cut to free disk
        if os.path.exists(local_hook_cut):
            os.remove(local_hook_cut)

        # 4. Extract audio from concat for subtitles (no video processing)
        subprocess.run([
            "ffmpeg", "-y", "-i", local_concat,
            "-vn", "-acodec", "pcm_s16le", "-ar", "16000", "-ac", "1",
            local_audio
        ], check=True, capture_output=True)

        # 5. Generate SRT subtitles
        srt_content = generate_srt(local_audio)
        with open(local_srt, "w", encoding="utf-8") as sf:
            sf.write(srt_content)

        # Clean up full audio early
        if os.path.exists(local_audio):
            os.remove(local_audio)

        # 6. Build title + subtitle filters
        title_clean = video_title.replace("\n", " ").strip()
        words_t = title_clean.split()
        line1, line2 = "", ""
        mid = len(words_t) // 2
        for cut in range(mid, len(words_t)):
            candidate1 = " ".join(words_t[:cut])
            candidate2 = " ".join(words_t[cut:])
            if len(candidate1) <= 22:
                line1, line2 = candidate1, candidate2
                break
        if not line1:
            line1 = title_clean
            line2 = ""

        def esc(s):
            return (s.replace("\\", "\\\\")
                     .replace("'", "\u2019")
                     .replace(":", "\\:")
                     .replace("%", "%%"))

        if os.path.exists(FONT_PATH):
            font_path_esc = FONT_PATH.replace(':', '\\:')
            font_base = f"fontfile={font_path_esc}:fontcolor=black:fontsize=50"
        else:
            font_base = "fontcolor=black:fontsize=50"

        if line2:
            title_filter = (
                f"drawtext=text='{esc(line1)}':{font_base}:x=(w-tw)/2:y=780:"
                f"box=1:boxcolor=white@1.0:boxborderw=12:enable='lt(t,4)',"
                f"drawtext=text='{esc(line2)}':{font_base}:x=(w-tw)/2:y=848:"
                f"box=1:boxcolor=white@1.0:boxborderw=12:enable='lt(t,4)'"
            )
        else:
            title_filter = (
                f"drawtext=text='{esc(line1)}':{font_base}:x=(w-tw)/2:y=800:"
                f"box=1:boxcolor=white@1.0:boxborderw=12:enable='lt(t,4)'"
            )

        sub_filters = build_subtitle_drawtext_filters(local_srt, FONT_PATH)
        all_filters = [title_filter] + sub_filters
        vf_filter = ",".join(all_filters)

        # 7. Single final encode: burn title + subtitles (the only heavy encode)
        result = subprocess.run([
            "ffmpeg", "-y", "-i", local_concat,
            "-vf", vf_filter,
            "-c:v", "libx264", "-preset", "ultrafast", "-crf", "23",
            "-c:a", "aac",
            "-movflags", "+faststart",
            local_out
        ], capture_output=True, text=True)

        # Clean up concat before checking result to free disk/RAM
        if os.path.exists(local_concat):
            os.remove(local_concat)

        if result.returncode != 0:
            print(f"  [{country}] Overlay stderr: {result.stderr[-500:]}")
            raise Exception(f"Final encode failed (exit {result.returncode})")

        if not os.path.exists(local_out) or os.path.getsize(local_out) < 10000:
            raise Exception(f"Output file missing or too small")

        final_duration = get_video_duration(local_out)
        print(f"  [{country}] Edited video: {final_duration:.2f}s")
        return local_out

    finally:
        for p in temps:
            try:
                if os.path.exists(p):
                    os.remove(p)
            except:
                pass

# ============================================================
# KIE.AI (VEO 3.1) — VIDEO GENERATION API
# ============================================================
KIE_HEADERS = {
    "Authorization": f"Bearer {KIE_API_KEY}",
    "Content-Type": "application/json",
}

def kie_generate_video(prompt: str) -> str:
    resp = http_requests.post(
        f"{KIE_API_BASE}/api/v1/veo/generate",
        headers=KIE_HEADERS,
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
    print(f"  kie.ai task created: {task_id}")
    return task_id

def kie_poll_video(task_id: str, max_wait=600, interval=15) -> str:
    elapsed = 0
    while elapsed < max_wait:
        time.sleep(interval)
        elapsed += interval
        resp = http_requests.get(
            f"{KIE_API_BASE}/api/v1/veo/record-info",
            headers=KIE_HEADERS,
            params={"taskId": task_id},
            timeout=30,
        )
        data = resp.json()
        if data.get("code") != 200:
            print(f"  kie.ai poll error: {data.get('msg', '')} — retrying...")
            continue

        flag = data["data"].get("successFlag", 0)
        if flag == 1:
            urls = data["data"].get("response", {}).get("resultUrls", [])
            if urls:
                print(f"  kie.ai video ready: {urls[0][:80]}...")
                return urls[0]
            raise Exception("kie.ai returned success but no resultUrls")
        elif flag in (2, 3):
            err = data["data"].get("errorMessage", "unknown error")
            raise Exception(f"kie.ai generation failed (flag={flag}): {err}")

        print(f"  kie.ai generating... ({elapsed}s / {max_wait}s)")

    raise Exception(f"kie.ai timeout after {max_wait}s for task {task_id}")

def kie_download_video(url: str, local_path: str):
    resp = http_requests.get(url, stream=True, timeout=120)
    resp.raise_for_status()
    with open(local_path, "wb") as f:
        for chunk in resp.iter_content(chunk_size=8192):
            f.write(chunk)
    print(f"  kie.ai video downloaded: {local_path}")

# ============================================================
# PROMPT SHEET — read prompts per country
# ============================================================
PROMPT_SHEET_HEADERS = [
    "Date", "prompt", "headline meta", "primary text", "title of video", "STATUS"
]

def get_prompts_for_country(gc, country_cfg, limit=None):
    if limit is None:
        limit = MAX_VIDEOS_PER_RUN

    prompt_tab = country_cfg["prompt_tab"]
    spreadsheet = gc.open_by_key(PROMPTS_SHEET_ID)
    try:
        ws = spreadsheet.worksheet(prompt_tab)
    except gspread.exceptions.WorksheetNotFound:
        ws = spreadsheet.add_worksheet(title=prompt_tab, rows=1000, cols=20)
        ws.append_row(PROMPT_SHEET_HEADERS, value_input_option="USER_ENTERED")
        print(f"  Created prompt tab '{prompt_tab}'")
        return []

    all_rows = ws.get_all_values()
    if len(all_rows) <= 1:
        return []

    headers = [h.strip().lower() for h in all_rows[0]]
    prompts = []

    for idx, row in enumerate(all_rows[1:], start=2):
        row_dict = {}
        for h_idx, h in enumerate(headers):
            row_dict[h] = row[h_idx].strip() if h_idx < len(row) else ""

        status = row_dict.get("status", "").lower()
        if status == "done":
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

def mark_prompt_done(gc, country_cfg, row_index):
    prompt_tab = country_cfg["prompt_tab"]
    spreadsheet = gc.open_by_key(PROMPTS_SHEET_ID)
    ws = spreadsheet.worksheet(prompt_tab)
    headers = [h.strip().lower() for h in ws.row_values(1)]
    if "status" in headers:
        col = headers.index("status") + 1
        ws.update_cell(row_index, col, "done")

# ============================================================
# CONTINUOUS PIPELINE — pool of N concurrent generations,
# threaded editing, auto-backfill from prompt queue
# ============================================================
sheet_lock = threading.Lock()
edit_semaphore = threading.Semaphore(1)

def process_ready_video_thread(task, drive_service, c_name, c_cfg,
                               local_part2_clean, edited_folder_id, original_folder_id,
                               date_du_jour, counters):
    """
    Thread target: download, upload original, edit, upload edited, log, mark done.
    Uses its own gspread client to avoid thread-safety issues.
    `counters` is a dict with a lock for thread-safe success/error counting.
    """
    p_data = task["prompt_data"]
    vid_index = task["vid_index"]
    video_url = task["video_url"]
    row_index = p_data["row_index"]
    prompt_text = p_data["prompt"]
    nom_final = f"{date_du_jour}_V{vid_index}.mp4"

    local_hook_raw = f"/tmp/hook_raw_{c_name}_{vid_index}.mp4"
    local_edited = None

    try:
        kie_download_video(video_url, local_hook_raw)

        orig_filename = f"veo_{datetime.datetime.now().strftime('%H%M%S')}_{os.urandom(3).hex()}.mp4"
        drive_upload_video(drive_service, local_hook_raw, original_folder_id, orig_filename)
        print(f"  [{c_name}] Original uploaded: {orig_filename}")

        # Mark done NOW so we never re-generate (and re-pay) this video
        with sheet_lock:
            _, gc_mark = get_google_services()
            mark_prompt_done(gc_mark, c_cfg, row_index)
        print(f"  [{c_name}] V{vid_index} marked done (row {row_index})")

        # Limit concurrent FFmpeg edits to avoid OOM on Render
        print(f"  [{c_name}] V{vid_index} waiting for edit slot...")
        edit_semaphore.acquire()
        try:
            print(f"  [{c_name}] V{vid_index} editing...")
            metadata = {
                "title": p_data["title_of_video"] or DEFAULT_TITLE,
                "headline": p_data["headline_meta"],
                "primary_text": p_data["primary_text"],
                "prompt": prompt_text,
            }
            local_edited = edit_single_video(
                local_hook_raw, local_part2_clean, metadata, c_name, vid_index
            )
        finally:
            edit_semaphore.release()

        if not local_edited or not os.path.exists(local_edited) or os.path.getsize(local_edited) < 10000:
            raise Exception(f"Edited file missing or too small ({os.path.getsize(local_edited) if local_edited and os.path.exists(local_edited) else 0} bytes)")

        out_id = drive_upload_video(drive_service, local_edited, edited_folder_id, nom_final)
        print(f"  [{c_name}] Edited uploaded: {nom_final}")

        version = ((vid_index - 1) // VIDEOS_PER_CAMPAIGN) + 1
        campaign_name = f"C{date_du_jour}_{c_name}_{version:02d}"
        adset_name    = f"adset{version}_{c_name}_{date_du_jour}"
        drive_link, direct_link = make_drive_links(out_id)

        with sheet_lock:
            _, gc_thread = get_google_services()
            master_ws = get_or_create_master_tab(gc_thread, c_cfg["master_tab"])
            master_ws.append_row(
                [
                    nom_final.replace(".mp4", ""),
                    drive_link, direct_link,
                    campaign_name, adset_name,
                    p_data["primary_text"],
                    p_data["headline_meta"],
                    prompt_text,
                ],
                value_input_option="USER_ENTERED"
            )

        print(f"  [{c_name}] V{vid_index} complete (row {row_index})")
        global last_activity_time
        last_activity_time = time.time()
        with counters["lock"]:
            counters["success"] += 1
        task["status"] = "done"

    except Exception as e:
        print(f"  [{c_name}] ERROR editing V{vid_index}: {e}")
        send_telegram(f"[{c_name}] Error V{vid_index} (row {row_index}): {str(e)[:150]}")
        with counters["lock"]:
            counters["errors"] += 1
        task["status"] = "failed"

    finally:
        for tmp in [local_hook_raw]:
            try:
                if os.path.exists(tmp):
                    os.remove(tmp)
            except:
                pass
        if local_edited:
            try:
                if os.path.exists(local_edited):
                    os.remove(local_edited)
            except:
                pass


def generate_and_process(country=None):
    """
    Continuous pipeline per country:
    - Load ALL pending prompts into a queue
    - Maintain MAX_CONCURRENT kie.ai generations in flight at all times
    - As soon as one finishes, edit it in a background thread and immediately
      submit the next prompt from the queue
    - No batch boundaries, no dead time
    """
    global is_generating
    date_du_jour = datetime.datetime.now().strftime("%d.%m")

    countries_to_process = {}
    if country:
        c = country.upper()
        if c not in COUNTRY_CONFIG:
            print(f"Unknown country: {c}. Available: {list(COUNTRY_CONFIG.keys())}")
            is_generating = False
            return
        countries_to_process = {c: COUNTRY_CONFIG[c]}
    else:
        countries_to_process = COUNTRY_CONFIG

    try:
        drive_service, gc = get_google_services()
        counters = {"success": 0, "errors": 0, "lock": threading.Lock()}

        for c_name, c_cfg in countries_to_process.items():
            print(f"\n{'='*50}")
            print(f"CONTINUOUS PIPELINE: {c_name}")
            print(f"{'='*50}")

            results_folder_id = c_cfg["results_folder_id"]
            part2_file_id = c_cfg["part2_file_id"]

            today_folder_id    = drive_get_or_create_folder(drive_service, results_folder_id, date_du_jour)
            edited_folder_id   = drive_get_or_create_folder(drive_service, today_folder_id, "edited")
            original_folder_id = drive_get_or_create_folder(drive_service, today_folder_id, "original")

            local_part2       = f"/tmp/part2_{c_name}.mp4"
            local_part2_clean = f"/tmp/part2_clean_{c_name}.mp4"
            for p in [local_part2, local_part2_clean]:
                if os.path.exists(p):
                    os.remove(p)
            drive_download_file(drive_service, part2_file_id, local_part2)
            reencode_video(local_part2, local_part2_clean)
            print(f"  [{c_name}] Part2 ready")

            _, gc_fresh = get_google_services()
            all_prompts = get_prompts_for_country(gc_fresh, c_cfg, limit=9999)
            if not all_prompts:
                print(f"  [{c_name}] No pending prompts")
                continue

            prompt_queue = deque(all_prompts)
            total_prompts = len(prompt_queue)
            send_telegram(f"[{c_name}] Starting continuous pipeline: {total_prompts} video(s), {MAX_CONCURRENT} concurrent")

            master_ws = get_or_create_master_tab(gc_fresh, c_cfg["master_tab"])
            today_prefix = f"{date_du_jour}_V"
            existing_rows = master_ws.get_all_values()
            existing_today = len([r for r in existing_rows[1:] if r and r[0].strip().startswith(today_prefix)]) if len(existing_rows) > 1 else 0
            next_vid_index = existing_today + 1
            vid_index_lock = threading.Lock()

            active_tasks = {}
            edit_threads = []
            max_wait = 900
            interval = 15

            def get_next_vid_index():
                nonlocal next_vid_index
                with vid_index_lock:
                    idx = next_vid_index
                    next_vid_index += 1
                    return idx

            def submit_next():
                """Pop next prompt from queue and submit to kie.ai."""
                if not prompt_queue:
                    return
                p_data = prompt_queue.popleft()
                try:
                    task_id = kie_generate_video(p_data["prompt"])
                    vid_idx = get_next_vid_index()
                    active_tasks[task_id] = {
                        "task_id": task_id,
                        "prompt_data": p_data,
                        "vid_index": vid_idx,
                        "status": "generating",
                        "video_url": None,
                        "elapsed": 0,
                    }
                    print(f"  [{c_name}] Submitted V{vid_idx} (row {p_data['row_index']}), {len(active_tasks)} in flight, {len(prompt_queue)} queued")
                except Exception as e:
                    with counters["lock"]:
                        counters["errors"] += 1
                    print(f"  [{c_name}] Failed to submit row {p_data['row_index']}: {e}")
                    send_telegram(f"[{c_name}] Submit error row {p_data['row_index']}: {str(e)[:150]}")

            # Seed the pool
            while prompt_queue and len(active_tasks) < MAX_CONCURRENT:
                submit_next()

            if not active_tasks:
                print(f"  [{c_name}] All submissions failed")
                continue

            # Poll loop: runs until no active tasks
            while active_tasks:
                time.sleep(interval)

                for task_id in list(active_tasks.keys()):
                    task = active_tasks[task_id]
                    if task["status"] != "generating":
                        continue

                    task["elapsed"] += interval

                    try:
                        resp = http_requests.get(
                            f"{KIE_API_BASE}/api/v1/veo/record-info",
                            headers=KIE_HEADERS,
                            params={"taskId": task_id},
                            timeout=30,
                        )
                        data = resp.json()
                        if data.get("code") != 200:
                            print(f"  kie.ai poll error for {task_id}: {data.get('msg', '')}")
                            continue

                        flag = data["data"].get("successFlag", 0)

                        if flag == 1:
                            last_activity_time = time.time()
                            urls = data["data"].get("response", {}).get("resultUrls", [])
                            if not urls:
                                task["status"] = "failed"
                                with counters["lock"]:
                                    counters["errors"] += 1
                                print(f"  [{c_name}] V{task['vid_index']}: success but no URL")
                                del active_tasks[task_id]
                                submit_next()
                                continue

                            task["video_url"] = urls[0]
                            task["status"] = "editing"
                            print(f"  [{c_name}] V{task['vid_index']} ready! Launching edit thread...")

                            t = threading.Thread(
                                target=process_ready_video_thread,
                                args=(task, drive_service, c_name, c_cfg,
                                      local_part2_clean, edited_folder_id, original_folder_id,
                                      date_du_jour, counters),
                                daemon=True,
                            )
                            t.start()
                            edit_threads.append(t)

                            del active_tasks[task_id]
                            submit_next()

                        elif flag in (2, 3):
                            err = data["data"].get("errorMessage", "unknown")
                            task["status"] = "failed"
                            with counters["lock"]:
                                counters["errors"] += 1
                            print(f"  [{c_name}] V{task['vid_index']} generation failed: {err}")
                            send_telegram(f"[{c_name}] V{task['vid_index']} failed: {err[:100]}")
                            del active_tasks[task_id]
                            submit_next()

                    except Exception as e:
                        print(f"  [{c_name}] Poll error V{task['vid_index']}: {e}")

                    if task_id in active_tasks and task["status"] == "generating" and task["elapsed"] >= max_wait:
                        task["status"] = "failed"
                        with counters["lock"]:
                            counters["errors"] += 1
                        print(f"  [{c_name}] V{task['vid_index']} timed out after {max_wait}s")
                        send_telegram(f"[{c_name}] V{task['vid_index']} timed out")
                        del active_tasks[task_id]
                        submit_next()

                in_flight = len(active_tasks)
                if in_flight > 0:
                    print(f"  [{c_name}] {in_flight} generating, {len(prompt_queue)} queued, {len(edit_threads)} edit threads")

            # Wait for all edit threads to finish
            for t in edit_threads:
                t.join(timeout=300)

            done_count = counters["success"]
            err_count = counters["errors"]
            print(f"  [{c_name}] Country complete: {done_count} OK, {err_count} errors out of {total_prompts}")

            for p in [local_part2, local_part2_clean]:
                try:
                    if os.path.exists(p):
                        os.remove(p)
                except:
                    pass

        msg = (
            f"Pipeline complete!\n\n"
            f"Countries: {', '.join(countries_to_process.keys())}\n"
            f"{counters['success']} video(s) generated + edited\n"
        )
        if counters["errors"]:
            msg += f"{counters['errors']} error(s)\n"
        send_telegram(msg)

    except Exception as e:
        send_telegram(f"Critical error: {str(e)[:200]}")
        print(f"Critical error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        is_generating = False

# ============================================================
# CRON — check sheet every 60s, auto-launch if prompts found
# ============================================================
CRON_INTERVAL = int(os.environ.get("CRON_INTERVAL", "60"))
cron_enabled = os.environ.get("CRON_ENABLED", "true").lower() == "true"

def has_pending_prompts():
    """Quick check: are there any non-done prompts in any country tab?"""
    try:
        _, gc = get_google_services()
        for c_name, c_cfg in COUNTRY_CONFIG.items():
            prompts = get_prompts_for_country(gc, c_cfg, limit=1)
            if prompts:
                print(f"  Cron: found pending prompts in {c_name}")
                return True
    except Exception as e:
        print(f"  Cron check error: {e}")
        import traceback
        traceback.print_exc()
    return False

def has_unedited_originals():
    """Check if any country has originals in today's folder without matching edited files."""
    try:
        drive_service, _ = get_google_services()
        date_str = datetime.datetime.now().strftime("%d.%m")

        for c_name, c_cfg in COUNTRY_CONFIG.items():
            results_id = c_cfg["results_folder_id"]
            q = f"'{results_id}' in parents and trashed=false and mimeType='application/vnd.google-apps.folder' and name='{date_str}'"
            resp = drive_service.files().list(q=q, fields="files(id)", supportsAllDrives=True, includeItemsFromAllDrives=True).execute()
            date_folders = resp.get("files", [])
            if not date_folders:
                continue

            date_folder_id = date_folders[0]["id"]
            sub_q = f"'{date_folder_id}' in parents and trashed=false and mimeType='application/vnd.google-apps.folder'"
            sub_resp = drive_service.files().list(q=sub_q, fields="files(id,name)", supportsAllDrives=True, includeItemsFromAllDrives=True).execute()
            orig_id = None
            edit_id = None
            for f in sub_resp.get("files", []):
                if f["name"] == "original":
                    orig_id = f["id"]
                elif f["name"] == "edited":
                    edit_id = f["id"]

            if not orig_id or not edit_id:
                continue

            orig_q = f"'{orig_id}' in parents and trashed=false"
            orig_count = len(drive_service.files().list(q=orig_q, fields="files(id)", supportsAllDrives=True, includeItemsFromAllDrives=True, pageSize=500).execute().get("files", []))
            edit_q = f"'{edit_id}' in parents and trashed=false"
            edit_count = len(drive_service.files().list(q=edit_q, fields="files(id)", supportsAllDrives=True, includeItemsFromAllDrives=True, pageSize=500).execute().get("files", []))

            if orig_count > edit_count:
                print(f"  Cron: {c_name} {date_str} has {orig_count} originals but only {edit_count} edited")
                return c_name, date_str

    except Exception as e:
        print(f"  Cron unedited check error: {e}")
    return None


def cron_loop():
    """Background thread: auto-generate new videos AND auto-reedit failed ones."""
    global is_generating, is_reediting, last_activity_time
    print(f"Cron started (interval={CRON_INTERVAL}s)")
    while True:
        time.sleep(CRON_INTERVAL)
        try:
            stale = time.time() - last_activity_time
            if is_generating and stale > WATCHDOG_TIMEOUT:
                print(f"WATCHDOG: is_generating stuck for {stale:.0f}s, forcing reset!")
                send_telegram(f"WATCHDOG: pipeline stuck for {stale:.0f}s, forcing is_generating=False")
                is_generating = False
            if is_reediting and stale > WATCHDOG_TIMEOUT:
                print(f"WATCHDOG: is_reediting stuck for {stale:.0f}s, forcing reset!")
                send_telegram(f"WATCHDOG: reedit stuck for {stale:.0f}s, forcing is_reediting=False")
                is_reediting = False

            if not is_reediting:
                print("Cron: checking for unedited originals...")
                result = has_unedited_originals()
                if result:
                    c_name, date_str = result
                    print(f"Cron: found unedited for {c_name}/{date_str}, launching reedit in thread...")
                    is_reediting = True
                    threading.Thread(target=reedit_originals, args=(c_name, date_str), daemon=True).start()
                    continue
                else:
                    print("Cron: no unedited originals")
            else:
                print("Cron: reedit in progress, skipping")
                continue

            if not is_generating:
                print("Cron: checking for pending prompts...")
                if has_pending_prompts():
                    print("Cron: pending prompts found, launching pipeline in thread...")
                    is_generating = True
                    threading.Thread(target=generate_and_process, daemon=True).start()
            else:
                print("Cron: generation already running")

        except Exception as e:
            print(f"Cron error: {e}")
            import traceback
            traceback.print_exc()

@app.on_event("startup")
def start_cron():
    if cron_enabled:
        t = threading.Thread(target=cron_loop, daemon=True)
        t.start()
        print(f"Cron thread started (every {CRON_INTERVAL}s)")
    else:
        print("Cron disabled (CRON_ENABLED=false)")

# ============================================================
# ENDPOINTS
# ============================================================
@app.get("/")
def root():
    return {
        "status": "Video Editor Server running",
        "generating": is_generating,
        "cron_enabled": cron_enabled,
        "cron_interval_s": CRON_INTERVAL,
        "countries": list(COUNTRY_CONFIG.keys()),
    }

@app.get("/status")
def status():
    stale = round(time.time() - last_activity_time)
    return {
        "generating": is_generating,
        "reediting": is_reediting,
        "cron_enabled": cron_enabled,
        "countries": list(COUNTRY_CONFIG.keys()),
        "last_activity_seconds_ago": stale,
    }

@app.post("/generate")
def trigger_generate(background_tasks: BackgroundTasks, country: str = None):
    """
    Full pipeline: read prompts -> generate via kie.ai -> edit -> upload to RESULTS.
    - No param: all countries.
    - country=UK: UK only.
    Also triggered automatically by cron every 60s.
    """
    global is_generating
    if is_generating:
        return JSONResponse({
            "status": "already_running",
            "message": "A generation is already in progress",
        })
    if country and country.upper() not in COUNTRY_CONFIG:
        return JSONResponse(
            {"status": "error", "message": f"Unknown country: {country}. Available: {list(COUNTRY_CONFIG.keys())}"},
            status_code=400,
        )
    is_generating = True
    background_tasks.add_task(generate_and_process, country)
    target = country.upper() if country else "ALL"
    return JSONResponse({
        "status": "started",
        "country": target,
        "max_concurrent": MAX_CONCURRENT,
        "message": f"Continuous pipeline started ({target})",
    })

@app.post("/test-edit")
def test_edit_one(country: str = "USA", date: str = None):
    """Synchronously try to edit ONE original and return the exact error or success."""
    c = country.upper()
    if c not in COUNTRY_CONFIG:
        return {"error": f"Unknown country: {c}"}
    c_cfg = COUNTRY_CONFIG[c]
    if not date:
        date = datetime.datetime.now().strftime("%d.%m")

    try:
        drive_service, gc = get_google_services()
        results_id = c_cfg["results_folder_id"]

        q = f"'{results_id}' in parents and trashed=false and mimeType='application/vnd.google-apps.folder' and name='{date}'"
        resp = drive_service.files().list(q=q, fields="files(id)", supportsAllDrives=True, includeItemsFromAllDrives=True).execute()
        date_folders = resp.get("files", [])
        if not date_folders:
            return {"error": f"No folder {date}"}

        date_folder_id = date_folders[0]["id"]
        sub_q = f"'{date_folder_id}' in parents and trashed=false and mimeType='application/vnd.google-apps.folder'"
        sub_resp = drive_service.files().list(q=sub_q, fields="files(id,name)", supportsAllDrives=True, includeItemsFromAllDrives=True).execute()
        orig_id = None
        for f in sub_resp.get("files", []):
            if f["name"] == "original":
                orig_id = f["id"]

        if not orig_id:
            return {"error": "No original/ folder"}

        orig_q = f"'{orig_id}' in parents and trashed=false"
        originals = drive_service.files().list(q=orig_q, fields="files(id,name)", supportsAllDrives=True, includeItemsFromAllDrives=True, pageSize=1).execute().get("files", [])
        if not originals:
            return {"error": "No originals found"}

        test_file = originals[0]
        local_raw = f"/tmp/test_edit_raw.mp4"
        local_part2 = f"/tmp/test_part2.mp4"
        local_part2_clean = f"/tmp/test_part2_clean.mp4"

        drive_download_file(drive_service, test_file["id"], local_raw)
        raw_size = os.path.getsize(local_raw)
        raw_duration = get_video_duration(local_raw)
        has_video = has_video_stream(local_raw)

        drive_download_file(drive_service, c_cfg["part2_file_id"], local_part2)
        reencode_video(local_part2, local_part2_clean)

        metadata = {"title": DEFAULT_TITLE, "headline": "", "primary_text": "", "prompt": "test"}
        local_edited = edit_single_video(local_raw, local_part2_clean, metadata, c, 9999)

        edited_size = os.path.getsize(local_edited) if local_edited and os.path.exists(local_edited) else 0
        edited_duration = get_video_duration(local_edited) if edited_size > 0 else 0

        # Cleanup
        for p in [local_raw, local_part2, local_part2_clean, local_edited]:
            try:
                if p and os.path.exists(p):
                    os.remove(p)
            except:
                pass

        return {
            "status": "success",
            "original": test_file["name"],
            "raw_size_mb": round(raw_size / 1024 / 1024, 2),
            "raw_duration": round(raw_duration, 2),
            "has_video": has_video,
            "edited_size_mb": round(edited_size / 1024 / 1024, 2),
            "edited_duration": round(edited_duration, 2),
        }

    except Exception as e:
        import traceback
        return {"error": str(e), "traceback": traceback.format_exc()}


@app.post("/reedit")
def trigger_reedit(background_tasks: BackgroundTasks, country: str = "USA", date: str = None):
    """
    Re-edit originals that are in Drive but not yet edited.
    No kie.ai calls, no cost — just FFmpeg editing of existing files.
    Runs independently of generate pipeline.
    """
    global is_reediting
    if is_reediting:
        return JSONResponse({"status": "already_running", "message": "A reedit is already in progress"})
    c = country.upper()
    if c not in COUNTRY_CONFIG:
        return JSONResponse({"status": "error", "message": f"Unknown country: {c}"}, status_code=400)
    if not date:
        date = datetime.datetime.now().strftime("%d.%m")
    is_reediting = True
    background_tasks.add_task(reedit_originals, c, date)
    return JSONResponse({"status": "started", "message": f"Re-editing unedited originals for {c} / {date}"})


def reedit_originals(country, date_str):
    """
    Resilient reedit: loops until ALL originals are edited.
    On any error, waits 10s and retries from where it left off.
    Never gives up until original_count == edited_count.
    """
    global is_reediting
    c_cfg = COUNTRY_CONFIG[country]
    total_success = 0
    total_errors = 0

    # Download Part2 once
    local_part2 = f"/tmp/part2_reedit_{country}.mp4"
    local_part2_clean = f"/tmp/part2_reedit_clean_{country}.mp4"
    try:
        drive_svc, _ = get_google_services()
        for p in [local_part2, local_part2_clean]:
            if os.path.exists(p):
                os.remove(p)
        drive_download_file(drive_svc, c_cfg["part2_file_id"], local_part2)
        reencode_video(local_part2, local_part2_clean)
        print(f"  [{country}] Part2 ready for reedit")
    except Exception as e:
        send_telegram(f"[{country}] Reedit: can't prepare Part2: {str(e)[:150]}")
        is_reediting = False
        return

    try:
        while True:
            try:
                drive_svc, gc = get_google_services()
                results_folder_id = c_cfg["results_folder_id"]

                q = f"'{results_folder_id}' in parents and trashed=false and mimeType='application/vnd.google-apps.folder' and name='{date_str}'"
                date_folders = drive_svc.files().list(q=q, fields="files(id)", supportsAllDrives=True, includeItemsFromAllDrives=True).execute().get("files", [])
                if not date_folders:
                    print(f"[reedit] No folder {date_str}")
                    break
                date_folder_id = date_folders[0]["id"]

                sub_q = f"'{date_folder_id}' in parents and trashed=false and mimeType='application/vnd.google-apps.folder'"
                subs = drive_svc.files().list(q=sub_q, fields="files(id,name)", supportsAllDrives=True, includeItemsFromAllDrives=True).execute().get("files", [])
                orig_folder_id = next((f["id"] for f in subs if f["name"] == "original"), None)
                edit_folder_id = next((f["id"] for f in subs if f["name"] == "edited"), None)
                if not orig_folder_id or not edit_folder_id:
                    print(f"[reedit] Missing subfolders")
                    break

                orig_files = drive_svc.files().list(q=f"'{orig_folder_id}' in parents and trashed=false", fields="files(id,name)", supportsAllDrives=True, includeItemsFromAllDrives=True, pageSize=500).execute().get("files", [])
                edit_count = len(drive_svc.files().list(q=f"'{edit_folder_id}' in parents and trashed=false", fields="files(id)", supportsAllDrives=True, includeItemsFromAllDrives=True, pageSize=500).execute().get("files", []))

                originals_sorted = sorted(orig_files, key=lambda x: x["name"])
                remaining = originals_sorted[edit_count:]

                if not remaining:
                    print(f"  [{country}] Reedit done: {edit_count}/{len(orig_files)}")
                    send_telegram(f"[{country}] Reedit finished! {edit_count}/{len(orig_files)} edited ({total_success} this run, {total_errors} errors)")
                    break

                print(f"  [{country}] Reedit: {len(remaining)} left ({edit_count}/{len(orig_files)} done)")

                # Get metadata
                _, gc2 = get_google_services()
                try:
                    ss = gc2.open_by_key(PROMPTS_SHEET_ID)
                    ws = ss.worksheet(c_cfg["prompt_tab"])
                    all_rows = ws.get_all_values()
                    headers = [h.strip().lower() for h in all_rows[0]] if all_rows else []
                    done_prompts = []
                    for row in all_rows[1:]:
                        rd = {h: (row[hi].strip() if hi < len(row) else "") for hi, h in enumerate(headers)}
                        if rd.get("status", "").lower() == "done" and rd.get("prompt", ""):
                            done_prompts.append(rd)
                except:
                    done_prompts = []

                vid_index = edit_count + 1
                for i, orig in enumerate(remaining):
                    v_idx = vid_index + i
                    nom_final = f"{date_str}_V{v_idx}.mp4"
                    local_raw = f"/tmp/reedit_{country}_{v_idx}.mp4"
                    local_edited = None

                    if done_prompts:
                        meta = done_prompts[i % len(done_prompts)]
                        meta_src = {"title_of_video": meta.get("title of video", ""), "headline_meta": meta.get("headline meta", ""), "primary_text": meta.get("primary text", ""), "prompt": meta.get("prompt", "")}
                    else:
                        meta_src = {"title_of_video": DEFAULT_TITLE, "headline_meta": "", "primary_text": "", "prompt": ""}

                    try:
                        d_svc, _ = get_google_services()
                        drive_download_file(d_svc, orig["id"], local_raw)

                        edit_semaphore.acquire()
                        try:
                            metadata = {
                                "title": meta_src["title_of_video"] or DEFAULT_TITLE,
                                "headline": meta_src["headline_meta"],
                                "primary_text": meta_src["primary_text"],
                                "prompt": meta_src["prompt"],
                            }
                            local_edited = edit_single_video(local_raw, local_part2_clean, metadata, country, v_idx)
                        finally:
                            edit_semaphore.release()

                        if not local_edited or not os.path.exists(local_edited) or os.path.getsize(local_edited) < 10000:
                            raise Exception("Edited file missing or too small")

                        out_id = drive_upload_video(d_svc, local_edited, edit_folder_id, nom_final)
                        print(f"  [{country}] Reedit V{v_idx} done ({edit_count + i + 1}/{len(orig_files)})")

                        version = ((v_idx - 1) // VIDEOS_PER_CAMPAIGN) + 1
                        drive_link, direct_link = make_drive_links(out_id)

                        for attempt in range(3):
                            try:
                                _, gc_t = get_google_services()
                                mws = get_or_create_master_tab(gc_t, c_cfg["master_tab"])
                                mws.append_row(
                                    [nom_final.replace(".mp4", ""), drive_link, direct_link,
                                     f"C{date_str}_{country}_{version:02d}",
                                     f"adset{version}_{country}_{date_str}",
                                     meta_src["primary_text"], meta_src["headline_meta"], meta_src["prompt"]],
                                    value_input_option="USER_ENTERED"
                                )
                                break
                            except Exception as se:
                                print(f"  Sheet write retry {attempt+1}: {se}")
                                if attempt < 2:
                                    time.sleep(5)
                                else:
                                    print(f"  [{country}] SHEET WRITE FAILED for V{v_idx} after 3 attempts")
                                    send_telegram(f"[{country}] Sheet write FAILED for V{v_idx}: {str(se)[:100]}")

                        total_success += 1
                        last_activity_time = time.time()
                        if total_success % 10 == 0:
                            send_telegram(f"[{country}] Reedit progress: {edit_count + i + 1}/{len(orig_files)} edited ({total_success} this run)")

                    except Exception as e:
                        total_errors += 1
                        print(f"  [{country}] Reedit V{v_idx} ERROR: {e}")
                        import traceback
                        traceback.print_exc()

                    finally:
                        for tmp in [local_raw]:
                            try:
                                if os.path.exists(tmp): os.remove(tmp)
                            except: pass
                        if local_edited:
                            try:
                                if os.path.exists(local_edited): os.remove(local_edited)
                            except: pass
                        time.sleep(3)

                # After processing this batch, loop back to re-check how many are left

            except Exception as loop_err:
                print(f"  [{country}] Reedit loop error: {loop_err}")
                import traceback
                traceback.print_exc()
                time.sleep(10)

    finally:
        for p in [local_part2, local_part2_clean]:
            try:
                if os.path.exists(p): os.remove(p)
            except: pass
        is_reediting = False


@app.get("/debug")
def debug_state():
    """Diagnostic: list what's in RESULTS folders and prompt sheet status."""
    try:
        drive_service, gc = get_google_services()
        result = {}

        for c_name, c_cfg in COUNTRY_CONFIG.items():
            country_info = {}

            # List RESULTS/<country> subfolders and files
            results_id = c_cfg["results_folder_id"]
            q = f"'{results_id}' in parents and trashed = false"
            resp = drive_service.files().list(
                q=q, fields="files(id, name, mimeType)",
                supportsAllDrives=True, includeItemsFromAllDrives=True
            ).execute()
            date_folders = resp.get("files", [])
            folders_info = {}
            for df in date_folders:
                if df["mimeType"] == "application/vnd.google-apps.folder":
                    sub_q = f"'{df['id']}' in parents and trashed = false"
                    sub_resp = drive_service.files().list(
                        q=sub_q, fields="files(id, name, mimeType)",
                        supportsAllDrives=True, includeItemsFromAllDrives=True
                    ).execute()
                    sub_files = sub_resp.get("files", [])
                    sub_info = {}
                    for sf in sub_files:
                        if sf["mimeType"] == "application/vnd.google-apps.folder":
                            inner_q = f"'{sf['id']}' in parents and trashed = false"
                            inner_resp = drive_service.files().list(
                                q=inner_q, fields="files(id, name)",
                                supportsAllDrives=True, includeItemsFromAllDrives=True
                            ).execute()
                            inner_files = inner_resp.get("files", [])
                            sub_info[sf["name"]] = {
                                "count": len(inner_files),
                                "files": [f["name"] for f in sorted(inner_files, key=lambda x: x["name"])]
                            }
                    folders_info[df["name"]] = sub_info
            country_info["results_folders"] = folders_info

            # Prompt sheet status
            try:
                ss = gc.open_by_key(PROMPTS_SHEET_ID)
                ws = ss.worksheet(c_cfg["prompt_tab"])
                all_rows = ws.get_all_values()
                headers = [h.strip().lower() for h in all_rows[0]] if all_rows else []
                status_idx = headers.index("status") if "status" in headers else -1
                prompt_idx = headers.index("prompt") if "prompt" in headers else -1
                done = 0
                not_done = 0
                for r in all_rows[1:]:
                    has_prompt = prompt_idx >= 0 and prompt_idx < len(r) and r[prompt_idx].strip()
                    is_done = status_idx >= 0 and status_idx < len(r) and r[status_idx].strip().lower() == "done"
                    if has_prompt:
                        if is_done:
                            done += 1
                        else:
                            not_done += 1
                country_info["prompts"] = {"total": done + not_done, "done": done, "pending": not_done}
            except Exception as e:
                country_info["prompts"] = {"error": str(e)}

            # Master sheet row count
            try:
                master_ws = get_or_create_master_tab(gc, c_cfg["master_tab"])
                master_rows = master_ws.get_all_values()
                country_info["master_sheet_rows"] = len(master_rows) - 1
            except Exception as e:
                country_info["master_sheet_rows"] = {"error": str(e)}

            result[c_name] = country_info

        return result
    except Exception as e:
        return {"error": str(e)}

@app.post("/fix-sheet")
def fix_sheet(country: str = "USA", date: str = None):
    """
    Sync Master Sheet with Drive: add rows for edited videos that are missing from the sheet.
    Compares edited/ folder contents with Ad_Name column in Master Sheet.
    """
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

        edited_files = drive_svc.files().list(
            q=f"'{edit_folder_id}' in parents and trashed=false",
            fields="files(id,name)", supportsAllDrives=True, includeItemsFromAllDrives=True, pageSize=500
        ).execute().get("files", [])

        master_ws = get_or_create_master_tab(gc, c_cfg["master_tab"])
        existing_rows = master_ws.get_all_values()
        existing_names = set()
        if existing_rows:
            for row in existing_rows[1:]:
                if row:
                    existing_names.add(row[0].strip())

        # Get metadata from prompts sheet
        try:
            ss = gc.open_by_key(PROMPTS_SHEET_ID)
            ws = ss.worksheet(c_cfg["prompt_tab"])
            all_rows = ws.get_all_values()
            headers = [h.strip().lower() for h in all_rows[0]] if all_rows else []
            done_prompts = []
            for row in all_rows[1:]:
                rd = {h: (row[hi].strip() if hi < len(row) else "") for hi, h in enumerate(headers)}
                if rd.get("status", "").lower() == "done" and rd.get("prompt", ""):
                    done_prompts.append(rd)
        except:
            done_prompts = []

        added = 0
        skipped = 0
        edited_sorted = sorted(edited_files, key=lambda x: x["name"])

        for i, ef in enumerate(edited_sorted):
            ad_name = ef["name"].replace(".mp4", "")
            if ad_name in existing_names:
                skipped += 1
                continue

            file_id = ef["id"]
            drive_link, direct_link = make_drive_links(file_id)

            # Extract version number from filename (e.g. 27.03_V55 -> 55)
            try:
                v_num = int(ad_name.split("_V")[-1])
            except:
                v_num = i + 1
            version = ((v_num - 1) // VIDEOS_PER_CAMPAIGN) + 1

            if done_prompts:
                meta = done_prompts[i % len(done_prompts)]
                primary_text = meta.get("primary text", "")
                headline = meta.get("headline meta", "")
                prompt = meta.get("prompt", "")
            else:
                primary_text = ""
                headline = ""
                prompt = ""

            for attempt in range(3):
                try:
                    master_ws.append_row(
                        [ad_name, drive_link, direct_link,
                         f"C{date}_{c}_{version:02d}",
                         f"adset{version}_{c}_{date}",
                         primary_text, headline, prompt],
                        value_input_option="USER_ENTERED"
                    )
                    added += 1
                    break
                except Exception as se:
                    print(f"  fix-sheet retry {attempt+1}: {se}")
                    if attempt < 2:
                        time.sleep(5)

            time.sleep(1)

        msg = f"[{c}] fix-sheet {date}: added {added}, skipped {skipped} (already in sheet)"
        send_telegram(msg)
        return {"status": "ok", "added": added, "skipped": skipped, "total_edited": len(edited_files)}

    except Exception as e:
        import traceback
        traceback.print_exc()
        return {"status": "error", "message": str(e)}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", 8000)))
