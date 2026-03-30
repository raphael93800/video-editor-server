import os
import io
import datetime
import subprocess
import time
import threading
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

KIE_API_KEY         = "2c6b074d932083a007b99a398bbe829c"
KIE_API_BASE        = "https://api.kie.ai"
KIE_MODEL           = os.environ.get("KIE_MODEL",           "veo3_fast")
KIE_ASPECT_RATIO    = "9:16"

PROMPTS_SHEET_ID    = os.environ.get("PROMPTS_SHEET_ID",    "13BxBpA1nZ8Vt-hlRDUqZcC6pTU0z-BMvwdtuZmnsy6g")
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
}

MASTER_SHEET_HEADERS = [
    "Ad_Name", "Drive_Share_Link", "Direct_Download_Link",
    "Campaign_Name", "AdSet_Name", "Primary_Text", "Headline", "Video_Prompt"
]

FONT_PATH = "/usr/share/fonts/truetype/custom/Montserrat-Bold.ttf"

is_processing = False
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
            print(f"  Upload attempt {attempt+1} failed: {e}")
            if attempt < 2:
                time.sleep(3)
                drive_service, _ = get_google_services()
            else:
                raise

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
# EDIT A SINGLE VIDEO
# ============================================================
edit_semaphore = threading.Semaphore(1)

def edit_single_video(local_hook_raw, local_part2_clean, metadata, country, vid_index):
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

        hook_duration = get_video_duration(local_hook_raw)
        print(f"  [{country}] Hook duration: {hook_duration:.2f}s")

        subprocess.run([
            "ffmpeg", "-y", "-i", local_hook_raw,
            "-vn", "-acodec", "pcm_s16le", "-ar", "16000", "-ac", "1",
            local_hook_audio
        ], check=True, capture_output=True)
        start_t, end_t = get_speech_bounds(local_hook_audio, hook_duration)
        print(f"  [{country}] Speech: {start_t:.2f}s -> {end_t:.2f}s")

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
# KIE.AI — VIDEO GENERATION
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
        try:
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
        except http_requests.exceptions.RequestException as e:
            print(f"  kie.ai poll network error: {e}")

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
            print(f"  kie.ai video downloaded: {local_path}")
            return
        except Exception as e:
            print(f"  kie.ai download attempt {attempt+1} failed: {e}")
            if attempt < 2:
                time.sleep(5)
            else:
                raise

# ============================================================
# PROMPT SHEET
# ============================================================
def get_prompts_for_country(gc, country_cfg, limit=5):
    prompt_tab = country_cfg["prompt_tab"]
    spreadsheet = gc.open_by_key(PROMPTS_SHEET_ID)
    try:
        ws = spreadsheet.worksheet(prompt_tab)
    except gspread.exceptions.WorksheetNotFound:
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

def mark_prompt_status(gc, country_cfg, row_index, status):
    prompt_tab = country_cfg["prompt_tab"]
    spreadsheet = gc.open_by_key(PROMPTS_SHEET_ID)
    ws = spreadsheet.worksheet(prompt_tab)
    headers = [h.strip().lower() for h in ws.row_values(1)]
    if "status" in headers:
        col = headers.index("status") + 1
        ws.update_cell(row_index, col, status)

# ============================================================
# FULL PIPELINE: read prompts -> generate -> edit -> upload -> log
# ============================================================
sheet_lock = threading.Lock()

def full_pipeline(country="USA"):
    """
    For each READY prompt:
      1. Mark as 'processing'
      2. Generate video via Kie.ai
      3. Download video
      4. Upload original to RESULTS/<date>/original/
      5. Edit video (FFmpeg + Part2 + subtitles)
      6. Upload edited to RESULTS/<date>/edited/
      7. Log to Master Sheet
      8. Mark prompt as 'done'
    Processes up to MAX_CONCURRENT prompts at a time.
    """
    global is_processing, last_activity_time
    c_cfg = COUNTRY_CONFIG[country]
    date_str = datetime.datetime.now().strftime("%d.%m")

    total_success = 0
    total_errors = 0

    local_part2 = f"/tmp/part2_{country}.mp4"
    local_part2_clean = f"/tmp/part2_clean_{country}.mp4"

    try:
        drive_svc, gc = get_google_services()

        # Get READY prompts
        prompts = get_prompts_for_country(gc, c_cfg, limit=999)
        if not prompts:
            print(f"[{country}] No READY prompts found")
            return

        print(f"[{country}] Found {len(prompts)} READY prompt(s)")
        send_telegram(f"[{country}] Starting pipeline: {len(prompts)} video(s)")

        # Create RESULTS folders
        results_folder_id = c_cfg["results_folder_id"]
        date_folder_id = drive_get_or_create_folder(drive_svc, results_folder_id, date_str)
        original_folder_id = drive_get_or_create_folder(drive_svc, date_folder_id, "original")
        edited_folder_id = drive_get_or_create_folder(drive_svc, date_folder_id, "edited")

        # Get max V number from existing edited files
        existing_edited = drive_svc.files().list(
            q=f"'{edited_folder_id}' in parents and trashed=false and mimeType='video/mp4'",
            fields="files(id,name)", supportsAllDrives=True, includeItemsFromAllDrives=True, pageSize=500
        ).execute().get("files", [])
        edited_names = {f["name"] for f in existing_edited}

        max_v = 0
        for ef in existing_edited:
            try:
                v_num = int(ef["name"].split("_V")[-1].replace(".mp4", ""))
                if v_num > max_v:
                    max_v = v_num
            except:
                pass

        # Download Part2
        for p in [local_part2, local_part2_clean]:
            if os.path.exists(p):
                os.remove(p)
        drive_download_file(drive_svc, c_cfg["part2_file_id"], local_part2)
        reencode_video(local_part2, local_part2_clean)
        print(f"  [{country}] Part2 ready")

        vid_index = max_v + 1

        for p_data in prompts:
            row_index = p_data["row_index"]
            prompt_text = p_data["prompt"]
            nom_final = f"{date_str}_V{vid_index}.mp4"

            if nom_final in edited_names:
                print(f"  [{country}] Skipping V{vid_index}: already exists")
                vid_index += 1
                continue

            local_raw = f"/tmp/gen_{country}_{vid_index}.mp4"
            local_edited = None

            try:
                # Mark processing
                with sheet_lock:
                    _, gc_s = get_google_services()
                    mark_prompt_status(gc_s, c_cfg, row_index, "processing")
                print(f"  [{country}] V{vid_index} (row {row_index}): generating...")

                # Generate via Kie.ai
                task_id = kie_generate_video(prompt_text)
                video_url = kie_poll_video(task_id)
                last_activity_time = time.time()

                # Download
                kie_download_video(video_url, local_raw)

                # Upload original
                orig_name = f"veo_{datetime.datetime.now().strftime('%H%M%S')}_{os.urandom(3).hex()}.mp4"
                drive_upload_video(drive_svc, local_raw, original_folder_id, orig_name)
                print(f"  [{country}] V{vid_index}: original uploaded")

                # Edit
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

                # Upload edited
                out_id = drive_upload_video(drive_svc, local_edited, edited_folder_id, nom_final)
                print(f"  [{country}] V{vid_index}: edited uploaded")

                # Log to Master Sheet
                version = ((vid_index - 1) // VIDEOS_PER_CAMPAIGN) + 1
                drive_link, direct_link = make_drive_links(out_id)

                for attempt in range(3):
                    try:
                        _, gc_t = get_google_services()
                        mws = get_or_create_master_tab(gc_t, c_cfg["master_tab"])
                        mws.append_row(
                            [nom_final.replace(".mp4", ""), drive_link, direct_link,
                             f"C{date_str}_{country}_{version:02d}",
                             f"adset{version}_{country}_{date_str}",
                             p_data["primary_text"], p_data["headline_meta"], prompt_text],
                            value_input_option="USER_ENTERED"
                        )
                        break
                    except Exception as se:
                        print(f"  Sheet write retry {attempt+1}: {se}")
                        if attempt < 2:
                            time.sleep(5)
                        else:
                            send_telegram(f"[{country}] Sheet write FAILED V{vid_index}: {str(se)[:100]}")

                # Mark done
                with sheet_lock:
                    _, gc_d = get_google_services()
                    mark_prompt_status(gc_d, c_cfg, row_index, "done")

                total_success += 1
                last_activity_time = time.time()
                vid_index += 1

                send_telegram(f"[{country}] V{vid_index-1} done! ({total_success}/{len(prompts)})")

            except Exception as e:
                total_errors += 1
                print(f"  [{country}] V{vid_index} ERROR: {e}")
                import traceback
                traceback.print_exc()
                send_telegram(f"[{country}] V{vid_index} error: {str(e)[:200]}")
                try:
                    with sheet_lock:
                        _, gc_e = get_google_services()
                        mark_prompt_status(gc_e, c_cfg, row_index, "error")
                except:
                    pass
                vid_index += 1

            finally:
                for tmp in [local_raw]:
                    try:
                        if os.path.exists(tmp): os.remove(tmp)
                    except: pass
                if local_edited:
                    try:
                        if os.path.exists(local_edited): os.remove(local_edited)
                    except: pass
                time.sleep(2)

        send_telegram(f"[{country}] Pipeline done: {total_success} OK, {total_errors} errors out of {len(prompts)}")

    except Exception as e:
        print(f"[{country}] Pipeline critical error: {e}")
        import traceback
        traceback.print_exc()
        send_telegram(f"[{country}] Pipeline error: {str(e)[:200]}")

    finally:
        for p in [local_part2, local_part2_clean]:
            try:
                if os.path.exists(p): os.remove(p)
            except: pass
        is_processing = False

# ============================================================
# CRON — check for READY prompts every 30s
# ============================================================
CRON_INTERVAL = int(os.environ.get("CRON_INTERVAL", "30"))
cron_enabled = os.environ.get("CRON_ENABLED", "true").lower() == "true"

def has_ready_prompts():
    try:
        _, gc = get_google_services()
        for c_name, c_cfg in COUNTRY_CONFIG.items():
            prompts = get_prompts_for_country(gc, c_cfg, limit=1)
            if prompts:
                return c_name
    except Exception as e:
        print(f"  Cron check error: {e}")
    return None

def cron_loop():
    global is_processing, last_activity_time
    print(f"Cron started (interval={CRON_INTERVAL}s)")
    while True:
        time.sleep(CRON_INTERVAL)
        try:
            stale = time.time() - last_activity_time
            if is_processing and stale > WATCHDOG_TIMEOUT:
                print(f"WATCHDOG: is_processing stuck for {stale:.0f}s, forcing reset!")
                send_telegram(f"WATCHDOG: processing stuck for {stale:.0f}s, forcing reset")
                is_processing = False

            if not is_processing:
                country = has_ready_prompts()
                if country:
                    print(f"Cron: found READY prompts for {country}, launching pipeline...")
                    is_processing = True
                    last_activity_time = time.time()
                    threading.Thread(target=full_pipeline, args=(country,), daemon=True).start()

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
        "status": "Video Editor Server (all-in-one mode)",
        "processing": is_processing,
        "cron_enabled": cron_enabled,
        "cron_interval_s": CRON_INTERVAL,
        "countries": list(COUNTRY_CONFIG.keys()),
    }

@app.get("/status")
def status():
    stale = round(time.time() - last_activity_time)
    return {
        "processing": is_processing,
        "cron_enabled": cron_enabled,
        "countries": list(COUNTRY_CONFIG.keys()),
        "last_activity_seconds_ago": stale,
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
        print(f"Reset revert error: {e}")

    send_telegram(f"Manual reset: processing {was}->False, reverted {reverted} processing->READY")
    return {"status": "ok", "was_processing": was, "reverted_to_ready": reverted}

@app.post("/process")
def trigger_process(background_tasks: BackgroundTasks, country: str = "USA"):
    global is_processing
    if is_processing:
        return JSONResponse({"status": "already_running", "message": "Processing already in progress"})
    c = country.upper()
    if c not in COUNTRY_CONFIG:
        return JSONResponse({"status": "error", "message": f"Unknown country: {c}"}, status_code=400)
    is_processing = True
    last_activity_time = time.time()
    background_tasks.add_task(full_pipeline, c)
    return {"status": "started", "country": c}

@app.get("/debug")
def debug_state():
    try:
        drive_service, gc = get_google_services()
        result = {}

        for c_name, c_cfg in COUNTRY_CONFIG.items():
            country_info = {}

            # RESULTS folder contents
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
                    inner_q = f"'{sf['id']}' in parents and trashed = false and mimeType='video/mp4'"
                    inner_count = len(drive_service.files().list(
                        q=inner_q, fields="files(id)",
                        supportsAllDrives=True, includeItemsFromAllDrives=True, pageSize=500
                    ).execute().get("files", []))
                    sub_info[sf["name"]] = inner_count
                folders_info[df["name"]] = sub_info
            country_info["results"] = folders_info

            # Prompt sheet status
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

            # Master sheet rows
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
            q=f"'{edit_folder_id}' in parents and trashed=false and mimeType='video/mp4'",
            fields="files(id,name)", supportsAllDrives=True, includeItemsFromAllDrives=True, pageSize=500
        ).execute().get("files", [])

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
            try:
                v_num = int(ad_name.split("_V")[-1])
            except:
                v_num = added + 1
            version = ((v_num - 1) // VIDEOS_PER_CAMPAIGN) + 1

            for attempt in range(3):
                try:
                    master_ws.append_row(
                        [ad_name, drive_link, direct_link,
                         f"C{date}_{c}_{version:02d}",
                         f"adset{version}_{c}_{date}",
                         "", "", ""],
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
        return {"status": "ok", "added": added, "skipped": skipped, "total_edited": len(edited_files)}

    except Exception as e:
        import traceback
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
        return {"status": "ok", "kept": kept, "removed": removed}

    except Exception as e:
        import traceback
        traceback.print_exc()
        return {"status": "error", "message": str(e)}


@app.get("/check-sheet")
def check_sheet(country: str = "USA"):
    """Debug: show raw sheet headers and first few rows."""
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
        return {"headers": headers, "headers_lower": [h.strip().lower() for h in headers], "rows": rows_preview, "total_rows": len(all_rows) - 1}
    except Exception as e:
        return {"error": str(e)}

@app.post("/clear-prompts")
def clear_prompts(country: str = "USA"):
    """Remove all non-header rows from the prompt sheet tab."""
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
        return {"status": "ok", "removed": len(all_rows) - 1}
    except Exception as e:
        return {"status": "error", "message": str(e)}

@app.post("/add-prompts")
def add_prompts(country: str = "USA", count: int = 2):
    """Write test prompts to the sheet with status READY."""
    c = country.upper()
    if c not in COUNTRY_CONFIG:
        return JSONResponse({"status": "error", "message": f"Unknown country: {c}"}, status_code=400)

    test_prompts = [
        "A 52 year old American White man AI developer in a charcoal suit seated on a bright TV morning show set in a plush cream armchair with the female host visible in profile to his right holding cue cards and a vase of sunflowers on the coffee table between them, he ignores the host and looks directly at camera with a grave expression. He says: I'm an AI developer and I know the host wants to keep this light but what AI does to your phone is not a light topic and every viewer needs to hear this right now. No special effects, no CGI, no holograms, no digital graphics, no text overlays, no post-production effects. Raw realistic footage only.",
        "A 58 year old American Black man AI developer in a navy blazer with a pocket square seated at a bright TV news desk beside the anchor with both their microphones visible and three monitors behind them showing phone scam statistics in red and white graphics, he turns from the anchor to camera with urgency. He says: The anchor just asked me if phones are safe and after 20 years of building AI my answer is no and here's why every viewer should be concerned right now. No special effects, no CGI, no holograms, no digital graphics, no text overlays, no post-production effects. Raw realistic footage only.",
        "A 50 year old American Indian man AI developer in a dark suit with no tie seated on a bright TV interview set on a grey velvet couch with a single floor lamp casting warm light on his face and a blurred abstract painting on the wall behind him, he sits on the edge of the couch and looks at camera seriously. He says: I develop AI and the show booked me for five minutes but what I have to say about your phone could fill five hours because that's how serious this problem is. No special effects, no CGI, no holograms, no digital graphics, no text overlays, no post-production effects. Raw realistic footage only.",
        "A 60 year old American White man AI developer in a grey three piece suit seated in a bright TV studio armchair with the host standing beside him holding a microphone toward him and studio cameras visible on dollies in the background, he pushes the microphone away slightly and looks directly at camera with determination. He says: I don't need a microphone in my face to say this clearly AI is accessing everything on your phone and I helped build the technology that makes it possible. No special effects, no CGI, no holograms, no digital graphics, no text overlays, no post-production effects. Raw realistic footage only.",
        "A 55 year old American Black man AI developer in a dark blazer seated at a bright TV roundtable set with four other empty chairs and name placards at each seat and overhead spotlights and a large screen behind showing YOUR PHONE AT RISK in bold white text, he looks at the empty chairs then at camera with concern. He says: I'm the only guest who showed up tonight because the other developers invited to this roundtable were too afraid to say on TV what AI does to your phone. No special effects, no CGI, no holograms, no digital graphics, no text overlays, no post-production effects. Raw realistic footage only.",
        "A 48 year old American White man AI developer in a navy suit seated on a bright TV set on a high stool across from the host also on a stool with a transparent screen between them showing data points and a minimalist white studio backdrop, he looks through the screen at camera with urgency. He says: I build AI and the data on this screen between us shows exactly how many phones AI accessed today and the number is so high that even I can't believe it. No special effects, no CGI, no holograms, no digital graphics, no text overlays, no post-production effects. Raw realistic footage only.",
        "A 60 year old American Indian man AI developer in a charcoal suit with wire rimmed glasses seated in a bright TV studio guest chair with the host's hand visible resting on the armrest of the chair next to him and a warm amber studio backdrop and soft overhead lighting, he takes off his glasses and holds them in his hand and looks at camera with a grave expression. He says: I've been developing AI for 30 years and I took my glasses off because I want to look every viewer directly in the eye when I say that AI is reading your phone right now. No special effects, no CGI, no holograms, no digital graphics, no text overlays, no post-production effects. Raw realistic footage only.",
        "A 55 year old American Black man AI developer in a dark three piece suit seated at a bright TV news desk next to the female anchor with a breaking news ticker scrolling across the bottom of the monitors behind them and studio cameras on robotic arms pointed at both of them, he leans into his desk microphone and looks at camera seriously. He says: I develop AI and I'm sitting at this news desk tonight because what AI companies are doing with your phone data is breaking news that should have been reported months ago. No special effects, no CGI, no holograms, no digital graphics, no text overlays, no post-production effects. Raw realistic footage only.",
        "A 52 year old American White man AI developer in a grey blazer seated on a bright TV talk show set in a deep leather chair with a small stack of books on the side table and warm golden spotlights above and the host leaning forward from across the coffee table listening intently, he holds up his own phone and shows it to camera with urgency. He says: This phone in my hand right now has the same vulnerability as yours and I'm an AI developer telling you that AI can see everything on it as we speak. No special effects, no CGI, no holograms, no digital graphics, no text overlays, no post-production effects. Raw realistic footage only.",
        "A 58 year old American Black man AI developer in a navy suit seated in a bright TV studio on a curved white couch with two other empty guest spots beside him and a massive LED wall behind showing a phone being scanned by red lines and cool studio lighting, he gestures at the empty seats and looks at camera with a grave expression. He says: I build AI and the two developers who were supposed to sit beside me tonight both pulled out at the last minute because they don't want to admit on camera what AI does to your phone. No special effects, no CGI, no holograms, no digital graphics, no text overlays, no post-production effects. Raw realistic footage only.",
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

        return {"status": "ok", "added": added, "country": c}
    except Exception as e:
        import traceback
        return {"status": "error", "message": str(e), "traceback": traceback.format_exc()}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", 8000)))
