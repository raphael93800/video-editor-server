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

# kie.ai (Veo 3.1) video generation
KIE_API_KEY         = os.environ.get("KIE_API_KEY",         "be30649990b3e9ee1d9644afeeddccf0")
KIE_API_BASE        = "https://api.kie.ai"
KIE_MODEL           = os.environ.get("KIE_MODEL",           "veo3_fast")
KIE_ASPECT_RATIO    = "9:16"

# Prompt sheet
PROMPTS_SHEET_ID    = os.environ.get("PROMPTS_SHEET_ID",    "13BxBpA1nZ8Vt-hlRDUqZcC6pTU0z-BMvwdtuZmnsy6g")
MAX_VIDEOS_PER_RUN  = int(os.environ.get("MAX_VIDEOS_PER_RUN", "5"))

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
    Full FFmpeg editing pipeline on a local video file.
    Returns path to the edited output file, or raises on error.
    metadata: dict with keys title, headline, primary_text, prompt
    """
    pfx = f"/tmp/edit_{country}_{vid_index}"
    local_hook_clean = f"{pfx}_clean.mp4"
    local_hook_audio = f"{pfx}_audio.wav"
    local_hook_cut   = f"{pfx}_cut.mp4"
    local_concat     = f"{pfx}_concat.mp4"
    local_audio      = f"{pfx}_full_audio.wav"
    local_srt        = f"{pfx}_subs.srt"
    local_out        = f"{pfx}_final.mp4"
    concat_list      = f"{pfx}_list.txt"

    temps = [local_hook_clean, local_hook_audio, local_hook_cut,
             local_concat, local_audio, local_srt, concat_list]

    try:
        video_title   = metadata.get("title", "") or DEFAULT_TITLE
        video_prompt  = metadata.get("prompt", "")

        # 1. Re-encode hook
        reencode_video(local_hook_raw, local_hook_clean)
        if not has_video_stream(local_hook_clean):
            raise Exception("No video stream after re-encoding")
        hook_duration = get_video_duration(local_hook_clean)
        print(f"  [{country}] Hook re-encoded: {hook_duration:.2f}s")

        # 2. Speech detection
        subprocess.run([
            "ffmpeg", "-y", "-i", local_hook_clean,
            "-vn", "-acodec", "pcm_s16le", "-ar", "16000", "-ac", "1",
            local_hook_audio
        ], check=True, capture_output=True)
        start_t, end_t = get_speech_bounds(local_hook_audio, hook_duration)
        print(f"  [{country}] Speech: {start_t:.2f}s -> {end_t:.2f}s")

        # 3. Cut hook to speech bounds
        subprocess.run([
            "ffmpeg", "-y", "-i", local_hook_clean,
            "-ss", str(start_t), "-to", str(end_t),
            "-c:v", "libx264", "-preset", "fast", "-crf", "23",
            "-c:a", "aac", "-ar", "44100",
            local_hook_cut
        ], check=True, capture_output=True)
        if not has_video_stream(local_hook_cut):
            raise Exception("No video stream after cutting")

        # 4. Concat hook + Part2
        with open(concat_list, "w") as cl:
            cl.write(f"file '{local_hook_cut}'\n")
            cl.write(f"file '{local_part2_clean}'\n")
        subprocess.run([
            "ffmpeg", "-y", "-f", "concat", "-safe", "0",
            "-i", concat_list,
            "-c:v", "libx264", "-c:a", "aac",
            "-preset", "fast", "-crf", "23",
            "-movflags", "+faststart",
            local_concat
        ], check=True, capture_output=True)
        if not has_video_stream(local_concat):
            raise Exception("No video stream after concat")

        # 5. Extract full audio for subtitles
        subprocess.run([
            "ffmpeg", "-y", "-i", local_concat,
            "-vn", "-acodec", "pcm_s16le", "-ar", "16000", "-ac", "1",
            local_audio
        ], check=True, capture_output=True)

        # 6. Generate SRT subtitles
        srt_content = generate_srt(local_audio)
        with open(local_srt, "w", encoding="utf-8") as sf:
            sf.write(srt_content)

        # 7. Build title filter
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

        # 8. Burn title + subtitles
        sub_filters = build_subtitle_drawtext_filters(local_srt, FONT_PATH)
        all_filters = [title_filter] + sub_filters
        vf_filter = ",".join(all_filters)

        result = subprocess.run([
            "ffmpeg", "-y", "-i", local_concat,
            "-vf", vf_filter,
            "-c:v", "libx264", "-c:a", "aac",
            "-preset", "fast", "-crf", "23",
            "-movflags", "+faststart",
            local_out
        ], capture_output=True, text=True)

        if result.returncode != 0:
            print(f"  [{country}] Overlay error, using concat as fallback: {result.stderr[-300:]}")
            import shutil
            shutil.copy(local_concat, local_out)

        if not has_video_stream(local_out):
            raise Exception("No video stream in final output")

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
# GENERATE + EDIT PIPELINE (parallel generation, edit-on-ready)
# ============================================================
def process_ready_video(task, drive_service, gc, c_name, c_cfg,
                        local_part2_clean, edited_folder_id, original_folder_id,
                        master_ws, date_du_jour):
    """
    Download, upload original, edit, upload edited, log, mark done.
    Called as soon as a kie.ai task finishes.
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
        # 1) Download generated video
        kie_download_video(video_url, local_hook_raw)

        # 2) Upload original
        orig_filename = f"veo_{datetime.datetime.now().strftime('%H%M%S')}_{os.urandom(3).hex()}.mp4"
        drive_upload_video(drive_service, local_hook_raw, original_folder_id, orig_filename)
        print(f"  [{c_name}] Original uploaded: {orig_filename}")

        # 3) Edit
        metadata = {
            "title": p_data["title_of_video"] or DEFAULT_TITLE,
            "headline": p_data["headline_meta"],
            "primary_text": p_data["primary_text"],
            "prompt": prompt_text,
        }
        local_edited = edit_single_video(
            local_hook_raw, local_part2_clean, metadata, c_name, vid_index
        )

        # 4) Upload edited
        out_id = drive_upload_video(drive_service, local_edited, edited_folder_id, nom_final)
        print(f"  [{c_name}] Edited uploaded: {nom_final}")

        # 5) Log in Master Sheet
        version = ((vid_index - 1) // VIDEOS_PER_CAMPAIGN) + 1
        campaign_name = f"C{date_du_jour}_{c_name}_{version:02d}"
        adset_name    = f"adset{version}_{c_name}_{date_du_jour}"
        drive_link, direct_link = make_drive_links(out_id)
        master_ws.append_row(
            [
                nom_final.replace(".mp4", ""),
                drive_link,
                direct_link,
                campaign_name,
                adset_name,
                p_data["primary_text"],
                p_data["headline_meta"],
                prompt_text,
            ],
            value_input_option="USER_ENTERED"
        )

        # 6) Mark prompt done
        mark_prompt_done(gc, c_cfg, row_index)
        print(f"  [{c_name}] V{vid_index} complete (row {row_index})")
        return True

    except Exception as e:
        print(f"  [{c_name}] ERROR editing V{vid_index}: {e}")
        send_telegram(f"[{c_name}] Error editing V{vid_index} (row {row_index}): {str(e)[:150]}")
        return False

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
    Parallel pipeline per country:
    1. Read prompts from sheet
    2. Fire ALL kie.ai generation requests at once
    3. Poll all tasks every 15s
    4. As soon as a video is ready, download + edit + upload immediately
    5. Continue polling remaining tasks
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
        total_success = 0
        total_errors = 0

        for c_name, c_cfg in countries_to_process.items():
            print(f"\n{'='*50}")
            print(f"PARALLEL GENERATE + EDIT: {c_name}")
            print(f"{'='*50}")

            results_folder_id = c_cfg["results_folder_id"]
            part2_file_id = c_cfg["part2_file_id"]

            # Prepare Drive folders
            today_folder_id    = drive_get_or_create_folder(drive_service, results_folder_id, date_du_jour)
            edited_folder_id   = drive_get_or_create_folder(drive_service, today_folder_id, "edited")
            original_folder_id = drive_get_or_create_folder(drive_service, today_folder_id, "original")

            # Download and re-encode Part2 once
            local_part2       = f"/tmp/part2_{c_name}.mp4"
            local_part2_clean = f"/tmp/part2_clean_{c_name}.mp4"
            for p in [local_part2, local_part2_clean]:
                if os.path.exists(p):
                    os.remove(p)
            drive_download_file(drive_service, part2_file_id, local_part2)
            reencode_video(local_part2, local_part2_clean)
            print(f"  [{c_name}] Part2 ready")

            master_ws = get_or_create_master_tab(gc, c_cfg["master_tab"])
            batch_num = 0

            # ── BATCH LOOP: process all prompts in batches of MAX_VIDEOS_PER_RUN ──
            while True:
                batch_num += 1
                prompts = get_prompts_for_country(gc, c_cfg)
                if not prompts:
                    if batch_num == 1:
                        print(f"[{c_name}] No pending prompts")
                    else:
                        print(f"[{c_name}] No more prompts after batch {batch_num - 1}")
                    break

                send_telegram(
                    f"[{c_name}] Batch {batch_num}: {len(prompts)} video(s) (parallel)"
                )

                # Video numbering: re-count each batch to stay accurate
                today_prefix = f"{date_du_jour}_V"
                existing_rows = master_ws.get_all_values()
                existing_today = len([r for r in existing_rows[1:] if r and r[0].strip().startswith(today_prefix)]) if len(existing_rows) > 1 else 0
                vid_index = existing_today + 1

                # ── Fire ALL generation requests for this batch ──
                tasks = []
                for p_data in prompts:
                    try:
                        task_id = kie_generate_video(p_data["prompt"])
                        tasks.append({
                            "task_id": task_id,
                            "prompt_data": p_data,
                            "vid_index": vid_index,
                            "status": "generating",
                            "video_url": None,
                            "elapsed": 0,
                        })
                        vid_index += 1
                    except Exception as e:
                        total_errors += 1
                        print(f"  [{c_name}] Failed to submit row {p_data['row_index']}: {e}")
                        send_telegram(f"[{c_name}] Submit error row {p_data['row_index']}: {str(e)[:150]}")

                if not tasks:
                    break

                print(f"  [{c_name}] Batch {batch_num}: {len(tasks)} generation(s) submitted")

                # ── Poll loop — check all pending, edit as soon as ready ──
                max_wait = 600
                interval = 15

                while any(t["status"] == "generating" for t in tasks):
                    time.sleep(interval)

                    for task in tasks:
                        if task["status"] != "generating":
                            continue

                        task["elapsed"] += interval
                        task_id = task["task_id"]

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
                                urls = data["data"].get("response", {}).get("resultUrls", [])
                                if not urls:
                                    task["status"] = "failed"
                                    total_errors += 1
                                    print(f"  [{c_name}] V{task['vid_index']}: success but no URL")
                                    continue

                                task["video_url"] = urls[0]
                                task["status"] = "ready"
                                print(f"  [{c_name}] V{task['vid_index']} ready! Editing now...")

                                ok = process_ready_video(
                                    task, drive_service, gc, c_name, c_cfg,
                                    local_part2_clean, edited_folder_id, original_folder_id,
                                    master_ws, date_du_jour,
                                )
                                task["status"] = "done" if ok else "failed"
                                if ok:
                                    total_success += 1
                                else:
                                    total_errors += 1

                            elif flag in (2, 3):
                                err = data["data"].get("errorMessage", "unknown")
                                task["status"] = "failed"
                                total_errors += 1
                                print(f"  [{c_name}] V{task['vid_index']} generation failed: {err}")
                                send_telegram(f"[{c_name}] V{task['vid_index']} failed: {err[:100]}")

                        except Exception as e:
                            print(f"  [{c_name}] Poll error V{task['vid_index']}: {e}")

                        if task["status"] == "generating" and task["elapsed"] >= max_wait:
                            task["status"] = "failed"
                            total_errors += 1
                            print(f"  [{c_name}] V{task['vid_index']} timed out after {max_wait}s")
                            send_telegram(f"[{c_name}] V{task['vid_index']} timed out")

                    pending = sum(1 for t in tasks if t["status"] == "generating")
                    if pending > 0:
                        print(f"  [{c_name}] {pending} still generating...")

                batch_ok = sum(1 for t in tasks if t["status"] == "done")
                print(f"  [{c_name}] Batch {batch_num} complete: {batch_ok}/{len(tasks)} OK")

            # Cleanup Part2
            for p in [local_part2, local_part2_clean]:
                try:
                    if os.path.exists(p):
                        os.remove(p)
                except:
                    pass

        # Final report
        msg = (
            f"Pipeline complete!\n\n"
            f"Countries: {', '.join(countries_to_process.keys())}\n"
            f"{total_success} video(s) generated + edited\n"
        )
        if total_errors:
            msg += f"{total_errors} error(s)\n"
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
        for c_cfg in COUNTRY_CONFIG.values():
            prompts = get_prompts_for_country(gc, c_cfg, limit=1)
            if prompts:
                return True
    except Exception as e:
        print(f"  Cron check error: {e}")
    return False

def cron_loop():
    """Background thread that checks for new prompts and auto-launches."""
    print(f"Cron started (interval={CRON_INTERVAL}s)")
    while True:
        time.sleep(CRON_INTERVAL)
        if is_generating:
            continue
        try:
            if has_pending_prompts():
                print("Cron: pending prompts found, launching pipeline...")
                generate_and_process()
        except Exception as e:
            print(f"Cron error: {e}")

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
    return {
        "generating": is_generating,
        "cron_enabled": cron_enabled,
        "countries": list(COUNTRY_CONFIG.keys()),
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
        "max_videos_per_country": MAX_VIDEOS_PER_RUN,
        "message": f"Generation + editing started ({target})",
    })

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", 8000)))
