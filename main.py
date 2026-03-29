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

HOOK_USA_FOLDER_ID  = os.environ.get("HOOK_USA_FOLDER_ID",  "1cXo5gwUkfraVUFY3PLsonja_Za_M7smk")

# ============================================================
# MULTI-COUNTRY CONFIG
# ============================================================
COUNTRY_CONFIG = {
    "USA": {
        "hook_folder_id":    HOOK_USA_FOLDER_ID,
        "results_folder_id": os.environ.get("RESULTS_USA_FOLDER_ID", "1ZTciHcp8LtbLjsuwUbE0MEwuCNMJzbPQ"),
        "part2_file_id":     PART2_FILE_ID,
        "master_tab":        os.environ.get("MASTER_TAB_USA", "To launch (USA)"),
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

def drive_move_file(drive_service, file_id, new_parent_id):
    f = drive_service.files().get(fileId=file_id, fields="parents", supportsAllDrives=True).execute()
    old_parents = ",".join(f.get("parents", []))
    drive_service.files().update(
        fileId=file_id,
        addParents=new_parent_id,
        removeParents=old_parents,
        supportsAllDrives=True,
        fields="id,parents"
    ).execute()

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
# HOOK SCANNER — process videos from HOOK folder
# ============================================================
def process_hook_videos(country="USA"):
    """
    Scan HOOK folder for new .mp4 videos.
    For each video:
      1. Download it
      2. Move original to RESULTS/<country>/<date>/original/
      3. Edit it (FFmpeg + Part2 + subtitles)
      4. Upload edited to RESULTS/<country>/<date>/edited/
      5. Log to Master Sheet
      6. Remove from HOOK folder (move to original/)
    """
    global is_processing, last_activity_time
    c_cfg = COUNTRY_CONFIG[country]
    hook_folder_id = c_cfg["hook_folder_id"]
    results_folder_id = c_cfg["results_folder_id"]
    date_str = datetime.datetime.now().strftime("%d.%m")

    total_success = 0
    total_errors = 0

    local_part2 = f"/tmp/part2_hook_{country}.mp4"
    local_part2_clean = f"/tmp/part2_hook_clean_{country}.mp4"

    try:
        drive_svc, gc = get_google_services()

        # List videos in HOOK folder
        hook_q = f"'{hook_folder_id}' in parents and trashed=false and mimeType='video/mp4'"
        hook_files = drive_svc.files().list(
            q=hook_q, fields="files(id,name)",
            supportsAllDrives=True, includeItemsFromAllDrives=True, pageSize=500
        ).execute().get("files", [])

        # Also list .txt metadata files in HOOK
        txt_q = f"'{hook_folder_id}' in parents and trashed=false and name contains '.txt'"
        txt_files_list = drive_svc.files().list(
            q=txt_q, fields="files(id,name)",
            supportsAllDrives=True, includeItemsFromAllDrives=True, pageSize=500
        ).execute().get("files", [])
        txt_files = {f["name"]: f for f in txt_files_list}

        if not hook_files:
            return

        print(f"[{country}] Found {len(hook_files)} video(s) in HOOK folder")
        send_telegram(f"[{country}] Processing {len(hook_files)} video(s) from HOOK")

        # Create date folders in RESULTS
        date_folder_id = drive_get_or_create_folder(drive_svc, results_folder_id, date_str)
        original_folder_id = drive_get_or_create_folder(drive_svc, date_folder_id, "original")
        edited_folder_id = drive_get_or_create_folder(drive_svc, date_folder_id, "edited")

        # Determine next V number from existing edited files
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

        # Download Part2 once
        for p in [local_part2, local_part2_clean]:
            if os.path.exists(p):
                os.remove(p)
        drive_download_file(drive_svc, c_cfg["part2_file_id"], local_part2)
        reencode_video(local_part2, local_part2_clean)
        print(f"  [{country}] Part2 ready")

        vid_index = max_v + 1
        for hook_file in sorted(hook_files, key=lambda x: x["name"]):
            nom_final = f"{date_str}_V{vid_index}.mp4"

            if nom_final in edited_names:
                print(f"  [{country}] Skipping V{vid_index}: already exists in edited/")
                vid_index += 1
                continue

            local_raw = f"/tmp/hook_{country}_{vid_index}.mp4"
            local_edited = None

            # Read metadata from .txt file if available
            txt_name = hook_file["name"].replace(".mp4", ".txt")
            meta = {"title": DEFAULT_TITLE, "headline": "", "primary_text": "", "prompt": ""}
            if txt_name in txt_files:
                try:
                    d_svc_meta, _ = get_google_services()
                    txt_content = d_svc_meta.files().get_media(
                        fileId=txt_files[txt_name]["id"], supportsAllDrives=True
                    ).execute().decode("utf-8")
                    txt_json = json.loads(txt_content)
                    meta = {
                        "title": txt_json.get("video_title", DEFAULT_TITLE) or DEFAULT_TITLE,
                        "headline": txt_json.get("headline", ""),
                        "primary_text": txt_json.get("primary_text", ""),
                        "prompt": txt_json.get("video_prompt", ""),
                    }
                except Exception as meta_err:
                    print(f"  [{country}] Could not read metadata {txt_name}: {meta_err}")

            try:
                d_svc, _ = get_google_services()

                # Download video from HOOK
                drive_download_file(d_svc, hook_file["id"], local_raw)

                # Move original video from HOOK to RESULTS/<date>/original/
                drive_move_file(d_svc, hook_file["id"], original_folder_id)
                print(f"  [{country}] Moved {hook_file['name']} to original/")

                # Move .txt metadata too if it exists
                if txt_name in txt_files:
                    try:
                        drive_move_file(d_svc, txt_files[txt_name]["id"], original_folder_id)
                    except Exception as txt_move_err:
                        print(f"  [{country}] Could not move {txt_name}: {txt_move_err}")

                # Edit video
                edit_semaphore.acquire()
                try:
                    local_edited = edit_single_video(local_raw, local_part2_clean, meta, country, vid_index)
                finally:
                    edit_semaphore.release()

                if not local_edited or not os.path.exists(local_edited) or os.path.getsize(local_edited) < 10000:
                    raise Exception("Edited file missing or too small")

                # Upload edited video
                out_id = drive_upload_video(d_svc, local_edited, edited_folder_id, nom_final)
                print(f"  [{country}] Uploaded edited: {nom_final}")

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
                             meta["primary_text"], meta["headline"], meta["prompt"]],
                            value_input_option="USER_ENTERED"
                        )
                        break
                    except Exception as se:
                        print(f"  Sheet write retry {attempt+1}: {se}")
                        if attempt < 2:
                            time.sleep(5)
                        else:
                            print(f"  [{country}] SHEET WRITE FAILED for V{vid_index}")
                            send_telegram(f"[{country}] Sheet write FAILED for V{vid_index}: {str(se)[:100]}")

                total_success += 1
                last_activity_time = time.time()
                vid_index += 1

                if total_success % 5 == 0:
                    send_telegram(f"[{country}] Progress: {total_success} edited so far")

            except Exception as e:
                total_errors += 1
                print(f"  [{country}] ERROR editing V{vid_index}: {e}")
                import traceback
                traceback.print_exc()
                send_telegram(f"[{country}] Error V{vid_index}: {str(e)[:200]}")
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

        if total_success > 0 or total_errors > 0:
            send_telegram(f"[{country}] HOOK batch done: {total_success} edited, {total_errors} errors")

    except Exception as e:
        print(f"[{country}] process_hook_videos critical error: {e}")
        import traceback
        traceback.print_exc()
        send_telegram(f"[{country}] HOOK processing error: {str(e)[:200]}")

    finally:
        for p in [local_part2, local_part2_clean]:
            try:
                if os.path.exists(p): os.remove(p)
            except: pass
        is_processing = False

# ============================================================
# CRON — scan HOOK folder every 30s
# ============================================================
CRON_INTERVAL = int(os.environ.get("CRON_INTERVAL", "30"))
cron_enabled = os.environ.get("CRON_ENABLED", "true").lower() == "true"

def has_hook_videos():
    """Quick check: are there any .mp4 files in any HOOK folder?"""
    try:
        drive_service, _ = get_google_services()
        for c_name, c_cfg in COUNTRY_CONFIG.items():
            hook_id = c_cfg["hook_folder_id"]
            q = f"'{hook_id}' in parents and trashed=false and mimeType='video/mp4'"
            resp = drive_service.files().list(
                q=q, fields="files(id)", supportsAllDrives=True,
                includeItemsFromAllDrives=True, pageSize=1
            ).execute()
            if resp.get("files"):
                return c_name
    except Exception as e:
        print(f"  Cron hook check error: {e}")
    return None

def cron_loop():
    """Background thread: scan HOOK folders for new videos and process them."""
    global is_processing, last_activity_time
    print(f"Cron started (interval={CRON_INTERVAL}s)")
    while True:
        time.sleep(CRON_INTERVAL)
        try:
            stale = time.time() - last_activity_time
            if is_processing and stale > WATCHDOG_TIMEOUT:
                print(f"WATCHDOG: is_processing stuck for {stale:.0f}s, forcing reset!")
                send_telegram(f"WATCHDOG: processing stuck for {stale:.0f}s, forcing is_processing=False")
                is_processing = False

            if not is_processing:
                country = has_hook_videos()
                if country:
                    print(f"Cron: found videos in HOOK/{country}, processing...")
                    is_processing = True
                    last_activity_time = time.time()
                    threading.Thread(target=process_hook_videos, args=(country,), daemon=True).start()

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
        "status": "Video Editor Server (HOOK scanner mode)",
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
    send_telegram(f"Manual reset: processing {was}->False")
    return {"status": "ok", "was_processing": was}

@app.post("/process")
def trigger_process(background_tasks: BackgroundTasks, country: str = "USA"):
    """Manually trigger HOOK processing for a country."""
    global is_processing
    if is_processing:
        return JSONResponse({"status": "already_running", "message": "Processing already in progress"})
    c = country.upper()
    if c not in COUNTRY_CONFIG:
        return JSONResponse({"status": "error", "message": f"Unknown country: {c}"}, status_code=400)
    is_processing = True
    last_activity_time = time.time()
    background_tasks.add_task(process_hook_videos, c)
    return {"status": "started", "country": c}

@app.post("/test-edit")
def test_edit_one(country: str = "USA"):
    """Synchronously edit ONE video from HOOK and return success/error."""
    c = country.upper()
    if c not in COUNTRY_CONFIG:
        return {"error": f"Unknown country: {c}"}
    c_cfg = COUNTRY_CONFIG[c]

    try:
        drive_service, gc = get_google_services()
        hook_id = c_cfg["hook_folder_id"]

        q = f"'{hook_id}' in parents and trashed=false and mimeType='video/mp4'"
        hook_files = drive_service.files().list(
            q=q, fields="files(id,name)", supportsAllDrives=True,
            includeItemsFromAllDrives=True, pageSize=1
        ).execute().get("files", [])

        if not hook_files:
            return {"error": "No videos in HOOK folder"}

        test_file = hook_files[0]
        local_raw = f"/tmp/test_edit_raw.mp4"
        local_part2 = f"/tmp/test_part2.mp4"
        local_part2_clean = f"/tmp/test_part2_clean.mp4"

        drive_download_file(drive_service, test_file["id"], local_raw)
        raw_size = os.path.getsize(local_raw)
        raw_duration = get_video_duration(local_raw)

        drive_download_file(drive_service, c_cfg["part2_file_id"], local_part2)
        reencode_video(local_part2, local_part2_clean)

        metadata = {"title": DEFAULT_TITLE, "headline": "", "primary_text": "", "prompt": "test"}
        local_edited = edit_single_video(local_raw, local_part2_clean, metadata, c, 9999)

        edited_size = os.path.getsize(local_edited) if local_edited and os.path.exists(local_edited) else 0
        edited_duration = get_video_duration(local_edited) if edited_size > 0 else 0

        for p in [local_raw, local_part2, local_part2_clean, local_edited]:
            try:
                if p and os.path.exists(p): os.remove(p)
            except: pass

        return {
            "status": "success",
            "original": test_file["name"],
            "raw_size_mb": round(raw_size / 1024 / 1024, 2),
            "raw_duration": round(raw_duration, 2),
            "edited_size_mb": round(edited_size / 1024 / 1024, 2),
            "edited_duration": round(edited_duration, 2),
        }

    except Exception as e:
        import traceback
        return {"error": str(e), "traceback": traceback.format_exc()}

@app.get("/debug")
def debug_state():
    """Show HOOK folder contents + RESULTS folder contents + Master Sheet row counts."""
    try:
        drive_service, gc = get_google_services()
        result = {}

        for c_name, c_cfg in COUNTRY_CONFIG.items():
            country_info = {}

            # HOOK folder contents
            hook_id = c_cfg["hook_folder_id"]
            hook_q = f"'{hook_id}' in parents and trashed=false"
            hook_resp = drive_service.files().list(
                q=hook_q, fields="files(id,name,mimeType)",
                supportsAllDrives=True, includeItemsFromAllDrives=True, pageSize=500
            ).execute().get("files", [])
            hook_videos = [f["name"] for f in hook_resp if f.get("mimeType") == "video/mp4"]
            hook_txts = [f["name"] for f in hook_resp if f["name"].endswith(".txt")]
            country_info["hook"] = {
                "videos": len(hook_videos),
                "txt_files": len(hook_txts),
                "video_names": sorted(hook_videos)[:20],
            }

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
    """Sync Master Sheet with edited/ folder — add missing rows."""
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
        orig_folder_id = next((f["id"] for f in subs if f["name"] == "original"), None)
        if not edit_folder_id:
            return {"status": "error", "message": "No edited/ folder found"}

        edited_files = drive_svc.files().list(
            q=f"'{edit_folder_id}' in parents and trashed=false and mimeType='video/mp4'",
            fields="files(id,name)", supportsAllDrives=True, includeItemsFromAllDrives=True, pageSize=500
        ).execute().get("files", [])

        # Load .txt metadata from original/ folder
        orig_txts = {}
        if orig_folder_id:
            txt_q = f"'{orig_folder_id}' in parents and trashed=false and name contains '.txt'"
            orig_txt_files = drive_svc.files().list(
                q=txt_q, fields="files(id,name)",
                supportsAllDrives=True, includeItemsFromAllDrives=True, pageSize=500
            ).execute().get("files", [])
            orig_txts = {f["name"]: f for f in orig_txt_files}

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

        msg = f"[{c}] fix-sheet {date}: added {added}, skipped {skipped}"
        send_telegram(msg)
        return {"status": "ok", "added": added, "skipped": skipped, "total_edited": len(edited_files)}

    except Exception as e:
        import traceback
        traceback.print_exc()
        return {"status": "error", "message": str(e)}

@app.post("/clean-sheet")
def clean_sheet(country: str = "USA", keep_date: str = None):
    """Remove all rows from Master Sheet that don't match keep_date."""
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
            return {"status": "ok", "message": "Sheet is empty", "kept": 0, "removed": 0}

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


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", 8000)))
