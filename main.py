import os
import io
import datetime
import subprocess
import threading
import tempfile
import whisper
import gspread
from fastapi import FastAPI, BackgroundTasks
from fastapi.responses import JSONResponse
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload, MediaFileUpload
import json
import base64

app = FastAPI()

# ============================================================
# CONFIGURATION (via variables d'environnement sur Render)
# ============================================================
MASTER_SHEET_URL    = os.environ.get("MASTER_SHEET_URL",    "https://docs.google.com/spreadsheets/d/1tlB7auPNU_fXUiuIbI-5EbmizInw4rw35tB2SWEvPas/edit")
VIDEOS_SHEET_URL    = os.environ.get("VIDEOS_SHEET_URL",    "https://docs.google.com/spreadsheets/d/13BxBpA1nZ8Vt-hlRDUqZcC6pTU0z-BMvwdtuZmnsy6g/edit")
HOOKS_FOLDER_ID     = os.environ.get("HOOKS_FOLDER_ID",     "12KQp_2d0witKtbASqM2caLW8zCfdIz00")
RESULTS_FOLDER_ID   = os.environ.get("RESULTS_FOLDER_ID",   "1ZTciHcp8LtbLjsuwUbE0MEwuCNMJzbPQ")
PART2_FILE_ID       = os.environ.get("PART2_FILE_ID",       "1INNY-MUaI0xFPd7dafeGx5_5TE9CwlbL")
DEFAULT_TITLE       = os.environ.get("DEFAULT_TITLE",       "The danger no\none told you about")
VIDEOS_PER_CAMPAIGN = int(os.environ.get("VIDEOS_PER_CAMPAIGN", "20"))
TELEGRAM_TOKEN      = os.environ.get("TELEGRAM_TOKEN",      "8747966519:AAEsz9JSa8OXcETu9OnUWwf6v1LdvNxrv3w")
TELEGRAM_CHAT_ID    = os.environ.get("TELEGRAM_CHAT_ID",    "1687730801")
GOOGLE_CREDS_JSON   = os.environ.get("GOOGLE_CREDS_JSON",   "")

FONT_PATH = "/usr/share/fonts/truetype/custom/Montserrat-Bold.ttf"

processing_lock = threading.Lock()
is_processing = False

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
# FONCTIONS DRIVE
# ============================================================
def drive_list_videos(drive_service, folder_id):
    q = f"'{folder_id}' in parents and trashed = false"
    out = []
    page_token = None
    while True:
        resp = drive_service.files().list(
            q=q, fields="nextPageToken, files(id, name)",
            pageToken=page_token, pageSize=1000,
            supportsAllDrives=True,
            includeItemsFromAllDrives=True
        ).execute()
        out.extend(resp.get("files", []))
        page_token = resp.get("nextPageToken")
        if not page_token:
            break
    out = [f for f in out if f["name"].lower().strip().endswith((".mp4", ".mov"))]
    out.sort(key=lambda x: x["name"].lower())
    return out

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

def drive_move_file(drive_service, file_id, new_parent_id):
    file_meta = drive_service.files().get(
        fileId=file_id, fields="parents", supportsAllDrives=True
    ).execute()
    old_parents = ",".join(file_meta.get("parents", []))
    drive_service.files().update(
        fileId=file_id,
        addParents=new_parent_id,
        removeParents=old_parents,
        supportsAllDrives=True
    ).execute()

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

def count_videos_in_drive_folder(drive_service, folder_id):
    q = f"'{folder_id}' in parents and trashed = false"
    resp = drive_service.files().list(
        q=q, fields="files(id,name)",
        supportsAllDrives=True,
        includeItemsFromAllDrives=True
    ).execute()
    return len([f for f in resp.get("files", []) if f["name"].lower().endswith((".mp4", ".mov"))])

# ============================================================
# TELEGRAM
# ============================================================
def send_telegram(msg):
    try:
        import requests
        requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            json={"chat_id": TELEGRAM_CHAT_ID, "text": msg, "parse_mode": "Markdown"},
            timeout=10
        )
    except:
        pass

# ============================================================
# CHARGER LES TITRES DEPUIS LE SHEET
# ============================================================
def load_titles_from_sheet(gc):
    try:
        videos_ws = gc.open_by_url(VIDEOS_SHEET_URL).sheet1
        all_rows = videos_ws.get_all_values()
        if not all_rows:
            return []
        headers = [h.lower().strip() for h in all_rows[0]]
        title_col = None
        for i, h in enumerate(headers):
            if "title" in h and "video" in h:
                title_col = i
                break
        if title_col is None:
            return []
        titles = []
        for row in all_rows[1:]:
            if len(row) > title_col and row[title_col].strip():
                titles.append(row[title_col].strip())
        return titles
    except Exception as e:
        print(f"Erreur chargement titres: {e}")
        return []

# ============================================================
# FFMPEG : obtenir la durée d'une vidéo
# ============================================================
def get_video_duration(path):
    result = subprocess.run(
        ["ffprobe", "-v", "error", "-show_entries", "format=duration",
         "-of", "default=noprint_wrappers=1:nokey=1", path],
        capture_output=True, text=True
    )
    return float(result.stdout.strip())

# ============================================================
# FFMPEG : détecter start/end de la parole via Whisper
# ============================================================
def get_speech_bounds(model, video_path, video_duration):
    result = model.transcribe(video_path, word_timestamps=True)
    words = []
    for segment in result.get("segments", []):
        words.extend(segment.get("words", []))
    if words:
        start_t = max(0, words[0]["start"] - 0.1)
        end_t = min(words[-1]["end"] + 0.8, video_duration - 0.05)
    else:
        start_t, end_t = 0, video_duration
    return start_t, end_t, result

# ============================================================
# FFMPEG : générer les sous-titres au format SRT
# ============================================================
def generate_srt(model, audio_path):
    result = model.transcribe(audio_path, word_timestamps=True)
    words_all = []
    for segment in result.get("segments", []):
        words_all.extend(segment.get("words", []))

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

# ============================================================
# MONTAGE PRINCIPAL VIA FFMPEG PUR
# ============================================================
def process_videos():
    global is_processing
    now = datetime.datetime.now()
    date_du_jour = now.strftime("%d.%m")
    day_month_slash = now.strftime("%d/%m")
    local_part2 = "/tmp/partie2.mp4"

    try:
        drive_service, gc = get_google_services()
        master_ws = gc.open_by_url(MASTER_SHEET_URL).sheet1
        titles_from_sheet = load_titles_from_sheet(gc)

        # Préparer les dossiers Drive
        today_folder_id    = drive_get_or_create_folder(drive_service, RESULTS_FOLDER_ID, date_du_jour)
        edited_folder_id   = drive_get_or_create_folder(drive_service, today_folder_id, "edited")
        original_folder_id = drive_get_or_create_folder(drive_service, today_folder_id, "original")

        # Télécharger Part2
        if os.path.exists(local_part2):
            os.remove(local_part2)
        drive_download_file(drive_service, PART2_FILE_ID, local_part2)

        # Lister les vidéos dans HOOKS
        hook_files = drive_list_videos(drive_service, HOOKS_FOLDER_ID)
        if not hook_files:
            send_telegram("✅ *Video Editor* — Aucune vidéo dans HOOKS. Rien à traiter.")
            return

        send_telegram(f"🎬 *Video Editor* — Début du montage de *{len(hook_files)} vidéo(s)*...")

        model = whisper.load_model("base")
        success_count = 0
        error_count = 0

        for index, f in enumerate(hook_files, start=1):
            hook_id   = f["id"]
            hook_name = f["name"]
            nom_final = f"{date_du_jour}_V{index}.mp4"
            local_hook     = f"/tmp/hook_{index}.mp4"
            local_hook_cut = f"/tmp/hook_cut_{index}.mp4"
            local_concat   = f"/tmp/concat_{index}.mp4"
            local_audio    = f"/tmp/audio_{index}.wav"
            local_srt      = f"/tmp/subs_{index}.srt"
            local_out      = f"/tmp/out_{index}.mp4"
            concat_list    = f"/tmp/list_{index}.txt"

            # Titre dynamique
            if index - 1 < len(titles_from_sheet):
                video_title = titles_from_sheet[index - 1]
            else:
                video_title = DEFAULT_TITLE

            try:
                # 1. Télécharger le hook
                drive_download_file(drive_service, hook_id, local_hook)
                hook_duration = get_video_duration(local_hook)

                # 2. Détecter les bornes de parole
                start_t, end_t, _ = get_speech_bounds(model, local_hook, hook_duration)

                # 3. Couper le hook
                subprocess.run([
                    "ffmpeg", "-y", "-i", local_hook,
                    "-ss", str(start_t), "-to", str(end_t),
                    "-c", "copy", local_hook_cut
                ], check=True, capture_output=True)

                # 4. Concaténer hook + part2
                with open(concat_list, "w") as cl:
                    cl.write(f"file '{local_hook_cut}'\n")
                    cl.write(f"file '{local_part2}'\n")

                subprocess.run([
                    "ffmpeg", "-y", "-f", "concat", "-safe", "0",
                    "-i", concat_list,
                    "-c:v", "libx264", "-c:a", "aac", "-movflags", "+faststart",
                    local_concat
                ], check=True, capture_output=True)

                # 5. Extraire l'audio pour Whisper
                subprocess.run([
                    "ffmpeg", "-y", "-i", local_concat,
                    "-vn", "-acodec", "pcm_s16le", "-ar", "16000", "-ac", "1",
                    local_audio
                ], check=True, capture_output=True)

                # 6. Générer les sous-titres SRT
                srt_content = generate_srt(model, local_audio)
                with open(local_srt, "w", encoding="utf-8") as sf:
                    sf.write(srt_content)

                # 7. Brûler les sous-titres + titre overlay avec FFmpeg
                # Titre en overlay pendant 4 secondes (boîte blanche avec texte noir)
                title_escaped = video_title.replace("'", "\\'").replace(":", "\\:").replace("\n", " ")
                font_path_escaped = FONT_PATH.replace(":", "\\:")

                # Vérifier si la police Montserrat existe
                if os.path.exists(FONT_PATH):
                    font_arg = f"fontfile={font_path_escaped}:fontcolor=black:fontsize=55"
                else:
                    font_arg = "fontcolor=black:fontsize=55"

                # Filtre FFmpeg : sous-titres + titre overlay
                vf_filter = (
                    f"subtitles={local_srt}:force_style='FontSize=28,PrimaryColour=&HFFFFFF,OutlineColour=&H000000,Outline=2,Alignment=2,MarginV=80',"
                    f"drawbox=x=(W-tw)/2-20:y=780-10:w=tw+40:h=th+20:color=white@1.0:t=fill:enable='lt(t,4)',"
                    f"drawtext=text='{title_escaped}':{font_arg}:x=(w-tw)/2:y=790:enable='lt(t,4)'"
                )

                subprocess.run([
                    "ffmpeg", "-y", "-i", local_concat,
                    "-vf", vf_filter,
                    "-c:v", "libx264", "-c:a", "aac",
                    "-preset", "fast", "-crf", "23",
                    "-movflags", "+faststart",
                    local_out
                ], check=True, capture_output=True)

                # 8. Upload vers Drive
                out_id = drive_upload_video(drive_service, local_out, edited_folder_id, nom_final)
                drive_move_file(drive_service, hook_id, original_folder_id)

                # 9. Log dans le Master Sheet
                existing_before = count_videos_in_drive_folder(drive_service, edited_folder_id) - 1
                version = (existing_before // VIDEOS_PER_CAMPAIGN) + 1
                campaign_name = f"C{version}_{day_month_slash}"
                adset_name    = f"adset{version}_{day_month_slash}"
                drive_link, direct_link = make_drive_links(out_id)
                master_ws.append_row(
                    [nom_final.replace(".mp4", ""), drive_link, direct_link, campaign_name, adset_name],
                    value_input_option="USER_ENTERED"
                )

                success_count += 1
                print(f"✅ {nom_final} monté avec succès")

            except Exception as e:
                error_count += 1
                print(f"❌ Erreur sur {hook_name}: {e}")

            finally:
                for path in [local_hook, local_hook_cut, local_concat, local_audio, local_srt, local_out, concat_list]:
                    try:
                        if os.path.exists(path):
                            os.remove(path)
                    except:
                        pass

        # Rapport final Telegram
        msg = f"🚀 *Video Editor — Mission accomplie!*\n\n"
        msg += f"✅ {success_count} vidéo(s) montée(s) avec succès\n"
        if error_count > 0:
            msg += f"❌ {error_count} erreur(s)\n"
        msg += f"📁 Résultats dans: RESULTATS/{date_du_jour}/edited/"
        send_telegram(msg)

    except Exception as e:
        send_telegram(f"❌ *Video Editor — Erreur critique:* {str(e)[:200]}")
        print(f"Erreur critique: {e}")
    finally:
        is_processing = False

# ============================================================
# ENDPOINTS
# ============================================================
@app.get("/")
def root():
    return {"status": "Video Editor Server running", "processing": is_processing}

@app.post("/process")
def trigger_processing(background_tasks: BackgroundTasks):
    global is_processing
    if is_processing:
        return JSONResponse({"status": "already_running", "message": "Un montage est déjà en cours"})
    is_processing = True
    background_tasks.add_task(process_videos)
    return JSONResponse({"status": "started", "message": "Montage démarré en arrière-plan"})

@app.get("/status")
def status():
    return {"processing": is_processing}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", 8000)))
