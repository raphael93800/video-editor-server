import os
import io
import datetime
import subprocess
import threading
import time
import openai
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
HOOKS_FOLDER_ID     = os.environ.get("HOOKS_FOLDER_ID",     "12KQp_2d0witKtbASqM2caLW8zCfdIz00")
RESULTS_FOLDER_ID   = os.environ.get("RESULTS_FOLDER_ID",   "1ZTciHcp8LtbLjsuwUbE0MEwuCNMJzbPQ")
PART2_FILE_ID       = os.environ.get("PART2_FILE_ID",       "1INNY-MUaI0xFPd7dafeGx5_5TE9CwlbL")
DEFAULT_TITLE       = os.environ.get("DEFAULT_TITLE",       "The danger no one told you about")
VIDEOS_PER_CAMPAIGN = int(os.environ.get("VIDEOS_PER_CAMPAIGN", "20"))
TELEGRAM_TOKEN      = os.environ.get("TELEGRAM_TOKEN",      "8747966519:AAEsz9JSa8OXcETu9OnUWwf6v1LdvNxrv3w")
TELEGRAM_CHAT_ID    = os.environ.get("TELEGRAM_CHAT_ID",    "1687730801")
GOOGLE_CREDS_JSON   = os.environ.get("GOOGLE_CREDS_JSON",   "")
OPENAI_API_KEY      = os.environ.get("OPENAI_API_KEY",      "")

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
def drive_list_all_files(drive_service, folder_id):
    """Liste tous les fichiers dans un dossier (vidéos ET .txt)."""
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
    return out

def drive_list_videos(drive_service, folder_id):
    """Liste uniquement les fichiers vidéo (.mp4, .mov) dans HOOKS."""
    all_files = drive_list_all_files(drive_service, folder_id)
    videos = [f for f in all_files if f["name"].lower().strip().endswith((".mp4", ".mov"))]
    videos.sort(key=lambda x: x["name"].lower())
    return videos

def drive_find_txt_for_hook(drive_service, folder_id, hook_name):
    """
    Cherche le fichier .txt qui correspond au hook.
    n8n crée veo_HHMMSS_xxx.mp4 ET veo_HHMMSS_xxx.txt dans le même dossier.
    """
    stem = os.path.splitext(hook_name)[0]  # "veo_HHMMSS_xxx"
    txt_name = stem + ".txt"
    all_files = drive_list_all_files(drive_service, folder_id)
    for f in all_files:
        if f["name"] == txt_name:
            return f["id"]
    return None

def drive_read_txt_metadata(drive_service, file_id):
    """
    Télécharge et parse le fichier .txt JSON créé par n8n.
    Format attendu :
    {
      "video_title": "...",
      "primary_text": "...",
      "headline": "...",
      "video_prompt": "..."
    }
    """
    try:
        request = drive_service.files().get_media(fileId=file_id, supportsAllDrives=True)
        buf = io.BytesIO()
        downloader = MediaIoBaseDownload(buf, request)
        done = False
        while not done:
            _, done = downloader.next_chunk()
        content = buf.getvalue().decode("utf-8")
        data = json.loads(content)
        return {
            "title":        data.get("video_title", "").strip(),
            "headline":     data.get("headline", "").strip(),
            "primary_text": data.get("primary_text", "").strip(),
            "video_prompt": data.get("video_prompt", "").strip(),
        }
    except Exception as e:
        print(f"⚠️ Impossible de lire le .txt metadata: {e}")
        return None

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

def drive_delete_file(drive_service, file_id):
    """Supprime un fichier de Drive (pour nettoyer les .txt après traitement)."""
    try:
        drive_service.files().delete(fileId=file_id, supportsAllDrives=True).execute()
    except Exception as e:
        print(f"⚠️ Impossible de supprimer le fichier Drive: {e}")

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
# FFMPEG : vérifier si une vidéo a une piste vidéo
# ============================================================
def has_video_stream(path):
    result = subprocess.run(
        ["ffprobe", "-v", "error", "-select_streams", "v:0",
         "-show_entries", "stream=codec_type",
         "-of", "default=noprint_wrappers=1:nokey=1", path],
        capture_output=True, text=True
    )
    return "video" in result.stdout.strip()

# ============================================================
# FFMPEG : ré-encoder proprement une vidéo (fix keyframes)
# ============================================================
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
# FFMPEG : détecter start/end de la parole via Whisper
# ============================================================
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
    return start_t, end_t, result

# ============================================================
# FFMPEG : générer les sous-titres au format SRT
# ============================================================
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

# ============================================================
# Construire les filtres drawtext pour les sous-titres
# ============================================================
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
# MONTAGE D'UN SEUL HOOK (fonction réutilisable)
# ============================================================
def process_single_hook(drive_service, master_ws, hook_file, global_index,
                        date_du_jour, edited_folder_id, original_folder_id,
                        local_part2_clean):
    """
    Monte une seule vidéo hook + Part2.
    Lit les métadonnées depuis le fichier .txt créé par n8n dans HOOKS.
    Retourne True si succès, False si erreur.
    """
    hook_id   = hook_file["id"]
    hook_name = hook_file["name"]
    nom_final = f"{date_du_jour}_V{global_index}.mp4"

    local_hook       = f"/tmp/hook_{global_index}.mp4"
    local_hook_clean = f"/tmp/hook_clean_{global_index}.mp4"
    local_hook_audio = f"/tmp/hook_audio_{global_index}.wav"
    local_hook_cut   = f"/tmp/hook_cut_{global_index}.mp4"
    local_concat     = f"/tmp/concat_{global_index}.mp4"
    local_audio      = f"/tmp/audio_{global_index}.wav"
    local_srt        = f"/tmp/subs_{global_index}.srt"
    local_out        = f"/tmp/out_{global_index}.mp4"
    concat_list      = f"/tmp/list_{global_index}.txt"

    try:
        # ── 1. Lire les métadonnées depuis le fichier .txt ──────────────────
        # n8n crée veo_HHMMSS_xxx.txt avec le même nom que le hook .mp4
        txt_file_id = drive_find_txt_for_hook(drive_service, HOOKS_FOLDER_ID, hook_name)
        if txt_file_id:
            meta = drive_read_txt_metadata(drive_service, txt_file_id)
            if meta:
                video_title    = meta["title"] or DEFAULT_TITLE
                video_headline = meta["headline"]
                video_primary  = meta["primary_text"]
                video_prompt   = meta["video_prompt"]
                print(f"📄 Métadonnées lues depuis .txt: titre='{video_title}'")
            else:
                video_title, video_headline, video_primary, video_prompt = DEFAULT_TITLE, "", "", ""
        else:
            print(f"⚠️ Pas de fichier .txt trouvé pour {hook_name} — utilisation des valeurs par défaut")
            video_title, video_headline, video_primary, video_prompt = DEFAULT_TITLE, "", "", ""

        # ── 2. Télécharger et ré-encoder le hook ────────────────────────────
        drive_download_file(drive_service, hook_id, local_hook)
        print(f"📥 Hook téléchargé: {hook_name}")
        reencode_video(local_hook, local_hook_clean)
        print(f"🔄 Hook ré-encodé")

        if not has_video_stream(local_hook_clean):
            raise Exception(f"Pas de piste vidéo dans {hook_name} après ré-encodage")

        hook_duration = get_video_duration(local_hook_clean)
        print(f"⏱ Durée hook: {hook_duration:.2f}s")

        # ── 3. Détecter les bornes de parole ────────────────────────────────
        subprocess.run([
            "ffmpeg", "-y", "-i", local_hook_clean,
            "-vn", "-acodec", "pcm_s16le", "-ar", "16000", "-ac", "1",
            local_hook_audio
        ], check=True, capture_output=True)
        start_t, end_t, _ = get_speech_bounds(local_hook_audio, hook_duration)
        print(f"🎤 Parole: {start_t:.2f}s → {end_t:.2f}s")

        # ── 4. Couper le hook ───────────────────────────────────────────────
        subprocess.run([
            "ffmpeg", "-y", "-i", local_hook_clean,
            "-ss", str(start_t), "-to", str(end_t),
            "-c:v", "libx264", "-preset", "fast", "-crf", "23",
            "-c:a", "aac", "-ar", "44100",
            local_hook_cut
        ], check=True, capture_output=True)
        print(f"✂️ Hook coupé")

        if not has_video_stream(local_hook_cut):
            raise Exception("Hook coupé sans piste vidéo")

        # ── 5. Concaténer hook + Part2 ──────────────────────────────────────
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
        print(f"🔗 Concaténation OK")

        if not has_video_stream(local_concat):
            raise Exception("Vidéo concaténée sans piste vidéo")

        # ── 6. Extraire l'audio pour Whisper ────────────────────────────────
        subprocess.run([
            "ffmpeg", "-y", "-i", local_concat,
            "-vn", "-acodec", "pcm_s16le", "-ar", "16000", "-ac", "1",
            local_audio
        ], check=True, capture_output=True)

        # ── 7. Générer les sous-titres SRT ──────────────────────────────────
        srt_content = generate_srt(local_audio)
        with open(local_srt, "w", encoding="utf-8") as sf:
            sf.write(srt_content)
        print(f"📝 Sous-titres générés")

        # ── 8. Construire le filtre titre ───────────────────────────────────
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

        # ── 9. Brûler titre + sous-titres ───────────────────────────────────
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
            print(f"⚠️ Erreur overlay: {result.stderr[-500:]}")
            import shutil
            shutil.copy(local_concat, local_out)

        if not has_video_stream(local_out):
            raise Exception("Fichier final sans piste vidéo")

        final_duration = get_video_duration(local_out)
        print(f"✅ Vidéo finale: {final_duration:.2f}s")

        # ── 10. Upload vers Drive ────────────────────────────────────────────
        out_id = drive_upload_video(drive_service, local_out, edited_folder_id, nom_final)
        drive_move_file(drive_service, hook_id, original_folder_id)

        # Supprimer le fichier .txt de HOOKS après traitement
        if txt_file_id:
            drive_delete_file(drive_service, txt_file_id)

        # ── 11. Log dans le Master Sheet ────────────────────────────────────
        version = ((global_index - 1) // VIDEOS_PER_CAMPAIGN) + 1
        campaign_name = f"C{date_du_jour}_{version:02d}"
        adset_name    = f"adset{version}_{date_du_jour}"
        drive_link, direct_link = make_drive_links(out_id)
        master_ws.append_row(
            [
                nom_final.replace(".mp4", ""),  # Ad_Name
                drive_link,                      # Drive_Share_Link
                direct_link,                     # Direct_Download_Link
                campaign_name,                   # Campaign_Name
                adset_name,                      # AdSet_Name
                video_primary,                   # Primary_Text
                video_headline,                  # Headline
                video_prompt,                    # Video_Prompt
                "ready",                         # status
            ],
            value_input_option="USER_ENTERED"
        )
        print(f"✅ {nom_final} monté et uploadé avec succès")
        return True

    except Exception as e:
        print(f"❌ Erreur sur {hook_name}: {e}")
        send_telegram(f"⚠️ Erreur sur {hook_name}: {str(e)[:150]}")
        return False

    finally:
        for path in [local_hook, local_hook_clean, local_hook_audio, local_hook_cut,
                     local_concat, local_audio, local_srt, local_out, concat_list]:
            try:
                if os.path.exists(path):
                    os.remove(path)
            except:
                pass

# ============================================================
# MONTAGE PRINCIPAL — BOUCLE CONTINUE
# ============================================================
def process_videos():
    """
    Boucle principale :
    1. Traite tous les hooks présents dans HOOKS
    2. Attend 30s et revérifie
    3. S'arrête seulement si HOOKS est vide 2 fois de suite (1 min d'inactivité)
    Permet à n8n de générer les hooks en batches de 5 pendant que Render monte les précédents.
    """
    global is_processing
    now = datetime.datetime.now()
    date_du_jour = now.strftime("%d.%m")
    local_part2 = "/tmp/partie2.mp4"
    local_part2_clean = "/tmp/partie2_clean.mp4"

    try:
        drive_service, gc = get_google_services()
        master_ws = gc.open_by_url(MASTER_SHEET_URL).sheet1

        # Préparer les dossiers Drive
        today_folder_id    = drive_get_or_create_folder(drive_service, RESULTS_FOLDER_ID, date_du_jour)
        edited_folder_id   = drive_get_or_create_folder(drive_service, today_folder_id, "edited")
        original_folder_id = drive_get_or_create_folder(drive_service, today_folder_id, "original")

        # Télécharger et ré-encoder Part2 (une seule fois pour toute la session)
        for p in [local_part2, local_part2_clean]:
            if os.path.exists(p):
                os.remove(p)
        drive_download_file(drive_service, PART2_FILE_ID, local_part2)
        reencode_video(local_part2, local_part2_clean)
        print("✅ Part2 téléchargée et ré-encodée")

        success_count = 0
        error_count = 0
        empty_checks = 0

        # Numérotation : compter les vidéos du jour déjà dans le master sheet
        today_prefix = f"{date_du_jour}_V"
        existing_rows = master_ws.get_all_values()
        existing_today = len([r for r in existing_rows[1:] if r and r[0].strip().startswith(today_prefix)]) if len(existing_rows) > 1 else 0
        global_index = existing_today + 1
        print(f"📊 {existing_today} vidéos aujourd'hui → prochaine: V{global_index}")

        # ── Boucle principale ────────────────────────────────────────────────
        while True:
            hook_files = drive_list_videos(drive_service, HOOKS_FOLDER_ID)

            if not hook_files:
                empty_checks += 1
                print(f"📭 HOOKS vide ({empty_checks}/2)")
                if empty_checks >= 2:
                    print("🏁 HOOKS vide 2 fois de suite — arrêt de la boucle")
                    break
                print("⏳ Attente 30s avant de revérifier...")
                time.sleep(30)
                continue

            # Des hooks sont disponibles → on traite
            empty_checks = 0
            send_telegram(f"🎬 *Video Editor* — {len(hook_files)} hook(s) à monter (V{global_index}→V{global_index + len(hook_files) - 1})...")

            for hook_file in hook_files:
                ok = process_single_hook(
                    drive_service=drive_service,
                    master_ws=master_ws,
                    hook_file=hook_file,
                    global_index=global_index,
                    date_du_jour=date_du_jour,
                    edited_folder_id=edited_folder_id,
                    original_folder_id=original_folder_id,
                    local_part2_clean=local_part2_clean,
                )
                if ok:
                    success_count += 1
                else:
                    error_count += 1
                global_index += 1

            # Après le batch : attendre 30s et revérifier
            print("⏳ Batch terminé — attente 30s avant de revérifier HOOKS...")
            time.sleep(30)

        # Rapport final
        msg = f"🚀 *Video Editor — Mission accomplie!*\n\n"
        msg += f"✅ {success_count} vidéo(s) montée(s) avec succès\n"
        if error_count > 0:
            msg += f"❌ {error_count} erreur(s)\n"
        msg += f"📁 Résultats dans: RESULTATS/{date_du_jour}/edited/"
        send_telegram(msg)

    except Exception as e:
        send_telegram(f"❌ *Video Editor — Erreur critique:* {str(e)[:200]}")
        print(f"Erreur critique: {e}")
        import traceback
        traceback.print_exc()
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
