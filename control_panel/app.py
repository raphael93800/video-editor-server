#!/usr/bin/env python3
"""
Video Pipeline Control Panel
Desktop app to pilot 5 Render servers for video generation at scale.
"""

import tkinter as tk
from tkinter import ttk, messagebox
import threading
import json
import time
import ssl
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError

try:
    import certifi
    SSL_CTX = ssl.create_default_context(cafile=certifi.where())
except ImportError:
    SSL_CTX = ssl.create_default_context()
    SSL_CTX.check_hostname = False
    SSL_CTX.verify_mode = ssl.CERT_NONE

try:
    from config import SERVERS, REFRESH_INTERVAL_MS, REQUEST_TIMEOUT
except ImportError:
    SERVERS = [
        {"id": "S1", "name": "Server 1", "url": "https://video-editor-server.onrender.com"},
        {"id": "S2", "name": "Server 2", "url": "https://video-editor-server-2.onrender.com"},
        {"id": "S3", "name": "Server 3", "url": "https://video-editor-server-3.onrender.com"},
        {"id": "S4", "name": "Server 4", "url": "https://video-editor-server-4.onrender.com"},
        {"id": "S5", "name": "Server 5", "url": "https://video-editor-server-1.onrender.com"},
    ]
    REFRESH_INTERVAL_MS = 10000
    REQUEST_TIMEOUT = 15

BG_COLOR = "#1e1e2e"
CARD_BG = "#2a2a3d"
TEXT_COLOR = "#cdd6f4"
ACCENT = "#89b4fa"
GREEN = "#a6e3a1"
RED = "#f38ba8"
YELLOW = "#f9e2af"
GRAY = "#585b70"
BTN_BG = "#313244"
BTN_HOVER = "#45475a"


_DEBUG_LOG = "/tmp/videopipeline_debug.log"

def _dbg(msg):
    with open(_DEBUG_LOG, "a") as f:
        f.write(f"[{time.strftime('%H:%M:%S')}] {msg}\n")

def api_call(base_url, path, method="GET", timeout=None):
    url = f"{base_url}{path}"
    req = Request(url, method=method)
    req.add_header("Content-Type", "application/json")
    if method == "POST":
        req.data = b"{}"
    _dbg(f"CALL {method} {url}")
    try:
        with urlopen(req, timeout=timeout or REQUEST_TIMEOUT, context=SSL_CTX) as resp:
            raw = resp.read().decode()
            _dbg(f"  OK {url} -> {raw[:200]}")
            return json.loads(raw)
    except Exception as e:
        _dbg(f"  ERR {url} -> {e}")
        return {"_error": str(e)}


class ServerCard(tk.Frame):
    def __init__(self, parent, server_info):
        super().__init__(parent, bg=CARD_BG, highlightbackground=GRAY,
                         highlightthickness=1, padx=10, pady=8)
        self.server = server_info
        self.sid = server_info["id"]

        header = tk.Frame(self, bg=CARD_BG)
        header.pack(fill="x")

        self.status_dot = tk.Label(header, text="\u25cf", fg=GRAY, bg=CARD_BG, font=("SF Pro", 14))
        self.status_dot.pack(side="left")

        self.title_label = tk.Label(header, text=f"  {self.sid}", fg=TEXT_COLOR, bg=CARD_BG,
                                    font=("SF Pro", 13, "bold"))
        self.title_label.pack(side="left")

        self.state_label = tk.Label(header, text="...", fg=GRAY, bg=CARD_BG,
                                    font=("SF Pro", 10))
        self.state_label.pack(side="right")

        self.progress_var = tk.DoubleVar(value=0)
        self.progress_bar = ttk.Progressbar(self, variable=self.progress_var,
                                            maximum=100, length=200, mode="determinate")
        self.progress_bar.pack(fill="x", pady=(6, 2))

        self.progress_label = tk.Label(self, text="-- / --", fg=TEXT_COLOR, bg=CARD_BG,
                                       font=("SF Pro", 10))
        self.progress_label.pack()

        self.eta_label = tk.Label(self, text="", fg=ACCENT, bg=CARD_BG,
                                  font=("SF Pro", 9))
        self.eta_label.pack()

        self._first_done_time = None
        self._first_done_count = 0

    def update_status(self, data):
        if "_error" in data:
            self.status_dot.config(fg=GRAY)
            self.state_label.config(text="OFFLINE", fg=GRAY)
            self.progress_label.config(text="--")
            self.eta_label.config(text="")
            self.progress_var.set(0)
            return

        processing = data.get("processing", False)
        prompts = data.get("prompts", {})
        done = prompts.get("done", 0)
        total = prompts.get("total", 0)
        errors = prompts.get("error", 0)
        in_progress = prompts.get("processing", 0)

        if processing:
            self.status_dot.config(fg=GREEN)
            self.state_label.config(text="ACTIVE", fg=GREEN)
        elif total > 0 and done == total:
            self.status_dot.config(fg=ACCENT)
            self.state_label.config(text="DONE", fg=ACCENT)
        else:
            self.status_dot.config(fg=YELLOW)
            self.state_label.config(text="IDLE", fg=YELLOW)

        if total > 0:
            pct = (done / total) * 100
            self.progress_var.set(pct)
            self.progress_label.config(text=f"{done}/{total}  ({pct:.0f}%)")

            if errors > 0:
                self.progress_label.config(
                    text=f"{done}/{total}  ({pct:.0f}%)  [{errors} err]")

            if done > 0 and processing:
                if self._first_done_time is None or done < self._first_done_count:
                    self._first_done_time = time.time()
                    self._first_done_count = done
                elapsed = time.time() - self._first_done_time
                done_since = done - self._first_done_count
                if done_since > 0 and elapsed > 30:
                    rate = done_since / elapsed
                    remaining = total - done
                    eta_s = remaining / rate
                    if eta_s < 60:
                        self.eta_label.config(text=f"ETA ~{eta_s:.0f}s")
                    elif eta_s < 3600:
                        self.eta_label.config(text=f"ETA ~{eta_s/60:.0f}min")
                    else:
                        h = int(eta_s // 3600)
                        m = int((eta_s % 3600) // 60)
                        self.eta_label.config(text=f"ETA ~{h}h{m:02d}")
                else:
                    self.eta_label.config(text="ETA calculating...")
            else:
                self.eta_label.config(text="")
                self._first_done_time = None
        else:
            self.progress_var.set(0)
            self.progress_label.config(text="No prompts")
            self.eta_label.config(text="")

    def get_done_total(self, data):
        if "_error" in data:
            return 0, 0
        prompts = data.get("prompts", {})
        return prompts.get("done", 0), prompts.get("total", 0)


class ControlPanel(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("Video Pipeline Control Panel")
        self.configure(bg=BG_COLOR)
        self.geometry("720x680")
        self.minsize(680, 620)

        self._server_data = {}

        style = ttk.Style()
        style.theme_use("default")
        style.configure("TProgressbar", troughcolor=GRAY, background=ACCENT,
                        thickness=12)

        self._build_ui()
        self._log("App started, connecting to servers...")
        self._schedule_refresh()

    def _build_ui(self):
        title = tk.Label(self, text="Video Pipeline Control Panel",
                         fg=TEXT_COLOR, bg=BG_COLOR, font=("SF Pro", 18, "bold"))
        title.pack(pady=(14, 8))

        cards_frame = tk.Frame(self, bg=BG_COLOR)
        cards_frame.pack(fill="x", padx=12)

        self.cards = {}
        for i, srv in enumerate(SERVERS):
            card = ServerCard(cards_frame, srv)
            card.grid(row=0, column=i, padx=4, pady=4, sticky="nsew")
            cards_frame.columnconfigure(i, weight=1)
            self.cards[srv["id"]] = card

        # Global progress
        global_frame = tk.Frame(self, bg=BG_COLOR)
        global_frame.pack(fill="x", padx=16, pady=(6, 2))

        tk.Label(global_frame, text="TOTAL", fg=TEXT_COLOR, bg=BG_COLOR,
                 font=("SF Pro", 11, "bold")).pack(side="left")

        self.global_progress_var = tk.DoubleVar(value=0)
        self.global_bar = ttk.Progressbar(global_frame, variable=self.global_progress_var,
                                          maximum=100, length=360, mode="determinate")
        self.global_bar.pack(side="left", padx=(10, 10), fill="x", expand=True)

        self.global_label = tk.Label(global_frame, text="-- / --", fg=TEXT_COLOR, bg=BG_COLOR,
                                     font=("SF Pro", 11))
        self.global_label.pack(side="left")

        sep1 = tk.Frame(self, bg=GRAY, height=1)
        sep1.pack(fill="x", padx=12, pady=10)

        # Action buttons row 1: distribute + mark ready + retry
        action_frame1 = tk.Frame(self, bg=BG_COLOR)
        action_frame1.pack(fill="x", padx=16, pady=4)

        self._make_button(action_frame1, "Distribuer les prompts\n(USA \u2192 USA_1..5)",
                          self._distribute, width=22, bg=ACCENT, fg="#1e1e2e").pack(side="left", padx=4)

        self._make_button(action_frame1, "Marquer READY\n(onglet USA)",
                          self._mark_ready, width=18).pack(side="left", padx=4)

        self._make_button(action_frame1, "Relancer\nles erreurs",
                          self._retry_errors, width=14).pack(side="left", padx=4)

        self.action_result = tk.Label(self, text="", fg=ACCENT, bg=BG_COLOR,
                                      font=("SF Pro", 10), wraplength=650)
        self.action_result.pack(pady=(2, 4))

        sep2 = tk.Frame(self, bg=GRAY, height=1)
        sep2.pack(fill="x", padx=12, pady=6)

        # Launch buttons
        launch_frame = tk.Frame(self, bg=BG_COLOR)
        launch_frame.pack(fill="x", padx=16, pady=4)

        tk.Label(launch_frame, text="Lancer:", fg=TEXT_COLOR, bg=BG_COLOR,
                 font=("SF Pro", 11, "bold")).pack(side="left", padx=(0, 8))

        for srv in SERVERS:
            sid = srv["id"]
            self._make_button(launch_frame, sid,
                              lambda s=srv: self._launch(s), width=5,
                              bg="#45475a").pack(side="left", padx=2)

        self._make_button(launch_frame, "TOUS",
                          self._launch_all, width=6, bg=GREEN,
                          fg="#1e1e2e").pack(side="left", padx=(10, 0))

        # Reset buttons
        reset_frame = tk.Frame(self, bg=BG_COLOR)
        reset_frame.pack(fill="x", padx=16, pady=4)

        tk.Label(reset_frame, text="Reset:", fg=TEXT_COLOR, bg=BG_COLOR,
                 font=("SF Pro", 11, "bold")).pack(side="left", padx=(0, 14))

        for srv in SERVERS:
            sid = srv["id"]
            self._make_button(reset_frame, sid,
                              lambda s=srv: self._reset(s), width=5,
                              bg=RED, fg="#1e1e2e").pack(side="left", padx=2)

        sep3 = tk.Frame(self, bg=GRAY, height=1)
        sep3.pack(fill="x", padx=12, pady=8)

        # Log area
        log_frame = tk.Frame(self, bg=BG_COLOR)
        log_frame.pack(fill="both", expand=True, padx=12, pady=(0, 8))

        tk.Label(log_frame, text="Log", fg=GRAY, bg=BG_COLOR,
                 font=("SF Pro", 10)).pack(anchor="w")

        self.log_text = tk.Text(log_frame, bg=CARD_BG, fg=TEXT_COLOR,
                                font=("SF Mono", 10), height=8,
                                insertbackground=TEXT_COLOR, wrap="word",
                                state="disabled")
        self.log_text.pack(fill="both", expand=True)

    def _make_button(self, parent, text, command, width=12, bg=BTN_BG, fg=TEXT_COLOR):
        btn = tk.Button(parent, text=text, command=command, width=width,
                        bg=bg, fg=fg, activebackground=BTN_HOVER,
                        activeforeground=fg, relief="flat", bd=0,
                        font=("SF Pro", 10, "bold"), cursor="hand2",
                        padx=6, pady=4)
        return btn

    def _log(self, msg):
        ts = time.strftime("%H:%M:%S")
        self.log_text.config(state="normal")
        self.log_text.insert("end", f"[{ts}] {msg}\n")
        self.log_text.see("end")
        self.log_text.config(state="disabled")

    def _schedule_refresh(self):
        threading.Thread(target=self._refresh_all, daemon=True).start()
        self.after(REFRESH_INTERVAL_MS, self._schedule_refresh)

    def _refresh_all(self):
        results = {}
        for srv in SERVERS:
            try:
                data = api_call(srv["url"], "/status", timeout=5)
            except Exception:
                data = {"_error": "timeout"}
            results[srv["id"]] = data

        self._server_data = results
        self.after(0, lambda r=results: self._apply_refresh(r))

    def _apply_refresh(self, results):
        for sid, data in results.items():
            try:
                self.cards[sid].update_status(data)
            except Exception as e:
                _dbg(f"UI update error {sid}: {e}")
        self._update_global()

    def _update_global(self):
        td = 0
        ta = 0
        for sid, data in self._server_data.items():
            if "_error" not in data:
                prompts = data.get("prompts", {})
                td += prompts.get("done", 0)
                ta += prompts.get("total", 0)
        if ta > 0:
            pct = (td / ta) * 100
            self.global_progress_var.set(pct)
            self.global_label.config(text=f"{td}/{ta}  ({pct:.0f}%)")
        else:
            self.global_progress_var.set(0)
            self.global_label.config(text="-- / --")

    def _threaded_action(self, label, func):
        self.action_result.config(text=f"{label}...", fg=YELLOW)
        self._log(f"{label}...")

        def run():
            try:
                result = func()
                msg = json.dumps(result, ensure_ascii=False) if isinstance(result, dict) else str(result)
                self.after(0, lambda: self.action_result.config(text=msg, fg=GREEN))
                self._log_safe(f"{label} -> {msg}")
            except Exception as e:
                self.after(0, lambda: self.action_result.config(text=f"Erreur: {e}", fg=RED))
                self._log_safe(f"{label} ERREUR: {e}")

        threading.Thread(target=run, daemon=True).start()

    def _log_safe(self, msg):
        self.after(0, lambda: self._log(msg))

    def _distribute(self):
        self._threaded_action("Distribuer les prompts",
                              lambda: api_call(SERVERS[0]["url"], "/distribute-prompts", "POST"))

    def _mark_ready(self):
        self._threaded_action("Marquer READY (USA)",
                              lambda: api_call(SERVERS[0]["url"], "/mark-ready", "POST"))

    def _retry_errors(self):
        self._threaded_action("Relancer les erreurs",
                              lambda: api_call(SERVERS[0]["url"], "/retry-errors", "POST"))

    def _launch(self, srv):
        self._threaded_action(f"Lancer {srv['id']}",
                              lambda: api_call(srv["url"], "/process?country=USA", "POST"))

    def _launch_all(self):
        def do_all():
            results = {}
            for srv in SERVERS:
                r = api_call(srv["url"], "/process?country=USA", "POST")
                results[srv["id"]] = r.get("status", r.get("_error", "?"))
            return results
        self._threaded_action("Lancer TOUS", do_all)

    def _reset(self, srv):
        if not messagebox.askyesno("Reset", f"Reset {srv['id']} ?\nCa va stopper le processing et remettre les prompts en READY."):
            return
        self._threaded_action(f"Reset {srv['id']}",
                              lambda: api_call(srv["url"], "/reset", "POST"))


def main():
    app = ControlPanel()
    app.mainloop()


if __name__ == "__main__":
    main()
