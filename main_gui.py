# main_gui.py  — SGSN Report Tool v2
# All input files come from Windows Downloads folder: C:\Users\HP\Downloads\
# Nokia SGSN DB tables populated by separate Java/NetAct script.

import tkinter as tk
from tkinter import ttk, scrolledtext, messagebox, filedialog
import threading
import subprocess
import os
import os.path
from datetime import datetime, timedelta

from downld_sgsn     import download_sgsn_summary, download_peak_throughput
from report_nokia    import generate_nokia_report
from report_4g       import generate_4g_report
from report_combined import generate_combined_report
from report_monthly  import generate_monthly_report
from report_trai     import generate_trai_report
from report_daily    import generate_daily_report
from csv_uploader    import upload_lan_all, upload_prtg_all, upload_all
from file_resolver   import resolve_all, example_filenames, DOWNLOADS, resolve_tcs_file
from tcs_store       import store_tcs, load_tcs_day
from config          import OUTPUT_DIR

# Make path helpers module-level so they never go out of scope
_basename = os.path.basename
_isfile   = os.path.isfile
_abspath  = os.path.abspath
_makedirs = os.makedirs

MONTHS = ["January","February","March","April","May","June",
          "July","August","September","October","November","December"]

C = {
    "topbar":   "#1F3864", "topbar_fg":  "#FFFFFF",
    "datebar":  "#E8EFF6", "datebar_fg": "#1F3864",
    "blue":     "#2E75B6", "blue_fg":    "#FFFFFF",
    "green":    "#217346", "green_fg":   "#FFFFFF",
    "orange":   "#C45911", "orange_fg":  "#FFFFFF",
    "purple":   "#7B2D8B", "purple_fg":  "#FFFFFF",
    "gray":     "#555555", "gray_fg":    "#FFFFFF",
    "teal":     "#0D6E72", "teal_fg":    "#FFFFFF",
    "red":      "#C00000", "red_fg":     "#FFFFFF",
    "section":  "#FFFFFF",
    "log_bg":   "#0D1117", "log_fg":     "#58A6FF",
    "log_ok":   "#3FB950", "log_err":    "#F85149",
    "log_warn": "#D29922",
    "status_bg":"#1F3864", "status_fg":  "#90B8D8",
}


class SGSNApp(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("SGSN Report Tool  —  BSNL Gujarat NOC")
        self.geometry("1200x820")
        self.minsize(1000, 700)
        self.configure(bg=C["topbar"])
        self._tcs_file = tk.StringVar(value="")
        self._build_ui()
        self._set_today()

    # ── BUILD ─────────────────────────────────────────────────────────────────

    def _build_ui(self):
        self._build_topbar()
        self._build_datebar()
        main = tk.Frame(self, bg="#F0F4F8")
        main.pack(fill=tk.BOTH, expand=True)
        left = tk.Frame(main, bg="#F0F4F8")
        left.pack(side=tk.LEFT, fill=tk.BOTH, expand=True, padx=(8,4), pady=8)
        right = tk.Frame(main, bg="#F0F4F8", width=290)
        right.pack(side=tk.RIGHT, fill=tk.Y, padx=(4,8), pady=8)
        right.pack_propagate(False)
        self._build_reports(left)
        self._build_upload(left)
        self._build_log(right)
        self._build_summary(right)
        self._build_statusbar()

    def _build_topbar(self):
        bar = tk.Frame(self, bg=C["topbar"], height=46)
        bar.pack(fill=tk.X); bar.pack_propagate(False)
        tk.Label(bar, text="SGSN Report Tool", bg=C["topbar"], fg=C["topbar_fg"],
                 font=("Arial",15,"bold")).pack(side=tk.LEFT, padx=16)
        tk.Label(bar, text="BSNL Gujarat NOC", bg=C["topbar"], fg="#90B8D8",
                 font=("Arial",10)).pack(side=tk.LEFT)
        tk.Label(bar, text="v2.0  |  Nokia SGSN  |  ZTE decommissioned",
                 bg=C["topbar"], fg="#90B8D8", font=("Arial",10)).pack(side=tk.RIGHT, padx=16)

    def _build_datebar(self):
        bar = tk.Frame(self, bg=C["datebar"], height=38)
        bar.pack(fill=tk.X); bar.pack_propagate(False)
        tk.Label(bar, text="Date:", bg=C["datebar"], fg=C["datebar_fg"],
                 font=("Arial",10,"bold")).pack(side=tk.LEFT, padx=(14,4))
        self.date_var = tk.StringVar()
        tk.Entry(bar, textvariable=self.date_var, width=11,
                 font=("Arial",11,"bold"), fg=C["datebar_fg"],
                 justify=tk.CENTER, relief=tk.GROOVE, bd=2).pack(side=tk.LEFT, pady=5)
        for txt, cmd in [("Prev",self._prev_day),("Today",self._set_today),("Next",self._next_day)]:
            tk.Button(bar, text=txt, command=cmd, bg=C["blue"], fg=C["blue_fg"],
                      font=("Arial",9,"bold"), relief=tk.FLAT,
                      padx=8, pady=2).pack(side=tk.LEFT, padx=3, pady=6)
        tk.Frame(bar, bg="#C8D8EC", width=1).pack(side=tk.LEFT, fill=tk.Y, padx=8)
        tk.Label(bar, text="Month:", bg=C["datebar"], fg=C["datebar_fg"],
                 font=("Arial",10,"bold")).pack(side=tk.LEFT, padx=(4,4))
        self.month_var = tk.StringVar()
        ttk.Combobox(bar, textvariable=self.month_var, values=MONTHS,
                     width=10, state="readonly", font=("Arial",10)).pack(side=tk.LEFT, pady=6)
        tk.Label(bar, text="Year:", bg=C["datebar"], fg=C["datebar_fg"],
                 font=("Arial",10,"bold")).pack(side=tk.LEFT, padx=(8,4))
        self.year_var = tk.StringVar()
        tk.Entry(bar, textvariable=self.year_var, width=6, font=("Arial",10),
                 justify=tk.CENTER, relief=tk.GROOVE, bd=2).pack(side=tk.LEFT, pady=6)
        tk.Frame(bar, bg="#C8D8EC", width=1).pack(side=tk.LEFT, fill=tk.Y, padx=8)
        tk.Button(bar, text="Check Files", command=self._check_files_status,
                  bg=C["gray"], fg=C["gray_fg"], font=("Arial",9,"bold"),
                  relief=tk.FLAT, padx=8, pady=2).pack(side=tk.LEFT, padx=2, pady=6)

    def _section(self, parent, title, fg_color="blue"):
        outer = tk.LabelFrame(parent, text=f"  {title}  ",
                               bg=C["section"], fg=C[fg_color],
                               font=("Arial",9,"bold"), relief=tk.GROOVE, bd=1)
        outer.pack(fill=tk.X, pady=3)
        return outer

    def _btn(self, parent, text, cmd, color="blue", width=18):
        bg = C[color]; fg = C[color+"_fg"]
        b = tk.Button(parent, text=text, command=cmd, bg=bg, fg=fg,
                      font=("Arial",9,"bold"), relief=tk.FLAT,
                      padx=6, pady=4, width=width, cursor="hand2")
        b.bind("<Enter>", lambda e: b.config(bg=self._dk(bg)))
        b.bind("<Leave>", lambda e: b.config(bg=bg))
        return b

    def _dk(self, h):
        r,g,b = int(h[1:3],16), int(h[3:5],16), int(h[5:7],16)
        return "#{:02x}{:02x}{:02x}".format(max(0,r-28), max(0,g-28), max(0,b-28))

    def _build_reports(self, parent):
        s = self._section(parent, "Nokia SGSN  (2G + 3G)  — report buttons")
        f = tk.Frame(s, bg=C["section"]); f.pack(fill=tk.X, padx=8, pady=5)
        for txt, fn in [
            ("Nokia Daily Report", self._run_nokia),
            ("Nokia Volume",       lambda: self._run(generate_nokia_report, "Nokia Vol")),
            ("Nokia Throughput",   lambda: self._run(generate_nokia_report, "Nokia Tput")),
            ("Nokia Session",      lambda: self._run(generate_nokia_report, "Nokia Sess")),
            ("Nokia PDP",          lambda: self._run(generate_nokia_report, "Nokia PDP")),
            ("Nokia Attach",       lambda: self._run(generate_nokia_report, "Nokia Att")),
            ("Nokia SBC KPI",      lambda: self._run(generate_nokia_report, "Nokia SBC")),
        ]:
            self._btn(f, txt, fn, "blue", 16).pack(side=tk.LEFT, padx=3)

        s = self._section(parent, "Nokia 4G LTE  — report buttons", "teal")
        f = tk.Frame(s, bg=C["section"]); f.pack(fill=tk.X, padx=8, pady=5)
        for txt, fn in [
            ("4G Daily Report",    self._run_4g),
            ("4G Volume",          lambda: self._run(generate_4g_report, "4G Vol")),
            ("4G VoLTE Stats",     lambda: self._run(generate_4g_report, "4G VoLTE")),
            ("4G Bearer Stats",    lambda: self._run(generate_4g_report, "4G Bearer")),
            ("4G EMM Users",       lambda: self._run(generate_4g_report, "4G EMM")),
            ("4G Peak Throughput", lambda: self._run(generate_4g_report, "4G Peak")),
            ("4G MOS / QoS",       lambda: self._run(generate_4g_report, "4G MOS")),
        ]:
            self._btn(f, txt, fn, "teal", 17).pack(side=tk.LEFT, padx=3)

        s = self._section(parent, "Combined & Summary Reports")
        f = tk.Frame(s, bg=C["section"]); f.pack(fill=tk.X, padx=8, pady=5)
        self._btn(f, "Daily Report (8-Sheet)", self._run_daily,    "red",   20).pack(side=tk.LEFT, padx=3)
        self._btn(f, "Combined (2G+3G+4G)",   self._run_combined, "blue",  20).pack(side=tk.LEFT, padx=3)
        self._btn(f, "Download Summary",       self._run_download, "orange",16).pack(side=tk.LEFT, padx=3)
        self._btn(f, "Monthly Report",         self._run_monthly,  "purple",15).pack(side=tk.LEFT, padx=3)
        self._btn(f, "TRAI Report",            self._run_trai,     "gray",  12).pack(side=tk.LEFT, padx=3)
        self._btn(f, "Open Output Folder",     self._open_folder,  "green", 16).pack(side=tk.LEFT, padx=3)

    def _build_upload(self, parent):
        s = self._section(parent,
            "Upload from Downloads  —  7 LAN + 7 Nokia 4G PRTG (→ PostgreSQL) + 1 TCS EMS (→ MySQL)  |  Nokia SGSN via Java/NetAct",
            "green")
        info = tk.Frame(s, bg="#EAF5EE"); info.pack(fill=tk.X, padx=8, pady=(5,3))
        tk.Label(info, text="All historicdata files auto-picked from:",
                 bg="#EAF5EE", fg=C["green"], font=("Arial",9)).pack(side=tk.LEFT, padx=4)
        tk.Label(info, text=DOWNLOADS,
                 bg="#EAF5EE", fg="#145230", font=("Courier New",9,"bold")).pack(side=tk.LEFT, padx=4)

        row = tk.Frame(s, bg=C["section"]); row.pack(fill=tk.X, padx=8, pady=4)

        # LAN block
        lan = tk.LabelFrame(row, text=" LAN Switch  (7 files) ", bg=C["section"],
                             fg=C["green"], font=("Arial",8,"bold"), relief=tk.GROOVE)
        lan.pack(side=tk.LEFT, fill=tk.BOTH, expand=True, padx=4)
        for fname in ["historicdata.csv"] + [f"historicdata ({i}).csv" for i in range(1,7)]:
            tk.Label(lan, text=f"  {fname}", bg=C["section"], fg="#555",
                     font=("Courier New",8), anchor="w").pack(fill=tk.X)
        self._btn(lan, "Upload LAN", self._upload_lan, "green", 14).pack(pady=5)

        # PRTG block
        prtg = tk.LabelFrame(row, text=" Nokia 4G PRTG  (7 files) ", bg=C["section"],
                              fg="#854F0B", font=("Arial",8,"bold"), relief=tk.GROOVE)
        prtg.pack(side=tk.LEFT, fill=tk.BOTH, expand=True, padx=4)
        for fname in [f"historicdata ({i}).csv" for i in range(7,14)]:
            tk.Label(prtg, text=f"  {fname}", bg=C["section"], fg="#555",
                     font=("Courier New",8), anchor="w").pack(fill=tk.X)
        self._btn(prtg, "Upload PRTG", self._upload_prtg, "orange", 14).pack(pady=5)

        # TCS block
        tcs = tk.LabelFrame(row, text=" TCS 4G EMS  (1 file) ", bg=C["section"],
                             fg="#217346", font=("Arial",8,"bold"), relief=tk.GROOVE)
        tcs.pack(side=tk.LEFT, fill=tk.BOTH, expand=True, padx=4)
        for line in ["WZ_Gujarat_RAN_KPIS","_eNBwise_Daily_","DD_MM_YYYY.csv",
                     "(not uploaded to DB)","(read at report time)"]:
            tk.Label(tcs, text=f"  {line}", bg=C["section"], fg="#888" if "(" in line else "#555",
                     font=("Courier New",8 if "(" not in line else 7), anchor="w").pack(fill=tk.X)
        tcs_lbl = tk.Label(tcs, textvariable=self._tcs_file, bg="#EAF5EE", fg="#217346",
                           font=("Courier New",7), anchor="w", wraplength=155, justify=tk.LEFT)
        tcs_lbl.pack(fill=tk.X, padx=4, pady=3)
        self._btn(tcs, "Browse TCS File",      self._browse_tcs,   "green",  14).pack(pady=2)
        self._btn(tcs, "Auto-Find TCS",         self._auto_tcs,     "gray",   14).pack(pady=2)
        self._btn(tcs, "Store TCS → MySQL",     self._store_tcs,    "purple", 14).pack(pady=2)

        # Nokia SGSN info (Java/NetAct)
        nok = tk.LabelFrame(row, text=" Nokia SGSN  (Java/NetAct) ", bg=C["section"],
                             fg=C["blue"], font=("Arial",8,"bold"), relief=tk.GROOVE)
        nok.pack(side=tk.LEFT, fill=tk.BOTH, expand=True, padx=4)
        for line in ["nokia_sgsn_report","nokia_4g_sgsn_report",
                     "nokia_sgsn_report_pdp_out","nokia_sgsn_report_att_out",
                     "sgsn_report_sbc_kpi","volte_bearer_act","p_obs_4g"]:
            tk.Label(nok, text=f"  {line}", bg=C["section"], fg="#888",
                     font=("Courier New",7), anchor="w").pack(fill=tk.X)
        tk.Label(nok, text="\n  Populated automatically\n  by Java/NetAct script.\n  No action needed here.",
                 bg="#F0F5FF", fg="#2E75B6", font=("Arial",8), justify=tk.LEFT).pack(
                     fill=tk.X, padx=4, pady=4)

        self._btn(s, "Upload ALL  (LAN + PRTG + TCS  —  15 files from Downloads)",
                  self._upload_all, "green", 58).pack(pady=(2,8), padx=8)

    def _build_log(self, parent):
        tk.Label(parent, text="Activity Log", bg="#F0F4F8", fg=C["datebar_fg"],
                 font=("Arial",9,"bold")).pack(anchor="w")
        self.log_box = scrolledtext.ScrolledText(
            parent, bg=C["log_bg"], fg=C["log_fg"],
            font=("Courier New",8), state=tk.DISABLED, height=24, wrap=tk.WORD)
        self.log_box.tag_config("ok",   foreground=C["log_ok"])
        self.log_box.tag_config("err",  foreground=C["log_err"])
        self.log_box.tag_config("warn", foreground=C["log_warn"])
        self.log_box.tag_config("info", foreground=C["log_fg"])
        self.log_box.pack(fill=tk.BOTH, expand=True, pady=(2,4))
        tk.Button(parent, text="Clear Log", command=self._clear_log,
                  bg="#333", fg="#aaa", font=("Arial",8), relief=tk.FLAT,
                  pady=2).pack(anchor="e")
        self._log("info", "[System] SGSN Report Tool started")
        self._log("info", f"[System] Downloads: {DOWNLOADS}")
        self._log("info", "[System] LAN: historicdata.csv + historicdata(1-6)")
        self._log("info", "[System] PRTG: historicdata(7-13)")
        self._log("info", "[System] TCS: WZ_Gujarat_RAN_KPIS_eNBwise_Daily_*.csv")
        self._log("info", "[System] Nokia SGSN DB: via Java/NetAct (automatic)")
        self._log("ok",   "[System] Ready")

    def _build_summary(self, parent):
        sf = tk.LabelFrame(parent, text=" Last Summary ", bg=C["section"],
                           fg=C["blue"], font=("Arial",9,"bold"), relief=tk.GROOVE)
        sf.pack(fill=tk.X, pady=(6,0))
        self.sum_vars = {}
        for label, key in [
            ("Nokia 2G/3G", "nokia"),
            ("DT / LAN",    "lan"),
            ("Nokia 4G",    "g4"),
            ("TCS 4G",      "tcs"),
            ("ZTE 2G/3G",   "zte"),
            ("Grand Total", "total"),
        ]:
            r = tk.Frame(sf, bg=C["section"]); r.pack(fill=tk.X, padx=8, pady=1)
            tk.Label(r, text=label+":", bg=C["section"], fg="#555",
                     font=("Arial",9)).pack(side=tk.LEFT)
            v = tk.StringVar(value="—")
            self.sum_vars[key] = v
            fg = C["orange"] if key == "total" else C["blue"]
            tk.Label(r, textvariable=v, bg=C["section"], fg=fg,
                     font=("Arial",9,"bold")).pack(side=tk.RIGHT)
        tf = tk.Frame(sf, bg=C["section"]); tf.pack(fill=tk.X, padx=8, pady=(3,5))
        tk.Label(tf, text="TCS file:", bg=C["section"], fg="#555",
                 font=("Arial",8)).pack(side=tk.LEFT)
        self._tcs_status     = tk.StringVar(value="Not selected")
        self._tcs_status_lbl = tk.Label(tf, textvariable=self._tcs_status,
                                         bg=C["section"], fg="#C45911",
                                         font=("Arial",8,"bold"))
        self._tcs_status_lbl.pack(side=tk.RIGHT)
        def _upd_tcs(*_):
            p = self._tcs_file.get()
            if p and _isfile(p):
                self._tcs_status.set(_basename(p))
                self._tcs_status_lbl.config(fg="#217346")
            else:
                self._tcs_status.set("Not selected")
                self._tcs_status_lbl.config(fg="#C45911")
        self._tcs_file.trace_add("write", _upd_tcs)

    def _build_statusbar(self):
        self.status_var = tk.StringVar(value="Ready")
        tk.Label(self, textvariable=self.status_var, bg=C["status_bg"], fg=C["status_fg"],
                 font=("Arial",9), anchor="w", padx=12).pack(side=tk.BOTTOM, fill=tk.X)

    # ── HELPERS ───────────────────────────────────────────────────────────────

    def _log(self, tag, msg):
        self.log_box.config(state=tk.NORMAL)
        ts = datetime.now().strftime("%H:%M:%S")
        self.log_box.insert(tk.END, f"[{ts}] {msg}\n", tag)
        self.log_box.see(tk.END)
        self.log_box.config(state=tk.DISABLED)

    def _clear_log(self):
        self.log_box.config(state=tk.NORMAL)
        self.log_box.delete(1.0, tk.END)
        self.log_box.config(state=tk.DISABLED)

    def _status(self, msg):  self.status_var.set(msg)
    def _date(self):         return self.date_var.get().strip()
    def _month_year(self):
        return MONTHS.index(self.month_var.get()) + 1, int(self.year_var.get())

    def _set_today(self):
        y = datetime.today() - timedelta(days=1)
        self.date_var.set(y.strftime("%Y-%m-%d"))
        self.month_var.set(MONTHS[y.month - 1])
        self.year_var.set(str(y.year))

    def _prev_day(self):
        d = datetime.strptime(self._date(), "%Y-%m-%d") - timedelta(days=1)
        self.date_var.set(d.strftime("%Y-%m-%d"))

    def _next_day(self):
        d = datetime.strptime(self._date(), "%Y-%m-%d") + timedelta(days=1)
        self.date_var.set(d.strftime("%Y-%m-%d"))

    def _open_folder(self):
        path = _abspath(OUTPUT_DIR)
        _makedirs(path, exist_ok=True)
        subprocess.Popen(f'explorer "{path}"')

    def _browse_tcs(self):
        path = filedialog.askopenfilename(
            title="Select TCS EMS 4G eNBwise Daily CSV",
            filetypes=[("TCS EMS CSV","*.csv"), ("All files","*.*")],
            initialdir=DOWNLOADS,
        )
        if path:
            self._tcs_file.set(path)
            self._log("ok", f"[TCS] Selected: {_basename(path)}")

    def _auto_tcs(self):
        date = self._date()
        info = resolve_tcs_file(date)
        if info["exists"]:
            self._tcs_file.set(info["path"])
            self._log("ok", f"[TCS] Found: {_basename(info['path'])}")
        else:
            # Strict: file for exact date not found — do NOT suggest wrong date file
            dt = datetime.strptime(date, "%Y-%m-%d")
            expected = (f"WZ_Gujarat_RAN_KPIS_eNBwise_Daily_"
                        f"{dt.day:02d}_{dt.month:02d}_{dt.year}.csv")
            self._log("warn", f"[TCS] File NOT found for {date}.")
            self._log("warn", f"[TCS] Expected: {expected}")
            self._log("warn", f"[TCS] Location: {DOWNLOADS}\\")
            self._log("warn", "[TCS] TCS 4G columns will be ZERO if you proceed.")
            messagebox.showwarning(
                "TCS File Missing",
                f"TCS EMS file not found for {date}:\n\n"
                f"  {expected}\n\n"
                f"Location: {DOWNLOADS}\\\n\n"
                "Copy the correct TCS file and click Auto-Find TCS again.\n"
                "TCS 4G columns will be ZERO if you run without it."
            )

    def _store_tcs(self):
        """Read currently selected TCS file and save to MySQL sgsn_tcs_daily."""
        tcs_path = self._tcs_file.get().strip()
        if not tcs_path or not _isfile(str(tcs_path)):
            # Try auto-find first
            info = resolve_tcs_file(self._date())
            if info["exists"]:
                tcs_path = info["path"]
                self._tcs_file.set(tcs_path)
            else:
                messagebox.showwarning("No TCS File",
                    "No TCS file selected or found.\n"
                    "Use Browse TCS File or Auto-Find TCS first.")
                return
        _path = tcs_path
        _log  = lambda m: self.after(0, lambda msg=m: self._log("info", msg))
        def _do():
            from tcs_reader import read_tcs_daily
            data = read_tcs_daily(_path)
            if not data:
                self.after(0, lambda: self._log("err", "[TCS Store] Could not read TCS file."))
                return
            data['day'] = int(str(data.get('date', '')).split('-')[-1] or 0)
            store_tcs(data, log=_log)
            self.after(0, lambda: self._log("ok",
                f"[TCS Store] Saved to MySQL: "
                f"DL={data['dl_mb']/1024:.0f}GB  UL={data['ul_mb']/1024:.0f}GB"))
        self._thread(_do)

    # ── THREADING ─────────────────────────────────────────────────────────────

    def _thread(self, fn):
        def worker():
            try:
                fn()
            except FileNotFoundError as e:
                msg = str(e)
                self._log("err", msg)
                self.after(0, lambda: messagebox.showerror("File Not Found", msg))
            except ValueError as e:
                msg = str(e)
                # Date mismatch gets a special large warning
                if "DATE MISMATCH" in msg or "UPLOAD BLOCKED" in msg:
                    self._log("err", f"[DATE MISMATCH] {msg}")
                    self.after(0, lambda m=msg: messagebox.showerror(
                        "⚠  DATE MISMATCH — UPLOAD BLOCKED",
                        m
                    ))
                else:
                    self._log("err", f"[ERROR] {msg}")
                    self.after(0, lambda m=msg: messagebox.showerror("Error", m))
            except Exception as e:
                msg = str(e)
                self._log("err", f"[ERROR] {msg}")
                self.after(0, lambda: messagebox.showerror("Error", msg))
            finally:
                self.after(0, lambda: self._status("Ready"))
        threading.Thread(target=worker, daemon=True).start()

    def _run(self, fn, name):
        date  = self._date()
        _name = name
        self._log("info", f"[{_name}] Starting for {date}...")
        self._status(f"Running {_name}...")
        def _do():
            fn(date, log=lambda m: self.after(0, lambda msg=m: self._log("info", msg)))
            self.after(0, lambda: self._log("ok", f"[{_name}] Done — saved to {OUTPUT_DIR}\\"))
            self.after(0, lambda: self._status(f"[{_name}] Done"))
        self._thread(_do)

    # ── REPORT ACTIONS ────────────────────────────────────────────────────────

    def _run_daily(self):
        date     = self._date()
        tcs_path = self._tcs_file.get().strip() or None

        # Auto-find TCS if not already selected
        if not tcs_path or not _isfile(str(tcs_path)):
            info = resolve_tcs_file(date)
            if info["exists"]:
                tcs_path = info["path"]
                self._tcs_file.set(tcs_path)
                self._log("ok", f"[TCS] Auto-found: {_basename(tcs_path)}")
            else:
                ans = messagebox.askyesno(
                    "TCS EMS File Not Found",
                    f"TCS EMS file not found in:\n{DOWNLOADS}\n\n"
                    "TCS 4G columns will be ZERO.\n\nContinue without TCS data?"
                )
                if not ans:
                    return
                tcs_path = None

        self._log("info", f"[Daily Report] Starting 8-sheet report for {date}...")
        if tcs_path:
            self._log("info", f"[Daily Report] TCS: {_basename(tcs_path)}")
        else:
            self._log("warn", "[Daily Report] No TCS file — TCS columns = 0")
        self._status("Running Daily Report (8 sheets)...")

        # Capture everything needed by the thread as plain local variables
        _date     = date
        _tcs      = tcs_path
        _self     = self

        def _do():
            fp    = generate_daily_report(
                _date, tcs_file=_tcs,
                log=lambda m: _self.after(0, lambda msg=m: _self._log("info", msg))
            )
            fname = _basename(fp)
            _self.after(0, lambda: _self._log("ok", f"[Daily Report] Saved: {fname}"))
            _self.after(0, lambda: _self._status(f"Done: {fname}"))
            # Refresh Last Summary from MySQL after report
            try:
                from tcs_store import load_tcs_day
                _tcs_row = load_tcs_day(_date)
                _tcs_gb  = round((_tcs_row["dl_mb"] + _tcs_row["ul_mb"]) / 1024.0, 2) if _tcs_row else 0
                _self.after(0, lambda t=_tcs_gb: [
                    _self.sum_vars["tcs"].set(f"{t:,.2f} GB" if t else "— (no TCS)")
                ])
            except Exception:
                pass

        self._thread(_do)

    def _run_nokia(self):    self._run(generate_nokia_report,    "Nokia Report")
    def _run_4g(self):       self._run(generate_4g_report,       "4G Report")
    def _run_combined(self): self._run(generate_combined_report, "Combined Report")

    def _run_trai(self):
        m, y = self._month_year()
        self._log("info", f"[TRAI] Starting for {y}-{m:02d}...")
        self._status("Running TRAI Report...")
        def _do():
            generate_trai_report(y, m,
                log=lambda msg: self.after(0, lambda x=msg: self._log("info", x)))
            self.after(0, lambda: self._log("ok", "[TRAI] Done"))
        self._thread(_do)

    def _run_monthly(self):
        m, y = self._month_year()
        self._log("info", f"[Monthly] Starting for {y}-{m:02d}...")
        self._status("Running Monthly Report...")
        def _do():
            generate_monthly_report(y, m,
                log=lambda msg: self.after(0, lambda x=msg: self._log("info", x)))
            self.after(0, lambda: self._log("ok", "[Monthly] Done"))
        self._thread(_do)

    def _run_download(self):
        date = self._date()
        self._log("info", f"[Download Summary] Starting for {date}...")
        self._status("Running Download Summary...")
        def _do():
            _log = lambda msg: self.after(0, lambda x=msg: self._log("info", x))
            # Step 1: peak throughput (needed by TRAI populate)
            download_peak_throughput(date, log=_log)
            # Step 2: main summary (volumes + DT calculation)
            result = download_sgsn_summary(date, log=_log)
            if result:
                # downld_sgsn returns all values in GB including TCS
                _nok = result.get("nokia_gb",    0)
                _dt  = result.get("dt_gb",       0)   # DT = NIB - Nokia - ZTE
                _g4  = result.get("nokia_4g_gb", 0)
                _zte = result.get("zte_gb",      0)
                _tcs = result.get("tcs_gb",      0)
                _tot = result.get("total_gb",    0)
                self.after(0, lambda n=_nok, d=_dt, g=_g4, t=_tcs, z=_zte, tot=_tot: [
                    self.sum_vars["nokia"].set(f"{n:,.2f} GB"),
                    self.sum_vars["lan"].set(f"{d:,.2f} GB"),
                    self.sum_vars["g4"].set(f"{g:,.2f} GB"),
                    self.sum_vars["tcs"].set(f"{t:,.2f} GB" if t else "— (no TCS)"),
                    self.sum_vars["zte"].set(f"{z:,.2f} GB"),
                    self.sum_vars["total"].set(f"{tot:,.2f} GB"),
                ])
            self.after(0, lambda: self._log("ok", "[Download Summary] Done"))
        self._thread(_do)

    # ── UPLOAD ACTIONS ────────────────────────────────────────────────────────

    def _upload_lan(self):
        date = self._date()
        self._log("warn", f"[LAN Upload] Scanning {DOWNLOADS} for historicdata files...")
        self._status("Uploading LAN Switch files...")
        def _do():
            upload_lan_all(date,
                log=lambda m: self.after(0, lambda x=m: self._log("info", x)))
            self.after(0, lambda: self._log("ok", "[LAN Upload] Complete"))
        self._thread(_do)

    def _upload_prtg(self):
        date = self._date()
        self._log("warn", f"[PRTG Upload] Scanning {DOWNLOADS} for historicdata(7-13)...")
        self._status("Uploading Nokia 4G PRTG files...")
        def _do():
            upload_prtg_all(date,
                log=lambda m: self.after(0, lambda x=m: self._log("info", x)))
            self.after(0, lambda: self._log("ok", "[PRTG Upload] Complete"))
        self._thread(_do)

    def _upload_all(self):
        date = self._date()
        self._log("warn", f"[Upload ALL] LAN + PRTG + TCS from {DOWNLOADS}...")
        self._status("Uploading all 14 files...")
        def _do():
            upload_all(date,
                log=lambda m: self.after(0, lambda x=m: self._log("info", x)))
            self.after(0, lambda: self._log("ok", "[Upload ALL] 14 PRTG/LAN rows + TCS stored to MySQL"))
        self._thread(_do)

    # ── CHECK FILES ───────────────────────────────────────────────────────────

    def _check_files_status(self):
        from csv_uploader import detect_csv_date
        date  = self._date()
        self._log("info", f"[Check Files] Scanning for {date}...")
        files   = resolve_all(date)
        found   = [f for f in files if f["exists"]]
        missing = [f for f in files if not f["exists"]]
        total   = len(files)
        tag = "ok" if not missing else "warn"
        self._log(tag,
            f"[Check Files] {len(found)}/{total} files found  "
            f"(7 LAN + 7 PRTG + 1 TCS  |  Nokia SGSN via Java)")
        for f in missing:
            self._log("err", f"  MISSING: {f['path']}")

        # ── Date validation check ──────────────────────────────────────
        mismatch = False
        for grp, label in [
            ([f for f in files if "LAN" in f.get("description","")], "LAN"),
            ([f for f in files if "PRTG" in f.get("description","")], "PRTG"),
        ]:
            for f in grp:
                if f["exists"]:
                    csv_dt = detect_csv_date(f["path"])
                    if csv_dt and csv_dt != date:
                        from datetime import datetime as _dt
                        cd = _dt.strptime(csv_dt, "%Y-%m-%d").strftime("%d-%b-%Y")
                        sd = _dt.strptime(date,   "%Y-%m-%d").strftime("%d-%b-%Y")
                        self._log("err",
                            f"  ⚠ DATE MISMATCH: {label} file contains {cd} "
                            f"data but selected date is {sd}!")
                        mismatch = True
                    elif csv_dt == date:
                        self._log("ok", f"  {label} date check: {csv_dt} ✓")
                    break  # check only first file per group

        if mismatch:
            self._log("err",
                "[Check Files] ⚠ DATE MISMATCH DETECTED — "
                "change date to match the files before uploading!")
            messagebox.showwarning(
                "Date Mismatch",
                f"CSV files do NOT match selected date: {date}\n\n"
                "Change the date in the tool to match your CSV files\n"
                "BEFORE clicking Upload ALL."
            )
        elif not missing:
            self._log("ok", "[Check Files] All 15 files present — date verified — ready!")
        else:
            ex = example_filenames(date)
            self._log("warn", "[Check Files] Expected filenames:")
            for folder, names in ex.items():
                self._log("info", f"  {folder}:")
                for n in names:
                    self._log("info", f"      {n}")


if __name__ == "__main__":
    app = SGSNApp()
    app.mainloop()
