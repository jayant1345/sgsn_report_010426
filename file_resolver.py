# file_resolver.py
# Resolves all input file paths for SGSN Report Tool.
#
# ALL FILES COME FROM WINDOWS DOWNLOADS FOLDER:
#   historicdata.csv              → LAN Switch switch #1  (base file, no number)
#   historicdata (1).csv          → LAN Switch switch #2
#   historicdata (2).csv          → LAN Switch switch #3
#   historicdata (3).csv          → LAN Switch switch #4
#   historicdata (4).csv          → LAN Switch switch #5
#   historicdata (5).csv          → LAN Switch switch #6
#   historicdata (6).csv          → LAN Switch switch #7
#   historicdata (7).csv          → Nokia 4G PRTG port #1
#   historicdata (8).csv          → Nokia 4G PRTG port #2
#   historicdata (9).csv          → Nokia 4G PRTG port #3
#   historicdata (10).csv         → Nokia 4G PRTG port #4
#   historicdata (11).csv         → Nokia 4G PRTG port #5
#   historicdata (12).csv         → Nokia 4G PRTG port #6
#   historicdata (13).csv         → Nokia 4G PRTG port #7
#   WZ_Gujarat_RAN_KPIS_eNBwise_Daily_DD_MM_YYYY.csv  → TCS 4G EMS daily
#
# Nokia SGSN DB tables are populated by a separate Java script (NetAct→PostgreSQL).
# This Python tool does NOT upload Nokia SGSN data.

import os
import glob
from datetime import datetime

# Standard Windows Downloads folder
# PC username is HP — Downloads folder is fixed
DOWNLOADS = r"C:\Users\HP\Downloads"

# LAN Switch: base file (no number) + (1) to (6) = 7 files total
LAN_FILES = [
    ("lan_base", "historicdata.csv",      "LAN Switch #1 (base)"),
    ("lan_1",    "historicdata (1).csv",  "LAN Switch #2"),
    ("lan_2",    "historicdata (2).csv",  "LAN Switch #3"),
    ("lan_3",    "historicdata (3).csv",  "LAN Switch #4"),
    ("lan_4",    "historicdata (4).csv",  "LAN Switch #5"),
    ("lan_5",    "historicdata (5).csv",  "LAN Switch #6"),
    ("lan_6",    "historicdata (6).csv",  "LAN Switch #7"),
]

# Nokia 4G PRTG: (7) to (13) = 7 files
PRTG_FILES = [
    ("prtg_7",  "historicdata (7).csv",  "Nokia 4G PRTG #1"),
    ("prtg_8",  "historicdata (8).csv",  "Nokia 4G PRTG #2"),
    ("prtg_9",  "historicdata (9).csv",  "Nokia 4G PRTG #3"),
    ("prtg_10", "historicdata (10).csv", "Nokia 4G PRTG #4"),
    ("prtg_11", "historicdata (11).csv", "Nokia 4G PRTG #5"),
    ("prtg_12", "historicdata (12).csv", "Nokia 4G PRTG #6"),
    ("prtg_13", "historicdata (13).csv", "Nokia 4G PRTG #7"),
]


def resolve_lan_files():
    """Resolve 7 LAN switch files from Downloads."""
    result = []
    for key, fname, desc in LAN_FILES:
        path = os.path.join(DOWNLOADS, fname)
        result.append({
            "key":         key,
            "description": desc,
            "path":        path,
            "exists":      os.path.isfile(path),
        })
    return result


def resolve_prtg_files():
    """Resolve 7 Nokia 4G PRTG files from Downloads."""
    result = []
    for key, fname, desc in PRTG_FILES:
        path = os.path.join(DOWNLOADS, fname)
        result.append({
            "key":         key,
            "description": desc,
            "path":        path,
            "exists":      os.path.isfile(path),
        })
    return result


def resolve_tcs_file(date_str):
    """
    Find TCS EMS eNBwise daily CSV in Downloads folder.
    Pattern: WZ_Gujarat_RAN_KPIS_eNBwise_Daily_DD_MM_YYYY.csv

    STRICT: only returns the file for the EXACT requested date.
    If that file is missing, returns exists=False with a clear message.
    Never falls back to a different date file — that would insert wrong data.
    """
    dt    = datetime.strptime(date_str, "%Y-%m-%d")
    fname = (f"WZ_Gujarat_RAN_KPIS_eNBwise_Daily_"
             f"{dt.day:02d}_{dt.month:02d}_{dt.year}.csv")
    exact = os.path.join(DOWNLOADS, fname)

    # 1. Exact filename match
    if os.path.isfile(exact):
        return {"path": exact, "exists": True, "approx": False,
                "message": f"Found: {fname}"}

    # 2. Glob: same date but with extra suffix (e.g. trailing space or (1))
    pattern = os.path.join(DOWNLOADS,
        f"WZ_Gujarat_RAN_KPIS_eNBwise_Daily_{dt.day:02d}_{dt.month:02d}_{dt.year}*.csv")
    hits = glob.glob(pattern)
    if hits:
        return {"path": hits[0], "exists": True, "approx": False,
                "message": f"Found (variant): {os.path.basename(hits[0])}"}

    # 3. File not found for this date — do NOT use another date's file
    return {
        "path":    exact,
        "exists":  False,
        "approx":  False,
        "message": (
            f"TCS file NOT found for {date_str}.\n"
            f"Expected: {fname}\n"
            f"Location: {DOWNLOADS}\\\n"
            f"TCS 4G columns will be zero in MAR-26 sheet.\n"
            f"Copy the correct file and re-run Daily Report."
        ),
    }


def resolve_all(date_str):
    """
    Resolve all 15 files:
      7 LAN + 7 PRTG = 14 CSV files from Downloads (uploaded to DB)
      1 TCS EMS CSV  from Downloads (read directly, not uploaded)
    Nokia SGSN DB tables are populated by the separate Java/NetAct script.
    """
    files  = resolve_lan_files()
    files += resolve_prtg_files()
    tcs    = resolve_tcs_file(date_str)
    files.append({
        "key":         "tcs_ems",
        "description": "TCS 4G EMS eNBwise Daily",
        "path":        tcs["path"],
        "exists":      tcs["exists"],
    })
    return files


def example_filenames(date_str):
    dt = datetime.strptime(date_str, "%Y-%m-%d")
    return {
        "Downloads (LAN Switch — 7 files)":
            ["historicdata.csv"] + [f"historicdata ({i}).csv" for i in range(1, 7)],
        "Downloads (Nokia 4G PRTG — 7 files)":
            [f"historicdata ({i}).csv" for i in range(7, 14)],
        "Downloads (TCS EMS — 1 file)":
            [f"WZ_Gujarat_RAN_KPIS_eNBwise_Daily_{dt.day:02d}_{dt.month:02d}_{dt.year}.csv"],
    }
