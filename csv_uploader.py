# csv_uploader.py
# Uploads LAN Switch + Nokia 4G PRTG historicdata files to PostgreSQL.
# Also stores TCS EMS daily data into MySQL sgsn_tcs_daily.
#
# ALL FILES FROM WINDOWS DOWNLOADS FOLDER:
#   historicdata.csv + historicdata (1-6).csv  → p_obs_zte_lan          (7 files)
#   historicdata (7-13).csv                    → p_obs_zte_lan_4g_tput  (7 files)
#   WZ_Gujarat_RAN_KPIS_eNBwise_Daily_*.csv    → MySQL sgsn_tcs_daily   (1 file)
#
# Nokia SGSN tables are populated by separate Java/NetAct script — NOT handled here.

import csv
from datetime import datetime
from db import pg_execute, pg_execute_many
from file_resolver import resolve_lan_files, resolve_prtg_files, DOWNLOADS, resolve_tcs_file
from tcs_reader import read_tcs_daily
from tcs_store  import store_tcs

KB = 1024.0


def _to_float(val):
    try:    return float(str(val).strip().replace(',', '') or 0)
    except: return 0.0


def _parse_prtg_ts(val):
    """Parse PRTG timestamp: '13.03.2026 00:00:00 - 00:05:00' → start datetime."""
    s = str(val).strip()
    try:
        return datetime.strptime(s.split(' - ')[0].strip(), '%d.%m.%Y %H:%M:%S')
    except:
        raise ValueError(f"Cannot parse PRTG timestamp: {s}")


def _read_csv(filepath):
    rows = []
    with open(filepath, newline='', encoding='utf-8-sig') as f:
        reader = csv.reader(f)
        next(reader)   # skip header
        for row in reader:
            if any(c.strip() for c in row):
                rows.append(row)
    return rows


def detect_csv_date(filepath):
    """
    Read the date from the first data row of a PRTG historicdata CSV.
    Returns date as 'YYYY-MM-DD' string, or None if cannot detect.
    PRTG timestamp format: "13.03.2026 00:00:00 - 00:05:00"
    """
    try:
        with open(filepath, newline='', encoding='utf-8-sig') as f:
            reader = csv.reader(f)
            next(reader)  # skip header
            for row in reader:
                if row and row[0].strip():
                    ts_raw = row[0].strip()
                    # Extract date part before the space
                    date_part = ts_raw.split(' ')[0].strip()
                    dt = datetime.strptime(date_part, '%d.%m.%Y')
                    return dt.strftime('%Y-%m-%d')
    except Exception:
        pass
    return None


def validate_csv_date(files, date_str, label, log=print):
    """
    Check that the first existing CSV file contains data for date_str.
    Raises ValueError with a clear message if mismatch detected.
    files: list of resolve dicts with 'path' and 'description'
    """
    for f in files:
        if f.get('exists'):
            csv_date = detect_csv_date(f['path'])
            if csv_date and csv_date != date_str:
                # Parse both dates for human-readable message
                csv_dt  = datetime.strptime(csv_date,  '%Y-%m-%d')
                sel_dt  = datetime.strptime(date_str,  '%Y-%m-%d')
                raise ValueError(
                    f"DATE MISMATCH — {label} files are for "
                    f"{csv_dt.strftime('%d-%b-%Y')} "
                    f"but selected date is {sel_dt.strftime('%d-%b-%Y')}.\n\n"
                    f"File:  {f['path']}\n"
                    f"File date:     {csv_dt.strftime('%d/%m/%Y')}\n"
                    f"Selected date: {sel_dt.strftime('%d/%m/%Y')}\n\n"
                    f"UPLOAD BLOCKED.\n"
                    f"Please change the date in the tool to match the files,\n"
                    f"or copy the correct files for {sel_dt.strftime('%d-%b-%Y')} "
                    f"to Downloads."
                )
            log(f"[{label}] Date check: file date = {csv_date} ✓")
            return  # only need to check first available file
    log(f"[{label}] Date check: no files available to verify")


def _is_summary_row(row):
    """Skip PRTG 'Sums' and 'Averages' rows at end of file."""
    ts = row[0] if row else ''
    return any(k in ts.lower() for k in ('sum', 'average', 'total'))


def _parse_prtg_rows(rows):
    """
    Extract (ts, in_kb, in_kbps, out_kb, out_kbps) from PRTG historicdata rows.
    RAW columns (pure floats):
      col3  = Traffic Total Volume (bytes)
      col7  = Traffic In  Volume  (bytes)  → in DB as KB
      col9  = Traffic In  Speed   (bits/s) → in DB as Kbps
      col11 = Traffic Out Volume  (bytes)  → in DB as KB
      col13 = Traffic Out Speed   (bits/s) → in DB as Kbps
    """
    data = []
    for row in rows:
        if _is_summary_row(row): continue
        try:
            ts      = _parse_prtg_ts(row[0])
            in_kb   = _to_float(row[7])  / KB          # bytes  → KB
            in_kbps = _to_float(row[9])  / 1000.0      # bits/s → Kbps
            out_kb  = _to_float(row[11]) / KB
            out_kbps= _to_float(row[13]) / 1000.0
            data.append((ts, in_kb, in_kbps, out_kb, out_kbps))
        except Exception:
            continue
    return data


# =============================================================================
#  Upload LAN Switch — historicdata.csv + historicdata (1-6) from Downloads
# =============================================================================

def upload_lan_all(date_str, log=print):
    files   = resolve_lan_files()
    missing = [f for f in files if not f["exists"]]
    if missing:
        names = "\n".join(f"  - {f['description']}: {f['path']}" for f in missing)
        raise FileNotFoundError(
            f"LAN Switch files missing:\n{names}\n\n"
            f"Copy historicdata.csv and historicdata (1-6).csv to:\n  {DOWNLOADS}\\"
        )
    # ── Validate CSV date matches selected date BEFORE touching DB ───────
    validate_csv_date(files, date_str, "LAN", log=log)
    log(f"[LAN Upload] All 7 files found. Clearing p_obs_zte_lan for {date_str}...")
    pg_execute("DELETE FROM public.p_obs_zte_lan WHERE date(date_time) = %s", (date_str,))
    sql = """INSERT INTO public.p_obs_zte_lan
                 (date_time, in_volume_kb, in_speed_kbps, out_volume_kb, out_speed_kbps)
             VALUES (%s, %s, %s, %s, %s)"""
    total = 0
    for f in files:
        log(f"[LAN Upload] Reading {f['description']}...")
        rows = _read_csv(f["path"])
        data = _parse_prtg_rows(rows)
        if data:
            pg_execute_many(sql, data)
        total += len(data)
        log(f"[LAN Upload]   {f['description']}: {len(data)} rows inserted.")
    log(f"[LAN Upload] Done. {total} total rows → p_obs_zte_lan.")
    return total


# =============================================================================
#  Upload Nokia 4G PRTG — historicdata (7-13) from Downloads
# =============================================================================

def upload_prtg_all(date_str, log=print):
    files   = resolve_prtg_files()
    missing = [f for f in files if not f["exists"]]
    if missing:
        names = "\n".join(f"  - {f['description']}: {f['path']}" for f in missing)
        raise FileNotFoundError(
            f"Nokia 4G PRTG files missing:\n{names}\n\n"
            f"Copy historicdata (7-13).csv to:\n  {DOWNLOADS}\\"
        )
    # ── Validate CSV date matches selected date BEFORE touching DB ───────
    validate_csv_date(files, date_str, "PRTG", log=log)
    log(f"[PRTG Upload] All 7 files found. Clearing p_obs_zte_lan_4g_tput for {date_str}...")
    pg_execute("DELETE FROM public.p_obs_zte_lan_4g_tput WHERE date(date_time) = %s", (date_str,))
    sql = """INSERT INTO public.p_obs_zte_lan_4g_tput
                 (date_time, in_volume_kb, in_speed_kbps, out_volume_kb, out_speed_kbps)
             VALUES (%s, %s, %s, %s, %s)"""
    total = 0
    for f in files:
        log(f"[PRTG Upload] Reading {f['description']}...")
        rows = _read_csv(f["path"])
        data = _parse_prtg_rows(rows)
        if data:
            pg_execute_many(sql, data)
        total += len(data)
        log(f"[PRTG Upload]   {f['description']}: {len(data)} rows inserted.")
    log(f"[PRTG Upload] Done. {total} total rows → p_obs_zte_lan_4g_tput.")
    return total


# =============================================================================
#  Upload TCS — WZ_Gujarat_RAN_KPIS_eNBwise_Daily_DD_MM_YYYY.csv → MySQL
# =============================================================================

def upload_tcs(date_str, log=print):
    """
    Find the TCS EMS CSV for date_str and store its daily totals
    into MySQL reportsDB.sgsn_tcs_daily.
    """
    info = resolve_tcs_file(date_str)
    if not info["exists"]:
        from datetime import datetime as _dt
        dt = _dt.strptime(date_str, "%Y-%m-%d")
        expected = (f"WZ_Gujarat_RAN_KPIS_eNBwise_Daily_"
                    f"{dt.day:02d}_{dt.month:02d}_{dt.year}.csv")
        log(f"[TCS Upload] File not found: {expected}")
        log(f"[TCS Upload] Location: {DOWNLOADS}\\")
        log(f"[TCS Upload] TCS data for {date_str} NOT stored.")
        return 0

    log(f"[TCS Upload] Reading {info['path']}...")
    data = read_tcs_daily(info["path"])
    if not data:
        log("[TCS Upload] ERROR: could not read TCS file.")
        return 0

    store_tcs(data, log=log)
    log(f"[TCS Upload] Stored to MySQL sgsn_tcs_daily: "
        f"DL={data['dl_mb']/1024:.0f}GB  "
        f"UL={data['ul_mb']/1024:.0f}GB  "
        f"{data['enb_count']} eNBs")
    return 1


# =============================================================================
#  Upload ALL — LAN (7) + PRTG (7) + TCS (1) = 15 files
# =============================================================================

def upload_all(date_str, log=print):
    """
    Upload all 15 files from Downloads folder:
      - 7 LAN switch CSV → PostgreSQL p_obs_zte_lan
      - 7 Nokia 4G PRTG CSV → PostgreSQL p_obs_zte_lan_4g_tput
      - 1 TCS EMS CSV → MySQL sgsn_tcs_daily
    Nokia SGSN tables are handled by the separate Java/NetAct script.
    """
    log(f"[Upload All] Starting for {date_str}...")
    log(f"[Upload All] Source: {DOWNLOADS}\\")
    lan  = upload_lan_all(date_str,  log=log)
    prtg = upload_prtg_all(date_str, log=log)
    log(f"[Upload All] Storing TCS EMS data to MySQL...")
    tcs  = upload_tcs(date_str, log=log)
    total = lan + prtg
    log(f"[Upload All] Done. LAN={lan} + PRTG={prtg} rows + TCS={'OK' if tcs else 'NOT FOUND'}.")
    log(f"[Upload All] Nokia SGSN tables: handled by Java/NetAct script (not touched).")
    return total
