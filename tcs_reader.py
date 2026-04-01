# tcs_reader.py
# Reads TCS EMS eNBwise Daily CSV export.
#
# File: WZ_Gujarat_RAN_KPIS_eNBwise_Daily_DD_MM_YYYY.csv
# Format: one row per eNB per day (NOT 5-min intervals)
#   col0  : Date          (DD/MM/YYYY)
#   col1  : Site Id
#   col2  : LOCATION
#   col4  : Region
#   col72 : Data Volume DL(MB)
#   col73 : Data Volume UL(MB)
#   col74 : Data Volume - Total (GB)
#   col77 : Average Cell Throughput (DL) Mbps
#   col78 : Average Cell Throughput (UL) Mbps
#   col79 : Max Cell throughput (DL) Mbps
#   col80 : Max Cell throughput (UL) Mbps
#
# This is a DAILY file — one record per eNB for the whole day.
# We SUM across all eNBs to get the total daily figure.
# For the report we need: total DL MB, total UL MB, avg DL Mbps, peak DL Mbps.

import csv
from datetime import datetime


COL_DATE    = 0
COL_SITE_ID = 1
COL_LOC     = 2
COL_REGION  = 4
COL_DL_MB   = 72
COL_UL_MB   = 73
COL_TOT_GB  = 74
COL_AVG_DL  = 77   # Average Cell Throughput DL Mbps
COL_AVG_UL  = 78   # Average Cell Throughput UL Mbps
COL_MAX_DL  = 79   # Max Cell throughput DL Mbps
COL_MAX_UL  = 80   # Max Cell throughput UL Mbps


def _safe_float(v):
    try:
        s = str(v).strip()
        if s in ('', '-', 'None', 'nan'): return 0.0
        return float(s.replace(',', ''))
    except:
        return 0.0


def _parse_date(s):
    s = str(s).strip()
    for fmt in ('%d/%m/%Y', '%Y-%m-%d', '%d-%m-%Y', '%d.%m.%Y'):
        try:
            return datetime.strptime(s, fmt).date()
        except:
            pass
    return None


def read_tcs_daily(filepath):
    """
    Read TCS EMS eNBwise daily CSV.
    Returns dict with daily totals:
    {
        date        : date object,
        dl_mb       : total DL volume MB (sum of all eNBs),
        ul_mb       : total UL volume MB,
        total_gb    : total volume GB,
        avg_dl_mbps : sum of avg DL tput across eNBs (for MAR-26 sheet),
        avg_ul_mbps : sum of avg UL tput,
        peak_dl_mbps: max of max DL tput across eNBs,
        peak_ul_mbps: max of max UL tput,
        enb_count   : number of eNBs processed,
    }
    """
    rows = _read_csv(filepath)
    if not rows:
        return None

    dl_mb_total  = 0.0
    ul_mb_total  = 0.0
    tot_gb_total = 0.0
    avg_dl_sum   = 0.0
    avg_ul_sum   = 0.0
    peak_dl      = 0.0
    peak_ul      = 0.0
    date_val     = None
    count        = 0

    for r in rows:
        if not date_val:
            date_val = _parse_date(r[COL_DATE])

        dl  = _safe_float(r[COL_DL_MB])
        ul  = _safe_float(r[COL_UL_MB])
        tot = _safe_float(r[COL_TOT_GB])
        adl = _safe_float(r[COL_AVG_DL])
        aul = _safe_float(r[COL_AVG_UL])
        mdl = _safe_float(r[COL_MAX_DL])
        mul = _safe_float(r[COL_MAX_UL])

        dl_mb_total  += dl
        ul_mb_total  += ul
        tot_gb_total += tot
        avg_dl_sum   += adl
        avg_ul_sum   += aul
        if mdl > peak_dl: peak_dl = mdl
        if mul > peak_ul: peak_ul = mul
        count += 1

    return {
        "date":         date_val,
        "dl_mb":        dl_mb_total,
        "ul_mb":        ul_mb_total,
        "total_gb":     tot_gb_total,
        "avg_dl_mbps":  avg_dl_sum,
        "avg_ul_mbps":  avg_ul_sum,
        "peak_dl_mbps": peak_dl,
        "peak_ul_mbps": peak_ul,
        "enb_count":    count,
    }


def _read_csv(filepath):
    rows = []
    try:
        with open(filepath, newline='', encoding='utf-8-sig') as f:
            reader = csv.reader(f)
            next(reader)  # skip header
            for row in reader:
                if len(row) > COL_MAX_DL and row[COL_DATE].strip():
                    # Skip summary/blank rows
                    if _parse_date(row[COL_DATE]):
                        rows.append(row)
    except Exception as e:
        pass
    return rows
