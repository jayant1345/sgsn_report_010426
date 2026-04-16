# report_combined.py
# Combined Total SGSN Report — joins ZTE + Nokia + LAN + 4G
# The big 4-table join query from Java bytecode

from db import pg_query
from excel_helper import (Workbook, write_title, write_subheader, write_row,
                           set_col_widths, freeze, save_wb, C_HEADER_BG)


def _combined_query(date_str):
    """
    4-table join: ZTE(t3) + Nokia(t4) + ZTE_LAN(t5) + Nokia_4G(t6)
    Exact query from Java Reports.class bytecode.
    """
    return pg_query("""
        SELECT
            t3.dtime,
            t6.upload_4g,
            t6.download_4g,
            (t5.upload - (t3.upload_2g + t4.upload_2g))          AS upload_3g_total,
            (t5.download - (t3.download_2g + t4.download_2g))     AS download_3g_total,
            (t3.upload_2g + t4.upload_2g)                         AS upload_2g_total,
            (t3.download_2g + t4.download_2g)                     AS download_2g_total,
            (t6.upload_4g + t6.download_4g)                       AS total_4g,
            (t5.upload - (t3.upload_2g + t4.upload_2g)
             + t5.download - (t3.download_2g + t4.download_2g))   AS total_3g,
            (t3.upload_2g + t4.upload_2g
             + t3.download_2g + t4.download_2g)                   AS total_2g,
            (t3.upload_2g + t4.upload_2g + t3.download_2g + t4.download_2g
             + t5.upload - (t3.upload_2g + t4.upload_2g)
             + t5.download - (t3.download_2g + t4.download_2g))
             + (t6.upload_4g + t6.download_4g)                    AS totalgn,
            (t6.upload_4g + t6.download_4g) * 8.0 / 3600          AS peak4gthru,
            (t5.upload - (t3.upload_2g + t4.upload_2g)
             + t5.download - (t3.download_2g + t4.download_2g)) * 8.0 / 3600  AS peak3gthru,
            (t3.upload_2g + t4.upload_2g
             + t3.download_2g + t4.download_2g) * 8.0 / 3600      AS peak2gthru,
            ((t3.upload_2g + t4.upload_2g + t3.download_2g + t4.download_2g
              + t5.upload - (t3.upload_2g + t4.upload_2g)
              + t5.download - (t3.download_2g + t4.download_2g))
              + (t6.upload_4g + t6.download_4g)) * 8.0 / 3600     AS peakgnthru
        FROM (
            SELECT min(end_time) dtime,
                   sum(GTP81) / 1048576 AS upload_3g,
                   sum(GTP83) / 1048576 AS download_3g,
                   sum(GTP73) / 1048576 AS upload_2g,
                   sum(GTP75) / 1048576 AS download_2g
            FROM p_obs_zte
            WHERE date(end_time) = %s
            GROUP BY extract(hour FROM end_time)
            ORDER BY extract(hour FROM end_time)
        ) t3
        INNER JOIN (
            SELECT min(a.period_start_time) dtime,
                   sum(a.upload_3g)   AS upload_3g,
                   sum(a.download_3g) AS download_3g,
                   sum(a.upload_2g)   AS upload_2g,
                   sum(a.download_2g) AS download_2g
            FROM public.nokia_sgsn_report_out a
            WHERE date(a.period_start_time) = %s
            GROUP BY extract(hour FROM a.period_start_time)
            ORDER BY dtime
        ) t4 ON extract(hour FROM t3.dtime) = extract(hour FROM t4.dtime)
        INNER JOIN (
            SELECT min(date_time)           AS date_tm,
                   sum(in_volume_kb) / 1024  AS download,
                   sum(out_volume_kb) / 1024 AS upload,
                   max(in_speed_kbps) / 1024 AS down_speed,
                   max(out_speed_kbps) / 1024 AS up_speed,
                   max(in_speed_kbps + out_speed_kbps) / 1024 AS tot_speed
            FROM public.p_obs_zte_lan
            WHERE date(date_time) = %s
            GROUP BY extract(hour FROM date_time)
            ORDER BY extract(hour FROM date_time)
        ) t5 ON extract(hour FROM t3.dtime) = extract(hour FROM t5.date_tm)
        INNER JOIN (
            SELECT min(period_start_time)   AS dtime,
                   sum(lte_5212a) / 1024    AS download_4g,
                   sum(lte_5213a) / 1024    AS upload_4g
            FROM public.nokia_4g_sgsn_report
            WHERE date(period_start_time) = %s
            GROUP BY extract(hour FROM period_start_time)
            ORDER BY extract(hour FROM period_start_time)
        ) t6 ON extract(hour FROM t3.dtime) = extract(hour FROM t6.dtime)
    """, (date_str, date_str, date_str, date_str))


def generate_combined_report(date_str, log=print):
    log(f"[Combined Report] Querying all 4 sources for {date_str}...")
    rows = _combined_query(date_str)

    if not rows:
        raise ValueError(f"No combined data found for {date_str}. Ensure ZTE + Nokia + LAN data is uploaded.")

    wb = Workbook()
    ws = wb.active
    ws.title = "Combined"
    ws.sheet_view.showGridLines = False

    write_title(ws, 1, 1, f"Total SGSN Combined Report (2G+3G+4G) — {date_str}", span=16,
                bg=C_HEADER_BG, size=13)

    headers = [
        "Hour",
        "UL 4G (MB)", "DL 4G (MB)", "Total 4G (MB)",
        "UL 3G (MB)", "DL 3G (MB)", "Total 3G (MB)",
        "UL 2G (MB)", "DL 2G (MB)", "Total 2G (MB)",
        "Grand Total (MB)",
        "Peak 4G (Mbps)", "Peak 3G (Mbps)", "Peak 2G (Mbps)", "Peak GN (Mbps)"
    ]
    write_subheader(ws, 2, headers)

    totals = {k: 0.0 for k in ["total_4g", "total_3g", "total_2g", "totalgn"]}
    peaks  = {k: 0.0 for k in ["peak4gthru", "peak3gthru", "peak2gthru", "peakgnthru"]}

    for i, r in enumerate(rows):
        rn = i + 3
        alt = i % 2 == 1
        v = lambda k: round(float(r.get(k) or 0), 2)
        hour_label = str(r["dtime"])[:16] if r["dtime"] else f"Hour {i}"

        write_row(ws, rn, [
            hour_label,
            v("upload_4g"), v("download_4g"), v("total_4g"),
            v("upload_3g_total"), v("download_3g_total"), v("total_3g"),
            v("upload_2g_total"), v("download_2g_total"), v("total_2g"),
            v("totalgn"),
            v("peak4gthru"), v("peak3gthru"), v("peak2gthru"), v("peakgnthru")
        ], alt=alt)

        for k in totals:
            totals[k] += float(r.get(k) or 0)
        for k in peaks:
            peaks[k] = max(peaks[k], float(r.get(k) or 0))

    tr = len(rows) + 3
    write_row(ws, tr, [
        "TOTAL",
        "", "", round(totals["total_4g"], 2),
        "", "", round(totals["total_3g"], 2),
        "", "", round(totals["total_2g"], 2),
        round(totals["totalgn"], 2),
        round(peaks["peak4gthru"], 2), round(peaks["peak3gthru"], 2),
        round(peaks["peak2gthru"], 2), round(peaks["peakgnthru"], 2)
    ], is_total=True, bold=True)

    set_col_widths(ws, [(1, 18)] + [(i, 14) for i in range(2, 17)])
    freeze(ws, row=3)
    path = save_wb(wb, f"Combined_SGSN_{date_str}.xlsx")
    log(f"[Combined Report] Saved: {path}")
    return path
