# report_zte.py
# ZTE SGSN Daily Report — reads from PostgreSQL p_obs_zte
# Equivalent of Java Reports.class buttons 1-6 (SGSNZTE sheet logic)

from db import pg_query
from excel_helper import (Workbook, write_title, write_header, write_subheader,
                           write_row, set_col_widths, freeze, save_wb,
                           hdr_font, hdr_fill, body_font, row_fill, total_fill,
                           center, right, left_align, BORDER, C_HEADER_BG, C_SUB_BG,
                           C_TOTAL_BG)
from openpyxl.styles import Font, PatternFill, Alignment, numbers as xl_numbers
from config import MB
import os


def _zte_hourly_query(date_str):
    """Main ZTE SGSN hourly aggregation query — exact logic from Java bytecode."""
    return pg_query("""
        SELECT
            min(end_time)                                          AS dtime,
            sum(GTP81) / %s                                        AS upload_3g,
            sum(GTP83) / %s                                        AS download_3g,
            sum(GTP73) / %s                                        AS upload_2g,
            sum(GTP75) / %s                                        AS download_2g,
            sum(GTP73 + GTP75) / %s                                AS total2g,
            sum(GTP81 + GTP83) / %s                                AS total3g,
            sum(GTP73 + GTP75 + GTP81 + GTP83) / %s               AS totalgn,
            max(GTP73 + GTP75) * 8.0 / 300 / %s                   AS peak2gthru,
            max(GTP81 + GTP83) * 8.0 / 300 / %s                   AS peak3gthru,
            max(GTP73 + GTP75 + GTP81 + GTP83) * 8.0 / 300 / %s  AS peakgnthru,
            max(user13)                                            AS pdp_3g,
            max(session19)                                         AS actpdp_3g,
            max(user14)                                            AS pdp_2g,
            max(session20)                                         AS actpdp_2g,
            max(GTP28) / 1024                                      AS thu_up,
            max(GTP29) / 1024                                      AS thu_down,
            max(GTP28 + GTP29) / 1024                             AS thu_total,
            sum(session1  + session2)                              AS att,
            sum(session3  + session4)                              AS succ,
            sum(session97 + session98)                             AS rej_ggsn,
            sum(session10 + session68 + session106 +
                session108 + session107 + session109)              AS rej_in,
            (sum(ovrld8) + sum(ovrld15))                           AS rej_ovrld,
            sum(session96 + session95)                             AS rej_rf,
            COALESCE((sum(session3 + session4) /
                NULLIF(sum(session1 + session2), 0)), 0) * 100    AS per_succ,
            COALESCE((sum(session97 + session98) /
                NULLIF(sum(session1 + session2), 0)), 0) * 100    AS per_rej_ggsn,
            COALESCE((sum(session10 + session68 + session106 +
                session108 + session107 + session109) /
                NULLIF(sum(session1 + session2), 0)), 0) * 100    AS per_rej_in,
            COALESCE(((sum(ovrld8) + sum(ovrld15)) /
                NULLIF(sum(session1 + session2), 0)), 0) * 100    AS per_rej_ovrld,
            COALESCE((sum(session96 + session95) /
                NULLIF(sum(session1 + session2), 0)), 0) * 100    AS per_rej_rf,
            sum(Att_gsm1  + Att_umts1)                             AS at_attempt,
            sum(Att_gsm2  + Att_umts2)                             AS at_succ,
            COALESCE((sum(Att_gsm2 + Att_umts2) /
                NULLIF(sum(Att_gsm1 + Att_umts1), 0)), 0) * 100  AS at_per_succ
        FROM p_obs_zte
        WHERE date(end_time) = %s
        GROUP BY extract(hour FROM end_time)
        ORDER BY extract(hour FROM end_time)
    """, (MB, MB, MB, MB, MB, MB, MB, MB, MB, MB, date_str))


def generate_zte_report(date_str, log=print):
    """Generate ZTE SGSN daily report Excel for given date."""
    log(f"[ZTE Report] Querying data for {date_str}...")
    rows = _zte_hourly_query(date_str)

    if not rows:
        raise ValueError(f"No ZTE data found in PostgreSQL for {date_str}. Please upload CSV files first.")

    wb = Workbook()
    ws = wb.active
    ws.title = "SGSNZTE"
    ws.sheet_view.showGridLines = False

    # Title row
    write_title(ws, 1, 1, f"ZTE SGSN Daily Traffic Report — {date_str}", span=28,
                bg=C_HEADER_BG, size=13)

    # Section headers
    headers_vol = [
        "Hour", "UL 2G (MB)", "DL 2G (MB)", "Total 2G (MB)",
        "UL 3G (MB)", "DL 3G (MB)", "Total 3G (MB)", "Total GN (MB)",
        "Peak 2G (Mbps)", "Peak 3G (Mbps)", "Peak GN (Mbps)"
    ]
    headers_pdp = [
        "PDP 3G", "Act PDP 3G", "PDP 2G", "Act PDP 2G",
        "Thru UP (Mbps)", "Thru DL (Mbps)", "Thru Total"
    ]
    headers_sess = [
        "Att", "Succ", "Rej GGSN", "Rej IN", "Rej OVRLD", "Rej RF",
        "Succ %", "Rej GGSN %", "Rej IN %", "Rej OVRLD %"
    ]

    write_subheader(ws, 2, headers_vol, col_start=1)
    write_subheader(ws, 2, headers_pdp, col_start=12)
    write_subheader(ws, 2, headers_sess, col_start=19)

    # Data rows
    totals = {k: 0.0 for k in [
        "upload_2g", "download_2g", "total2g", "upload_3g", "download_3g",
        "total3g", "totalgn", "att", "succ", "rej_ggsn", "rej_in", "rej_ovrld", "rej_rf"
    ]}
    peak_vals = {"peak2gthru": 0.0, "peak3gthru": 0.0, "peakgnthru": 0.0,
                 "thu_up": 0.0, "thu_down": 0.0, "thu_total": 0.0}

    for i, r in enumerate(rows):
        row_num = i + 3
        alt = i % 2 == 1
        hour_label = str(r["dtime"])[:16] if r["dtime"] else f"Hour {i}"

        def v(k):
            val = r.get(k) or 0
            return round(float(val), 2)

        row_data_vol = [
            hour_label, v("upload_2g"), v("download_2g"), v("total2g"),
            v("upload_3g"), v("download_3g"), v("total3g"), v("totalgn"),
            v("peak2gthru"), v("peak3gthru"), v("peakgnthru")
        ]
        row_data_pdp = [
            int(v("pdp_3g")), int(v("actpdp_3g")),
            int(v("pdp_2g")), int(v("actpdp_2g")),
            v("thu_up"), v("thu_down"), v("thu_total")
        ]
        row_data_sess = [
            int(v("att")), int(v("succ")), int(v("rej_ggsn")),
            int(v("rej_in")), int(v("rej_ovrld")), int(v("rej_rf")),
            v("per_succ"), v("per_rej_ggsn"), v("per_rej_in"), v("per_rej_ovrld")
        ]

        write_row(ws, row_num, row_data_vol, alt=alt, col_start=1)
        write_row(ws, row_num, row_data_pdp, alt=alt, col_start=12)
        write_row(ws, row_num, row_data_sess, alt=alt, col_start=19)

        # Accumulate totals
        for k in ["upload_2g", "download_2g", "total2g", "upload_3g", "download_3g",
                  "total3g", "totalgn", "att", "succ", "rej_ggsn", "rej_in", "rej_ovrld", "rej_rf"]:
            totals[k] += float(r.get(k) or 0)
        for k in ["peak2gthru", "peak3gthru", "peakgnthru", "thu_up", "thu_down", "thu_total"]:
            peak_vals[k] = max(peak_vals[k], float(r.get(k) or 0))

    # Totals row
    total_row = len(rows) + 3

    def rt(k):
        return round(totals[k], 2)

    def rp(k):
        return round(peak_vals[k], 2)

    total_succ_pct = round(totals["succ"] / totals["att"] * 100, 2) if totals["att"] else 0
    total_rej_ggsn_pct = round(totals["rej_ggsn"] / totals["att"] * 100, 2) if totals["att"] else 0
    total_rej_in_pct = round(totals["rej_in"] / totals["att"] * 100, 2) if totals["att"] else 0
    total_rej_ovrld_pct = round(totals["rej_ovrld"] / totals["att"] * 100, 2) if totals["att"] else 0

    write_row(ws, total_row, [
        "TOTAL", rt("upload_2g"), rt("download_2g"), rt("total2g"),
        rt("upload_3g"), rt("download_3g"), rt("total3g"), rt("totalgn"),
        rp("peak2gthru"), rp("peak3gthru"), rp("peakgnthru")
    ], is_total=True, bold=True, col_start=1)

    write_row(ws, total_row, [
        "", "", "", "",
        rp("thu_up"), rp("thu_down"), rp("thu_total")
    ], is_total=True, bold=True, col_start=12)

    write_row(ws, total_row, [
        int(totals["att"]), int(totals["succ"]), int(totals["rej_ggsn"]),
        int(totals["rej_in"]), int(totals["rej_ovrld"]), int(totals["rej_rf"]),
        total_succ_pct, total_rej_ggsn_pct, total_rej_in_pct, total_rej_ovrld_pct
    ], is_total=True, bold=True, col_start=19)

    # Column widths
    set_col_widths(ws, [(1, 18)] + [(i, 13) for i in range(2, 29)])
    freeze(ws, row=3)

    fname = f"ZTE_SGSN_{date_str}.xlsx"
    path = save_wb(wb, fname)
    log(f"[ZTE Report] Saved: {path}")
    return path
