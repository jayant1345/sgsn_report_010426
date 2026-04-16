# report_nokia.py
# Nokia SGSN Daily Report — reads from nokia_sgsn_report_out + pdp_out + att_out
# Equivalent of Java Reports.class SGSNNOKIA sheet logic

from db import pg_query
from excel_helper import (Workbook, write_title, write_subheader, write_row,
                           set_col_widths, freeze, save_wb, C_HEADER_BG)
from config import MB


def _nokia_hourly_query(date_str):
    return pg_query("""
        SELECT
            min(a.period_start_time)                                         AS dtime,
            sum(a.upload_3g)                                                 AS upload_3g,
            sum(a.download_3g)                                               AS download_3g,
            sum(a.upload_2g)                                                 AS upload_2g,
            sum(a.download_2g)                                               AS download_2g,
            sum(a.total2g)                                                   AS total2g,
            sum(a.total3g)                                                   AS total3g,
            sum(a.totalgn)                                                   AS totalgn,
            max(a.peak2gthru)                                                AS peak2gthru,
            max(a.peak3gthru)                                                AS peak3gthru,
            max(a.peakgnthru)                                                AS peakgnthru,
            max(a.pdp_3g)                                                    AS pdp_3g,
            max(a.actpdp_3g)                                                 AS actpdp_3g,
            max(a.pdp_2g)                                                    AS pdp_2g,
            max(a.actpdp_2g)                                                 AS actpdp_2g,
            max(a.thu_up)                                                    AS thu_up,
            max(a.thu_down)                                                  AS thu_down,
            max(a.thu_total)                                                 AS thu_total,
            sum(b.att)                                                       AS att,
            sum(b.succ)                                                      AS succ,
            sum(b.rej_ggsn)                                                  AS rej_ggsn,
            sum(b.rej_in)                                                    AS rej_in,
            sum(b.rej_other)                                                 AS rej_other,
            sum(b.rej_overload)                                              AS rej_overload,
            sum(b.rej_rf)                                                    AS rej_rf,
            round((sum(b.succ) * 100.0 / NULLIF(sum(b.att), 0))::numeric, 2)        AS per_succ,
            round((sum(b.rej_ggsn) * 100.0 / NULLIF(sum(b.att), 0))::numeric, 2)   AS per_rej_ggsn,
            round((sum(b.rej_in) * 100.0 / NULLIF(sum(b.att), 0))::numeric, 2)     AS per_rej_in,
            round((sum(b.rej_other) * 100.0 / NULLIF(sum(b.att), 0))::numeric, 2)  AS per_rej_other,
            round((sum(b.rej_overload) * 100.0 / NULLIF(sum(b.att), 0))::numeric, 2) AS per_rej_ovrld,
            round((sum(b.rej_rf) * 100.0 / NULLIF(sum(b.att), 0))::numeric, 2)     AS per_rej_rf,
            sum(c.att)                                                       AS at_attempt,
            sum(c.succ)                                                      AS at_succ,
            round((sum(c.succ) * 100.0 / NULLIF(sum(c.att), 0))::numeric, 2)       AS at_per_succ
        FROM public.nokia_sgsn_report_out a
        INNER JOIN public.nokia_sgsn_report_pdp_out b
            ON a.period_start_time = b.period_start_time
        INNER JOIN public.nokia_sgsn_report_att_out c
            ON c.period_start_time = b.period_start_time
        WHERE date(a.period_start_time) = %s
          AND date(b.period_start_time) = %s
        GROUP BY extract(hour FROM a.period_start_time)
        ORDER BY dtime
    """, (date_str, date_str))


def generate_nokia_report(date_str, log=print):
    log(f"[Nokia Report] Querying data for {date_str}...")
    rows = _nokia_hourly_query(date_str)

    if not rows:
        raise ValueError(f"No Nokia SGSN data found for {date_str}. Check NetAct data availability.")

    wb = Workbook()
    ws = wb.active
    ws.title = "SGSNNOKIA"
    ws.sheet_view.showGridLines = False

    write_title(ws, 1, 1, f"Nokia SGSN Daily Traffic Report — {date_str}", span=25,
                bg=C_HEADER_BG, size=13)

    headers_vol = [
        "Hour", "UL 2G (MB)", "DL 2G (MB)", "Total 2G",
        "UL 3G (MB)", "DL 3G (MB)", "Total 3G", "Total GN",
        "Peak 2G (Mbps)", "Peak 3G (Mbps)", "Peak GN (Mbps)"
    ]
    headers_pdp = [
        "PDP 3G", "Act PDP 3G", "PDP 2G", "Act PDP 2G",
        "Thru UP", "Thru DL", "Thru Total"
    ]
    headers_sess = [
        "Att", "Succ", "Rej GGSN", "Rej IN", "Rej Other",
        "Rej OVRLD", "Rej RF", "Succ %"
    ]

    write_subheader(ws, 2, headers_vol, col_start=1)
    write_subheader(ws, 2, headers_pdp, col_start=12)
    write_subheader(ws, 2, headers_sess, col_start=19)

    totals = {k: 0.0 for k in ["upload_2g", "download_2g", "total2g", "upload_3g",
                                "download_3g", "total3g", "totalgn", "att", "succ",
                                "rej_ggsn", "rej_in", "rej_other", "rej_overload", "rej_rf"]}
    peaks = {k: 0.0 for k in ["peak2gthru", "peak3gthru", "peakgnthru",
                                "thu_up", "thu_down", "thu_total"]}

    for i, r in enumerate(rows):
        alt = i % 2 == 1
        rn = i + 3
        v = lambda k: round(float(r.get(k) or 0), 2)
        hour_label = str(r["dtime"])[:16] if r["dtime"] else f"Hour {i}"

        write_row(ws, rn, [hour_label, v("upload_2g"), v("download_2g"), v("total2g"),
                            v("upload_3g"), v("download_3g"), v("total3g"), v("totalgn"),
                            v("peak2gthru"), v("peak3gthru"), v("peakgnthru")], alt=alt, col_start=1)
        write_row(ws, rn, [int(v("pdp_3g")), int(v("actpdp_3g")),
                            int(v("pdp_2g")), int(v("actpdp_2g")),
                            v("thu_up"), v("thu_down"), v("thu_total")], alt=alt, col_start=12)
        write_row(ws, rn, [int(v("att")), int(v("succ")), int(v("rej_ggsn")),
                            int(v("rej_in")), int(v("rej_other")), int(v("rej_overload")),
                            int(v("rej_rf")), v("per_succ")], alt=alt, col_start=19)

        for k in totals:
            totals[k] += float(r.get(k) or 0)
        for k in peaks:
            peaks[k] = max(peaks[k], float(r.get(k) or 0))

    tr = len(rows) + 3
    succ_pct = round(totals["succ"] / totals["att"] * 100, 2) if totals["att"] else 0
    write_row(ws, tr, ["TOTAL"] + [round(totals[k], 2) for k in [
        "upload_2g", "download_2g", "total2g", "upload_3g", "download_3g", "total3g", "totalgn"
    ]] + [round(peaks[k], 2) for k in ["peak2gthru", "peak3gthru", "peakgnthru"]],
              is_total=True, bold=True, col_start=1)
    write_row(ws, tr, [int(totals["att"]), int(totals["succ"]), int(totals["rej_ggsn"]),
                        int(totals["rej_in"]), int(totals["rej_other"]), int(totals["rej_overload"]),
                        int(totals["rej_rf"]), succ_pct],
              is_total=True, bold=True, col_start=19)

    set_col_widths(ws, [(1, 18)] + [(i, 13) for i in range(2, 27)])
    freeze(ws, row=3)
    path = save_wb(wb, f"Nokia_SGSN_{date_str}.xlsx")
    log(f"[Nokia Report] Saved: {path}")
    return path
