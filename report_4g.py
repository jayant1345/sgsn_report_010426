# report_4g.py
# Nokia 4G LTE Daily Report
# Joins nokia_sgsn_report + sgsn_report_sbc_kpi + sgsn_report_volte_dedicated_bearer_act

from db import pg_query
from excel_helper import (Workbook, write_title, write_subheader, write_row,
                           set_col_widths, freeze, save_wb, C_HEADER_BG)


def _4g_hourly_query(date_str):
    return pg_query("""
        SELECT
            t1.period_start_time,
            t1.avg_emm_reg_users,
            t1.avg_active_eps_bearers,
            t1.avg_registered_eps_volte_users,
            t1.peak_active_def_eps_bearers_mme,
            t1.peak_active_ded_eps_bearers_mme,
            t1.peak_emm_reg_users_mme,
            t1.avg_emm_dereg_users,
            t2.mosvalue,
            t2.avgrtppacketloss,
            t2.reregistrationsuccessrate,
            t3.att_flns_5054b,
            t3.succ,
            t3.per_succ_flns_5053b
        FROM (
            SELECT
                min(period_start_time) period_start_time,
                round(avg(flns_5035a)::numeric, 0)  AS avg_emm_reg_users,
                round(avg(flns_5050a)::numeric, 0)  AS avg_active_eps_bearers,
                round(max(flns_5055a)::numeric, 0)  AS avg_registered_eps_volte_users,
                round(max(flns_3285a)::numeric, 0)  AS peak_active_def_eps_bearers_mme,
                round(max(flns_3286a)::numeric, 0)  AS peak_active_ded_eps_bearers_mme,
                round(max(flns_5025a)::numeric, 0)  AS peak_emm_reg_users_mme,
                round(avg(flns_5026a)::numeric, 0)  AS avg_emm_dereg_users
            FROM public.nokia_sgsn_report
            WHERE date(period_start_time) = %s
            GROUP BY extract(hour FROM period_start_time)
        ) t1
        INNER JOIN (
            SELECT
                min(period_start_time) period_start_time,
                round(avg(mosvalue)::numeric, 2)                     AS mosvalue,
                round(avg(avgrtppacketloss)::numeric, 2)             AS avgrtppacketloss,
                round(avg(reregistrationsuccessrate)::numeric, 2)    AS reregistrationsuccessrate
            FROM public.sgsn_report_sbc_kpi
            WHERE date(period_start_time) = %s
            GROUP BY extract(hour FROM period_start_time)
        ) t2 ON t1.period_start_time = t2.period_start_time
        INNER JOIN (
            SELECT
                period_start_time,
                att_flns_5054b,
                succ,
                round(per_succ_flns_5053b::numeric, 2) AS per_succ_flns_5053b
            FROM public.sgsn_report_volte_dedicated_bearer_act
            WHERE date(period_start_time) = %s
        ) t3 ON t3.period_start_time = t2.period_start_time
        ORDER BY t1.period_start_time
    """, (date_str, date_str, date_str))


def _4g_volume_query(date_str):
    """Hourly 4G volume upload/download from nokia_4g_sgsn_report."""
    return pg_query("""
        SELECT
            min(period_start_time)          AS dtime,
            sum(lte_5213a) / 1024           AS upload_4g_mb,
            sum(lte_5212a) / 1024           AS download_4g_mb,
            (sum(lte_5213a) + sum(lte_5212a)) / 1024 AS total_4g_mb
        FROM public.nokia_4g_sgsn_report
        WHERE date(period_start_time) = %s
        GROUP BY extract(hour FROM period_start_time)
        ORDER BY extract(hour FROM period_start_time)
    """, (date_str,))


def generate_4g_report(date_str, log=print):
    log(f"[4G Report] Querying data for {date_str}...")
    vol_rows  = _4g_volume_query(date_str)
    kpi_rows  = _4g_hourly_query(date_str)

    if not vol_rows:
        raise ValueError(f"No Nokia 4G data found for {date_str}. Check NetAct 4G data.")

    wb = Workbook()
    ws = wb.active
    ws.title = "Nokia4G"
    ws.sheet_view.showGridLines = False

    write_title(ws, 1, 1, f"Nokia 4G LTE Daily Report — {date_str}", span=20,
                bg=C_HEADER_BG, size=13)

    headers_vol = [
        "Hour", "UL 4G (MB)", "DL 4G (MB)", "Total 4G (MB)"
    ]
    headers_kpi = [
        "Avg EMM Reg", "Avg EPS Bearers", "VoLTE Users",
        "Peak Def Bearer", "Peak Ded Bearer", "Peak EMM Users",
        "Avg Dereg Users"
    ]
    headers_volte = [
        "MOS Value", "RTP Pkt Loss %", "Re-Reg Succ %",
        "VoLTE Att", "VoLTE Succ", "VoLTE Succ %"
    ]

    write_subheader(ws, 2, headers_vol,   col_start=1)
    write_subheader(ws, 2, headers_kpi,   col_start=5)
    write_subheader(ws, 2, headers_volte, col_start=12)

    # Build kpi dict keyed by hour
    kpi_dict = {}
    for r in kpi_rows:
        if r["period_start_time"]:
            h = r["period_start_time"].hour
            kpi_dict[h] = r

    total_ul = total_dl = total_4g = 0.0

    for i, r in enumerate(vol_rows):
        rn = i + 3
        alt = i % 2 == 1
        dtime = r["dtime"]
        hour = dtime.hour if dtime else i
        hour_label = str(dtime)[:16] if dtime else f"Hour {hour}"

        v = lambda x: round(float(x or 0), 2)
        ul = v(r["upload_4g_mb"])
        dl = v(r["download_4g_mb"])
        tot = v(r["total_4g_mb"])

        total_ul += ul
        total_dl += dl
        total_4g += tot

        write_row(ws, rn, [hour_label, ul, dl, tot], alt=alt, col_start=1)

        kpi = kpi_dict.get(hour, {})
        kv = lambda k: round(float(kpi.get(k) or 0), 2) if kpi else 0

        write_row(ws, rn, [
            kv("avg_emm_reg_users"), kv("avg_active_eps_bearers"),
            kv("avg_registered_eps_volte_users"), kv("peak_active_def_eps_bearers_mme"),
            kv("peak_active_ded_eps_bearers_mme"), kv("peak_emm_reg_users_mme"),
            kv("avg_emm_dereg_users")
        ], alt=alt, col_start=5)

        write_row(ws, rn, [
            kv("mosvalue"), kv("avgrtppacketloss"), kv("reregistrationsuccessrate"),
            int(kv("att_flns_5054b")), int(kv("succ")), kv("per_succ_flns_5053b")
        ], alt=alt, col_start=12)

    tr = len(vol_rows) + 3
    write_row(ws, tr, ["TOTAL", round(total_ul, 2), round(total_dl, 2), round(total_4g, 2)],
              is_total=True, bold=True, col_start=1)

    set_col_widths(ws, [(1, 18)] + [(i, 13) for i in range(2, 20)])
    freeze(ws, row=3)
    path = save_wb(wb, f"Nokia_4G_{date_str}.xlsx")
    log(f"[4G Report] Saved: {path}")
    return path
