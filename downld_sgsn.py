# downld_sgsn.py
# Download Summary — reads SGSN traffic data from PostgreSQL,
# converts ALL volumes to GB, stores into MySQL reportsDB.
#
# MySQL downldSgsn columns — ALL stored in GB:
#   downldSGSNZTE   = ZTE 2G+3G GB
#   downldSGSNdt    = LAN/DT total GB
#   downldSGSNNOKIA = Nokia 2G+3G GB
#   download4g      = Nokia 4G GB
#   downldSGSNtot   = Grand total GB
#
# Queries verified from Java downldSgsn.class bytecode.

from db import pg_query, pg_execute, pg_execute_many, my_execute, my_query
from tcs_store import load_tcs_day
from config import ZTE_GGSN_CUTOVER_DATE

MB_TO_GB = 1024.0


def download_sgsn_summary(date_str, log=print):
    """
    Reads all sources for date_str, converts to GB, writes to MySQL downldSgsn.
    """
    log(f"[downldSgsn] Starting for {date_str}...")

    # ── Nokia 4G volumes (lte counters are in MB → /1024 = GB) ───────────
    log("[downldSgsn] Reading Nokia 4G volumes...")
    nokia_4g_ud = pg_query("""
        SELECT round(sum(lte_5213a)::numeric / 1024, 4) AS vol_4g_ul_gb,
               round(sum(lte_5212a)::numeric / 1024, 4) AS vol_4g_dl_gb
        FROM public.nokia_4g_sgsn_report
        WHERE date(period_start_time) = %s
    """, (date_str,))
    nokia_4g_ul_gb = float(nokia_4g_ud[0]["vol_4g_ul_gb"] or 0) if nokia_4g_ud else 0
    nokia_4g_dl_gb = float(nokia_4g_ud[0]["vol_4g_dl_gb"] or 0) if nokia_4g_ud else 0
    nokia_4g_gb    = nokia_4g_ul_gb + nokia_4g_dl_gb

    # ── Nokia 4G hourly rows for p_obs_4g (store raw MB as before) ────────
    nokia_4g_rows = pg_query("""
        SELECT min(period_start_time) AS ts,
               sum(lte_5212a + lte_5213a) AS vol_4g_mb
        FROM public.nokia_4g_sgsn_report
        WHERE date(period_start_time) = %s
        GROUP BY extract(hour FROM period_start_time)
        ORDER BY extract(hour FROM period_start_time)
    """, (date_str,))
    log("[downldSgsn] Updating p_obs_4g...")
    pg_execute("DELETE FROM public.p_obs_4g WHERE date(dtime) < date(now() - interval '12 months')")
    pg_execute("DELETE FROM public.p_obs_4g WHERE date(dtime) = %s", (date_str,))
    if nokia_4g_rows:
        pg_execute_many(
            "INSERT INTO public.p_obs_4g(dtime, vol_4g) VALUES (%s, %s)",
            [(r["ts"], float(r["vol_4g_mb"] or 0)) for r in nokia_4g_rows if r["ts"]]
        )

    # ── ZTE SGSN 2G+3G (bytes / 1048576 = MB, /1024 = GB) ────────────────
    log("[downldSgsn] Reading ZTE volume...")
    zte_rows = pg_query("""
        SELECT SUM(GTP81 + GTP83 + GTP73 + GTP75) / 1048576.0 / 1024.0 AS total_gb
        FROM p_obs_zte
        WHERE date(end_time) = %s
    """, (date_str,))
    zte_gb = float((zte_rows[0]["total_gb"] or 0)) if zte_rows else 0

    # ── PRTG 4G LAN (p_obs_zte_lan_4g_tput) — carries Nokia+DT combined traffic ──
    log("[downldSgsn] Reading PRTG 4G LAN (DT) volume...")
    prtg4g_rows = pg_query("""
        SELECT SUM(coalesce(in_volume_kb,0) + coalesce(out_volume_kb,0)) / 1024.0 / 1024.0 AS total_gb
        FROM public.p_obs_zte_lan_4g_tput
        WHERE date(date_time) = %s
    """, (date_str,))
    prtg4g_gb = float((prtg4g_rows[0]["total_gb"] or 0)) if prtg4g_rows else 0

    # ── Nokia SGSN 2G+3G (flns counters in MB, /1024 = GB) ───────────────
    log("[downldSgsn] Reading Nokia SGSN 2G+3G volume...")
    nokia_rows = pg_query("""
        SELECT SUM(flns_19a + flns_20a + flns_2079a + flns_2080a) / 1024.0 AS total_gb
        FROM public.nokia_sgsn_report
        WHERE date(period_start_time) = %s
    """, (date_str,))
    nokia_gb = float((nokia_rows[0]["total_gb"] or 0)) if nokia_rows else 0

    # ── TCS 4G from MySQL sgsn_tcs_daily (stored by Upload ALL) ─────────
    try:
        tcs_row = load_tcs_day(date_str)
        tcs_gb  = round((float(tcs_row["dl_mb"] or 0) + float(tcs_row["ul_mb"] or 0)) / 1024.0, 4) if tcs_row else 0.0
    except Exception:
        tcs_gb = 0.0

    # ── DT (Direct Tunneling) — formula depends on cutover date ──────────
    # BEFORE cutover (< 2026-03-30):
    #   p_obs_zte_lan_4g_tput carries Nokia SGSN 2G/3G + DT data combined.
    #   DT = prtg4g_total - Nokia_2G3G
    #   Total = DT + Nokia_2G3G + Nokia_4G + TCS = prtg4g + Nokia_4G + TCS
    #
    # AFTER cutover (>= 2026-03-30, ZTE GGSN closed):
    #   DT carries signaling only (no data).
    #   p_obs_zte_lan_4g_tput now carries Nokia_2G3G + Nokia_4G + diverted DT data.
    #   DT_data = prtg4g_total - Nokia_2G3G - Nokia_4G  (residual = diverted DT data)
    #   Total = DT_data + Nokia_2G3G + Nokia_4G + TCS = prtg4g + TCS (no double-count)
    is_post_cutover = date_str >= ZTE_GGSN_CUTOVER_DATE
    if is_post_cutover:
        dt_gb = max(0.0, round(prtg4g_gb - nokia_gb - nokia_4g_gb, 4))
    else:
        dt_gb = max(0.0, round(prtg4g_gb - nokia_gb, 4))
    lan_gb = prtg4g_gb  # keep lan_gb for return value / logging

    # ── Grand total = DT + Nokia + 4G + TCS (ZTE=0, shutdown) ───────────
    # Simplifies to: prtg4g + Nokia_4G + TCS (pre-cutover)
    #             or: prtg4g + TCS           (post-cutover, Nokia_4G in prtg4g)
    total_gb = dt_gb + nokia_gb + nokia_4g_gb + tcs_gb

    cutover_flag = " [POST-CUTOVER: DT=signaling-only]" if is_post_cutover else ""
    log(f"[downldSgsn] ZTE={zte_gb:.2f}GB  Nokia2G3G={nokia_gb:.2f}GB  "
        f"PRTG4G={prtg4g_gb:.2f}GB  DT={dt_gb:.2f}GB  4G={nokia_4g_gb:.2f}GB  "
        f"TCS={tcs_gb:.2f}GB  Total={total_gb:.2f}GB{cutover_flag}")

    # ── Write to MySQL downldSgsn (all in GB) ─────────────────────────────
    log("[downldSgsn] Writing to MySQL downldSgsn...")
    my_execute("DELETE FROM downldSgsn WHERE DATE(Date_) = %s", (date_str,))
    my_execute("""
        INSERT INTO downldSgsn
            (Date_, downldSGSNDA, downldSGSNWMM,
             downldSGSNZTE, downldSGSNdt,  downldSGSNtot,
             downldSGSNNOKIA, download4g)
        VALUES (%s, 0, 0, %s, %s, %s, %s, %s)
    """, (date_str,
          round(zte_gb,      4),
          round(dt_gb,       4),   # DT = NIB - Nokia - ZTE (not raw NIB)
          round(total_gb,    4),
          round(nokia_gb,    4),
          round(nokia_4g_gb, 4)))

    log(f"[downldSgsn] Done for {date_str}")
    return {
        "zte_gb":      round(zte_gb,      4),
        "lan_gb":      round(lan_gb,      4),   # raw NIB total
        "dt_gb":       round(dt_gb,       4),   # DT = NIB - Nokia - ZTE
        "nokia_gb":    round(nokia_gb,    4),
        "nokia_4g_gb": round(nokia_4g_gb, 4),
        "tcs_gb":      round(tcs_gb,      4),
        "total_gb":    round(total_gb,    4),
    }


def download_peak_throughput(date_str, log=print):
    """
    Calculate and store peak throughput into MySQL peakThroughputSgsn.
    All speeds in Mbps. Verified from Java bytecode.
    """
    log("[Peak] Reading ZTE peak throughput...")
    zte_rows = pg_query("""
        SELECT GTP28 / 1024.0 AS upload_thru,
               GTP29 / 1024.0 AS download_thru
        FROM p_obs_zte
        WHERE date(end_time) = %s
        ORDER BY end_time
    """, (date_str,))
    zte_up   = max((float(r["upload_thru"]   or 0) for r in zte_rows), default=0)
    zte_down = max((float(r["download_thru"] or 0) for r in zte_rows), default=0)

    log("[Peak] Reading LAN peak speed...")
    lan_rows = pg_query("""
        SELECT round(out_speed_kbps / 1024.0, 2) AS up_speed,
               round(in_speed_kbps  / 1024.0, 2) AS down_speed
        FROM public.p_obs_zte_lan
        WHERE date(date_time) = %s
        ORDER BY date_time
    """, (date_str,))
    lan_up   = max((float(r["up_speed"]   or 0) for r in lan_rows), default=0)
    lan_down = max((float(r["down_speed"] or 0) for r in lan_rows), default=0)

    log("[Peak] Reading Nokia SGSN peak...")
    nokia_peak = pg_query("""
        SELECT round(flns_907a::numeric, 2) AS thu_up,
               round(flns_905b::numeric, 2) AS thu_down
        FROM public.nokia_sgsn_report
        WHERE date(period_start_time) = %s
        ORDER BY (flns_907a + flns_905b) DESC
        LIMIT 1
    """, (date_str,))
    nokia_up   = float(nokia_peak[0]["thu_up"]   or 0) if nokia_peak else 0
    nokia_down = float(nokia_peak[0]["thu_down"] or 0) if nokia_peak else 0

    log("[Peak] Reading Nokia 4G PRTG peak speed...")
    g4_rows = pg_query("""
        SELECT round(out_speed_kbps / 1024.0, 2) AS up_speed,
               round(in_speed_kbps  / 1024.0, 2) AS down_speed
        FROM public.p_obs_zte_lan_4g_tput
        WHERE date(date_time) = %s
        ORDER BY date_time
    """, (date_str,))
    nokia4g_up   = max((float(r["up_speed"]   or 0) for r in g4_rows), default=0)
    nokia4g_down = max((float(r["down_speed"] or 0) for r in g4_rows), default=0)

    log(f"[Peak] ZTE={zte_up:.2f}/{zte_down:.2f}  LAN={lan_up:.2f}/{lan_down:.2f}  "
        f"Nokia={nokia_up:.2f}/{nokia_down:.2f}  4G={nokia4g_up:.2f}/{nokia4g_down:.2f} Mbps")

    log("[Peak] Writing to MySQL peakThroughputSgsn...")
    my_execute(
        "DELETE FROM `reportsDB`.`peakThroughputSgsn` "
        "WHERE DATE(`Date_`) < NOW() - INTERVAL 3 YEAR"
    )
    my_execute("DELETE FROM peakThroughputSgsn WHERE DATE(Date_) = %s", (date_str,))
    my_execute("""
        INSERT INTO peakThroughputSgsn
            (Date_,
             upPeakThruDA,    downPeakThruDA,
             upPeakThruWMM,   downPeakThruWMM,
             upPeakThruZte,   downPeakThruZte,
             upPeakThruDt,    downPeakThruDt,
             upPeakThruNokia, downPeakThruNokia,
             upPeakThruNokia4g, downPeakThruNokia4g)
        VALUES (%s, 0,0, 0,0, %s,%s, %s,%s, %s,%s, %s,%s)
    """, (date_str,
          round(zte_up,     2), round(zte_down,     2),
          round(lan_up,     2), round(lan_down,     2),
          round(nokia_up,   2), round(nokia_down,   2),
          round(nokia4g_up, 2), round(nokia4g_down, 2)))

    log(f"[Peak] Done for {date_str}")
    return {
        "zte_up": zte_up, "zte_down": zte_down,
        "lan_up": lan_up, "lan_down": lan_down,
        "nokia_up": nokia_up, "nokia_down": nokia_down,
        "nokia4g_up": nokia4g_up, "nokia4g_down": nokia4g_down,
    }


