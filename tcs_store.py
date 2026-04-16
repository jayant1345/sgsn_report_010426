# tcs_store.py
# Stores TCS EMS daily data into MySQL table sgsn_tcs_daily
# so MAR-26 TCS U/L and D/L columns populate correctly every day
# even when the TCS CSV file is not present at report time.
#
# MySQL table: reportsDB.sgsn_tcs_daily
#   date       DATE         PK
#   dl_mb      DOUBLE       Total DL volume MB (sum all eNBs)
#   ul_mb      DOUBLE       Total UL volume MB (sum all eNBs)
#   total_gb   DOUBLE       Total volume GB
#   enb_count  INT          Number of eNBs processed

from db import my_execute, my_query


# ─── table DDL (run once automatically) ──────────────────────────────────────

_DDL = """
CREATE TABLE IF NOT EXISTS `reportsDB`.`sgsn_tcs_daily` (
    `date`      DATE         NOT NULL PRIMARY KEY,
    `dl_mb`     DOUBLE       NOT NULL DEFAULT 0,
    `ul_mb`     DOUBLE       NOT NULL DEFAULT 0,
    `total_gb`  DOUBLE       NOT NULL DEFAULT 0,
    `enb_count` INT          NOT NULL DEFAULT 0,
    `updated_at` TIMESTAMP   DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""


def ensure_table():
    """Create sgsn_tcs_daily table if it doesn't exist."""
    try:
        my_execute(_DDL)
    except Exception:
        pass


def store_tcs(tcs_dict, log=print):
    """
    Save one day's TCS totals into MySQL sgsn_tcs_daily.
    tcs_dict = result from tcs_reader.read_tcs_daily()
    """
    if not tcs_dict:
        return
    ensure_table()
    date_str = str(tcs_dict.get('date', ''))
    if not date_str or date_str == 'None':
        log("[TCS Store] WARNING: no date in TCS data, skipping store.")
        return
    my_execute("""
        INSERT INTO `reportsDB`.`sgsn_tcs_daily`
            (`date`, `dl_mb`, `ul_mb`, `total_gb`, `enb_count`)
        VALUES (%s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            `dl_mb`     = VALUES(`dl_mb`),
            `ul_mb`     = VALUES(`ul_mb`),
            `total_gb`  = VALUES(`total_gb`),
            `enb_count` = VALUES(`enb_count`),
            `updated_at`= CURRENT_TIMESTAMP
    """, (date_str,
          round(tcs_dict.get('dl_mb',    0), 3),
          round(tcs_dict.get('ul_mb',    0), 3),
          round(tcs_dict.get('total_gb', 0), 4),
          int(tcs_dict.get('enb_count',  0))))
    log(f"[TCS Store] Saved {date_str}: "
        f"DL={tcs_dict['dl_mb']/1024:.0f}GB  "
        f"UL={tcs_dict['ul_mb']/1024:.0f}GB  "
        f"eNBs={tcs_dict['enb_count']}")


def load_tcs_month(year, month):
    """
    Load all TCS daily rows for a month from MySQL.
    Returns {day: {dl_mb, ul_mb, total_gb, enb_count}}
    """
    ensure_table()
    try:
        rows = my_query("""
            SELECT DAY(`date`) AS day_,
                   `dl_mb`, `ul_mb`, `total_gb`, `enb_count`
            FROM `reportsDB`.`sgsn_tcs_daily`
            WHERE MONTH(`date`) = %s AND YEAR(`date`) = %s
            ORDER BY `date`
        """, (month, year))
        return {int(r['day_']): r for r in rows}
    except Exception:
        return {}


def load_tcs_day(date_str):
    """Load one day's TCS data from MySQL. Returns dict or None."""
    ensure_table()
    try:
        rows = my_query("""
            SELECT `dl_mb`, `ul_mb`, `total_gb`, `enb_count`
            FROM `reportsDB`.`sgsn_tcs_daily`
            WHERE `date` = %s
        """, (date_str,))
        if rows:
            r = rows[0]
            return {
                'dl_mb':     float(r['dl_mb']     or 0),
                'ul_mb':     float(r['ul_mb']      or 0),
                'total_gb':  float(r['total_gb']   or 0),
                'enb_count': int(r['enb_count']    or 0),
                'day':       int(date_str[8:10]),
            }
    except Exception:
        pass
    return None


def clear_tcs_month(year, month):
    """Delete all TCS rows for a given month — use to fix wrong data."""
    ensure_table()
    my_execute("""
        DELETE FROM `reportsDB`.`sgsn_tcs_daily`
        WHERE MONTH(`date`) = %s AND YEAR(`date`) = %s
    """, (month, year))


def clear_tcs_all():
    """Delete ALL rows from sgsn_tcs_daily — full reset."""
    ensure_table()
    my_execute("DELETE FROM `reportsDB`.`sgsn_tcs_daily`")
