# config.py
# Central configuration for SGSN Report Tool
# Edit this file to change server IP, credentials, or output paths

import os

# PostgreSQL settings (Nokia NetAct database)
PG_HOST     = "10.135.9.41"
PG_PORT     = 5432
PG_DB       = "noc"
PG_USER     = "sdemsc"
PG_PASS     = "sdEmsC@1004"

# MySQL settings (BSNL Reports database)
MY_HOST     = "10.135.9.41"
MY_PORT     = 3306
MY_DB       = "reportsDB"
MY_USER     = "sdemsc"
MY_PASS     = "sdEmsC@1004"

# SQLite local DB (optional - overrides above if present)
SQLITE_PATH = "myLocalDB.slt"

# Output folder for generated Excel reports
OUTPUT_DIR  = "SGSN Reports"

# Timeline file (288 five-minute slots)
TIMELINE_FILE = "impFile/timeLines.tm"

# Bytes to MB conversion
MB = 1048576

# App title
APP_TITLE = "SGSN Report Tool — BSNL Gujarat NOC"
APP_VERSION = "v2.0 (Python)"

# ZTE GGSN closure — after this date DT carries signaling only (no data).
# All DT data traffic is redirected through p_obs_zte_lan_4g_tput (files 7-13).
# p_obs_zte_lan (files 0-6) carries only DT signaling from this date onward.
ZTE_GGSN_CUTOVER_DATE = "2026-03-30"

os.makedirs(OUTPUT_DIR, exist_ok=True)
