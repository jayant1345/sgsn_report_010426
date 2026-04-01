# SGSN Report Tool — Python v2.0
**BSNL Gujarat NOC | Replacement for SGSNReport.jar**

## Requirements
- Python 3.8+
- PostgreSQL access to NetAct server (10.135.9.41)
- MySQL access to reportsDB (10.135.9.41)

## Installation
```
pip install -r requirements.txt
```

## Configuration
Edit `config.py` to change:
- PostgreSQL host/user/password
- MySQL host/user/password
- Output folder path

## Run
```
python main_gui.py
```

## How It Works

### Report Buttons (NO file picker needed)
- Select date using the date bar
- Click any report button
- Tool queries PostgreSQL/MySQL directly
- Excel file saved to `SGSN Reports/` folder
- Click "Open Output Folder" to view

### CSV Upload (file picker required)
1. Select date
2. Click "Select & Upload" for ZTE GTP / LAN / 4G files
3. Select CSV exported from ZTE EMS
4. Data uploaded to PostgreSQL automatically

### Workflow for Daily Report
1. Upload ZTE CSV files (if new data)
2. Click "Download Summary" — calculates daily totals, stores in MySQL
3. Click report buttons as needed

## File Structure
```
sgsn_v2/
├── main_gui.py          Main window (Tkinter)
├── config.py            DB credentials and settings
├── db.py                PostgreSQL and MySQL helpers
├── excel_helper.py      Excel styling utilities
├── downld_sgsn.py       Download summary (PG → MySQL)
├── report_zte.py        ZTE SGSN daily report
├── report_nokia.py      Nokia SGSN daily report
├── report_4g.py         Nokia 4G LTE report
├── report_combined.py   Combined 2G+3G+4G report
├── report_monthly.py    Monthly trend report
├── report_trai.py       TRAI regulatory report
├── csv_uploader.py      ZTE CSV upload to PostgreSQL
└── SGSN Reports/        Output Excel files (auto-created)
```

## Database Summary

| Database | Host | Tables Used |
|---|---|---|
| PostgreSQL noc | 10.135.9.41:5432 | p_obs_zte, p_obs_zte_lan, p_obs_zte_lan_4g_tput, p_obs_4g, nokia_sgsn_report, nokia_4g_sgsn_report, nokia_sgsn_report_out, nokia_sgsn_report_pdp_out, nokia_sgsn_report_att_out, sgsn_report_sbc_kpi, sgsn_report_volte_dedicated_bearer_act |
| MySQL reportsDB | 10.135.9.41:3306 | downldSgsn, peakThroughputSgsn, SGSNrpt_, sgsn_2g3g_pdp_max, sgsn_trai_report |
