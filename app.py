import os, json, datetime, re
from flask import Flask, request, abort
from linebot.v3 import WebhookHandler
from linebot.v3.exceptions import InvalidSignatureError
from linebot.v3.messaging import (
    Configuration, ApiClient, MessagingApi,
    ReplyMessageRequest, PushMessageRequest, TextMessage
)
from linebot.v3.webhooks import MessageEvent, TextMessageContent
import gspread
from google.oauth2.service_account import Credentials
from apscheduler.schedulers.background import BackgroundScheduler
import pytz

app = Flask(__name__)

LINE_TOKEN   = os.environ["LINE_CHANNEL_ACCESS_TOKEN"]
LINE_SECRET  = os.environ["LINE_CHANNEL_SECRET"]
SHEET_ID     = "1pKm2MHPNoPOvWEv-y-YqUBQ3an-IVraGlKFv3cci1q8"
USER_ID      = os.environ["LINE_USER_ID"]
GSHEET_CREDS = os.environ["GOOGLE_CREDS_JSON"]
TZ           = pytz.timezone("Asia/Taipei")

handler       = WebhookHandler(LINE_SECRET)
configuration = Configuration(access_token=LINE_TOKEN)

# ── 狀態與對應設定 ──
STATUS_MAP = {
    "已排程": "已排程", "排程": "已排程", "排程中": "已排程",
    "已上片": "✓ 已上片", "上片": "✓ 已上片", "完成": "✓ 已上片", "已確認": "✓ 已上片",
    "不上片": "⚠ 不上片", "有問題": "⚠ 不上片", "失敗": "⚠ 不上片",
    "未排程": "—未排程", "不上": "—未排程", "\\未排程": "—未排程",
}

# 統一節目別名 (根據排程表欄位統一)
SHOW_ALIASES = {
    "董律": "董律師", "董律師": "董律師",
    "蟎人": "蟎人",
    "aida": "AIDA", "AIDA": "AIDA",
    "mico": "MICO", "MICO": "MICO",
    "真心話": "真心話", "真心話長": "真心話", "真心話短": "真心話",
    "芯芯": "芯芯",
    "而璽": "而璽", "而璽設計": "而璽",
    "今晚": "今晚", "今晚長": "今晚", "今晚短": "今晚",
}

# 根據 04月排程表 CSV 結構：D欄(4)開始
SHOW_COL_BASE = {
    "董律師": 4, "蟎人": 5, "AIDA": 6, "MICO": 7,
    "真心話": 8, "芯芯": 9, "而璽": 10, "今晚": 11,
}

# 所有IP表 4月區塊偏移量 (+12)
SHOW_COL_APR_ALLIP = {k: v+12 for k, v in SHOW_COL_BASE.items()}

S_SCHED = "已排程"; S_DONE = "✓ 已上片"; S_ERR = "⚠ 不上片"; S_SKIP = "—未排程"

# ── 工具函式 ──────────────────────────────────
def get_client():
    info   = json.loads(GSHEET_CREDS)
    scopes = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds  = Credentials.from_service_account_info(info, scopes=scopes)
    return gspread.authorize(creds)

def open_workbook():
    return get_client().open_by_key(SHEET_ID)

def get_confirm_sheet(wb=None):
    month = datetime.datetime.now(TZ).month
    wb    = wb or open_workbook()
    return wb.worksheet(f"{month:02d}月確認表")

def get_month_schedule_sheet(wb=None):
    month = datetime.datetime.now(TZ).month
    wb    = wb or open_workbook()
    return wb.worksheet(f"{month:02d}月排程表")

def get_quick_confirm_sheet(wb=None):
    wb = wb or open_workbook()
    return wb.worksheet("今日快速確認")

def get_allip_sheet(wb=None):
    wb = wb or open_workbook()
    return wb.worksheet("所有IP上片排程表")

def normalize_show(raw):
    raw = raw.strip()
    for alias, canonical in SHOW_ALIASES.items():
        if alias.lower() in raw.lower() or raw.lower() in alias.lower():
            return canonical
    return raw

def format_date(raw_date):
    """將日期統一轉換為 M/D 格式"""
    dm = re.search(r'(\d+)/(\d+)', str(raw_date))
    return f"{int(dm.group(1))}/{int(dm.group(2))}" if dm else str(raw_date).strip()

# ── 核心同步邏輯：新增/補集數 ────────────────────────────
def write_to_schedule_sheets(show_name, ep_num, date_str=None, action="fill"):
    wb = open_workbook(); results = []; new_ep = f"EP{ep_num}"
    target_date = format_date(date_str) if date_str else None
    short_prefix = show_name[:2]

    # 1. 確認表
    try:
        sh = get_confirm_sheet(wb); rows = sh.get_all_values(); updated = []
        for i, row in enumerate(rows):
            if len(row) < 5: continue
            if not (show_name.lower() in str(row[3]).lower()): continue
            cell_date = format_date(row[0])
            if (action == "fill" and "EP" in str(row[4]).upper() and not re.search(r'\d', str(row[4]))) or \
               (action == "add" and cell_date == target_date):
                sh.update_cell(i + 1, 5, new_ep); updated.append(cell_date)
        results.append(f"✅ [確認表]: " + (f"更新 {', '.join(updated)}" if updated else "無變動"))
    except Exception as e: results.append(f"⚠️ [確認表] 錯誤: {e}")

    # 2. 月排程表
    try:
        sh = get_month_schedule_sheet(wb); col = SHOW_COL_BASE.get(show_name); updated = []
        if sh and col:
            rows = sh.get_all_values()
            for i, row in enumerate(rows):
                cell_date = format_date(row[2]) if len(row) > 2 else ""
                cur = str(row[col-1]).strip() if col <= len(row) else ""
                if (action == "fill" and "EP" in cur.upper() and not re.search(r'\d', cur)) or \
                   (action == "add" and cell_date == target_date):
                    sh.update_cell(i + 1, col, f"{short_prefix} {new_ep}"); updated.append(cell_date)
            results.append(f"✅ [月排程表]: " + (f"更新 {', '.join(updated)}" if updated else "無變動"))
    except Exception as e: results.append(f"⚠️ [月排程表] 錯誤: {e}")

    # 3. 所有IP表
    try:
        sh = get_allip_sheet(wb); month = datetime.datetime.now(TZ).month
        col = SHOW_COL_APR_ALLIP.get(show_name) if month == 4 else SHOW_COL_BASE.get(show_name)
        date_col = 15 if month == 4 else 3; updated = []
        if sh and col:
            rows = sh.get_all_values()
            for i, row in enumerate(rows):
                if len(row) < date_col: continue
                cell_date = format_date(row[date_col-1])
                cur = str(row[col-1]).strip() if col <= len(row) else ""
                if (action == "fill" and "EP" in cur.upper() and not re.search(r'\d', cur)) or \
                   (action == "add" and cell_date == target_date):
                    sh.update_cell(i + 1, col, f"{short_prefix} {new_ep}"); updated.append(cell_date)
            results.append(f"✅ [所有IP表]: " + (f"更新 {', '.join(updated)}" if updated else "無變動"))
    except Exception as e: results.append(f"⚠️ [所有IP表] 錯誤: {e}")

    # 4. 今日快速確認
    try:
        sh = get_quick_confirm_sheet(wb); rows = sh.get_all_values(); updated = []
        for i, row in enumerate(rows):
            if i < 4 or len(row) < 3: continue # 略過前4行標題
            if show_name.lower() in str(row[1]).lower():
                sh.update_cell(i + 1, 3, new_ep); updated.append(str(row[1]))
        results.append(f"✅ [快速確認]: " + (f"更新 {', '.join(updated)}" if updated else "無變動"))
    except Exception as e: results.append(f"⚠️ [快速確認] 錯誤: {e}")

    return results

# ── 核心同步邏輯：刪除集數 ────────────────────────────
def delete_ep_from_sheets(show_name, ep_num=None, date_str=None):
    wb = open_workbook(); results = []; target_date = format_date(date_str) if date_str else None
    ep_str = f"EP{ep_num}" if ep_num else None

    # 1. 確認表
    try:
        sh = get_confirm_sheet(wb); rows = sh.get_all_values(); deleted = []
        for i, row in enumerate(rows):
            if len(row) < 5: continue
            if not (show_name.lower() in str(row[3]).lower()): continue
            cell_date, row_ep = format_date(row[0]), str(row[4]).upper()
            if (ep_str and ep_str.upper() in row_ep) or (target_date and cell_date == target_date):
                sh.update_cell(i + 1, 5, "EP"); deleted.append(cell_date)
        results.append(f"🗑 [確認表]: " + (f"清除 {', '.join(deleted)}" if deleted else "無變動"))
    except Exception as e: results.append(f"⚠️ [確認表] 錯誤: {e}")

    # 2. 月排程表 / 3. 所有IP表 (合併邏輯)
    def clear_schedule(sheet_func, col_map, date_idx):
        try:
            s = sheet_func(wb); col = col_map.get(show_name); del_list = []
            if s and col:
                rows = s.get_all_values()
                for i, row in enumerate(rows):
                    if len(row) < max(col, date_idx): continue
                    cell_date, cur = format_date(row[date_idx-1]), str(row[col-1])
                    if (ep_str and ep_str.upper() in cur.upper()) or (target_date and cell_date == target_date):
                        prefix = re.sub(r'EP.*$', '', cur).strip()
                        s.update_cell(i + 1, col, f"{prefix} EP".strip()); del_list.append(cell_date)
            return del_list
        except: return []

    results.append(f"🗑 [月排程表]: 清除 {', '.join(clear_schedule(get_month_schedule_sheet, SHOW_COL_BASE, 3)) or '無變動'}")
    
    month = datetime.datetime.now(TZ).month
    allip_col = SHOW_COL_APR_ALLIP if month == 4 else SHOW_COL_BASE
    allip_date = 15 if month == 4 else 3
    results.append(f"🗑 [所有IP表]: 清除 {', '.join(clear_schedule(get_allip_sheet, allip_col, allip_date)) or '無變動'}")

    # 4. 今日快速確認
    try:
        sh = get_quick_confirm_sheet(wb); rows = sh.get_all_values(); deleted = []
        for i, row in enumerate(rows):
            if i < 4 or len(row) < 3: continue
            if show_name.lower() in str(row[1]).lower():
                sh.update_cell(i + 1, 3, "EP"); deleted.append(str(row[1]))
        results.append(f"🗑 [快速確認]: 清除 {', '.join(deleted) if deleted else '無變動'}")
    except Exception as e: results.append(f"⚠️ [快速確認] 錯誤: {e}")

    return results

# ── 以下維持原有的 Webhook 與 Message Handler 邏輯 ──
# (包含今日清單、狀態查詢、補集數指令、新增指令、刪集數指令等)
# ... (此處省略部分重複的 Line Bot 框架代碼，請保留你原始檔案的後半段)
