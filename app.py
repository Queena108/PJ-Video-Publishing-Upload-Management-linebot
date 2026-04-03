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

# ── 環境變數讀取 ──
LINE_TOKEN   = os.environ["LINE_CHANNEL_ACCESS_TOKEN"]
LINE_SECRET  = os.environ["LINE_CHANNEL_SECRET"]
SHEET_ID     = "1pKm2MHPNoPOvWEv-y-YqUBQ3an-IVraGlKFv3cci1q8" # 試算表 ID
USER_ID      = os.environ["LINE_USER_ID"]
GSHEET_CREDS = os.environ["GOOGLE_CREDS_JSON"]
TZ           = pytz.timezone("Asia/Taipei")

handler       = WebhookHandler(LINE_SECRET)
configuration = Configuration(access_token=LINE_TOKEN)

# ── 狀態與 IP 欄位對應 (根據實測 CSV 結構) ──
STATUS_MAP = {
    "已排程": "已排程", "排程": "已排程",
    "已上片": "✓ 已上片", "上片": "✓ 已上片", "完成": "✓ 已上片",
    "不上片": "⚠ 不上片", "失敗": "⚠ 不上片",
    "未排程": "—未排程", "不上": "—未排程",
}

# 統一節目名稱對應到排程表欄位
SHOW_ALIASES = {
    "董律師": "董律師", "董律": "董律師",
    "蟎人": "蟎人",
    "AIDA": "AIDA", "aida": "AIDA",
    "MICO": "MICO", "mico": "MICO",
    "真心話": "真心話", "真心話長": "真心話", "真心話短": "真心話",
    "芯芯": "芯芯",
    "而璽": "而璽",
    "今晚": "今晚", "今晚長": "今晚", "今晚短": "今晚",
}

# 欄位索引：D(4), E(5), F(6), G(7), H(8), I(9), J(10), K(11)
SHOW_COL_BASE = {
    "董律師": 4, "蟎人": 5, "AIDA": 6, "MICO": 7,
    "真心話": 8, "芯芯": 9, "而璽": 10, "今晚": 11,
}

# 4 月份所有 IP 表區塊偏移 (+12)
SHOW_COL_APR_ALLIP = {k: v + 12 for k, v in SHOW_COL_BASE.items()}

# ── 工具函式 ──────────────────────────────────
def get_client():
    info = json.loads(GSHEET_CREDS)
    creds = Credentials.from_service_account_info(info, scopes=["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"])
    return gspread.authorize(creds)

def open_workbook():
    return get_client().open_by_key(SHEET_ID)

def format_date(raw_date):
    """將日期標準化為 M/D 格式，解決 2026/4/3 或 4/3 的比對問題"""
    if not raw_date: return ""
    dm = re.search(r'(\d+)/(\d+)', str(raw_date))
    return f"{int(dm.group(1))}/{int(dm.group(2))}" if dm else str(raw_date).strip()

def normalize_show(raw):
    raw = raw.strip()
    for alias, canonical in SHOW_ALIASES.items():
        if alias.lower() in raw.lower() or raw.lower() in alias.lower():
            return canonical
    return raw

# ── 核心功能：抓取今日資料 ────────────────────────────
def get_today_rows(sheet):
    today = datetime.datetime.now(TZ).date()
    target = f"{today.month}/{today.day}" # M/D 格式
    
    rows = sheet.get_all_values()
    results = []
    for i, row in enumerate(rows):
        if len(row) < 5: continue
        cell_date = format_date(row[0])
        # 模糊比對日期字串
        if cell_date == target or target in cell_date:
            results.append({
                "row_num": i + 1, "date": cell_date, "slot": row[2],
                "name": row[3], "ep": row[4],
                "IG_FB": row[5] if len(row)>5 else "",
                "TK": row[6] if len(row)>6 else "",
                "YT": row[7] if len(row)>7 else "",
                "status": row[9] if len(row)>9 else (row[8] if len(row)>8 else "")
            })
    return results

# ── 核心功能：同步更新 (新增/刪除) ──────────────────────
def sync_all_sheets(show_name, ep_num, date_str, action="update"):
    """
    action: "update" (新增/填寫), "delete" (刪除)
    """
    wb = open_workbook()
    target_date = format_date(date_str)
    new_val = f"EP{ep_num}" if action == "update" else "EP"
    short_prefix = show_name[:2]
    results = []

    # 1. 確認表更新
    try:
        sh = wb.worksheet(f"{datetime.datetime.now(TZ).month:02d}月確認表")
        rows = sh.get_all_values(); updated = []
        for i, row in enumerate(rows):
            if len(row) < 5: continue
            if show_name.lower() in str(row[3]).lower() and format_date(row[0]) == target_date:
                sh.update_cell(i + 1, 5, new_val); updated.append(target_date)
        results.append(f"✅ [確認表]: " + (f"同步 {', '.join(updated)}" if updated else "無變動"))
    except Exception as e: results.append(f"⚠️ [確認表] 錯誤: {e}")

    # 2. 月排程表 / 3. 所有 IP 表 (邏輯相似)
    try:
        month_sh = wb.worksheet(f"{datetime.datetime.now(TZ).month:02d}月排程表")
        col = SHOW_COL_BASE.get(show_name)
        if month_sh and col:
            rows = month_sh.get_all_values(); updated = []
            for i, row in enumerate(rows):
                if format_date(row[2] if len(row)>2 else "") == target_date:
                    cell_val = f"{short_prefix} {new_val}" if action == "update" else f"{short_prefix} EP"
                    month_sh.update_cell(i + 1, col, cell_val); updated.append(target_date)
            results.append(f"✅ [月排程表]: " + (f"同步 {', '.join(updated)}" if updated else "無變動"))
    except Exception as e: results.append(f"⚠️ [月排程表] 錯誤: {e}")

    # 4. 今日快速確認 (跳過前 4 行標題)
    try:
        quick = wb.worksheet("今日快速確認")
        rows = quick.get_all_values(); updated = []
        for i, row in enumerate(rows):
            if i < 4 or len(row) < 3: continue
            if show_name.lower() in str(row[1]).lower():
                quick.update_cell(i + 1, 3, new_val); updated.append(show_name)
        results.append(f"✅ [快速確認]: " + (f"同步 {', '.join(updated)}" if updated else "無變動"))
    except Exception as e: results.append(f"⚠️ [快速確認] 錯誤: {e}")

    return results

# ── LINE Bot 事件處理 ────────────────────────────
@handler.add(MessageEvent, message=TextMessageContent)
def on_msg(event):
    text = event.message.text.strip()
    token = event.reply_token
    wb = open_workbook()

    # 指令：今日清單
    if text in ("今日", "今天", "清單"):
        sh = wb.worksheet(f"{datetime.datetime.now(TZ).month:02d}月確認表")
        rows = get_today_rows(sh)
        if not rows:
            with ApiClient(configuration) as api_client:
                MessagingApi(api_client).reply_message(ReplyMessageRequest(
                    reply_token=token, messages=[TextMessage(text="📅 今天沒有排定節目喔！")]))
            return
        
        msg = f"📋 {datetime.datetime.now(TZ).month}/{datetime.datetime.now(TZ).day} 今日清單\n"
        for r in rows:
            icon = "✅" if "✓" in r["status"] else "⏳"
            msg += f"\n{icon} {r['name']} {r['ep']} ({r['slot']})"
        
        with ApiClient(configuration) as api_client:
            MessagingApi(api_client).reply_message(ReplyMessageRequest(reply_token=token, messages=[TextMessage(text=msg)]))

    # 指令：新增 (格式：新增 董律師 EP180 4/3)
    elif text.startswith("新增"):
        parts = text.split()
        if len(parts) >= 4:
            show = normalize_show(parts[1])
            ep = parts[2].replace("EP", "")
            date = parts[3]
            res = sync_all_sheets(show, ep, date, "update")
            reply = f"🚀 已為您同步新增：\n" + "\n".join(res)
            with ApiClient(configuration) as api_client:
                MessagingApi(api_client).reply_message(ReplyMessageRequest(reply_token=token, messages=[TextMessage(text=reply)]))

    # 指令：刪除 (格式：刪除 董律師 EP180 4/3)
    elif text.startswith("刪除") or text.startswith("刪集數"):
        parts = text.split()
        if len(parts) >= 4:
            show = normalize_show(parts[1])
            ep = parts[2].replace("EP", "")
            date = parts[3]
            res = sync_all_sheets(show, ep, date, "delete")
            reply = f"🗑 已為您同步清除：\n" + "\n".join(res)
            with ApiClient(configuration) as api_client:
                MessagingApi(api_client).reply_message(ReplyMessageRequest(reply_token=token, messages=[TextMessage(text=reply)]))

@app.route("/callback", methods=["POST"])
def callback():
    signature = request.headers.get("X-Line-Signature", "")
    body = request.get_data(as_text=True)
    try: handler.handle(body, signature)
    except InvalidSignatureError: abort(400)
    return "OK"

if __name__ == "__main__":
    app.run(port=int(os.environ.get("PORT", 5000)))
