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

# ── 狀態對應 ──
STATUS_MAP = {
    "已排程": "已排程", "排程": "已排程", "排程中": "已排程",
    "已上片": "✓ 已上片", "上片": "✓ 已上片", "完成": "✓ 已上片", "已確認": "✓ 已上片",
    "不上片": "⚠ 不上片", "有問題": "⚠ 不上片", "失敗": "⚠ 不上片",
    "未排程": "—未排程", "不上": "—未排程", "\\未排程": "—未排程",
}

# ── 節目別名 ──
SHOW_ALIASES = {
    "董律": "董律師", "董律師": "董律師",
    "蟎人": "蟎人",
    "aida": "AIDA", "AIDA": "AIDA",
    "mico": "MICO", "MICO": "MICO",
    "真心話長": "真心話長", "真心話短": "真心話短", "真心話": "真心話短",
    "芯芯": "芯芯",
    "而璽": "而璽", "而璽設計": "而璽",
    "今晚": "今晚", "今晚長": "今晚長", "今晚短": "今晚短",
}

# ── 節目→排程表欄位對應 ──
# 4月排程表 & 所有IP的3月區塊（col 4-11）
SHOW_COL_BASE = {
    "董律師": 4, "蟎人": 5, "AIDA": 6, "MICO": 7,
    "真心話短": 8, "真心話長": 8, "芯芯": 9, "而璽": 10, "今晚": 11,
}
# 所有IP 4月區塊（+12）
SHOW_COL_APR_ALLIP = {k: v+12 for k, v in SHOW_COL_BASE.items()}

S_SCHED = "已排程"; S_DONE = "✓ 已上片"
S_ERR   = "⚠ 不上片"; S_SKIP = "—未排程"

# ── 工具函式 ──────────────────────────────────
def send_reply(reply_token, text):
    with ApiClient(configuration) as api_client:
        MessagingApi(api_client).reply_message(ReplyMessageRequest(
            reply_token=reply_token, messages=[TextMessage(text=text)]))

def send_push(text):
    with ApiClient(configuration) as api_client:
        MessagingApi(api_client).push_message(PushMessageRequest(
            to=USER_ID, messages=[TextMessage(text=text)]))

def get_client():
    info   = json.loads(GSHEET_CREDS)
    scopes = ["https://spreadsheets.google.com/feeds",
              "https://www.googleapis.com/auth/drive"]
    creds  = Credentials.from_service_account_info(info, scopes=scopes)
    return gspread.authorize(creds)

def open_workbook():
    return get_client().open_by_key(SHEET_ID)

def get_confirm_sheet(wb=None):
    month = datetime.datetime.now(TZ).month
    wb    = wb or open_workbook()
    return wb.worksheet(f"{month:02d}月確認表")

def get_month_schedule_sheet(wb=None):
    """當月排程表，例如 04月排程表"""
    month = datetime.datetime.now(TZ).month
    wb    = wb or open_workbook()
    try:
        return wb.worksheet(f"{month:02d}月排程表")
    except:
        return None

def get_quick_confirm_sheet(wb=None):
    """今日快速確認表"""
    wb = wb or open_workbook()
    try:
        return wb.worksheet("今日快速確認")
    except:
        return None

def get_allip_sheet(wb=None):
    """所有IP上片排程表"""
    wb = wb or open_workbook()
    try:
        return wb.worksheet("所有IP上片排程表")
    except:
        return None

def normalize_show(raw):
    raw = raw.strip()
    for alias, canonical in SHOW_ALIASES.items():
        if alias.lower() in raw.lower() or raw.lower() in alias.lower():
            return canonical
    return raw

# ── 確認表功能 ────────────────────────────────
def get_today_rows(sheet=None):
    if sheet is None:
        sheet = get_confirm_sheet()
    today    = datetime.datetime.now(TZ).date()
    date_str = f"{today.month}/{today.day}"
    rows     = sheet.get_all_values()
    results  = []
    for i, row in enumerate(rows):
        if len(row) >= 9 and row[0].strip() == date_str:
            results.append({
                "row_num": i + 1,
                "date": row[0], "slot": row[2],
                "name": row[3], "ep":   row[4],
                "IG_FB": row[5], "TK": row[6], "YT": row[7],
                "status": row[9] if len(row) > 9 else row[8],
            })
    return results

def build_today_msg(rows):
    if not rows:
        return "今天沒有排定的上片節目 🎉"
    today = datetime.datetime.now(TZ).date()
    lines = [f"📋 {today.month}/{today.day} 今日清單\n"]
    cur_slot = ""
    for idx, r in enumerate(rows, 1):
        if r["slot"] != cur_slot:
            cur_slot = r["slot"]
            lines.append(f"\n⏰ {cur_slot}")
        plats = [p for p,v in [("IG/FB",r["IG_FB"]),("TK",r["TK"]),("YT",r["YT"])]
                 if v.strip() not in (S_SKIP,"")]
        icon = "✅" if S_DONE in r["status"] else ("⚠️" if S_ERR in r["status"] else "⏳")
        lines.append(f"{idx}. {icon} {r['name']} {r['ep']} [{' '.join(plats)}]")
    lines += ["\n──────────────────",
              "輸入：節目名 EP號 狀態",
              "例：董律師EP177 已排程",
              "其他：今日 / 狀態 / 全部"]
    return "\n".join(lines)

def find_confirm_rows(sheet, show_name, ep_num):
    all_vals = sheet.get_all_values()
    matched  = []
    ep_str   = f"EP{ep_num}" if ep_num else None
    for i, row in enumerate(all_vals):
        if len(row) < 6: continue
        row_show = str(row[3]).strip()
        row_ep   = str(row[4]).strip()
        show_match = (show_name.lower() in row_show.lower() or
                      row_show.lower() in show_name.lower())
        ep_ok = True
        if ep_str:
            ep_ok = ep_str.upper() in row_ep.upper() or ep_num in row_ep
        if show_match and ep_ok and row_show:
            matched.append((i + 1, row))
    return matched

def update_platforms(sheet, row_num, row_data, new_status):
    updates = []
    for plat, col in [("IG/FB", 6), ("TK", 7), ("YT", 8)]:
        cur = row_data[col - 1] if len(row_data) >= col else ""
        if cur.strip() not in (S_SKIP, ""):
            sheet.update_cell(row_num, col, new_status)
            updates.append(plat)
    return updates

# ── 排程表回寫核心 ────────────────────────────
def write_to_schedule_sheets(show_name, ep_num, date_str=None, action="fill"):
    """
    同步回寫到三張工作表：
    - 確認表：補集數或新增
    - 月排程表（04月排程表）：找對應日期+節目欄位更新
    - 所有IP排程表：找對應日期+節目欄位更新
    action: "fill"=補集數, "add"=新增排程
    """
    wb       = open_workbook()
    new_ep   = f"EP{ep_num}"
    results  = []

    # ── 1. 確認表 ──
    try:
        confirm = get_confirm_sheet(wb)
        confirm_rows = confirm.get_all_values()
        confirm_updated = []
        for i, row in enumerate(confirm_rows):
            if len(row) < 5: continue
            row_show = str(row[3]).strip()
            row_ep   = str(row[4]).strip()
            row_date = str(row[0]).strip()
            show_match = (show_name.lower() in row_show.lower() or
                         row_show.lower() in show_name.lower())
            if not show_match: continue
            if action == "fill":
                # 補集數：只更新沒有數字的 EP 欄
                if re.match(r'^EP\s*$', row_ep, re.IGNORECASE) or row_ep.upper() == "EP":
                    confirm.update_cell(i + 1, 5, new_ep)
                    confirm_updated.append(f"{row_date} {row_show}")
            elif action == "add" and date_str:
                # 新增：找指定日期的欄位
                parts = date_str.split("/")
                target = f"{int(parts[0])}/{int(parts[1])}" if len(parts) == 2 else date_str
                if row_date == target:
                    confirm.update_cell(i + 1, 5, new_ep)
                    confirm_updated.append(f"{row_date} {row_show}")
        if confirm_updated:
            results.append(f"✅ 確認表：{', '.join(confirm_updated)}")
    except Exception as e:
        results.append(f"⚠️ 確認表更新失敗：{e}")

    # ── 2. 月排程表（04月排程表）──
    try:
        month_ws = get_month_schedule_sheet(wb)
        if month_ws:
            # 找節目對應欄
            show_col = None
            for name, col in SHOW_COL_BASE.items():
                if show_name.lower() in name.lower() or name.lower() in show_name.lower():
                    show_col = col
                    break
            if show_col:
                month_rows = month_ws.get_all_values()
                month_updated = []
                for i, row in enumerate(month_rows):
                    if len(row) < 3: continue
                    row_date = row[2] if len(row) > 2 else ""
                    # row[2] is datetime or date string
                    if not row_date: continue
                    # parse date
                    if hasattr(row_date, 'month'):
                        cell_date = f"{row_date.month}/{row_date.day}"
                    else:
                        row_date = str(row_date).strip()
                        dm = re.search(r'(\d+)/(\d+)', row_date)
                        cell_date = f"{int(dm.group(1))}/{int(dm.group(2))}" if dm else row_date

                    if action == "fill":
                        # 補集數：找該節目欄位為空或 EP 無數字
                        if show_col - 1 < len(row):
                            cur = str(row[show_col - 1]).strip()
                            ep_no_num = re.match(r'^(?:.*\s)?EP\s*$', cur) or (show_name[:2] in cur and "EP" in cur and not re.search(r'EP\d', cur))
                            if ep_no_num:
                                # Build new value keeping show prefix
                                prefix = re.sub(r'EP\s*\d*$', '', cur).strip()
                                new_val = f"{prefix} {new_ep}".strip() if prefix else new_ep
                                month_ws.update_cell(i + 1, show_col, new_val)
                                month_updated.append(cell_date)
                    elif action == "add" and date_str:
                        parts = date_str.split("/")
                        target = f"{int(parts[0])}/{int(parts[1])}" if len(parts) == 2 else date_str
                        if cell_date == target:
                            show_prefix = re.sub(r'\s+', '', show_name[:2])
                            new_val = f"{show_prefix} {new_ep}"
                            month_ws.update_cell(i + 1, show_col, new_val)
                            month_updated.append(cell_date)
                if month_updated:
                    month = datetime.datetime.now(TZ).month
                    results.append(f"✅ {month:02d}月排程表：{', '.join(month_updated)}")
    except Exception as e:
        results.append(f"⚠️ 月排程表更新失敗：{e}")

    # ── 3. 所有IP上片排程表 ──
    try:
        allip = get_allip_sheet(wb)
        if allip:
            month    = datetime.datetime.now(TZ).month
            # Determine which col block to use based on current month
            col_map  = SHOW_COL_APR_ALLIP if month == 4 else SHOW_COL_BASE
            show_col = None
            for name, col in col_map.items():
                if show_name.lower() in name.lower() or name.lower() in show_name.lower():
                    show_col = col
                    break
            if show_col:
                # date col is show_col_base-2 for that month block
                date_col = 3 if month == 3 else 15  # col3 for Mar, col15 for Apr
                allip_rows = allip.get_all_values()
                allip_updated = []
                for i, row in enumerate(allip_rows):
                    if len(row) < date_col: continue
                    raw_date = row[date_col - 1]
                    if hasattr(raw_date, 'month'):
                        cell_date = f"{raw_date.month}/{raw_date.day}"
                    else:
                        raw_date = str(raw_date).strip()
                        dm = re.search(r'(\d+)/(\d+)', raw_date)
                        cell_date = f"{int(dm.group(1))}/{int(dm.group(2))}" if dm else ""
                    if not cell_date: continue

                    if action == "fill":
                        if show_col - 1 < len(row):
                            cur = str(row[show_col - 1]).strip()
                            ep_no_num = "EP" in cur and not re.search(r'EP\d', cur)
                            if ep_no_num:
                                prefix = re.sub(r'EP\s*\d*$', '', cur).strip()
                                new_val = f"{prefix} {new_ep}".strip() if prefix else new_ep
                                allip.update_cell(i + 1, show_col, new_val)
                                allip_updated.append(cell_date)
                    elif action == "add" and date_str:
                        parts = date_str.split("/")
                        target = f"{int(parts[0])}/{int(parts[1])}" if len(parts) == 2 else date_str
                        if cell_date == target:
                            show_prefix = re.sub(r'\s+', '', show_name[:2])
                            new_val = f"{show_prefix} {new_ep}"
                            allip.update_cell(i + 1, show_col, new_val)
                            allip_updated.append(cell_date)
                if allip_updated:
                    results.append(f"✅ 所有IP排程表：{', '.join(allip_updated)}")
    except Exception as e:
        results.append(f"⚠️ 所有IP排程表更新失敗：{e}")

    # ── 4. 今日快速確認 ──
    try:
        quick = get_quick_confirm_sheet(wb)
        if quick:
            quick_rows    = quick.get_all_values()
            quick_updated = []
            for i, row in enumerate(quick_rows):
                if len(row) < 3 or i < 4: continue  # skip headers
                row_show = str(row[1]).strip()   # B欄 = 節目
                row_ep   = str(row[2]).strip()   # C欄 = 集數
                show_match = (show_name.lower() in row_show.lower() or
                             row_show.lower() in show_name.lower())
                if not show_match: continue
                if action == "fill":
                    if re.match(r'^EP\s*$', row_ep, re.IGNORECASE) or row_ep.upper() == "EP":
                        quick.update_cell(i + 1, 3, new_ep)
                        quick_updated.append(f"{row[0]} {row_show}")
                elif action == "add" and date_str:
                    # 今日快速確認沒有日期欄，用節目+EP空白判斷
                    if re.match(r'^EP\s*$', row_ep, re.IGNORECASE) or row_ep.upper() == "EP":
                        quick.update_cell(i + 1, 3, new_ep)
                        quick_updated.append(f"{row[0]} {row_show}")
            if quick_updated:
                results.append(f"✅ 今日快速確認：{', '.join(quick_updated)}")
    except Exception as e:
        results.append(f"⚠️ 今日快速確認更新失敗：{e}")

    return results
def delete_ep_from_sheets(show_name, ep_num=None, date_str=None):
    wb      = open_workbook()
    results = []
    target_date = None
    if date_str:
        parts = date_str.split("/")
        if len(parts) == 2:
            try: target_date = f"{int(parts[0])}/{int(parts[1])}"
            except: pass
    ep_str = f"EP{ep_num}" if ep_num else None

    # 確認表
    try:
        confirm      = get_confirm_sheet(wb)
        confirm_rows = confirm.get_all_values()
        confirm_del  = []
        for i, row in enumerate(confirm_rows):
            if len(row) < 5: continue
            row_show = str(row[3]).strip()
            row_ep   = str(row[4]).strip()
            row_date = str(row[0]).strip()
            show_match = (show_name.lower() in row_show.lower() or row_show.lower() in show_name.lower())
            if not show_match: continue
            hit = (ep_str and ep_str.upper() in row_ep.upper()) or \
                  (target_date and row_date == target_date and re.search(r'EP\d', row_ep))
            if hit:
                confirm.update_cell(i + 1, 5, "EP")
                confirm_del.append(f"{row_date} {row_show} {row_ep}")
        if confirm_del:
            results.append(f"🗑 確認表：{', '.join(confirm_del)}")
    except Exception as e:
        results.append(f"⚠️ 確認表：{e}")

    # 月排程表
    try:
        month_ws = get_month_schedule_sheet(wb)
        if month_ws:
            show_col = next((col for name, col in SHOW_COL_BASE.items()
                            if show_name.lower() in name.lower() or name.lower() in show_name.lower()), None)
            if show_col:
                month_del = []
                for i, row in enumerate(month_ws.get_all_values()):
                    if len(row) < show_col: continue
                    rd = row[2] if len(row) > 2 else ""
                    dm = re.search(r'(\d+)/(\d+)', str(rd))
                    cell_date = f"{int(dm.group(1))}/{int(dm.group(2))}" if dm else ""
                    cur = str(row[show_col - 1]).strip()
                    hit = (ep_str and ep_str.upper() in cur.upper()) or \
                          (target_date and cell_date == target_date and re.search(r'EP\d', cur))
                    if hit:
                        prefix  = re.sub(r'\s*EP\d+.*$', '', cur).strip()
                        month_ws.update_cell(i + 1, show_col, f"{prefix} EP".strip())
                        month_del.append(f"{cell_date} {cur}")
                if month_del:
                    month = datetime.datetime.now(TZ).month
                    results.append(f"🗑 {month:02d}月排程表：{', '.join(month_del)}")
    except Exception as e:
        results.append(f"⚠️ 月排程表：{e}")

    # 所有IP排程表
    try:
        allip = get_allip_sheet(wb)
        if allip:
            month    = datetime.datetime.now(TZ).month
            col_map  = SHOW_COL_APR_ALLIP if month == 4 else SHOW_COL_BASE
            show_col = next((col for name, col in col_map.items()
                            if show_name.lower() in name.lower() or name.lower() in show_name.lower()), None)
            if show_col:
                date_col  = 3 if month == 3 else 15
                allip_del = []
                for i, row in enumerate(allip.get_all_values()):
                    if len(row) < date_col or show_col - 1 >= len(row): continue
                    rd = row[date_col - 1]
                    dm = re.search(r'(\d+)/(\d+)', str(rd))
                    cell_date = f"{int(dm.group(1))}/{int(dm.group(2))}" if dm else ""
                    cur = str(row[show_col - 1]).strip()
                    hit = (ep_str and ep_str.upper() in cur.upper()) or \
                          (target_date and cell_date == target_date and re.search(r'EP\d', cur))
                    if hit:
                        prefix = re.sub(r'\s*EP\d+.*$', '', cur).strip()
                        allip.update_cell(i + 1, show_col, f"{prefix} EP".strip())
                        allip_del.append(f"{cell_date} {cur}")
                if allip_del:
                    results.append(f"🗑 所有IP排程表：{', '.join(allip_del)}")
    except Exception as e:
        results.append(f"⚠️ 所有IP排程表：{e}")

    # 今日快速確認
    try:
        quick = get_quick_confirm_sheet(wb)
        if quick:
            quick_del = []
            for i, row in enumerate(quick.get_all_values()):
                if len(row) < 3 or i < 4: continue
                row_show = str(row[1]).strip()
                row_ep   = str(row[2]).strip()
                show_match = (show_name.lower() in row_show.lower() or row_show.lower() in show_name.lower())
                if not show_match: continue
                hit = (ep_str and ep_str.upper() in row_ep.upper()) or \
                      (target_date and re.search(r'EP\d', row_ep))
                if hit:
                    quick.update_cell(i + 1, 3, "EP")
                    quick_del.append(f"{row[0]} {row_show} {row_ep}")
            if quick_del:
                results.append(f"🗑 今日快速確認：{', '.join(quick_del)}")
    except Exception as e:
        results.append(f"⚠️ 今日快速確認：{e}")

    return results

# ── 快取 ──────────────────────────────────────
_cache = {"date": None, "rows": []}

# 對話暫存：記住待確認日期的指令
# 格式：{"show_name": "董律師", "ep_num": "178", "status": "已排程"}
_pending = {}

def cached_rows():
    today = str(datetime.datetime.now(TZ).date())
    if _cache["date"] != today:
        sh = get_confirm_sheet()
        _cache.update({"date": today, "rows": get_today_rows(sh)})
    return _cache["rows"]

def bust():
    _cache["date"] = None

def push_daily():
    try:
        bust()
        send_push(build_today_msg(cached_rows()))
    except Exception as e:
        print(f"[push error] {e}")

sched = BackgroundScheduler(timezone=TZ)
sched.add_job(push_daily, "cron", hour=8, minute=0)
sched.start()

# ── Webhook ───────────────────────────────────
@app.route("/callback", methods=["POST"])
def callback():
    sig  = request.headers.get("X-Line-Signature","")
    body = request.get_data(as_text=True)
    try:
        handler.handle(body, sig)
    except InvalidSignatureError:
        abort(400)
    return "OK"

@handler.add(MessageEvent, message=TextMessageContent)
def on_msg(event):
    text  = event.message.text.strip()
    token = event.reply_token
    rows  = cached_rows()

    # ── 今日清單 ──
    if text in ("今日","清單","今天","list"):
        bust(); send_reply(token, build_today_msg(cached_rows())); return

    # ── 狀態查詢 ──
    if text in ("狀態","status","進度"):
        done = sum(1 for r in rows if S_DONE in r["status"])
        err  = sum(1 for r in rows if S_ERR  in r["status"])
        wait = len(rows) - done - err
        send_reply(token, f"📊 今日進度（共{len(rows)}個）\n✅ 已上片 {done}  ⚠️ 不上片 {err}  ⏳ 待確認 {wait}"); return

    # ── 全部已上片 ──
    if text in ("全部","all"):
        sh = get_confirm_sheet(); count = 0
        for r in rows:
            if S_DONE not in r["status"]:
                update_platforms(sh, r["row_num"], sh.row_values(r["row_num"]), S_DONE)
                count += 1
        bust()
        send_reply(token, f"✅ 今日 {count} 個節目全部標記已上片！"); return

    # ══════════════════════════════════════════
    # 補集數：同步更新三張表
    # 格式：補集數 董律師 EP178
    # ══════════════════════════════════════════
    if re.match(r'^(補集數|補ep|補EP)', text):
        remaining = re.sub(r'^(補集數|補[Ee][Pp])\s*', '', text).strip()
        ep_match  = re.search(r'EP?\s*(\d+)', remaining, re.IGNORECASE)
        ep_num    = ep_match.group(1) if ep_match else None
        show_raw  = re.sub(r'EP?\s*\d+', '', remaining, flags=re.IGNORECASE).strip()
        show_name = normalize_show(show_raw)

        if not show_name or not ep_num:
            send_reply(token, "格式：補集數 節目名 EP號\n例：補集數 董律師 EP178"); return

        send_reply(token, f"⏳ 正在同步更新三張表，請稍候...")
        try:
            results = write_to_schedule_sheets(show_name, ep_num, action="fill")
        except Exception as e:
            send_reply(token, f"更新失敗：{e}"); return

        bust()
        msg = f"✅ 補集數完成 {show_name} EP{ep_num}\n\n" + "\n".join(results) if results else f"找不到 {show_name} 需要補集數的欄位"
        send_reply(token, msg); return

    # ══════════════════════════════════════════
    # 新增排程：同步更新三張表
    # 格式：新增 董律師 EP178 4/10 1500
    # ══════════════════════════════════════════
    if re.match(r'^(新增|更新排程|加排程)', text):
        remaining  = re.sub(r'^(新增|更新排程|加排程)\s*', '', text).strip()
        ep_match   = re.search(r'EP\s*(\d+)', remaining, re.IGNORECASE)
        ep_num     = ep_match.group(1) if ep_match else None
        date_match = re.search(r'(\d{1,2})[/月](\d{1,2})', remaining)
        date_str   = f"{date_match.group(1)}/{date_match.group(2)}" if date_match else None
        show_raw   = re.sub(r'EP\s*\d+', '', remaining, flags=re.IGNORECASE)
        show_raw   = re.sub(r'\d{1,2}[/月]\d{1,2}', '', show_raw)
        show_raw   = re.sub(r'\b(0900|1200|1500|1800|2000|2100)\b', '', show_raw).strip()
        show_name  = normalize_show(show_raw)

        if not show_name or not ep_num or not date_str:
            send_reply(token, "格式：新增 節目名 EP號 日期\n例：新增 董律師 EP178 4/10"); return

        send_reply(token, f"⏳ 正在同步更新三張表，請稍候...")
        try:
            results = write_to_schedule_sheets(show_name, ep_num, date_str=date_str, action="add")
        except Exception as e:
            send_reply(token, f"更新失敗：{e}"); return

        bust()
        msg = f"✅ 新增排程完成 {show_name} EP{ep_num} ({date_str})\n\n" + "\n".join(results) if results else f"找不到 {date_str} {show_name} 的對應欄位"
        send_reply(token, msg); return

    # ══════════════════════════════════════════
    # 確認表狀態更新（原有功能）
    # 例：董律師EP177 已排程
    # ══════════════════════════════════════════
    found_status = None
    found_key    = None
    for key in sorted(STATUS_MAP.keys(), key=len, reverse=True):
        if key in text:
            found_status = STATUS_MAP[key]
            found_key    = key
            break

    # ══════════════════════════════════════════
    # 略過同步
    if text in ("略過", "skip", "不用", "不需要") and USER_ID in _pending:
        pending   = _pending.pop(USER_ID)
        action    = pending.get("action", "status")
        show_name = pending["show_name"]
        ep_num    = pending["ep_num"]
        if action == "delete":
            try:
                results = delete_ep_from_sheets(show_name, ep_num=ep_num)
            except Exception as e:
                send_reply(token, f"刪除失敗：{e}"); return
            bust()
            msg = f"🗑 刪集數完成 {show_name} EP{ep_num}（所有日期）\n\n" + "\n".join(results) if results else f"找不到符合的集數"
            send_reply(token, msg)
        else:
            send_reply(token, "✅ 已略過集數同步，只更新確認表狀態。")
        return

    # 待確認日期回覆
    date_only = re.match(r'^(\d{1,2})[/月](\d{1,2})$', text.strip())
    if date_only and USER_ID in _pending:
        pending   = _pending.pop(USER_ID)
        show_name = pending["show_name"]
        ep_num    = pending["ep_num"]
        action    = pending.get("action", "status")
        date_str  = f"{date_only.group(1)}/{date_only.group(2)}"

        if action == "delete":
            try:
                results = delete_ep_from_sheets(show_name, ep_num=ep_num, date_str=date_str)
            except Exception as e:
                send_reply(token, f"刪除失敗：{e}"); return
            bust()
            msg = f"🗑 刪集數完成 {show_name} EP{ep_num}（{date_str}）\n\n" + "\n".join(results) if results else f"找不到 {show_name} EP{ep_num}（{date_str}）的集數"
            send_reply(token, msg)
        else:
            found_status = pending["status"]
            label = {S_SCHED:"已排程", S_DONE:"✓ 已上片",
                     S_ERR:"⚠ 不上片", S_SKIP:"—未排程"}.get(found_status, found_status)
            try:
                sh      = get_confirm_sheet()
                matched = find_confirm_rows(sh, show_name, ep_num)
                status_results = []
                for row_num, row_data in matched:
                    updated = update_platforms(sh, row_num, row_data, found_status)
                    status_results.append(f"  {row_data[0]} {row_data[4]} [{' '.join(updated)}]")
                sync_results = []
                if ep_num:
                    sync_results = write_to_schedule_sheets(show_name, ep_num, date_str=date_str, action="add")
                bust()
                msg = f"✅ {show_name} EP{ep_num} 狀態 → {label}\n"
                if status_results:
                    msg += "\n".join(status_results)
                if sync_results:
                    msg += f"\n\n📋 集數同步（{date_str}）：\n" + "\n".join(sync_results)
                send_reply(token, msg)
            except Exception as e:
                send_reply(token, f"更新失敗：{e}")
        return

    if found_status:
        remaining = text.replace(found_key, "").strip()
        ep_match  = re.search(r'EP\s*(\d+)', remaining, re.IGNORECASE)
        if not ep_match: ep_match = re.search(r'(\d+)', remaining)
        ep_num    = ep_match.group(1) if ep_match else None
        show_raw  = re.sub(r'EP\s*\d+', '', remaining, flags=re.IGNORECASE).strip()
        show_raw  = re.sub(r'\d+', '', show_raw).strip()
        show_name = normalize_show(show_raw)

        if not show_name:
            send_reply(token, "找不到節目名稱，請輸入如：董律師EP176 已排程"); return
        try:
            sh = get_confirm_sheet()
        except Exception as e:
            send_reply(token, f"連線失敗：{e}"); return

        matched = find_confirm_rows(sh, show_name, ep_num)
        if not matched:
            ep_str = f"EP{ep_num}" if ep_num else "（未指定集數）"
            send_reply(token, f"找不到「{show_name} {ep_str}」\n輸入「今日」查看今日清單"); return

        # 1. 更新確認表平台狀態
        status_results = []
        for row_num, row_data in matched:
            updated = update_platforms(sh, row_num, row_data, found_status)
            status_results.append(f"  {row_data[0]} {row_data[4]} [{' '.join(updated)}]")

        label = {S_SCHED:"已排程", S_DONE:"✓ 已上片",
                 S_ERR:"⚠ 不上片", S_SKIP:"—未排程"}.get(found_status, found_status)

        # 2. 如果有集數，詢問日期後同步四張表
        if ep_num:
            # 儲存待確認狀態
            _pending[USER_ID] = {
                "show_name": show_name,
                "ep_num":    ep_num,
                "status":    found_status,
            }
            bust()
            msg  = f"✅ {show_name} EP{ep_num} 狀態 → {label}\n"
            msg += "\n".join(status_results)
            msg += f"\n\n📅 請問是哪一天的排程？\n輸入日期同步四張表，例：4/10\n（輸入「略過」跳過同步）"
            send_reply(token, msg)
        else:
            bust()
            send_reply(token, f"✅ {show_name} 狀態 → {label}\n" + "\n".join(status_results))
        return

    # ══════════════════════════════════════════
    # 刪集數：同步清空三張表
    # 格式：刪集數 董律師 EP178
    #       刪集數 董律師 4/10
    # ══════════════════════════════════════════
    if re.match(r'^(刪集數|刪除集數|清空集數|刪ep|刪EP)', text):
        remaining  = re.sub(r'^(刪集數|刪除集數|清空集數|刪[Ee][Pp])\s*', '', text).strip()
        ep_match   = re.search(r'EP\s*(\d+)', remaining, re.IGNORECASE)
        ep_num     = ep_match.group(1) if ep_match else None
        date_match = re.search(r'(\d{1,2})[/月](\d{1,2})', remaining)
        date_str   = f"{date_match.group(1)}/{date_match.group(2)}" if date_match else None
        show_raw   = re.sub(r'EP\s*\d+', '', remaining, flags=re.IGNORECASE)
        show_raw   = re.sub(r'\d{1,2}[/月]\d{1,2}', '', show_raw).strip()
        show_name  = normalize_show(show_raw)

        if not show_name or not ep_num:
            send_reply(token,
                "格式：刪集數 節目名 EP號\n"
                "例：刪集數 董律師 EP178"); return

        # 若已有日期直接執行，否則詢問日期
        if date_str:
            send_reply(token, f"⏳ 正在同步清空四張表，請稍候...")
            try:
                results = delete_ep_from_sheets(show_name, ep_num=ep_num, date_str=date_str)
            except Exception as e:
                send_reply(token, f"刪除失敗：{e}"); return
            bust()
            target = f"EP{ep_num}（{date_str}）"
            msg = f"🗑 刪集數完成 {show_name} {target}\n\n" + "\n".join(results) if results else f"找不到 {show_name} {target} 的集數"
            send_reply(token, msg)
        else:
            # 儲存待確認日期
            _pending[USER_ID] = {
                "show_name": show_name,
                "ep_num":    ep_num,
                "action":    "delete",
            }
            send_reply(token,
                f"📅 請問要刪除哪一天的 {show_name} EP{ep_num}？\n"
                f"輸入日期同步四張表，例：4/10\n"
                f"（輸入「略過」刪除所有符合的）")
        return

    # ── 說明 ──
    send_reply(token,
        "📖 指令說明\n"
        "──────────────\n"
        "【確認上片狀態】\n"
        "  董律師EP177 已排程\n"
        "  董律師EP177 已上片\n"
        "  董律師EP177 不上片\n\n"
        "【補集數（同步四張表）】\n"
        "  補集數 董律師 EP178\n\n"
        "【刪集數（同步四張表）】\n"
        "  刪集數 董律師 EP178\n"
        "  刪集數 董律師 4/10\n\n"
        "【新增/更新排程（同步四張表）】\n"
        "  新增 董律師 EP178 4/10\n\n"
        "【其他】\n"
        "  今日　查看今日清單\n"
        "  全部　今日全部標記已上片\n"
        "  狀態　查看今日進度"
    )

@app.route("/")
def index():
    return "Bot running ✓"

if __name__ == "__main__":
    app.run(port=int(os.environ.get("PORT", 5000)))
# patch marker

