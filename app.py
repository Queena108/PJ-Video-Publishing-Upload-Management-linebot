import os, json, datetime, re, calendar
from collections import defaultdict
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
    "真心話長": "真心話長", "真心話短": "真心話短",
    "芯芯": "芯芯",
    "而璽": "而璽", "而璽設計": "而璽",
    "今晚": "今晚", "今晚長": "今晚長", "今晚短": "今晚短",
}

# ── 節目完整元資料（平台、時段、LINE 縮寫）──
SHOW_META = {
    "董律師": {"slot": "1500", "prefix": "董律",    "platforms": ["FB", "IG", "TK"]},
    "蟎人":   {"slot": "1800", "prefix": "蟎人",    "platforms": ["FB", "IG", "TK"]},
    "AIDA":   {"slot": "2000", "prefix": "AIDA",   "platforms": ["FB", "IG", "TK"]},
    "MICO":   {"slot": "2000", "prefix": "MICO",   "platforms": ["IG", "FB", "YT", "TK"]},
    "真心話長": {"slot": "2100", "prefix": "真心話長", "platforms": ["YT", "IG", "限動"]},
    "真心話短": {"slot": "2100", "prefix": "真心話短", "platforms": ["FB", "IG", "YT", "TK", "限動"]},
    "芯芯":   {"slot": "2100", "prefix": "芯芯",    "platforms": ["IG", "FB", "TK", "YT"]},
    "而璽":   {"slot": "2100", "prefix": "而璽",    "platforms": ["IG", "FB", "TK"]},
    "今晚":   {"slot": "2100", "prefix": "今晚",    "platforms": ["IG", "FB", "YT", "TK"]},
}

WEEKDAY_ZH = ["(一)", "(二)", "(三)", "(四)", "(五)", "(六)", "(日)"]

# ── 歧義節目清單 ──
AMBIGUOUS_SHOWS = {
    "真心話": ["真心話短", "真心話長"],
    "今晚":   ["今晚短",  "今晚長"],
}

# ════════════════════════════════════════════
#  動態月份索引（核心：從表頭動態解析欄位）
# ════════════════════════════════════════════
_month_index_cache = {"ts": None, "data": {}}

def build_allip_month_index(rows):
    """
    掃描 所有IP上片排程表，動態建立月份→欄位對應：
    {
      month_int: {
        "weekday_col": int,  # gspread 1-indexed 星期欄
        "date_col":    int,  # gspread 1-indexed 日期欄
        "show_cols":  {show_name: gspread_col_int}
      }
    }
    從此不再 hardcode month==4 或固定 offset。
    """
    result = {}
    if len(rows) < 4:
        return result

    row1 = rows[1]  # 月份起始日期行
    row2 = rows[2]  # 節目標頭行（主）
    row3 = rows[3]  # 節目標頭行（副，真心話短）

    # Step 1：找各月 block 的星期欄（0-indexed）
    month_starts = {}  # month_num → weekday_col (0-indexed)
    for ci, val in enumerate(row1):
        s = str(val).strip()
        if not s or s in ("nan", "NaT", "None", ""):
            continue
        m = re.search(r'\d{4}-(\d{2})-\d{2}', s)
        if m:
            month_num = int(m.group(1))
            month_starts[month_num] = ci  # 0-indexed Python list position

    for month_num, wday_ci in month_starts.items():
        result[month_num] = {
            "weekday_col": wday_ci + 1,   # gspread 1-indexed
            "date_col":    wday_ci + 2,   # gspread 1-indexed
            "show_cols":   {}
        }

    # Step 2：掃描 row2 + row3 找節目標頭
    # 「屬於哪個月」= 最後一個 weekday_col < 當前欄 的月份
    sorted_months = sorted(month_starts.items(), key=lambda x: x[1])

    def owning_month(ci):
        best = None
        for mn, wday_ci in sorted_months:
            if ci > wday_ci:
                best = mn
        return best

    for row_data in [row2, row3]:
        for ci, val in enumerate(row_data):
            s = str(val).strip()
            if not s or s in ("nan", "NaT", "None", ""):
                continue
            for show_name in SHOW_META.keys():
                if show_name in s:
                    mn = owning_month(ci)
                    if mn is not None:
                        # 同一 show+month 只記錄一次（row2 優先）
                        if show_name not in result[mn]["show_cols"]:
                            result[mn]["show_cols"][show_name] = ci + 1  # gspread 1-indexed
                    break

    return result


def get_month_index(allip_rows=None):
    """快取版本，30 分鐘內不重新掃描"""
    now = datetime.datetime.now(TZ)
    cached_ts = _month_index_cache.get("ts")
    if cached_ts is None or (now - cached_ts).seconds > 1800:
        if allip_rows is None:
            allip = get_allip_sheet()
            allip_rows = allip.get_all_values()
        _month_index_cache["data"] = build_allip_month_index(allip_rows)
        _month_index_cache["ts"]   = now
    return _month_index_cache["data"]


def bust_month_index():
    _month_index_cache["ts"] = None


def get_show_col_for_month(month_num, show_name, month_index):
    """回傳 gspread 1-indexed 欄號；找不到回傳 None"""
    block = month_index.get(month_num, {})
    show_cols = block.get("show_cols", {})
    for name, col in show_cols.items():
        if show_name.lower() in name.lower() or name.lower() in show_name.lower():
            return col
    return None


def get_date_col_for_month(month_num, month_index):
    """回傳 gspread 1-indexed 日期欄號；找不到回傳 None"""
    block = month_index.get(month_num, {})
    return block.get("date_col")


# ════════════════════════════════════════════
#  工具函式
# ════════════════════════════════════════════
def parse_date_str(raw):
    """多格式 → M/D 字串；無法解析回傳 None"""
    raw = raw.strip()
    m = re.match(r'^(\d{1,2})[/\-](\d{1,2})$', raw)
    if m:
        return f"{int(m.group(1))}/{int(m.group(2))}"
    m = re.match(r'^(\d{1,2})月(\d{1,2})日?$', raw)
    if m:
        return f"{int(m.group(1))}/{int(m.group(2))}"
    return None


def parse_cell_date(val):
    """datetime 物件或字串 → M/D 格式"""
    if val is None: return ""
    if hasattr(val, 'month'):
        return f"{val.month}/{val.day}"
    s = str(val).strip()
    dm = re.search(r'(\d{1,2})/(\d{1,2})', s)
    return f"{int(dm.group(1))}/{int(dm.group(2))}" if dm else ""


S_SCHED = "已排程"; S_DONE = "✓ 已上片"
S_ERR   = "⚠ 不上片"; S_SKIP = "—未排程"


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
    month = datetime.datetime.now(TZ).month
    wb    = wb or open_workbook()
    try:
        return wb.worksheet(f"{month:02d}月排程表")
    except:
        return None

def get_allip_sheet(wb=None):
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

def get_ambiguous_candidates(raw):
    raw = raw.strip()
    for key, candidates in AMBIGUOUS_SHOWS.items():
        if raw == key:
            return candidates
    return None


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
              "其他：今日 / 狀態 / 全部 / 查詢"]
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

def update_platforms(sheet, row_num, row_data, new_status, show_name=None):
    """
    更新確認表的平台欄位。
    優先用 SHOW_META 判斷該節目實際使用哪些平台，
    避免因為欄位當前值是 —未排程 而被誤跳過。
    同步更新 限動（col 9）和 全部完成?（col 10）。
    """
    updates = []
    # (平台顯示名, gspread欄號, SHOW_META keys)
    plat_map = [
        ("IG/FB", 6, ["IG", "FB"]),
        ("TK",    7, ["TK"]),
        ("YT",    8, ["YT"]),
        ("限動",  9, ["限動"]),
    ]

    for plat, col, keys in plat_map:
        if show_name and show_name in SHOW_META:
            # 用 SHOW_META 判斷：只更新節目實際有的平台
            applies = any(k in SHOW_META[show_name]["platforms"] for k in keys)
        else:
            # fallback：欄位有值（含 —未排程）就更新，空白才跳過
            cur = row_data[col - 1] if len(row_data) >= col else ""
            applies = cur.strip() != ""

        if applies:
            sheet.update_cell(row_num, col, new_status)
            updates.append(plat)

    # 「全部完成?」欄（col 10）：已排程 或 已上片 才更新，不上片/未排程 不動
    if updates and new_status in (S_SCHED, S_DONE):
        sheet.update_cell(row_num, 10, new_status)

    return updates


# ════════════════════════════════════════════
#  排程表回寫核心（使用動態欄位索引）
# ════════════════════════════════════════════
def write_to_schedule_sheets(show_name, ep_num, date_str=None, action="fill"):
    wb      = open_workbook()
    new_ep  = f"EP{ep_num}"
    results = []
    month   = datetime.datetime.now(TZ).month

    # ── 1. 確認表 ──
    try:
        confirm      = get_confirm_sheet(wb)
        confirm_rows = confirm.get_all_values()
        confirm_upd  = []
        for i, row in enumerate(confirm_rows):
            if len(row) < 5: continue
            row_show = str(row[3]).strip()
            row_ep   = str(row[4]).strip()
            row_date = str(row[0]).strip()
            show_match = (show_name.lower() in row_show.lower() or
                         row_show.lower() in show_name.lower())
            if not show_match: continue
            if action == "fill":
                if re.match(r'^EP\s*$', row_ep, re.IGNORECASE) or row_ep.upper() == "EP":
                    confirm.update_cell(i + 1, 5, new_ep)
                    confirm_upd.append(f"{row_date} {row_show}")
            elif action == "add" and date_str:
                if row_date == date_str:
                    confirm.update_cell(i + 1, 5, new_ep)
                    confirm_upd.append(f"{row_date} {row_show}")
        if confirm_upd:
            results.append(f"✅ 確認表：{', '.join(confirm_upd)}")
    except Exception as e:
        results.append(f"⚠️ 確認表更新失敗：{e}")

    # ── 2. 月排程表 ──
    try:
        month_ws = get_month_schedule_sheet(wb)
        if month_ws:
            show_col    = None
            month_index = get_month_index()
            # 月排程表的節目欄：直接比對標頭
            headers = month_ws.row_values(2)
            for ci, hdr in enumerate(headers):
                if show_name.lower() in hdr.lower() or (
                    normalize_show(hdr).lower() == show_name.lower()
                ):
                    show_col = ci + 1  # gspread 1-indexed
                    break
            if show_col is None:
                # fallback: 用節目別名比對
                for ci, hdr in enumerate(headers):
                    n = normalize_show(hdr)
                    if n.lower() == show_name.lower():
                        show_col = ci + 1
                        break
            if show_col:
                month_rows = month_ws.get_all_values()
                month_upd  = []
                for i, row in enumerate(month_rows):
                    if len(row) < 3: continue
                    cell_date = parse_cell_date(row[1] if len(row) > 1 else None)
                    if not cell_date: continue
                    if action == "fill":
                        if show_col - 1 < len(row):
                            cur = str(row[show_col - 1]).strip()
                            if cur and "EP" in cur and not re.search(r'EP\d', cur):
                                prefix  = re.sub(r'EP\s*\d*$', '', cur).strip()
                                new_val = f"{prefix} {new_ep}".strip()
                                month_ws.update_cell(i + 1, show_col, new_val)
                                month_upd.append(f"{cell_date} → {new_val}")
                    elif action == "add" and date_str:
                        if cell_date == date_str:
                            prefix  = SHOW_META.get(show_name, {}).get("prefix", show_name)
                            new_val = f"{prefix} {new_ep}"
                            month_ws.update_cell(i + 1, show_col, new_val)
                            month_upd.append(f"{cell_date} → {new_val}")
                if month_upd:
                    results.append(f"✅ {month:02d}月排程表：{', '.join(month_upd)}")
    except Exception as e:
        results.append(f"⚠️ 月排程表更新失敗：{e}")

    # ── 3. 所有IP上片排程表（動態欄位）──
    try:
        allip       = get_allip_sheet(wb)
        if allip:
            allip_rows  = allip.get_all_values()
            month_index = get_month_index(allip_rows)
            show_col    = get_show_col_for_month(month, show_name, month_index)
            date_col    = get_date_col_for_month(month, month_index)
            if show_col and date_col:
                allip_upd = []
                for i, row in enumerate(allip_rows):
                    if len(row) < date_col: continue
                    cell_date = parse_cell_date(row[date_col - 1])
                    if not cell_date: continue
                    if action == "fill":
                        if show_col - 1 < len(row):
                            cur = str(row[show_col - 1]).strip()
                            if cur and "EP" in cur and not re.search(r'EP\d', cur):
                                prefix  = re.sub(r'EP\s*\d*$', '', cur).strip()
                                new_val = f"{prefix} {new_ep}".strip()
                                allip.update_cell(i + 1, show_col, new_val)
                                allip_upd.append(cell_date)
                    elif action == "add" and date_str:
                        if cell_date == date_str:
                            prefix  = SHOW_META.get(show_name, {}).get("prefix", show_name[:2])
                            new_val = f"{prefix} {new_ep}"
                            allip.update_cell(i + 1, show_col, new_val)
                            allip_upd.append(cell_date)
                if allip_upd:
                    results.append(f"✅ 所有IP排程表：{', '.join(allip_upd)}")
            elif show_col is None:
                results.append(f"⚠️ 所有IP排程表：找不到 {month}月 {show_name} 的欄位（表頭尚未新增？）")
    except Exception as e:
        results.append(f"⚠️ 所有IP排程表更新失敗：{e}")

    return results


def delete_ep_from_sheets(show_name, ep_num=None, date_str=None):
    wb      = open_workbook()
    results = []
    month   = datetime.datetime.now(TZ).month
    ep_str  = f"EP{ep_num}" if ep_num else None

    def is_hit(cur_ep, cur_date):
        has_ep_num = bool(re.search(r'EP\d', str(cur_ep)))
        if ep_str and date_str:
            return ep_str.upper() in str(cur_ep).upper() and cur_date == date_str
        elif ep_str:
            return ep_str.upper() in str(cur_ep).upper()
        elif date_str:
            return cur_date == date_str and has_ep_num
        return False

    # ── 確認表 ──
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
            if is_hit(row_ep, row_date):
                confirm.update_cell(i + 1, 5, "EP")
                confirm_del.append(f"{row_date} {row_show} {row_ep}")
        if confirm_del:
            results.append(f"🗑️ 確認表：{', '.join(confirm_del)}")
    except Exception as e:
        results.append(f"⚠️ 確認表：{e}")

    # ── 月排程表 ──
    try:
        month_ws = get_month_schedule_sheet(wb)
        if month_ws:
            headers  = month_ws.row_values(2)
            show_col = None
            for ci, hdr in enumerate(headers):
                if show_name.lower() in hdr.lower() or normalize_show(hdr).lower() == show_name.lower():
                    show_col = ci + 1
                    break
            if show_col:
                month_del = []
                for i, row in enumerate(month_ws.get_all_values()):
                    if len(row) < show_col: continue
                    cell_date = parse_cell_date(row[1] if len(row) > 1 else None)
                    if not cell_date: continue
                    cur = str(row[show_col - 1]).strip() if row[show_col - 1] else ""
                    if not cur: continue
                    if is_hit(cur, cell_date):
                        prefix = re.sub(r'\s*EP\d+.*$', '', cur).strip()
                        month_ws.update_cell(i + 1, show_col, f"{prefix} EP".strip())
                        month_del.append(f"{cell_date} {cur}")
                if month_del:
                    results.append(f"🗑️ {month:02d}月排程表：{', '.join(month_del)}")
    except Exception as e:
        results.append(f"⚠️ 月排程表：{e}")

    # ── 所有IP排程表（動態欄位）──
    try:
        allip = get_allip_sheet(wb)
        if allip:
            allip_rows  = allip.get_all_values()
            month_index = get_month_index(allip_rows)
            show_col    = get_show_col_for_month(month, show_name, month_index)
            date_col    = get_date_col_for_month(month, month_index)
            if show_col and date_col:
                allip_del = []
                for i, row in enumerate(allip_rows):
                    if len(row) < date_col or show_col - 1 >= len(row): continue
                    cell_date = parse_cell_date(row[date_col - 1])
                    if not cell_date: continue
                    cur = str(row[show_col - 1]).strip() if row[show_col - 1] else ""
                    if not cur: continue
                    if is_hit(cur, cell_date):
                        prefix = re.sub(r'\s*EP\d+.*$', '', cur).strip()
                        allip.update_cell(i + 1, show_col, f"{prefix} EP".strip())
                        allip_del.append(f"{cell_date} {cur}")
                if allip_del:
                    results.append(f"🗑️ 所有IP排程表：{', '.join(allip_del)}")
    except Exception as e:
        results.append(f"⚠️ 所有IP排程表：{e}")

    return results


# ════════════════════════════════════════════
#  自動建立月份表單（核心新功能）
#  從所有IP上片排程表讀取指定月份，
#  自動生成 XX月排程表 + XX月確認表
# ════════════════════════════════════════════
def _build_line_format(date_str, slot, show_name, ep_value, status):
    """組合 LINE 回報格式字串"""
    parts    = date_str.split("/")
    date_fmt = f"{int(parts[0]):02d}/{int(parts[1]):02d}"
    platforms = SHOW_META.get(show_name, {}).get("platforms", [])
    plat_str  = " ".join(f"✓{p}" for p in platforms)
    return f"[{date_fmt} {slot}] {show_name} {ep_value} {plat_str} {status}"


def create_month_sheets(month_num, force=False):
    """
    讀取 所有IP上片排程表 中指定月份的資料，
    自動建立 XX月排程表 和 XX月確認表。
    force=True 時覆蓋已存在的工作表。
    """
    wb = open_workbook()

    # ── Step 1：讀取 所有IP表 並建立動態索引 ──
    allip      = get_allip_sheet(wb)
    allip_rows = allip.get_all_values()
    bust_month_index()  # 強制重建快取
    month_index = get_month_index(allip_rows)

    if month_num not in month_index:
        available = sorted(month_index.keys())
        return False, (
            f"⚠️ 所有IP上片排程表 裡找不到 {month_num}月 的資料。\n"
            f"目前有資料的月份：{available}\n"
            f"請先在 所有IP排程表 新增 {month_num}月 的欄位後再試。"
        )

    block    = month_index[month_num]
    date_col = block["date_col"]      # gspread 1-indexed
    shows    = block["show_cols"]     # {show_name: gspread_1idx_col}

    if not shows:
        return False, f"⚠️ {month_num}月 的節目欄位為空，請確認所有IP排程表的表頭是否正確。"

    # ── Step 2：從 所有IP表 抽取該月所有排程資料 ──
    entries = []   # [{date, weekday, show, ep}]
    for row_idx, row in enumerate(allip_rows):
        if row_idx < 6:  # 跳過標頭行
            continue
        if len(row) < date_col:
            continue
        date_raw  = row[date_col - 1]
        cell_date = parse_cell_date(date_raw)
        if not cell_date:
            continue
        # 只保留當月的資料
        try:
            mo = int(cell_date.split("/")[0])
        except:
            continue
        if mo != month_num:
            continue

        weekday = row[block["weekday_col"] - 1] if len(row) >= block["weekday_col"] else ""
        for show_name, show_col in shows.items():
            if show_col - 1 < len(row) and row[show_col - 1]:
                ep_val = str(row[show_col - 1]).strip()
                if ep_val:
                    entries.append({
                        "date":    cell_date,
                        "weekday": weekday,
                        "show":    show_name,
                        "ep":      ep_val,
                    })

    if not entries:
        return False, f"⚠️ 所有IP排程表 中 {month_num}月 沒有任何排程資料。"

    results     = []
    year        = 2026  # 可擴展為動態取年份
    sched_name  = f"{month_num:02d}月排程表"
    confirm_name= f"{month_num:02d}月確認表"

    # 已存在的工作表
    existing_sheets = [ws.title for ws in wb.worksheets()]
    if not force:
        conflicts = [n for n in [sched_name, confirm_name] if n in existing_sheets]
        if conflicts:
            return False, (
                f"⚠️ 以下表單已存在：{', '.join(conflicts)}\n"
                f"若要覆蓋請輸入：建立{month_num}月表單 覆蓋"
            )

    # ── Step 3：建立 XX月排程表 ──
    try:
        if sched_name in existing_sheets:
            wb.del_worksheet(wb.worksheet(sched_name))

        ws_sched = wb.add_worksheet(title=sched_name, rows=50, cols=12)

        # 節目欄位按時段排序
        show_order = sorted(shows.keys(),
                            key=lambda s: (SHOW_META.get(s, {}).get("slot", "9999"), s))

        # 組合欄位標頭
        col_headers = ["星期", "日期"]
        for show in show_order:
            meta   = SHOW_META.get(show, {})
            slot   = meta.get("slot", "")
            plats  = "  ".join(meta.get("platforms", []))
            col_headers.append(f"{slot} {show}\n({plats})")

        ws_sched.update("A1", [[f"{year}年 {month_num:02d}月 上片排程表"]])
        ws_sched.update("A2", [col_headers])

        # 依日期聚合資料
        date_data = {}
        for e in entries:
            d = e["date"]
            if d not in date_data:
                date_data[d] = {"weekday": e["weekday"], "shows": {}}
            date_data[d]["shows"][e["show"]] = e["ep"]

        # 填入當月每一天（含空白天）
        first_day = datetime.date(year, month_num, 1)
        last_day  = datetime.date(year, month_num,
                                  calendar.monthrange(year, month_num)[1])
        data_rows = []
        cur = first_day
        while cur <= last_day:
            ds  = f"{cur.month}/{cur.day}"
            wd  = WEEKDAY_ZH[cur.weekday()]
            row_vals = [wd, ds]
            if ds in date_data:
                for show in show_order:
                    row_vals.append(date_data[ds]["shows"].get(show, ""))
            else:
                row_vals += [""] * len(show_order)
            data_rows.append(row_vals)
            cur += datetime.timedelta(days=1)

        ws_sched.update("A3", data_rows)
        filled = sum(1 for r in data_rows if any(v for v in r[2:]))
        results.append(f"✅ {sched_name}：建立完成（{filled} 天有排程）")
    except Exception as e:
        results.append(f"⚠️ {sched_name} 建立失敗：{e}")

    # ── Step 4：建立 XX月確認表 ──
    try:
        if confirm_name in existing_sheets:
            wb.del_worksheet(wb.worksheet(confirm_name))

        ws_confirm = wb.add_worksheet(title=confirm_name, rows=100, cols=12)
        ws_confirm.update("A1", [[f"{year}年 {month_num:02d}月 上片確認表"]])
        ws_confirm.update("A2", [["日期", "星期", "時段", "節目", "影片集數",
                                   "IG/FB", "TK", "YT", "限動", "全部完成?",
                                   "LINE 回報格式"]])

        # 依日期分組，並按時段排序
        date_entries = defaultdict(list)
        for e in entries:
            date_entries[e["date"]].append(e)

        sorted_dates = sorted(
            date_entries.keys(),
            key=lambda x: (int(x.split("/")[0]), int(x.split("/")[1]))
        )

        confirm_data = []
        for ds in sorted_dates:
            day_entries = date_entries[ds]
            mo, dy = int(ds.split("/")[0]), int(ds.split("/")[1])
            date_obj = datetime.date(year, mo, dy)
            wd       = WEEKDAY_ZH[date_obj.weekday()]
            wd_char  = wd.strip("()")
            # 日期標頭行（合併提示用）
            confirm_data.append([f"  {year}/{mo:02d}/{dy:02d}（{wd_char}）",
                                  "", "", "", "", "", "", "", "", "", ""])

            # 依時段排序
            day_entries.sort(key=lambda e: (SHOW_META.get(e["show"], {}).get("slot", "9999"),
                                            e["show"]))

            for e in day_entries:
                show_name = e["show"]
                ep_val    = e["ep"]
                meta      = SHOW_META.get(show_name, {})
                slot      = meta.get("slot", "")
                platforms = meta.get("platforms", [])

                has_num     = bool(re.search(r'EP\d', ep_val))
                init_status = "已排程" if has_num else "—未排程"

                # 各平台初始狀態
                has = lambda p: p in platforms
                ig_fb = init_status if has("IG") or has("FB") else "—未排程"
                tk    = init_status if has("TK")              else "—未排程"
                yt    = init_status if has("YT")              else "—未排程"
                xian  = init_status if has("限動")            else "—未排程"

                all_done  = init_status
                line_fmt  = _build_line_format(ds, slot, show_name, ep_val, init_status)

                confirm_data.append([
                    ds, wd, slot, show_name, ep_val,
                    ig_fb, tk, yt, xian, all_done, line_fmt
                ])

        ws_confirm.update("A3", confirm_data)
        n_rows = sum(1 for r in confirm_data if r[3])  # 有節目名稱的行
        results.append(f"✅ {confirm_name}：建立完成（{n_rows} 筆排程）")
    except Exception as e:
        results.append(f"⚠️ {confirm_name} 建立失敗：{e}")

    bust_month_index()
    summary = f"🗓 {month_num}月表單建立完成！\n\n" + "\n".join(results)
    return True, summary


# ── 快取 ──────────────────────────────────────
_cache   = {"date": None, "rows": []}
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


# ════════════════════════════════════════════
#  Webhook
# ════════════════════════════════════════════
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
                update_platforms(sh, r["row_num"], sh.row_values(r["row_num"]), S_DONE, show_name=r["name"])
                count += 1
        bust()
        send_reply(token, f"✅ 今日 {count} 個節目全部標記已上片！"); return

    # ── 取消 pending ──
    if text in ("取消", "cancel", "退出"):
        if USER_ID in _pending:
            op = _pending.pop(USER_ID)
            send_reply(token, f"✅ 已取消操作（{op.get('show_name','')}）")
        else:
            send_reply(token, "目前沒有待確認的操作")
        return

    # ══════════════════════════════════════════
    # 建立月份表單（新功能）
    # 格式：建立5月表單  /  建立05月表單  /  建立5月表單 覆蓋
    # ══════════════════════════════════════════
    create_m = re.match(r'^建立\s*(\d{1,2})\s*月表單(.*)$', text)
    if create_m:
        month_num = int(create_m.group(1))
        force     = "覆蓋" in create_m.group(2)
        if month_num < 1 or month_num > 12:
            send_reply(token, "月份輸入錯誤，請輸入 1-12"); return
        send_reply(token, f"⏳ 正在從 所有IP上片排程表 讀取 {month_num}月 資料，建立表單中，請稍候...")
        try:
            ok, msg = create_month_sheets(month_num, force=force)
        except Exception as e:
            send_reply(token, f"建立失敗：{e}"); return
        bust()
        send_reply(token, msg); return

    # ══════════════════════════════════════════
    # 補集數
    # ══════════════════════════════════════════
    if re.match(r'^(補集數|補ep|補EP)', text):
        remaining = re.sub(r'^(補集數|補[Ee][Pp])\s*', '', text).strip()
        ep_match  = re.search(r'EP?\s*(\d+)', remaining, re.IGNORECASE)
        ep_num    = ep_match.group(1) if ep_match else None
        show_raw  = re.sub(r'EP?\s*\d+', '', remaining, flags=re.IGNORECASE).strip()
        show_name = normalize_show(show_raw)
        if not show_name or not ep_num:
            send_reply(token, "格式：補集數 節目名 EP號\n例：補集數 董律師 EP178"); return
        send_reply(token, f"⏳ 正在同步更新，請稍候...")
        try:
            results = write_to_schedule_sheets(show_name, ep_num, action="fill")
        except Exception as e:
            send_reply(token, f"更新失敗：{e}"); return
        bust()
        msg = f"✅ 補集數完成 {show_name} EP{ep_num}\n\n" + "\n".join(results) if results else f"找不到 {show_name} 需要補集數的欄位"
        send_reply(token, msg); return

    # ══════════════════════════════════════════
    # 新增排程（兩步流程）
    # ══════════════════════════════════════════
    if re.match(r'^(新增|更新排程|加排程)', text):
        remaining  = re.sub(r'^(新增|更新排程|加排程)\s*', '', text).strip()
        ep_match   = re.search(r'EP\s*(\d+)', remaining, re.IGNORECASE)
        ep_num     = ep_match.group(1) if ep_match else None
        date_match = re.search(r'(\d{1,2})[/\-月](\d{1,2})日?', remaining)
        date_str   = f"{int(date_match.group(1))}/{int(date_match.group(2))}" if date_match else None
        show_raw   = re.sub(r'EP\s*\d+', '', remaining, flags=re.IGNORECASE)
        show_raw   = re.sub(r'\d{1,2}[/\-月]\d{1,2}日?', '', show_raw)
        show_raw   = re.sub(r'\b(0900|1200|1500|1800|2000|2100)\b', '', show_raw).strip()
        show_name  = normalize_show(show_raw)
        if not show_name or not ep_num:
            send_reply(token, "格式：新增 節目名 EP號\n例：新增 董律師 EP178"); return
        if not date_str:
            _pending[USER_ID] = {"action": "add", "show_name": show_name, "ep_num": ep_num}
            send_reply(token,
                f"📅 {show_name} EP{ep_num} 要新增到哪一天？\n"
                f"請輸入日期，例：4/10 或 4月10日\n（輸入「取消」放棄）")
            return
        send_reply(token, f"⏳ 正在同步更新三張表，請稍候...")
        try:
            results = write_to_schedule_sheets(show_name, ep_num, date_str=date_str, action="add")
        except Exception as e:
            send_reply(token, f"更新失敗：{e}"); return
        bust()
        msg = f"✅ 新增排程完成 {show_name} EP{ep_num}（{date_str}）\n\n" + "\n".join(results) if results else f"找不到 {date_str} {show_name} 的對應欄位"
        send_reply(token, msg); return

    # ── 略過 ──
    if text in ("略過", "skip", "不用") and USER_ID in _pending:
        pending   = _pending.pop(USER_ID)
        action    = pending.get("action", "status")
        show_name = pending["show_name"]
        ep_num    = pending.get("ep_num")
        if action == "delete":
            try:
                results = delete_ep_from_sheets(show_name, ep_num=ep_num)
            except Exception as e:
                send_reply(token, f"刪除失敗：{e}"); return
            bust()
            msg = f"🗑️ 刪集數完成 {show_name} EP{ep_num}（所有日期）\n\n" + "\n".join(results) if results else "找不到符合的集數"
            send_reply(token, msg)
        else:
            send_reply(token, "✅ 已略過集數同步。")
        return

    # ── 日期回覆 ──
    date_raw = re.match(r'^(\d{1,2})[/\-月](\d{1,2})日?$', text.strip())
    if date_raw and USER_ID in _pending:
        pending   = _pending.pop(USER_ID)
        show_name = pending["show_name"]
        ep_num    = pending.get("ep_num")
        action    = pending.get("action", "status")
        date_str  = f"{int(date_raw.group(1))}/{int(date_raw.group(2))}"

        if action == "delete":
            try:
                results = delete_ep_from_sheets(show_name, ep_num=ep_num, date_str=date_str)
            except Exception as e:
                send_reply(token, f"刪除失敗：{e}"); return
            bust()
            msg = f"🗑️ {show_name} EP{ep_num}（{date_str}）\n\n" + "\n".join(results) if results else f"找不到對應資料"
            send_reply(token, msg)

        elif action == "add":
            send_reply(token, f"⏳ 正在同步更新三張表，請稍候...")
            try:
                results = write_to_schedule_sheets(show_name, ep_num, date_str=date_str, action="add")
            except Exception as e:
                send_reply(token, f"更新失敗：{e}"); return
            bust()
            msg = f"✅ 新增排程完成 {show_name} EP{ep_num}（{date_str}）\n\n" + "\n".join(results) if results else f"找不到 {date_str} {show_name} 的對應欄位"
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
                    updated = update_platforms(sh, row_num, row_data, found_status, show_name=show_name)
                bust()
                msg = f"✅ {show_name} EP{ep_num} 狀態 → {label}\n"
                if status_results: msg += "\n".join(status_results)
                if sync_results:   msg += f"\n\n📋 集數同步（{date_str}）：\n" + "\n".join(sync_results)
                send_reply(token, msg)
            except Exception as e:
                send_reply(token, f"更新失敗：{e}")
        return

    # ── EP 號回覆 ──
    ep_only = re.match(r'^[Ee][Pp]\s*(\d+)$', text.strip())
    if ep_only and USER_ID in _pending:
        pending = _pending.get(USER_ID, {})
        if pending.get("action") == "date_show_ask_ep":
            _pending.pop(USER_ID)
            ep_num    = ep_only.group(1)
            show_name = pending["show_name"]
            date_str  = pending["date_str"]
            send_reply(token, f"⏳ 正在同步更新三張表，請稍候...")
            try:
                results = write_to_schedule_sheets(show_name, ep_num, date_str=date_str, action="add")
            except Exception as e:
                send_reply(token, f"更新失敗：{e}"); return
            bust()
            msg = f"✅ 新增排程完成 {show_name} EP{ep_num}（{date_str}）\n\n" + "\n".join(results) if results else f"找不到 {date_str} {show_name} 的對應欄位"
            send_reply(token, msg); return

    # ── 狀態更新 ──
    found_status = None
    found_key    = None
    for key in sorted(STATUS_MAP.keys(), key=len, reverse=True):
        if key in text:
            found_status = STATUS_MAP[key]
            found_key    = key
            break

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
        status_results = []
        for row_num, row_data in matched:
            updated = update_platforms(sh, row_num, row_data, found_status, show_name=show_name)
            status_results.append(f"  {row_data[0]} {row_data[4]} [{' '.join(updated)}]")
        label = {S_SCHED:"已排程", S_DONE:"✓ 已上片",
                 S_ERR:"⚠ 不上片", S_SKIP:"—未排程"}.get(found_status, found_status)
        if ep_num:
            _pending[USER_ID] = {"show_name": show_name, "ep_num": ep_num, "status": found_status}
            bust()
            msg  = f"✅ {show_name} EP{ep_num} 狀態 → {label}\n" + "\n".join(status_results)
            msg += f"\n\n📅 請問是哪一天的排程？\n輸入日期同步，例：4/10\n（輸入「略過」跳過 / 「取消」放棄）"
            send_reply(token, msg)
        else:
            bust()
            send_reply(token, f"✅ {show_name} 狀態 → {label}\n" + "\n".join(status_results))
        return

    # ── 刪除排程 ──
    if re.match(r'^(刪集數|刪除集數|清空集數|刪ep|刪EP|刪除)', text):
        remaining  = re.sub(r'^(刪集數|刪除集數|清空集數|刪[Ee][Pp]|刪除)\s*', '', text).strip()
        ep_match   = re.search(r'EP\s*(\d+)', remaining, re.IGNORECASE)
        ep_num     = ep_match.group(1) if ep_match else None
        date_match = re.search(r'(\d{1,2})[/\-月](\d{1,2})日?', remaining)
        date_str   = f"{int(date_match.group(1))}/{int(date_match.group(2))}" if date_match else None
        show_raw   = re.sub(r'EP\s*\d+', '', remaining, flags=re.IGNORECASE)
        show_raw   = re.sub(r'\d{1,2}[/\-月]\d{1,2}日?', '', show_raw).strip()
        show_name  = normalize_show(show_raw)
        if not show_name or not ep_num:
            send_reply(token, "格式：刪除 節目名 EP號\n例：刪除 董律師 EP178"); return
        if date_str:
            send_reply(token, f"⏳ 正在同步清空，請稍候...")
            try:
                results = delete_ep_from_sheets(show_name, ep_num=ep_num, date_str=date_str)
            except Exception as e:
                send_reply(token, f"刪除失敗：{e}"); return
            bust()
            msg = f"🗑️ 刪集數完成 {show_name} EP{ep_num}（{date_str}）\n\n" + "\n".join(results) if results else f"找不到對應集數"
            send_reply(token, msg)
        else:
            _pending[USER_ID] = {"show_name": show_name, "ep_num": ep_num, "action": "delete"}
            send_reply(token,
                f"📅 請問要刪除哪一天的 {show_name} EP{ep_num}？\n"
                f"輸入日期，例：4/10\n（輸入「略過」刪除所有符合的 / 「取消」放棄）")
        return

    # ── 日期+節目 → 問EP ──
    date_show_m = re.match(r'^(\d{1,2})[/\-月](\d{1,2})日?\s+(.+)$', text.strip())
    if date_show_m:
        date_str  = f"{int(date_show_m.group(1))}/{int(date_show_m.group(2))}"
        show_raw  = date_show_m.group(3).strip()
        candidates = get_ambiguous_candidates(show_raw)
        if candidates:
            _pending[USER_ID] = {"action": "disambig_then_ep", "date_str": date_str, "candidates": candidates}
            send_reply(token,
                f"📺 {date_str} 「{show_raw}」是哪個節目？\n"
                f"請回覆：{'  '.join(candidates)}\n（輸入「取消」放棄）")
            return
        show_name = normalize_show(show_raw)
        _pending[USER_ID] = {"action": "date_show_ask_ep", "date_str": date_str, "show_name": show_name}
        send_reply(token,
            f"📺 {date_str} {show_name} 是第幾集？\n"
            f"請輸入 EP 號碼，例：EP178 或 ep178\n（輸入「取消」放棄）")
        return

    # ── 歧義節目確認 ──
    if USER_ID in _pending and _pending[USER_ID].get("action") == "disambig_then_ep":
        pending    = _pending.get(USER_ID)
        candidates = pending["candidates"]
        matched_show = next((c for c in candidates if text.strip() in c or c in text.strip()), None)
        if matched_show:
            pending["action"]    = "date_show_ask_ep"
            pending["show_name"] = matched_show
            _pending[USER_ID]    = pending
            send_reply(token,
                f"📺 {pending['date_str']} {matched_show} 是第幾集？\n"
                f"請輸入 EP 號碼，例：EP178\n（輸入「取消」放棄）")
        else:
            send_reply(token, f"請輸入：{'  '.join(candidates)}　（或「取消」放棄）")
        return

    # ── 查詢 ──
    if re.match(r'^(查詢|查|search)\s*', text):
        query = re.sub(r'^(查詢|查|search)\s*', '', text).strip()
        if not query:
            send_reply(token, "格式：查 節目名  或  查 日期\n例：查 董律師 / 查 4/10"); return
        date_q = re.match(r'^(\d{1,2})[/\-月](\d{1,2})日?$', query)
        try:
            sh       = get_confirm_sheet()
            rows_all = sh.get_all_values()
            found    = []
            if date_q:
                target = f"{int(date_q.group(1))}/{int(date_q.group(2))}"
                for row in rows_all:
                    if len(row) >= 5 and row[0].strip() == target and row[3].strip():
                        found.append(f"  {row[2]} {row[3]} {row[4]}")
                header = f"📅 {target} 的排程"
            else:
                show_name = normalize_show(query)
                for row in rows_all:
                    if len(row) >= 5 and row[3].strip() and (
                        show_name.lower() in row[3].lower() or row[3].lower() in show_name.lower()
                    ):
                        found.append(f"  {row[0]} {row[2]} {row[4]}")
                header = f"📺 {show_name} 排程清單"
            msg = f"{header}（{len(found)}筆）\n" + "\n".join(found) if found else f"找不到「{query}」的排程資料"
            send_reply(token, msg)
        except Exception as e:
            send_reply(token, f"查詢失敗：{e}")
        return

    # ── 說明 ──
    send_reply(token,
        "📖 指令說明\n"
        "──────────────\n"
        "【確認上片狀態】\n"
        "  董律師EP177 已排程\n"
        "  董律師EP177 已上片\n\n"
        "【新增排程（兩步）】\n"
        "  新增 董律師 EP178\n"
        "  → Bot 問日期，輸入 4/10\n\n"
        "【刪除排程（兩步）】\n"
        "  刪除 董律師 EP178\n"
        "  → Bot 問日期，輸入 4/10\n\n"
        "【日期+節目（自動問集數）】\n"
        "  4/10 董律師\n"
        "  → Bot 問集數，輸入 EP178\n\n"
        "【補集數】\n"
        "  補集數 董律師 EP178\n\n"
        "【建立新月份表單】\n"
        "  建立5月表單\n"
        "  建立5月表單 覆蓋（已存在時覆蓋）\n\n"
        "【查詢】\n"
        "  查 董律師  或  查 4/10\n\n"
        "【其他】\n"
        "  今日  狀態  全部  取消"
    )

@app.route("/")
def index():
    return "Bot running ✓"

if __name__ == "__main__":
    app.run(port=int(os.environ.get("PORT", 5000)))

