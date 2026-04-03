import os, json, datetime, re
from flask import Flask, request, abort
from linebot import LineBotApi, WebhookHandler
from linebot.exceptions import InvalidSignatureError
from linebot.models import MessageEvent, TextMessage, TextSendMessage
import gspread
from google.oauth2.service_account import Credentials
from apscheduler.schedulers.background import BackgroundScheduler
import pytz

app = Flask(__name__)

LINE_TOKEN   = os.environ["LINE_CHANNEL_ACCESS_TOKEN"]
LINE_SECRET  = os.environ["LINE_CHANNEL_SECRET"]
SHEET_ID     = "1tu07qjZkLhK1v6endWDL-xUEk-EJGruP"
USER_ID      = os.environ["LINE_USER_ID"]
GSHEET_CREDS = os.environ["GOOGLE_CREDS_JSON"]
TZ           = pytz.timezone("Asia/Taipei")

line_bot_api = LineBotApi(LINE_TOKEN)
handler      = WebhookHandler(LINE_SECRET)

# ── 狀態對應表（使用者輸入 → Sheet 值）──
STATUS_MAP = {
    "已排程": "已排程", "排程": "已排程", "排程中": "已排程",
    "已上片": "✓ 已上片", "上片": "✓ 已上片", "完成": "✓ 已上片", "已確認": "✓ 已上片",
    "不上片": "⚠ 不上片", "有問題": "⚠ 不上片", "失敗": "⚠ 不上片", "問題": "⚠ 不上片",
    "未排程": "—未排程", "不上": "—未排程", "\\未排程": "—未排程",
}

# ── 節目名稱別名對應（模糊比對用）──
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

# 平台欄對應（Col F=6 IG/FB, G=7 TK, H=8 YT）
PLATFORM_COL = {
    "IG/FB": 6, "FB/IG": 6, "IG": 6, "FB": 6,
    "TK": 7,
    "YT": 8,
}
STATUS_COL_INDEX = 9   # I欄 = 全部完成

S_SCHED = "已排程"; S_DONE = "✓ 已上片"
S_ERR   = "⚠ 不上片"; S_SKIP = "—未排程"

# ── Google Sheet 連線 ──
def get_sheet():
    info   = json.loads(GSHEET_CREDS)
    scopes = ["https://spreadsheets.google.com/feeds",
              "https://www.googleapis.com/auth/drive"]
    creds  = Credentials.from_service_account_info(info, scopes=scopes)
    gc     = gspread.authorize(creds)
    month  = datetime.datetime.now(TZ).month
    return gc.open_by_key(SHEET_ID).worksheet(f"{month:02d}月確認表")

# ── 解析使用者輸入 ──────────────────────────────
def parse_input(text):
    """
    解析自然語言輸入，回傳 (show_name, ep_num, status) 或 None
    支援格式：
      董律師EP176 已排程
      董律師 EP176 已排程
      董律師176 已上片
      EP176董律師 已排程
    """
    text = text.strip()

    # 找狀態（最後出現的關鍵字）
    found_status = None
    found_status_key = None
    for key in sorted(STATUS_MAP.keys(), key=len, reverse=True):
        if key in text:
            found_status = STATUS_MAP[key]
            found_status_key = key
            break
    if not found_status:
        return None

    # 移除狀態關鍵字，剩下的是節目+EP
    remaining = text.replace(found_status_key, "").strip()

    # 找 EP 號碼
    ep_match = re.search(r'EP\s*(\d+)', remaining, re.IGNORECASE)
    if not ep_match:
        ep_match = re.search(r'(\d+)', remaining)
    ep_num = ep_match.group(1) if ep_match else None

    # 移除 EP 部分，剩下是節目名稱
    show_raw = re.sub(r'EP\s*\d+', '', remaining, flags=re.IGNORECASE).strip()
    show_raw = re.sub(r'\d+', '', show_raw).strip()

    # 節目名稱比對
    show_name = None
    for alias, canonical in SHOW_ALIASES.items():
        if alias.lower() in show_raw.lower() or show_raw.lower() in alias.lower():
            show_name = canonical
            break
    if not show_name and show_raw:
        show_name = show_raw  # 直接用原始輸入

    return show_name, ep_num, found_status

# ── 在 Sheet 裡找到對應列 ──────────────────────
def find_rows(sheet, show_name, ep_num):
    """
    找到符合 show_name + ep_num 的所有列
    回傳 list of (row_index_1based, row_data)
    """
    all_vals = sheet.get_all_values()
    matched  = []
    ep_str   = f"EP{ep_num}" if ep_num else None

    for i, row in enumerate(all_vals):
        if len(row) < 6:
            continue
        row_show  = str(row[3]).strip()   # D欄 = 節目名稱
        row_ep    = str(row[4]).strip()   # E欄 = 影片集數

        # 節目名稱比對（模糊）
        show_match = (
            show_name.lower() in row_show.lower() or
            row_show.lower() in show_name.lower()
        )
        # EP號碼比對
        ep_match = True
        if ep_str:
            ep_match = ep_str.upper() in row_ep.upper() or ep_num in row_ep

        if show_match and ep_match and row_show:
            matched.append((i + 1, row))  # 1-indexed

    return matched

# ── 更新平台狀態 ───────────────────────────────
def update_platforms(sheet, row_num, row_data, new_status):
    """把非「—未排程」的平台欄全部更新為 new_status"""
    updates = []
    for plat, col in [("IG/FB", 6), ("TK", 7), ("YT", 8)]:
        cur = row_data[col - 1] if len(row_data) >= col else ""
        if cur.strip() not in (S_SKIP, ""):
            sheet.update_cell(row_num, col, new_status)
            updates.append(plat)
    return updates

# ── 今日節目清單 ───────────────────────────────
def get_today_rows(sheet=None):
    if sheet is None:
        sheet = get_sheet()
    today    = datetime.datetime.now(TZ).date()
    date_str = f"{today.month}/{today.day}"
    rows     = sheet.get_all_values()
    results  = []
    for i, row in enumerate(rows):
        if len(row) >= 10 and row[0].strip() == date_str:
            results.append({
                "row_num": i + 1,
                "date": row[0], "slot": row[2],
                "name": row[3], "ep":   row[4],
                "IG_FB": row[5], "TK": row[6], "YT": row[7],
                "status": row[8],
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
              "直接輸入：節目名 EP號 狀態",
              "例：董律師EP176 已排程",
              "例：董律師EP176 已上片",
              "其他指令：今日 / 狀態 / 全部"]
    return "\n".join(lines)

# ── 快取 ──
_cache = {"date": None, "rows": [], "sheet": None}

def cached_rows():
    today = str(datetime.datetime.now(TZ).date())
    if _cache["date"] != today:
        sh = get_sheet()
        _cache.update({"date": today, "rows": get_today_rows(sh), "sheet": sh})
    return _cache["rows"], _cache["sheet"]

def bust():
    _cache["date"] = None

# ── 每日 08:00 推播 ──
def push_daily():
    try:
        bust()
        rows, _ = cached_rows()
        line_bot_api.push_message(USER_ID, TextSendMessage(text=build_today_msg(rows)))
    except Exception as e:
        print(f"[push error] {e}")

sched = BackgroundScheduler(timezone=TZ)
sched.add_job(push_daily, "cron", hour=8, minute=0)
sched.start()

# ── Webhook ──
@app.route("/callback", methods=["POST"])
def callback():
    sig  = request.headers.get("X-Line-Signature","")
    body = request.get_data(as_text=True)
    try: handler.handle(body, sig)
    except InvalidSignatureError: abort(400)
    return "OK"

@handler.add(MessageEvent, message=TextMessage)
def on_msg(event):
    text  = event.message.text.strip()
    reply = event.reply_token

    def send(t):
        line_bot_api.reply_message(reply, TextSendMessage(text=t))

    rows, sheet = cached_rows()

    # ── 今日清單 ──
    if text in ("今日","清單","今天","list"):
        bust(); rows, sheet = cached_rows()
        send(build_today_msg(rows)); return

    # ── 狀態查詢 ──
    if text in ("狀態","status","進度"):
        done = sum(1 for r in rows if S_DONE in r["status"])
        err  = sum(1 for r in rows if S_ERR  in r["status"])
        wait = len(rows) - done - err
        send(f"📊 今日進度（共{len(rows)}個）\n✅ 已上片 {done}  ⚠️ 不上片 {err}  ⏳ 待確認 {wait}"); return

    # ── 全部已上片 ──
    if text in ("全部","all"):
        sh = get_sheet(); count = 0
        for r in rows:
            if S_DONE not in r["status"]:
                row_data = sheet.row_values(r["row_num"]) if sheet else []
                update_platforms(sh, r["row_num"], row_data, S_DONE)
                count += 1
        bust(); send(f"✅ 今日 {count} 個節目全部標記已上片！"); return

    # ══════════════════════════════════════════
    # 核心功能：自然語言解析 → 自動填寫
    # 例：「董律師EP176 已排程」
    # ══════════════════════════════════════════
    parsed = parse_input(text)
    if parsed:
        show_name, ep_num, new_status = parsed

        if not show_name:
            send("找不到節目名稱，請輸入如：董律師EP176 已排程"); return

        try:
            sh = get_sheet()
        except Exception as e:
            send(f"連線 Google Sheet 失敗：{e}"); return

        # 找對應列
        matched = find_rows(sh, show_name, ep_num)

        if not matched:
            ep_str = f"EP{ep_num}" if ep_num else "（未指定集數）"
            send(f"找不到「{show_name} {ep_str}」\n請確認節目名稱和集數是否正確\n輸入「今日」查看今日清單"); return

        # 更新所有找到的列
        results = []
        for row_num, row_data in matched:
            updated_plats = update_platforms(sh, row_num, row_data, new_status)
            date_val = row_data[0] if row_data else "?"
            ep_val   = row_data[4] if len(row_data) > 4 else "?"
            results.append(f"  {date_val} {ep_val} [{' '.join(updated_plats)}]")

        bust()
        status_label = {
            S_SCHED: "已排程", S_DONE: "✓ 已上片",
            S_ERR: "⚠ 不上片", S_SKIP: "—未排程"
        }.get(new_status, new_status)

        send(
            f"✅ 已更新 {show_name}：\n"
            + "\n".join(results)
            + f"\n狀態 → {status_label}"
        )
        return

    # ── 說明 ──
    send(
        "📖 使用方式\n"
        "──────────────\n"
        "輸入節目名稱 + EP號 + 狀態：\n"
        "  董律師EP176 已排程\n"
        "  董律師EP176 已上片\n"
        "  董律師EP176 不上片\n"
        "  董律師EP176 未排程\n\n"
        "其他指令：\n"
        "  今日　查看今日清單\n"
        "  全部　今日全部標記已上片\n"
        "  狀態　查看今日進度"
    )

@app.route("/")
def index():
    return "Bot running ✓"

if __name__ == "__main__":
    app.run(port=int(os.environ.get("PORT", 5000)))
