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

STATUS_MAP = {
    "已排程": "已排程", "排程": "已排程", "排程中": "已排程",
    "已上片": "✓ 已上片", "上片": "✓ 已上片", "完成": "✓ 已上片", "已確認": "✓ 已上片",
    "不上片": "⚠ 不上片", "有問題": "⚠ 不上片", "失敗": "⚠ 不上片", "問題": "⚠ 不上片",
    "未排程": "—未排程", "不上": "—未排程", "\\未排程": "—未排程",
}

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

S_SCHED = "已排程"; S_DONE = "✓ 已上片"
S_ERR   = "⚠ 不上片"; S_SKIP = "—未排程"

def send_reply(reply_token, text):
    with ApiClient(configuration) as api_client:
        api = MessagingApi(api_client)
        api.reply_message(ReplyMessageRequest(
            reply_token=reply_token,
            messages=[TextMessage(text=text)]
        ))

def send_push(text):
    with ApiClient(configuration) as api_client:
        api = MessagingApi(api_client)
        api.push_message(PushMessageRequest(
            to=USER_ID,
            messages=[TextMessage(text=text)]
        ))

def get_sheet():
    info   = json.loads(GSHEET_CREDS)
    scopes = ["https://spreadsheets.google.com/feeds",
              "https://www.googleapis.com/auth/drive"]
    creds  = Credentials.from_service_account_info(info, scopes=scopes)
    gc     = gspread.authorize(creds)
    month  = datetime.datetime.now(TZ).month
    return gc.open_by_key(SHEET_ID).worksheet(f"{month:02d}月確認表")

def get_today_rows(sheet=None):
    if sheet is None:
        sheet = get_sheet()
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
               "status": row[9],
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
              "例：董律師EP176 已排程",
              "其他：今日 / 狀態 / 全部"]
    return "\n".join(lines)

def parse_input(text):
    text = text.strip()
    found_status = None
    found_key    = None
    for key in sorted(STATUS_MAP.keys(), key=len, reverse=True):
        if key in text:
            found_status = STATUS_MAP[key]
            found_key    = key
            break
    if not found_status:
        return None
    remaining = text.replace(found_key, "").strip()
    ep_match  = re.search(r'EP\s*(\d+)', remaining, re.IGNORECASE)
    if not ep_match:
        ep_match = re.search(r'(\d+)', remaining)
    ep_num    = ep_match.group(1) if ep_match else None
    show_raw  = re.sub(r'EP\s*\d+', '', remaining, flags=re.IGNORECASE).strip()
    show_raw  = re.sub(r'\d+', '', show_raw).strip()
    show_name = None
    for alias, canonical in SHOW_ALIASES.items():
        if alias.lower() in show_raw.lower() or show_raw.lower() in alias.lower():
            show_name = canonical
            break
    if not show_name and show_raw:
        show_name = show_raw
    return show_name, ep_num, found_status

def find_rows(sheet, show_name, ep_num):
    all_vals = sheet.get_all_values()
    matched  = []
    ep_str   = f"EP{ep_num}" if ep_num else None
    for i, row in enumerate(all_vals):
        if len(row) < 6: continue
        row_show = str(row[3]).strip()
        row_ep   = str(row[4]).strip()
        show_match = (show_name.lower() in row_show.lower() or
                      row_show.lower() in show_name.lower())
        ep_match = True
        if ep_str:
            ep_match = ep_str.upper() in row_ep.upper() or ep_num in row_ep
        if show_match and ep_match and row_show:
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

_cache = {"date": None, "rows": [], "sheet": None}

def cached_rows():
    today = str(datetime.datetime.now(TZ).date())
    if _cache["date"] != today:
        sh = get_sheet()
        _cache.update({"date": today, "rows": get_today_rows(sh), "sheet": sh})
    return _cache["rows"], _cache["sheet"]

def bust():
    _cache["date"] = None

def push_daily():
    try:
        bust()
        rows, _ = cached_rows()
        send_push(build_today_msg(rows))
    except Exception as e:
        print(f"[push error] {e}")

sched = BackgroundScheduler(timezone=TZ)
sched.add_job(push_daily, "cron", hour=8, minute=0)
sched.start()

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
    rows, sheet = cached_rows()

    if text in ("今日","清單","今天","list"):
        bust(); rows, sheet = cached_rows()
        send_reply(token, build_today_msg(rows)); return

    if text in ("狀態","status","進度"):
        done = sum(1 for r in rows if S_DONE in r["status"])
        err  = sum(1 for r in rows if S_ERR  in r["status"])
        wait = len(rows) - done - err
        send_reply(token, f"📊 今日進度（共{len(rows)}個）\n✅ 已上片 {done}  ⚠️ 不上片 {err}  ⏳ 待確認 {wait}"); return

    if text in ("全部","all"):
        sh = get_sheet(); count = 0
        for r in rows:
            if S_DONE not in r["status"]:
                row_data = sh.row_values(r["row_num"])
                update_platforms(sh, r["row_num"], row_data, S_DONE)
                count += 1
        bust()
        send_reply(token, f"✅ 今日 {count} 個節目全部標記已上片！"); return

    parsed = parse_input(text)
    if parsed:
        show_name, ep_num, new_status = parsed
        if not show_name:
            send_reply(token, "找不到節目名稱，請輸入如：董律師EP176 已排程"); return
        try:
            sh = get_sheet()
        except Exception as e:
            send_reply(token, f"連線失敗：{e}"); return
        matched = find_rows(sh, show_name, ep_num)
        if not matched:
            ep_str = f"EP{ep_num}" if ep_num else "（未指定集數）"
            send_reply(token, f"找不到「{show_name} {ep_str}」\n輸入「今日」查看今日清單"); return
        results = []
        for row_num, row_data in matched:
            updated = update_platforms(sh, row_num, row_data, new_status)
            results.append(f"  {row_data[0]} {row_data[4]} [{' '.join(updated)}]")
        bust()
        label = {S_SCHED:"已排程",S_DONE:"✓ 已上片",S_ERR:"⚠ 不上片",S_SKIP:"—未排程"}.get(new_status, new_status)
        send_reply(token, f"✅ 已更新 {show_name}：\n" + "\n".join(results) + f"\n狀態 → {label}")
        return

    send_reply(token,
        "📖 使用方式\n──────────────\n"
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

