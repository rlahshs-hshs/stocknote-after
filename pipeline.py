"""
시간외 단일가 분석 파이프라인
- 텔레그램 채널에서 상승 종목 파싱
- DART 재무데이터 수집
- Claude 업로드용 HTML 생성
- stocknote-after repo 커밋
"""

import asyncio
import re
import os
import time
import base64
import requests
import pandas as pd
from datetime import datetime
from telethon import TelegramClient
from telethon.sessions import StringSession

# ─────────────────────────────────────────
# 환경변수
# ─────────────────────────────────────────
TELEGRAM_API_ID    = int(os.environ.get("TELEGRAM_API_ID", "0"))
TELEGRAM_API_HASH  = os.environ.get("TELEGRAM_API_HASH", "")
TELEGRAM_SESSION   = os.environ.get("TELEGRAM_SESSION", "")
DART_API_KEY       = os.environ.get("DART_API_KEY", "")
GITHUB_TOKEN       = os.environ.get("GITHUB_TOKEN", "")
GITHUB_REPO        = os.environ.get("GITHUB_REPOSITORY", "rlahshs-hshs/stocknote-after")

CHANNEL_NAME       = "타점 읽어주는 여자(타자)"
DART_BASE_URL      = "https://opendart.fss.or.kr/api"

_CORP_CODE_CACHE   = None


# ─────────────────────────────────────────
# 1. 텔레그램: 시간외 메시지 파싱
# ─────────────────────────────────────────
async def get_afterhours_stocks() -> list:
    """텔레그램 채널에서 오늘 시간외 상승 종목 파싱"""
    stocks = []
    async with TelegramClient(StringSession(TELEGRAM_SESSION), TELEGRAM_API_ID, TELEGRAM_API_HASH) as client:
        # 채널 찾기
        entity = None
        async for dialog in client.iter_dialogs():
            if hasattr(dialog.entity, "title") and CHANNEL_NAME in dialog.entity.title:
                entity = dialog.entity
                break

        if not entity:
            print(f"  ❌ 채널 없음: {CHANNEL_NAME}")
            return []

        print(f"  ✅ 채널 확인: {entity.title}")

        # 가장 최근 시간외 메시지 찾기 (날짜 무관, 최근 100개 스캔)
        found_msg = None
        async for msg in client.iter_messages(entity, limit=100):
            if msg.text and "시간외" in msg.text and ("특이종목" in msg.text or "단일가" in msg.text):
                found_msg = msg
                print(f"  ✅ 시간외 메시지 발견: {msg.date.strftime('%Y-%m-%d %H:%M')} (KST)")
                break

        if not found_msg:
            print("  ❌ 시간외 메시지 없음 (최근 100개 확인)")
            return []

        stocks = parse_afterhours_message(found_msg.text)
        # 상승 종목만 필터링
        stocks = [s for s in stocks if float(s["change_pct"].replace("%","").replace("+","")) > 0]
        print(f"  상승 종목 {len(stocks)}개 파싱 완료")

    return stocks


def parse_afterhours_message(text: str) -> list:
    """시간외 메시지 파싱 → 종목 리스트"""
    pattern = re.compile(
        r'([가-힣A-Za-z0-9&\.\s]+?)\s*\n'
        r'\((\d{6})\)\s*\n'
        r'\(\s*([+-][\d.]+%)\s*\)\s*\n'
        r'(.*?)(?=\n[가-힣A-Za-z]|\Z)',
        re.DOTALL
    )
    stocks = []
    for m in pattern.finditer(text):
        name   = m.group(1).strip()
        code   = m.group(2).strip()
        change = m.group(3).strip()
        reason = m.group(4).strip().replace("\n", " ")
        stocks.append({
            "name":       name,
            "code":       code,
            "change_pct": change,
            "reason":     reason[:300],
        })
    return stocks


# ─────────────────────────────────────────
# 2. DART: 재무데이터 수집
# ─────────────────────────────────────────
def get_corp_code(stock_code: str) -> str:
    global _CORP_CODE_CACHE
    import zipfile, io, xml.etree.ElementTree as ET

    if _CORP_CODE_CACHE is None:
        res = requests.get(f"{DART_BASE_URL}/corpCode.xml", params={"crtfc_key": DART_API_KEY})
        try:
            with zipfile.ZipFile(io.BytesIO(res.content)) as z:
                with z.open("CORPCODE.xml") as f:
                    _CORP_CODE_CACHE = ET.parse(f).getroot()
        except Exception as e:
            print(f"  DART 코드 로딩 실패: {e}")
            return ""

    for item in _CORP_CODE_CACHE.findall("list"):
        if item.findtext("stock_code", "").strip() == stock_code:
            return item.findtext("corp_code", "")
    return ""


def get_financial_statement(corp_code: str, year: int, fs_div: str = "CFS") -> pd.DataFrame:
    res = requests.get(f"{DART_BASE_URL}/fnlttSinglAcntAll.json", params={
        "crtfc_key": DART_API_KEY,
        "corp_code":  corp_code,
        "bsns_year":  str(year),
        "reprt_code": "11011",
        "fs_div":     fs_div,
    })
    data = res.json()
    if data.get("status") != "000":
        return pd.DataFrame()
    df = pd.DataFrame(data["list"])
    df["year"] = year
    return df


def get_financials(stock_code: str) -> str:
    """3개년 재무 요약 텍스트 반환"""
    corp_code = get_corp_code(stock_code)
    if not corp_code:
        return "DART 매칭 실패"

    start_year = datetime.now().year - 3
    all_dfs = []

    for yr in range(start_year, start_year + 3):
        df = get_financial_statement(corp_code, yr, "CFS")
        if df.empty:
            df = get_financial_statement(corp_code, yr, "OFS")
        if not df.empty:
            df["year"] = yr
            all_dfs.append(df)
        time.sleep(0.2)

    if not all_dfs:
        return "재무 데이터 없음"

    raw = pd.concat(all_dfs, ignore_index=True)

    # 계정과목 매핑
    KEY_MAP = {
        "매출액": "매출",
        "영업이익": "영업이익",
        "영업이익(dart)": "영업이익(dart)",
        "당기순이익": "순이익(연결)",
        "지배주주순이익": "순이익(지배)",
    }

    lines = []
    for yr in sorted(raw["year"].unique()):
        yr_df = raw[raw["year"] == yr]
        parts = []
        for account, label in KEY_MAP.items():
            row = yr_df[yr_df["account_nm"] == account]
            if row.empty:
                continue
            try:
                val = float(row.iloc[0]["thstrm_amount"].replace(",", ""))
                parts.append(f"{label} {val/1e8:,.0f}억")
            except Exception:
                pass
        if parts:
            lines.append(f"[{yr}년] " + " / ".join(parts))

    return "\n".join(lines) if lines else "재무 데이터 없음"


# ─────────────────────────────────────────
# 3. HTML 생성
# ─────────────────────────────────────────
def generate_html(date_str: str, stocks: list, financials: dict) -> str:
    today_label = datetime.now().strftime("%Y.%m.%d")

    cards = ""
    for s in stocks:
        fin = financials.get(s["code"], "재무 데이터 없음")
        cards += f"""
<div class="stock-card">
  <div class="card-header">
    <div class="stock-name">{s['name']}</div>
    <div class="stock-meta">
      <span class="code">{s['code']}</span>
      <span class="rate">{s['change_pct']}</span>
    </div>
  </div>
  <div class="section-box">
    <div class="section-label">📢 시간외 재료</div>
    <div class="section-text">{s['reason']}</div>
  </div>
  <div class="section-box">
    <div class="section-label">📊 재무 요약 (DART)</div>
    <div class="section-text fin-text">{fin.replace(chr(10), '<br>')}</div>
  </div>
</div>"""

    return f"""<!DOCTYPE html>
<html lang="ko">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>{today_label} 시간외 단일가 상승 종목</title>
<style>
*{{margin:0;padding:0;box-sizing:border-box}}
body{{font-family:-apple-system,'Apple SD Gothic Neo','Noto Sans KR',sans-serif;
     font-size:15px;line-height:1.6;background:#f5f4f0;color:#1a1a1a;padding-bottom:40px}}
.hero{{background:#1a1a2e;color:#fff;padding:36px 20px 28px}}
.hero-date{{font-size:11px;opacity:.6;letter-spacing:.08em;margin-bottom:8px}}
.hero-title{{font-size:20px;font-weight:800;margin-bottom:6px}}
.hero-sub{{font-size:13px;opacity:.65}}
.container{{max-width:720px;margin:0 auto;padding:16px}}
.summary{{background:#fff;border-radius:14px;padding:14px 16px;margin-bottom:12px;
          font-size:13px;color:#555;border:1px solid rgba(0,0,0,.06)}}
.stock-card{{background:#fff;border-radius:16px;border:1px solid rgba(0,0,0,.06);
             margin-bottom:12px;overflow:hidden;box-shadow:0 2px 8px rgba(0,0,0,.04)}}
.card-header{{background:#1a1a2e;color:#fff;padding:14px 16px;
               display:flex;justify-content:space-between;align-items:center}}
.stock-name{{font-size:17px;font-weight:800}}
.stock-meta{{display:flex;gap:8px;align-items:center}}
.code{{font-size:11px;opacity:.6}}
.rate{{font-size:15px;font-weight:700;color:#ff6b6b}}
.section-box{{padding:12px 16px;border-bottom:1px solid #f4f4f4}}
.section-box:last-child{{border-bottom:none}}
.section-label{{font-size:10px;letter-spacing:.07em;text-transform:uppercase;
                color:#aaa;font-weight:600;margin-bottom:5px}}
.section-text{{font-size:13px;color:#444;line-height:1.65}}
.fin-text{{font-family:monospace;font-size:12px;color:#333}}
.footer{{text-align:center;padding:20px;font-size:11px;color:#bbb}}
</style>
</head>
<body>
<div class="hero">
  <div class="hero-date">{today_label} · 시간외 단일가</div>
  <div class="hero-title">📈 시간외 상승 종목 분석자료</div>
  <div class="hero-sub">타점 읽어주는 여자(타자) · DART 재무데이터 · 총 {len(stocks)}종목</div>
</div>
<div class="container">
  <div class="summary">
    이 파일을 Claude에 업로드하고 분석을 요청하세요.<br>
    수집 시각: {datetime.now().strftime('%Y-%m-%d %H:%M')} KST
  </div>
  {cards}
</div>
<div class="footer">DART 공개 데이터 기반 · 투자 판단은 본인 책임</div>
</body>
</html>"""


# ─────────────────────────────────────────
# 4. GitHub 커밋
# ─────────────────────────────────────────
def commit_to_github(filename: str, content: str, date_str: str):
    headers = {
        "Authorization": f"token {GITHUB_TOKEN}",
        "Accept": "application/vnd.github.v3+json",
    }
    url = f"https://api.github.com/repos/{GITHUB_REPO}/contents/{filename}"

    sha = None
    check = requests.get(url, headers=headers)
    if check.status_code == 200:
        sha = check.json().get("sha")

    payload = {
        "message": f"Add after-hours {date_str}",
        "content": base64.b64encode(content.encode("utf-8")).decode("utf-8"),
        "branch": "main",
    }
    if sha:
        payload["sha"] = sha

    r = requests.put(url, headers=headers, json=payload)
    if r.status_code in (200, 201):
        print(f"  ✅ {filename} 커밋 완료")
    else:
        print(f"  ❌ 커밋 실패: {r.status_code} {r.text[:200]}")


# ─────────────────────────────────────────
# 메인
# ─────────────────────────────────────────
async def main():
    now = datetime.now()
    date_str = now.strftime("%Y%m%d")
    date_label = now.strftime("%Y.%m.%d")

    print(f"\n{'='*50}")
    print(f"  시간외 파이프라인 시작: {now.strftime('%Y.%m.%d %H:%M')}")
    print(f"{'='*50}")

    # 1. 텔레그램 파싱
    print("\n[1] 텔레그램 채널 파싱 중...")
    stocks = await get_afterhours_stocks()

    if not stocks:
        print("  상승 종목 없음 → 종료")
        return

    print(f"\n  파싱된 상승 종목:")
    for s in stocks:
        print(f"    {s['name']} ({s['code']}) {s['change_pct']}")

    # 2. DART 재무 수집
    print("\n[2] DART 재무 수집 중...")
    financials = {}
    for s in stocks:
        print(f"  {s['name']} ({s['code']}) 조회 중...")
        financials[s["code"]] = get_financials(s["code"])
        time.sleep(0.5)

    # 3. HTML 생성
    print("\n[3] HTML 생성 중...")
    html = generate_html(date_label, stocks, financials)
    filename = f"after_{date_str}.html"

    # 로컬 저장
    os.makedirs("output", exist_ok=True)
    with open(f"output/{filename}", "w", encoding="utf-8") as f:
        f.write(html)
    print(f"  로컬 저장: output/{filename}")

    # 4. GitHub 커밋
    print("\n[4] GitHub 커밋 중...")
    commit_to_github(filename, html, date_str)

    print(f"\n{'='*50}")
    print(f"  ✅ 완료! {len(stocks)}종목 처리")
    print(f"  파일: https://github.com/{GITHUB_REPO}/blob/main/{filename}")
    print(f"{'='*50}\n")


if __name__ == "__main__":
    asyncio.run(main())
