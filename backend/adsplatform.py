# backend/adsplatform.py
"""
Core Ads Platform logic (single-file prototype)

Features:
- SQLite-backed simple schema (ads, creators, impressions, clicks, user_ad_history, creator_earnings)
- Ad selection by country + CPM/CPC bidding (chooses ad with highest expected revenue)
- Impression recording with anti-fraud rules (cooldown, per-user-per-ad daily cap)
- Click recording with click limits per user/ad/day
- Earning calculation per impression and click; summary aggregation to creator_earnings table
- Payout calculation with creator share, TDS and optional transfer fee borne by platform or user
- Demo simulation when run as __main__
"""

import sqlite3
import time
import datetime
from typing import Optional, Tuple, List, Dict

DB = "adsplatform_demo.db"

# ---------- Config ----------
DEFAULT_CTR_ESTIMATE = 0.02   # estimated click-through-rate used to convert CPC to CPM-equivalent when selecting ads
VIEW_EARNING_ROUND = 4        # decimals for earning amounts
CREATOR_SHARE = 0.50          # fraction of total revenue given to creator (adjustable per plan)
TDS_PERCENT = 0.10            # TDS deduction percent (10%)
TRANSFER_FEE = 2.00           # flat transfer fee (currency units) for example
PLATFORM_PAYS_TRANSFER_FEE = True  # if True platform pays transfer fee; else user pays
MAX_VIEWS_PER_AD_PER_VIEWER_PER_DAY = 3
COOLDOWN_SECONDS_BETWEEN_COUNTED_IMPRESSIONS = 60  # seconds before same viewer/ad counts again
MAX_CLICKS_PER_AD_PER_VIEWER_PER_DAY = 2

# ---------- DB helpers ----------
def get_conn():
    conn = sqlite3.connect(DB, detect_types=sqlite3.PARSE_DECLTYPES|sqlite3.PARSE_COLNAMES)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    conn = get_conn()
    c = conn.cursor()
    # Ads table (advertiser defined)
    c.execute("""
    CREATE TABLE IF NOT EXISTS ads (
        ad_id TEXT PRIMARY KEY,
        advertiser_id TEXT,
        ad_type TEXT CHECK(ad_type IN ('CPM','CPC')),
        cpm_rate REAL DEFAULT 0,   -- rupees per 1000 views
        cpc_rate REAL DEFAULT 0,   -- rupees per click
        target_country TEXT,       -- country code or 'GLOBAL'
        status TEXT DEFAULT 'ACTIVE', -- ACTIVE/PAUSED
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )""")
    # Creators
    c.execute("""
    CREATE TABLE IF NOT EXISTS creators (
        creator_id TEXT PRIMARY KEY,
        name TEXT,
        country TEXT,
        followers INTEGER DEFAULT 0,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )""")
    # Impressions: one row per counted impression
    c.execute("""
    CREATE TABLE IF NOT EXISTS ad_impressions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ad_id TEXT,
        creator_id TEXT,
        viewer_id TEXT,
        viewer_country TEXT,
        cpm_value REAL,      -- ad's cpm at time
        earning REAL,        -- earning from this impression (CPM/1000 or 0)
        timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )""")
    # Clicks
    c.execute("""
    CREATE TABLE IF NOT EXISTS ad_clicks (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ad_id TEXT,
        creator_id TEXT,
        viewer_id TEXT,
        cpc_value REAL,
        earning REAL,
        timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )""")
    # user_ad_history: track last_seen and count to enforce cooldown and per-day caps
    c.execute("""
    CREATE TABLE IF NOT EXISTS user_ad_history (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        viewer_id TEXT,
        ad_id TEXT,
        last_seen TIMESTAMP,
        view_count_today INTEGER DEFAULT 0,
        last_seen_date DATE
    )""")
    # creator earnings summary (aggregated)
    c.execute("""
    CREATE TABLE IF NOT EXISTS creator_earnings (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        creator_id TEXT,
        date DATE,
        total_views INTEGER DEFAULT 0,
        total_clicks INTEGER DEFAULT 0,
        total_earning REAL DEFAULT 0,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(creator_id, date)
    )""")
    conn.commit()
    conn.close()

# ---------- Utility functions ----------
def now_ts():
    return datetime.datetime.utcnow()

def today_date():
    return datetime.date.today()

def to_date(dt):
    return dt.date() if isinstance(dt, datetime.datetime) else dt

# ---------- Core platform functions ----------
def add_ad(ad_id: str, advertiser_id: str, ad_type: str, cpm_rate: float=0.0,
           cpc_rate: float=0.0, target_country: str="GLOBAL"):
    conn = get_conn()
    c = conn.cursor()
    c.execute("""
    INSERT OR REPLACE INTO ads (ad_id, advertiser_id, ad_type, cpm_rate, cpc_rate, target_country, status)
    VALUES (?, ?, ?, ?, ?, ?, 'ACTIVE')
    """, (ad_id, advertiser_id, ad_type, cpm_rate, cpc_rate, target_country))
    conn.commit()
    conn.close()

def add_creator(creator_id: str, name: str, country: str, followers: int=0):
    conn = get_conn()
    c = conn.cursor()
    c.execute("""
    INSERT OR REPLACE INTO creators (creator_id, name, country, followers)
    VALUES (?, ?, ?, ?)
    """, (creator_id, name, country, followers))
    conn.commit()
    conn.close()

def get_active_ads_for_country(country: str) -> List[sqlite3.Row]:
    conn = get_conn()
    c = conn.cursor()
    # preferred: exact country ads first, then GLOBAL
    c.execute("SELECT * FROM ads WHERE status='ACTIVE' AND (target_country = ? OR target_country = 'GLOBAL')", (country,))
    rows = c.fetchall()
    conn.close()
    return rows

def estimate_ad_expected_cpm_equivalent(ad_row: sqlite3.Row) -> float:
    """
    Convert ad's bidder value into a comparable CPM-equivalent number.
    For CPM ads: return cpm_rate.
    For CPC ads: convert using estimated CTR: CPM_equiv = cpc_rate * (CTR * 1000)
    """
    if ad_row["ad_type"] == "CPM":
        return float(ad_row["cpm_rate"])
    else:
        # CPC -> estimate CPM equivalent
        ctr = DEFAULT_CTR_ESTIMATE
        cpc = float(ad_row["cpc_rate"])
        cpm_equiv = cpc * ctr * 1000.0
        return cpm_equiv

def select_ad_for_view(creator_id: str, viewer_country: str) -> Optional[sqlite3.Row]:
    """
    Choose which ad to show for this view:
    - Filter ACTIVE ads that target the viewer_country or GLOBAL
    - Compute expected CPM-equivalent and pick highest
    """
    ads = get_active_ads_for_country(viewer_country)
    if not ads:
        return None
    best = None
    best_value = -1.0
    for ad in ads:
        val = estimate_ad_expected_cpm_equivalent(ad)
        # small tie-breaker by higher CPM or CPC absolute
        if val > best_value:
            best_value = val
            best = ad
    return best

# ---------- Anti-fraud rules: cooldown and per-day caps ----------
def can_count_impression(viewer_id: str, ad_id: str) -> bool:
    """Return True if impression should be counted for earnings, else False (e.g., repeated within cooldown or exceeded daily cap)."""
    conn = get_conn()
    c = conn.cursor()
    today = today_date()
    c.execute("""
    SELECT id, last_seen, view_count_today, last_seen_date FROM user_ad_history
    WHERE viewer_id = ? AND ad_id = ?
    """, (viewer_id, ad_id))
    row = c.fetchone()
    if not row:
        conn.close()
        return True  # no history: count
    # check date reset
    last_seen = row["last_seen"]
    last_seen_date = row["last_seen_date"]
    vcount = row["view_count_today"]
    # if last_seen_date != today, reset count
    if last_seen_date is None or to_date(last_seen_date) != today:
        conn.close()
        return True
    # check daily cap
    if vcount >= MAX_VIEWS_PER_AD_PER_VIEWER_PER_DAY:
        conn.close()
        return False
    # check cooldown
    if last_seen is not None:
        diff = (now_ts() - datetime.datetime.fromisoformat(last_seen)).total_seconds()
        if diff < COOLDOWN_SECONDS_BETWEEN_COUNTED_IMPRESSIONS:
            conn.close()
            return False
    conn.close()
    return True

def register_view_history(viewer_id: str, ad_id: str):
    """Update or insert user_ad_history when a view occurs (counted or not)."""
    conn = get_conn()
    c = conn.cursor()
    today = today_date()
    c.execute("""
    SELECT id, last_seen, view_count_today, last_seen_date FROM user_ad_history
    WHERE viewer_id = ? AND ad_id = ?
    """, (viewer_id, ad_id))
    row = c.fetchone()
    tnow = now_ts().isoformat()
    if not row:
        c.execute("""
        INSERT INTO user_ad_history (viewer_id, ad_id, last_seen, view_count_today, last_seen_date)
        VALUES (?, ?, ?, ?, ?)
        """, (viewer_id, ad_id, tnow, 1, today.isoformat()))
    else:
        last_seen_date = row["last_seen_date"]
        vcount = row["view_count_today"]
        if last_seen_date is None or to_date(last_seen_date) != today:
            # reset
            vcount = 1
        else:
            vcount = vcount + 1
        c.execute("""
        UPDATE user_ad_history SET last_seen = ?, view_count_today = ?, last_seen_date = ?
        WHERE id = ?
        """, (tnow, vcount, today.isoformat(), row["id"]))
    conn.commit()
    conn.close()

# ---------- Record impression and click ----------
def record_impression(ad_row: sqlite3.Row, creator_id: str, viewer_id: str, viewer_country: str) -> Tuple[bool, float]:
    """
    Try to record an impression.
    Returns (counted, earning_added).
    If anti-fraud blocks counting, return (False, 0.0) but still update history.
    """
    ad_id = ad_row["ad_id"]
    counted = can_count_impression(viewer_id, ad_id)
    # always update history record to note the last seen
    register_view_history(viewer_id, ad_id)
    earning = 0.0
    if counted:
        # CPM earning per view = ad.cpm_rate / 1000
        cpm_rate = float(ad_row["cpm_rate"])
        earning = round((cpm_rate / 1000.0), VIEW_EARNING_ROUND)
        conn = get_conn()
        c = conn.cursor()
        c.execute("""
        INSERT INTO ad_impressions (ad_id, creator_id, viewer_id, viewer_country, cpm_value, earning, timestamp)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (ad_id, creator_id, viewer_id, viewer_country, cpm_rate, earning, now_ts()))
        conn.commit()
        conn.close()
        # update summary
        update_creator_summary(creator_id, today_date(), view_inc=1, earning_inc=earning)
    return counted, earning

def record_click(ad_row: sqlite3.Row, creator_id: str, viewer_id: str) -> Tuple[bool, float]:
    """
    Record a click event. Enforce per viewer/ad/day click cap.
    Returns (counted, earning).
    """
    ad_id = ad_row["ad_id"]
    # enforce click cap:
    conn = get_conn()
    c = conn.cursor()
    today = today_date()
    # count clicks today for this viewer/ad
    c.execute("""
    SELECT COUNT(*) as cnt FROM ad_clicks
    WHERE ad_id = ? AND viewer_id = ? AND DATE(timestamp) = ?
    """, (ad_id, viewer_id, today.isoformat()))
    cnt = c.fetchone()["cnt"]
    if cnt >= MAX_CLICKS_PER_AD_PER_VIEWER_PER_DAY:
        conn.close()
        return False, 0.0
    # count click
    cpc = float(ad_row["cpc_rate"])
    earning = round(cpc, VIEW_EARNING_ROUND)
    c.execute("""
    INSERT INTO ad_clicks (ad_id, creator_id, viewer_id, cpc_value, earning, timestamp)
    VALUES (?, ?, ?, ?, ?, ?)
    """, (ad_id, creator_id, viewer_id, cpc, earning, now_ts()))
    conn.commit()
    conn.close()
    # update summary
    update_creator_summary(creator_id, today_date(), click_inc=1, earning_inc=earning)
    return True, earning

# ---------- Summary / aggregation ----------
def update_creator_summary(creator_id: str, date: datetime.date, view_inc: int=0, click_inc: int=0, earning_inc: float=0.0):
    conn = get_conn()
    c = conn.cursor()
    # ensure row exists
    c.execute("SELECT id FROM creator_earnings WHERE creator_id = ? AND date = ?", (creator_id, date.isoformat()))
    row = c.fetchone()
    if not row:
        c.execute("""
        INSERT INTO creator_earnings (creator_id, date, total_views, total_clicks, total_earning, updated_at)
        VALUES (?, ?, ?, ?, ?, ?)
        """, (creator_id, date.isoformat(), view_inc, click_inc, earning_inc, now_ts()))
    else:
        c.execute("""
        UPDATE creator_earnings
        SET total_views = total_views + ?, total_clicks = total_clicks + ?, total_earning = total_earning + ?, updated_at = ?
        WHERE id = ?
        """, (view_inc, click_inc, earning_inc, now_ts(), row["id"]))
    conn.commit()
    conn.close()

def get_creator_summary(creator_id: str, start_date: datetime.date, end_date: datetime.date) -> Dict:
    conn = get_conn()
    c = conn.cursor()
    c.execute("""
    SELECT SUM(total_views) AS views, SUM(total_clicks) AS clicks, SUM(total_earning) AS earning
    FROM creator_earnings
    WHERE creator_id = ? AND date BETWEEN ? AND ?
    """, (creator_id, start_date.isoformat(), end_date.isoformat()))
    row = c.fetchone()
    conn.close()
    return {"views": row["views"] or 0, "clicks": row["clicks"] or 0, "earning": round(row["earning"] or 0.0, VIEW_EARNING_ROUND)}

# ---------- Payout calculations ----------
def calculate_payout_amount(creator_total_earning: float, creator_share: float = CREATOR_SHARE,
                            tds_percent: float = TDS_PERCENT, transfer_fee: float = TRANSFER_FEE,
                            platform_pays_fee: bool = PLATFORM_PAYS_TRANSFER_FEE) -> Dict:
    """
    Given total platform earning for a creator, compute:
    - creator_gross (share)
    - tds_amount (deducted)
    - transfer_fee (if user pays or platform pays)
    - final_payout (amount user receives)
    """
    # creator share
    gross = round(creator_total_earning * creator_share, VIEW_EARNING_ROUND)
    tds = round(gross * tds_percent, VIEW_EARNING_ROUND)
    if platform_pays_fee:
        payout = round(gross - tds, VIEW_EARNING_ROUND)
        platform_net_cost = round(transfer_fee, VIEW_EARNING_ROUND)
    else:
        payout = round(gross - tds - transfer_fee, VIEW_EARNING_ROUND)
        platform_net_cost = 0.0
    return {
        "gross": gross,
        "tds": tds,
        "transfer_fee": transfer_fee if not platform_pays_fee else 0.0,
        "final_payout": payout,
        "platform_cost": platform_net_cost
    }

# ---------- Reporting helpers ----------
def report_top_creators_by_period(start_date: datetime.date, end_date: datetime.date, limit=10):
    conn = get_conn()
    c = conn.cursor()
    c.execute("""
    SELECT creator_id, SUM(total_views) as views, SUM(total_clicks) as clicks, SUM(total_earning) as earning
    FROM creator_earnings
    WHERE date BETWEEN ? AND ?
    GROUP BY creator_id
    ORDER BY earning DESC
    LIMIT ?
    """, (start_date.isoformat(), end_date.isoformat(), limit))
    rows = c.fetchall()
    conn.close()
    return rows

# ---------- Demo / Simulation helpers ----------
def simulate_view_flow(viewer_id: str, creator_id: str, viewer_country: str):
    """Select an ad and try to record an impression. Return ad_id and earning info."""
    ad = select_ad_for_view(creator_id, viewer_country)
    if ad is None:
        return None, False, 0.0
    counted, earning = record_impression(ad, creator_id, viewer_id, viewer_country)
    return ad["ad_id"], counted, earning

def simulate_click_flow(ad_id: str, creator_id: str, viewer_id: str):
    """Fetch ad row and record click."""
    conn = get_conn()
    c = conn.cursor()
    c.execute("SELECT * FROM ads WHERE ad_id = ?", (ad_id,))
    ad = c.fetchone()
    conn.close()
    if not ad:
        return False, 0.0
    return record_click(ad, creator_id, viewer_id)

# ---------- CLI demo if run directly ----------
def seed_demo_data():
    # reset DB
    import os
    if os.path.exists(DB):
        os.remove(DB)
    init_db()
    # add creators
    add_creator("C001", "Ali", "India", 1200)
    add_creator("C002", "John", "USA", 5000)
    add_creator("C003", "Sara", "UK", 800)
    add_creator("C004", "Ahmed", "India", 300)
    add_creator("C005", "Mike", "USA", 15000)
    add_creator("C006", "Zoya", "India", 2200)
    add_creator("C007", "David", "USA", 7000)
    add_creator("C008", "Fatima", "UAE", 1800)
    add_creator("C009", "Rahul", "India", 900)
    add_creator("C010", "Emma", "USA", 25000)
    # add ads
    add_ad("AD001", "ADV_India", "CPM", cpm_rate=100.0, cpc_rate=2.0, target_country="India")
    add_ad("AD002", "ADV_USA", "CPM", cpm_rate=400.0, cpc_rate=5.0, target_country="USA")
    add_ad("AD003", "ADV_Global", "CPC", cpm_rate=0.0, cpc_rate=10.0, target_country="GLOBAL")

def run_demo_simulation():
    seed_demo_data()
    # Simulate views & clicks (some patterns)
    viewers = [
        ("V100", "India"),
        ("V101", "USA"),
        ("V102", "UK"),
        ("V103", "India"),
        ("V104", "USA"),
        ("V105", "UAE"),
    ]
    # simulate multiple views for creators
    import random
    for _ in range(2000):
        viewer_id, viewer_country = random.choice(viewers)
        creator = random.choice(["C001","C002","C003","C004","C005","C006","C007","C008","C009","C010"])
        ad_id, counted, earning = simulate_view_flow(viewer_id, creator, viewer_country)
        # occasionally click
        if ad_id and random.random() < 0.02:  # small click probability
            simulate_click_flow(ad_id, creator, viewer_id)
    # print summary for 10 creators
    start = today_date() - datetime.timedelta(days=1)
    end = today_date()
    rows = report_top_creators_by_period(start, end, limit=20)
    print("Creator report (views, clicks, earning) for today:")
    for r in rows:
        print(f"Creator={r['creator_id']}, views={r['views']}, clicks={r['clicks']}, earning=₹{round(r['earning'],4)}")
    # show example payouts for top creator
    if rows:
        top = rows[0]
        payout = calculate_payout_amount(top["earning"])
        print("\nPayout example for top creator:")
        print(payout)

if __name__ == "__main__":
    print("Initializing demo DB and running simulation (this may take a few seconds)...")
    run_demo_simulation()
    print("Demo finished. DB file:", DB)
