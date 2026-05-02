import datetime
import zoneinfo

NSE_CLOSE = datetime.time(15, 30)
IST = zoneinfo.ZoneInfo("Asia/Kolkata")

def last_closed_week_monday(now_ist: datetime.datetime | None = None) -> datetime.date:
    """Return the Monday of the most recently closed weekly candle (NSE)."""
    now_ist = now_ist or datetime.datetime.now(IST)
    today = now_ist.date()
    weekday = today.weekday()  # Mon=0 ... Sun=6
    monday_this_week = today - datetime.timedelta(days=weekday)

    is_friday_post_close = (weekday == 4 and now_ist.time() >= NSE_CLOSE)
    is_weekend = weekday >= 5

    if is_friday_post_close or is_weekend:
        return monday_this_week
    return monday_this_week - datetime.timedelta(days=7)
