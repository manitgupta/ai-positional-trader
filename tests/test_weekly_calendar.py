import datetime
import zoneinfo
from src.pipeline.weekly_calendar import last_closed_week_monday

IST = zoneinfo.ZoneInfo("Asia/Kolkata")

def at(y, m, d, hh, mm):
    return datetime.datetime(y, m, d, hh, mm, tzinfo=IST)

# Anchor: Mon 2026-04-27 ... Fri 2026-05-01 ... Mon 2026-05-04 ...
def test_monday_morning():
    assert last_closed_week_monday(at(2026, 5, 4, 9, 0)) == datetime.date(2026, 4, 27)

def test_thursday_evening():
    assert last_closed_week_monday(at(2026, 5, 7, 20, 10)) == datetime.date(2026, 4, 27)

def test_friday_pre_close():
    assert last_closed_week_monday(at(2026, 5, 8, 14, 0)) == datetime.date(2026, 4, 27)

def test_friday_at_close():
    assert last_closed_week_monday(at(2026, 5, 8, 15, 30)) == datetime.date(2026, 5, 4)

def test_friday_post_close_production_run():
    # 20:10 IST cron time
    assert last_closed_week_monday(at(2026, 5, 8, 20, 10)) == datetime.date(2026, 5, 4)

def test_saturday():
    assert last_closed_week_monday(at(2026, 5, 9, 10, 0)) == datetime.date(2026, 5, 4)

def test_sunday():
    assert last_closed_week_monday(at(2026, 5, 10, 23, 0)) == datetime.date(2026, 5, 4)
