# Enhancement Backlog

The bot's bones are solid: data → screener → analyst → journal → notifications.
Everything below is **additive** — each plugs in as a new tool, a new pipeline
phase, or a small change to an existing module. Nothing here requires
restructuring `main.py`.

Each item is tagged with rough impact (HIGH / MEDIUM / LOW) and effort
(TINY ~tens of lines / LOW ~100 lines / MEDIUM ~few hundred lines / HIGH bigger).

---

## TL;DR — Top 6 to do first

1. **Real macro tool** — replace the mocked stub.
2. **Sector classification + sector RS** — unlocks rotation analysis and exposure limits.
3. **Earnings calendar** — single biggest avoidable risk.
4. **Memo archive + tool-call logging** — enables every downstream learning loop.
5. **Risk-based position sizing** — portfolio safety, currently fully delegated to Gemini.
6. **VCP / base detection + pivot flag** — separates "in uptrend" from "actionable setup."

The rest are nice-to-haves.

---

## 1. Data enrichment

### Replace the mocked macro snapshot — HIGH / LOW

`ContextBuilder.get_macro_snapshot()` returns hardcoded text. This is the single
most misleading thing in the kernel — Gemini reasons as if the macro is
permanently bullish. Replace with a real `get_macro_snapshot()` tool:

- Nifty 50 close, % change, position vs 50/200 DMA (yfinance `^NSEI`)
- India VIX (yfinance `^INDIAVIX`)
- USD/INR (`USDINR=X`)
- Brent crude (`BZ=F`) — drives RBI/inflation expectations
- US 10Y (`^TNX`) — global risk appetite proxy

Five `yf.Ticker(...).history(period="5d")` calls. Massive payoff: Gemini can
now actually reason about whether to be aggressive or defensive.

### Market breadth — HIGH / LOW

Add `get_breadth()` computed from your existing `signals` table:
- % of universe above 200 DMA
- % above 50 DMA
- New 52-week highs today vs new lows
- Advance/decline ratio

All data already exists. Single SQL query exposed as a tool. Critical signal for
"is this a tape where breakouts work?" Without breadth, every breakout looks
equally good.

### Sector classification — HIGH / MEDIUM

Schema has no sector. Add:

```sql
ALTER TABLE universe ADD COLUMN sector VARCHAR;
ALTER TABLE universe ADD COLUMN industry VARCHAR;
```

Backfill once from NSE's industry CSV or via `yfinance.Ticker(s).info["sector"]`
per symbol. Enables:
- Sector RS rankings (which sectors are leading?)
- Sector exposure check before entry (`config.MAX_SECTOR_EXPOSURE_PCT = 25`
  is currently defined but never enforced).
- Sector-relative momentum (stock vs its sector, not just the market).

### Earnings calendar — HIGH / MEDIUM

Going long into earnings is the single biggest avoidable risk in this strategy.
Add `get_earnings_calendar(symbol, days_ahead=14)` tool. Sources:
- Screener.in shows next results date in page metadata.
- NSE corporate actions API.

Add a hard rule in the system prompt: **"If earnings within 5 trading days,
max conviction = 5 and entry trigger must wait for post-earnings confirmation."**

### Better news + sentiment — MEDIUM / LOW

Current `analyze_sentiment` uses ~9 positive and ~9 negative keywords. Easily
fooled by negation, sarcasm, mixed coverage. Loose substring matching ("INFY"
matches unrelated text).

Cheap fix: replace with a Gemini Flash sentiment call. Cost ~few hundred tokens
per item × ~25 candidates daily. Return structured `{score, material, summary}`.
Use proper company-name matching (look up `universe.company_name`) instead of
ticker substring.

### Earnings surprise — MEDIUM / MEDIUM

`fundamentals.earnings_surprise` is hardcoded to 0.0. This is one of the
strongest factors in the original O'Neil/Minervini frameworks. Pull actual vs
consensus from Trendlyne or Screener.in's "Estimates" tab and compute the beat %.

---

## 2. Setup quality

### VCP / base detection — HIGH / MEDIUM

Currently the bot finds stocks in Stage 2 uptrends but doesn't distinguish
"in a tight base ready to break out" from "extended and ripping." Minervini's
edge **is** base quality; without it, the bot is missing the strategy's core.

Add a `detect_base_pattern(symbol)` tool returning:
- Base type heuristic: VCP / cup-and-handle / flat base / extended / no base.
- Days in base.
- Number of contractions and depth of each (for VCP).
- Pivot point (highest high in the base).

Even rough heuristics beat nothing. ~150 lines using rolling-window logic.

### Pivot point + breakout flag — HIGH / LOW

Add to `signals`:
- `pivot_high_50d` — max high over last 50 days excluding most recent 5 sessions.
- `breakout_today` — close > pivot AND volume > 1.5× 20d avg.

Lets Gemini focus on actionable setups vs observational ones. Maybe even add
this to the hard filters as an OR clause.

### Volatility contraction score — MEDIUM / LOW

A simple "tightness" metric: rolling stdev of last 5 days / rolling stdev of
last 30 days. Values < 0.5 indicate consolidation. Add to signals. Pairs
naturally with VCP detection.

### Distance-from-MA filter — MEDIUM / TINY

Add `pct_from_50ma` to signals. SEPA prefers entries within 5–10% of 50 MA;
stocks 30%+ extended are higher-risk entries. Filter or downgrade.

### Multi-day pattern flags — MEDIUM / LOW

A few cheap binary flags computed at signal time:
- `tight_3w` — last 15 sessions' range < 8% of price.
- `inside_week` — this week's high/low inside last week's high/low.
- `gap_up_with_volume` — gap up > 3% on volume > 2× 20d.

These are exactly the patterns Minervini flags by eye.

---

## 3. Portfolio + risk management

### Risk-based position sizing — HIGH / LOW

Currently `position_size_pct` is whatever Gemini suggests. Should be
deterministic and computed from stop distance:

```
risk_per_trade   = MAX_POSITION_SIZE_PCT% × total_capital × (RISK_PER_TRADE_PCT / 100)
shares           = risk_per_trade / (entry_price - stop_loss)
position_value   = shares × entry_price
position_pct     = position_value / total_capital × 100, capped at MAX_POSITION_SIZE_PCT
```

Implement in `PortfolioManager.open_position` before insert. Gemini still
suggests `entry_zone` and `stop_loss`; the system owns the size.

### Sector exposure enforcement — MEDIUM / LOW

Once sectors exist (see §1), reject (or warn loudly in the memo) any entry that
would push sector exposure past `MAX_SECTOR_EXPOSURE_PCT`. The config field
already exists.

### Daily stop-loss check — HIGH / LOW

Add a Phase 1.5 in `main.py`: for every OPEN position, check today's close vs
`stop_loss`. If hit, surface in the kernel context under a top-level
`## STOPS HIT TODAY` heading so Gemini's portfolio review explicitly addresses
it (or even auto-closes via `PortfolioManager.close_position`).

### Rule-based trailing stops — MEDIUM / LOW

A `suggest_trails()` step nightly:
- Position up 20% → raise stop to entry.
- Position up 50% → raise to 10W MA.
- Close < 10W MA on volume → flag for exit.

Gemini retains final say but gets a system recommendation it can defer to.

### Trade analytics tool — HIGH / LOW

`get_trade_stats()` computed from CLOSED rows: win rate, avg winner %, avg loser %,
expectancy, current drawdown, longest streak. Expose as a tool — Gemini can
self-assess whether the strategy is working and adjust aggression accordingly.

### Capital sync — LOW / MEDIUM

Total capital is hardcoded ₹10L. If you actually trade off this, sync from
broker API (Kite Connect). Otherwise leave as-is.

---

## 4. Conviction + accuracy

### Memo archive + outcome tracking — HIGH / LOW

The single highest-leverage addition. Create:

```sql
CREATE TABLE memos (
  date DATE PRIMARY KEY,
  kernel_context TEXT,
  full_memo TEXT,
  summary TEXT,
  decisions_json TEXT
);
```

Save every nightly memo. Weekly cron: for each past memo, join its decisions
with subsequent price action — did conviction-9 calls actually outperform
conviction-7? Did watchlist triggers fire? Did stops hit?

This is how you stop guessing whether the bot is good. Without this, every other
quality improvement is unmeasurable.

### Tool-call logging — HIGH / TINY

Already partial — `tools.py` prints. Save to a table:

```sql
CREATE TABLE tool_calls (
  run_date DATE, tool_name VARCHAR, args VARCHAR,
  result_chars INTEGER, ts TIMESTAMP
);
```

Then auditable: "did Gemini check weekly for X before writing it up?" Required
for trust and for prompt iteration.

### Self-critique pass — MEDIUM / LOW

After memo generation, a second Gemini call:

> "You are a senior PM reviewing this junior analyst's memo. Find the 2 weakest
> theses and explain why. Is anything force-fitted? Are stops too tight? Are
> conviction scores calibrated to the evidence?"

Append the critique to the saved memo. Optionally feed back for revision before
sending. Cheap; catches force-fits and overconfidence.

### Conviction backtesting — HIGH / MEDIUM

Once memo archive exists: weekly job that asks "do conviction-9 calls actually
outperform conviction-7 over 4–12 weeks?" If not, the model is mis-calibrated
and the system prompt needs tuning. This is the closing of the learning loop.

### Confidence floors from data quality — MEDIUM / LOW

Hard rules in the system prompt, machine-checkable in self-critique:

- No fundamentals data → max conviction 6.
- No weekly data → max conviction 6.
- Earnings within 5 days → max conviction 5.
- News sentiment < -0.3 in last 5 days → max conviction 6.
- Macro VIX > 20 → max conviction 7 across the board.

Stops Gemini confidently writing up data-poor names.

### Multi-perspective ensemble — LOW / MEDIUM

Three sequential roles: SEPA analyst, value skeptic, risk manager. Disagreement
is signal. Combine into final memo. Costs 3× tokens. Worth experimenting once
you have memo archive to measure whether it actually helps.

---

## 5. Observability

### Memo diff tool — MEDIUM / LOW

Compare today's memo to yesterday's: which symbols moved between sections
(watchlist → buy setup, buy setup → exit, anything dropped silently)? Surface as
a "changes since yesterday" preamble in Telegram. Makes daily reading much easier
and catches inconsistency.

### Weekly performance digest — MEDIUM / MEDIUM

Sunday-evening Telegram report: open positions PnL, closed positions stats this
week, watchlist hit rate, equity curve since inception. One Markdown template,
data already exists.

### Run health checks — MEDIUM / LOW

Pre-flight assertions before Phase 4:
- Latest price date is within 2 trading days.
- At least N% of universe has signals.
- Fundamentals freshness (some scraped within last 30 days).

Fail loudly to Telegram if not. Beats silent staleness.

---

## 6. Quality of life

### Manual override CLI — MEDIUM / LOW

Extend `update_portfolio.py` to support:
- Adjust stop loss / target on existing position.
- Add a manual research note that flows into `research_journal`.
- Mark a watchlist symbol as "ignored" so Gemini stops re-suggesting it.

### Symbol notes file — LOW / TINY

A `manual_notes.yaml` you commit alongside the DB: per-symbol overrides, e.g.
"avoid: XYZ — corporate governance issue." Gets injected into the kernel.
Lets you encode hard knowledge without prompt-tuning.

### History pruning — LOW / TINY

DB grows ~30MB/month. Add a quarterly "delete prices older than 2 years" job
once it crosses 200MB. One year of daily is enough for all 200-day MAs.

---

## Things to deliberately skip

- **Options data.** Indian options data is a rabbit hole. Skip unless you
  actively trade options.
- **Intraday data + execution.** Different strategy. The bot is nightly by design.
- **Real broker integration for auto-execution.** Major scope, regulatory
  headaches, marginal benefit. Keep it advisory.
- **Reinforcement learning / fine-tuning.** Premature. The deterministic system
  isn't even fully measured yet.
- **Insider/promoter activity scraping.** Data on Indian exchanges is messy and
  the alpha is marginal vs effort.
- **Custom ML models for sentiment / patterns.** Gemini Flash is fine for these.
  Don't reinvent.

---

## Suggested rollout order

| Week | Theme | Items |
|------|-------|-------|
| 1 | Foundation for learning | Memo archive, tool-call logging, real macro tool, breadth tool |
| 2 | Risk reduction | Earnings calendar, daily stop check, sector classification |
| 3 | Portfolio hygiene | Risk-based sizing, sector exposure check, trade analytics tool |
| 4 | Setup quality | Pivot/breakout flags, distance-from-MA, VCP detection |
| 5 | Quality loop | Self-critique pass, confidence floors, memo diff |
| 6+ | Measurement | Conviction backtesting on accumulated memo archive |

Each batch is 100–300 lines of code, fits the existing architecture as new
tools or a small pipeline phase, and produces a Telegram-visible improvement.
