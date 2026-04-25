"""Builds the minimal kernel context passed to Gemini as the opening message."""
import datetime
import duckdb
import pandas as pd

MACRO_STUB = """Nifty 50: Holding above 200 MA, in Stage 2 uptrend.
VIX: 14.5 (Normal).
FII Flows: Net buyers last 5 days.
Leading Sectors: IT, Auto, Capital Goods."""


class ContextBuilder:
    def __init__(self, db_path: str):
        self.db_path = db_path

    def _capital(self) -> str:
        try:
            with duckdb.connect(self.db_path, read_only=True) as c:
                total = (
                    c.execute("SELECT total_capital FROM account LIMIT 1").fetchone()
                    or [1_000_000.0]
                )[0]
                invested = c.execute(
                    "SELECT COALESCE(SUM(entry_price * quantity), 0) "
                    "FROM portfolio WHERE status = 'OPEN'"
                ).fetchone()[0]
                realized = c.execute(
                    "SELECT COALESCE(SUM((exit_price - entry_price) * quantity), 0) "
                    "FROM portfolio WHERE status = 'CLOSED'"
                ).fetchone()[0]
            cash = total + realized - invested
            return (
                f"total_capital={total:,.0f}  invested={invested:,.0f}  "
                f"realized_pnl={realized:,.0f}  available_cash={cash:,.0f}"
            )
        except Exception as e:
            return f"Error computing capital state: {e}"

    def _open_positions(self) -> str:
        try:
            with duckdb.connect(self.db_path, read_only=True) as c:
                df = c.execute("""
                    SELECT symbol, entry_date, entry_price, stop_loss, target
                    FROM portfolio WHERE status = 'OPEN'
                    ORDER BY entry_date
                """).fetchdf()
            return "(none)" if df.empty else df.to_string(index=False)
        except Exception as e:
            return f"Error fetching positions: {e}"

    def _journal_symbols(self) -> str:
        try:
            with duckdb.connect(self.db_path, read_only=True) as c:
                syms = c.execute("""
                    SELECT DISTINCT symbol FROM research_journal
                    WHERE date > current_date - INTERVAL 45 DAY
                    ORDER BY symbol
                """).fetchdf()["symbol"].tolist()
            return ", ".join(syms) if syms else "(none)"
        except Exception:
            return "(none)"

    def build_context(self, candidates_df: pd.DataFrame) -> str:
        today = datetime.date.today().strftime("%Y-%m-%d")

        if candidates_df is None or candidates_df.empty:
            cand_text = "(no candidates passed hard filters today)"
        else:
            wanted = [
                "symbol", "close", "rs_rank", "adx_14", "volume_ratio_20d",
                "pct_from_52w_high", "eps_growth_yoy", "rev_growth_yoy",
                "promoter_holding", "composite_score",
            ]
            cols = [c for c in wanted if c in candidates_df.columns]
            cand_text = candidates_df[cols].to_string(index=False)

        return f"""Today: {today}

## Macro
{MACRO_STUB}

## Capital state
{self._capital()}

## Open positions (symbol + entry info only)
{self._open_positions()}
Use get_open_position_detail() and get_position_history() to drill into any position.

## Symbols tracked in research notes (last 45 days)
{self._journal_symbols()}
Use get_research_notes(symbol) to pull full note detail for any of these.

## Today's screener candidates (ranked by composite score, top-line metrics only)
{cand_text}
Use get_price_history(), get_weekly_history(), get_fundamentals() to research any candidate.

Do not reason from memory or assumptions. Fetch data with tools before forming a view."""
