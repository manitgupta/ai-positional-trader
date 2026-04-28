# ENHANCEMENT_BACKLOG_V2.md - AI Positional Trader

This document outlines strategic technical enhancements to improve the **accuracy**, **conviction**, and **depth** of the AI trading agent's analysis.

---

## 1. Technical Analysis
*   **[ ] Delivery Percentage Integration**: Fetch NSE Delivery Volume data. A high-volume breakout with >50% delivery indicates institutional accumulation and should increase the agent's conviction score.

## 2. Fundamental Depth & Smart Money
*   **[ ] Expanded Fundamental Scraping**: Update `src/pipeline/fetch_fundamentals.py` to capture **ROE (Return on Equity)**, **Debt-to-Equity**, and **Promoter Holding changes**.
*   **[ ] Smart Money Filters**: Integrate institutional activity (FII/DII) and promoter buy/sell transactions. In the Indian market, increasing promoter skin-in-the-game is a primary conviction signal.
*   **[ ] Earnings Acceleration Logic**: Enhance the `get_fundamentals` tool to return a 3-year historical trend rather than just a TTM snapshot, allowing the agent to distinguish between a one-off recovery and a structural growth story.

## 3. Sectoral & Industry "Cluster" Logic
*   **[ ] Enhanced Peer Comparison**: Update `get_sector_peers` in `src/analyst/tools.py` to include fundamental metrics (EPS Growth, ROE) for peers. This allows the agent to identify if the candidate is a "Quality Leader" or just a "Momentum Laggard."
*   **[ ] Sectoral Breadth Signal**: Add a "Cluster Breadth" metric to sectoral tools (e.g., "X of Y peers are trading within 5% of 52-week highs"). Concurrent industry-wide breakouts are high-probability setups.
*   **[ ] Sub-Industry Mapping**: Refine the `universe` mapping to use specific Industries (e.g., "Public Sector Banks") instead of broad Sectors (e.g., "Financial Services") for more precise peer benchmarking.

## 4. Agent Reasoning & Risk Management
*   **[ ] Cluster Reasoning Prompting**: Update `CANDIDATE_EVALUATOR_PROMPT` to mandate "Cluster Analysis." The agent should explicitly check if it is buying a lone wolf or part of a "Pack Move."
*   **[ ] Multi-Timeframe Validation**: Enforce a "Weekly First" rule where the agent must confirm a stock is in a primary Stage-2 uptrend on the weekly chart before even looking at the daily "cheat" entry.
*   **[ ] Corporate Action Tracking**: Add a tool to fetch Buybacks and Dividends. Large buybacks in the Indian market often provide a price floor for positional trades.

---

### Priority Roadmap
1.  **High Impact / Low Effort**: Enhance `get_sector_peers` with fundamental data and breadth counts.
2.  **High Impact / Med Effort**: Update `ScreenerFetcher` to scrape ROE, Debt, and Promoter Holdings.
3.  **High Impact / High Effort**: Implement the VCP Detector and NSE Delivery Data pipeline.
