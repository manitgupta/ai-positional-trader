import pandas as pd
import numpy as np

def compute_composite_scores(df):
    """
    Compute composite scores for a DataFrame of candidates.
    Uses IBD-style RS, proximity to high, base tightness, sector RS, Code 33, and RVOL.
    """
    if df.empty:
        return df
        
    # Helper for min-max normalization
    def normalize(series):
        if series.max() == series.min():
            return pd.Series(0.5, index=series.index)
        return (series - series.min()) / (series.max() - series.min())

    # 1. IBD RS (if return columns available, else fallback to rs_rank)
    if all(col in df for col in ['ret_3m', 'ret_6m', 'ret_9m', 'ret_12m']):
        df['ibd_rs'] = (
            0.3 * df['ret_3m'].fillna(0)
          + 0.3 * df['ret_6m'].fillna(0)
          + 0.2 * df['ret_9m'].fillna(0)
          + 0.2 * df['ret_12m'].fillna(0)
        )
        df['norm_rs'] = normalize(df['ibd_rs'])
    else:
        df['norm_rs'] = df['rs_rank'] / 100.0 if 'rs_rank' in df else pd.Series(0.5, index=df.index)
        
    # 2. Proximity to 52-w high
    df['norm_proximity'] = normalize(df['pct_from_52w_high']) if 'pct_from_52w_high' in df else pd.Series(0.5, index=df.index)
    
    # 3. Base Tightness (Lower ATR/Close is better)
    if 'atr_14' in df and 'close' in df:
        df['norm_tightness'] = 1 - normalize(df['atr_14'] / df['close'])
    else:
        df['norm_tightness'] = pd.Series(0.5, index=df.index)
        
    # 4. Sector RS
    if 'sector' in df and 'rs_rank' in df:
        sector_avg = df.groupby('sector')['rs_rank'].transform('mean')
        df['norm_sector_rs'] = normalize(sector_avg)
    else:
        df['norm_sector_rs'] = pd.Series(0.5, index=df.index)

    # 5. Code 33 (Earnings Acceleration)
    if 'code33_eps' in df and 'code33_rev' in df:
        df['code33_score'] = 0.0
        df.loc[(df['code33_eps'] == True) & (df['code33_rev'] == True), 'code33_score'] = 1.0
        df.loc[(df['code33_eps'] == True) ^ (df['code33_rev'] == True), 'code33_score'] = 0.5
    else:
        df['code33_score'] = pd.Series(0.5, index=df.index)
        
    # 6. Relative Volume (RVOL)
    df['norm_rvol'] = normalize(df['volume_ratio_20d']) if 'volume_ratio_20d' in df else pd.Series(0.5, index=df.index)

    # Fill NaN in normalized columns with 0.5
    norm_cols = ['norm_rs', 'norm_proximity', 'norm_tightness', 'norm_sector_rs', 'code33_score', 'norm_rvol']
    for col in norm_cols:
        df[col] = df[col].fillna(0.5)

    # Calculate score with new weights
    df['composite_score'] = (
        0.25 * df['norm_rs']
      + 0.15 * df['norm_proximity']
      + 0.20 * df['norm_tightness']
      + 0.15 * df['norm_sector_rs']
      + 0.15 * df['code33_score']
      + 0.10 * df['norm_rvol']
    )
    
    return df

if __name__ == "__main__":
    # Test with dummy DataFrame
    data = {
        'symbol': ['A', 'B', 'C'],
        'rs_rank': [90, 80, 70],
        'pct_from_52w_high': [-5, -10, -20],
        'atr_14': [2, 5, 10],
        'close': [100, 100, 100],
        'sector': ['IT', 'IT', 'AUTO'],
        'ret_3m': [10, 5, 2],
        'ret_6m': [20, 10, 5],
        'ret_9m': [30, 15, 10],
        'ret_12m': [40, 20, 15],
        'volume_ratio_20d': [2.0, 1.5, 1.0],
        'code33_eps': [True, False, False],
        'code33_rev': [True, True, False]
    }
    df = pd.DataFrame(data)
    scored_df = compute_composite_scores(df)
    print(scored_df[['symbol', 'composite_score']])
