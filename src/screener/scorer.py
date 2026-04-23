import pandas as pd
import numpy as np

def compute_composite_scores(df):
    """
    Compute composite scores for a DataFrame of candidates.
    Normalizes values and applies weights.
    """
    if df.empty:
        return df
        
    # Helper for min-max normalization
    def normalize(series):
        if series.max() == series.min():
            return pd.Series(0.5, index=series.index)
        return (series - series.min()) / (series.max() - series.min())

    # Technicals
    df['norm_rs_rank'] = df['rs_rank'] / 100.0 if 'rs_rank' in df else 0.5
    df['norm_volume_ratio'] = normalize(df['volume_ratio_20d']) if 'volume_ratio_20d' in df else 0.5
    df['norm_adx'] = normalize(df['adx_14']) if 'adx_14' in df else 0.5
    
    # Fundamentals (handle missing columns gracefully)
    df['norm_eps_growth'] = normalize(df['eps_growth_yoy']) if 'eps_growth_yoy' in df else pd.Series(0.5, index=df.index)
    df['norm_rev_growth'] = normalize(df['rev_growth_yoy']) if 'rev_growth_yoy' in df else pd.Series(0.5, index=df.index)
    df['norm_surprise'] = normalize(df['earnings_surprise']) if 'earnings_surprise' in df else pd.Series(0.5, index=df.index)

    # Fill NaN in normalized columns with 0.5
    norm_cols = ['norm_rs_rank', 'norm_volume_ratio', 'norm_adx', 'norm_eps_growth', 'norm_rev_growth', 'norm_surprise']
    for col in norm_cols:
        df[col] = df[col].fillna(0.5)

    # Calculate score
    df['composite_score'] = (
        0.30 * df['norm_rs_rank']
      + 0.20 * df['norm_eps_growth']
      + 0.15 * df['norm_volume_ratio']
      + 0.15 * df['norm_adx']
      + 0.10 * df['norm_rev_growth']
      + 0.10 * df['norm_surprise']
    )
    
    return df

if __name__ == "__main__":
    # Test with dummy DataFrame
    data = {
        'symbol': ['A', 'B', 'C'],
        'rs_rank': [90, 80, 70],
        'volume_ratio_20d': [2.0, 1.5, 1.0],
        'adx_14': [30, 25, 20],
        'eps_growth_yoy': [50, 30, 10],
        'rev_growth_yoy': [30, 20, 5],
        'earnings_surprise': [10, 5, -2]
    }
    df = pd.DataFrame(data)
    scored_df = compute_composite_scores(df)
    print(scored_df[['symbol', 'composite_score']])
