import pandas as pd

def passes_hard_filters(row) -> bool:
    """
    Check if a stock row passes the hard filters.
    Expects a row object (e.g., pandas Series or dict) with necessary columns.
    """
    try:
        # Technical filters
        tech_pass = (
            row['close'] > row['sma_200']
            and row['sma_50'] > row['sma_150']
            and row['sma_150'] > row['sma_200']
            and row['pct_from_52w_high'] > -25
            and row['rs_rank'] >= 70
            and row['adx_14'] >= 20
            and row['volume_ratio_20d'] >= 1.2
        )
        
        # Fundamental filters (optional for now if not available)
        # We check if columns exist and are not null
        fund_pass = True
        if 'eps_growth_yoy' in row and pd.notnull(row['eps_growth_yoy']):
            fund_pass = fund_pass and (row['eps_growth_yoy'] >= 20)
        if 'rev_growth_yoy' in row and pd.notnull(row['rev_growth_yoy']):
            fund_pass = fund_pass and (row['rev_growth_yoy'] >= 15)
        if 'promoter_holding' in row and pd.notnull(row['promoter_holding']):
            fund_pass = fund_pass and (row['promoter_holding'] >= 30)
            
        return tech_pass and fund_pass
        
    except KeyError as e:
        # print(f"Missing column for filtering: {e}")
        return False
    except Exception as e:
        # print(f"Error in filters: {e}")
        return False

if __name__ == "__main__":
    # Test with dummy data
    test_row = {
        'close': 110,
        'sma_200': 100,
        'sma_50': 105,
        'sma_150': 102,
        'pct_from_52w_high': -5,
        'rs_rank': 80,
        'adx_14': 25,
        'volume_ratio_20d': 1.5,
        'eps_growth_yoy': 25,
        'rev_growth_yoy': 20,
        'promoter_holding': 40
    }
    print(f"Passes filters: {passes_hard_filters(test_row)}") # Should be True
    
    test_row['close'] = 90
    print(f"Passes filters (low price): {passes_hard_filters(test_row)}") # Should be False
