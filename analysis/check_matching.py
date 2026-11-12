import pandas as pd
from common.params import EXCHANGES_LOWER

def check_matching(orders: pd.DataFrame): 
    """Checks if the amount filled by the maker matches the one filled by taker within each txn"""
    orders["is_taker_order"] = orders["taker"].isin(EXCHANGES_LOWER)
    check = orders.groupby(["tx_hash", "is_taker_order"])['shares'].sum().unstack()
    check["taker_maker_match"] = abs(check[True] - check[False]) <= 1e-6
    return check