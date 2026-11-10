import requests 
import pandas as pd 

ENDPOINT_URL = 'https://polygon-mainnet.g.alchemy.com/v2/alch_AeMPX8j4XsYFoHN43BEKh'

EXCHANGES = ["0xE111180000d2663C0091e4f400237545B87B996B", # CTF 
             "0xe2222d279d744050d28e00520010520000310F59", # Neg Risk 
             "0xe3333700cA9d93003F00f0F71f8515005F6c00Aa"  # V2
             ] 
EXCHANGES_LOWER = [a.lower() for a in EXCHANGES]

ORDER_FILLED_HASH = "0xd543adfd945773f1a62f74f0ee55a5e3b9b1a28262980ba90b1a89f2ea84d8ee"


def decode(log: dict): 

    # --- Filter on orders filled with 4 topics 
    if log['topics'][0] != ORDER_FILLED_HASH: 
        return None
    assert len(log['topics']) == 4, f'Topics format error'


    # --- Topic parse 
    order_hash = log['topics'][1]
    maker_id = "0x" + log['topics'][2][-40:]
    taker_id = "0x" + log['topics'][3][-40:]

    # --- Data parse
    d = log['data'][2:] 
    assert len(d) % 64 == 0
    blocks = [d[i:i+64] for i in range(0, len(d), 64)]

    token_id = int(blocks[1], 16)
    maker_amount_filled = int(blocks[2], 16) / 1000000
    taker_amount_filled = int(blocks[3], 16) / 1000000

    side = int(blocks[0], 16)
    if side == 0:      # BUY: maker gives pUSD, gets shares
        usdc, shares = maker_amount_filled, taker_amount_filled
    elif side == 1:    # SELL: maker gives shares, gets pUSD
        shares, usdc = maker_amount_filled, taker_amount_filled
    else:
        raise ValueError(f"unknown side {side}")
    
    fees = int(blocks[4], 16) / 1000000
    share_price = usdc / shares

    return {
        "tx_hash": log["transactionHash"],
        "log_index": int(log["logIndex"], 16),
        "block": int(log["blockNumber"], 16),
        "order_hash": order_hash,
        "maker": maker_id,
        "taker": taker_id,
        "side": "BUY" if side == 0 else "SELL",
        "token_id": str(token_id), 
        "shares": shares,
        "usdc": usdc,
        "price": share_price,
        "fee": fees,
    }

def check_matching(orders: pd.DataFrame): 
    """Checks if the amount filled by the maker matches the one filled by taker within each txn"""
    orders["is_taker_order"] = orders["taker"].isin(EXCHANGES_LOWER)
    check = orders.groupby(["tx_hash", "is_taker_order"])['shares'].sum().unstack()
    check["taker_maker_match"] = abs(check[True] - check[False]) <= 1e-6
    return check


# --- RPC Endpoint call 
def rpc(method, params=[]):
    r = requests.post(ENDPOINT_URL, json={"jsonrpc": "2.0", "id": 1, "method": method, "params": params})
    response = r.json()
    if "error" in response: 
        raise RuntimeError(response['error'])
    return response['result']

latest_block = int(rpc("eth_blockNumber"), 16)
logs = rpc("eth_getLogs", 
           [{"fromBlock": hex(latest_block - 9),
             "toBlock": hex(latest_block),
             "address": EXCHANGES,
}])


""" ----------------------------------------------------------------------------------
Match confirmation Script : 
In every transaction, the taker’s order equals the sum of its maker pieces, 
so crediting only the maker field never double counts.
"""
# orders = [order for order in (decode(log) for log in logs) if order is not None]
# orders_df = pd.DataFrame(orders)
# check_orders = check_matching(orders_df)
# print(len(check_orders))
# print(check_orders["taker_maker_match"].mean())
# ----------------------------------------------------------------------------------

