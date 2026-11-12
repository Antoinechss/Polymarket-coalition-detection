from common.params import ORDER_FILLED_HASH

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
        "ts": int(log["blockTimestamp"], 16), 
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
