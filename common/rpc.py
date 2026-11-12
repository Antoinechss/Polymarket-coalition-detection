from common.params import ENDPOINT_URL
import requests

# --- RPC Endpoint call 
def rpc(method, params=[]):
    r = requests.post(ENDPOINT_URL, json={"jsonrpc": "2.0", "id": 1, "method": method, "params": params})
    response = r.json()
    if "error" in response: 
        raise RuntimeError(response['error'])
    return response['result']