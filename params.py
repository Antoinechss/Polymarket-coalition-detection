import os 
from dotenv import load_dotenv

load_dotenv()

ENDPOINT_URL = os.environ["ENDPOINT_URL"]

EXCHANGES = ["0xE111180000d2663C0091e4f400237545B87B996B", # CTF 
             "0xe2222d279d744050d28e00520010520000310F59", # Neg Risk 
             "0xe3333700cA9d93003F00f0F71f8515005F6c00Aa"  # V2
             ] 

EXCHANGES_LOWER = [a.lower() for a in EXCHANGES]

ORDER_FILLED_HASH = "0xd543adfd945773f1a62f74f0ee55a5e3b9b1a28262980ba90b1a89f2ea84d8ee"
