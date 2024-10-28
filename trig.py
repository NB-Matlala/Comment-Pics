import requests
import os

base_url = os.getenv("BASE_URL")
con_str_coms = os.getenv("CON_STR_COMS")
log_trg = os.getenv("LOG_TRG")

body = {"Pipeline" : 1}
requests.post(f'{log_trg}', json=body)
print("Request sent.")
