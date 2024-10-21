import requests

body = '{"Pipeline": 1}'
requests.post('https://prod-31.southafricanorth.logic.azure.com:443/workflows/85956207b15d4806af09d4ed0d78a404/triggers/When_a_HTTP_request_is_received/paths/invoke?api-version=2016-10-01&sp=%2Ftriggers%2FWhen_a_HTTP_request_is_received%2Frun&sv=1.0&sig=9wSTRAla28G_81SoFVz14lbacRE14PJMRmjjKpo3v_I', json=body)
print("Request sent.")
