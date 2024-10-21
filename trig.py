import requests

body = '{"Pipeline": 1}'
requests.post('https://prod2-09.southafricanorth.logic.azure.com:443/workflows/623f55b4346742178048b209a003f895/triggers/When_a_HTTP_request_is_received/paths/invoke?api-version=2016-10-01&sp=%2Ftriggers%2FWhen_a_HTTP_request_is_received%2Frun&sv=1.0&sig=PxhThWSLOC3sS3JAg54Z3uZEhTvU9zAbNIhkaAhMPN0', json=body)
print("Request sent.")
