import requests

print('ok')
print('requests', requests.__version__)
print('httpbin', requests.get('https://httpbin.org/get', timeout=10).status_code)
