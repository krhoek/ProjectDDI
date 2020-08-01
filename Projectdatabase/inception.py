# Import requests module
import requests
from requests.auth import HTTPBasicAuth

# Making a get request
login = 'restuser'
password = 'cB20lBVzXRqO8x6ERgM63fA6'
url = 'http://localhost:8081/rest/user/login'

auth_values = (login, password)
response = requests.get(url, auth=auth_values)

print(response.json())