import requests
from requests_oauthlib import OAuth2Session

def get_token(s: requests.Session, user: str, psw: str):

    client_id = user
    client_secret = psw
    authorization_base_url = 'https://hst-api.wialon.host/oauth'
    


    url = 'https://hosting.wialon.host/login.html'
    result = s.get(url)
    url = 'https://hosting.wialon.host/oauth/authorize'
    params = {
        "response_type": "code",
        "client_id": "tpgk",
        "redirect_uri": "https://hosting.wialon.host/login.html"
        }

print('hello')