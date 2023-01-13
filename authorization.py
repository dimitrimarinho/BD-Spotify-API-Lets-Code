import requests

def get_access_token():
    CLIENT_ID = '4a293dfca9f3429aa9dabd24610f4ea9'
    CLIENT_SECRET = 'da75bc2d32f74295b4f97cbe4d53763a'

    AUTH_URL = 'https://accounts.spotify.com/api/token'
    auth_response = requests.post(AUTH_URL, {
    'grant_type': 'client_credentials',
    'client_id': CLIENT_ID,
    'client_secret': CLIENT_SECRET,
    })  
    # convert the response to JSON
    auth_response_data = auth_response.json()

    # save the access token
    return auth_response_data['access_token']

def get_headers():
    
    return {
                'Authorization': 'Bearer {token}'.format(token=get_access_token())
           }

def db_secrets():
    return {"host": "localhost",
            "port": 5432,
            "database": "postgres",
            "user": "postgres",
            "pass": "postgres"}
