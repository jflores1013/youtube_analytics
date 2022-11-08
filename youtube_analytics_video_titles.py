
import os
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google_auth_oauthlib.flow import InstalledAppFlow
import google.oauth2.credentials
import google_auth_oauthlib.flow
import googleapiclient.discovery
import googleapiclient.errors
import json
import pandas as pd
import requests
import urllib

client_id = "REPLACE_WITH_YOUR_CLIENT_ID"
client_secret = "REPLACE_WITH_YOUR_CLIENT_SECRET"
auth_uri = "https://accounts.google.com/o/oauth2/auth"
token_uri = "https://oauth2.googleapis.com/token"
auth_provider_x509_cert_url = "https://www.googleapis.com/oauth2/v1/certs"
redirect_uris = ["http://localhost"]
refresh_token = "REPLACE_WITH_YOUR_REFRESH_TOKEN"
channel_id='REPLACE_WITH_YOUR_YOUTUBE_CHANNEL_ID'

def get_all_video_in_channel(channel_id):
    api_key = 'REPLACE_WITH_YOUR_API_KEY'

    base_search_url = 'https://www.googleapis.com/youtube/v3/search?'

    first_url = base_search_url+'key={}&channelId={}&part=snippet,id&order=date&maxResults=100'.format(api_key, channel_id)

    video_ids = []
    url = first_url
    while True:
        inp = urllib.request.urlopen(url)
        resp = json.load(inp)

        for i in resp['items']:
            if i['id']['kind'] == "youtube#video":
                video_ids.append(i['id']['videoId'])

        try:
            next_page_token = resp['nextPageToken']
            url = first_url + '&pageToken={}'.format(next_page_token)
        except:
            break
    return video_ids

video_ids = get_all_video_in_channel(channel_id)

# This function creates a new Access Token using the Refresh Token
# and also refreshes the ID Token (see comment below).
def refreshToken(client_id, client_secret, refresh_token):
        params = {
                "grant_type": "refresh_token",
                "client_id": client_id,
                "client_secret": client_secret,
                "refresh_token": refresh_token
        }

        authorization_url = "https://oauth2.googleapis.com/token"

        r = requests.post(authorization_url, data=params)

        if r.ok:
                return r.json()['access_token']
        else:
                return None

# Call refreshToken which creates a new Access Token
access_token = refreshToken(client_id, client_secret, refresh_token)

scopes = ["https://www.googleapis.com/auth/youtube.readonly"]

# creating a dummy DataFrame to put into the function and overwrite
test = {'data': [1, 2, 3]}
df = pd.DataFrame(test, columns=['data'])


def main(df):
    # Disable OAuthlib's HTTPS verification when running locally.
    # *DO NOT* leave this option enabled in production.
    os.environ["OAUTHLIB_INSECURE_TRANSPORT"] = "1"

    api_service_name = "youtube"
    api_version = "v3"
    client_secrets_file = "REPLACE_WITH_YOUR_CLIENT_SECRET_FILE_PATH/client_secret.json"

    # Get credentials and create an API client
    flow = google_auth_oauthlib.flow.InstalledAppFlow.from_client_secrets_file(
        client_secrets_file, scopes)
    credentials = google.oauth2.credentials.Credentials(
        access_token,
        refresh_token=refresh_token,
        token_uri=token_uri,
        client_id=client_id,
        client_secret=client_secret
        )
    youtube = googleapiclient.discovery.build(
        api_service_name, api_version, credentials=credentials)

    request = youtube.videos().list(
        part="snippet",

        id=video_ids
    )

    response = request.execute()

    # create empty lists to store all column values
    video_id_list = []
    title_list = []
    published_list = []
    description_list = []

    i=0
    while i < len(response['items']):
        video_id_list.append(response['items'][i]['id'])
        title_list.append(response['items'][i]['snippet']['title'])
        published_list.append(response['items'][i]['snippet']['publishedAt'])
        description_list.append(response['items'][i]['snippet']['description'])
        i+=1

    table = {'video_id': video_id_list, 'title': title_list, 'published_date' : published_list, 'description': description_list}
    df = pd.DataFrame(table, columns=['video_id', 'title', 'published_date', 'description'])

    df = df.astype({'video_id': 'str', 'title': 'str', 'published_date': 'str', 'description': 'str'})

    return df

main(df).to_csv('REPLACE_WITH_YOUR_OUTPUT_LOCATION/youtube_analytics_video_titles.csv')

def get_output_schema():
   return pd.DataFrame({
       'video_id': prep_string(),
      'title': prep_string(),
       'published_date': prep_string(),
       'description': prep_string()
   })