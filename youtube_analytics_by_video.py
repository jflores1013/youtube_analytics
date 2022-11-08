import os
import numpy as np
import google.oauth2.credentials
import google_auth_oauthlib.flow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google_auth_oauthlib.flow import InstalledAppFlow
import json
import pandas as pd
import requests
import urllib
from datetime import datetime

current_date = datetime.now()
current_date = current_date.strftime('%Y-%m-%d')

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

video_id_list = get_all_video_in_channel(channel_id)

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

# creating a dummy DataFrame to put into the function and overwrite
test = {'data': [1, 2, 3]}
df = pd.DataFrame(test, columns=['data'])

SCOPES = ['https://www.googleapis.com/auth/yt-analytics.readonly']

API_SERVICE_NAME = 'youtubeAnalytics'
API_VERSION = 'v2'
CLIENT_SECRETS_FILE = 'REPLACE_WITH_YOUR_CLIENT_SECRET_FILE_PATH/client_secret.json'
def get_service():
  flow = InstalledAppFlow.from_client_secrets_file(CLIENT_SECRETS_FILE, SCOPES)
  credentials = google.oauth2.credentials.Credentials(
        access_token,
        refresh_token=refresh_token,
        token_uri=token_uri,
        client_id=client_id,
        client_secret=client_secret
        )
  return build(API_SERVICE_NAME, API_VERSION, credentials = credentials, cache_discovery=False)

def execute_api_request(client_library_function, **kwargs):
  response = client_library_function(
    **kwargs
  ).execute()

  return response

def main(df):
  #if __name__ == '__main__':
    # Disable OAuthlib's HTTPs verification when running locally.
    # *DO NOT* leave this option enabled when running in production.
  os.environ['OAUTHLIB_INSECURE_TRANSPORT'] = '1'

  # create empty lists to store all column values
  video_id_day_list = []
  date_list = []
  emw_list = []
  views_list = []
  likes_list = []
  subscribers_gained_list = []
  
  subscribers_lost_list = []
  average_view_list = []
  shares_list = []
  dislikes_list = []

  for i in video_id_list:
    youtubeAnalytics = get_service()
    request=execute_api_request(
        youtubeAnalytics.reports().query,
        ids='channel==MINE',
        startDate='2021-08-25',
        endDate=current_date,
        metrics='estimatedMinutesWatched,views,likes,subscribersGained,subscribersLost,averageViewDuration,shares,dislikes',
        dimensions='day',
        sort='day',
        filters='video=='+i
    )

    response = request['rows']

    rows = response

    # parse the values out of each row into the appropriate column
    for x in rows:
      # fill each row with the video_id value
      v=i
      video_id_day_list.append(v)

      # fill each row with the date value
      v=x[0]
      date_list.append(v)

      # fill each row with the estimated_minutes_watched value
      v=x[1]
      emw_list.append(v)

      # fill each row with the views value
      v=x[2]
      views_list.append(v)

      # fill each row with the likes value
      v=x[3]
      likes_list.append(v)

      # fill each row with the subscribers_gained value
      v=x[4]
      subscribers_gained_list.append(v)

      # fill each row with the subscribers_lost value
      v=x[5]
      subscribers_lost_list.append(v)

      # fill each row with the average_view_duration value
      v=x[6]
      average_view_list.append(v)

      # fill each row with the shares value
      v=x[7]
      shares_list.append(v)

      # fill each row with the dislikes value
      v=x[8]
      dislikes_list.append(v)


  

  table = {'video_id': video_id_day_list, 'date': date_list, 'estimated_minutes_watched': emw_list, 'views': views_list, 'likes': likes_list, 'subscribers_gained': subscribers_gained_list, 'subscribers_lost': subscribers_lost_list, 'average_view_duration': average_view_list, 'shares': shares_list, 'dislikes': dislikes_list}
  df = pd.DataFrame(table, columns=['video_id', 'date', 'estimated_minutes_watched', 'views', 'likes', 'subscribers_gained', 'subscribers_lost', 'average_view_duration', 'shares', 'dislikes'])
  
  df = df.astype({'video_id': 'str', 'date': 'str', 'estimated_minutes_watched': 'int', 'views': 'int', 'likes': 'int', 'subscribers_gained': 'int', 'subscribers_lost': 'int', 'average_view_duration': 'int', 'shares': 'int', 'dislikes': 'int'})

  return df

main(df).to_csv('REPLACE_WITH_YOUR_OUTPUT_LOCATION/youtube_analytics_by_video.csv')

def get_output_schema():
   return pd.DataFrame({
       'video_id': prep_string(),
       'date': prep_string(),
       'estimated_minutes_watched': prep_int(),
       'views': prep_int(),
       'likes': prep_int(),
       'subscribers_gained': prep_int(),
       'subscribers_lost': prep_int(),
       'average_view_duration': prep_int(),
       'shares': prep_int(),
       'dislikes': prep_int()
   })