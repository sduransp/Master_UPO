#First, we are going to import all the libraries needed for this script
from flask import Flask
from flask_cors import CORS
from six.moves import http_client
import os
import sys
from TwitterAPI import TwitterAPI
import json
from TwitterAPI import TwitterPager
import pandas as pd
from datetime import datetime
import numpy as np
import string
import re
import emoji
import requests
from google.oauth2 import service_account
from google.cloud import bigquery
from google.cloud import storage
from io import BytesIO
import io
from datetime import datetime
import time
from io import StringIO
import csv

# Build flask app
app = Flask(__name__)

# Enable the API endpoint to accept CORS requests.
CORS(app)


@app.route('/', methods=['GET'])
def root():
    """
    This function runs the code for the TWitter API.
    """
    
    # We need to define the API keys to access the Twitter Account
    API_KEY ='***'
    API_KEY_SECRET = '***'
    ACCESS_TOKEN = '***'
    ACCESS_TOKEN_SECRET = '***'

    # User authentification
    api = TwitterAPI(API_KEY, API_KEY_SECRET, ACCESS_TOKEN, ACCESS_TOKEN_SECRET)
    api.auth

    
    # Now we are going to collect information on Twitter about the different products that Laurent Perrier has, as well as 
    # tweets from different users about these products

    List_products = ["#LaurentPerrier","Laurent-Perrier Cuvee Rose", "Laurent-Perrier La Cuvee", "Laurent-Perrier Brut Millesime",\
        "Grand Siecle par Laurent-Perrier", "Laurent-Perrier Ultra Brut", "Laurent-Perrier Alexandra Rose", \
            "Laurent-Perrier Harmony Demi-Sec", "Laurent-Perrier Blanc de Blancs Brut Nature", \
                "Grand Siecle par Laurent-Perrier No 24", "Laurent-Perrier Robe Edition Cuvee Brut Rose"]


    # As we did before, we need to create lists to store the information
    TweetsID = []
    keywords = []
    Tweet_creation = []
    Tweet_likes = []
    Tweet_Retweets = []
    Tweet_Text = []
    Tweet_Lang = []
    Tweet_Location = []
    Tweet_User = []
    User_Followers = []
    User_Friends = []
    Followers = 19299
    NameProducer = "Laurent Perrier"


        # Importantly, we need to clean the text from Twitter and take out some punctuations. I will define a function for this
    def textCleaner(text):

        text  = "".join([char for char in text if char not in string.punctuation])
        text = re.sub('[0-9]+', '', text)
        allchars = [str for str in text]
        list = [c for c in allchars if c in emoji.UNICODE_EMOJI]
        filtered = [str for str in text.split() if not any(i in str for i in list)]
        clean_text = ' '.join(filtered)
        return(clean_text)

    # And I look through all the keywords to retrieve tweets containing them
    for keyword in List_products:

        try:
            SINCE_ID = keyword['id']
        except:
            SINCE_ID = ''

        r_product = TwitterPager(api, 'search/tweets', {'q':keyword, 'count':100, 'since_id': SINCE_ID})
        round = 0
        for item in r_product.get_iterator():
            round += 1
            print(round)
            if item['id'] not in TweetsID:
                TweetsID.append(item['id'])
                keywords.append(keyword)
                creation = datetime.strptime(item['created_at'],'%a %b %d %H:%M:%S %z %Y')
                Tweet_creation.append(creation.strftime('%Y-%m-%d'))
                Tweet_likes.append(item['favorite_count'])
                Tweet_Retweets.append(item['retweet_count'])
                text = textCleaner(item['text'])
                Tweet_Text.append(text)
                Tweet_Lang.append(item['lang'])
                location1 = textCleaner(item['user'].get('location'))
                Tweet_Location.append(location1)
                Tweet_User.append(item['user'].get('id'))
                User_Followers.append(item['user'].get('followers_count'))
                User_Friends.append(item['user'].get('friends_count'))
                
    
    print("All the tweets have been successfully retrieved")

    # Finally, I create a dataframe with these lists, which will be saved and excell and subsequently uploaded to Google Storage
    Tweet_inf = pd.DataFrame({"Producer_name":NameProducer, "Number_of_Followers_on_Twitter": Followers ,"keyword":keywords, "Tweet_ID": TweetsID, "Date": Tweet_creation, "Likes":Tweet_likes, "Retweets":Tweet_Retweets, "Text":Tweet_Text, "Original_Language": Tweet_Lang, "Location": Tweet_Location, "Posted_by": Tweet_User, "User_Follower":User_Followers, "User_Following": User_Friends})
    time1 = datetime.now().strftime("%Y%m%d-%H%M%S")
    name_file = 'Tweet_inf'+time1+'.csv'
    loc_namefile = '/tmp/'+name_file  
    print("The file is going to be saved as csv") 
    Tweet_inf.to_csv(loc_namefile, index=False, header = False, sep = ';')

    print("The code is ready to upload the csv to google storage")
    # To upload the text file to Google Storage, we firstly define a function
    def upload_to_bucket(bucket_name, blob_path, local_path):
        bucket = storage.Client().bucket(bucket_name)
        blob = bucket.blob(blob_path)
        blob.upload_from_filename(local_path)
        return blob.public_url 

    # Specify the parameters we are going to use
    bucket_name = "tweets-iwine"
    bucket_name2 = "tweets-iwine2"
    blob_path = name_file
    local_path = loc_namefile

    #and we call the function
    upload_to_bucket(bucket_name, blob_path, local_path)
    upload_to_bucket(bucket_name2, blob_path, local_path)


    #And now the file is redundant so
    os.remove(loc_namefile)
    return '', http_client.OK


if __name__ == '__main__':
    # This is used when running locally. Gunicorn is used to run the
    # application on Google App Engine. See entrypoint in app.yaml.
    app.run(host='127.0.0.1', port=8080, debug=False)