#First, we are going to import all the libraries needed for this script
from flask import Flask
from flask_cors import CORS
from six.moves import http_client
import json
import pandas as pd
from google.oauth2 import service_account
from google.cloud import bigquery
from google.cloud import storage
from datetime import datetime
import requests
import time
from random import randint
from random import random
import smtplib
import numpy as np
from datetime import datetime
from datetime import timedelta
from xml.etree import ElementTree
import os


# Build flask app
app = Flask(__name__)

# Enable the API endpoint to accept CORS requests.
CORS(app)


@app.route('/', methods=['GET'])
def root():
    """
    This function runs the code for the WS API.
    """
    
    # First I define the API call from our table
    Brand_Producer = 'Laurent-Perrier'
    Name = ['Laurent-Perrier Brut Millesime', 'Laurent-Perrier Cuvee Rose Brut', 'Laurent-Perrier La Cuvee Brut', \
        'Grand Siecle par Laurent-Perrier Les Reserves Cuvee 571J','Grand Siecle par Laurent-Perrier No 22','Grand Siecle par Laurent-Perrier No 24', \
        'Grand Siecle par Laurent-Perrier', 'Laurent-Perrier Alexandra Rose','Laurent-Perrier Blanc de Blancs Brut Nature', \
        'Laurent-Perrier Harmony Demi-Sec', 'Laurent-Perrier Ultra Brut', 'Laurent-Perrier Kosher Cuvee Brut Rose', \
        'Grand Siecle par Laurent-Perrier Les Reserves No 17','Laurent-Perrier Cuvee Rose Brut with Glasses',\
        'Laurent-Perrier Kosher Brut','Laurent-Perrier Robe Edition Cuvee Brut Rose']
    Vintage = list(str(range(1969, 2019)))
    Format = 'All format'
    OfferDate = ['22-01-2021','21-01-2021','20-01-2021']
    key = '***'

    #Defining a dataframe with the previous information
    df = pd.DataFrame({'Brand_Producer': Brand_Producer, 'Name':Name, 'Vintage':Vintage, 'Dosage':Dosage, 'Format':Format, 'OfferDate':OfferDate})

    #Now I need to import the urls that I have already called on previous days
    if os.path.isfile(os.path.join('tmp','url.csv')):
        filename = '/tmp/'+'url.csv'
        df_url_ = pd.read_csv(filename, sep=';')
        url_used = []
        #and I fill the list 'url_used' with the url that we have called
        for i in range(df_url_.shape[0]):
            url_used.append(df_url_['url'].iloc[i])
    
    #if there is no such file, it means I have not called any url yet!
    else: 
        url_used = []   
    
    #now I create a list where I'll store all the url I call this time
    url = []



    # I generate the URLs to call the API 
    count = 0
    for item in Name:
        for item2 in Vintage:
            for item3 in OfferDate:
                url_ = 'https://api.wine-searcher.com/x?api_key=' + key + '&winename=' + item + '&vintage=' + item2 + '&autoexpand=' + 'Y' + '&OfferDate=' + item3
                #I check whether this url has already been invoked
                if url_ in url_used:
                    continue
                #If not, I carry on and put it in the list of 'url to be called' --> 'url'
                else:
                    url.append(url_)
                    count = count + 1
                    url_used.append(url_)
                    #And I stop it when we have 500 calls in the url list
                    if count == 500:
                        break
    
    #Function to read the files from API
    def json_data(file_name):
        with open(file_name) as f:
            json_data = json.loads(json.dumps(ast.literal_eval(f.read())))
        return json_data
    
    #Function to create a DataFrame with results from API Market
    def extract_info_MARKET_api(info_json):

        counts = int(info_json['wine-searcher']['list-count'])

        for i in range(counts):
            df_aux = pd.DataFrame({
            'ReturnCode': [info_json['wine-searcher']['return-code']],
            'Comment': [info_json['wine-searcher']['list-comment']],
            'Location': [info_json['wine-searcher']['list-location']],
            'State': [info_json['wine-searcher']['list-state']],
            'CurrencyCode': [info_json['wine-searcher']['list-currency-code']],
            'Count': [info_json['wine-searcher']['list-count']],
            'MerchantName': [info_json['wine-searcher']['prices-and-stores']['store'][i]['merchant-name']],
            'MerchantDescription': [info_json['wine-searcher']['prices-and-stores']['store'][i]['merchant-description']],
            'PhysicalAddress': [info_json['wine-searcher']['prices-and-stores']['store'][i]['physical-address']],
            'ZipCode': [info_json['wine-searcher']['prices-and-stores']['store'][i]['zip-code']],
            'Latitude': [info_json['wine-searcher']['prices-and-stores']['store'][i]['latitude']],
            'Longitude': [info_json['wine-searcher']['prices-and-stores']['store'][i]['longitude']],
            'Country': [info_json['wine-searcher']['prices-and-stores']['store'][i]['country']],
            'State': [info_json['wine-searcher']['prices-and-stores']['store'][i]['state']],
            'Vintage': [info_json['wine-searcher']['prices-and-stores']['store'][i]['vintage']],
            'Price': [info_json['wine-searcher']['prices-and-stores']['store'][i]['price']],
            'BottleSize': [info_json['wine-searcher']['prices-and-stores']['store'][i]['bottle-size']],
            'Link': [info_json['wine-searcher']['prices-and-stores']['store'][i]['link']]
            })

            if i == 0:
                df = df_aux
            else:
                df = df.append(df_aux)

            return df

    # Now we can start calling the API with the 500 urls that I have stored in 'url'

    for i in range(500):

        #we call the api
        response = requests.get(url[i])
        info_text = json.dumps(xmltodict.parse(response.content))
        info_json = json.loads(info_text)

        #we can read the files from the API
        info_json2 = json_data(info_json)

        #and turn them into a dataframe
        df_Market = extract_info_MARKET_api(info_json2)

        if i == 0:
            df_final = df_Market
        else:
            df_final = df_Market.append(df_Market)


        
    #now we have the dataframe ready to be exported with the info corresponding to the 500 calls 
    time1 = datetime.now().strftime("%Y%m%d-%H%M%S")
    name_file = 'ApiMarketWS_'+time1+'.csv'
    loc_namefile = '/tmp/'+name_file 

    df_final.to_csv(loc_namefile, index=False, header = False, sep = ';')

    # To upload the text file to Google Storage, we firstly define a function
    def upload_to_bucket(bucket_name, blob_path, local_path):
        bucket = storage.Client().bucket(bucket_name)
        blob = bucket.blob(blob_path)
        blob.upload_from_filename(local_path)
        return blob.public_url 

    # Specify the parameters we are going to use
    bucket_name = "iwine-ws"
    blob_path = name_file
    local_path = loc_namefile

    #And we upload it to GCS and remove the text file form the APPengine memory
    upload_to_bucket(bucket_name, blob_path, local_path)
    os.remove(loc_namefile)

    #appending new urls
    if os.path.isfile(os.path.join('tmp','url.csv')):
        filename = '/tmp/'+'url.csv'
        with open(filename, 'a') as the_file:
            for row in url_used:
                the_file.write(row)
    else:
        url_df = pd.DataFrame({'url':url})
        filename = '/tmp/'+'url.csv'
        url_df.to_csv(filename, index=False, header = False, sep = ';')


    return '', http_client.OK


if __name__ == '__main__':
    # This is used when running locally. Gunicorn is used to run the
    # application on Google App Engine. See entrypoint in app.yaml.
    app.run(host='127.0.0.1', port=8080, debug=False)