from google.cloud import storage
import pandas as pd
import io
import csv
from io import BytesIO 
import os 
from google.cloud import language_v1
import six
from google.cloud import translate_v2 as translate
from datetime import datetime


def ML_tweet(data, context):

  #we get the bucket and file
  object_name = str(data['name'])
  bucket_name = str(data['bucket'])
  

  #instantiating the clients
  storage_client = storage.Client()
  bucket = storage_client.bucket(bucket_name)
  blob = bucket.blob(object_name)
  data = blob.download_as_string()
  client_s = language_v1.LanguageServiceClient()
  translate_client = translate.Client()
  target_language = "en"
        
  #read the upload csv file as Byte type.
  file1 = 'gs://tweets-iwine2/'+object_name
  df = pd.read_csv(file1, encoding='utf-8', sep=";")
  
  
  #The number of iterations
  nrows = df.shape[0]

  #we create some list to store all the data
  translatedTexts = []
  sentimentText = []
  keywords_ = []
  followers = []
  location = []
  likes = []
  date = []
  Tweet_Id = []

  for i in range(nrows):
    if i == 0:
        Tweet_Id.append(df.iloc[i,3])
    elif i > 0:
#for i in range(nrows):
        if Tweet_Id[i-1] == df.iloc[i,3]:
            Tweet_Id.append(df.iloc[i,3]) 
        else:
            text = df.iloc[i,7]
            text = text.replace(";"," ")
            if (df.iloc[i,8]) != "en":
                result = translate_client.translate(text, target_language=target_language)
                text = result["translatedText"]
                text = text.replace(";"," ")
            print(text)
            document = language_v1.Document(content=text, type_=language_v1.Document.Type.PLAIN_TEXT)
            sentiment = client_s.analyze_sentiment(request={'document': document}).document_sentiment
            print("Sentiment: {}, {}".format(sentiment.score, sentiment.magnitude))

            emotion = float(sentiment.score)

            #Now we fill the lists up
            Tweet_Id.append(df.iloc[i,3])
            translatedTexts.append(text)
            sentimentText.append(emotion)
            keywords_.append(df.iloc[i,2])
            followers.append(int(df.iloc[i,11]))
            location.append(df.iloc[i,9])
            likes.append(int(df.iloc[i,5]))
            date.append(df.iloc[i,4])
  df2 = pd.DataFrame({"Date":date, "keyword":keywords_, "text": translatedTexts, "sentiment": sentimentText, "likes":likes, "user_followers":followers})
  time1 = datetime.now().strftime("%Y%m%d-%H%M%S")
  name_file = 'Tweet_ml'+time1+'.csv'
  loc_namefile = '/tmp/'+name_file 
  df2.to_csv(loc_namefile, index=False, header = False, sep = ';')

  def upload_to_bucket(bucket_name, blob_path, local_path):
    bucket = storage.Client().bucket(bucket_name)
    blob = bucket.blob(blob_path)
    blob.upload_from_filename(local_path)
    return blob.public_url 

  bucket_name = 'tweets-ml'
  blob_path = name_file
  local_path = loc_namefile

  #and we call the function
  upload_to_bucket(bucket_name, blob_path, local_path)
  os.remove(loc_namefile)

  print("Done!")