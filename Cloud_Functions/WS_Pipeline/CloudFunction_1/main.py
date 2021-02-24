def startDataflowProcess(data, context):
     from googleapiclient.discovery import build
     #replace with your projectID
     project = 'iwine-tfm'
     #job = project + " " + str(data['timeCreated'])
     time1 = str(data['timeCreated'])
     time1.replace(":","-")
     time1.replace(".","-")
     job = project + "_" + time1
     #path of the dataflow template on google storage bucket
     template = 'gs://dataflow-templates-us-central1/latest/GCS_Text_to_BigQuery'
     inputFile = 'gs://iwine-ws/' + str(data['name'])
     #user defined parameters to pass to the dataflow pipeline job
     parameters = {
          'javascriptTextTransformFunctionName': 'transform',
          'javascriptTextTransformGcsPath':'gs://dataflow-wsapp/UDFunction.js', 
          'JSONPath': 'gs://dataflow-wsapp/BQschema.json',
          'inputFilePattern': inputFile,
          'outputTable': 'iwine-tfm:wine_searcher.MarketPlace_WS',
          'bigQueryLoadingTemporaryDirectory':'gs://dataflow-wsapp/BQ_temp'
     }
     #tempLocation is the path on GCS to store temp files generated during the dataflow job
     environment = {'tempLocation': 'gs://dataflow-wsapp/BQ_temp/temp'}

     service = build('dataflow', 'v1b3', cache_discovery=False)
     #below API is used when we want to pass the location of the dataflow job
     request = service.projects().locations().templates().launch(
          projectId=project,
          gcsPath=template,
          location='us-central1',
          body={
               'jobName': job,
               'parameters': parameters,
               'environment':environment
          },
     )
     response = request.execute()
     print(str(response))