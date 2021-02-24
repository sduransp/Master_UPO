Esta carpeta se divide, a su vez, en tres carpetas diferentes:

### Cloud Function 1
Cloud Function encargada de crear un DataFlow que procese y almacene la información de la API de Twitter (archivo de texto) en BigQuery

### Cloud Function 2
Cloud Function encargada de filtrar y analizar la información procedente de la API de Twitter, así como almacenar el resultado de dichos análisis de un bucket de GCS

### Cloud Function 3
Cloud Function encargada de crear un DataFlow que procese y almacene el archivo generado por la Cloud Function 2 en BigQuery.
