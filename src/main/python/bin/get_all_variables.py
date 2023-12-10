import os
# import pprint as pp
#
# pp.pprint(dict(os.environ)['JAVA_HOME'])

### Set Environment Variables
os.environ['envn'] = 'TEST'
os.environ['header'] = 'True'
os.environ['inferSchema'] = 'True'

### Get Environment Variables
envn = os.environ['envn']
header = os.environ['header']
inferSchema = os.environ['inferSchema']

### Set Other Variables
appName = "Movie Recommendation Analysis"
current_path = os.getcwd()
staging_movie = current_path + '\..\staging\movie'
staging_rating = current_path + '\..\staging\\ratings'