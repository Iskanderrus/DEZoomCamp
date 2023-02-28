import os
import requests

TAXI_TYPE = input('Which type of taxi to download? \n')
YEAR = int(input('For what year do you nead the data? \n'))

URL_SOURCE = 'https://d37ci6vzurychx.cloudfront.net/trip-data/'


for month in range(4, 5): 
    local_url = f'./WEEK_5/data/{TAXI_TYPE}/{str(YEAR)}/{month:02d}'

    if not os.path.exists(local_url):
        os.makedirs(local_url)

    local_file_name = f"{TAXI_TYPE}_tripdata_{YEAR}-{month:02d}.parquet"
    file_web = f'{URL_SOURCE}{local_file_name}'
    response = requests.get(file_web)
    try:
        open(f'{local_url}/{local_file_name}', "wb").write(response.content)
    except FileNotFoundError:
        print(f"File {local_file_name} not found on the server.")
    else: 
        break
