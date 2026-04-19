import pandas as pd
import requests 
from datetime import datetime
import json
from pathlib import Path
import time



Path("data").mkdir(parents=True, exist_ok=True)

params = {
    "resolution" : "quarterhour",
    "filters" : [1223, 1224, 1225, 1226, 1227, 1228, 4066, 4067, 4068, 4069, 4070, 4071, 410, 4359, 4387],
    "region" : "DE-LU"
}

def get_data(filter_id, region, resolution, timestamp):
    url = f"https://www.smard.de/app/chart_data/{filter_id}/{region}/{filter_id}_{region}_{resolution}_{timestamp}.json"
    response = requests.get(url=url)
    return response


if __name__ == "__main__":

    for f_id in params["filters"]:

        index_url = f"https://www.smard.de/app/chart_data/{f_id}/{params['region']}/index_{params['resolution']}.json"
        response_index = requests.get(index_url)

        if response_index.status_code == 200:
            timestamps = response_index.json().get('timestamps', [])

            for ts in timestamps:

                year = datetime.fromtimestamp(ts / 1000.0).year

                if year in [2023, 2024, 2025]:

                    data_response = get_data(f_id, region=params["region"], resolution=params["resolution"], timestamp=ts)

                    if data_response.status_code == 200:

                        file_name = f"data/{f_id}_{params['region']}_{ts}.json"

                        with open(file_name, 'w') as f:
                            json.dump(data_response.json(), f)
                    else:
                        print(f" Fehler beim Download von timestamp {ts}: {data_response.status_code}")
                    
                    time.sleep(0.05)
        
        else: 

            print(f" index für {f_id} konnte nicht geladen : {response_index.status_code}")
