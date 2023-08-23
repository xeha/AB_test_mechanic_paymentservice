from urllib.parse import urlencode
import pandas as pd

import requests


def download_data(base_url, public_key, separ):
    # Получаем доп.таблицу с группами
 
    # получаем url
    final_url = base_url + urlencode(dict(public_key=public_key))
    response = requests.get(final_url)
    download_url_orders = response.json()['href']
    
    # загружаем файл в df
    return pd.read_csv(download_url_orders, delimiter=separ)