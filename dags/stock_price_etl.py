import asyncio

import aiohttp
import pendulum
import numpy as np
from airflow.decorators import dag, task

API_KEY = 'AUTjJPIpZFKE3Wwausacx0jrpI6rUnZ2'
BASE_URL = 'https://api.polygon.io/v2/aggs/ticker/AAPL/range/1/day/2023-01-09/2023-01-09?adjusted=true&sort=asc&limit' \
           '=120'


@dag(
    schedule=None,
    start_date=pendulum.DateTime(2023, 1, 1),
    catchup=False,
    tags=['stock_price']
)
def stock_price_etl():

    @task()
    def extract():
        return asyncio.get_event_loop().run_until_complete(__fetch())

    async def __fetch():
        async with aiohttp.ClientSession() as session:
            url = f'{BASE_URL}&apiKey={API_KEY}'
            async with session.get(url) as resp:
                stock_prices = await resp.json()
                return stock_prices['results']

    @task(multiple_outputs=True)
    def transform(stock_prices: list):
        prices = np.array([p['h'] for p in stock_prices])

        return {
            'median': np.median(prices),
            'mean': np.mean(prices),
            'max': np.max(prices),
            'min': np.min(prices)
        }

    @task()
    def load(median: float):
        print(f'Median value prices {median}')

    price_data = extract()
    transformed_data = transform(price_data)
    load(transformed_data['median'])


stock_price_etl()
