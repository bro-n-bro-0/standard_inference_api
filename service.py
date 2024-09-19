from config import *
import clickhouse_connect
import aiohttp
import asyncio
import pandas as pd
from query import query

async def fetch_rank(session, particle):
    url = f"{LCD_API}/cyber/rank/v1beta1/rank/rank/{particle}"
    async with session.get(url) as response:
        result = await response.json()
        return particle, int(result['rank'])


async def fetch_all_ranks(df):
    async with aiohttp.ClientSession() as session:
        tasks = [fetch_rank(session, particle) for particle in df['particle']]
        results = await asyncio.gather(*tasks)
        for particle, rank in results:
            df.loc[df['particle'] == particle, 'rank'] = rank


async def service(particle: str) -> list:
    client = clickhouse_connect.get_client(host=DB_HOST, port=DB_PORT, user=DB_USER, password=DB_PASSWORD)
    result = client.query(query(particle)).result_set
    result = [r for r in result if r[1] > 1]
    result_df = pd.DataFrame(result, columns=['particle', 'balance'])
    await  fetch_all_ranks(result_df)
    result_df['inference'] = result_df['balance'] * result_df['rank']
    result_df.sort_values(by='inference', ascending=False, inplace=True)
    return result_df.to_dict(orient='records')