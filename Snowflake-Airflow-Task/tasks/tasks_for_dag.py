import pandas as pd
import os
from dotenv import load_dotenv
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

load_dotenv()
input = os.getenv("INPUT_DATA")
connection_id = os.getenv("sf_con_id")


def parse_file():
    data = pd.read_csv(input)
    hook = SnowflakeHook(snowflake_conn_id=connection_id)
    alch = hook.get_sqlalchemy_engine()
    var = 0
    while var < len(data) // 10000:
        data.loc[var * 10000:(var + 1) * 10000].to_sql('raw_table', if_exists="append", con=alch, index=False)
        var += 1
    data.loc[var * 10000:].to_sql('raw_table', if_exists="append", con=alch, index=False)
