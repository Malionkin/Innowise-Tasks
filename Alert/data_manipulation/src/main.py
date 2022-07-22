import pandas as pd
from data_manip_tasks import tasks as t

data = pd.read_csv('data.csv')

t.log_alert_10_errors_in_a_minute(data)
t.log_alert_for_bundle_id(data)







