from pytrends.request import TrendReq
import requests
import pandas as pd
from collections import defaultdict
import csv

session = requests.Session()
session.get('https://trends.google.com')
cookies_map = session.cookies.get_dict()
nid_cookie = cookies_map['NID']

pytrends = TrendReq(requests_args={'headers': {'Cookie': f'NID={nid_cookie}'}})

beta_companies = pd.read_csv('../doc/beta_companies.csv').T
kw_list = beta_companies.loc['Name'].to_list()

kw_dict = defaultdict(list, {key: [] for key in kw_list})

for company in kw_list:

    name = company.replace("'", "")

    pytrends.build_payload([name])

    rising = pytrends.related_topics()[name]['rising']

    kw_dict[company] = rising['topic_title'].to_list()[:5]

with open('../doc/beta_companies_keywords.csv', 'w', newline='') as cf:
    writer = csv.writer(cf)
    writer.writerow(kw_dict.keys())
    writer.writerows(zip(*kw_dict.values()))
