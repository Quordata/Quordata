from pytrends.request import TrendReq
import requests

session = requests.Session()
session.get('https://trends.google.com')
cookies_map = session.cookies.get_dict()
nid_cookie = cookies_map['NID']

pytrends = TrendReq(requests_args={'headers': {'Cookie': f'NID={nid_cookie}'}})

kw_list = ['Amazon']
pytrends.build_payload(kw_list)

print(pytrends.related_topics())
