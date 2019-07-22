import requests
import json
import pickle
from datetime import datetime as dt

import joblib
from joblib import Parallel, delayed
from tqdm import tqdm
import numpy as np

header={'Accept': 'application/json, text/javascript, */*; q=0.01'
        ,'Referer': 'https://finance.daum.net/domestic/market_cap?market=KOSDAQ'
        ,'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.142 Safari/537.36'
        ,'X-Requested-With': 'XMLHttpRequest'}

def crawl_company_list(i , market='KOSPI'):
    url = f'https://finance.daum.net/api/trend/market_capitalization?page={i+1}&perPage=100&fieldName=marketCap&order=desc&market={market}&pagination=true'
    with requests.get(url, headers=header) as body:
        comlist = json.loads(body.text)['data']
    return [x for x in comlist if x['symbolCode'][0] == 'A' and x['symbolCode'][1:] in x['code']]

def crawl_company_detail(x):
    url1 = f"https://finance.daum.net/api/quotes/{x['symbolCode']}?summary=false&changeStatistics=true"
    with requests.get(url1, headers=header) as body:
        detail = json.loads(body.text)
        x.update(detail)
    return x

def crawl_company_report(company):
    url1 = f"https://wisefn.finance.daum.net/v1/company/cF1001.aspx?cmp_cd={company['symbolCode'][1:]}&finGubun=MAIN"
    with requests.get(url1, headers=header) as body:
        startidx = body.text.find("changeFinData = ")
        endidx   = body.text.rfind("Cmd_Financial();")
        resultobj = eval("".join(body.text[startidx+len("changeFinData = "):endidx-2].split("\n")) )
    tempdict = {}
    for idx, x in enumerate(resultobj):
        for idx2, x2 in enumerate(x[0]):
            tempdict[x2[0]] = [float(v.replace(',','')) if len(v)>0 else None for v in x2[1:]]
            tempdict[x2[0]].extend([float(v.replace(',','')) if len(v)>0 else None for v in x[1][idx2]])
    company.update(tempdict)
    return company

def crawl_company_stock(company):
    url2 = f"http://m.stock.naver.com/api/item/getPriceDayList.nhn?code={company['symbolCode'][1:]}&pageSize=999999"
    with requests.get(url2, headers=header) as body:
        company['stock_history'] = [(int(x['dt']), x['ncv'], x['aq']) for x in json.loads(body.text)["result"]["list"]]
    return company

#def main():
tasker_cnt = joblib.cpu_count()*8
tasker = Parallel(n_jobs=tasker_cnt, backend="threading")
company_list=[]

crawl_result1 = tasker(delayed(crawl_company_list)(x, 'KOSPI' ) for x in tqdm(range(16)))
for templist in crawl_result1:
    company_list.extend(templist)

crawl_result2 = tasker(delayed(crawl_company_list)(x, 'KOSDAQ') for x in tqdm(range(13)))
for templist in crawl_result2:
    company_list.extend(templist)

company_list = tasker(delayed(crawl_company_detail)(x) for x in tqdm(company_list))
company_list = tasker(delayed(crawl_company_report)(x) for x in tqdm(company_list))
company_list = tasker(delayed(crawl_company_stock)(x) for x in tqdm(company_list))

with open(f"company_list-{dt.now().strftime('%Y%m%d_%H%M')}.pickle", 'wb') as f:
    pickle.dump(company_list, f)

'''company_list[0]['PER(배)'],company_list[0]['PER(배)'][3],company_list[0]['ROA(%)'],company_list[0]['ROA(%)'][3]
pers = [z if z is not None else 99999. for z in[x['PER(배)'][3] for x in company_list]]
pers = [x if x >0 else 99999 for x in pers]
roas = [z if z is not None else 0. for z in [x['ROA(%)'][3] for x in company_list]]
roas = [-x if x >0 else 0 for x in roas]
print(min(pers), max(pers), min(roas), max(roas))
perrank = np.argsort(np.argsort(np.array(pers)))
roarank = np.argsort(np.argsort(np.array(roas)))
magicrank = np.argsort(np.argsort(perrank+roarank))
asdf = [company_list[r] for r in magicrank][:30]
for ddd in asdf:
    print(ddd['PER(배)'][3]+ ddd['ROA(%)'][3])'''
