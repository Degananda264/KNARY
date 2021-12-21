# -*- coding: utf-8 -*-
"""
Created on Tue May 12 18:18:47 2020

@author: degananda.reddy
"""

from flask import Flask,request, url_for, redirect, render_template, jsonify
from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search,Q
from elasticsearch_dsl.query import MoreLikeThis
from datetime import timedelta, date
import pandas as pd
import json
import re
import datetime
import numpy as np

app = Flask(__name__)

es = Elasticsearch([{'host': 'localhost', 'port': 9200}])


def search(expiry_in_days,contract_party,adrt):
    s = Search(using=es, index='k-nary')
    if expiry_in_days!="":
        Date_req = date.today() + timedelta(days=expiry_in_days)
        s=s.query(Q('range', Expiration_Date={"gte": "now","lte":Date_req}))
    else:
        pass
    if contract_party!="":
        s=s.filter('multi_match', query=contract_party ,fields=['Party_1_Name', 'Party_2_Name'],minimum_should_match="100%")
    else:
        pass
    if adrt!="":
        s=s.filter("match",Alternate_Dispute_Resolution_Terms=adrt)
    else:
        pass
    s=s[0:s.count()]
    response = s.execute()
    total_contracts=s.count()
    df=pd.DataFrame(columns=['contract_id','contract_party1','contract_party2','Expiration_date','Alternate_Dispute_Resolution_Terms'])
    for hit in response:
        contract_id=hit.contract_id
        #filename=hit.file_name
        contract_party1=hit.Party_1_Name
        contract_party2=hit.Party_2_Name
        expirydate=hit.Expiration_Date
        Alternate_Dispute_Resolution_Terms=hit.Alternate_Dispute_Resolution_Terms
        df.loc[len(df)]=[contract_id,contract_party1,contract_party2,expirydate,Alternate_Dispute_Resolution_Terms]
    return df,total_contracts
        
        

@app.route('/')     
def home():
    return render_template("test.html")

@app.route('/search',methods=['POST'])
def predict():
    values = [x for x in request.form.values()]
    print(values)
    print(type(values[0]))
    if values[0]=='':
        out,total_contracts=search('',values[1],values[2])
    else:
        out,total_contracts=search(int(values[0]),values[1],values[2])
    #print(out)
    return render_template('search.html',pred=out,pred2='Total contracts :{}'.format(total_contracts))


if __name__ == '__main__':
    app.run(host='localhost', port=9000)