import base64
from io import BytesIO, IOBase
from os import close
from typing import final
from urllib import parse
from django.shortcuts import render
from django.http import HttpResponse, response
from django.http import HttpRequest
from numpy import load
import pandas as pd
from pandas.core.frame import DataFrame
from sqlalchemy.sql.expression import bindparam
from . import DM as dm
from sqlalchemy import create_engine
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from sqlalchemy import text
import multiprocessing
import json
import pymysql

#after first install
#- set the database and tables

#url format:
#http://localhost:8000/covid19dkiapi/?start=20 5 2021&end=30 5 2021&status=get

# Create your views here.

#link to target website
src_url="https://riwayat-file-covid-19-dki-jakarta-jakartagis.hub.arcgis.com/"

#create the database connection
engine = create_engine('mysql+pymysql://root:@localhost/covid19-dki-kcm')

#test function
def abc(request):
    opts = Options()
    opts.set_headless()
    assert opts.headless
    opts.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.77 Safari/537.36")
    browser = webdriver.Chrome(executable_path = "C:\djangoDMWeb\covid-19-project\covid19dkiapi/chromedriver.exe", options = opts)
    #give time to wait until the element is loaded
    browser.implicitly_wait(60)

    #get webpage from url source
    browser.get(src_url)
    return response.HttpResponse("it works !!")

#funciton will be call from django
def index(request):
    #open the headless browser
    #set up the headless browser
    opts = Options()
    opts.set_headless()
    assert opts.headless
    opts.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.77 Safari/537.36")
    browser = webdriver.Chrome(executable_path = "chromedriver.exe", options = opts)
    #give time to wait until the element is loaded
    browser.implicitly_wait(60)

    #get webpage from url source
    browser.get(src_url)
    final_data=DataFrame()
    r=dm.range_date(request.GET['start'],request.GET['end'])
    stat_flag=request.GET['status']
    #status explanation:
    #normal: download and clean data, update one if exists. create model. create graphic
    #model: update model on existing data, download and clean data, create model if data not exists
    #graphic: update graphic on existing data, download and clean data, create model if data not exists
    #get: get avaialable data, download and clean data, create model if data not exists
    for sd in r:
        exists=True
        #if user update data at certain range of date, update all of them
        #if not, dont update, add date that is not exist
        if((stat_flag=="normal") or (stat_flag=="model") or(stat_flag=="graphic") or (stat_flag=="get")):
            exists=dm.date_check(engine,sd)
            #if exists and not "normal" then fetch data from DB
            if((exists==True) and (stat_flag!="normal")):
                exec=text("SELECT * FROM `covid19-dki-kecamatan` WHERE tanggal=:dates")
                exec=exec.bindparams(dates=sd)
                df=pd.read_sql_query(exec,engine,index_col='index')
        #if data not exists then download data
        if((stat_flag=="normal") or (exists==False)):
            #load data from web
            df=dm.load_web(sd,browser)
            #return data or "not exist"
            if(df.empty):
                #close the browser
                browser.close()
                return response.HttpResponseNotFound("no such data exists")
            #clean the data
            df=dm.clean_data(df)
        print(sd)
        print(exists)
        if((stat_flag=="normal") or (stat_flag=="model") or (exists==False)):
            #cluster it with K-Means
            df=dm.create_clus(df,sd)
        if((stat_flag=="normal") or (stat_flag=="model") or (stat_flag=="graphic") or (exists==False)):
            #add image plot
            p=multiprocessing.Pool(1)
            a=[BytesIO().getvalue()]*43
            pair=[df,sd]
            c=p.map(dm.draw_plot,[pair])
            p.terminate()
            p.join()
            p.close()
            b=c[0][0]
            print(len(b))
            a[0]=b[0]
            a[1]=b[1]
            a[2]=b[2]
            a[3]=b[3]
            a[4]=b[4]
            a[5]=b[5]
            if((exists==True) and (stat_flag!="normal")):
                df.drop("image",axis=1)
                df["image"]=a
            else:
                df["image"]=a
        if((exists==False)):
            #add to the Database
            Table='covid19-dki-kecamatan'
            df.to_sql(Table,con=engine,if_exists='append')
        if((exists==True) and (stat_flag!="get")):
            #update the database
            dm.upd_sql(df,engine,sd)

        #encode to base64 for JSON delivery
        df.loc[1,'image']=base64.b64encode(df.loc[1,'image'])
        df.loc[2,'image']=base64.b64encode(df.loc[2,'image'])
        df.loc[3,'image']=base64.b64encode(df.loc[3,'image'])
        df.loc[4,'image']=base64.b64encode(df.loc[4,'image'])
        df.loc[5,'image']=base64.b64encode(df.loc[5,'image'])
        df.loc[6,'image']=base64.b64encode(df.loc[6,'image'])
        #if the final_data is "first" then get a copy from first date
        if(final_data.empty):
            final_data=df
        else:
            final_data=final_data.append(df,ignore_index=True)

    #close browser
    browser.close()
    result=final_data.to_json(orient="columns")
    parsed=json.loads(result)
    return response.JsonResponse(parsed,safe=False)

#note: set each image, no subplot