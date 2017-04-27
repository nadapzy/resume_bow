# -*- coding: utf-8 -*-
"""
Created on Fri Mar 24 14:30:27 2017

@author: zpeng
"""

from os import listdir
from os.path import isfile, join
import pandas as pd

import os
path=os.path.dirname(os.path.abspath(__file__))
#get parent folder
par_folder=os.path.dirname(path)   

files=[f for f in listdir(path)]

mapping=pd.read_csv('IBMBRASSRING_Job_Applications_20170309.txt',sep='|')

file_name=mapping.RESUMEKEY.astype(str)
file_name='R_'+file_name+'.txt'

set_file_from_map=set(file_name)
set_dir_files=set(files)
set_read_files=set_file_from_map.intersection(set_dir_files)

index=[]
resume_raw=[]
for file in set_read_files:
    with open(file,'r') as f:
        read_data=f.readlines()
    #mapping.loc[int(file[2:-4]),'resume_raw']=read_data
    index.append(int(file[2:-4]))
    resume_raw.append(' '.join(read_data))

from sklearn.feature_extraction.text import TfidfVectorizer
import re,nltk

#using porter stemming for words
from nltk.stem.porter import PorterStemmer
token_dict = {}
stemmer = PorterStemmer()

def stem_tokens(tokens, stemmer):
    stemmed = []
    for item in tokens:
        stemmed.append(stemmer.stem(item))
    return stemmed

def tokenize(text):
    tokens = nltk.word_tokenize(text)
    stems = stem_tokens(tokens, stemmer)
    return stems

def preprocessor(desc):
    return re.sub("[^a-zA-Z]"," ", desc.lower())
vectorizer = TfidfVectorizer(analyzer = "word",   \
                             tokenizer = tokenize,lowercase=True,    \
                             preprocessor = None, \
                             stop_words =None,\
                             max_features = None,\
                             ngram_range=(1,2),min_df=5\
                             ,decode_error='ignore',sublinear_tf=True)   
                             #potential set binary=True
                            #potentially set preprocessor to remove all numbers                              
                            #stop words list:  list(stopwords.words("english"))  

resume_bows = vectorizer.fit_transform(resume_raw)
vocab=vectorizer.get_feature_names()
print('size of vocab',len(vocab))
print(vocab)
abc()
#test=[v for v in vocab if re.match(v,r'[^a-zA-Z]')]   # test how many of the vocabulary has non-letters.

#df_vocab=pd.DataFrame(data=resume_bows.toarray(),columns=['resume_'+word.strip() for word in vocab],index=index)

from sklearn.decomposition import TruncatedSVD

seed=25
svd=TruncatedSVD(n_components=int(resume_bows.shape[1]*0.8),random_state=seed) #
svd.fit(resume_bows)
red_resume_bows=svd.transform(resume_bows)
print(svd.explained_variance_ratio_.sum())

df_vocab=pd.DataFrame(data=red_resume_bows,columns=['svd'+str(i) for i in range(red_resume_bows.shape[1])],index=index)

# import GRS file, the export from GRS system, please note that we resaved the file in standard csv format after dowloading
import codecs
grs_file=par_folder+'\\All Field Resume_28Mar_1846-v1.csv'
with codecs.open(grs_file,'rb') as f: #,'utf-16'
    grs_front=pd.read_csv(f,quoting=0) #encoding='utf-16',
    
grs_front['job_code'],grs_front['job_desc']=grs_front['Job Code'].str.split(pat=':').str
grs_front.rename(columns={'Req ID':'req_id','WIN Number [WIN]':'win_nbr','BRUID':'bruid','Candidate ref num':'can_ref_num'},inplace=True)
grs_front=grs_front.loc[:,['req_id','bruid','can_ref_num','job_code','win_nbr']]

mapping_grs=mapping.merge(grs_front,how='inner',left_on=['BRUID','AUTOREQ'],right_on=['bruid','req_id'])

mapping_grs.set_index(['RESUMEKEY'],inplace=True)

df_vocab_map=df_vocab.merge(mapping_grs,how='inner',left_index=True,right_index=True)


import pyodbc
cnxn=pyodbc.connect(r'DSN=WMB;UID=zpeng;PWD=Prepare4best;Authentication=LDAP;database=hr001_wm_ad_hoc')
from sqlalchemy import create_engine
import sqlalchemy
td_engine = create_engine('teradata://'+ 'zpeng' +':' + 'Prepare4best' + '@'+ 'wmb.wal-mart.com'+'/'+'wm_ad_hoc'+'?authentication=LDAP' ,echo=True)
#td_engine = create_engine(creator=cnxn)
conn = td_engine.connect()
mapping.to_sql(name='zpeng_resume',con=conn,index=False,dtype={'RESUMEKEY':sqlalchemy.BIGINT,'BRUID':sqlalchemy.BIGINT,'JOBCODE':sqlalchemy.NVARCHAR(length=11),'AUTOREQ':sqlalchemy.NVARCHAR(length=12)},if_exists='replace')



