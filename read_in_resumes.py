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
folder_names=['20170414_1','20170414_2','20170414_3','20170414_4','20170414_5','20170414_6']
folders=[os.path.join(path,folder_name) for folder_name in folder_names ]
#par_folder=os.path.dirname(path)
#abc()

def read_mapping(path,folders):
    mapping=[]
    mapping.append(pd.read_csv(os.path.join(path,'WALMART_JobApplications_Index.txt'),sep='|'))
    mapping=pd.concat(mapping)
    return mapping  

mapping=read_mapping(path,folders)

# -----------------------------------1-------------------------------
# import GRS file, the export from GRS system, please note that we resaved the file in standard csv format after dowloading
import codecs
grs_file=path+'\\All Field Resume_28Mar_1846-v2.xlsx'
with codecs.open(grs_file,'rb') as f: #,'utf-16'
    grs_front=pd.read_excel(f,sheetname='All Field Resume_28Mar_1846-v1') #encoding='utf-16',
    
#grs_front['job_code'],grs_front['job_desc']=grs_front['Job Code'].str.split(pat=':').str
grs_front.rename(columns={'Req ID':'req_id','WIN Number [WIN]':'win_nbr','BRUID':'bruid','Candidate ref num':'can_ref_num'},inplace=True)
grs_front=grs_front.loc[:,['req_id','bruid','can_ref_num','job_code','win_nbr','job_family']]
grs_front=grs_front[grs_front.job_family=='SM']

mapping_grs=mapping.merge(grs_front,how='inner',left_on=['BRUID','AUTOREQ'],right_on=['bruid','req_id'])


# -----------------------------------2-------------------------------
# import all resumes 
import collections

files=collections.defaultdict(str)

def read_resume(folders):
    for folder in folders:    
        for f in listdir(folder):
            files[f]=folder[-1]
    return files
    
files=read_resume(folders)  

file_name=mapping.RESUMEKEY.astype(str)
file_name='R_'+file_name+'.txt'

set_file_from_map=set(file_name)
set_dir_files=set(files.keys())
set_read_files=set_file_from_map.intersection(set_dir_files)

#abc()

index=[]
resume_raw=[]
folder_pref=folder_names[0][:-1]
for file in set_read_files:
    if len(index)%1000==0:
        print('processing #',len(index),'out of',len(set_read_files),'resumes')
    file_path=os.path.join(path,folder_pref+files[file],file)
    with open(file_path,'r') as f:
        read_data=f.readlines()
    #mapping.loc[int(file[2:-4]),'resume_raw']=read_data
    index.append(int(file[2:-4]))
    resume_raw.append(' '.join(read_data))

from sklearn.feature_extraction.text import TfidfVectorizer
import re

def preprocessor(desc):
    return re.sub("[^a-zA-Z]"," ", desc.lower())
vectorizer = TfidfVectorizer(analyzer = "word",   \
                             tokenizer = None,lowercase=True,    \
                             preprocessor = None, \
                             stop_words =None,\
                             max_features = None,\
                             ngram_range=(1,1),min_df=5\
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



mapping_grs.set_index(['RESUMEKEY'],inplace=True)

df_vocab_map=df_vocab.merge(mapping_grs,how='inner',left_index=True,right_index=True)

import pyodbc
cnxn=pyodbc.connect('DSN=GM2P;UID=userid;pwd=pwd')

sql='''
SELECT 
THD_CLNT_RQSTN_ID,
g.candidate_id,
g.thd_candidate_id,
--TRIM (G.first_name),
--TRIM (G.last_name),
CASE WHEN g.ncrypt_ssn_nbr is not NULL THEN wmpsysa.id_crypt('D',g.ncrypt_ssn_nbr)
ELSE NULL END AS ssn_nbr
,g.win_nbr,
C.RQSTN_STATUS_DESC AS CURRENT_REQ_STATUS,
 E.RQSTN_PAY_DESC AS POSITION_TYPE,
F.CHRG_DEPT_NBR, 
f.store_nbr,
B.JOB_TITLE_DESC,
f.WRK_DEPT_NAME AS Category,
DATE(B.CLOSED_TS) AS date_closed,
 D.STATUS_TYPE_DESC AS hr_status,
 CASE WHEN a.status_type_code IN (274,156,104,37,370,366,306,227,343,169,275,152,147,236,118,178,356,36,85,251,398,232,388) THEN 1
 ELSE 0 END AS hired,
DATE(A.STATUS_UPDATE_TS) AS hr_status_dt
FROM 
DPHRATS.ATS_CND_RQSTN_STAT A,
DPHRATS.ATS_REQUISITION B,
DPHRATS.ATS_RQSTN_STAT_TXT C,
DPHRATS.ATS_STATUS_TXT D,  --d is for c.status_type_code
DPHRATS.ATS_RQSTN_PAY_TXT E,
DPHRATS. ATS_RQSTN_STR_ALGN F,  --current store alignment of stores
DPHRATS.ATS_CANDIDATE G   --candidate info
WHERE 
A.REQUISITION_ID = B.REQUISITION_ID
AND B.REQUISITION_ID = F.REQUISITION_ID
AND A.CANDIDATE_ID = G.CANDIDATE_ID
AND A.STATUS_TYPE_CODE = D.STATUS_TYPE_CODE
AND B.RQSTN_STATUS_CODE = C.RQSTN_STATUS_CODE
AND B.RQSTN_PAY_TYPE_CD = E.RQSTN_PAY_TYPE_CD

AND DATE(A.STATUS_UPDATE_TS) BETWEEN '2015-02-01' AND '2017-02-01'
AND b.wmt_job_code IN ('100014694','100014691','100014695','100000368','100000369','100008271','100008252','100014437','100008253','100008270','100014692','100014693','100014696','100008255','100000382','100008257','100000381','100014697','100008258','100000387','100008254','100000388','100014698','100014699','100008256','100000383','100014700','100014705','100009441','100013587','100014704','100014754','100014703','100014702','100000396','100014701','100008263','100012500','100014833','100014832','100014831','100000414','100008437','100000408','100009140','100014757','100000409','100014712','100008265','100014713','100014711','100000392','100000360','100008259','100000394','100014710','100000395','100014714','100008251','100008261','100014708','100014756','100014755','100000393','100008262','100008260','100008264','100006678','100014709')
WITH UR;
'''

grs_back=pd.read_sql(sql,cnxn,parse_dates=True)
# check whether we can find any can_ref_num in grs_back.thd_candidate_id, using .isin()
df_vocab_hire=df_vocab_map.merge(grs_back,how='inner',left=['can_ref_num','AUTOREQ'],right=['THD_CANDIDATE_ID','THD_CLNT_RQSTN_ID'])


