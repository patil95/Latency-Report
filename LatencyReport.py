import pandas as pd
import pymongo
import datetime


'''
connection mongodb to python client defines mongodb credentials.
'''
try:
    client = pymongo.MongoClient('mongodb://127.0.0.1:27017')
    print("Server connected")
except Exception as e:
    print(e)


'''
db is mongodb database name and collection is collection name.
'''
db = client.Documents
collection = db['spectaData.daily_dim_ip_latency']

'''
Using datetime library we can see the any previous date data.
'''
date = datetime.datetime.strptime((datetime.datetime.today() - datetime.timedelta(days=9)).strftime("%m/%d/%Y"),"%m/%d/%Y")
print(date)

'''
Using mongodb query collectionname.find we get the required date data.
'''
df = collection.find({'recorddate':date})

'''
Due to nested json file we flatten it by using pandas json_normalize method.
'''
df = pd.json_normalize(list(df))

'''
Using warning library we can avoid it.
'''


bucket = ["fbkt","dbkt","cbkt"]
for i in bucket:
    col_tmp = ['appname', 'destip','recorddate'] + [col for col in df if i in col ] #creating dataframe if columns present in bucket
    df_tmp = df.loc[:,col_tmp]                                                              
    df_tmp["0ms-20ms"] = df[i+".0ms-20ms"] if  i+".0ms-20ms" in col_tmp else 0           #create new columns and if value is not present it will take 0
    df_tmp["100ms-200ms"] = df[i+".100ms-200ms"] if  i+".100ms-200ms" in col_tmp else 0
    df_tmp["200ms-500ms"] = df[i+".200ms-500ms"] if  i+".200ms-500ms" in col_tmp else 0
    df_tmp["500ms-1s"] = df[i+".500ms-1s"] if  i+".500ms-1s" in col_tmp else 0
    df_tmp["50ms-100ms"] = df[i+".50ms-100ms"] if  i+".50ms-100ms" in col_tmp else 0
    df_tmp["1s-5s"] = df[i+".1s-5s"] if  i+".1s-5s" in col_tmp else 0
    df_tmp["5s-10s"] = df[i+".5s-10s"] if  i+".5s-10s" in col_tmp else 0
    df_tmp["10s above"] = df[i+".10s above"] if  i+".10s above" in col_tmp else 0
    df_tmp["20ms-50ms"] = df[i+".20ms-50ms"] if  i+".20ms-50ms" in col_tmp else 0
    df_tmp.drop([col for col in df if i in col ],inplace = True,axis=1)                  #drops old columns
    df_tmp['%_of_samples_above_500_msecs'] = ((df_tmp['500ms-1s']+df_tmp['1s-5s']+df_tmp['5s-10s']+df_tmp['10s above'])/df_tmp.sum(axis=1))*100 #create new column which store every rows percentage
    df_tmp.fillna(0)
    df_tmp = df_tmp.sort_values(by ='%_of_samples_above_500_msecs',ascending = False) #sorted ip in descending order according to sum of buckets
    df_tmp.to_csv("Latency_report_"+str(date.date())+"_"+i+".csv",index = False)      #dump into csv with date
    
    
    
