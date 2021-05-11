import pandas as pd 
from xml.dom import minidom
import ntpath
import sys

#DAILY/MONTHLY RUN  
FX_RUN_VERSION="D"
DATE="20210506"


#CURRENCY LIST
currency_chk_cdn={}
f_currlist = open("C:/Users/patelsm/Desktop/fx_rates/GO_FXRATE_UPDATE/currency_list.txt", "r")
currlist_content = f_currlist.read()
content_list = currlist_content.split(",")

for line in content_list:    
    currency_chk_cdn[line.strip()]=False
f_currlist.close()

currency_chk_usd=currency_chk_cdn.copy()

#GL TRANSLATIONCODES
gl_translationcodes=[]
f_gltrancodes = open("C:/Users/patelsm/Desktop/fx_rates/GO_FXRATE_UPDATE/gl_translationcode.txt", "r")
gltranscode_content = f_gltrancodes.read()
content_list = gltranscode_content.split(",")
for line in content_list:    
    gl_translationcodes.append(line.strip())
f_gltrancodes.close()

def main():
    appended_data = []

    #read the list of inputfiles
    with open('C:/Users/patelsm/Desktop/fx_rates/inbound/filelist.txt') as f:
        lines = f.read().splitlines()
    
    #process each file and append to final dataframe
    for x in range(len(lines)):
        appended_data.append(process(lines[x]))

    appended_data = pd.concat(appended_data, ignore_index = True)
    appended_data.columns = ['from_currency','to_currency','date','daily_fx','3m_average_fx','triangulate_flag']

    #CREATE DAILY EXTRACT
    dailymonthly(appended_data[['from_currency','to_currency','date','daily_fx','triangulate_flag']],"D")

    #CREATE MONTHLY/QUARTERY EXTRACT
    if(FX_RUN_VERSION=="M"):
        dailymonthly(appended_data[['from_currency','to_currency','date','daily_fx','triangulate_flag']],FX_RUN_VERSION)
    if(True):
        print("this is quarterly") 
        quarterlyExtract(appended_data[['from_currency','to_currency','date','3m_average_fx','triangulate_flag']],gl_translationcodes)        
           
    
    #Missing Rates
    missingRates()


def process(file_name):
    tree = minidom.parse(file_name)
    spot_rate_type="BPL_CLASSIFIER_II"
    quaterly_rate_type="ML_3M_AVG_PRIOR_RATE"
    seqnum=0
    items = tree.getElementsByTagName('Price')
    
    list = []
    for item in items:       
        BPL_type = item.getElementsByTagName(spot_rate_type)
        BPL_value = BPL_type[0].firstChild.nodeValue
        if (BPL_value == "SPX"):
            currency = item.getElementsByTagName("ML_CROSS_CURRENCY")[0].firstChild.nodeValue
            price_date = item.getElementsByTagName("ML_PRICE_DATE")[0].firstChild.nodeValue
            to_rate = float(item.getElementsByTagName("ML_RATE")[0].firstChild.nodeValue)
            quarterly_rate=1
            #quarterly_rate=float(item.getElementsByTagName(quaterly_rate_type)[0].firstChild.nodeValue)
            triangulate_flag=1                       
            temp_list = ["USD",currency, price_date, to_rate,quarterly_rate,triangulate_flag]
            list.append(temp_list)
            currency_chk_usd[currency]=True 
        if(BPL_value == "XFX"):            
            currency = item.getElementsByTagName("ML_CROSS_CURRENCY")[0].firstChild.nodeValue
            price_date = item.getElementsByTagName("ML_PRICE_DATE")[0].firstChild.nodeValue
            to_rate = float(item.getElementsByTagName("ML_RATE")[0].firstChild.nodeValue)
            quarterly_rate=1
            #quarterly_rate=float(item.getElementsByTagName(quaterly_rate_type)[0].firstChild.nodeValue)
            triangulate_flag=0                       
            temp_list = ["CAD",currency, price_date, to_rate,quarterly_rate,triangulate_flag]
            list.append(temp_list)            
            currency_chk_cdn[currency]=True
    df = pd.DataFrame(list)
    
    return(df)          

def dailymonthly(df,runtype):
   
    #triangulate all to usd and all to cad
    temp_df = pd.DataFrame()
    temp_df['to_currency']=df['from_currency']    
    temp_df['from_currency']=df['to_currency']    
    temp_df['date']=df['date']    
    temp_df['daily_fx']=df['daily_fx'].apply(lambda x: 1/x)     
    
    
    temp_df.columns.name = None
    final_output=df.append(temp_df,ignore_index=True)
    #drop cad to all, data not needed
    final_output.drop( final_output[ final_output['triangulate_flag'] == 0 ].index, inplace=True)    
    final_output['FinanceEnterpriseGroup']='MFC'
    final_output['GLExchangeRateInterface']=final_output.index+1
    if(runtype == 'D'):
        final_output['CurrencyTable']='APCB'
        filename="C:/Users/patelsm/Desktop/fx_rates/GO_FXRATE_UPDATE/daily.txt"
    else:
        final_output['CurrencyTable']='MFC'
        filename="C:/Users/patelsm/Desktop/fx_rates/GO_FXRATE_UPDATE/monthly.txt"

    #temporary fix to remove null rows
    nan_value = float("NaN")
    final_output.replace("", nan_value, inplace=True)
    final_output.dropna(subset = ["from_currency"], inplace=True)   

    final_output= final_output[['FinanceEnterpriseGroup','GLExchangeRateInterface','CurrencyTable','from_currency','to_currency','date','daily_fx']]
    final_output.to_csv(filename,index=False)

def quarterlyExtract(df,glcodes):
    final_output = pd.DataFrame([])

    for glcode in glcodes:
        temp_df1=df
        temp_df2 = pd.DataFrame()
        temp_df2['to_currency']=temp_df1['from_currency']    
        temp_df2['from_currency']=temp_df1['to_currency']    
        temp_df2['date']=temp_df1['date']    
        temp_df2['3m_average_fx']=1                    
        temp_df1['GeneralLedgerTranslationCode']=glcode
        temp_df2['GeneralLedgerTranslationCode']=glcode                      
        temp_df1 = pd.concat([temp_df1,temp_df2])
        final_output=pd.concat([final_output,temp_df1])  


    final_output['FinanceEnterpriseGroup']='MFC'
    final_output['GLTranslationRateInterface']=final_output.index+1 
    #print(df)
    final_output= final_output[['FinanceEnterpriseGroup','GLTranslationRateInterface','GeneralLedgerTranslationCode','from_currency','to_currency','date','3m_average_fx']]
    final_output.to_csv("C:/Users/patelsm/Desktop/fx_rates/GO_FXRATE_UPDATE/quarterly.txt",index=False)


def missingRates():
    missingList_usd = dict(filter(lambda elem: elem[1] == False, currency_chk_usd.items()))
    missingList_cdn = dict(filter(lambda elem: elem[1] == False, currency_chk_cdn.items()))
    print(missingList_usd.keys()) 
    print(missingList_cdn.keys()) 

def quarterCheck():    
    return (True)

main()