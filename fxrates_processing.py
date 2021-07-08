import pandas as pd
import datetime as dt  
from datetime import datetime
from xml.dom import minidom
import ntpath
import sys

#DAILY/MONTHLY RUN  
FX_RUN_VERSION="M"
strDate="2021-06-30"

utilDir="C:/Users/patelsm/Desktop/fx_rates/GO_FXRATE_UPDATE/"
processDir="C:/Users/patelsm/Desktop/fx_rates/GO_FXRATE_UPDATE/output/"

#I/O names
currencyList=utilDir+"currency_list.txt"
glTranslationCodes=utilDir+"gl_translationcode.txt"
fileList=processDir+"filelist.txt"
dailyOutput=processDir+"APWeeklyRates.csv"
monthlyOutput=processDir+"GLExchangeRate.csv"
quarterlyOutput=processDir+"GLTranslationRateInterface.csv"
err_CAD=processDir+"missingCAD.txt"
err_USD=processDir+"missingUSD.txt"

#CREATE DATE OBJ
date_obj = datetime.strptime(strDate, "%Y-%m-%d").date()
first_day_of_month = date_obj.replace(day=1)
curr_month=date_obj.month
quarterEnd = [3, 6, 9, 12]


#CURRENCY LIST
currency_chk_cdn={}
f_currlist = open(currencyList, "r")
currlist_content = f_currlist.read()
content_list = currlist_content.split(",")

for line in content_list:    
    currency_chk_cdn[line.strip()]=False
f_currlist.close()

currency_chk_usd=currency_chk_cdn.copy()

#GL TRANSLATIONCODES
gl_translationcodes=[]
f_gltrancodes = open(glTranslationCodes, "r")
gltranscode_content = f_gltrancodes.read()
content_list = gltranscode_content.split(",")
for line in content_list:    
    gl_translationcodes.append(line.strip())
f_gltrancodes.close()

def main():
    appended_data = []

    #read the list of inputfiles
    with open(fileList) as f:
        lines = f.read().splitlines()
    
    #process each file and append to final dataframe
    for x in range(len(lines)):
        appended_data.append(process(lines[x]))

    appended_data = pd.concat(appended_data, ignore_index = True)

    #Check for missing Rates
    missingRates()

    if(True):    
        appended_data.columns = ['from_currency','to_currency','date','daily_fx','3m_average_fx','triangulate_flag']

        new_df = currencyTriangualate(appended_data)     
        
        #CREATE DAILY EXTRACT
        dailymonthly(new_df[['from_currency','to_currency','date','daily_fx']],"D")
        

        #CREATE MONTHLY/QUARTERY EXTRACT
        if(FX_RUN_VERSION=="M"):
            dailymonthly(new_df[['from_currency','to_currency','date','daily_fx']],FX_RUN_VERSION)
            if(curr_month in quarterEnd):        
                quarterlyExtract(new_df[['from_currency','to_currency','date','daily_fx','3m_average_fx']],gl_translationcodes) 
    else:
        print("Missing Currencies")        
   
def process(file_name):  
    
    tree = minidom.parse(file_name)
    spot_rate_type="BPL_CLASSIFIER_II"
    quaterly_rate_type="ML_QUARTER_AVG_RATE"    
    items = tree.getElementsByTagName('Price')
    
    list = []
    try:
        for item in items:       
            BPL_type = item.getElementsByTagName(spot_rate_type)
            BPL_value = BPL_type[0].firstChild.nodeValue
            if (BPL_value == "SPX"):
                currency = item.getElementsByTagName("ML_CROSS_CURRENCY")[0].firstChild.nodeValue
                price_date = item.getElementsByTagName("ML_PRICE_DATE")[0].firstChild.nodeValue
                to_rate = float(item.getElementsByTagName("ML_RATE")[0].firstChild.nodeValue)            
                quarterly_rate=float(item.getElementsByTagName(quaterly_rate_type)[0].firstChild.nodeValue)                
                triangulate_flag=1                       
                temp_list = ["USD",currency, price_date, to_rate,quarterly_rate,triangulate_flag]
                list.append(temp_list)
                currency_chk_usd[currency]=True 
            if(BPL_value == "XFX"):            
                currency = item.getElementsByTagName("ML_CROSS_CURRENCY")[0].firstChild.nodeValue
                price_date = item.getElementsByTagName("ML_PRICE_DATE")[0].firstChild.nodeValue
                to_rate = float(item.getElementsByTagName("ML_RATE")[0].firstChild.nodeValue)            
                quarterly_rate=float(item.getElementsByTagName(quaterly_rate_type)[0].firstChild.nodeValue)                
                triangulate_flag=0                       
                temp_list = ["CAD",currency, price_date, to_rate,quarterly_rate,triangulate_flag]
                list.append(temp_list)            
                currency_chk_cdn[currency]=True
        df = pd.DataFrame(list)
        return(df)

    except IndexError as e:
        print(e)

def dailymonthly(df,runtype): 
    
    df['FinanceEnterpriseGroup']='MFC'
    df['GLExchangeRateInterface']=df.index+1
    if(runtype == 'D'):
        df['CurrencyTable']='APCB'
        f_name=dailyOutput
    else:
        df['CurrencyTable']='MFC'
        df['date']=first_day_of_month
        f_name=monthlyOutput

    df= df[['FinanceEnterpriseGroup','GLExchangeRateInterface','CurrencyTable','from_currency','to_currency','date','daily_fx']]
    df.to_csv(f_name,index=False)

def quarterlyExtract(df,glcodes):
    final_output = pd.DataFrame([])
    quarterly_avg = ['IS', 'BI']    
    for glcode in glcodes:        
        temp_df2 = pd.DataFrame()
        if(glcode in quarterly_avg):                          
            temp_df2['3m_average_fx']=df['3m_average_fx']            
        else:            
            temp_df2['3m_average_fx']=df['daily_fx']            
        temp_df2['from_currency']=df['from_currency']
        temp_df2['to_currency']=df['to_currency']
        temp_df2['date']=df['date']                               
        temp_df2['GeneralLedgerTranslationCode']=glcode                          
        
        final_output=pd.concat([final_output,temp_df2])          
    
    final_output.drop( final_output[ final_output['from_currency'] == final_output['to_currency'] ].index, inplace=True)
    final_output['FinanceEnterpriseGroup']='MFC'
    final_output['GLTranslationRateInterface']=final_output.index+1 
    final_output= final_output[['FinanceEnterpriseGroup','GLTranslationRateInterface','GeneralLedgerTranslationCode','from_currency','to_currency','date','3m_average_fx']]
    final_output.to_csv(quarterlyOutput,index=False)

def missingRates():
    missingList_usd = dict(filter(lambda elem: elem[1] == False, currency_chk_usd.items()))
    missingList_cdn = dict(filter(lambda elem: elem[1] == False, currency_chk_cdn.items()))
    if not missingList_usd and not missingList_cdn:
        
        return True
    else:
        print("USD Rates missing")
        print(missingList_usd)
        print("CAD Rates missing")
        print(missingList_cdn)
        return False
 
def currencyTriangualate(df):
    temp_df = pd.DataFrame()
    temp_df['to_currency']=df['from_currency']    
    temp_df['from_currency']=df['to_currency']    
    temp_df['date']=df['date']    
    temp_df['daily_fx']=df['daily_fx'].apply(lambda x: 1/x)
    temp_df['3m_average_fx']=df['3m_average_fx'].apply(lambda x: 1/x)
    final_output=df.append(temp_df,ignore_index=True)
    final_output.drop( final_output[ final_output['triangulate_flag'] == 0 ].index, inplace=True) 
    
    return(final_output)

main()