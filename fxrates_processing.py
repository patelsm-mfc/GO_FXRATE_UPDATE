import pandas as pd 
from xml.dom import minidom
import ntpath
import sys

#DAILY/MONTHLY RUN  
FX_RUN_VERSION="M"


#CURRENCY LIST
currency_chk={}
f_currlist = open("C:/Users/patelsm/Desktop/fx_rates/GO_FXRATE_UPDATE/currency_list.txt", "r")
currlist_content = f_currlist.read()
content_list = currlist_content.split(",")

for line in content_list:    
    currency_chk[line.strip()]=False
f_currlist.close()

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
    with open('C:/Users/patelsm/Desktop/fx_rates/inbound/print.txt') as f:
        lines = f.read().splitlines()
    
    #process each file and append to final dataframe
    for x in range(len(lines)):
        appended_data.append(process(lines[x]))

    appended_data = pd.concat(appended_data, ignore_index = True)
    appended_data.columns = ['from_currency','to_currency','date','daily_fx','3m_average_fx']


    #CREATE DAILY FILE
    daily(appended_data[['from_currency','to_currency','date','daily_fx']])

    #CREATE MONTHLY FILE

    #CREATE QUARTERLY FILE
    
    missingList = dict(filter(lambda elem: elem[1] == False, currency_chk.items()))
    print(missingList.keys()) 


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
            currency = item.getElementsByTagName("PRICE_CRNCY")[0].firstChild.nodeValue
            price_date = item.getElementsByTagName("ML_PRICE_DATE")[0].firstChild.nodeValue
            usd_to_rate = float(item.getElementsByTagName("ML_RATE")[0].firstChild.nodeValue)
            quarterly_rate=float(item.getElementsByTagName(quaterly_rate_type)[0].firstChild.nodeValue)           
            temp_list = ["USD",currency, price_date, usd_to_rate,quarterly_rate]
            list.append(temp_list)            
            currency_chk[currency]=True
    df = pd.DataFrame(list)
    
    return(df)          

def daily(df):
   
    temp_df = pd.DataFrame()
    temp_df['to_currency']=df['from_currency']
    temp_df['from_currency']=df['to_currency']
    temp_df['date']=df['date']
    temp_df['daily_fx']=df['daily_fx'].apply(lambda x: 1/x) 
    temp_df.columns.name = None

    final_output=df.append(temp_df,ignore_index=True)
    final_output['FinanceEnterpriseGroup']='MFC'
    final_output['GLExchangeRateInterface']=final_output.index+1
    final_output['CurrencyTable']='APCB'

    final_output= final_output[['FinanceEnterpriseGroup','GLExchangeRateInterface','CurrencyTable','from_currency','to_currency','date','daily_fx']]

    final_output.to_csv('C:/Users/patelsm/Desktop/fx_rates/GO_FXRATE_UPDATE/output.txt',index=False)
    print(final_output)

main()