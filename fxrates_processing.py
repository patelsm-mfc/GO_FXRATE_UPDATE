import pandas as pd 
from xml.dom import minidom
import ntpath
import sys

#store currency list into boolean dict
#store currency list into boolean dict
currency_chk={}
myfile = open("C:/Users/patelsm/Desktop/fx_rates/GO_FXRATE_UPDATE/currency_list.txt", "r")
content = myfile.read()
content_list = content.split(",")

for line in content_list:    
    currency_chk[line.strip()]=False
myfile.close()


def main():
    appended_data = []

    #read the list of inputfiles
    with open('C:/Users/patelsm/Desktop/fx_rates/inbound/print.txt') as f:
        lines = f.read().splitlines()
    
    #process each file and append to final dataframe
    for x in range(len(lines)):
        appended_data.append(process(lines[x]))

    appended_data = pd.concat(appended_data, ignore_index = True)
    #df = pd.DataFrame(list2)
    #appended_data.columns=['FinanceEnterpriseGroup','CurrencyTable','FromCurrency','ToCurrency','ExchangeDate','Rate']
    print(appended_data)
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
            temp_list = [currency, price_date, usd_to_rate,quarterly_rate]
            list.append(temp_list)            
            currency_chk[currency]=True
    df = pd.DataFrame(list)
    
    return(df)           
            

main()