import pandas as pd 
from xml.dom import minidom
import ntpath
import sys

currency_chk={  'AED' :False,'ANG' :False,'AUD' :False,'BBD' :False,'BMD' :False,'BND' :False,'BRL' :False,'BSD' :False,'BZD' :False,'CHF' :False,
                'CNH' :False,'CNY' :False,'COP' :False,'CUP' :False,'DKK' :False,'DOP' :False,'EGP' :False,'EUR' :False,'GBP' :False,'GYD' :False,
                'HKD' :False,'HTG' :False,'HUF' :False,'IDR' :False,'ILS' :False,'INR' :False,'JMD' :False,'JOD' :False,'JPY' :False,'KES' :False,
                'KHR' :False,'KRW' :False,'KWD' :False,'KYD' :False,'LKR' :False,'MMK' :False,'MOP' :False,'MXN' :False,'MYR' :False,'NOK' :False,
                'NZD' :False,'PAB' :False,'PHP' :False,'PLN' :False,'SEK' :False,'SGD' :False,'SRD' :False,'THB' :False,'TTD' :False,'TWD' :False,
                'TZS' :False,'UGX' :False,'USD' :False,'UYU' :False,'VND' :False,'XCD' :False,'ZAR' :False,'ZMW' :False,'ZWD' :False    }

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
    appended_data.columns=['FinanceEnterpriseGroup','CurrencyTable','FromCurrency','ToCurrency','ExchangeDate','Rate']
    print(appended_data)
    missingList = dict(filter(lambda elem: elem[1] == False, currency_chk.items()))
    print(missingList.keys())   





def process(file_name):
    tree = minidom.parse(file_name)
    spot_rate_type="BPL_CLASSIFIER_II"
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
            temp_list = ["MFC","APCB","USD", currency, price_date, usd_to_rate]
            list.append(temp_list)
            usd_from_rate= float(1/usd_to_rate)       
            temp_list = ["MFC","APCB",currency, "USD", price_date, usd_from_rate]
            list.append(temp_list)
            currency_chk[currency]=True
    df = pd.DataFrame(list)
    
    return(df)           
            

main()