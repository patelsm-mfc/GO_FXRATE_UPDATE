import pandas as pd 
from xml.dom import minidom
import ntpath
import sys

file_name='/Users/patelsm/Desktop/fx_rates/PriceMaster_1884632_20210301_153430_LAWSON_Euro_EOD_PRC.xml'
output='APWeeklyRates_Test'+ntpath.basename(file_name)[12:35]+'.csv'
output2='GL_Exchange'+ntpath.basename(file_name)[12:35]+'.csv'
rate_type='D'
tree = minidom.parse(file_name)
spot_rate_type="BPL_CLASSIFIER_II"
seqnum=0

currency_chk={  'AED' :False,'ANG' :False,'AUD' :False,'BBD' :False,'BMD' :False,'BND' :False,'BRL' :False,'BSD' :False,'BZD' :False,'CHF' :False,
                'CNH' :False,'CNY' :False,'COP' :False,'CUP' :False,'DKK' :False,'DOP' :False,'EGP' :False,'EUR' :False,'GBP' :False,'GYD' :False,
                'HKD' :False,'HTG' :False,'HUF' :False,'IDR' :False,'ILS' :False,'INR' :False,'JMD' :False,'JOD' :False,'JPY' :False,'KES' :False,
                'KHR' :False,'KRW' :False,'KWD' :False,'KYD' :False,'LKR' :False,'MMK' :False,'MOP' :False,'MXN' :False,'MYR' :False,'NOK' :False,
                'NZD' :False,'PAB' :False,'PHP' :False,'PLN' :False,'SEK' :False,'SGD' :False,'SRD' :False,'THB' :False,'TTD' :False,'TWD' :False,
                'TZS' :False,'UGX' :False,'USD' :False,'UYU' :False,'VND' :False,'XCD' :False,'ZAR' :False,'ZMW' :False,'ZWD' :False    }

def daily(daily_list,currency,price_date,usd_to_rate):
    daily_header = ["MFC","APCB","USD", currency, price_date, usd_to_rate]
    daily_list.append(daily_header)

def monthly(monthly_list,count,currency,price_date,usd_to_rate):
    monthly_header = ["MFC",count,"MFC", "USD",currency, price_date, usd_to_rate]
    monthly_list.append(monthly_header)

def main():
    items = tree.getElementsByTagName('Price')
    daily_list = []
    monthly_list=[]
    count=1
    for item in items:       
        BPL_type = item.getElementsByTagName(spot_rate_type)
        BPL_value = BPL_type[0].firstChild.nodeValue
        if (BPL_value == "SPX"):            
            currency = item.getElementsByTagName("PRICE_CRNCY")[0].firstChild.nodeValue
            price_date = item.getElementsByTagName("ML_PRICE_DATE")[0].firstChild.nodeValue
            usd_to_rate = float(item.getElementsByTagName("ML_RATE")[0].firstChild.nodeValue)
            daily(daily_list,currency,price_date,usd_to_rate)
            if(rate_type == 'M'):
                monthly(monthly_list,count,currency,price_date)           
                count+=1     
            currency_chk[currency]=True        
        # print(currency, price_date, price)

    df = pd.DataFrame(daily_list)
    df2 = pd.DataFrame(monthly_list)

    missingList = dict(filter(lambda elem: elem[1] == False, currency_chk.items()))


    if(not bool(missingList) and rate_type == 'D'):
        df.columns=['FinanceEnterpriseGroup','CurrencyTable','FromCurrency','ToCurrency','ExchangeDate','Rate']        
        df.to_csv('/Users/patelsm/Desktop/fx_rates/'+output, index=False)
    elif(not bool(missingList) and rate_type == 'M'):
        df2.columns=['FinanceEnterpriseGroup','GLExchangeRateInterface','CurrencyTable','FromCurrency','ToCurrency','ExchangeDate','Rate']
        df2.to_csv('/Users/patelsm/Desktop/fx_rates/'+output2, index=False)
    else:
        print("Following currencies are missing")
        print(missingList.keys())

main()