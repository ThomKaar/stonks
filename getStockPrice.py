# Purpose: to pull a generic stock's information 
# from the Yahoo Finance API and store it in a mongoDB database.
import http.client
import json
import pymongo
import time
from secrets import api_key, api_host, db_password
# Purpose: Pull a stock's information
# Args: 
# - name: string ticker symbol of the stock
# - info: list of lists defining the path to the desired data
# - tags: list of strings to name the desired data, respective to info elements
# Example call: pullStock("TSLA", [["price", "regularMarketDayHigh", "raw"], ["price", "regularMarketDayLow", "raw"]])
def pullStock(name, info, tags, time):
   
   conn = http.client.HTTPSConnection("apidojo-yahoo-finance-v1.p.rapidapi.com")

   headers = {
      'x-rapidapi-key': api_key,
      'x-rapidapi-host': api_host
   }

   conn.request("GET", "/stock/v2/get-summary?symbol={sym}&region=US".format(sym=name), headers=headers)

   print("fetching {symbol} data".format(symbol=name))
   res = conn.getresponse()
   print("{symbol} data fetched".format(symbol=name))
   print("converting data to string")
   data = res.read()

   print("converting {symbol} data string to python dict".format(symbol=name))
   jsonData = json.loads(data.decode("utf-8"))

   # dictionary of data to be stored in database
   desiredDict = {}
   i = 0
   for dataPath in info:
      key = tags[i]
      desiredDict[key] = accessDict(jsonData, dataPath)
      i += 1
   desiredDict["time"] = time
   db = connectToDB(db_password)
   result = db.daily_data.insert_one(desiredDict)
   return

# Purpose: Access a dictionary value given a list with strings that are the path to the desired val
# Args:
# - d: dictonary to be accessed
# - los: list of strings
# Example call: accessDict(["price", "regularMarketDayHigh", "raw"])
def accessDict(d, los):
   curDict = d
   for val in los:
      curDict = curDict[val]
   return curDict

# Purpose: Connect to the stonks_data_0 database
def connectToDB(password):
   client = pymongo.MongoClient("mongodb+srv://tkaar-dev:{pswrd}@cluster0.rvgpl.mongodb.net/test?retryWrites=true&w=majority".format(pswrd=password))
   return client.stonks_data_0

# Purpose: Pull all market data from 
def pullAllStocks(stock_list, info, tags):
   epoch_time = int(time.time())
   for stock in stock_list:
      pullStock(stock, info, tags, epoch_time)
   return

info = [
   ["price", "regularMarketDayHigh", "raw"], 
   ["price", "regularMarketDayLow", "raw"], 
   ["price", "regularMarketPrice", "raw"], 
   ["price", "regularMarketVolume", "raw"], 
   ["price", "symbol"], 
   ["price", "regularMarketChangePercent", "raw"],
   ["price", "averageDailyVolume3Month", "raw"],
   ["price", "averageDailyVolume10Day", "raw"]
]

tags = [
   "rawDayHigh",
   "rawDayLow",
   "rawMarketPrice",
   "rawMarketVolume",
   "symbol",
   "rawChangePercent",
   "averageDailyVolume3Month",
   "averageDailyVolume10Day"
]

stock_list = ["SQ", "SPY", "VOO", "IVV", "VTI", "GOLD", "GLD", "TSLA", "PYPL", "SLV"]
pullAllStocks(stock_list, info, tags)