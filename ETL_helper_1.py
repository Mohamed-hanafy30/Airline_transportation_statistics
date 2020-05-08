
"""
The United States Department of Transportation has Flight Stats available through
the Bureau of Transportation Statistics. This data comes from The Bureau of Transportation Statistics
and tracks destinations, distance, and delay information of flights across U.S. For this project,
I chose the years 1995 through February 2020
"""

# Import libraries and load datasets.

import pandas as pd
import numpy as np
import sqlite3
import math
import datetime
import time as t




'''
this part for classify columns and partition them into groups for preprocessing by dropping unnecessary 
columns and downcast or convert into other datatypes
'''



'''
We are not interested in further information about more than one DIVERTED airport so i will drop theses columns 
furthermore these columns contain almot 100% Nan values beacuse amot all flights didn't need diverted airport or 
at most only one diverted Airport
'''


Diverted_cols = ['Div2Airport',
                 'Div2AirportID',
                 'Div2AirportSeqID',
                 'Div2WheelsOn',
                 'Div2TotalGTime',
                 'Div2LongestGTime',
                 'Div2WheelsOff',
                 'Div2TailNum',
                 'Div3Airport',
                 'Div3AirportID',
                 'Div3AirportSeqID',
                 'Div3WheelsOn',
                 'Div3TotalGTime',
                 'Div3LongestGTime',
                 'Div3WheelsOff',
                 'Div3TailNum',
                 'Div4Airport',
                 'Div4AirportID',
                 'Div4AirportSeqID',
                 'Div4WheelsOn',
                 'Div4TotalGTime',
                 'Div4LongestGTime',
                 'Div4WheelsOff',
                 'Div4TailNum',
                 'Div5Airport',
                 'Div5AirportID',
                 'Div5AirportSeqID',
                 'Div5WheelsOn',
                 'Div5TotalGTime',
                 'Div5LongestGTime',
                 'Div5WheelsOff',
                 'Div5TailNum',
                 'Unnamed: 109']

'''
Redundant columns
the follwing columns are repeted or contain information that not necessary or repetead in another form 
in another columns so i will drop it for memory issues

'DOT_ID_Reporting_Airline' ------ didn't provide useful info

'IATA_CODE_Reporting_Airline' ------- i will use Reporting_Airline column instead

'OriginAirportSeqID' ------- i will use OriginAirportID as this column is unique for each airport over the years

'OriginStateFips' ---------- this column contain fedral identfication number for each airport

'OriginState' -------- OriginStateName column in more handy

'OriginWac' ------ origin area code is not helpful for my analysis

'DestAirportSeqID' --------- i will use DestAirportID as this column is unique for each airport over the years

'DestState' ------- DestStateName column in more handy

'DestStateFips' ----- this column contain fedral identfication number for each airport

'DestWac' -------- Destination area code is not helpful for my analysis

'''



unrelevant_col = ['DOT_ID_Reporting_Airline',
                  'IATA_CODE_Reporting_Airline',
                  'OriginAirportSeqID',
                  'OriginStateFips',
                  'OriginState',
                  'OriginWac',
                  'DestAirportSeqID',
                  'DestState',
                  'DestStateFips',
                  'Div1WheelsOff',
                  'Div1TailNum',
                  'Div1AirportSeqID',
                  'DestWac']





'''
the following columns will be downcasted into int16
'''

int_downcast=['OriginAirportID',
 'DestAirportID',
 'DistanceGroup']


'''
the following columns will be downcasted from float64 into float16
'''



float_downcast=['Distance',
 'CarrierDelay',
 'WeatherDelay',
 'NASDelay',
 'SecurityDelay',
 'LateAircraftDelay',
 'DivArrDelay',
 'DivDistance','Div1TotalGTime',
 'Div1LongestGTime',
 'Div1AirportID']



int_8 =['Flights']

'''
the following columns used as an indicator so i will convert it into BOOLEAN 
'''



bool_downcast=[
 'DepDel15',
 'ArrDel15',
 'Cancelled',
 'Diverted',
 'DivReachedDest']



'''
After that we still have columns their dtypes are int64 while we can convert them into int32 to save more memory
as their range fit with int32 type
'''


int_downcast_32=['Flight_Number_Reporting_Airline',
 'OriginCityMarketID',
 'DestCityMarketID',
 'CRSDepTime',
 'CRSArrTime']


'''
Categorical columns conversion

after investigating tcolumns which has less number of unique values i will convert 

'DivAirportLandings', 'DepartureDelayGroups',
'ArrivalDelayGroups','DistanceGroup','Year',
'Quarter','Month','DayofMonth','DayOfWeek',
'CancellationCode'
 
 and the rest of columns will be converted into appropriate form of date and time
'''


categorical_downcast=['DivAirportLandings',
                      'DepartureDelayGroups',
                      'ArrivalDelayGroups',
                      'DistanceGroup',
                      'Year',
                      'Quarter',
                      'Month',
                      'DayofMonth',
                      'DayOfWeek',
                      'CancellationCode']



'''
 columns describe date or time information and after investigating their range all of them can fit into float16 format 
 this is a transitional step before converting them into approprite datetime format
'''


time_float_downcast=['DepTime',
 'DepDelay',
 'DepDelayMinutes',
 'TaxiOut',
 'WheelsOff',
 'WheelsOn',
 'TaxiIn',
 'ArrTime',
 'ArrDelay',
 'ArrDelayMinutes',
 'CRSElapsedTime',
 'ActualElapsedTime',
 'AirTime',
 'FirstDepTime',
 'TotalAddGTime',
 'LongestAddGTime',
 'DivActualElapsedTime',
 'Div1WheelsOn',
 'CRSArrTime',
 'CRSDepTime']


'''
the following columns are in string format but they infer time according to record layout in HHMM format but they
need preprocessing before converting them into datetime
'''

hhmm=['DepTime',
      'CRSArrTime',
      'CRSDepTime',
      'WheelsOff',
      'WheelsOn',
      'ArrTime']


def read_dataframe(filepath):
    '''
    this function read csv files into pandas dataframe object

    INPUT : 'filepath' this is file path for desired file in string format
    OUTPUT : dataframe object
    '''

    data = pd.read_csv(filepath, low_memory=False)
    return data



def std_time_1(col):

    Hours = int(col / 100)
    Minute = round(float(col / 100) - Hours, 2)
    Hours = str(int(Hours)).zfill(2)
    Minute = str(Minute).replace('0.', '').zfill(2)
    my_str = Hours + ':' + Minute

    return my_str



def std_time(string):

    return datetime.datetime.strptime(string, '%H:%M').time()


def clean_dataframe(data):

    data = data.drop(Diverted_cols, axis=1)
    data = data.drop(unrelevant_col, axis=1)
    data[int_downcast] = data[int_downcast].astype('int16')
    data[int_8] = data[int_8].astype('int8')
    data[float_downcast] = data[float_downcast].astype('float16')
    data[bool_downcast] = data[bool_downcast].astype('bool')
    data[int_downcast_32] = data[int_downcast_32].astype('int32')
    data[categorical_downcast] = data[categorical_downcast].astype('category')
    data[time_float_downcast] = data[time_float_downcast].astype('float16')
    data['OriginCityName'] = data['OriginCityName'].str.split(',')
    data['DestCityName'] = data['DestCityName'].str.split(',')
    data['OriginCityName'] = data['OriginCityName'].str[0]
    data['DestCityName'] = data['DestCityName'].str[0]
    data = data.dropna(subset=hhmm)
    for i in hhmm:
        data[i] = data[i].apply(lambda x: std_time_1(x))
    for i in hhmm:
        data[i] = data[i].str.replace('24:00', '23:59')

    for i in hhmm:
        data[i] = data[i].apply(lambda x: std_time(x))

    return data
