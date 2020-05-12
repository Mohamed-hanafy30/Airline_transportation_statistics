

def map_airport_name(data_frame,data_frame_airport):
    '''
    This function take 2 data frames as argument to map origin column code into names using airport_names.csv
     file

    :param data_frame:
    :param data_frame_airport:
    :return: data frame after mapping origin code with names
    '''

    mydict = dict(zip(data_frame_airport.Code, data_frame_airport.Description))
    data_frame["Origin"]=data_frame['Origin'].map(mydict)
    data_frame["Origin"] = data_frame['Origin'].str.split(':')
    data_frame['Origin'] = [i[1] for i in data_frame['Origin']]
    data_frame["Origin"] = data_frame['Origin'].str.replace('/',' ')
    data_frame['Origin']=data_frame['Origin'].apply(lambda x: str(x)+' '+'airport')
    return data_frame



def map_time_block(data_frame,data_frame_time):
    '''
    This function take 2 data frames as argument to map ariival and depetupre time block column code into
    names using ArrTimeBlk.csv
     file

    :param our data data_frame:
    :param timeblock dataframe:
    :return: data frame after mapping origin code with names
    '''

    mydict = dict(zip(data_frame_time.Code, data_frame_time.Description))
    data_frame["ArrTimeBlk"]=data_frame['ArrTimeBlk'].map(mydict)
    data_frame["DepTimeBlk"]=data_frame['DepTimeBlk'].map(mydict)
    return data_frame



def map_Reporting_Airline(data_frame,data_frame_report):

    '''
    This function take 2 data frames as argument first is data and second is timeblock to map Reporting_Airlines
    cod with airlines names using Reporting_airline.csv file
     file

    :param our data data_frame:
    :param timeblock dataframe:
    :return: data frame after mapping origin code with names
    '''
    mydict = dict(zip(data_frame_report.Code, data_frame_report.Description))
    data_frame["Reporting_Airline"]=data_frame['Reporting_Airline'].map(mydict)
    return data_frame