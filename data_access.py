from config import *
import pandas as pd
from os import getenv
import numpy as np
[
{"field":"loair_avg_c", "station": "SBSP"},
{"field":"loair_avg_c", "station": "SASP"},
{"field":"air_avg_c", "station": "PTSP"}]

def get_data_from_station(station, fields, start, end, interval):

    sql = 'SELECT datetime, {field} as {station}_{field} ' + \
          'FROM {table} ' + \
          "WHERE arrayid = {arrayid} AND " + \
          "datetime BETWEEN '{start}' AND '{end}'"


    table = tablenames[station]

    da = get_data_arrays(station)
    arrayid = da[da.label == interval].index[0]

    if type(fields) in (list, tuple):
        fields = ', '.join(fields)
    sql = sql.format(field=fields, table=tablenames[station],
               start=start.strftime("%Y-%m-%d %H:%M:%S"),
               end=end.strftime("%Y-%m-%d %H:%M:%S"),
               arrayid=arrayid,
               station=station)

    try:
        df = pd.read_sql_query(sql, engine, parse_dates=True, index_col='datetime')
    except Exception:   # IF FAILED TRY AGAIN  SOMETIMES THE FIRST SEEMS TO FAIL
        df = pd.read_sql_query(sql, engine, parse_dates=True, index_col='datetime')

    return df

def get_data(fieldslist, start, end, interval='1 Hour'):

    dfs = [get_data_from_station(st['station'],
                                 st['field'],
                                 start,
                                 end,
                                 interval) for st in fieldslist]

    # CONCATENATING ALL DATAFRAMES
    out = pd.concat(dfs, names=[st['station'] for st in fieldslist], axis=1)

    # REMOVING COLUMNS IF THEY ARE DUPLICATED
    idx = np.unique(out.columns.values, return_index=True)[1]
    out = out.iloc[:, idx]
    return out

def get_header_info(station):
    '''This grabs the header info for a specific station and
    returns it as a pandas dataframe'''

    fields = pd.read_excel(stationxlsfile, station,
                           skiprows=0, header=1, index_col=1)
    fields.index = fields.index.str.lower()
    return fields

def get_data_arrays(station):
    '''Reads the text files in stationinfo directory and returns a dataframe of
    data array info including the id, the label, the interval in minutes, and
    a second label without spaces and no number to start the label
    '''
    filename = "%s_data_arrays.txt" % station
    filepath = join(stationinfodir, filename)

    data_arrs = pd.read_csv(filepath,index_col='ID')
    return data_arrs

def get_last_date(table, arrayid):
    sql = "SELECT MAX(datetime) FROM %s WHERE arrayid=%i" % (table, arrayid)
    out = engine.execute(sql).fetchall()
    return out
