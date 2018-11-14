import pandas as pd
from os.path import join
from os import getenv
import sys
from sqlalchemy import create_engine
from datetime import datetime, timedelta
import re

# CREATING THE CONNECTION TO THE DATABASE FOR MYSQL WILL LOOK MORE LIKE THIS:
# engine = create_engine('mysql://jeff:%s@localhost/databasename' % getenv('CSAS_DB_PASSWORD'))   # NOTE WE NEED TO PUT A PASSWORD IN YOUR .PROFILE!!!
engine = create_engine('postgresql://evan:%s@localhost:5432/csas' % getenv(
                       'CSAS_DB_PASSWORD'))

# FOR NOW I HAVE A SINGLE WORK DIR WITH THE DAT FILES IN IT
# INSDE THAT DIR IS ANOTHER DIR CALLED stationinfo WHERE I KEEP INFO
# ON EACH STATIONS COLUMN NAMES AND DATA ARRAYS
wkdir = '/Users/airsci/Downloads/Data Info 2'

def get_column_header(station):
    '''Reads the text files in stationinfo and returns the column headers
    for the specified station.
    '''
    filename = "%s.txt" % station
    filepath = join(wkdir, 'stationinfo', filename)
    with open(filepath) as f:
        lines = f.read().splitlines()  # splitlines removes the hard return at the end of each line

    lines = [item for item in lines if len(item) > 0]     # removes any blank lines in the list
    return lines


def get_data_arrays(station):
    '''Reads the text files in stationinfo directory and returns a dataframe of
    data array info including the id, the label, the interval in hours, and
    a second label without spaces and no number to start the label (useful in
    python)
    '''
    filename = "%s_data_arrays.txt" % station
    filepath = join(wkdir, 'stationinfo', filename)

    data_arrs = pd.read_csv(filepath, index_col='ID')
    return data_arrs


def doyDate2datetime(year, doy, hour):
    '''Takes a year, doy, and hour and returns a datetime.datetime instance'''
    return datetime(int(year), 1, 1) + timedelta(int(doy) - 1,
                                                 hours=int(hour/100))   # here we divide hour by 100 because we need the hours without the two 00 representing minutes


def make_datetime_index(df):
    '''This function takes a dataframe from a raw dat file and combines the
year, doy, and time columns and returns DatetimeIndex which is a single index
column that retains all of that date time like info in a single nice spot.
'''
    # HERE DEALING WITH THE VARIOUS DIFFERENT DATETIME HEADER NAMES
    year_col_name = 'Year'
    doy_col_name = 'DOY' if 'DOY' in df.columns else 'Day of Year'
    if 'Time (MST)' in df.columns:
        time_col_name = 'Time (MST)'
    elif 'Time MST' in df.columns:
        time_col_name = 'Time MST'
    elif 'Hour' in df.columns:
        time_col_name = 'Hour'

    # LOOPING THROUGH EACH ROW AND CONVERTING THE YEAR, DOY AND TIME INTO A
    # DATETIME
    dtms = []
    for _, row in df.iterrows():
        dtms.append(doyDate2datetime(row[year_col_name],
                                     row[doy_col_name],
                                     row[time_col_name]))

    return pd.DatetimeIndex(dtms, name='datetime')


class DatFile(object):
    mapper = {'1 Hour': 'Hour1',
              '3 Hour': 'Hour3',
              '24 Hour': 'Hour24',
              'Solar Noon': 'SolarNoon'}

    tablenames = {'SBSG': 'senatorbeckstream',
                  'SASP': 'senatorbeck??'}

    def __init__(self, station, datfile_path=None):

        self.station = station
        self.table = self.tablenames[station]

        header = get_column_header(station)
        self.data_arrays = get_data_arrays(station)

        self.rawfile = pd.read_csv(datfile_path, names=header)
        self.rawfile.index = make_datetime_index(self.rawfile)
        self.rawfile.sort_index(inplace=True)

    def find_database_duplicates(self):
        '''This method will go to the datbase and crosscheck each datetime with
         the dattimes in the dat file.  If all of the datetimes in the dat file
         are new, this funtion returns None.  If there are some datetimes that
         are duplicates of entries already in the table then this fuction
         returns a dictionary of datetimes to be inserted and datetimes to be
         updated in the database
'''
        datetimes = self.rawfile.index.strftime('%Y-%m-%d %H:%M:%S')
        alreadythere = []
        newdates = []
        for dt in datetimes:
            anythere = engine.execute("""SELECT datetime
                            FROM senatorbeckstream
                            WHERE datetime = '%s';""" % dt).fetchall()
            if len(anythere) == 0:
                newdates.append(dt)
            else:
                alreadythere.append(dt)
        if len(alreadythere) > 0:
            return dict(inserts=newdates, updates=alreadythere)
        else:
            return None

    def check_dat_follows_database(self, dataarrayid):
        """Here we are checking to make sure that the first row in the dat file
         is exactly is one interval after the last row in the database for a
         specific data array id.  The function returns a timedelta object which
         tells you the time difference between the two dates.
"""
        hourinterval = self.data_arrays.loc[dataarrayid].hourinterval

        last_date_inDB = engine.execute("""SELECT MAX(datetime)
                            FROM senatorbeckstream
                            WHERE {station}_ID={arrayid};""".format(
                                  station=self.station,
                                  arrayid=dataarrayid)).fetchall()[0][0]

        first_date_in_dat = self.rawfile[
            self.rawfile.iloc[:, 0] == dataarrayid].index.min().to_pydatetime()

        timediff = first_date_in_dat - last_date_inDB

        return timediff, first_date_in_dat, last_date_inDB

    def upload2db(self, dataarrays=None, insert_despite_interval_issue=False):

        # CHECKING TO MAKE SURE THAT ALL ROWS FROM THIS DAT FILE WILL BE
        # NEW ROWS TO THE DATABASE
        if self.find_database_duplicates() is not None:
            raise NotImplementedError('''
Inserting this dat file will create duplicate datetimes in the database''')

        # CHECKING TO MAKE SURE THAT THE DATA IN THE DAT FILE IS ONE
        # INTERVAL AFTER THE LAST DATE IN THE DB
        if not insert_despite_interval_issue:
            for id, row in self.data_arrays.iterrows():
                timediff, datdate, dbdate = self.check_dat_follows_database(id)
                # IF THE INTERVAL IS NOT CORRECT FOR NOW WE WILL RAISE AN
                # ERROR BUT IN THE FUTURE WE CAN ADD LOGIC TO DO SOMETHING
                # TO RESOLVE
                if timediff.total_seconds()/60/60. != row.hourinterval:
                    raise NotImplementedError('''
                    For dataarrayid %s, the first date in the dat file %s is
                    not one interval after the last date in the
                    database %s''' % (id, datdate, dbdate))

        self.rawfile.to_sql(self.table, engine, 'public', 'append')

    @property
    def Hour1(self):
        for d in self.data_arrays:
            if d['label'] == '1 Hour':
                return self.rawfile.loc[self.rawfile.iloc[:, 0] == d['value']]
        return None

    @property
    def Hour3(self):
        for d in self.data_arrays:
            if d['label'] == '3 Hour':
                return self.rawfile.loc[self.rawfile.iloc[:, 0] == d['value']]
        return None

    @property
    def Hour24(self):
        for d in self.data_arrays:
            if d['label'] == '24 Hour':
                return self.rawfile.loc[self.rawfile.iloc[:, 0] == d['value']]
        return None

    @property
    def SolarNoon(self):
        for d in self.data_arrays:
            if d['label'] == 'Solar Noon':
                return self.rawfile.loc[self.rawfile.iloc[:, 0] == d['value']]
        return None


if __name__ == '__main__':
    station_name, datfilepath = sys.argv
    dat = DatFile(station_name, datfilepath)
    dat.upload2db()




# THIS IS JUST A TEMPLATE OF AN EXAMPLE TABLE FOR SAFE KEEPING
#    CREATE TABLE SenatorBeckStream (
#            SBSG_ID SERIAL,
#            datetime timestamp,
#            "Flag" integer,
#            "Year" integer,
#            "Day of Year" integer,
#            "Time MST" integer,
#            "EC no T Correct" real,
#            "Water Temp C" real,
#            "EC Correct 25C" real,
#            "Water Depth Ft" real,
#            "Discharge CFS" real,
#            "System Voltage" real);
