from pdb import set_trace
import pandas as pd
from os.path import join, splitext
from datetime import datetime as dtm, timedelta
import re
import numpy as np
import time
from config import *
from data_access import *

def hold_til_(hold_til='min', accuracy_secs=1):
    """This will stall until you are at an even time.  For exmaple:
    'hour' hold till top of hour
    'min'  hold till top of min
    'sec'  AND '1 sec' IS NOT AN OPTION CURRENTLY!
    '5 sec'  hold until 5, 10, 15, 20 ... seconds for the time
    '2 min'  hold until the top of the min at 2 min intervals
    '3 hour'  hold until the top of the hour at 3 hour intervals
    """
    now = dtm.now()

    if hold_til == 'hour':
        # wait until the top of the hour
        while now.minute != 0 or now.second != 0:
            now = dtm.now()
            time.sleep(accuracy_secs)

    elif hold_til == 'min':
        while now.second != 0:
            now = dtm.now()
            time.sleep(accuracy_secs)
    else:
        if '1 sec' in hold_til:
            raise NotImplementedError('cant yet do a single second')

        interval, unit = re.split('\s+', hold_til)
        interval = int(interval)

        if 'sec' in unit:
            while now.second % interval != 0:
                now = dtm.now()
                time.sleep(accuracy_secs)
        if 'min' in unit:
            while now.minute % interval != 0 or now.second != 0:
                now = dtm.now()
                time.sleep(accuracy_secs)
        if 'hour' in unit:
            while now.minute != 0 or now.second != 0 or \
                  now.hour % interval != 0:
                now = dtm.now()
                time.sleep(accuracy_secs)

class DatFile(object):
    tablenames = tablenames

    def __init__(self, station, datfile_path=None):
        '''Open a dat file, specify a station by its four character
capitalized name like SBSP, and specify the full path to the dat file.
the dat file is now contained in a pandas dataframe at dat.rawfile'''

        # used to convert a DOY to a python datetime object
        def doyDate2datetime(row):
            year = row['year']
            doy = row['doy']
            time = str(row['hour'])
            _, hours, mins, _ = (re.split('(\d?\d)(\d\d)', time))
            return dtm(int(year), 1, 1) + timedelta(int(doy) - 1,
                                                         hours=int(hours),
                                                         minutes=int(mins))

        self.station = station
        self.datfile_path = datfile_path
        self.table = self.tablenames[station]

        self.uploadlogfile = join(upload_logfile_dir,
                                  "%s_upload_log.txt" % self.station)

        self.header = get_header_info(station)
        self.data_arrays = get_data_arrays(station)

        self.rawfile = pd.read_csv(datfile_path,
                                   names=self.header.index, dtype={'Hour': str})
        self.rawfile['datetime'] = self.rawfile.apply(doyDate2datetime, axis=1)
        self.rawfile.set_index(['arrayid', 'datetime'], drop=True, inplace=True)
        self.rawfile.sort_index(inplace=True)

    def copy(self):
        'Copies the dat python object not the dat file itself'
        new = DatFile.__new__(DatFile)
        new.station = self.station
        new.table = self.table
        new.header = self.header.copy()
        new.data_arrays = self.data_arrays.copy()
        new.rawfile = self.rawfile.copy()
        new.uploadlogfile = self.uploadlogfile
        return new

    def clear_rows_already_in_database(self, inplace=True):
        '''Clears the rawfile dataframe of any arrayid and datetimes
that match one in the database'''
        done=False
        while not done:
            try:
                alreadyup = engine.execute("""SELECT arrayid,datetime
                            FROM %s;""" % self.table).fetchall()
                done=True
            except:
                done=False
                print('Connection Failed at %s' % dtm.now())
                time.sleep(5)

        if inplace:
            self.rawfile = self.rawfile[~self.rawfile.index.isin(alreadyup)]
            return self
        else:
            new = self.copy()
            new.rawfile = self.rawfile[~self.rawfile.index.isin(alreadyup)]
            return new

    def add_albedo(self):
        '''Adds albedo to the rawfile'''
        self.rawfile[albedo_info['fieldname']] = \
           self.rawfile.loc[:,albedo_info['pydwn_field_name']] /   \
           self.rawfile.loc[:,albedo_info['pyup_field_name']]
        infs = self.rawfile[albedo_info['fieldname']].isin([np.inf,-np.inf])
        self.rawfile.loc[infs, albedo_info['fieldname']] = np.nan

    def check_dat_interval_after_db(self, arrayid):
        """ Here we are checking to make sure that the first row in the dat file
         is exactly is one interval after the last row in the database for a
         specific data array id.  The function returns true if they are timed
         correctly and if they are incorrect it returns the difference in
         minutes, and the two times"""

        intervalminutes = self.data_arrays.loc[arrayid].intervalminutes

        last_date_inDB = get_last_date(self.table, arrayid)[0][0]
        if last_date_inDB is None:
            return True

        last_date_inDB = last_date_inDB.replace(tzinfo=None)

        first_date_in_dat = self.rawfile.loc[arrayid].index.min().to_pydatetime()

        diff_minutes = (first_date_in_dat - last_date_inDB).total_seconds()/60.

        if diff_minutes == intervalminutes:
            return True
        else:
            return diff_minutes, first_date_in_dat, last_date_inDB

    def upload2db(self, insert_despite_interval_issue=True,
                  catch_upload=True):
        '''Uploads the rawfile to the database, it checks to remove duplicate
        rows, and checks the intervals, if the intervals show there are hours
        missing it still uploads but logs the upload in the log file'''

        # REMOVING DATA FROM THE FILE THAT ALREADY EXISTS IN THE DATABASE
        upload = self.clear_rows_already_in_database(inplace=True)
        uploadf = upload.rawfile
        nrows = uploadf.shape[0]
        if nrows == 0:
            self.log_no_new_rows()
            return

        # CHECKING THE INTERVALS TO SEE IF WE ARE MISSING RECORDS
        # THIS MIGHT BE UNNECESSARY
        arrayids = uploadf.index.get_level_values('arrayid').unique()
        for arrayid in arrayids:
            good = upload.check_dat_interval_after_db(arrayid)
            if good is not True:
                if not insert_despite_interval_issue:
                    upload.log_did_not_insert(arrayid, good[0])
                    return
                else:
                    upload.log_break_in_records(arrayid, good[0])

        if catch_upload:
            try:
                uploadf.reset_index().to_sql(self.table, engine,
                                         'public', 'append',
                                         index=False)
            except Exception:
                upload.log_upload_failed()
                return
        else:
            uploadf.reset_index().to_sql(self.table, engine,
                                         None, 'append',
                                         index=False)

        upload.log_successful()

    def _log(self, txt, log=True, stdout=True):
        txt = txt + '\n'
        if log:
            with open(self.uploadlogfile, 'a') as up:
                up.write(txt)
        if stdout:
            print(txt)

    def log_did_not_insert(self, arrayid, minutediff, log=True, stdout=True):
        txt = """ Opted to not upload data from arrayid %s that
is %s hours from the last data point""" % (arrayid, minutediff/60.)
        self._log(txt, log, stdout)

    def log_break_in_records(self, arrayid, minutediff, log=True, stdout=True):
        txt = """ Uploading data from arrayid %s that
is %s hours from the last data point""" % (arrayid, minutediff/60.)
        self._log(txt, log, stdout)

    def log_successful(self, log=True, stdout=True):
        txt = 'Successful upload of %s records at %s' % (
                     self.rawfile.shape[0], dtm.now())
        self._log(txt, log, stdout)

    def log_no_new_rows(self, log=True, stdout=True):
        txt = 'No new rows to upload at %s' % dtm.now()
        self._log(txt, log, stdout)

    def log_upload_failed(self, error=None, log=True, stdout=True):
        txt = 'Upload to database failed at %s' % dtm.now()
        if error is not None:
            txt = txt + error
        self._log(txt, log, stdout)


def create_table_sql(station, tablename):
    '''Helper function to make a table'''
    df = get_header_info(station)

    print('CREATE TABLE %s (' % tablename)
    print('%s_ID SERIAL PRIMARY KEY,' % tablename)
    print('datetime timestamp,')
    print('albedo real,')
    for name, dtype in df.Data_Type[:-1].iteritems():
        if 'Float' in dtype:
            dtype = 'real'
        elif 'Integer' in dtype:
            dtype = 'integer'
        print("%s %s," % (name, dtype))
    lastvalue = df.Data_Type.iloc[-1]
    if 'Float' in lastvalue:
        lastvalue = 'real'
    elif 'Integer' in lastvalue:
        lastvalue = 'integer'
    print("%s %s);" % (df.index[-1], lastvalue))


if __name__ == '__main__':

    #########################################################################
    #########################################################################
    # THIS IS THE OTHER SECTION YOU SHOULD KNOW HOW IT WORKS
    #########################################################################

    # THIS IS THE LIST OF FILES AND STATIONS TO BE UPLOADED
    stationlist = [['SASP', datfiledir + 'SASP-Oct2_2019.dat'],
                   ['SBSG', datfiledir + 'SBSG.dat'],
                   ['SBSP', datfiledir + 'SBSP-Oct2_2019.dat'],
                   ['PTSP', datfiledir + 'PTSP-Oct2_2019.dat']]

    # UPLOADING THINGS INITIALLY WHEN WE START THE SCRIPT
    # LOOPING THROUGH EASH DAT FILE AND UPLOADING TO THE DATABASE
    for station,filepath in stationlist:
        print(station)
        dat = DatFile(station, filepath)    # opening the file
        if station in ('SASP','SBSP'):
            dat.add_albedo()
        dat.upload2db(catch_upload=False)                     # uploading the file

    # LOOPING INDEFINATELY
    while True:
        # WAITING UNTIL THE TOP OF THE HOUR, THEN WAITING ANOTHER 12 MIN
        hold_til_('hour')
        time.sleep(5)
        hold_til_('12 min')

        # LOOPING THROUGH EASH DAT FILE AND UPLOADING TO THE DATABASE
        for station,filepath in stationlist:
            dat = DatFile(station, filepath)    # opening the file
            if station in ('SASP','SBSP'):
                dat.add_albedo()
            dat.upload2db(catch_upload=False)                     # uploading the file
