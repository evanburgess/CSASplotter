from sqlalchemy import create_engine

#############################################################################
#############################################################################
# THIS IS THE STUFF YOU WILL NEED TO CHANGE!
#############################################################################
# CREATING THE CONNECTION TO THE DATABASE FOR MYSQL WILL LOOK MORE LIKE THIS:
#engine = create_engine('mysql+mysqldb://csasdb:Csas1040!:192.186.235.162/snowstudies?charset=utf8mb4&binary_prefix=true' )# NOTE WE NEED TO PUT YOUR PASSWORD IN AN ENVIRONMENT VARIABLE
engine = create_engine('mysql+mysqldb://csasdb:%s@192.186.235.162:3306/snowstudies' % getenv(
	                   'CSAS_DB_PASSWORD'))

# DIRECTORY HOLDING THE STATION INFO DATA FILES
#stationinfodir = '/Users/airsci/Documents/CSASPlotter/stationinfo'
stationinfodir = 'C:\\Users\\Kimberly\\Documents\\python-data-transfer\\CSASPlotter\\stationinfo'

# DIRECTORY HOLD THE DAT FILES
datfiledir = 'C:\\Users\\Kimberly\\Dropbox\\Campbellsci\LoggerNet\\'

# DIRECTORY HOLDING THE LOG FILES THAT RECORD EACH UPLOAD AND ITS SUCCESS OR FAILURE
upload_logfile_dir = 'C:\\Users\\Kimberly\\Documents\\python-data-transfer\\CSASPlotter'

#############################################################################
#############################################################################
# THIS STUFF YOU MIGHT NEED TO CHANGE BUT I THINK YOU ARE OK
stationxlsfile = join(stationinfodir, 'Field_Lists.xlsx')

tablenames = dict(SASP='SwampAngel', SBSP='SenatorBeck',
                  SBSG='SenatorBeckStream', PTSP='Putney')

albedo_info = {'fieldname': 'albedo', 'pyup_field_name': 'pyup_unfilt_w',
               'pydwn_field_name': 'pydwn_unfilt_w', "Data_Type": float,
               'Description': 'Albedo', 'Common Name': 'Albedo',
               'Data Check': '0,1'}
