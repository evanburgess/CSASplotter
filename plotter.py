from pdb import set_trace
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime as dtm, timedelta
from data_access import get_data_arrays, get_header_info
from config import *
from bokeh.plotting import figure, show, output_file, save
from bokeh.embed import file_html
from bokeh.models.widgets import Panel, Tabs
from bokeh.resources import CDN
from bokeh.models.ranges import Range1d
from bokeh.layouts import column
from config import *
from data_access import *
import json
from bokeh.models.sources import ColumnDataSource
from os import remove, getenv
from os.path import exists as fileexists, dirname
from copy import copy
import time
import sys
import argparse
import codecs
import paramiko
from sftp import *


## STUFF YOU MIGHT WANT TO CHANGE:
legend_font_size = '10pt'
legend_location= 'top_left'
plot_width = 800
plot_height = 300


# PARSING COMMAND LINE ARGUEMENTS
parser = argparse.ArgumentParser(description="""
This script needs to be executed on a regular basis to create the plots and it can also upload the plot 
to a remote server for you.  See the help below for how to execute this file on the commandline. Once you
get this working on your machine of choice, update the shell script and execute the shell script with cron.
\n
As an example:
\n
python plotter.py /fullpath/to/output/htmlfile/on/local/machine /fullpath/to/json/template/file 40 7 --sftp_to 124.234.12.44 --remote_username matt --remote_password mypassword --remote_filepath /fullpath/to/outputhtml/on/remote/server

""")
parser.add_argument("output",
                    help='the full path to the outputed html file on the local machine.  If you ' +
		    'ultimately want this on a different server still specify a location for this ' +
                    'file.  It will be created locally at that location and THEN sftped to your ' + 
                    'specified location',
                    type=str)
parser.add_argument("jsonfile",
                    help='the full path to the json template file',
                    type=str)
parser.add_argument("tdelta_days",
                    help='The number of days of data to make available to the ' +
                    'client computer.  This is not the number of days that will ' +
                    ' be shown initially',
                    type=int)
parser.add_argument("tdelta_days_showing",
                    help='The number of days of data to show on the inital ' +
                    'page load.  The user will still be able to zoom out or ' +
                    ' pan back to tdelta_days',
                    type=int)
parser.add_argument('--sftp_to',dest='remote_ip',
                    help='If you would like to sftp this file over to a different '+ 
                    'sever then enter the ip address here',
                    default=False,
                    type=str)
parser.add_argument('--remote_username',
                    help='If you would like to sftp this file over to a different '+ 
                    'sever then enter the username to login to that server here',
                    default=False,
                    type=str)
parser.add_argument('--remote_password',
                    help='If you would like to sftp this file over to a different '+ 
                    'sever then enter the password to login to that server here',
                    default=False,
                    type=str)
parser.add_argument('--remote_filepath',
                    help='If you would like to sftp this file over to a different ' +
                    'sever then enter the full file path describing where to save ' +
		    'the file on the other server',
                    default=False,
                    type=str)

# sftp = SftpClient(remote_ip,22,remote_username,remote_password)

args = parser.parse_args()
output = args.output
jsonfile = args.jsonfile
tdelta_days = args.tdelta_days
tdelta_days_showing = args.tdelta_days_showing


# SANITY CHECKS
if not fileexists(jsonfile):
    raise RuntimeError('Could not find file %s' % jsonfile)

if not fileexists(dirname(output)):
    raise RuntimeError('Invalid path for %s' % output)

if fileexists(output):
    remove(output)

if tdelta_days < tdelta_days_showing:
    raise RuntimeError('The number of days of data is less than the number' +
    'of days showing when loaded, did you switch them?')

if tdelta_days < 1: raise RuntimeError ('tdelta_days must be >= 1')
if tdelta_days_showing < 1: raise RuntimeError ('tdelta_days_showing must be >= 1')

# if fileexists(output):
#     remove(output)
output_file(output)

colorslist = {
'PTSP':'#e41a1c',
'SBSG':'#377eb8',
'SBSP':'#4daf4a',
'SASP':'#984ea3'}

# READING THE TEMPLATE JSON FILE
try:
    with open(jsonfile,'r') as f:
        template = json.load(f)
except Exception:
    with codecs.open(jsonfile, 'r', 'utf-8-sig') as f:
        template = json.load(f)



#ORGANIZING JSON TEMPLATE DATA
querydata = []
for templ in template:
    for plot in templ['plots']:
        for line in plot['lines']:
            querydata.append(line)


# SETTING TIME RANGE
end = dtm.now()
start = end + timedelta(days=-tdelta_days)
start_initial = end + timedelta(days=-tdelta_days_showing)

# ASSEMBLING THE DATA INTO A DATASOURCE FOR THE PLOTTER
df = get_data(querydata,start,end)
source = ColumnDataSource(df)

# SETTING VARIOUS OPTIONS AND XLIMITS FOR THE PLOTTERS
xrange = Range1d(start=start_initial, end=end)# bounds=[start,end],
options = {'width':plot_width,'height':plot_height,'tools':'xwheel_zoom,xpan,crosshair',
           'x_axis_type':"datetime",'x_range':xrange, 'x_axis_type':'datetime'}

# IF ONLY ONE PAGE OF DATA IS LISTED IN THE JSON THEN TABS WILL NOT BE CREATED
should_i_make_tabs = True if len(template) > 1 else False
if should_i_make_tabs:
    tabs = []

# LOOPING THROUGH EACH OF THE TABS
for tab in template:
    plots = []
    # LOOPING THROUGH EACH OF THE PLOTS
    for plot in tab['plots']:
        # print(plot['axes_title'], plot['yrange'])
        yrange = Range1d(start=plot['yrange'][0],   # SETTING THE Y LIMITS FOR THE AXES
                         end=plot['yrange'][1])

        f = figure(title=plot['axes_title'],   # MAKING THE FIGURE
                   y_range=yrange,
                   **options)

        # LOOPING THROUGH EACH LINE FOR THIS AXES AND PLOTTING IT
        for line in plot['lines']:

            # GETTING THE FIELDNAME NEEDED TO PULL THE CORRECT DATA FROM THE DATA SOURCE
            station = line['station']

            # SETTING THE COLOR AND LEGEND LABEL
            color = colorslist[station] if not 'color' in line else line['color']
            label = station if not 'label' in line else line['label']

            fieldname = "%s_%s" % (station.lower(), line['field'])
            # print('    ', line['field'], fieldname)
            # print('        ',color,label)

            # IF ALL THE DATA IS MISSING DON'T PLOT THIS LINE
            if df[fieldname].isnull().all(): continue

            # PLOTTING THE LINE
            f.line('datetime', fieldname,
                   legend=label,
                   source=source,
                   color=color)

        f.legend.location = legend_location
        f.legend.label_text_font_size = legend_font_size
        plots.append(f)    # APPENDING THIS PLOT TO A LIST OF PLOTS

    # STACKING ALL OF THESE PLOTS INTO A SINGLE COLUMN OF PLOTS
    all = column(plots)
    if should_i_make_tabs:
        tab = Panel(child=all, title=tab["page_name"])
        tabs.append(tab)

if should_i_make_tabs:
    all = Tabs(tabs=tabs)
# DONE!!
save(all)
#show(all)
if args.remote_ip:
    sftp = SftpClient(args.remote_ip,22,args.remote_username,args.remote_password)
    sftp.upload(output, args.remote_filepath)
    sftp.close()

