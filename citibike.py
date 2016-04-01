# -*- coding: utf-8 -*-
"""
Created on Sun Mar 27 20:17:33 2016

@author: Erin
"""
import requests

r = requests.get('http://www.citibikenyc.com/stations/json')
r.json()

key_list = [] #unique list of keys for each station listing
for station in r.json()['stationBeanList']:
    for k in station.keys():
        if k not in key_list:
            key_list.append(k)

print key_list

# getting the data into a data frame

from pandas.io.json import json_normalize

df = json_normalize(r.json()['stationBeanList'])
            
#checking the range of values
import matplotlib.pyplot as plt
import pandas as pd

#available bikes distribution
df['availableBikes'].hist()
plt.show()
#total docks distribution
df['totalDocks'].hist()
plt.show()

df['totalDocks'].mean()
print "mean of stations"
print df['totalDocks'].mean()

#find mean of only the stations that are active
condition = (df['statusValue'] == 'In Service')
df[condition]['totalDocks'].mean()
print "mean of only the stations that are active"
print df[condition]['totalDocks'].mean()

#find median of the stations 

df['totalDocks'].median()
print "median of stations"
print df['totalDocks'].median()

#find median of stations that are active
df[df['statusValue']== 'In Service']['totalDocks'].median()
print "median of stations that are active"
print df[df['statusValue']== 'In Service']['totalDocks'].median()

#storing data in SQLite
#making a reference table
import sqlite3 as lite

con = lite.connect('citi_bike.db')
cur = con.cursor()

with con:
    cur.execute('CREATE TABLE citibike_reference (id INT PRIMARY KEY, totalDocks INT, city TEXT, altitude INT, stAddress2 TEXT, longitude NUMERIC, postalCode TEXT, testStation TEXT, stAddress1 TEXT, stationName TEXT, landMark TEXT, latitude NUMERIC, location TEXT )')
    
#a prepared SQL statement we're going to execute over and over again
sql = "INSERT INTO citibike_reference (id, totalDocks, city, altitude, stAddress2, longitude, postalCode, testStation, stAddress1, stationName, landMark, latitude, location) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)"

#for loop to populate values in the database
with con:
    for station in r.json()['stationBeanList']:
        #id, totalDocks, city, altitude, stAddress2, longitude, postalCode, testStation, stAddress1, stationName, landMark, latitude, location)
        cur.execute(sql,(station['id'],station['totalDocks'],station['city'],station['altitude'],station['stAddress2'],station['longitude'],station['postalCode'],station['testStation'],station['stAddress1'],station['stationName'],station['landMark'],station['latitude'],station['location']))
#extract the column from the DataFrame and put them into a list
station_ids = df['id'].tolist() 

#add the '_' to the station name and also add the data type for SQLite
station_ids = ['_' + str(x) + ' INT' for x in station_ids]

#create the table
#in this case, we're concatenating the string and joining all the station ids (now with '_' and 'INT' added)
with con:
    cur.execute("CREATE TABLE available_bikes ( execution_time INT, " +  ", ".join(station_ids) + ");")

# a package with datetime objects
import time

# a package for parsing a string into a Python datetime object
from dateutil.parser import parse 

import collections

#take the string and parse it into a Python datetime object
for i in range(60):
    exec_time = parse(r.json()['executionTime'])

#create an entry for the execution time by inserting it into the database
with con:
    cur.execute('INSERT INTO available_bikes (execution_time) VALUES (?)', (exec_time.strftime('%s'),))
    
#iterate through stations in stationBeanList
id_bikes = collections.defaultdict(int) 
#loop through the stations in the station list
for station in r.json()['stationBeanList']:
    id_bikes[station['id']] = station['availableBikes']

#iterate through the defaultdict to update the values in the database
with con:
    for k, v in id_bikes.iteritems():
        cur.execute("UPDATE available_bikes SET _" + str(k) + " = " + str(v) + "WHERE execution_time = " + exec_time.strftime('%s') + ";")

#sleep for one minute 
time.sleep(60)
#loop code 60 times

    
        