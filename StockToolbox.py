#!/bin/python
#
# Connects to Y! Finance, and downloads EOD stock information 
# for all symbols listed in the provided symbol file.
# 
# It then connects to an Oracle database provided by the connect string,
# creates one table per symbol and loads the EOD information into it.
#
#   GPL3
#   ----
#    This program is free software: you can redistribute it and/or modify
#    it under the terms of the GNU General Public License as published by
#    the Free Software Foundation, either version 3 of the License, or
#    (at your option) any later version.
#
#    This program is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#    GNU General Public License for more details.
#
#    You should have received a copy of the GNU General Public License
#    along with this program.  If not, see <http://www.gnu.org/licenses/>.
# 

import urllib2
import csv
import cx_Oracle

symbols = []


# startdate string as 'mm-dd-yyyy'
# enddate string as 'mm-dd-yyyy'
def buildURL(symbol, startdate="00-00-0000", enddate="00-00-0000"):
    baseURL="http://real-chart.finance.yahoo.com/table.csv?s=" + symbol + ".NS"
    trailURL = "&g=d&ignore=.csv"
    startdateFormat = "&a=" + startdate.split("-")[0] + "&b=" + startdate.split("-")[1] + "&c=" + startdate.split("-")[2]
    enddateFormat = "&d=" + enddate.split("-")[0] + "&e=" + enddate.split("-")[1] + "&f=" + enddate.split("-")[2]
    print baseURL + startdateFormat + enddateFormat + trailURL
    return baseURL + startdateFormat + enddateFormat + trailURL
    

def returnCSV(url, backupurl=""):
    print "Getting data from " + url + "\n"
    try:
        response = urllib2.urlopen(url)
    except urllib2.HTTPError, e:
        if e.code == 404:
            logf.write("YAHOO WARNING : " + url + "\n")
            print "Page does not exist. Trying with Backup Symbol.\n"
            try:
                response = urllib2.urlopen(backupurl)
            except urllib2.HTTPError, e:
                if e.code == 404:
                    logf.write("YAHOO ERROR : " + url + "\n")
                    print "Symbol and Backup Symbol lookup failed."
                    return []
    cr = csv.reader(response)
    return cr
    
    
def buildSymbolListNSE(file):
    syms = []
    sf = open(file, 'r')
    for line in sf.readlines():
        syms.append(line.strip())
    sf.close()
    return syms
    

logf = open("test.log", "w")

## TODO : Change this to a valid connection string!
connect_string = "USERNAME/PASSWORD@HOST/DB_SIDE"


# Connect to Oracle
try:
    orcl = cx_Oracle.connect(connect_string)
except cx_Oracle.DatabaseError:
    print "\tUnable to connect to database."
    print "\tError is : " + str(e)

curs = orcl.cursor()

# Build Symbol list from file
symbols = buildSymbolListNSE('NSE-SymbolList.txt')

# Iterate through symbols
for s in symbols:    
    # Generate Tablename 
    tabname = "NSE_" + s
    
    # Drop table if it exists
    sql = "DROP TABLE " + tabname
    try:
        curs.execute(sql)
    except cx_Oracle.DatabaseError as e:
        logf.write("DROP TABLE Error : " + tabname + " : " + str(e) + "\n")        
        print "\tError while dropping table " + tabname + "\n"
        print "\tError is : " + str(e)
    
    # Create table 
    sql = "CREATE TABLE " + tabname + " ( STARTDATE DATE, OPEN NUMBER(10,2), HIGH NUMBER(10,2), LOW NUMBER(10, 2), CLOSE NUMBER(10, 2), VOLUME NUMBER, ADJ_CLOSE NUMBER(10, 2)) "
    try:
        curs.execute(sql)
    except cx_Oracle.DatabaseError as e:
        logf.write("=CREATE==========================================\n")
        logf.write("Error : " + tabname + " : " + str(e) + "\n")
        print "\tError while creating table " + tabname + "\n"
        print "\tError is : " + str(e)
        logf.write("=/CREATE==========================================\n")
        
    sql = "INSERT ALL "
        
    # Pull out data and build INSERT ALL statement
    cr = returnCSV(buildURL(s), buildURL(s+"-EQ"))
    if cr == []:
        continue

    for line in cr:
        #print line
        if line[0] == "Date":
            continue
        sql = sql + " INTO " + tabname + " (STARTDATE, OPEN, HIGH, LOW, CLOSE, VOLUME, ADJ_CLOSE) VALUES ( TO_DATE('" + line[0] + "', 'YYYY-MM-DD'), " + line[1] + "," + line[2] + "," + line[3] + "," + line[4] + "," + line[5] + "," + line[6] + ") "
    sql = sql + " SELECT * FROM DUAL"
    try:
        #print sql
        #logf.write(sql)
        #logf.write("\n")
        curs.execute(sql)
    except cx_Oracle.DatabaseError as e:
        logf.write("=INSERT==========================================\n")
        logf.write("Error : " + tabname + " : " + str(e) + "\n")
        logf.write(sql)
        logf.write("=/INSERT==========================================\n")
        print "\tError while inserting data into table " + tabname + "\n"
        print "\tError is : " + str(e)