#!/usr/bin/python2.7
#
# Interface for the assignement
#

import psycopg2
import os

RANGE_TABLE_PREFIX = 'range_part'
RROBIN_TABLE_PREFIX = 'rrobin_part'
USER_ID_COLNAME = 'userid'
MOVIE_ID_COLNAME = 'movieid'
RATING_COLNAME = 'rating'

def getOpenConnection(user='postgres', password='1234', dbname='postgres'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")


def loadRatings(ratingstablename, ratingsfilepath, openconnection):
    cur = openconnection.cursor()
    cur.execute("CREATE TABLE " + ratingstablename + " (userid int, dummy1 varchar, movieid int, dummy2 varchar, rating float, dummy3 varchar, timestamp bigint);")
#    cur.execute("COPY Ratings FROM 'C:/Users/Ephraim/Coursera/Distributed Database Systems/Assignment 1/test_data.txt' DELIMITER ':';")
    absratingsfilepath = os.path.abspath(ratingsfilepath)
    cur.execute("COPY Ratings FROM '" + absratingsfilepath + "' DELIMITER ':';")
    cur.execute("ALTER TABLE " + ratingstablename + " DROP COLUMN  dummy1 ;")
    cur.execute("ALTER TABLE " + ratingstablename + " DROP COLUMN  dummy2 ;")
    cur.execute("ALTER TABLE " + ratingstablename + " DROP COLUMN  dummy3 ;")
    cur.execute("ALTER TABLE " + ratingstablename + " DROP COLUMN timestamp;")
    cur.close()


def rangePartition(ratingstablename, numberofpartitions, openconnection):
    cur = openconnection.cursor()
    cur.execute("CREATE TABLE system_" + RANGE_TABLE_PREFIX +" (numparts int);")
    cur.execute("INSERT INTO system_" + RANGE_TABLE_PREFIX +" (numparts) VALUES (%s);" , (numberofpartitions,))
    interval = 5.0 / numberofpartitions
    cur.execute("CREATE TABLE " + RANGE_TABLE_PREFIX +"%s AS SELECT * FROM " + ratingstablename + " WHERE rating >= %s and rating <= %s ;", (0, 0, interval))
    lowerbound = interval
    for i in range (1, numberofpartitions):
        cur.execute("CREATE TABLE " + RANGE_TABLE_PREFIX +"%s AS SELECT * FROM " + ratingstablename + " WHERE rating > %s and rating <= %s ;", (i,lowerbound, lowerbound+interval))
        lowerbound += interval
    cur.close()


def roundRobinPartition(ratingstablename, numberofpartitions, openconnection):
    cur = openconnection.cursor()
    cur2 = openconnection.cursor()
    for i in range (numberofpartitions):
        cur.execute("CREATE TABLE " + RROBIN_TABLE_PREFIX +"%s (userid int, movieid int,  rating float);",(i,))
    cur.execute("CREATE TABLE system_" + RROBIN_TABLE_PREFIX + " ( numparts int, rrobinindex int);")
#walk the original ratings table distributing entries
# For each entry in ratings db
    cur.execute("SELECT * FROM " + ratingstablename + ";")
    row = cur.fetchone()
    rrobin_index=0;
    while row is not None:
        cur2.execute("INSERT INTO " + RROBIN_TABLE_PREFIX +"%s (userid,movieid, rating ) VALUES (%s, %s, %s);", (rrobin_index, row[0], row[1], row[2]))
        row = cur.fetchone()
        rrobin_index += 1
        rrobin_index = rrobin_index % numberofpartitions
    cur.execute("INSERT INTO system_" + RROBIN_TABLE_PREFIX +" (numparts, rrobinindex) VALUES (%s, %s);" , (numberofpartitions, rrobin_index))
    cur.close()
    
def roundrobininsert(ratingstablename, userid, itemid, rating, openconnection):
    cur = openconnection.cursor()
    cur.execute("SELECT * FROM system_" + RROBIN_TABLE_PREFIX + ";")
    row = cur.fetchone()
    numberofpartitions = int(row[0])
    rrobin_index = int(row[1])
    cur.execute("INSERT INTO " + RROBIN_TABLE_PREFIX +"%s (userid,movieid, rating ) VALUES (%s, %s, %s);", (rrobin_index, userid, itemid, rating))
    cur.execute("INSERT INTO " + ratingstablename + " (userid, movieid, rating ) VALUES (%s, %s, %s);", ( userid, itemid, rating))
    rrobin_index += 1
    rrobin_index = rrobin_index % numberofpartitions
    cur.execute("INSERT INTO system_" + RROBIN_TABLE_PREFIX +" (rrobinindex) VALUES (%s);" , (rrobin_index,))
    
def rangeinsert(ratingstablename, userid, itemid, rating, openconnection):
    cur = openconnection.cursor()
    cur.execute("SELECT numparts FROM system_" + RANGE_TABLE_PREFIX + ";")
    numberofpartitions = int(cur.fetchone()[0])
    interval = 5.0 / numberofpartitions
    if (rating <= interval) :
        partition_index = 0
    else:
        partition_index = int(rating // interval) -1
    cur.execute("INSERT INTO " + RANGE_TABLE_PREFIX +"%s (userid,movieid, rating ) VALUES (%s, %s, %s);", (partition_index, userid, itemid, rating))
    cur.execute("INSERT INTO " + ratingstablename + " (userid, movieid, rating ) VALUES (%s, %s, %s);", ( userid, itemid, rating))
    cur.close()
    
def createDB(dbname='dds_assignment'):
    """
    We create a DB by connecting to the default user and database of Postgres
    The function first checks if an existing database exists for a given name, else creates it.
    :return:None
    """
    # Connect to the default database
    con = getOpenConnection(dbname='postgres')
    con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()

    # Check if an existing database with the same name exists
    cur.execute('SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname=\'%s\'' % (dbname,))
    count = cur.fetchone()[0]
    if count == 0:
        cur.execute('CREATE DATABASE %s' % (dbname,))  # Create the database
    else:
        print ('A database named {0} already exists'.format(dbname))

    # Clean up
    cur.close()
    con.close()

def deletepartitionsandexit(openconnection):
    cur = openconnection.cursor()
    cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
    l = []
    for row in cur:
        l.append(row[0])
    for tablename in l:
        cur.execute("drop table if exists {0} CASCADE".format(tablename))

    cur.close()

def deleteTables(ratingstablename, openconnection):
    try:
        cursor = openconnection.cursor()
        if ratingstablename.upper() == 'ALL':
            cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
            tables = cursor.fetchall()
            for table_name in tables:
                cursor.execute('DROP TABLE %s CASCADE' % (table_name[0]))
        else:
            cursor.execute('DROP TABLE %s CASCADE' % (ratingstablename))
        openconnection.commit()
    except psycopg2.DatabaseError as e:
        if openconnection:
            openconnection.rollback()
        print ('Error %s' % e)
    except IOError as e:
        if openconnection:
            openconnection.rollback()
        print ('Error %s' % e)
    finally:
        if cursor:
            cursor.close()
