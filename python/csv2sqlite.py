#!/usr/bin/env python3
# if system version of python is python3, then above can simply be 'python'

"""
csv2sqlite.py: Extract records from CSV files and insert into Sqlite3 database

Usage: csv2sqlite.py [-h] [-d] [-p PKi1,PKi2,..] [-b dbfile] csvfile1 [csvfile2 ...]

Description:
    Create an Sqlite3 database from provided CSV files,
    with table names based on file names,
    table field names from CSV column headers
    and table data inserted from CSV row entries.
    Note if primary key index(es) not specified, defaults to first column,
    use '-p none' for no primary key

Author: Justin Lee, June 2018.
"""

import sys
import getopt
import logging
import os

import tempfile
import csv
import sqlite3
import re
import itertools


##############################################################


def getDBTableNames(dbc):
    tables = dbc.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tablenames = [ t[0] for t in tables ]
    return tablenames


def printTable(dbc, table):
    print("Table {}:".format(table))
    dbc.execute("SELECT * FROM {}".format(table))
    rows = dbc.fetchall()
    for row in rows: #  note that each row returned by query is a tuple
        # python converts NULL to None, replace these with "NULL"
        # for printing.
        rowstrs = [ "NULL" if not v else str(v) for v in row ]
        print("{}".format(", ".join(rowstrs)))


def printDB(dbc):
    for table in getDBTableNames(dbc):
        printTable(dbc, table)


def addRecordToTable(dbc, tname, col2fname, rec):
    # insert ordered dict into database. Eg:
    # ['ID', 'Sample1', 'Sample2', 'Sample3']
    # [OrderedDict([('ID', 'H0001'), ('Sample1', 'H0001'), ('Sample2', ''), ('Sample3', '')]),

    # Note: "if len(k) > 0" is in case of (trailing) empty csv columns
    fieldnames = [ col2fname[k] for k, v in rec.items() if len(k) > 0 ]
    # replace empty ('') values with None (becomes NULL in sqlite)
    fieldvals = [ None if not v else v for k, v in rec.items() if len(k) > 0 ]
    valstr = [ "NULL" if not v else v for k, v in rec.items() if len(k) > 0 ]
    valqs = '?, ' * len(fieldvals)
    valqs = valqs[:-2] # trim off trailing ", "

    qrystr = "INSERT OR IGNORE INTO {}({}) VALUES({});".format(tname, ", ".join(fieldnames), valqs)
    dstr = "adding record {} to table with: {}".format(", ".join(valstr), qrystr)
    logging.debug(dstr)
    dbc.execute(qrystr, fieldvals)


def addDictListToTable(dbc, tname, col2fname, datarecords):
    if len(datarecords) < 1:
        return None
    for rec in datarecords:
        dstr = "processing record {} for table {}".format(rec, tname)
        logging.debug(dstr)
        addRecordToTable(dbc, tname, col2fname, rec)


def createDBTable(dbc, tname, fieldnames, fieldtypes, pkidxes):
    #dstr = "tname={} fieldnames={} fieldtypes={} pkidxes={}".format(tname, fieldnames, fieldtypes, pkidxes)
    #logging.debug(dstr)
    fielddes = [ "{} {}".format(f, fieldtypes[f]) for f in fieldnames ]
    #fielddes[0] = "{} PRIMARY KEY NOT NULL".format(fielddes[0])
    if pkidxes:
        pks = [ fieldnames[p-1] for p in pkidxes ] # pk idx is 1..N, fields: 0..N-1
    fielddes.append("PRIMARY KEY ({})".format(", ".join(pks)))
    tfields = ",\n\t".join(fielddes)

    qrystr = """
    DROP TABLE IF EXISTS {};
    CREATE TABLE {}(
        {}
    );
    """.format(tname, tname, tfields)
    dstr = "creating table with: {}".format(qrystr)
    logging.debug(dstr)
    dbc.executescript(qrystr)


def cleanColNames(colnames):
    #return [ re.sub("[^A-Za-z0-9_]", "", c) for c in colnames ] # strip invalid
    #return [ repr(c) for c in colnames ] # single quote
    return [ "\"{}\"".format(c) for c in colnames ] # double quote


def map2cleanColNames(colnames):
    #return { c: re.sub("[^A-Za-z0-9_]", "", c) for c in colnames } # strip invalid
    #return { c: repr(c) for c in colnames } # single quote
    return { c: "\"{}\"".format(c) for c in colnames } # double quote


def getCleanFieldnameTypes(coltypes, mapcol2fieldname):
    return { mapcol2fieldname[c]: t for c, t in coltypes.items() } 


def cleanTableName(tablename):
    return re.sub("[^A-Za-z0-9_]", "", tablename)


#def getType(val):
#    #typtests = (lambda val: datetime.strptime(val, "%Y-%m-%d"), int, float)
#    types = (int, float)
#    for typ in types:
#        try:
#            return type(val)
#        except ValueError:
#            continue
#    # no match, so must be string
#    return str


def getSqliteType(val):
    # Sqlite types can be NULL, INTEGER, REAL, TEXT, BLOB
    # dates aren't supported as a type
    if val is None:
        return "NULL"
    typetests = [
            ("INTEGER", int),
            ("REAL", float)#,
            #("DATE", lambda val: datetime.strptime(val, "%Y-%m-%d"))
    ]
    for typ, test in typetests:
        try:
            test(val)
            return typ
        except ValueError:
            continue
    # no match, so must be string
    return "TEXT"


def getCSVColumnTypes(data, colnames):
    coltypes = {}

    for row in data:
        unknowncoltypes = [ c for c in colnames if c not in coltypes.keys() ]
        dstr = "processing row: {}".format(row)
        logging.debug(dstr)
        if not unknowncoltypes: # know all the column types now
            break
        for field, value in row.items():
            if len(field) == 0: # empty (trailing) column header in CSV
                break
            if value is None : # empty value
                continue
            coltypes[field] = getSqliteType(value)

    if len(unknowncoltypes) > 0:
        dstr = "Unable to determine types for columns {}! Assuming 'TEXT'".format(unknowncoltypes)
        logging.warn(dstr)
        for c in unknowncoltypes:
            coltypes[c] = "TEXT"

    dstr = "CSV column types {}".format(coltypes)
    logging.debug(dstr)

    return coltypes


def readCSVFileToDictList(fn):
    dstr = "reading CSV file {}".format(fn)
    logging.info(dstr)
    colnames = None
    data = []
    # read csv file into dictionary
    with open(fn, 'r') as f:
        dr = csv.DictReader(f) # comma is default delimiter

        # get column names from DictReader object and store in list
        # stop at first empty column name (can be trailing empty columns)
        colnames = list(itertools.takewhile(lambda c: len(c)>0, dr.fieldnames))
        dstr = "CSV data has non-empty fields {}".format(colnames)
        logging.debug(dstr)

        data = [ r for r in dr ] # read dict records into list
        dstr = "CSV data contains {} rows of data:\n{}".format(len(data), data)
        logging.debug(dstr)

        coltypes = getCSVColumnTypes(data, colnames)

    return colnames, coltypes, data


##############################################################

def main(argv):

    debug = 0
    primarykeyindexes = [ 1 ]
    paramstr = "[-h] [-d] [-p PKi1,PKi2,...] [-b dbfile] csvfile1 [csvfile2 ...]"
    usagestr = "Usage: {} {}".format(os.path.basename(sys.argv[0]), paramstr)

    dbfile=None

    try:
        options, remainder = getopt.gnu_getopt(argv[1:],"hdp:b:")
    except getopt.GetoptError:
        print(usagestr, file=sys.stderr)
        sys.exit(2)

    for opt, arg in options:
        if opt == '-h':
            print(__doc__, file=sys.stderr) # print docstring at start of file
            sys.exit()
        elif opt == '-d':
            debug = 1
        elif opt == '-p':
            if arg.lower() == "none":
                primarykeyindexes = None
            else:
                strpi = arg.split(',')
                primarykeyindexes = [ int(i) for i in strpi ]
        elif opt == '-b':
            dbfile = arg
        else:
            assert False, "unhandled option"

    # for logging to stderr (by default on level WARN and above)
    if debug>0:
        logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.DEBUG)
    else:
        #logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.WARNING)
        logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.INFO)

    dstr = "number of remaining args = {}; args = {}".format(len(remainder), str(remainder))
    logging.debug(dstr)

    if (len(remainder) < 1):
        print(usagestr)
        sys.exit(2)

    dstr = "primary key indexes: {}".format(primarykeyindexes)
    logging.info(dstr)

    dstr = "{} csv files to process".format(len(remainder))
    logging.info(dstr)

    # if running in debug mode, create temp database file
    if debug and dbfile is None:
        # create temporary filename in current working directory
        #pid = os.getpid()
        cwd = os.getcwd()
        with tempfile.NamedTemporaryFile(delete=False, dir=cwd, suffix='.sqlite') as tmpf:
            tempfname = tmpf.name
        dstr = "using temp file {} for database".format(tempfname)
        logging.debug(dstr)
        conn = sqlite3.connect(tempfname) # temp db on file
    elif dbfile: # create datbase with provided filename
        conn = sqlite3.connect(dbfile) # db on file
    else: # otherwise create database in memory
        #conn = sqlite3.connect(':memory:') # create db solely in memory
        conn = sqlite3.connect('') # db in mem but can use swap

    with conn:
        # use a dictionary cursor
        conn.row_factory = sqlite3.Row
        dbc = conn.cursor()

        # process provided csv files, and add data to database
        for fn in remainder:
            colnames, coltypes, data = readCSVFileToDictList(fn)
            if not colnames or len(data) == 0: # skip if empty
                continue

            # create tablename from filename minus non-alphanumeric characters
            # strip off path and filename extension (csv)
            tn = os.path.basename(os.path.splitext(fn)[0])
            tname = cleanTableName(tn)
            dstr = "creating table with name: {}".format(tname)
            logging.debug(dstr)

            # clean CSV column names to be suitable DB table names
            fieldnames = cleanColNames(colnames)
            mapcol2fieldname = map2cleanColNames(colnames)
            dstr = "field names: {}".format(','.join(fieldnames))
            logging.debug(dstr)
            fieldtypes = getCleanFieldnameTypes(coltypes, mapcol2fieldname)
            dstr = "field types: {}".format(fieldtypes)
            logging.debug(dstr)

            createDBTable(dbc, tname, fieldnames, fieldtypes, primarykeyindexes)
            dstr = "adding {} data to table {}".format(fn, tname)
            logging.info(dstr)
            addDictListToTable(dbc, tname, mapcol2fieldname, data)

        #dbc.commit()
        if debug:
            printDB(dbc)




##############################################################

if __name__ == "__main__":
    sys.exit(main(sys.argv))

