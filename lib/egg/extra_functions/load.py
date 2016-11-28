import os
import re
import sys
import time
import subprocess
from constants import *
from random import random, shuffle
from pyspark.sql.functions import udf
from pyspark.sql.types import BooleanType
from datetime import datetime
from boto.s3.connection import S3Connection
from boto.exception import *
from functools import reduce  # For Python 3.x
from pyspark.sql import DataFrame
from py4j.protocol import Py4JError,Py4JJavaError

def unionAll(*dfs):
    return reduce(DataFrame.unionAll, dfs)

def format_date(date):
    if type(date) == float or type(date) == int:
        date = datetime.fromtimestamp(date/1000).strftime('%Y-%m-%d')
    if "/" in date:
        date = "-".join(date.split("/")[::-1])
    return date


def get_starting_date(path):
    return re.search(r"(\d\d\d\d-\d\d-\d\d)", path).group()


def get_fraction(data, sampling_fraction):
    return (data.filter(lambda line: random() <= sampling_fraction))


def filter_list_files(files, start_date = 19700101, end_date = 20200101):
#     if start_date != 19700101:
#         start_date = date2num(format_date(start_date))
#     if end_date != 20200101:
#         end_date = date2num(format_date(end_date))
    results = []
    for f in files:
        
        date = date2num(get_starting_date(f))
        if date >= start_date and date <= end_date:
            results.append(f)
    return results

def date2num(date):
    return int(date.replace("-",""))

def list_files(bucket, path):
    try:
        aws_key = os.environ["AWS_ACCESS_KEY_ID"]
        aws_secret = os.environ["AWS_SECRET_ACCESS_KEY"]
        conn = S3Connection(aws_key,aws_secret)
        bucket = conn.get_bucket(bucket)
        results = []
        for key in bucket.list(path, "/"):
            results.append(key.name.encode('utf-8'))
        return results
    except boto.exception.S3ResponseError:
        raise Exception("Wrong bucket or path\n")

def get_paths(bucket, path, start_date = 19700101, end_date = 20200101):
    return filter_list_files(list_files(bucket, path), start_date = start_date, end_date = end_date)



# def load_new_data(bucket,files, sqlContext):
#     idx= 0
#     while True:
#         full_path = "s3a://" + bucket + "/" + files[idx] + "*/bad_data=false"
#         try:
#             data = sqlContext.read.load(full_path, "com.databricks.spark.avro")
#             break
#         except Py4JJavaError:
#             print full_path
#             idx += 1
#     for f in files[idx:]:
#         print f
#         full_path = "s3a://" + bucket + "/" + files[idx] + "*/bad_data=false"
        
#         try:
#             new_data = sqlContext.read.load(full_path, "com.databricks.spark.avro")
#         except Py4JJavaError:
#             print full_path
#             continue
#         data = data.unionAll(new_data)
#     return data.rdd#.sample(withReplacement = False, fraction = sampling_fraction).rdd


def load_new_data(bucket,files, sqlContext, sc):
    idx= 0
    rdds = []
    while True:
        full_path = "s3a://" + bucket + "/" + files[idx] + "*/bad_data=false"
        try:
            data = sqlContext.read.load(full_path, "com.databricks.spark.avro").rdd
            rdds.append(data)
            break
        except Py4JJavaError:
            print full_path
            idx += 1
    for f in files[idx:]:
        print f
        full_path = "s3a://" + bucket + "/" + f + "*/bad_data=false"
        
        try:
            new_data = sqlContext.read.load(full_path, "com.databricks.spark.avro").rdd
            rdds.append(new_data)
        except Py4JJavaError:
            continue
        
    return sc.union(rdds)


def load_new_data_2(bucket,files, sqlContext, sc):
    idx= 0
    while True:
        full_path = "s3a://" + bucket + "/" + files[idx] + "*/bad_data=false"
        try:
            data = (sqlContext.read.load(full_path, "com.databricks.spark.avro"))
            break
        except Py4JJavaError:
            print full_path
            idx += 1
    for f in files[idx:]:
        print f
        full_path = "s3a://" + bucket + "/" + files[idx] + "*/bad_data=false"
        
        try:
            new_data = (sqlContext.read.load(full_path, "com.databricks.spark.avro"))
            data = data.unionAll(new_data)
        except Py4JJavaError:
            continue
        
    return (data
                    .drop("processedTimestamp")
                    .drop("time_bin")
                    .drop("run_id")
                    .drop("bad_data"))


def determine_interval(start_date, end_date):
    limit = date2num(format_date(STARTING_DATE_NEW_DATA))
    old_data = set(range(19700101, limit))
    new_data = set(range(limit, 20200101))
    
    time_interval = set(range(min(start_date, end_date), max(start_date, end_date)))
    
    if len(time_interval & old_data) > 0 and len(time_interval & new_data) > 0:
        return "old_and_new"
    if len(time_interval & old_data) == 0 and len(time_interval & new_data) > 0:
        return "new"
    if len(time_interval & old_data) > 0 and len(time_interval & new_data) == 0:
        return "old"   


def load_old_data(sqlContext, input_folder):
    """
    Load the avro file and it returns an RDD
    """
    data = (sqlContext
            .read
            .load("s3a://" + input_folder + "*.avro", "com.databricks.spark.avro")
            #.sample(withReplacement = False, fraction = sampling_fraction)
            .rdd
            .filter(lambda line: line.endTimestamp < date_to_milliseconds("01/02/2016"))
           )
    return data

def load_old_data_2(sqlContext, input_folder):
    """
    Load the avro file and it returns an RDD
    """
    data = (sqlContext
            .read
            .load("s3a://" + input_folder + "*.avro", "com.databricks.spark.avro")
            )

    data2 = (data
            #.sample(withReplacement = False, fraction = sampling_fraction)
            .filter(data.endTimestamp < date_to_milliseconds("01/02/2016"))
            )
    return data2

def join_old_new_data(bucket, old_path, new_path, sqlContext, sc,
                      sampling_fraction = 1.0, 
                      start_date = '1970-01-01', 
                      end_date = '2020-01-01'):
    new_start_date = date2num(format_date(start_date))
    new_end_date = date2num(format_date(end_date))
    print new_start_date, new_end_date
    interval_type = determine_interval(new_start_date, new_end_date)
    
    if interval_type == "old":
        data =  get_fraction(load_old_data(sqlContext, old_path), sampling_fraction)
        
    if interval_type == "new":
        data = get_fraction(load_new_data(bucket, get_paths(bucket, new_path,
                                               start_date = new_start_date, 
                                               end_date = new_end_date),
                             sqlContext, sc),
                             sampling_fraction)
    
    if interval_type == "old_and_new":
        new_start_date = date2num(format_date(STARTING_DATE_NEW_DATA))
        data = get_fraction(sc.union([load_old_data(sqlContext, old_path),
                load_new_data(bucket, get_paths(bucket, new_path,
                                                start_date = new_start_date, 
                                                end_date = new_end_date),
                              sqlContext, sc)]), sampling_fraction)
        
    return data
    
def get_random(sampling_fraction):
    rdn = random()
    return rdn <= sampling_fraction


def join_old_new_data_2(bucket, old_path, new_path, sqlContext, sc,
                      sampling_fraction = 1.0, 
                      start_date = '1970-01-01', 
                      end_date = '2020-01-01'):
    new_start_date = date2num(format_date(start_date))
    new_end_date = date2num(format_date(end_date))
    print new_start_date, new_end_date
    interval_type = determine_interval(new_start_date, new_end_date)
    
    if interval_type == "old":
        data = load_partial_data(old_path, sqlContext, sampling_fraction = sampling_fraction)
        
    if interval_type == "new":
        data = get_fraction(load_new_data(bucket, get_paths(bucket, new_path,
                                               start_date = new_start_date, 
                                               end_date = new_end_date),
                             sqlContext, sc),
                             sampling_fraction)
    
    if interval_type == "old_and_new":
        new_start_date = date2num(format_date(STARTING_DATE_NEW_DATA))

        if sampling_fraction == 1.0:
            sampling_fraction = 2.0
            
        data = (load_partial_data(old_path, sqlContext, sampling_fraction = sampling_fraction/2.0) + 
                get_fraction(load_new_data(bucket, get_paths(bucket, new_path,
                                                start_date = new_start_date, 
                                                end_date = new_end_date),
                              sqlContext, sc), sampling_fraction/2.0))


        
    return data
    


def date_to_milliseconds(date):
    try:
        len_date = len(date.split("/"))
        if len_date == 3:
            timestamp = time.mktime(datetime.strptime(date, "%d/%m/%Y").timetuple())
        if len_date == 2:
            timestamp = time.mktime(datetime.strptime(date, "%m/%Y").timetuple())
        if len_date == 1:
            timestamp = time.mktime(datetime.strptime(date, "%Y").timetuple())
        return timestamp*1000
    except ValueError:
        raise ValueError("Wrong Date Format. Enter dd/mm/yyyy")



def load_partial_data(path, sqlContext, sampling_fraction = 1.0):

    try:
        if sampling_fraction < 1.0:
            ls_lines = ["s3a://" + path + line.split()[-1] for line in subprocess.check_output(['aws','s3','ls',path]).splitlines() if "avro" in line.split()[-1]]
            shuffle(ls_lines)

            data = sqlContext.read.load(ls_lines[0], "com.databricks.spark.avro")
            for l in ls_lines[1:]:
                rdn = random()
                if rdn <= sampling_fraction:
                    data = data.unionAll(sqlContext.read.load(l, "com.databricks.spark.avro"))
        else:    
            data = (sqlContext
                        .read
                        .load("s3a://" + path + "*.avro", "com.databricks.spark.avro")
                    )

        return (data
                .rdd
                .filter(lambda line: line.endTimestamp < date_to_milliseconds("01/02/2016")))

    except subprocess.CalledProcessError:
        raise Exception("Check if you have AWS permissions to read bucket or if you entered correct path.\n")










