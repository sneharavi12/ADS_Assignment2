import luigi
from Classes.LoanData.CleanData import CleanData
import pandas as pd
from Classes.Utils import create_directory
from Classes.Utils import get_logger
import numpy as np
import time
import urllib.response
import urllib.request
from bs4 import BeautifulSoup
import os
import re
from urllib.request import urlopen
import csv
import string
from string import punctuation
import zipfile
import os
import sys
import logging
import boto
import boto.s3
from boto.s3.key import Key


def check_if_file_exists(aws_access_key,aws_secret_key,fileName,filePath):
    

    bucket_name = aws_access_key.lower()   
    conn = boto.connect_s3(aws_access_key,aws_secret_key)
    #print(bucket_name)
    filePresetflag = False
    bucket = conn.lookup(bucket_name)
    #print(bucket)
    if not (bucket == None):

        for key in bucket.list(delimiter='/'):
            # print("S3 == " + key.name)
            for f in bucket.list(key.name):
                # print("2 == " + f.name)
                if(f.name == filePath):
                    # print (f.name + " = " + fileName + " !!! Already exists !!!")
                    filePresetflag = True
                    
                        

    if (filePresetflag == True):
        return True
    else:
        return False
            
            
def amazon_upload(aws_access_key,aws_secret_key,file):    
    bucket_name = aws_access_key.lower()   
    conn = boto.connect_s3(aws_access_key,aws_secret_key)

    bucket = conn.lookup(bucket_name)
    if bucket is None:
        bucket = conn.create_bucket(bucket_name, location=boto.s3.connection.Location.DEFAULT)

    #testfile = "LendingClubLoan.csv"
    def percent_cb(complete, total):
        sys.stdout.write('.')
        sys.stdout.flush()

    k = Key(bucket)
    k.key = file
    k.set_contents_from_filename(file,cb=percent_cb, num_cb=10)
    
    buckets = conn.get_all_buckets()#Get the bucket list
    for i in buckets:
        print(i.name)


class Start(luigi.Task):

 
    def requires(self):
        return [CleanData()]
 
    def output(self):
        return luigi.LocalTarget("Data/Results/resultsloandata.csv")
 
    def run(self):
        #READ AMAZON KEYS FROM USER
        aws_access_read = input("Enter your aws access key: ")
        aws_secret_read = input("Enter your aws secret key: ")

        aws_access_key = aws_access_read.strip()
        aws_secret_key = aws_secret_read.strip()
        #print("check")


        fileName1 = "cleaned_loandata.csv"
        # fileName2 = "cleaned_reject_loandata.csv"
        clean_dir = "Data/Cleaned/"
        filePath1 = clean_dir + fileName1
        # filePath2 = clean_dir + fileName2

        r = check_if_file_exists(aws_access_key,aws_secret_key,fileName1, filePath1)
        if (r==False):
            amazon_upload(aws_access_key,aws_secret_key,filePath1)
        # r = check_if_file_exists(aws_access_key,aws_secret_key,fileName2, filePath2)
        # if (r==False):
        #     amazon_upload(aws_access_key,aws_secret_key,filePath2)

        # check if upload successful
        r1 = check_if_file_exists(aws_access_key,aws_secret_key,fileName1, filePath1)
        # r2 = check_if_file_exists(aws_access_key,aws_secret_key,fileName2, filePath2)
        if(r==True):
            create_directory("Data/Results")
            open("Data/Results/resultsloandata.csv", 'a').close()


if __name__ == '__main__':
    try:
        os.remove("Data/Results/resultsloandata.csv")
    except:
        pass
    luigi.run()