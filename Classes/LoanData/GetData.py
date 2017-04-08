import luigi
from bs4 import BeautifulSoup
import urllib.request
import urllib.response
import mechanicalsoup
import pandas as pd
import requests
import luigi
from bs4 import BeautifulSoup
import urllib.request
import urllib.response
import mechanicalsoup
import pandas as pd
from Classes.Utils import create_directory
# import getpass
import re, os, zipfile, io
from Classes.Utils import get_logger

class GetData(luigi.Task):
    def requires(self):
        return []
 
    def output(self):
        return { 'output1' : luigi.LocalTarget("Data/Downloads/LoanData/LoanStats3a_securev1.csv"),\
        'output2' : luigi.LocalTarget("Data/Downloads/LoanData/LoanStats3b_securev1.csv"),\
        'output3' : luigi.LocalTarget("Data/Downloads/LoanData/LoanStats3c_securev1.csv"),\
        'output4' : luigi.LocalTarget("Data/Downloads/LoanData/LoanStats3d_securev1.csv"),\
        'output5' : luigi.LocalTarget("Data/Downloads/LoanData/LoanStats_securev1_2016Q1.csv"),\
        'output6' : luigi.LocalTarget("Data/Downloads/LoanData/LoanStats_securev1_2016Q2.csv"),\
        'output7' : luigi.LocalTarget("Data/Downloads/LoanData/LoanStats_securev1_2016Q3.csv"),\
        'output8' : luigi.LocalTarget("Data/Downloads/LoanData/LoanStats_securev1_2016Q4.csv") }
 
    def run(self):
        create_directory("Data")
        create_directory("Data/Downloads")
        create_directory("Data/Downloads/LoanData")
        # create_directory("Data/Downloads/DeclinedLoanData")
        downloads_dir = "Data/Downloads/"
        url = "https://www.lendingclub.com/account/gotoLogin.action"
        # login credentials
        username = input("Enter your Lending Club usenname: ")
        password = input("Enter your Lending Club password: ")
        browser = mechanicalsoup.Browser()
        login_page = browser.get(url)
        login_form = login_page.soup.find('form', {"id":"member-login"})
        login_form.find("input", {"name" : "login_email"})["value"] = username
        login_form.find("input", {"name" : "login_password"})["value"] = password 
        response = browser.submit(login_form, login_page.url)

        if (response.url == "https://www.lendingclub.com/account/myAccount.action"):

# CODE TO DOWNLOAD DATASET INTO THE DIRECTORY
            url = "https://www.lendingclub.com/info/download-data.action"
            
            folder1 = "LoanData/"
            # folder2 = "DeclinedLoanData/"
            link = 'https://www.lendingclub.com/info/download-data.action'
            r = browser.get(link)
            soup = BeautifulSoup(r.text, "html.parser")
            loan_namelist = (soup.find('div', {'id': "loanStatsFileNamesJS"})).text
            loan_names = parts = loan_namelist.split('|')
            prefix = (soup.find('div', {'id': "urlPublicPrefix"})).text
    #Download Loan
            i=0
            for name in loan_names:
            	if(name.strip() != ""):
    	            i = i+1
    	            url_n=(prefix+name)
    	            if not (os.path.isfile(downloads_dir+folder1+name.split('.')[0]+'.csv')):
    	                zf = browser.get(url_n)
    	                z = zipfile.ZipFile(io.BytesIO(zf.content))
    	                z.extractall(path=downloads_dir+folder1)
        else:
            print("Try again with correct Lending Club credentials")
    	                # print(str(i) + " out of "+str(len(loan_names)-1) + " files downloaded")
    # #Download Declined Loan
    #         rejectedloan_namelist = (soup.find('div', {'id': "rejectedLoanStatsFileNamesJS"})).text
    #         rejectedloan_names = parts = rejectedloan_namelist.split('|')


    #         i=0


    #         # del rejectedloan_names[-1]


    #         for name in rejectedloan_names:
    #             if(name.strip() != ""):
    #                 i = i+1
    #                 url_n=(prefix+name)
    #                 if not (os.path.isfile(downloads_dir+folder2+name.split('.')[0]+'.csv')):
    #                     zf = browser.get(url_n)
    #                     # print(zf)
    #                     z = zipfile.ZipFile(io.BytesIO(zf.content))
    #                     z.extractall(path=downloads_dir+folder2)
    #                     # print(str(i) + " out of "+str(len(rejectedloan_names)-1) + " files downloaded")
