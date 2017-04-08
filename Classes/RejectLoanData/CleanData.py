import luigi
from Classes.RejectLoanData.GetData import GetData
import pandas as pd
from Classes.Utils import create_directory
import os, re
import numpy as np

# def create_dummy_variable(irisData):
#     # iris_class_df = pd.get_dummies(irisData['iris_class'])
#     # irisData = irisData.join(iris_class_df)
#     irisData.loc[(irisData['iris_class'] == 'Iris-setosa'), ['iris_class']] = 1
#     irisData.loc[(irisData['iris_class'] == 'Iris-versicolor'), ['iris_class']] = 2
#     irisData.loc[(irisData['iris_class'] == 'Iris-virginica'), ['iris_class']] = 3
#     return irisData



def remove_columns(fullData):
    fullData = fullData.drop(['Loan Title'], axis=1)
    return fullData

def replace_by_na(fullData):
    fullData['State'] = np.where(fullData['State'].isnull(), 'NA', fullData['State'])
    return fullData

def separate_application_date(fullData):
    fullData['Application Date'] = pd.to_datetime(fullData['Application Date'])
    fullData['Year'] = fullData['Application Date'].dt.year
    fullData['Month'] = fullData['Application Date'].dt.month
    fullData['Day'] = fullData['Application Date'].dt.day
    return fullData

def replace_by_default(fullData):
    fullData['Risk_Score'] = fullData['Risk_Score'].fillna(999)
    return fullData

    

class CleanData(luigi.Task):
    def requires(self):
        return [GetData()]
 
    def output(self):
        return { 'output1' : luigi.LocalTarget("Data/Cleaned/cleaned_reject_loandata.csv")}
                
    def run(self):
        create_directory("Data/Cleaned")
        create_directory("Data/Summary")
        downloads_dir_loan = "Data/Downloads/DeclinedLoanData"
        cleaned_dir = "Data/Cleaned/"
        summary_dir = "Data/Summary/"

        if (os.path.isfile(downloads_dir_loan + "/combined_downloaded_reject_loandata.xls" )):
            fullData = pd.read_csv(downloads_dir_loan + "/combined_downloaded_reject_loandata.xls", sep = ",", encoding = "ISO-8859-1", low_memory= False)
        else:
            #Looping over all the csv to load and process the data
            ls_dir = os.listdir(downloads_dir_loan)
            fullData = None
            for file in ls_dir:
                #     only if file is csv
                regexp = re.compile(r'.csv')
                if(regexp.search(file)):
                    print(file)
                    filePath = downloads_dir_loan + "/" + file
                    # # # READ DATA INTO DATAFRAME
                    data = pd.read_csv(filePath + '', sep = ",", skiprows=[0], encoding = "ISO-8859-1", low_memory= False)

                    # Removing Rows where all columns are null or only have value for 1 column
                    data.dropna(how='all', inplace = True)
                    data.dropna(thresh=2, inplace = True)

                    try:
                        fullData = pd.concat([fullData, data])
                        fullData1 = fullData2 = fullData
                    except:
                        fullData = data

                    data = None

            # Describe downloaded dataset
            fullData['Employment Length'] = fullData['Employment Length'].str.replace('+','')
            fullData['Employment Length'] = fullData['Employment Length'].str.replace('<','')
            fullData['Employment Length'] = fullData['Employment Length'].str.replace('years','')
            fullData['Employment Length'] = fullData['Employment Length'].str.replace('year','')
            fullData['Employment Length'] = fullData['Employment Length'].str.replace('n/a','0')
            fullData['Zip Code'] = fullData['Zip Code'].str.replace('xx','00')


            fullData['Employment Length'] = fullData['Employment Length'].astype(np.int64)

            fullData.to_csv(downloads_dir_loan + "/combined_downloaded_reject_loandata.xls", sep=',', index = False)
            summary = fullData.describe()

            # SAVE summary of downloaded data to files
            # print(summary)
            summary.to_csv(summary_dir + "summary_downloaded_reject_loandata.csv"  , sep=',', index = True)

             # CLEAN DATA
            fullData = remove_columns(fullData)

            fullData = replace_by_na(fullData)

            fullData = separate_application_date(fullData)

            fulldata = replace_by_default(fullData)

            summary = fullData.describe()

            # SAVE Cleaned/Preprocessed Data
            fullData.to_csv(cleaned_dir + "cleaned_reject_loandata.csv", sep=',', index = False)


            # Summarize and save cleaned dataset
            summary.to_csv(summary_dir + "summary_cleaned_reject_loandata.csv"  , sep=',', index = True)