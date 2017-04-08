import luigi
from Classes.GetData import GetData
import pandas as pd
from Classes.Utils import create_directory

class SummarizeData(luigi.Task):
    def requires(self):
        return [GetData()]
 
    def output(self):
        return luigi.LocalTarget("Data/Summary/summary_irisdataset.csv")
 
    def run(self):
        create_directory("Summary")
        downloads_dir = "Data/Downloads/"
        summary_dir = "Data/Summary/"
        # READ DATA INTO DATAFRAME
        irisData = pd.read_csv(downloads_dir + 'irisdataset.data', sep = ",", header = None,
                             names = ['sepal_length_in_cm', 'sepal_width_in_cm', 'petal_length_in_cm', 'petal_width_in_cm', 'iris_class'])




        # SUMMARIZE DATA



        # SAVE DATAFRAME
        irisData.to_csv(summary_dir + "summary_irisdataset.csv", sep=',', index = False)
        