import luigi
from Classes.LoanData.GetData import GetData
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



def replace_by_mean(fullData):
    return fullData

def replace_by_median(fullData):
    fullData['loan_amnt'] = fullData['loan_amnt'].fillna(fullData['loan_amnt'].median())
    fullData['funded_amnt'] = fullData['funded_amnt'].fillna(fullData['funded_amnt'].median())
    fullData['funded_amnt_inv'] = fullData['funded_amnt_inv'].fillna(fullData['funded_amnt_inv'].median())
    fullData['installment'] = fullData['installment'].fillna(fullData['installment'].median())
    fullData['annual_inc'] = fullData['annual_inc'].fillna(fullData['annual_inc'].median())
    fullData['dti'] = fullData['dti'].fillna(fullData['dti'].median())
    fullData['open_acc'] = fullData['open_acc'].fillna(fullData['open_acc'].median())
    fullData['pub_rec'] = fullData['pub_rec'].fillna(fullData['pub_rec'].median())
    fullData['revol_bal'] = fullData['revol_bal'].fillna(fullData['revol_bal'].median())
    fullData['out_prncp'] = fullData['out_prncp'].fillna(fullData['out_prncp'].median())
    fullData['out_prncp_inv'] = fullData['out_prncp_inv'].fillna(fullData['out_prncp_inv'].median())
    fullData['total_pymnt'] = fullData['total_pymnt'].fillna(fullData['total_pymnt'].median())
    fullData['total_pymnt_inv'] = fullData['total_pymnt_inv'].fillna(fullData['total_pymnt_inv'].median())
    fullData['total_rec_prncp'] = fullData['total_rec_prncp'].fillna(fullData['total_rec_prncp'].median())
    fullData['total_rec_int'] = fullData['total_rec_int'].fillna(fullData['total_rec_int'].median())
    fullData['total_rec_late_fee'] = fullData['total_rec_late_fee'].fillna(fullData['total_rec_late_fee'].median())
    fullData['recoveries'] = fullData['recoveries'].fillna(fullData['recoveries'].median())
    fullData['collection_recovery_fee'] = fullData['collection_recovery_fee'].fillna(fullData['collection_recovery_fee'].median())
    fullData['last_pymnt_amnt'] = fullData['last_pymnt_amnt'].fillna(fullData['last_pymnt_amnt'].median())
    fullData['collections_12_mths_ex_med'] = fullData['collections_12_mths_ex_med'].fillna(fullData['collections_12_mths_ex_med'].median())
    fullData['policy_code'] = fullData['policy_code'].fillna(fullData['policy_code'].median())
    fullData['tot_coll_amt'] = fullData['tot_coll_amt'].fillna(fullData['tot_coll_amt'].median())
    fullData['tot_cur_bal'] = fullData['tot_cur_bal'].fillna(fullData['tot_cur_bal'].median())
    fullData['total_rev_hi_lim'] = fullData['total_rev_hi_lim'].fillna(fullData['total_rev_hi_lim'].median())
    fullData['acc_open_past_24mths'] = fullData['acc_open_past_24mths'].fillna(fullData['acc_open_past_24mths'].median())
    fullData['tot_hi_cred_lim'] = fullData['tot_hi_cred_lim'].fillna(fullData['tot_hi_cred_lim'].median())
    fullData['total_bal_ex_mort'] = fullData['total_bal_ex_mort'].fillna(fullData['total_bal_ex_mort'].median())
    fullData['total_bc_limit'] = fullData['total_bc_limit'].fillna(fullData['total_bc_limit'].median())
    fullData['total_il_high_credit_limit'] = fullData['total_il_high_credit_limit'].fillna(fullData['total_il_high_credit_limit'].median())
    fullData['mo_sin_old_il_acct'] = fullData['mo_sin_old_il_acct'].fillna(fullData['mo_sin_old_il_acct'].median())
    fullData['mo_sin_old_rev_tl_op'] = fullData['mo_sin_old_rev_tl_op'].fillna(fullData['mo_sin_old_rev_tl_op'].median())
    fullData['mo_sin_rcnt_rev_tl_op'] = fullData['mo_sin_rcnt_rev_tl_op'].fillna(fullData['mo_sin_rcnt_rev_tl_op'].median())
    fullData['mo_sin_rcnt_tl'] = fullData['mo_sin_rcnt_tl'].fillna(fullData['mo_sin_rcnt_tl'].median())
    fullData['mort_acc'] = fullData['mort_acc'].fillna(fullData['mort_acc'].median())
    fullData['mths_since_recent_bc'] = fullData['mths_since_recent_bc'].fillna(fullData['mths_since_recent_bc'].median())
    return fullData

def replace_by_zero(fullData):
    fullData['delinq_2yrs'] = np.where(fullData['delinq_2yrs'].isnull(), 0, fullData['delinq_2yrs'])
    fullData['inq_last_6mths'] = np.where(fullData['inq_last_6mths'].isnull(), 0, fullData['inq_last_6mths'])
    fullData['mths_since_last_delinq'] = np.where(fullData['mths_since_last_delinq'].isnull(), 0, fullData['mths_since_last_delinq'])
    fullData['mths_since_last_record'] = np.where(fullData['mths_since_last_record'].isnull(), 0, fullData['mths_since_last_record'])
    fullData['total_acc'] = np.where(fullData['total_acc'].isnull(), 0, fullData['total_acc'])
    fullData['acc_now_delinq'] = np.where(fullData['acc_now_delinq'].isnull(), 0, fullData['acc_now_delinq'])
    fullData['chargeoff_within_12_mths'] = np.where(fullData['chargeoff_within_12_mths'].isnull(), 0, fullData['chargeoff_within_12_mths'])
    fullData['delinq_amnt'] = np.where(fullData['delinq_amnt'].isnull(), 0, fullData['delinq_amnt'])
    fullData['mths_since_recent_bc_dlq'] = np.where(fullData['mths_since_recent_bc_dlq'].isnull(), 0, fullData['mths_since_recent_bc_dlq'])
    fullData['mths_since_recent_inq'] = np.where(fullData['mths_since_recent_inq'].isnull(), 0, fullData['mths_since_recent_inq'])
    fullData['mths_since_recent_revol_delinq'] = np.where(fullData['mths_since_recent_revol_delinq'].isnull(), 0, fullData['mths_since_recent_revol_delinq'])
    fullData['num_accts_ever_120_pd'] = np.where(fullData['num_accts_ever_120_pd'].isnull(), 0, fullData['num_accts_ever_120_pd'])
    fullData['num_actv_bc_tl'] = np.where(fullData['num_actv_bc_tl'].isnull(), 0, fullData['num_actv_bc_tl'])
    fullData['num_actv_rev_tl'] = np.where(fullData['num_actv_rev_tl'].isnull(), 0, fullData['num_actv_rev_tl'])
    fullData['num_bc_sats'] = np.where(fullData['num_bc_sats'].isnull(), 0, fullData['num_bc_sats'])
    fullData['num_bc_tl'] = np.where(fullData['num_bc_tl'].isnull(), 0, fullData['num_bc_tl'])
    fullData['num_il_tl'] = np.where(fullData['num_il_tl'].isnull(), 0, fullData['num_il_tl'])
    fullData['num_op_rev_tl'] = np.where(fullData['num_op_rev_tl'].isnull(), 0, fullData['num_op_rev_tl'])
    fullData['num_rev_accts'] = np.where(fullData['num_rev_accts'].isnull(), 0, fullData['num_rev_accts'])
    fullData['num_rev_tl_bal_gt_0'] = np.where(fullData['num_rev_tl_bal_gt_0'].isnull(), 0, fullData['num_rev_tl_bal_gt_0'])
    fullData['num_sats'] = np.where(fullData['num_sats'].isnull(), 0, fullData['num_sats'])
    fullData['num_tl_120dpd_2m'] = np.where(fullData['num_tl_120dpd_2m'].isnull(), 0, fullData['num_tl_120dpd_2m'])
    fullData['num_tl_30dpd'] = np.where(fullData['num_tl_30dpd'].isnull(), 0, fullData['num_tl_30dpd'])
    fullData['num_tl_90g_dpd_24m'] = np.where(fullData['num_tl_90g_dpd_24m'].isnull(), 0, fullData['num_tl_90g_dpd_24m'])
    fullData['num_tl_op_past_12m'] = np.where(fullData['num_tl_op_past_12m'].isnull(), 0, fullData['num_tl_op_past_12m'])
    fullData['pct_tl_nvr_dlq'] = np.where(fullData['pct_tl_nvr_dlq'].isnull(), 0, fullData['pct_tl_nvr_dlq'])
    fullData['percent_bc_gt_75'] = np.where(fullData['percent_bc_gt_75'].isnull(), 0, fullData['percent_bc_gt_75'])
    fullData['pub_rec_bankruptcies'] = np.where(fullData['pub_rec_bankruptcies'].isnull(), 0, fullData['pub_rec_bankruptcies'])
    fullData['tax_liens'] = np.where(fullData['tax_liens'].isnull(), 0, fullData['tax_liens'])
    return fullData

def remove_rows(fullData):
    return fullData

def remove_columns(fullData):
    fullData = fullData.drop(['open_acc_6m','open_il_6m','open_il_12m','open_il_24m','mths_since_rcnt_il',\
                                'total_bal_il','il_util','open_rv_12m','open_rv_24m','max_bal_bc','all_util',\
                                'inq_fi','total_cu_tl','inq_last_12m','bc_util'], axis=1)

    return fullData

def add_derived_columns(fullData):
    # 90day worse rating
    fullData['90day_worse_rating'] = np.where(fullData['mths_since_last_major_derog'].isnull(), 0, 1)
    fullData = fullData.drop(['mths_since_last_major_derog'], axis=1)
    # Annual Income and DTI individual and joint consolidation
    fullData['annual_inc_joint'].fillna(fullData['annual_inc'], inplace=True)
    fullData['dti_joint'].fillna(fullData['dti'], inplace=True)
    fullData = fullData.drop(['annual_inc', 'dti'], axis=1)			
    # Change column names of annual income joint and dti joint
    fullData.rename(columns = {'annual_inc_joint':'annual_inc'}, inplace = True)
    fullData.rename(columns = {'dti_joint':'dti'}, inplace = True)
    # open to buy = credit limit - (sum of holds and outstanding balance) ...assuming that sum of holds and outstanding balance is zero in this case
    fullData['bc_open_to_buy'].fillna(fullData['tot_hi_cred_lim'], inplace=True)
    # average current balance of all accounts replaced with total current balanace (assuming that this is the case with only one account)
    fullData['avg_cur_bal'].fillna(fullData['tot_cur_bal'], inplace=True)
    #average of FICO score (risk_score)
    fullData['risk_score'] = fullData[['fico_range_low', 'fico_range_high']].mean(axis=1)
    #average FICO Score - Last and First FICO score 
    fullData['avg_fico_range'] = fullData[['last_fico_range_low', 'last_fico_range_high']].mean(axis=1)
    #Fill NAs in Fico Score
    fullData['risk_score'] = np.where(fullData['risk_score'].isnull(), '999', fullData['risk_score'])
    fullData['avg_fico_range'] = np.where(fullData['avg_fico_range'].isnull(), '999', fullData['avg_fico_range'])
    #Binning the average credit scores to diffferent levels
    #fullData['risk_score_level']
    # def level(c):
    #     if c['risk_score'] >= 730:
    #         return 'A+'
    #     elif( c['risk_score'] >= 680) and ( c['risk_score'] <=729) :
    #         return 'A'
    #     elif( c['risk_score'] >= 640) and ( c['risk_score'] <=679) :
    #         return 'B'
    #     elif( c['risk_score'] >= 600) and ( c['risk_score'] <=639) :
    #         return 'C'
    #     elif( c['risk_score'] >= 550) and ( c['risk_score'] <=599) :
    #         return 'D'
    #     if c['risk_score'] <= 550:
    #         return 'E'
    #     else:
    #         return 'U'
    # fullData['Risk_Score_Level'] = fullData.apply(level, axis=1)
    return fullData

def add_dummy_variable(fullData):
    return fullData


def clean_text_columns(fullData):
    #-----------Term - Remove months 
    fullData['term'] = np.where(fullData['term'].isnull(), 'Unknown', fullData['term'])
    fullData['term'].replace(regex=True,inplace=True,to_replace=r'\D',value=r'')

    #------------Interest Rate - Remove %
    fullData['int_rate'] = fullData['int_rate'].str.replace('%', '')
    fullData['int_rate'] = fullData['int_rate'].astype(float)
    fullData['int_rate'] = np.where(fullData['int_rate'].isnull(), 'Unknown', fullData['int_rate'])

    #------------Grade - Fill NAs
    fullData['grade'] = np.where(fullData['grade'].isnull(), 'Unknown', fullData['grade'])

    fullData['sub_grade'] = np.where(fullData['sub_grade'].isnull(), 'Unknown', fullData['sub_grade'])


    #------------Employment Length
    fullData['emp_length'] = np.where(fullData['emp_length'].isnull(), 'Unknown', fullData['emp_length'])
    #Employee Length - Integter conversion 
    fullData['emp_length'] = fullData.emp_length.str.replace('+','')
    fullData['emp_length'] = fullData.emp_length.str.replace('<','')
    fullData['emp_length'] = fullData.emp_length.str.replace('years','')
    fullData['emp_length'] = fullData.emp_length.str.replace('year','')
    fullData['emp_length'] = fullData.emp_length.str.replace('n/a','0')
    fullData['emp_length'] = fullData['emp_length'].astype(int)

    #------------home_ownership
    fullData['home_ownership'] = np.where(fullData['home_ownership'].isnull(), 'Unknown', fullData['home_ownership'])
    fullData["home_ownership"] = fullData["home_ownership"].apply(lambda home_ownership: "UNKNOWN" if home_ownership == "OTHER" else home_ownership)
    fullData["home_ownership"] = fullData["home_ownership"].apply(lambda home_ownership: "UNKNOWN" if home_ownership == "NONE" else home_ownership)
    fullData["home_ownership"] = fullData["home_ownership"].apply(lambda home_ownership: "UNKNOWN" if home_ownership == "ANY" else home_ownership)

    #------------Verification status
    fullData['verification_status'] = np.where(fullData['verification_status'].isnull(), 'Unknown', fullData['verification_status'])

    #------------Loan Status
    fullData['loan_status'] = np.where(fullData['loan_status'].isnull(), 'Unknown', fullData['loan_status'])

    #------------Payment Plan
    fullData['pymnt_plan'] = np.where(fullData['pymnt_plan'].isnull(), 'Unknown', fullData['pymnt_plan'])

    #------------purpose
    fullData['purpose'] = np.where(fullData['purpose'].isnull(), 'Unknown', fullData['purpose'])

    #------------zipcode
    fullData['zip_code'] = np.where(fullData['zip_code'].isnull(), 'Unknown', fullData['zip_code'])
    fullData['zip_code'] = fullData['zip_code'].str.replace('x', '0')
    fullData['zip_code'] = fullData['zip_code'].astype(int)

    #------------addr_state
    fullData['addr_state'] = np.where(fullData['addr_state'].isnull(), 'Unknown', fullData['addr_state'])

    #-------------application_type
    fullData['application_type'] = np.where(fullData['application_type'].isnull(), 'Unknown', fullData['application_type'])
    fullData['Issue_month'], fullData['Issue_Year'] = fullData['issue_d'].str.split('-', 1).str
    return fullData

    

class CleanData(luigi.Task):
    def requires(self):
        return [GetData()]
 
    def output(self):
        return { 'output1' : luigi.LocalTarget("Data/Cleaned/cleaned_loandata.csv")}
                
    def run(self):
        create_directory("Data/Cleaned")
        create_directory("Data/Summary")
        downloads_dir_loan = "Data/Downloads/LoanData"
        cleaned_dir = "Data/Cleaned/"
        summary_dir = "Data/Summary/"


        # 
        if (os.path.isfile(downloads_dir_loan + "/full_downloaded_loandata.xls" )):
            fullData = pd.read_csv(downloads_dir_loan + "/full_downloaded_loandata.xls", sep = ",", encoding = "ISO-8859-1", low_memory= False)

        else:

            #Looping over all the csv to load and process the data
            ls_dir = os.listdir(downloads_dir_loan)
            fullData = None
            for file in ls_dir:
                #     only if file is csv
                regexp = re.compile(r'.csv')
                if(regexp.search(file)):

                    filePath = downloads_dir_loan + "/" + file
                    # # # READ DATA INTO DATAFRAME
                    data = pd.read_csv(filePath, sep = ",", skiprows=[0], encoding = "ISO-8859-1", low_memory= False)

                    # Removing Rows where all columns are null or only have value for 1 column
                    data.dropna(how='all', inplace = True)
                    data.dropna(thresh=2, inplace = True)

                    try:
                        fullData = pd.concat([fullData, data])
                    except:
                        fullData = data

                    data = None


            # Save fullData (full Raw dataset) in Downloads directory -- just to view
            fullData.to_csv(downloads_dir_loan + "/full_downloaded_loandata.xls"  , sep=',', index = True)

        # Describe downloaded dataset
        summary = fullData.describe()
                # grouped_data = data.groupby(['iris_class'])
                # print(grouped_data.describe().unstack())

        # SAVE summary of downloaded data to files
                # print(summary)
        summary.to_csv(summary_dir + "summary_downloaded_loandata.csv"  , sep=',', index = True)
                
        # CLEAN DATA

        # fullData = replace_by_mean(fullData)
        fullData = remove_columns(fullData)

        fullData = replace_by_median(fullData)

        fullData = replace_by_zero(fullData)

        # fullData = remove_rows(fullData)

        fullData = add_derived_columns(fullData)

        fullData = add_dummy_variable(fullData)


        fullData = clean_text_columns(fullData)


        # SAVE Cleaned/Preprocessed Data
        fullData.to_csv(cleaned_dir + "cleaned_loandata.csv", sep=',', index = False)


        # Summarize and save cleaned dataset
        summary = fullData.describe()
        summary.to_csv(summary_dir + "summary_cleaned_loandata.csv"  , sep=',', index = True)
