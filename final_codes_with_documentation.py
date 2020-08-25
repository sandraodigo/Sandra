"""
The first step needs to be setting the path to the directory containing all necessary csv files.

Clean_master() function reads master file in directory and removes rows with null values and salaries less than $500. Then, inner joins tickers
with symbols from the Russell 3000 file to remove any job postings that are from companies not listed on the Russell 3000. A cleaned master
dataframe is returned as well as a list of all the unique companies and sectors identified in the master file. It takes no inputs but will
need to be changed in the read_csv line to the actual file name.

Clean_timelog() function takes no inputs and the read_csv line will need to be changed to the file name. Function will create a cleaned dataframe
from the timelog file where all jobs that do not have a remove date (considered open at the time the data was pulled) will be set equal
to the maximum post_date.

Clean_role() function reads in the csv for roles and then the second file is a list of roles that will be mapped to manager, sales, key it, 
it, and hourly roles. The list of roles can be manipulated in the excel file to change which roles are mapped to which bucket.
The second part of the function determines if each role for each job id maps to one of those buckets and includes essentially a boolean
column that denotes if that type of role maps to that specific bucket. The role column is dropped to enable grouping based on job id.
Then the job ids are grouped and the max value is taken for the bucket columns. Therefore, if a job id has three different roles and 
maps to three different buckets then only one job id will exist but will have a value of 1 (True) in each bucket column.

Create_date_list() function serves to create a list of date ranges between the minimum post_date and maximum post_date that will then be used
as dates for the metric calculations. The date values are derived from the timelog file but can be set manually using a 
datetime.date variable that is then plugged in as start_date and end_date respectively. The function will default to the dates
calculated from the timelog file unless manually changed.

Merge_dfs() function merges the master and role dataframes first and then merges the resulting dataframe with the timelog dataframe. All previous
dataframes are still available globally in case further changes need to be made before merging. 

Co_metrics_fx() starts by creating intermediate lists for each metrics calculation so that every calculation for each company will be stored in a 
specific list that will then be added to an ordered dictionary. The final cleaned dataframe termed df_final in these codes will be filtered for every
unique company identified in the unique_companies list.df_company will be filtered again to only show the job ids that are considered open for the date
of analysis as identified by the second for loop. The metrics are then calculated on this df_count dataframe based on filtering, groupby, and count. 
These values are appended to the intermediate lists as well as the date of analysis, company name, and the company sector. The lists are then added to 
an ordered dictionary to maintain the specific order they were entered in order to ensure that all of the data lines up appropriately. A dataframe is 
then constructed from the ordered dictionary containing all the metric values. 

For co_average_metrics_fx() intermediate lists are created similar to the process for the non-averaged company metrics. A range is created based on index value using the
min and max dates to identify when df_co_metrics moves to data from another company. Average calculation occur between the identified start and end
index values. The rolling window is set at 28 and will create the average based on a mean calculation. The intermediate lists are added to df_co_metrics.

Sector_metrics_fx() creates a copy of the df_co_metrics and then drops the company_ref column in order to enable grouping based on sector and date.
Each of the metric columns are grouped by summing all the company metrics that fall into that sector. 
"""
import pathlib
import pandas as pd
import os
import numpy as np
import datetime

"""
The first step needs to be setting the path to the directory containing all necessary csv files.
"""
path = pathlib.Path(r'C:\Users\grigg\Documents\BSAD 499\Data_Set')
os.chdir(path)



def clean_master():
	"""
	Clean_master() function reads master file in directory and removes rows with null values and salaries less than $500. Then, inner joins tickers
	with symbols from the Russell 3000 file to remove any job postings that are from companies not listed on the Russell 3000. A cleaned master
	dataframe is returned as well as a list of all the unique companies and sectors identified in the master file. It takes no inputs but will
	need to be changed in the read_csv line to the actual file name.
	"""
    columns_master = ['job_id', 'company','salary', 'company_ref','ticker']
    master_dict = {'job_id': str, 'company': str, 'salary': str, 'company_ref': str,'ticker': str}

    print ('loading data...')
    global df_master, unique_companies, unique_sectors
    master_iterator = pd.read_csv('greenwich_master_loyola_2019-11-04.csv', encoding='latin-1', usecols = columns_master, dtype= master_dict, chunksize=1000000)
    master_chunk_list = []
    for data_chunk in master_iterator:
        master_chunk_list.append(data_chunk)
    df_master = pd.concat(master_chunk_list)
    print('loaded master file')
    print('cleaning salary...')
    df_cleaning = df_master
    df_cleaning = df_cleaning.replace(to_replace = r'\N', value = None)
    df_cleaning = df_cleaning.replace(to_replace = r'\\N', value = None)
    df_cleaning = df_cleaning.replace(to_replace = 'Nan', value = None)
    df_cleaning_salary = df_cleaning
    df_cleaning_salary['salary'] = df_cleaning['salary'].astype(int)
    df_filtered_salary = df_cleaning_salary[df_cleaning_salary['salary'] <=500]
    zero_salary_index = df_filtered_salary.index.to_list()
    df_cleaned_salary = df_cleaning_salary.drop(zero_salary_index, axis = 0)
    print('merging with Russell 3000...')
    df_master = df_cleaned_salary
    russell_columns = ['Symbol', 'Name', 'Sector', 'Industry']
    df_russell = pd.read_csv('Russell3000_industries.csv', usecols = russell_columns)
    df_master = df_master.merge(df_russell, how = 'inner', left_on = 'ticker', right_on = 'Symbol')
    print('master merged with Russell 3000')
    df_master['company_ref'] = df_master['Name']
    df_master['company'] = df_master['company_ref']
    unique_companies = df_master.company_ref.unique()
    unique_sectors = df_master.Sector.unique()

def clean_timelog():
	"""
	Clean_timelog() function takes no inputs and the read_csv line will need to be changed to the file name. Function will create a cleaned dataframe
	from the timelog file where all jobs that do not have a remove date (considered open at the time the data was pulled) will be set equal
	to the maximum post_date.
	"""
    columns_timelog = ['job_id', 'post_date', 'remove_date']
    timelog_dict = {'post_date': np.datetime64, 'remove_date': np.datetime64}

    print ('loading data...')
    global df_timelog
    timelog_iterator = pd.read_csv('greenwich_timelog_loyola_2019-11-04.csv', encoding='latin-1', usecols = columns_timelog,  parse_dates=[1,2], chunksize=100000)
    timelog_chunk_list = []
    for data_chunk in timelog_iterator:
        timelog_chunk_list.append(data_chunk)
    df_timelog = pd.concat(timelog_chunk_list)
    print('loaded timelogs file...')
    df_cleaning = df_timelog
    max_close_date = df_cleaning['post_date'].max()
    max_close_date = str(max_close_date)
    df_cleaning['remove_date'].replace("0000-00-00 00:00:00", value = max_close_date, inplace = True)
    df_cleaning['remove_date'] = df_cleaning['remove_date'].astype(np.datetime64)
    df_cleaning['post_date'] = df_cleaning['post_date'].dt.date
    df_cleaning['remove_date'] = df_cleaning['remove_date'].dt.date
    df_timelog = df_cleaning

def clean_role():
	"""
	Clean_role() function reads in the csv for roles and then the second file is a list of roles that will be mapped to manager, sales, key it, 
	it, and hourly roles. The list of roles can be manipulated in the excel file to change which roles are mapped to which bucket.
	The second part of the function determines if each role for each job id maps to one of those buckets and includes essentially a boolean
	column that denotes if that type of role maps to that specific bucket. The role column is dropped to enable grouping based on job id.
	Then the job ids are grouped and the max value is taken for the bucket columns. Therefore, if a job id has three different roles and 
	maps to three different buckets then only one job id will exist but will have a value of 1 (True) in each bucket column. 
	"""
    columns_role = ['job_id', 'role']
    role_dict = {'job_id': str, 'role': str}
    
    print ('loading data...')
    global df_role
    role_iterator = pd.read_csv('greenwich_role_loyola_2019-11-04.csv', encoding='latin-1', usecols = columns_role,  dtype = role_dict, chunksize=100000)
    role_chunk_list = []
    for data_chunk in role_iterator:
        role_chunk_list.append(data_chunk)
    df_role = pd.concat(role_chunk_list)
    print('loaded role file...')
    manager_list = []
    sales_list = []
    key_roles_list = []
    it_list = []
    hourly_list = []
    leftover_list = []

    list_of_role_lists = [manager_list, sales_list, key_roles_list, it_list, hourly_list, leftover_list]

    import xlrd 
    workbook = xlrd.open_workbook(filename = 'unique_roles.xlsx')
    for x in range(len(list_of_role_lists)):
        sheet = workbook.sheet_by_index(x)
        list_name = list_of_role_lists[x]
        for y in range(sheet.nrows):
            list_name.append(str(sheet.cell_value(y,0)))
    print('unique role list created...')
    
    
    df_role['role'] = df_role['role'].astype(str)
    df_role['role'] = df_role['role'].str.lower()
    
    def role_bool():
        global df_role
        df_cleaning_role = df_role
        for x in range(len(df_cleaning_role['role'])):
            if df_cleaning_role.at[x, 'role'] in manager_list:
                df_cleaning_role.at[x, 'manager_role'] = 1
            else:
                df_cleaning_role.at[x, 'manager_role'] = 0

            if df_cleaning_role.at[x, 'role'] in sales_list:
                df_cleaning_role.at[x, 'sales_role'] = 1
            else:
                df_cleaning_role.at[x, 'sales_role'] = 0

            if df_cleaning_role.at[x, 'role'] in key_roles_list:
                df_cleaning_role.at[x, 'key_roles_role'] = 1
            else:
                df_cleaning_role.at[x, 'key_roles_role'] = 0

            if df_cleaning_role.at[x, 'role'] in it_list:
                df_cleaning_role.at[x, 'it_role'] = 1
            else:
                df_cleaning_role.at[x, 'it_role'] = 0

            if df_cleaning_role.at[x, 'role'] in hourly_list:
                df_cleaning_role.at[x, 'hourly_role'] = 1
            else:
                df_cleaning_role.at[x, 'hourly_role'] = 0
        df_role = df_cleaning_role
        print('bucket logic added...')
    role_bool()
    
    df_test_role = df_role.drop(labels=['role'], axis=1)
    df_grouped_role = df_test_role.groupby(['job_id'], as_index= False).max()
    df_role = df_grouped_role
    print('job ids grouped by max...')
    return df_role


def create_date_list(start_date = start_date, end_date = end_date):
	"""
	Create_date_list() function serves to create a list of date ranges between the minimum post_date and maximum post_date that will then be used
	as dates for the metric calculations. The date values are derived from the timelog file but can be set manually using a 
	datetime.date variable that is then plugged in as start_date and end_date respectively. The function will default to the dates
	calculated from the timelog file unless manually changed.
	"""
    global date_list
    start = start_date
    end = end_date
    date_list = [start + datetime.timedelta(days=x) for x in range(0, (end-start).days)]

def merge_dfs():
    global df_final
	"""
	Merge_dfs() function merges the master and role dataframes first and then merges the resulting dataframe with the timelog dataframe. All previous
	dataframes are still available globally in case further changes need to be made before merging. 
	"""
	global df_analyze
	df_analyze = df_master
	df_analyze = df_analyze.merge(df_role, how = 'inner', on = 'job_id')
	df_analyze = df_analyze.merge(df_timelog, how = 'inner', on = 'job_id')
    df_final = df_analyze
	print('dataframes merged...')


clean_master()
clean_timelog()
clean_role()
start_date = df_timelog['post_date'].min()
end_date = df_timelog['post_date'].max()
create_date_list()
merge_dfs()


def co_metrics_fx():
    """
    Co_metrics_fx() starts by creating intermediate lists for each metrics calculation so that every calculation for each company will be stored in a 
    specific list that will then be added to an ordered dictionary. The final cleaned dataframe termed df_final in these codes will be filtered for every
    unique company identified in the unique_companies list.df_company will be filtered again to only show the job ids that are considered open for the date
    of analysis as identified by the second for loop. The metrics are then calculated on this df_count dataframe based on filtering, groupby, and count. 
    These values are appended to the intermediate lists as well as the date of analysis, company name, and the company sector. The lists are then added to 
    an ordered dictionary to maintain the specific order they were entered in order to ensure that all of the data lines up appropriately. A dataframe is 
    then constructed from the ordered dictionary containing all the metric values. 
    """
    global co_metric_dict, df_co_metrics
    co_metric_dict = OrderedDict()
    intermediate_date_list = []
    intermediate_company_list = []
    intermediate_industry_list = []
    intermediate_inv_count_list = []
    intermediate_inv_50k_list = []
    intermediate_inv_100k_list = []
    intermediate_inv_manager_list = []
    intermediate_inv_sales_list = []
    intermediate_inv_key_roles_list = []
    intermediate_inv_it_list = []
    intermediate_inv_hourly_list = []
    intermediate_cost_count_list = []
    intermediate_cost_50k_list = []
    intermediate_cost_100k_list = []
    intermediate_cost_manager_list = []
    intermediate_cost_sales_list = []
    intermediate_cost_key_roles_list = []
    intermediate_cost_it_list = []
    intermediate_cost_hourly_list = []
    for x in range(len(unique_companies)):
        chosen_company = unique_companies[x]
        df_company = df_final[df_final['company_ref'] == chosen_company]
        sector_index_list = df_company.index.tolist()
        company_sector = df_final.at[sector_index_list[0], 'Sector']
        print(x)
        for y in range(len(date_list)):
            chosen_date = date_list[y]
        
            df_count = df_company[(df_company['post_date'] <= chosen_date) & (df_company['remove_date'] >= chosen_date)]
            inv_count_value = df_count['job_id'].count()
            co_inv_overall = inv_count_value
            
            df_50k = df_count[df_count['salary'] > 50000]
            salary_50k_count_value = df_50k['job_id'].count()
            co_inv_50k = salary_50k_count_value
            
            df_100k = df_count[df_count['salary'] > 100000]
            salary_100k_count_value = df_100k['job_id'].count()
            co_inv_100k = salary_100k_count_value
            
            df_manager = df_count[df_count['manager_role'] == 1]
            manager_count_value = df_manager['job_id'].count()
            co_inv_manager = manager_count_value
            
            df_sales = df_count[df_count['sales_role'] == 1]
            sales_count_value = df_sales['job_id'].count()
            co_inv_sales = sales_count_value
            
            df_key_roles = df_count[df_count['key_roles_role'] == 1]
            key_roles_count_value = df_key_roles['job_id'].count()
            co_inv_key_roles = key_roles_count_value
            
            df_it = df_count[df_count['it_role'] == 1]
            it_count_value = df_it['job_id'].count()
            co_inv_it = it_count_value
            
            df_hourly = df_count[df_count['hourly_role'] == 1]
            hourly_count_value = df_hourly['job_id'].count()
            co_inv_hourly = hourly_count_value

            df_count = df_company[(df_company['post_date'] <= chosen_date) & (df_company['remove_date'] >= chosen_date)]
            cost_count_value = df_count['salary'].sum()
            co_cost_overall = cost_count_value
            
            df_50k = df_count[df_count['salary'] > 50000]
            salary_50k_cost_value = df_50k['salary'].sum()
            co_cost_50k = salary_50k_cost_value
            
            df_100k = df_count[df_count['salary'] > 100000]
            salary_100k_cost_value = df_100k['salary'].sum()
            co_cost_100k = salary_100k_cost_value
            
            df_manager = df_count[df_count['manager_role'] == 1]
            manager_cost_value = df_manager['salary'].sum()
            co_cost_manager = manager_cost_value
            
            df_sales = df_count[df_count['sales_role'] == 1]
            sales_cost_value = df_sales['salary'].sum()
            co_cost_sales = sales_cost_value
            
            df_key_roles = df_count[df_count['key_roles_role'] == 1]
            key_roles_cost_value = df_key_roles['salary'].sum()
            co_cost_key_roles = key_roles_cost_value
            
            df_it = df_count[df_count['it_role'] == 1]
            it_cost_value = df_it['salary'].sum()
            co_cost_it = it_cost_value
            
            df_hourly = df_count[df_count['hourly_role'] == 1]
            hourly_cost_value = df_hourly['salary'].sum()
            co_cost_hourly = hourly_cost_value
            
            
            
            intermediate_date_list.append(chosen_date)
            intermediate_company_list.append(chosen_company)
            intermediate_industry_list.append(company_sector)
            intermediate_inv_count_list.append(co_inv_overall)
            intermediate_inv_50k_list.append(co_inv_50k)
            intermediate_inv_100k_list.append(co_inv_100k)
            intermediate_inv_manager_list.append(co_inv_manager)
            intermediate_inv_sales_list.append(co_inv_sales)
            intermediate_inv_key_roles_list.append(co_inv_key_roles)
            intermediate_inv_it_list.append(co_inv_it)
            intermediate_inv_hourly_list.append(co_inv_hourly)

            intermediate_cost_count_list.append(co_cost_overall)
            intermediate_cost_50k_list.append(co_cost_50k)
            intermediate_cost_100k_list.append(co_cost_100k)
            intermediate_cost_manager_list.append(co_cost_manager)
            intermediate_cost_sales_list.append(co_cost_sales)
            intermediate_cost_key_roles_list.append(co_cost_key_roles)
            intermediate_cost_it_list.append(co_cost_it)
            intermediate_cost_hourly_list.append(co_cost_hourly)
                
    co_metric_dict['date_list'] = intermediate_date_list
    co_metric_dict['company_ref'] = intermediate_company_list
    co_metric_dict['sector'] = intermediate_industry_list
    co_metric_dict['co_inv_overall'] = intermediate_inv_count_list
    co_metric_dict['co_inv_50k'] = intermediate_inv_50k_list
    co_metric_dict['co_inv_100k'] = intermediate_inv_100k_list
    co_metric_dict['co_inv_manager'] = intermediate_inv_manager_list
    co_metric_dict['co_inv_sales'] = intermediate_inv_sales_list
    co_metric_dict['co_inv_key_roles'] = intermediate_inv_key_roles_list
    co_metric_dict['co_inv_it'] = intermediate_inv_it_list
    co_metric_dict['co_inv_hourly'] = intermediate_inv_hourly_list
    
    co_metric_dict['co_fut_cost_overall'] = intermediate_cost_count_list
    co_metric_dict['co_fut_cost_50k'] = intermediate_cost_50k_list
    co_metric_dict['co_fut_cost_100k'] = intermediate_cost_100k_list
    co_metric_dict['co_fut_cost_manager'] = intermediate_cost_manager_list
    co_metric_dict['co_fut_cost_sales'] = intermediate_cost_sales_list
    co_metric_dict['co_fut_cost_key_roles'] = intermediate_cost_key_roles_list
    co_metric_dict['co_fut_cost_it'] = intermediate_cost_it_list
    co_metric_dict['co_fut_cost_hourly'] = intermediate_cost_hourly_list

    df_co_metrics = pd.DataFrame(co_metric_dict, columns=co_metric_dict.keys())



def co_average_metrics_fx():
    """
    Intermediate lists are created similar to the process for the non-averaged company metrics. A range is created based on index value using the
    min and max dates to identify when df_co_metrics moves to data from another company. Average calculation occur between the identified start and end
    index values. The rolling window is set at 28 and will create the average based on a mean calculation. The intermediate lists are added to df_co_metrics.
    """
    co_inv_overall_28d_list = []
    co_inv_50k_28d_list = []
    co_inv_100k_28d_list = []
    co_inv_manager_28d_list = []
    co_inv_sales_28d_list = []
    co_inv_key_roles_28d_list = []
    co_inv_it_28d_list = []
    co_inv_hourly_28d_list = []

    co_fut_cost_overall_28d_list = []
    co_fut_cost_50k_28d_list = []
    co_fut_cost_100k_28d_list = []
    co_fut_cost_manager_28d_list = []
    co_fut_cost_sales_28d_list = []
    co_fut_cost_key_roles_28d_list = []
    co_fut_cost_it_28d_list = []
    co_fut_cost_hourly_28d_list = []

    for x in range(len(start_list)):
        a = start_list[x]
        b = end_list[x]

        co_inv_overall_28d_values = df_co_metrics.iloc[a:b, df_co_metrics.columns.get_loc('co_inv_overall')].rolling(window=28).mean()
        co_inv_50k_28d_values = df_co_metrics.iloc[a:b, df_co_metrics.columns.get_loc('co_inv_50k')].rolling(window=28).mean()
        co_inv_100k_28d_values = df_co_metrics.iloc[a:b, df_co_metrics.columns.get_loc('co_inv_100k')].rolling(window=28).mean()
        co_inv_manager_28d_values = df_co_metrics.iloc[a:b,df_co_metrics.columns.get_loc('co_inv_manager')].rolling(window=28).mean()
        co_inv_sales_28d_values = df_co_metrics.iloc[a:b, df_co_metrics.columns.get_loc('co_inv_sales')].rolling(window=28).mean()
        co_inv_key_roles_28d_values = df_co_metrics.iloc[a:b, df_co_metrics.columns.get_loc('co_inv_key_roles')].rolling(window=28).mean()
        co_inv_it_28d_values = df_co_metrics.iloc[a:b, df_co_metrics.columns.get_loc('co_inv_it')].rolling(window=28).mean()
        co_inv_hourly_28d_values = df_co_metrics.iloc[a:b, df_co_metrics.columns.get_loc('co_inv_hourly')].rolling(window=28).mean()

        co_fut_cost_overall_28d_values = df_co_metrics.iloc[a:b, df_co_metrics.columns.get_loc('co_fut_cost_overall')].rolling(window=28).mean()
        co_fut_cost_50k_28d_values = df_co_metrics.iloc[a:b, df_co_metrics.columns.get_loc('co_fut_cost_50k')].rolling(window=28).mean()
        co_fut_cost_100k_28d_values = df_co_metrics.iloc[a:b, df_co_metrics.columns.get_loc('co_fut_cost_100k')].rolling(window=28).mean()
        co_fut_cost_manager_28d_values = df_co_metrics.iloc[a:b, df_co_metrics.columns.get_loc('co_fut_cost_manager')].rolling(window=28).mean()
        co_fut_cost_sales_28d_values = df_co_metrics.iloc[a:b, df_co_metrics.columns.get_loc('co_fut_cost_sales')].rolling(window=28).mean()
        co_fut_cost_key_roles_28d_values = df_co_metrics.iloc[a:b, df_co_metrics.columns.get_loc('co_fut_cost_key_roles')].rolling(window=28).mean()
        co_fut_cost_it_28d_values = df_co_metrics.iloc[a:b, df_co_metrics.columns.get_loc('co_fut_cost_it')].rolling(window=28).mean()
        co_fut_cost_hourly_28d_values = df_co_metrics.iloc[a:b, df_co_metrics.columns.get_loc('co_fut_cost_hourly')].rolling(window=28).mean()

        co_inv_overall_28d_list.append(co_inv_overall_28d_values)
        co_inv_50k_28d_list.append(co_inv_50k_28d_values)
        co_inv_100k_28d_list.append(co_inv_100k_28d_values)
        co_inv_manager_28d_list.append(co_inv_manager_28d_values)
        co_inv_sales_28d_list.append(co_inv_sales_28d_values)
        co_inv_key_roles_28d_list.append(co_inv_key_roles_28d_values)
        co_inv_it_28d_list.append(co_inv_it_28d_values)
        co_inv_hourly_28d_list.append(co_inv_hourly_28d_values)

        co_fut_cost_overall_28d_list.append(co_fut_cost_overall_28d_values)
        co_fut_cost_50k_28d_list.append(co_fut_cost_50k_28d_values)
        co_fut_cost_100k_28d_list.append(co_fut_cost_100k_28d_values)
        co_fut_cost_manager_28d_list.append(co_fut_cost_manager_28d_values)
        co_fut_cost_sales_28d_list.append(co_fut_cost_sales_28d_values)
        co_fut_cost_key_roles_28d_list.append(co_fut_cost_key_roles_28d_values)
        co_fut_cost_it_28d_list.append(co_fut_cost_it_28d_values)
        co_fut_cost_hourly_28d_list.append(co_fut_cost_hourly_28d_values)

    print('avaerages calculated...')    
    df_co_metrics['co_inv_overall_28d'] = pd.concat(co_inv_overall_28d_list)
    df_co_metrics['co_inv_50k_28d'] = pd.concat(co_inv_50k_28d_list)
    df_co_metrics['co_inv_100k_28d'] = pd.concat(co_inv_100k_28d_list)
    df_co_metrics['co_inv_manager_28d'] = pd.concat(co_inv_manager_28d_list)
    df_co_metrics['co_inv_sales_28d'] = pd.concat(co_inv_sales_28d_list)
    df_co_metrics['co_inv_key_roles_28d'] = pd.concat(co_inv_key_roles_28d_list)
    df_co_metrics['co_inv_it_28d'] = pd.concat(co_inv_it_28d_list)
    df_co_metrics['co_inv_hourly_28d'] = pd.concat(co_inv_hourly_28d_list)

    df_co_metrics['co_fut_cost_overall_28d'] = pd.concat(co_fut_cost_overall_28d_list)
    df_co_metrics['co_fut_cost_50k_28d'] = pd.concat(co_fut_cost_50k_28d_list)
    df_co_metrics['co_fut_cost_100k_28d'] = pd.concat(co_fut_cost_100k_28d_list)
    df_co_metrics['co_fut_cost_manager_28d'] = pd.concat(co_fut_cost_manager_28d_list)
    df_co_metrics['co_fut_cost_sales_28d'] = pd.concat(co_fut_cost_sales_28d_list)
    df_co_metrics['co_fut_cost_key_roles_28d'] = pd.concat(co_fut_cost_key_roles_28d_list)
    df_co_metrics['co_fut_cost_it_28d'] = pd.concat(co_fut_cost_it_28d_list)
    df_co_metrics['co_fut_cost_hourly_28d'] = pd.concat(co_fut_cost_hourly_28d_list)
    print(columns added...)

def sector_metrics_fx():
    """
    Sector_metrics_fx() creates a copy of the df_co_metrics and then drops the company_ref column in order to enable grouping based on sector and date.
    Each of the metric columns are grouped by summing all the company metrics that fall into that sector. 
    """
    global df_sector_metrics
    
    df_sector_metrics = df_co_metrics
    drop_company = ['company_ref']
    df_sector_metrics.drop(labels = drop_company, axis = 1, inplace = True)
    df_sector_metrics = df_sector_metrics.groupby(['date_list', 'sector']).agg({'co_inv_overall': 'sum', 'co_inv_50k': 'sum', 'co_inv_100k': 'sum', 'co_inv_manager': 'sum', 'co_inv_sales': 'sum', 'co_inv_key_roles': 'sum', 'co_inv_it': 'sum', 'co_inv_hourly': 'sum','co_fut_cost_overall': 'sum', 'co_fut_cost_50k': 'sum', 'co_fut_cost_100k': 'sum', 'co_fut_cost_manager': 'sum', 'co_fut_cost_sales': 'sum', 'co_fut_cost_key_roles': 'sum', 'co_fut_cost_it': 'sum', 'co_fut_cost_hourly': 'sum','co_inv_overall_28d': 'sum', 'co_inv_50k_28d': 'sum', 'co_inv_100k_28d': 'sum', 'co_inv_manager_28d': 'sum', 'co_inv_sales_28d': 'sum', 'co_inv_key_roles_28d': 'sum', 'co_inv_it_28d': 'sum', 'co_inv_hourly_28d': 'sum','co_fut_cost_overall_28d': 'sum', 'co_fut_cost_50k_28d': 'sum', 'co_fut_cost_100k_28d': 'sum', 'co_fut_cost_manager_28d': 'sum', 'co_fut_cost_sales_28d': 'sum', 'co_fut_cost_key_roles_28d': 'sum', 'co_fut_cost_it_28d': 'sum', 'co_fut_cost_hourly_28d': 'sum'})


co_metrics_fx()
start_list = df_co_metrics[df_co_metrics['date_list'] == start_date].index.to_list()
end_list = df_co_metrics[df_co_metrics['date_list'] == end_date].index.to_list()
co_average_metrics_fx()
sector_metrics_fx()