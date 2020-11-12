import pandas as pd
import numpy as np
import plotly.express as px

# Task 2
acc_df = pd.read_csv('Data_Cleaning_Activity_Data.csv')

# Task 3: Check data types of variables
# *Note: Object data type refers to strings
acc_df.info()



# Task 4: Fill down columns
def fill_down(df):

    cols = ['kids_count', 'credit_score', 'returned_checks',
                           'collection_agency_code', 'date_turnover', 'amount_owed']

    # Group by account ID and forward fill data
    df[cols] = df.groupby(['account_ID'], sort=False)[cols].ffill()

    return df


acc_df = fill_down(acc_df)

# Task 5: Conflicting Columns
# Check if collection_agency_code indeed conflicts w/ agency_code
def remove_col_ag_code(df):

    # Check if the two columns are similar
    check_df = np.where(df['collection_agency_code'] == df['agency_code'],
             df['agency_code'], 'no match')

    # If not similar remove the column
    if 'no match' in check_df:
        df = df.drop(['collection_agency_code'], axis=1)

    return df

acc_df = remove_col_ag_code(acc_df)

c = acc_df.loc[acc_df['account_ID'] == '902429_61468372']

# Task 6: Remove a variable that is not needed in the data set
# Since we already have the agency code we don't need the agency tab
def remove_agency(df):

    df = df.drop(['agency'], axis=1)

    return df

acc_df = remove_agency(acc_df)


# Task 7: Aggregate data
def aggregate_data(df):

    # Change dates to datetime objects
    df['account_closed'] = pd.to_datetime(df['account_closed'])
    df['date_turnover'] = pd.to_datetime(df['date_turnover'])

    # Apply aggregate functions to data
    agg_dict = {'amount_paid': ['sum'], 'kids_count': [pd.Series.mode],
                'agency_code': [pd.Series.mode], 'date_turnover': [np.min],
                'account_closed': [np.max]}
    df_agg = df.groupby(['account_ID'], sort=False, as_index=False).agg(agg_dict)

    # Rename aggregate data frame columns
    df_agg.columns = ['account_ID', 'amount_paid_total', 'kids_count_mode', 'agency_code_mode',
                      'date_turnover_min', 'account_closed_max']

    # Drop duplicate account IDs
    sub_df = df.drop_duplicates(subset=['account_ID'], keep='last')

    sub_df = sub_df.drop(['amount_paid', 'kids_count', 'agency_code'], axis=1)

    # Merge aggregate data w/ dropped account IDs
    x_merge = pd.merge(sub_df, df_agg, left_on='account_ID', right_on='account_ID', how='left')

    return x_merge

acc_df = aggregate_data(acc_df)

# Task 8:
def calc_day_difference(df):

    # Get the difference between account closed and date turnover
    df['time_to_pay'] = df['account_closed_max'] - df['date_turnover_min']

    # Mark NaNs as -1
    df['time_to_pay'] = df['time_to_pay'].fillna('-1')

    # Change time_to_pay column to string
    df['time_to_pay'] = df['time_to_pay'].astype(str)

    # Change days to int
    df['time_to_pay'] = df['time_to_pay'].str.replace('\sdays.*', '').astype(int)

    # Check that no negative day values were calculated
    print(df.loc[df['time_to_pay'] < -1])

    return df


def create_day_bins(df):

    # Create bins
    df['0-30_days'] = np.where((df['time_to_pay'] >= 0) & (df['time_to_pay'] <= 30), 1, 0)
    #df['31-60_days'] = np.where(df['time_to_pay'] >= 31 & df['time_to_pay'] <= 60, 1, 0)
    #df['61-90_days'] = np.where(df['time_to_pay'] >= 61 & df['time_to_pay'] <= 90 , 1, 0)
    #df['90+days'] = np.where(df['time_to_pay'] >= 90, 1, 0)

    return df

acc_df = calc_day_difference(acc_df)
acc_df = create_day_bins(acc_df)


# Task 9 Create histogram of number of accounts with # of kids
fig = px.histogram(acc_df, x='kids_count_mode')
fig.show()

acc_df.to_csv('cleaned_accounts_data.csv')