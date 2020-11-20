import pandas as pd
import numpy as np
import plotly.express as px

# Task 2
acc_df = pd.read_csv('Data_Cleaning_Activity_Data.csv')

# Task 3: Check data types of variables
# *Note: Object data type refers to strings
acc_df.info()


# Task 4: Fill down columns
def fill_down_dates(xdf):

    # Check if there is only one non-NA row
    if xdf['date_turnover'].count() == 0:
        print(xdf['date_turnover'])
        pass

    elif xdf['date_turnover'].count() == 1:
        xdf['date_turnover'] = xdf['date_turnover'].ffill()

    else:
        xdf['date_turnover'].fillna(method='ffill', inplace=True)

        # Minimum date


        # Keep turnover days less than account_closed, replace ones greater with the minimum account_closed date
        #xdf['date_turnover'] = np.where((xdf['account_closed'] - xdf['date_turnover']).dt.days > 0, np.NaN,
                                        #min_date)
        for row in xdf.itertuples():

            fval = xdf.at[row.Index, 'date_turnover']

            # Change rows that have account_closed dates after date_turnover
            if (xdf.at[row.Index,'account_closed'] - xdf.at[row.Index, 'date_turnover']).days < 0:

                fval = xdf['date_turnover'].min()

            xdf.at[row.Index, 'date_turnover'] = fval


    return xdf['date_turnover']


def fill_down(df):

    # Change dates to datetime objects
    df['account_closed'] = pd.to_datetime(df['account_closed'])
    df['date_turnover'] = pd.to_datetime(df['date_turnover'])

    cols = ['kids_count', 'credit_score', 'returned_checks',
                           'collection_agency_code', 'amount_owed']

    # Group by account ID and forward fill data
    df[cols] = df.groupby(['account_ID'], sort=False, as_index=False)[cols].ffill()

    # Fill down dates
    dfcol = df.groupby(['account_ID'], sort=False).apply(fill_down_dates)
    df['date_turnover'] = dfcol.reset_index(level=0, drop=True)

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
    # Make a dictionary of agency codes and agency
    agency_key = dict(zip(df.agency_code, df.agency))
    agency_key_out = {}

    for key in agency_key:

        # Remove all nan from dict and print out the codes w/ agency name
        if type(agency_key[key]) is str:

            # Add keys to new dictionary w/ lists as values
            if agency_key[key] in agency_key_out:
                agency_key_out[agency_key[key]].append(key)

            else:
                agency_key_out.update({agency_key[key]: [key]})

    print(agency_key_out)
    # Drop the agency column
    df = df.drop(['agency_code'], axis=1)

    return df


acc_df = remove_agency(acc_df)


# Task 7: Aggregate data
def aggregate_data(df):

    # Apply aggregate functions to data
    agg_dict = {'amount_paid': ['sum'], 'kids_count': [pd.Series.mode],
                'zip_code': 'last'}
    df_agg = df.groupby(['account_ID', 'date_turnover', 'account_closed'], sort=False, as_index=False).agg(agg_dict)

    # Rename aggregate data frame columns
    df_agg.columns = ['account_ID', 'date_turnover', 'account_closed', 'amount_paid_total', 'kids_count_mode',
                      'zip_code']

    # Merge aggregate data w/ dropped account IDs
    merge_cols = ['account_ID', 'date_turnover', 'account_closed']
    df = df.drop(['zip_code', 'amount_paid'], axis=1)

    # Left merge aggregated data with the original dataframe
    x_merge = pd.merge(df_agg, df, how='left', left_on=merge_cols, right_on=merge_cols)

    x_merge = x_merge.drop_duplicates(merge_cols)

    return x_merge

acc_df = aggregate_data(acc_df)

# Task 8:
def calc_day_difference(df):

    # Get the difference between account closed and date turnover
    df['time_to_pay'] = df['account_closed'] - df['date_turnover']

    # Mark NaNs as -1
    df['time_to_pay'] = df['time_to_pay'].fillna('-1')
    df['account_closed'] = df['account_closed'].fillna('-1')

    # Change time_to_pay column to string
    df['time_to_pay'] = df['time_to_pay'].astype(str)

    # Change days to int
    df['time_to_pay'] = df['time_to_pay'].str.replace('\sdays.*', '').astype(int)

    return df


def create_day_bins(df):

    conditions = [(df['time_to_pay'] >= 0) & (df['time_to_pay'] <= 30),
                                         (df['time_to_pay'] >= 31) & (df['time_to_pay'] <= 60),
                                         (df['time_to_pay'] >= 61) & (df['time_to_pay'] <= 90),
                                         df['time_to_pay'] >= 90]
    intervals = ['0-30 days', '31-60 days', '61-90 days', '90+ days']

    df['pay_time_intervals'] = np.select(conditions, intervals, default='No time interval')

    return df

acc_df = calc_day_difference(acc_df)
acc_df = create_day_bins(acc_df)


# Task 9 Create histogram of number of accounts with # of kids
fig = px.histogram(acc_df, x='kids_count_mode')
fig.show()

# Task 10
def top_4_agencies(df):
    # Plot 1 Data
    # Get the sum for the total paid amount for each agency
    groupby_agency = df.groupby(['agency'], sort=False, as_index=False)

    # Get the sum of amount_paid_total for unique accounts for each agency
    amount_paid_agency = groupby_agency['amount_paid_total'].sum()

    # Sort grouped accounts by amount_paid_total asc
    amount_paid_agency = amount_paid_agency.sort_values(by=['amount_paid_total'], ascending=False)

    # Get top 4 agencies
    top4 = amount_paid_agency['agency'].tolist()[:4]

    # Group by agency and time invervals
    groupby_agency_time = df.groupby(['agency', 'pay_time_intervals'])

    # Total amount paid for each agency for each time interval
    total_paid_per_time = groupby_agency_time['amount_paid_total'].sum().reset_index()

    # Only get rows that have the top 4 agencies
    target_rows = total_paid_per_time.agency.isin(top4)
    filtered_total_paid_per_time = total_paid_per_time[target_rows]

    # Plot 2 Data
    # Gets the total number of unique accounts (unique ID and date combination)
    total_accounts = groupby_agency_time.size().reset_index()
    total_accounts = total_accounts.rename(columns={0: 'total_accounts'})

    # Only get rows that have the top 4 agencies
    target_rows = total_accounts.agency.isin(top4)
    filtered_total_accounts = total_accounts[target_rows]

    return filtered_total_paid_per_time, filtered_total_accounts


agency_paid, agency_accounts = top_4_agencies(acc_df)

fig10 = px.bar(agency_paid, x='agency', y='amount_paid_total', color='pay_time_intervals', barmode='group',
               title='Total Money Collected from Top 4 Agencies',
               labels={'agency': 'Agencies', 'amount_paid_total': 'Amount Paid ($)',
                       'pay_time_intervals': 'Pay Time Intervals'})
fig10.show()

fig10_part2 = px.bar(agency_accounts, x='agency', y='total_accounts', color='pay_time_intervals', barmode='group',
                     title='Total Paying Accounts from Top 4 Agencies',
                     labels={'agency': 'Agencies', 'total_accounts': 'Number of Accounts',
                             'pay_time_intervals': 'Pay Time Intervals'})
fig10_part2.show()

acc_df.to_csv('cleaned_accounts_data.csv')
