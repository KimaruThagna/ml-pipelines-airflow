# ETL-pipelines-airflow
Demonstrating and Building ETL pipelines in Airflow
This repo demonstrates a use case for n Ecommerce business that has a platform that generates transaction data each time a purchase is made. With this transaction data, the functions in the pipeline seek to answer 3 business questions

1. Who is our platinum customer? Anyone with purchase value equal to or more than 5000
2. What is the purchase history like for each user? This builds a dataset that can be used for a *recommendation engine* downstream
3. What items are commonly purchased together? This builds a dataset that can be used for *Basket Analysis* downstream

## Analysis Implementation
The code can be found in `etl_utils.py` file.
Question 1 is implemented using `pd.merge()` to get the combined dataset and `df.groupby().sum()` to get total purchases. 

To get the platinum customer, we apply a filter 

`final_df = df.loc[df['total_purchase_value']>=5000]`

Both question 2 and 3 are achieved using Pandas **Pivot Tables** `pd.pivtot_table()`
