import pandas as pd
import requests, os
import numpy as np
import psycopg2
from airflow.models import Variable
from sqlalchemy import create_engine
from sqlalchemy.sql.type_api import Variant
'''
The faux data lake is to represent a cloud based storage like s3 or GCS
'''
file_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
create_table_query = """
    CREATE TABLE IF NOT EXISTS platinum_customers(
        user_id INTEGER PRIMARY KEY not null,
        total_purchase_value FLOAT not null,
        timestamp date not null default CURRENT_DATE
    )
    """
    
    
# modeled as loading job
def _load_platinum_customers_to_db(df): 
    connection = psycopg2.connect(user=Variable.get("POSTGRES_USER"),
                                  password=Variable.get("POSTGRES_PASSWORD"),
                                  host="remote_db",
                                  database=Variable.get("DB_NAME"))

     
    cursor = connection.cursor()
    # Print PostgreSQL details
    print("PostgreSQL server information")
    print(connection.get_dsn_parameters(), "\n")

    


    try:
        con = create_engine(f'postgresql://{Variable.get("POSTGRES_USER")}:{Variable.get("POSTGRES_PASSWORD")}@remote_db:{Variable.get("DB_PORT")}/{Variable.get("DB_NAME")}')
        df.to_sql("platinum_customers", con, index=False,if_exists='append')

        
    except (Exception) as error:
        print("Error while connecting to PostgreSQL", error)
    finally:
        if connection:
            cursor.close()
            connection.close()
            print("PostgreSQL connection is closed")
    

# modeled as extraction jobs

def pull_user_data(): 
    user_data = requests.get('http://mock-api:5000/users')
    user_data = pd.DataFrame(user_data.json())
    user_data.to_csv(os.path.join(file_root,'faux_data_lake/user_lean_customer_data.csv'), index=False)

def pull_product_data(): 
    product_data = requests.get('http://mock-api:5000/products')
    product_data = pd.DataFrame(product_data.json())
    product_data.to_csv(os.path.join(file_root,'faux_data_lake/product_lean_customer_data.csv'), index=False)

def pull_transaction_data(): 
    transaction_data = requests.get('http://mock-api:5000/transactions')
    transaction_data = pd.DataFrame(transaction_data.json())
    transaction_data.to_csv(os.path.join(file_root,'faux_data_lake/transaction_lean_customer_data.csv'), index=False)

# modeled as transformation jobs

def get_platinum_customer(): 
    '''
    a platinum customer has purchased goods worth over 5000 
    '''
    transaction_data = pd.read_csv(os.path.join(file_root,'faux_data_lake/transaction_lean_customer_data.csv'))
    user_data = pd.read_csv(os.path.join(file_root,'faux_data_lake/user_lean_customer_data.csv'))
    user_tx_data = pd.merge(user_data,transaction_data)
    # also need product lean_customer_data for product price
    product_data = pd.read_csv(os.path.join(file_root,'faux_data_lake/product_lean_customer_data.csv'))
    product_data = product_data[['product_id','price','product_name']]
    
    enriched_customer_data = pd.merge(user_tx_data,product_data)
    # get total value
    enriched_customer_data['total_purchase_value'] = enriched_customer_data['quantity'] * enriched_customer_data['price']
    #retain only the columns necessary for analysis
    lean_customer_data = enriched_customer_data[['user_id',
                                                     'total_purchase_value',
                                                     ]]
    # get total purchase value per customer
    lean_customer_data = lean_customer_data.groupby(['user_id']).sum().reset_index()
    # filter platinum customers PREDICATE: total_purchase_value>=10000
    platinum_customers = lean_customer_data.loc[lean_customer_data['total_purchase_value'] >= 10000]
    # save to csv file
    platinum_customers.to_csv(os.path.join(file_root,'faux_data_lake/platinum_customers.csv'), index=False)
    # to database
    _load_platinum_customers_to_db(platinum_customers)
    
    
    # special case: FIND BIG SPENDER CUSTOMERS WITH TOTAL VALUE OF 5000 PER PRODUCT
    
    special_customer_data = enriched_customer_data[['user_id',
                                                     'total_purchase_value',
                                                     'product_name',
                                                     ]]
    
    special_customer_data = special_customer_data.groupby(['user_id','product_name']).sum().reset_index()
    
    special_customers = special_customer_data.loc[special_customer_data['total_purchase_value'] >= 5000]
    # save to csv file
    special_customers.to_csv(os.path.join(file_root,'faux_data_lake/platinum_customers_per_product.csv'), index=False)
    
    
    
def get_basket_analysis_dataset(): 
    '''
    group by purchase ID and store data
    '''
    transaction_data = pd.read_csv(os.path.join(file_root,'faux_data_lake/transaction_lean_customer_data.csv'))
    transaction_data = transaction_data[['product_id','quantity','purchase_id']]
    # group to have unique purchase ID
    grouped_data = transaction_data.groupby(['purchase_id','product_id']).count().reset_index()
    # pivot to have purchase ID as index, product IDs as columns and quantity(mean) as the values
    # for basket analysis, one is considering what goes together(cause and effect). So average is a better agg function
    grouped_data = pd.pivot_table(grouped_data,index='purchase_id', 
                   columns='product_id',
                   values='quantity', 
                   fill_value=0, # empty values that may arise from pivoting
                   ).add_suffix('_product_id')
    
    grouped_data.to_csv(os.path.join(file_root,'faux_data_lake/basket_analysis.csv'), index=False)


def get_recommendation_engine_dataset(): 
    '''
    group by user ID and send to data lake as dataset
    '''
    transaction_data = pd.read_csv(os.path.join(file_root,'faux_data_lake/transaction_lean_customer_data.csv'))
    transaction_data = transaction_data[['user_id','quantity','product_id']]
    # pivot to have user ID as index, product IDs as columns and quantity(sum) as the values
    # for recommendation engines, it may be critical to know what kind of products are bought in large quantities over time
    transaction_data = pd.pivot_table(transaction_data,index='user_id', 
                   columns='product_id',
                   values='quantity', 
                   fill_value=0,
                   aggfunc=np.sum).add_suffix('_product_id')
    
    transaction_data.to_csv(os.path.join(file_root,'faux_data_lake/recommendation_engine_analysis.csv'), index=False)
    

print(file_root)
