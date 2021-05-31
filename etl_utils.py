import pandas as pd
import requests
import numpy as np
import psycopg2
'''
The faux data lake is to represent a cloud based storage like s3 or GCS
'''
# modeled as loading job
def _load_platinum_customers_to_db(df): 
    connection = psycopg2.connect(user="postgres",
                                  password="postgres",
                                  host="127.0.0.1",
                                  port="5432",
                                  database="customers")

     
    cursor = connection.cursor()
    # Print PostgreSQL details
    print("PostgreSQL server information")
    print(connection.get_dsn_parameters(), "\n")

    create_table_query = """
    CREATE TABLE IF NOT EXISTS platinum_customers(
        user_id INTEGER PRIMARY KEY not null,
        product_name VARCHAR(200) not null,
        total_purchase_value FLOAT not null,
        timestamp date not null default CURRENT_DATE
    )
    """


    try:
        cursor.execute(create_table_query)
        connection.commit()
        print("Table created successfully in PostgreSQL ")
        df.to_sql("platinum_customers", connection, index=False, if_exists='append')
        
    except (Exception) as error:
        print("Error while connecting to PostgreSQL", error)
    finally:
        if connection:
            cursor.close()
            connection.close()
            print("PostgreSQL connection is closed")
    

# modeled as extraction jobs

def pull_user_data(): 
    user_data = requests.get('http://0.0.0.0:5000/users')
    user_lean_customer_data = pd.read_json(user_data.json())
    user_lean_customer_data.to_csv('faux_data_lake/user_lean_customer_data.csv')

def pull_product_data(): 
    product_data = requests.get('http://0.0.0.0:5000/products')
    product_lean_customer_data = pd.read_json(product_data.json())
    product_lean_customer_data.to_csv('faux_data_lake/product_lean_customer_data.csv')

def pull_transaction_data(): 
    transaction_data = requests.get('http://0.0.0.0:5000/transactions')
    transaction_lean_customer_data = pd.read_json(transaction_data.json())
    transaction_lean_customer_data.to_csv('faux_data_lake/transaction_lean_customer_data.csv')

# modeled as transformation jobs

def get_platinum_customer(): 
    '''
    a platinum customer has purchased goods worth over 5000 
    '''
    transaction_lean_customer_data = pd.read_csv('faux_data_lake/transaction_lean_customer_data.csv')
    user_lean_customer_data = pd.read_csv('faux_data_lake/user_lean_customer_data.csv')
    user_tx_data = pd.merge(user_lean_customer_data,transaction_lean_customer_data)
    # also need product lean_customer_data for product price
    product_lean_customer_data = pd.read_csv('faux_data_lake/product_lean_customer_data.csv')
    product_lean_customer_data = product_lean_customer_data[['product_id','price','product_name']]
    
    enriched_customer_data = pd.merge(user_tx_data,product_lean_customer_data)
    # get total value
    enriched_customer_data['total_purchase_value'] = enriched_customer_data['quantity'] * enriched_customer_data['price']
    #retain only the columns necessary for analysis
    lean_customer_data = enriched_customer_data[['user_id',
                                                     'total_purchase_value',
                                                     'product_name',
                                                     ]]
    # get total purchase value per customer
    lean_customer_data = lean_customer_data.groupby('user_id', 
                                                  as_index=False).agg(
                                                      {'total_purchase_value': ['sum'],
                                                       'product_name':pd.Series.mode}) # most common product
    # filter platinum customers PREDICATE: total_purchase_value>=5000
    platinum_customers = lean_customer_data.loc[lean_customer_data['total_purchase_value'] >= 5000]
    # save to csv file
    platinum_customers.to_csv('aux_data_lake/platinum_customers')
    # to database
    _load_platinum_customers_to_db(platinum_customers)
    
    
def get_basket_analysis_dataset(): 
    '''
    group by purchase ID and store data
    '''
    transaction_lean_customer_data = pd.read_csv('faux_data_lake/transaction_lean_customer_data.csv')
    transaction_lean_customer_data = transaction_lean_customer_data[['product_id','quantity','purchase_id']]
    # pivot to have purchase ID as index, product IDs as columns and quantity(mean) as the values
    # for basket analysis, one is considering what goes together(cause and effect). So average is a better agg function
    transaction_lean_customer_data.pivot(index='purchase_id', columns='product_id',values='quantity')
    transaction_lean_customer_data.to_csv('faux_data_lake/basket_analysis.csv')


def get_recommendation_engine_dataset(): 
    '''
    group by user ID and send to data lake as dataset
    '''
    transaction_lean_customer_data = pd.read_csv('faux_data_lake/transaction_lean_customer_data.csv')
    transaction_lean_customer_data = transaction_lean_customer_data[['user_id','quantity','product_id']]
    # pivot to have user ID as index, product IDs as columns and quantity(sum) as the values
    # for recommendation engines, it may be critical to know what kind of products are bought in large quantities over time
    transaction_lean_customer_data = pd.pivot_table(transaction_lean_customer_data,index='user_id', 
                   columns='product_id',
                   values='quantity', aggfunc=np.sum)
    
    transaction_lean_customer_data.to_csv('faux_data_lake/recommendation_engine_analysis.csv')
    






