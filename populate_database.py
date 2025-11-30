import psycopg2
import os

import psycopg2
from psycopg2 import extras
import csv
from pathlib import Path
import time
import datetime

from dotenv import load_dotenv
load_dotenv()

def generate_url():
    DATABASE_USERNAME = os.environ.get("DATABASE_USERNAME") 
    DATABASE_PASSWORD = os.environ.get("DATABASE_PASSWORD") 
    DATABASE_SERVER = os.environ.get("DATABASE_SERVER") 
    DATABASE_NAME = os.environ.get("DATABASE_NAME") 

    return f"postgresql://{DATABASE_USERNAME}:{DATABASE_PASSWORD}@{DATABASE_SERVER}/{DATABASE_NAME}"

DATABASE_URL = generate_url() 


from psycopg2 import Error

def create_connection(db_file, delete_db=False):
    if delete_db and os.path.exists(db_file):
        os.remove(db_file)

    conn = None
    try:
        conn = psycopg2.connect(DATABASE_URL)
    except Error as e:
        print(e)

    return conn


def create_table(conn, create_table_sql, drop_table_name=None):
    
    if drop_table_name: # You can optionally pass drop_table_name to drop the table. 
        try:
            c = conn.cursor()
            c.execute("""DROP TABLE IF EXISTS %s""" % (drop_table_name))
        except Error as e:
            print(e)
    
    try:
        c = conn.cursor()
        c.execute(create_table_sql)
    except Error as e:
        print(e)

    conn.commit()    
        
def execute_sql_statement(sql_statement, conn):
    cur = conn.cursor()
    cur.execute(sql_statement)
    rows = cur.fetchall()
    return rows


normalized_database = 'orders_normalized.db'
data_file = 'project2/data.csv'


## -- Connection dictionaries -- ##

def step2_create_region_to_regionid_dictionary(normalized_database_filename):

  conn_norm = create_connection(normalized_database_filename, delete_db=False)
  fetch_region = """select * from Region"""
  fetch_region_data = execute_sql_statement(fetch_region, conn_norm)

  region_dict = {reg : id for id, reg in fetch_region_data}
  
  conn_norm.close()
  return region_dict

def step4_create_country_to_countryid_dictionary(normalized_database_filename):
    
  conn_norm = create_connection(normalized_database_filename, delete_db=False)
  fetch_country = """select * from Country"""
  fetch_country_data = execute_sql_statement(fetch_country, conn_norm)

  country_dict = { c: c_id for c_id, c, r_id in fetch_country_data }
  
  conn_norm.close()
  return country_dict  

def step6_create_customer_to_customerid_dictionary(normalized_database_filename):
    
  conn_norm = create_connection(normalized_database_filename, delete_db=False)
  fetch_customer = """select * from Customer"""
  fetch_customer_data = execute_sql_statement(fetch_customer, conn_norm)

  customer_dict = { n1 + ' ' + n2 : c_id for c_id, n1, n2, a, city, ct_id in fetch_customer_data }
  
  conn_norm.close()
  return customer_dict 

def step8_create_productcategory_to_productcategoryid_dictionary(normalized_database_filename):
    
  conn_norm = create_connection(normalized_database_filename, delete_db=False)
  fetch_prd_cat = """select * from ProductCategory"""
  fetch_prd_cat_data = execute_sql_statement(fetch_prd_cat, conn_norm)

  prd_cat_dict = { pd_cat : cat_id for cat_id, pd_cat, p_cat_desc in fetch_prd_cat_data }
  
  conn_norm.close()
  return prd_cat_dict

def step10_create_product_to_productid_dictionary(normalized_database_filename):
    
  conn_norm = create_connection(normalized_database_filename, delete_db=False)
  fetch_prd = """select * from Product"""
  fetch_prd_data = execute_sql_statement(fetch_prd, conn_norm)

  prd_dict = { p_name : p_id for p_id, p_name, p_price, p_cat_id in fetch_prd_data }
  
  conn_norm.close()
  return prd_dict


## -- Table creation -- ##

def step1_create_region_table(data_filename, normalized_database_filename):
    # Inputs: Name of the data and normalized database filename
    # Output: None

    ## Extracting Region ##
    region_list = []
    with open(data_filename, 'r') as f:
        lines = f.readlines()

        if not lines:
            raise ValueError("CSV file is empty.")

        for line in lines[1:]:
            val_list = line.strip().split('\t')
            row_data = tuple((val_list[4],))
            if row_data not in region_list:
              region_list.append(row_data)

    region_list1 = sorted(region_list, key = lambda a: a[0])
    print(region_list1[0:2])

    ## Creating Region Table ##
    conn_norm = create_connection(normalized_database_filename, delete_db=False)
    create_table_region = """ CREATE TABLE IF NOT EXISTS Region(
        RegionID SERIAL not null primary key,
        Region TEXT not null
    )"""
    create_table(conn_norm, create_table_region, drop_table_name="Region")
    print("table created")

    ## Inserting Data ##
    with conn_norm:
        region_insert = """ INSERT INTO Region(Region) VALUES(%s)"""
        cur = conn_norm.cursor()
        cur.executemany(region_insert, region_list1)
        cur.close()
    print("data inserted")
    
    conn_norm.close()

step1_create_region_table(data_file, normalized_database)


def step3_create_country_table(data_filename, normalized_database_filename):
    # Inputs: Name of the data and normalized database filename
    # Output: None
    
    ## Extracting Country Region ##
    country_region_list = []
    with open(data_filename, 'r') as f:
        lines = f.readlines()

        if not lines:
            raise ValueError("CSV file is empty.")

        for line in lines[1:]:
            val_list = line.strip().split('\t')
            row_data = tuple((val_list[3], val_list[4]))
            if row_data not in country_region_list:
              country_region_list.append(row_data)

    country_region_list1 = sorted(country_region_list, key = lambda a: a[0])
    print(country_region_list1[0:2])

    region_dict = step2_create_region_to_regionid_dictionary(normalized_database_filename)
    country_region_list2 = list(map(lambda a: (a[0],region_dict[a[1]]) , country_region_list1 ))

    ## Creating Country Table ##
    conn_norm = create_connection(normalized_database_filename, delete_db=False)
    create_table_country = """ CREATE TABLE IF NOT EXISTS Country(
        CountryID SERIAL not null Primary key,
        Country Text not null,
        RegionID integer not null,
        FOREIGN KEY(RegionID) REFERENCES Region(RegionID)
    )"""
    create_table(conn_norm, create_table_country, drop_table_name="Country")

    ## Inserting Data ##
    with conn_norm:
        country_insert = """ INSERT INTO Country(Country,RegionID) VALUES(%s,%s)"""
        cur = conn_norm.cursor()
        cur.executemany(country_insert, country_region_list2)
        cur.close()
    
    conn_norm.close()

step3_create_country_table(data_file, normalized_database)


def step5_create_customer_table(data_filename, normalized_database_filename):

    ## Extracting Data ##
    customer_list = []
    with open(data_filename, 'r') as f:
        lines = f.readlines()

        if not lines:
            raise ValueError("CSV file is empty.")

        for line in lines[1:]:
            val_list = line.strip().split('\t')
            f_name, l_name = val_list[0].split(' ',1)
            row_data = tuple((f_name, l_name, val_list[1], val_list[2], val_list[3]))
            if row_data not in customer_list:
              customer_list.append(row_data)

    customer_list = sorted(customer_list, key = lambda a: a[0]+a[1])

    country_dict = step4_create_country_to_countryid_dictionary(normalized_database_filename)
    customer_list1 = list(map(lambda a: (a[0],a[1],a[2],a[3],country_dict[a[4]]) , customer_list))
    print(customer_list1[0:2])

    ## Creating Table ##
    conn_norm = create_connection(normalized_database_filename, delete_db=False)
    create_table_customer = """ CREATE TABLE IF NOT EXISTS Customer(
        CustomerID SERIAL not null Primary Key,
        FirstName Text not null,
        LastName Text not null,
        Address Text not null,
        City Text not null,
        CountryID integer not null,
        UNIQUE (FirstName, LastName, Address),
        FOREIGN KEY(CountryID) REFERENCES Country(CountryID)
    )"""
    create_table(conn_norm, create_table_customer, drop_table_name="Customer")
    print("table created")

    ## Inserting Data ##
    with conn_norm:
        customer_insert = """ INSERT INTO Customer(FirstName,LastName,Address,City,CountryID) 
                        VALUES(%s,%s,%s,%s,%s)
                        ON CONFLICT (FirstName, LastName, Address) DO NOTHING"""
        cur = conn_norm.cursor()
        cur.executemany(customer_insert, customer_list1)
        cur.close()
    print("data inserted")
    
    conn_norm.close()

step5_create_customer_table(data_file, normalized_database)


def step7_create_productcategory_table(data_filename, normalized_database_filename):
    # Inputs: Name of the data and normalized database filename
    # Output: None

    ## Extracting Data ##
    prd_cat_list = []
    with open(data_filename, 'r') as f:
        lines = f.readlines()

        if not lines:
            raise ValueError("CSV file is empty.")

        for line in lines[1:]:
            val_list = line.strip().split('\t')
            prd_cat = val_list[6].strip().split(';')
            prd_cat_desc = val_list[7].strip().split(';')
            for i in range(len(prd_cat)):
                row_data = tuple((prd_cat[i],prd_cat_desc[i]))
                if row_data not in prd_cat_list:
                    prd_cat_list.append(row_data)

    prd_cat_list = sorted(prd_cat_list, key = lambda a: a[0])
    print(prd_cat_list[0:2])

    ## Creating Table ##
    conn_norm = create_connection(normalized_database_filename, delete_db=False)
    create_table_prd_cat = """ CREATE TABLE IF NOT EXISTS ProductCategory(
        ProductCategoryID SERIAL not null Primary Key,
        ProductCategory Text not null,
        ProductCategoryDescription Text not null,
        UNIQUE(ProductCategory)
    )"""
    create_table(conn_norm, create_table_prd_cat, drop_table_name="ProductCategory")
    print("table created")

    ## Inserting Data ##
    with conn_norm:
        prd_cat_insert = """ INSERT INTO ProductCategory(ProductCategory,ProductCategoryDescription) 
                        VALUES(%s,%s)
                        ON CONFLICT (ProductCategory) DO NOTHING"""
        cur = conn_norm.cursor()
        cur.executemany(prd_cat_insert, prd_cat_list)
        cur.close()
    print("data inserted")
    
    conn_norm.close()

step7_create_productcategory_table(data_file, normalized_database)


def step9_create_product_table(data_filename, normalized_database_filename):
    # Inputs: Name of the data and normalized database filename
    # Output: None

    ## Extracting Data ##
    product_list = []
    with open(data_filename, 'r') as f:
        lines = f.readlines()

        if not lines:
            raise ValueError("CSV file is empty.")

        for line in lines[1:]:
            val_list = line.strip().split('\t')
            prd_name = val_list[5].strip().split(';')
            prd_unit_p = val_list[8].strip().split(';')
            prd_cat = val_list[6].strip().split(';')
            
            for i in range(len(prd_cat)):
                row_data = tuple((prd_name[i],prd_unit_p[i],prd_cat[i]))
                if row_data not in product_list:
                    product_list.append(row_data)

    product_list = sorted(product_list, key = lambda a: a[0])

    prd_cat_dict = step8_create_productcategory_to_productcategoryid_dictionary(normalized_database_filename)
    product_list1 = list(map(lambda a: (a[0],a[1],prd_cat_dict[a[2]]) , product_list))
    print(product_list1[0:2])

    ## Creating Table ##
    conn_norm = create_connection(normalized_database_filename, delete_db=False)
    create_table_prd = """ CREATE TABLE IF NOT EXISTS Product(
        ProductID SERIAL not null Primary key,
        ProductName Text not null,
        ProductUnitPrice Real not null,
        ProductCategoryID integer not null,
        UNIQUE (ProductName),
        FOREIGN KEY(ProductCategoryID) REFERENCES ProductCategory(ProductCategoryID)
    )"""
    create_table(conn_norm, create_table_prd, drop_table_name="Product")
    print("table created")
    
    ## Inserting Data ##
    with conn_norm:
        prd_insert = """ INSERT INTO Product(ProductName,ProductUnitPrice, ProductCategoryID) 
                        VALUES(%s,%s,%s)
                        ON CONFLICT (ProductName) DO NOTHING"""
        cur = conn_norm.cursor()
        cur.executemany(prd_insert, product_list1)
        cur.close()
    print("dats inserted")
    
    conn_norm.close()

step9_create_product_table(data_file, normalized_database)



def step11_create_orderdetail_table(data_filename, normalized_database_filename, batch_size = 50000):
    # Inputs: Name of the data and normalized database filename
    # Output: None

    ## Creating Table ##
    conn_norm = create_connection(normalized_database_filename, delete_db=False)
    create_table_ord = """ CREATE TABLE IF NOT EXISTS OrderDetail(
        OrderID SERIAL not null Primary Key,
        CustomerID integer not null,
        ProductID integer not null,
        OrderDate TIMESTAMP not null,
        QuantityOrdered integer not null,
        UNIQUE (CustomerID, ProductID),
        FOREIGN KEY(CustomerID) REFERENCES Customer(CustomerID),
        FOREIGN KEY(ProductID) REFERENCES Product(ProductID)
    )"""
    create_table(conn_norm, create_table_ord, drop_table_name="OrderDetail")
    print("table created")

    ## Extracting Data ##

    
    prd_dict = step10_create_product_to_productid_dictionary(normalized_database_filename)
    customer_dict = step6_create_customer_to_customerid_dictionary(normalized_database_filename)
    row_count_total = 0
    order_list = []
    
    with open(data_filename, 'r') as f:
        lines = f.readlines()

        if not lines:
            raise ValueError("CSV file is empty.")
        
        for line in lines[1:]:
            val_list = line.strip().split('\t')
            name = val_list[0].strip()
            prd_name = val_list[5].strip().split(';')
            order_dt = val_list[10].strip().split(';')
            qt_ord = val_list[9].strip().split(';')

            for i in range(len(prd_name)):
                order_dt1 = datetime.datetime.strptime(order_dt[i], '%Y%m%d').strftime('%Y-%m-%d')
                row_data = tuple((name,prd_name[i],order_dt1,int(qt_ord[i])))
                order_list.append(row_data)

                row_count_total += 1

            if row_count_total >= batch_size:
                order_list1 = [(customer_dict[name], prd_dict[prd_name_e],order_dt1,qt_ord_e) for name, prd_name_e,order_dt1,qt_ord_e in order_list]
                print(order_list1[0:2])
                
                with conn_norm:
                    ord_insert = """ INSERT INTO OrderDetail(CustomerID,ProductID,OrderDate,QuantityOrdered) 
                        VALUES(%s,%s,%s,%s)
                        ON CONFLICT (CustomerID, ProductID) DO NOTHING"""
                    cur = conn_norm.cursor()
                    cur.executemany(ord_insert, order_list1)
                    cur.close()
                    conn_norm.commit()

                print(f"inserted {row_count_total} rows")
                row_count_total = 0
                order_list.clear()

        if row_count_total:
            order_list1 = [(customer_dict[name], prd_dict[prd_name_e],order_dt1,qt_ord_e) for name, prd_name_e,order_dt1,qt_ord_e in order_list]

            with conn_norm:
                    ord_insert = """INSERT INTO OrderDetail(CustomerID,ProductID,OrderDate,QuantityOrdered) 
                        VALUES(%s,%s,%s,%s)
                        ON CONFLICT (CustomerID, ProductID) DO NOTHING"""
                    cur = conn_norm.cursor()
                    cur.executemany(ord_insert, order_list1)
                    cur.close()
                    conn_norm.commit()

            print(f"inserted {row_count_total} rows")
        
        conn_norm.close()

    

step11_create_orderdetail_table(data_file, normalized_database)
