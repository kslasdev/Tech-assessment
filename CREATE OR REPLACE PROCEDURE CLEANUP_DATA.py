import snowflake.connector


def clean_transactions():
    # for security we can import all this data from another file 
    conn = snowflake.connector.connect(
        user='kslas',
        password='K3v1n777--',
        account='uvofbuv-zeb36983',
        warehouse='COMPUTE_WH',
        database='DB01',
        schema='PUBLIC'
    )
    
    try:
        cur = conn.cursor()
        
     
        def remove_duplicates():
            sql = '''
                DELETE FROM TRANSACTIONS
                WHERE (CLIENT_NAME, CLIENT_LASTNAME, EMAIL, STORE_ID, STORE_NAME, LOCATION, PRODUCT_ID, PRODUCT_NAME, CATEGORY, BRAND, ADDRESS_ID, STREET, CITY, STATE, ZIP_CODE, TRANSACTION_ID, QUANTITY_OF_ITEMS_SOLD, UNIT_PRICE, DISCOUNT) IN (
                    SELECT CLIENT_NAME, CLIENT_LASTNAME, EMAIL, STORE_ID, STORE_NAME, LOCATION, PRODUCT_ID, PRODUCT_NAME, CATEGORY, BRAND, ADDRESS_ID, STREET, CITY, STATE, ZIP_CODE, TRANSACTION_ID, QUANTITY_OF_ITEMS_SOLD, UNIT_PRICE, DISCOUNT
                    FROM (
                        SELECT CLIENT_NAME, CLIENT_LASTNAME, EMAIL, STORE_ID, STORE_NAME, LOCATION, PRODUCT_ID, PRODUCT_NAME, CATEGORY, BRAND, ADDRESS_ID, STREET, CITY, STATE, ZIP_CODE, TRANSACTION_ID, QUANTITY_OF_ITEMS_SOLD, UNIT_PRICE, DISCOUNT,
                               ROW_NUMBER() OVER (PARTITION BY CLIENT_NAME, CLIENT_LASTNAME, EMAIL, STORE_ID, STORE_NAME, LOCATION, PRODUCT_ID, PRODUCT_NAME, CATEGORY, BRAND, ADDRESS_ID, STREET, CITY, STATE, ZIP_CODE, TRANSACTION_ID, QUANTITY_OF_ITEMS_SOLD, UNIT_PRICE, DISCOUNT ORDER BY CLIENT_NAME, CLIENT_LASTNAME, EMAIL, STORE_ID, STORE_NAME, LOCATION, PRODUCT_ID, PRODUCT_NAME, CATEGORY, BRAND, ADDRESS_ID, STREET, CITY, STATE, ZIP_CODE, TRANSACTION_ID, QUANTITY_OF_ITEMS_SOLD, UNIT_PRICE, DISCOUNT) AS row_num
                        FROM TRANSACTIONS
                    )
                    WHERE row_num > 1
                )
            '''
            return sql
     
        def fill_nulls():
            sql = '''
                UPDATE TRANSACTIONS
                SET 
                    CLIENT_NAME = COALESCE(CLIENT_NAME, CONCAT('client_', CLIENT_ID)),
                    CLIENT_LASTNAME = COALESCE(CLIENT_LASTNAME, 'lastname_' || CLIENT_ID), 
                    EMAIL = COALESCE(EMAIL, CONCAT('client_', CLIENT_ID, '.', CLIENT_LASTNAME, '_', CLIENT_ID, '@example.com')), 
                    STORE_NAME = COALESCE(STORE_NAME, CONCAT('store_', STORE_ID)),
                    LOCATION = COALESCE(LOCATION, CONCAT('location_', STORE_ID)),
                    PRODUCT_ID = COALESCE(PRODUCT_ID, 0),
                    PRODUCT_NAME = COALESCE(PRODUCT_NAME, CONCAT('Product_', PRODUCT_ID))
                WHERE
                     CLIENT_ID IS NULL OR CLIENT_NAME IS NULL OR CLIENT_LASTNAME IS NULL OR EMAIL IS NULL OR STORE_ID IS NULL OR STORE_NAME IS NULL OR LOCATION IS NULL OR PRODUCT_ID IS NULL OR PRODUCT_NAME IS NULL OR CATEGORY IS NULL OR BRAND IS NULL OR ADDRESS_ID IS NULL OR STREET IS NULL OR CITY IS NULL OR STATE IS NULL OR ZIP_CODE IS NULL OR TRANSACTION_ID IS NULL OR QUANTITY_OF_ITEMS_SOLD IS NULL OR UNIT_PRICE IS NULL OR DISCOUNT IS NULL;
            '''
            return sql
        
     
        def remove_special_characters():
            sql = '''
                UPDATE TRANSACTIONS
                SET
            '''
            columns = [
                'CLIENT_NAME', 'CLIENT_LASTNAME', 'EMAIL', 'STORE_NAME', 'LOCATION',
                'PRODUCT_NAME', 'CATEGORY', 'BRAND', 'STREET', 'CITY', 'STATE', 'ZIP_CODE'
                
            ]
            for column in columns:
                sql += f"{column} = REGEXP_REPLACE({column}, '[^a-zA-Z0-9 ]', ''), "
           
            sql = sql[:-2]
            return sql

       
        cur.execute(remove_duplicates())
        cur.execute(fill_nulls())
        cur.execute(remove_special_characters())
        
        
        cur.execute("COMMIT")
        cur.close()
        
        return 'The stored procedure executed successfully.'
        
    finally:
       
        conn.close()

resultado = clean_transactions()
print(resultado)
