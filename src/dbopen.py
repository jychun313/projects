import pyupbit
import yaml
import pymysql
import pandas as pd
import bcrypt

class DBOpen:
    """
    :param bool boolean_: control permission status (True or False)
    Example)
    db = DBOpen(False)  >> select
    db = DBOpen(True)   >> select, delete, update, insert
    """

    def __init__(self, boolean_, view_, config_file_path_):
        # Load the YAML file
        self.config_file_path = config_file_path_
        with open(self.config_file_path, 'r') as file: config = yaml.safe_load(file)

        # Accessing data
        self.host = config['database']['host']
        self.hostname = config['database']['hostname']
        self.port = config['database']['port']
        self.user = config['database']['user']
        self.password = config['database']['password']
        self.database_name = config['database']['database_name']
        self.table_upbit = config['database']['table_upbit']
        self.table_user = config['database']['table_user']
        self.created_date = config['database']['created_date']
        
        # Adjusting return values
        self.view = view_
        
        # Global variable for allowing modification of DB tables.
        self.modification_permit = boolean_
        
        self.conn = None

    def close_connection(self):
        self.conn.close()
        return
    
    def create_connection(self):
        conn = None
        try:
            conn = pymysql.connect(
                host=self.host, user=self.user, password=self.password, database=self.database_name,
                cursorclass=pymysql.cursors.DictCursor  # To receive results in dictionary format
            )
            self.conn = conn
            return conn
        except Exception as e:
            print(e)
        return conn
    
    def show_tables(self):
        """
        db.show_tables()
        """

        conn = self.create_connection()
        query = f"SHOW TABLES;"
        try:
            c = conn.cursor()
            c.execute(query)
            tables = c.fetchall()
            for no, table in enumerate(tables):
                no+=1
                table = table[f'Tables_in_{self.database_name}']
                print(str(no).zfill(2), table)
        except Exception as e:
            print(e)
    
    def create_table(self, query):
        """
        query = "CREATE TABLE TABLE_NAME (ID INT AUTO_INCREMENT PRIMARY KEY, TIMESTAMP TIMESTAMP NOT NULL, NAME VARCHAR(255) NOT NULL, OPEN FLOAT NULL, CREATED_AT TIMESTAMP DEFAULT CURRENT_TIMESTAMP);"
        db.create_table(query)
        """
        if not self.modification_permit:
            print("CREATE TABLE not permitted!")
            return
        
        conn = self.create_connection()
        try:
            c = conn.cursor()
            c.execute(query)
        except Exception as e:
            print(e)
    
    def select_user_info(self, **kwargs) :
        """
        db.select_user_info()
        """
        query = f'SELECT * FROM {self.table_user};'
        conn = self.create_connection()
        try:
            c = conn.cursor()
            c.execute(query)
            result = c.fetchall()
            res = pd.DataFrame(result)
            conn.close()
            return res
        except Exception as e:
            print(e)
    
    def insert_user_info(self, dataframe):
        """
        db.insert_user_info(dataframe)
        """
        if not self.modification_permit:
            print("insert not permitted!")
            return
        
        conn = self.create_connection()
        try:
            c = conn.cursor()
            for i, row in dataframe.iterrows():
                columns = ", ".join(row.index)
                placeholders = ", ".join(["%s"] * len(row))
                sql = f"INSERT INTO {self.table_user} ({columns}) VALUES ({placeholders})"
                values_to_insert = tuple(row[col] for col in row.index)
                c.execute(sql, values_to_insert)
            conn.commit()
        except Exception as e:
            print('insert error')
            print(e)    
        
        conn.close()
    
    def delete_user_info(self):
        """
        ex) db.delete_user_info()
        """
        if not self.modification_permit:
            print("delete not permitted!")
            return
        
        query = f"TRUNCATE TABLE {self.table_user};"
        conn = self.create_connection()
        try:
            c = conn.cursor()
            c.execute(query)
            conn.commit()
        except Exception as e:
            print('truncate error')
            print(e)
        
        conn.close()
    
    def select_from_table(self, **kwargs) :
        """
        db.select_from_table()
        """
        query = f'SELECT * FROM {self.table_upbit};'
        conn = self.create_connection()
        try:
            c = conn.cursor()
            c.execute(query)
            result = c.fetchall()
            res = pd.DataFrame(result)
            conn.close()
            return res
        except Exception as e:
            print(e)
    
    def insert_into_table(self, dataframe):
        """
        db.insert_into_table(dataframe)
        """
        if not self.modification_permit:
            print("insert not permitted!")
            return
        
        conn = self.create_connection()
        try:
            c = conn.cursor()
            for i, row in dataframe.iterrows():
                columns = ", ".join(row.index)
                placeholders = ", ".join(["%s"] * len(row))
                sql = f"INSERT INTO {self.table_upbit} ({columns}) VALUES ({placeholders})"
                values_to_insert = tuple(row[col] for col in row.index)
                c.execute(sql, values_to_insert)
            conn.commit()
        except Exception as e:
            print('insert error')
            print(e)    
        
        conn.close()
    
    def delete_from_table(self, *list_ids):
        """
        ex) db.delete_from_table([1,2,3,4,5])
        """
        if not self.modification_permit:
            print("delete not permitted!")
            return
        
        query = ''
        if len(list_ids) == 0:
            query = f"TRUNCATE TABLE {self.table_upbit};"
            conn = self.create_connection()
            try:
                c = conn.cursor()
                c.execute(query)
                conn.commit()
            except Exception as e:
                print('delete (truncate) error')
                print(e)
        else:
            query = f"DELETE FROM {self.table_upbit} WHERE ID IN (%s)" % ','.join(['%s'] * len(*list_ids))
            conn = self.create_connection()
            try:
                c = conn.cursor()
                c.execute(query, tuple(*list_ids))
                conn.commit()
            except Exception as e:
                print(f"Error deleting rows: {str(e)}")
        
        conn.close()


    # Recent OHLCV data (Upbit)
    def recent_ohlcv(self, DAYS):
        df = pyupbit.get_ohlcv('KRW-ADA', interval='day', to=pd.Timestamp.now(), count=DAYS).reset_index()
        df.columns = [col.upper() for col in df.columns]
        df['NAME'] = 'ADA'
        df.rename(columns={'INDEX': 'TIMESTAMP'}, inplace=True)
        df = df.iloc[:-1, :]
        return df
    
    # Streamlit Login
    def login(self, username, password, USER_DATA):
        """User Authentication Method"""
        if username in USER_DATA and bcrypt.checkpw(password.encode('utf-8'),
                                                    USER_DATA[username].encode('utf-8')):
            return True
        return False
    
    # Verify User Login
    def verify_user_login(self, ENTERED_USER_NAME, ENTERED_PASSWORD):
        """
        Retrieve the stored hash from the database
        """
        query = f'SELECT PASSWORD FROM {self.table_user} WHERE USERNAME = %s'
        conn = self.create_connection()
        try:
            c = conn.cursor()
            c.execute(query, (ENTERED_USER_NAME,))
            result = c.fetchone()
            if result:
                stored_hash = result['PASSWORD'].encode('utf-8')
                if bcrypt.checkpw(ENTERED_PASSWORD.encode('utf-8'), stored_hash):
                    print('Login successful!')
                else:
                    print('Wrong password..')
            else:
                print(f'{ENTERED_USER_NAME} not found.. Check your username..')
        except Exception as e:
            print(e)

        return

