from faker import Faker
import psycopg2
from psycopg2 import sql
import random as rn
import time
import faker_commerce

class InsertDataIntoPostgres:
    
    def __init__(self):
        self.DB_HOST = "localhost"
        self.DB_NAME = "test_db"
        self.DB_USER = "lotfy"
        self.DB_PASS = "lotfy123"
        self.DB_PORT = "5432"

    def generate_record(self):
        fake = Faker()
        fake.add_provider(faker_commerce.Provider)
        
        record = {}
        record['customer_first_name'] = fake.first_name()
        record['customer_last_name'] = fake.last_name()
        record['user_name'] = str(record['customer_first_name'])+'.'+str(record['customer_last_name'])+'@banana.com'
        record['password'] = fake.password()
        record['address'] = fake.address()
        record['country'] = fake.country()
        record['email'] = fake.email()
        record['product'] = fake.ecommerce_name()
        record['price'] = rn.randint(50, 8000)
        record['tax'] = rn.choice([0.10, 0.15])
        record['quantity'] = rn.randint(1, 20)
        record['delivary_fee'] = rn.choice([10, 20, 30])
        return record
    
    def establish_connection(self):
        try:
            conn=psycopg2.connect(
            host=self.DB_HOST,
            database=self.DB_NAME,
            user=self.DB_USER,
            password=self.DB_PASS,
            port=self.DB_PORT
            )
            print("connected to PostgreSQL successfully")
            return conn
        
        except Exception as e:
            print(f"connection to postgreSQL failed due to {e}")
            return None

    
    def insert_record(self, conn, record):
        cursor=conn.cursor()
        
        query=sql.SQL(
            f"""
            INSERT INTO sales(
                first_name,
                last_name,
                user_name,
                password,
                address,
                country,
                email,
                product,
                price,
                tax,
                quantity,
                delivary_fee
            )
            VALUES (
                {repr(record['customer_first_name'])},
                {repr(record['customer_last_name'])},
                {repr(record['user_name'])},
                {repr(record['password'])},
                {repr(record['address'])},
                {repr(record['country'])},
                {repr(record['email'])},
                {repr(record['product'])},
                {repr(record['price'])},
                {repr(record['tax'])},
                {repr(record['quantity'])},
                {repr(record['delivary_fee'])}
            );
            """)
        try:
            cursor.execute(query)
            print("record inserted !")
        except Exception as e:
            print(f"insert failed due to {e}")
            
        conn.commit()
        if cursor:
            cursor.close()
        if conn:
            conn.close()
        
    def start(self):
        conn=self.establish_connection()
        record=self.generate_record()
        self.insert_record(conn, record)

if __name__=="__main__":
    
    insert_a_record=InsertDataIntoPostgres()
    
    for i in range(100):
        n = rn.randint(1, 5)
        
        for i in range(n):
            insert_a_record.start()
            
        time.sleep(2)