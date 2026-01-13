import psycopg2
from psycopg2 import pool
import os
from dotenv import load_dotenv

load_dotenv()

class Database:
    def __init__(self):
        self.connection_pool = psycopg2.pool.SimpleConnectionPool(
            1, 20,
            host=os.getenv('DB_HOST', 'localhost'),
            database=os.getenv('DB_NAME', 'job_market_db'),
            user=os.getenv('DB_USER', 'postgres'),
            password=os.getenv('DB_PASSWORD'),
            port=os.getenv('DB_PORT', '5432')
        )
    
    def get_connection(self):
        return self.connection_pool.getconn()
    
    def return_connection(self, conn):
        self.connection_pool.putconn(conn)
    
    def create_tables(self):
        conn = self.get_connection()
        cursor = conn.cursor()
        
        # Jobs table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS jobs (
                id SERIAL PRIMARY KEY,
                job_id VARCHAR(255) UNIQUE,
                title VARCHAR(500),
                company VARCHAR(500),
                location VARCHAR(500),
                salary_min DECIMAL,
                salary_max DECIMAL,
                description TEXT,
                requirements TEXT,
                url VARCHAR(1000),
                source VARCHAR(100),
                posted_date TIMESTAMP,
                scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                is_active BOOLEAN DEFAULT TRUE
            )
        """)
        
        conn.commit()
        cursor.close()
        self.return_connection(conn)
        print("✅ Database tables created successfully!")

if __name__ == "__main__":
    db = Database()
    db.create_tables()