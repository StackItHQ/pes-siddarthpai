import os
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError


database_url = 'postgresql://postgres:roosh123@localhost:5432/superjoin' # got these details using /conninfo

try:
    engine = create_engine(database_url)
    with engine.connect() as connection:
        result = connection.execute(text("SELECT table_name FROM information_schema.tables WHERE table_schema='public'"))
        tables = result.fetchall()
        print("Successfully connected to the database!")
        print("Tables in the database:")
        for table in tables:
            print(table[0])
except SQLAlchemyError as e:
    print(f"An error occurred while connecting to the database: {e}")