 #PostgreSQL adapter for Python which allows python to interact with postgresql
import psycopg2

#return the connection object which interact with db
def connect_to_database():
    return psycopg2.connect(
        database="obsrv",
        user="postgres",
        password="postgres",
        host="localhost",
        port=5432
    )
