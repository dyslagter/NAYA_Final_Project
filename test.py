import psycopg2

# PostgreSQL configuration
POSTGRESQL_HOST = "postgres"
POSTGRESQL_PORT = "5432"
POSTGRESQL_DATABASE = "postgres"
POSTGRESQL_USER = "postgres"
POSTGRESQL_PASSWORD = "postgres"
POSTGRESQL_TABLE = "demo_table"


def create_table_and_insert_data():
    try:
        # Connect to PostgreSQL
        connection = psycopg2.connect(
            host=POSTGRESQL_HOST,
            port=POSTGRESQL_PORT,
            database=POSTGRESQL_DATABASE,
            user=POSTGRESQL_USER,
            password=POSTGRESQL_PASSWORD,
        )
        cursor = connection.cursor()

        # Create table
        cursor.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {POSTGRESQL_TABLE} (
                id SERIAL PRIMARY KEY,
                name VARCHAR(50) NOT NULL,
                value FLOAT NOT NULL
            );
        """
        )
        print("Table created successfully (if it didn't already exist).")

        # Insert demo data
        demo_data = [("Item A", 100.5), ("Item B", 200.75), ("Item C", 300.0)]

        for name, value in demo_data:
            cursor.execute(
                f"""
                INSERT INTO {POSTGRESQL_TABLE} (name, value)
                VALUES (%s, %s)
            """,
                (name, value),
            )

        # Commit changes
        connection.commit()
        print("Demo data inserted successfully.")

        # Close connection
        cursor.close()
        connection.close()
        print("Connection closed.")

    except Exception as e:
        print(f"Error interacting with PostgreSQL: {e}")


if __name__ == "__main__":
    create_table_and_insert_data()
