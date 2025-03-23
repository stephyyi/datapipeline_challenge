# debug_db.py
import sqlite3

# Connect to the database
conn = sqlite3.connect('data_pipeline.db')
cursor = conn.cursor()

# Check if the table exists
cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
tables = cursor.fetchall()
print(f"Tables in database: {tables}")

# If processed_data exists, check its structure and sample data
if ('processed_data',) in tables:
    # Get column names
    cursor.execute("PRAGMA table_info(processed_data);")
    columns = cursor.fetchall()
    print("\nTable structure:")
    for col in columns:
        print(col)
    
    # Get row count
    cursor.execute("SELECT COUNT(*) FROM processed_data;")
    count = cursor.fetchone()[0]
    print(f"\nTotal rows: {count}")
    
    # Get sample data if rows exist
    if count > 0:
        cursor.execute("SELECT * FROM processed_data LIMIT 5;")
        rows = cursor.fetchall()
        print("\nSample data:")
        for row in rows:
            print(row)
    else:
        print("\nNo data in table.")
else:
    print("\nprocessed_data table does not exist.")

conn.close()