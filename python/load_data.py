import os
import duckdb

duckdb_folder = '../duckdb'
data_folder = '../data'

con = duckdb.connect(duckdb_folder+'/kepler.duckdb')

for file in os.listdir(data_folder):
    if file.endswith(".csv"):
        table_name = os.path.splitext(file)[0]  # use filename as table name
        file_path = os.path.join(data_folder, file)
        query = f"""
            CREATE TABLE "{table_name}" AS 
            SELECT * FROM read_csv_auto('{file_path}')
        """
        con.execute(query)

# Show loaded tables
print(con.execute("SHOW TABLES").fetchall())
