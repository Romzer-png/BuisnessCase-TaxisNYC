import duckdb
import os

# Chemins
DB_PATH = "data_processed/nyc_taxis.duckdb"
DATA_PATH = "data_raw"

# Création dossier data_processed si absent
os.makedirs("data_processed", exist_ok=True)

# Connexion base
con = duckdb.connect(DB_PATH)

print("Création des tables mensuelles...")

# Janvier
con.execute(f"""
CREATE OR REPLACE TABLE trips_jan AS
SELECT *
FROM read_csv_auto('{DATA_PATH}/taxi_trip_2016-01.csv')
""")

# Février
con.execute(f"""
CREATE OR REPLACE TABLE trips_feb AS
SELECT *
FROM read_csv_auto('{DATA_PATH}/taxi_trip_2016-02.csv')
""")

# Mars
con.execute(f"""
CREATE OR REPLACE TABLE trips_mar AS
SELECT *
FROM read_csv_auto('{DATA_PATH}/taxi_trip_2016-03.csv')
""")

print("Création table fusionnée...")

con.execute("""
CREATE OR REPLACE TABLE trips_all AS
SELECT * FROM trips_jan
UNION ALL
SELECT * FROM trips_feb
UNION ALL
SELECT * FROM trips_mar
""")

print("Base construite avec succès.")

con.close()
