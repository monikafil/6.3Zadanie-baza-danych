from sqlalchemy import Table, Column, Integer, String, Float, Date, MetaData
from sqlalchemy import create_engine
import csv
from datetime import datetime

# Połączenie
engine = create_engine("sqlite:///weather.db")
meta = MetaData()

stations = Table(
    "stations", meta,
    Column("station", String, primary_key=True),
    Column("latitude", Float),
    Column("longitude", Float),
    Column("elevation", Float),
    Column("name", String),
    Column("country", String),
    Column("state", String),
)

measurements = Table(
    "measurements", meta,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("station", String),
    Column("date", Date),
    Column("precip", Float),
    Column("tobs", Float),
)

# Tworzenie tabel
meta.create_all(engine)

print("Tabele w bazie:", engine.table_names())

# Połączenie
conn = engine.connect()

# -----------------------------
# Wczytywanie clean_stations.csv
# -----------------------------
with open("clean_stations.csv") as f:
    reader = csv.DictReader(f)
    for row in reader:
        conn.execute(
            stations.insert().values(
                station=row["station"],
                latitude=float(row["latitude"]),
                longitude=float(row["longitude"]),
                elevation=float(row["elevation"]),
                name=row["name"],
                country=row["country"],
                state=row["state"]
            )
        )

conn.commit()
# -----------------------------
# Wczytywanie clean_measure.csv
# -----------------------------
with open("clean_measure.csv") as f:
    reader = csv.DictReader(f)
    for row in reader:
        date = datetime.strptime(row["date"], "%Y-%m-%d").date()
        conn.execute(
            measurements.insert().values(
                station=row["station"],
                date=date,
                precip=float(row["precip"]),
                tobs=float(row["tobs"])
            )
        )

conn.commit()
# Test
print(conn.execute("SELECT * FROM stations LIMIT 5").fetchall())









