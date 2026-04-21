# Tiny dialect modules (`dialects/*.py`)
# Each contains only the database-specific bits:
# - upsert strategy  
# - any type overrides (rare)  
# - SQL Server MERGE helper (if you choose to use it)  
# Dialect differences are tiny
# Only:
# - connection strings  
# - upsert behaviour  
# - maybe a type or two  
# **sql_writer/dialects/mssql.py**
# - optional MERGE helper  
# - identity column quirks (if needed)  
# **sql_writer/dialects/sqlite.py**
# - INSERT OR REPLACE helper  
# **sql_writer/dialects/postgres.py**
# - ON CONFLICT DO UPDATE helper  
