# config/settings.py

JDBC_URL = "jdbc:sqlserver://<host>.database.windows.net:1433;database=<db>;user=<user>;password=<pass>"
JDBC_DRIVER = "com.microsoft.sqlserver.jdbc.SQLServerDriver"

RAW_BASE_PATH = "abfss://raw@<storage>.dfs.core.windows.net"
SILVER_BASE_PATH = "abfss://silver@<storage>.dfs.core.windows.net"
