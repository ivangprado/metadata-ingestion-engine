# config/settings.py

JDBC_URL = "jdbc:sqlserver://<host>.database.windows.net:1433;database=<db>;user=<user>;password=<pass>"
JDBC_DRIVER = "com.microsoft.sqlserver.jdbc.SQLServerDriver"

RAW_BASE_PATH = "abfss://raw@<storage>.dfs.core.windows.net"
DATAHUB_BASE_PATH = "abfss://datahub@<storage>.dfs.core.windows.net"


# JDBC_URL = "jdbc:sqlserver://igpserver.database.windows.net:1433;database=metadata;user=myuser;password=Mypwddemo0"
# JDBC_DRIVER = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
#
# RAW_BASE_PATH = "abfss://raw@myigpdatalake.dfs.core.windows.net"
# DATAHUB_BASE_PATH = "abfss://datahub@myigpdatalake.dfs.core.windows.net"
