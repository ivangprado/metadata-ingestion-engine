-- sql/setup/insert_demo_metadata.sql

-- Insertar fuentes mock
INSERT INTO metadata.source (sourceid, sourcename, connectorstring, connectortype, username, password)
VALUES
('S_SQL', 'SQL Server Demo', 'jdbc:sqlserver://sqlserver-demo.database.windows.net:1433;database=demo', 'sqlserver', 'sql_user', 'sql_pass'),
('S_OLAP', 'OLAP Demo Cube', 'https://fake-xmla-endpoint.com/xmla', 'olap_cube', 'olap_user', 'olap_pass');

-- Insertar assets mock
INSERT INTO metadata.asset (assetid, assetname, sourceid, query)
VALUES
('A_CLIENTES', 'clientes', 'S_SQL', 'SELECT * FROM dbo.clientes'),
('A_VENTAS_OLAP', 'ventas_olap', 'S_OLAP', 'SELECT {[Measures].[Total Ventas]} ON COLUMNS, [Fecha].[Año].[2023] ON ROWS FROM [Ventas]');

-- Insertar columnas para A_CLIENTES
INSERT INTO metadata.assetcolumns (columnid, columnname, assetid, columntype, ispk) VALUES
('COL1', 'IdCliente', 'A_CLIENTES', 'int', 1),
('COL2', 'Nombre', 'A_CLIENTES', 'string', 0),
('COL3', 'Email', 'A_CLIENTES', 'string', 0);

-- Insertar columnas para A_VENTAS_OLAP
INSERT INTO metadata.assetcolumns (columnid, columnname, assetid, columntype, ispk) VALUES
('COL4', 'Año', 'A_VENTAS_OLAP', 'string', 1),
('COL5', 'Total Ventas', 'A_VENTAS_OLAP', 'double', 0);