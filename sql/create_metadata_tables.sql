-- sql/setup/create_metadata_tables.sql

-- Crear tabla source
CREATE TABLE metadata.source (
    sourceid NVARCHAR(50) PRIMARY KEY,
    sourcename NVARCHAR(100) NOT NULL,
    connectorstring NVARCHAR(MAX) NOT NULL,
    connectortype NVARCHAR(50) NOT NULL,
    username NVARCHAR(100) NULL,
    password NVARCHAR(100) NULL
);

-- Crear tabla asset
CREATE TABLE metadata.asset (
    assetid NVARCHAR(50) PRIMARY KEY,
    assetname NVARCHAR(100) NOT NULL,
    sourceid NVARCHAR(50) NOT NULL,
    query NVARCHAR(MAX) NOT NULL,
    CONSTRAINT FK_asset_source FOREIGN KEY (sourceid)
        REFERENCES metadata.source(sourceid)
);

-- Crear tabla assetcolumns
CREATE TABLE metadata.assetcolumns (
    columnid NVARCHAR(50) PRIMARY KEY,
    columnname NVARCHAR(100) NOT NULL,
    assetid NVARCHAR(50) NOT NULL,
    columntype NVARCHAR(50) NOT NULL,
    ispk BIT NOT NULL DEFAULT 0,
    CONSTRAINT FK_assetcolumns_asset FOREIGN KEY (assetid)
        REFERENCES metadata.asset(assetid)
);
