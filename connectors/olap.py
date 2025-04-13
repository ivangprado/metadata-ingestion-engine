# connectors/olap.py

from pyspark.sql import SparkSession

def connect_olap_xmla(spark: SparkSession, xmla_url: str, mdx_query: str, username: str, password: str):
    from olap.xmla.xmla_client import XmlaClient
    client = XmlaClient()
    client.connect(location=xmla_url, username=username, password=password)
    rowset = client.Execute(mdx_query, Catalog="DemoCubo")
    data = [dict(row) for row in rowset.fetchall()]
    return spark.read.json(spark.sparkContext.parallelize(data))

def connect_olap_xmla_mock(spark: SparkSession, xmla_url: str, mdx_query: str, username: str, password: str):
    mock_data = [
        {"Año": "2020", "Total Ventas": 120000},
        {"Año": "2021", "Total Ventas": 145000},
        {"Año": "2022", "Total Ventas": 162500},
        {"Año": "2023", "Total Ventas": 178900}
    ]
    return spark.read.json(spark.sparkContext.parallelize(mock_data))
