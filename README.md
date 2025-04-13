# Metadata-Driven Ingestion Engine for Databricks

This project provides a modular, professional-grade data ingestion engine based on metadata, designed for use in Azure Databricks with Git integration.

---

## 🔧 Project Structure

```bash
metadata-ingestion-engine/
│
├── notebooks/                  # Main entry notebooks
│   ├── main_ingestion_dispatcher.py
│   ├── main_ingestion_asset.py
│   └── raw_to_datahub.py
│
├── connectors/                # Modular connector functions
│   ├── jdbc.py, delta.py, file.py, api.py, olap.py
│   └── __init__.py
│
├── metadata/                  # Metadata readers from Azure SQL
│   └── reader.py
│
├── utils/                     # Helpers: SCD2, logging, validation
│   └── scd.py
│
├── config/                    # Central settings (JDBC, paths)
│   └── settings.py
│
└── README.md
```

---

## 🚀 Usage in Databricks

1. **Clone the repo into Databricks Repos**:
   > Repos > Add Repo > Paste Git URL

2. **Edit `sys.path.append()` in notebooks** to match your Databricks repo path:
   ```python
   sys.path.append("/Workspace/Repos/<your_user>/metadata-ingestion-engine")
   ```

3. **Run Dispatcher**:
   ```python
   dbutils.notebook.run("../notebooks/main_ingestion_dispatcher", 3600, {
       "sourceid": "S_OLAP",
       "max_threads": "4",
       "use_mock": "true"
   })
   ```

4. **Convert raw to DataHub**:
   ```python
   dbutils.notebook.run("../notebooks/raw_to_datahub", 3600, {
       "sourceid": "S_OLAP",
       "assetid": "A001",
       "assetname": "ventas_olap"
   })
   ```

---

## 📦 Dependencies

Install in cluster or in notebook (for OLAP XMLA support):
```python
%pip install olap.xmla
```

---

## 🧪 Test OLAP Mock
Run `test_mock_olap.py` in notebooks to validate end-to-end flow without real cube.

---

## 🔐 Security Note
Use Azure Key Vault or secret scopes in production instead of storing credentials in metadata directly.
