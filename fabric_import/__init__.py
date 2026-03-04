"""
Fabric Import Package — Generates Microsoft Fabric artifacts from Tableau extractions.

Generates:
- Lakehouse definitions (table schemas)
- Dataflow Gen2 definitions (Power Query M mashup documents)
- PySpark Notebooks (.ipynb) for ETL pipelines
- Semantic Model (standalone DirectLake TMDL model)
- Data Pipeline (orchestration: Dataflow → Notebook → Model refresh)
- Power BI Reports (.pbip with TMDL semantic models)
"""

from .import_to_fabric import FabricImporter

__all__ = ['FabricImporter']
