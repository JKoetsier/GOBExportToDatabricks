from pathlib import Path
from gobexport.exporter import CONFIG_MAPPING
from gobcore.logging.logger import logger
logger.name = "exporttodatabricks"

from gobcore.model import GOBModel
gob_model = GOBModel(legacy=True, reinit=True)
from gobapi.graphql_streaming.graphql2sql.graphql2sql import GraphQL2SQL, GraphQLVisitor
from exporttodatabricks.graphql2sql import DatabricksSqlGenerator

SQL_DIR = "sql/"
SQL_PATH = Path(__file__).parent / SQL_DIR
SQL_PATH.mkdir(parents=True, exist_ok=True)

NOTEBOOKS_DIR = "notebooks/"
NOTEBOOKS_PATH = Path(__file__).parent / NOTEBOOKS_DIR
NOTEBOOKS_PATH.mkdir(parents=True, exist_ok=True)

def _get_sql_for_graphql(graphql_query: str, mapping: dict, is_shape=False):
    g2s = GraphQL2SQL(graphql_query)
    visitor = GraphQLVisitor()
    sql = g2s.sql(generator=DatabricksSqlGenerator(visitor, mapping, is_shape), visitor=visitor)
    return sql

def _transform_format_string_to_dict(format: str) -> dict:
    mapping = {}
    for colidx, columndef in enumerate(format.split("|")):
        colsource = columndef.split(":")[0]
        mapping[f"col_{colidx}"] = colsource
    return mapping

def generate_databricks_queries():

    for catalog, catalog_config in CONFIG_MAPPING.items():
        if catalog == "test_catalogue" or catalog == "brk":  # BRK is old and won't be migrated
            continue

        notebook_queries = []
        for collection, config in catalog_config.items():

            for product, product_config in config.products.items():

                if product_config.get('api_type') in ('graphql', 'graphql_streaming'):
                    format = product_config.get('format')
                    name = f"{catalog}_{collection}_{product}"

                    is_shape = product_config.get('exporter') and product_config.get('exporter').__name__ == "shape_exporter"
                    print(name)

                    if isinstance(format, dict):
                        sql = _get_sql_for_graphql(product_config.get('query'), format, is_shape=is_shape)
                    elif isinstance(format, str):
                        dict_format = _transform_format_string_to_dict(format)
                        sql = _get_sql_for_graphql(product_config.get('query'), dict_format, is_shape=is_shape)
                    else:
                        raise Exception(f"Unknown format type {type(format)}")

                    with open(SQL_PATH / f"{name}.sql", 'w') as f:
                        f.write(sql)

                    notebook_queries.append((name, sql))

                else:
                    print(f"Skipping {catalog} {collection} {product} for now, not a graphql product")

        if notebook_queries:
            with open(NOTEBOOKS_PATH / f"{catalog}.sql", 'w') as f:
                f.write(f"-- Databricks notebook source\n")
                for name, query in notebook_queries:
                    f.write(f"-- DBTITLE 1, {name}\n")
                    f.write(query)
                    f.write("\n\n-- COMMAND ----------\n\n")

if __name__ == "__main__":
    generate_databricks_queries()
