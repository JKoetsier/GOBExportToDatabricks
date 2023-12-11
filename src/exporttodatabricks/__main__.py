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

def generate_databricks_queries():

    for catalog, catalog_config in CONFIG_MAPPING.items():
        if catalog == "test_catalogue" or catalog == "brk":  # BRK is old and won't be migrated
            continue

        for collection, config in catalog_config.items():
            for product, product_config in config.products.items():

                if product_config.get('api_type') in ('graphql', 'graphql_streaming'):
                    format = product_config.get('format')

                    if isinstance(format, dict):
                        print(catalog, collection, product)

                        g2s = GraphQL2SQL(product_config.get('query'))
                        visitor = GraphQLVisitor()
                        sql = g2s.sql(generator=DatabricksSqlGenerator(visitor, format), visitor=visitor)

                        with open(SQL_PATH / f"{catalog}_{collection}_{product}.sql", 'w') as f:
                            f.write(sql)

                    elif isinstance(format, str):
                        # TODO implement
                        print(f"Skipping {catalog} {collection} {product} for now, not implemented yet")
                        continue
                else:
                    print(f"Skipping {catalog} {collection} {product} for now, not a graphql product")

if __name__ == "__main__":
    generate_databricks_queries()
