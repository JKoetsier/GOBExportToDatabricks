# ExportToDatabricks

Utility repo that helps transforming the GOB export jobs as defined in GOB-Export to Databricks jobs. Generates
Spark SQL queries from the GOB-Export GraphQL queries. Uses the Graphql2SQL class from GOB-API as base.

This repo has some limitations, as it's not feasible to implement all exceptions that are defined in GOB-Export. The
output of this repo should act as a starting point for the Databricks jobs and should implement 95% of the logic.

### Known limitations:
The limitations are mostly BRK related. The BRK export is the most complex export and has the most exceptions. However,
there are some additional considerations.
- Does not implement the 'filter' or 'entity_filters' arguments.
- Does not implement appending products (as in BRK2 Aantekeningen exports). A manual UNION is needed for this.
- Mappings with brackets (e.g. heeftBetrekkingOpBrkKadastraalObject.[0].perceelnummer) should be implemented manually. 
  This is mainly impacted by the next point (unfolding). If the next point is fixed, this can be fixed too.
- In most cases, when multiple rows are returned for an object (i.e. when joins result in more rows), export takes
  the first row and drops the rest, except for when the 'unfold': True arguments is given. This is not (yet) 
  implemented). It may be useful to implement this later on, as most of the time the result set is not unfolded.
- The row_formatter is not implemented, see KadastraleObjecten export.
- In many cases, all possible edge cases are not implemented. Only the cases currently encountered in the GOB exports
  are implemented. In no way is this repo stable enough to be used blindly.


## Usage:

    dc up

The generated SQL files are stored in the ./src/exporttodatabricks/sql directory.