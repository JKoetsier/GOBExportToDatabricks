from typing import Union

from gobapi.graphql_streaming.graphql2sql.graphql2sql import SqlGenerator, to_snake, FIELD
from gobapi.legacy_views.legacy_views import get_custom_view_definition
from gobcore.model.relations import get_relations
from gobcore.model import GOBModel


class DatabricksSqlGenerator(SqlGenerator):
    SCHEMA = "dpbk_dev.02_silver"

    def __init__(self, visitor: 'GraphQLVisitor', format: dict, is_shape=False):
        super().__init__(visitor)

        self.format = format
        self.is_shape = is_shape
        self.relation_table_renames = {}  # map of legacy name -> databricks name

        for relation_name in get_relations(GOBModel(True))['collections'].keys():
            table_name = f"rel_{relation_name}"
            view_definition = get_custom_view_definition(table_name)

            if view_definition:
                self.relation_table_renames[table_name] = view_definition.table_name

    def _relation_table_name(self, relation_name: str):
        tablename = f"rel_{relation_name}"
        tablename = self.relation_table_renames.get(tablename, tablename)
        return self._full_table_name(tablename)

    def _geometry_as_text(self, geometry: str):
        return geometry

    def _add_relation_join_attributes_to_select_expressions(self, attributes: list, dst_catalog_name: str,
                                                            dst_collection_name: str, join_alias: str, relation_attr_name: str):

        for attribute in attributes:
            self._add_select_expression(f"{to_snake(join_alias)}.{to_snake(attribute)}", f"{to_snake(relation_attr_name)}_{to_snake(attribute)}")

        self._add_select_expression(f"'{dst_catalog_name}'", f"{to_snake(relation_attr_name)}_catalog")
        self._add_select_expression(f"'{dst_collection_name}'", f"{to_snake(relation_attr_name)}_collection")

    def _current_filter_expression(self, table_id: str = None):
        table = f"{table_id}." if table_id else ""

        return f"(COALESCE({table}{FIELD.EXPIRATION_DATE}, '9999-12-31'::timestamp_ntz) > NOW())"

    def _evaluate_action(self, action: dict) -> str:
        if action.get('action') == 'format':
            if action.get('formatter').__name__ == 'format_geometry':
                return self._get_mapped_expression(action['value'])
            if action.get('formatter').__name__ == 'format_soort_object':
                # Is solely used with woz deelobjecten. Value is always of the form invIsVerbondenMetBagLigplaatsWozDeelobjecten.soortObject.omschrijving
                relalias = self._get_relation_info(action['value'].split('.')[0])['alias']
                column = to_snake(action['value'].split('.')[1])
                return self._build_get_json_object(f"{relalias}.{column}", to_snake(action['value'].split('.')[2]))
            if action.get('formatter').__name__ == 'format_date':
                return f"date_format({self._get_mapped_expression(action['value'])}, \"yyyy-MM-dd'T'HH:mm:ss\")"
            if action.get('formatter').__name__ == 'format_timestamp':
                format = "yyyyMMddHHmmss"  # default

                map_formats = {
                    "%Y-%m-%d": "yyyy-MM-dd",
                }
                if action.get('kwargs'):
                    format = map_formats.get(action['kwargs'].get('format'))
                    if not format:
                        raise Exception(f"Unknown format {action['kwargs'].get('format')}")
                return f"date_format({self._get_mapped_expression(action['value'])}, \"{format}\")"
            if action.get('formatter').__name__ == 'format_kadgrootte':
                return f"ROUND({self._get_mapped_expression(action['value'])})"
            if action.get('formatter').__name__ == 'format_koopsom':
                return f"ROUND({self._get_mapped_expression(action['value'])})"
            if action.get('formatter').__name__ == 'format_bedrag':
                return f"ROUND({self._get_mapped_expression(action['value'])})"
            if action.get('formatter').__name__ == 'comma_concatter':
                return f"replace({self._get_mapped_expression(action['value'])}, '|', ',')"
            if action.get('formatter').__name__ == 'format_rotation':
                # Format float with 3 decimals
                return f"round({self._get_mapped_expression(action['value'])}, 3)"
            if action.get('formatter').__name__ == 'format_guid':
                return f"format_string('{{{{%s}}}}', {self._get_mapped_expression(action['value'])})"
        elif action.get('action') == 'literal':
            return f"\"{action['value']}\""
        elif action.get('action') == 'concat':
            return f"CONCAT({', '.join([self._evaluate_format_expression(val) for val in action['fields']])})"
        elif action.get('action') == 'fill':
            if action['fill_type'] == 'ljust':
                # Justify left is pad right
                return f"RPAD({self._get_mapped_expression(action['value'])}, {action['length']}, '{action['character']}')"
            else:
                # rjust
                return f"LPAD({self._get_mapped_expression(action['value'])}, {action['length']}, '{action['character']}')"
        elif action.get('action') == 'case':
            cases = " ".join([f"WHEN '{k}' THEN '{v}'" for k, v in action['values'].items()])
            return f"CASE {self._get_mapped_expression(action['reference'])} {cases} END"

        raise Exception(f"Not implemented action {action}")

    def _evaluate_condition(self, condition: dict):
        def evaluate_tfval(val):
            if isinstance(val, str):
                return self._get_mapped_expression(val)
            else:
                return self._evaluate_dict_query_attr(val)

        reference = self._get_mapped_expression(condition['reference']) if isinstance(condition['reference'], str) else self._evaluate_dict_query_attr(condition['reference'])
        negate = condition.get('negate', False)
        trueval = evaluate_tfval(condition['trueval']) if condition.get('trueval') else 'NULL'
        falseval = evaluate_tfval(condition['falseval']) if condition.get('falseval') else 'NULL'

        if condition.get('condition') == 'isempty' or condition.get('condition') == 'isnone':
            # Technically isempty and isnone are different: isnone only checks for null, whereas isempty also returns
            # true for empty strings and 0 values. The implementation for this would be too generic here. It doesn't
            # occur that often in the exports, so it's easier to fix that manually later on.
            if negate:
                return f"CASE WHEN {reference} IS NULL THEN {falseval} ELSE {trueval} END"
            else:
                return f"CASE WHEN {reference} IS NULL THEN {trueval} ELSE {falseval} END"
        else:
            raise NotImplementedError(f"Not implemented condition {condition['condition']}")

    def _evaluate_dict_query_attr(self, query_attr: dict) -> str:
        """Returns the query attribute or expression as a string

        :param query_attr:
        :return:
        """
        if query_attr.get('condition'):
            return self._evaluate_condition(query_attr)
        elif query_attr.get('action'):
            return self._evaluate_action(query_attr)
        else:
            raise NotImplementedError(f"Not implemented query_attr {query_attr}")

    def _get_mapped_expression(self, alias_or_identifier: str):
        """Returns the mapped expression for the given alias or identifier.

        :param alias_or_identifier:
        :return:
        """
        if '[' in alias_or_identifier and ']' in alias_or_identifier:
            # In filters/conditions we're sometimes using some Python syntax
            return f"'TODO: DO THIS MANUALLY ({alias_or_identifier})'"

        search_for = to_snake(alias_or_identifier.replace('.', '_'))

        for alias, select in self.aliased_select_expressions.items():
            if alias == search_for:
                return select

        for select in self.unaliased_select_expressions:
            if select.split('.')[-1] == search_for:
                return select

    def _build_get_json_object(self, column_expression: str, json_path: str):
        return f"get_json_object({column_expression}, '$.{json_path}')"

    def _evaluate_format_expression(self, format_expression: Union[str, dict]):
        """Evaluates the format expression, which can be a string reference or a dict with an action"""
        if isinstance(format_expression, dict):
            return self._evaluate_dict_query_attr(format_expression)
        else:
            return self._get_mapped_expression(format_expression)

    def _select_expressions_as_string(self):
        # Apply mapping to select expressions
        new_select_expressions = {}

        for export_attr, query_attr in self.format.items():
            expression = self._evaluate_format_expression(query_attr)

            if not expression:
                if "." in query_attr:
                    # Possibly JSON field from original GraphQL
                    expression = self._evaluate_format_expression(query_attr.split(".")[0])

                    if expression:
                        # Create get_json_object expression
                        expression = self._build_get_json_object(expression, query_attr.split('.')[1])

            new_select_expressions[export_attr] = expression if expression else "NULL"

        if self.is_shape and 'geometrie' not in new_select_expressions.keys():
            geometrie = self._evaluate_format_expression('geometrie')

            if geometrie:
                new_select_expressions['geometrie'] = geometrie


        # Check
        missing = False
        for export_attr in self.format.keys():
            if export_attr not in new_select_expressions.keys():
                print(f"Missing output attribute {export_attr}")
                missing = True
        if missing:
            raise Exception(f"Missing output attribute")

        return ",\n".join([f"{v} AS `{k}`" for k, v in new_select_expressions.items()])
