import json
from cgitb import reset


class QueryHandler:

    def __init__(self, valid, message, filter_compouding=False):
        """ Instance to handle the query"""
        """ Depend on table input we could get infos inside"""
        self.__valid = valid
        self.__message = message
        self.__filter_compouding = filter_compouding

    def valid(self):
        return self.__valid

    def get_message(self):
        return self.__message

    def is_filter_compounding(self):
        return self.__filter_compouding


class SelectDSL:

    def __init__(self, sql, table, config_sql=None):
        """ Declare some thing for connect and execute sql"""
        """ Depend on table input we could get infos inside"""
        """ table:
        {
            name: "Town",
            elements:
            [
                { "name": "code", "type": "int" },
                { "name": "name", "type": "str" },
                { "name": "population", "type": "int" },
                { "name": "average_age", "type": "float" },
                { "name": "distr_code", "type": "int" },
                { "name": "dept_code", "type": "int" },
                { "name": "region_code", "type": "int" },
                { "name": "region_name", "type": "str" }
            ]
        }
        
        sql : type of sql,
        
        config: some special config for sdl
        
        """
        self.__table = json.loads(table)
        self.__sql = sql
        self.__config_sql = config_sql

    def connect(self):
        return

    def execute_sql(self, sql_query):
        """To be implemented"""
        return

    def get_sql_table_name(self):
        """Get table name from self.table"""
        if "name" in self.__table:
            return self.__table["name"];
        
        return None

    def get_fields_from_table(self):
        """Get fields name form self.table"""
        """i.e: ["code", "name", "population", "average_age", "distr_code", "dept_code", "region_code", "region_name"]"""
        res = []
        if not "elements" in self.__table:
            return res
        
        for element in self.__table["elements"]:
            if "name" in element:
                res.append(element["name"])
        
        return res

    def is_valid_expression(self, field, value):
        """check if expression valid """

        if field in ["code", "population", "dept_code", "distr_code", "region_code"] and isinstance(value, int):
            return True

        if field in ["name", "region_name"] and isinstance(value, str):
            return True
        
        if field in ["average_age"] and isinstance(value, float):
            return True
        
        return False

    def execute (self, dsl_query):
        dsl_loaded_query = json.loads(dsl_query)
        res = self.message_step_four(dsl_loaded_query)
        if (res.valid()):
            self.execute_sql(res.get_message())

    def get_condition(self, dict_condition):

        if not type(dict_condition["value"]) in [int, float, str]:
            return QueryHandler(False, "The value format is not correct")

        condition_expresion = ""
        value = dict_condition["value"]
        if not "predicate" in dict_condition:
            if (len(dict_condition) > 2):
                return QueryHandler(False, "The filter format is not correct")
            condition_expresion = "="
        elif dict_condition["predicate"] == "gt":
            condition_expresion = ">"
        elif dict_condition["predicate"] == "lt":
            condition_expresion = "<"
        elif dict_condition["predicate"] == "contains":
            condition_expresion = "LIKE"
            value = "'%" + value + "%'"
        else:
            return (False, "The predicate format is not correct")

        if (len(dict_condition) > 3):
            return QueryHandler(False, "The filter format is not correct")

        if (not dict_condition["field"] in self.get_fields_from_table()):
            return QueryHandler(False, "The field in condition is not correct")

        if (not self.is_valid_expression(dict_condition["field"], value)):
            return QueryHandler(False, "The expression condition is not correct")

        return QueryHandler(True, dict_condition["field"] + " " + condition_expresion + " " + str(value))

    def get_condition_expression(self, condition, is_sub_op=False):
        """ This method contain both step 3 and 4"""

        list_condition = []
        operator = ""

        if "and" in condition:
            if not isinstance(condition["and"], list) or len(condition["and"]) < 2:
                return QueryHandler(False, "The and format is not correct")

            list_condition = condition["and"]
            operator = " AND "
        elif "or" in condition:
            if not isinstance(condition["or"], list) or len(condition["or"]) < 2:
                return QueryHandler(False, "The and format is not correct")

            list_condition = condition["or"]
            operator = " OR "
        else:
            return QueryHandler(False, "The filter format is not correct")

        exp_list = [];
        for condition in list_condition:
            if ("and" in condition or "or" in condition):
                exp_condition = self.get_condition_expression(condition, True)
            else:
                exp_condition = self.get_condition(condition)

            if not exp_condition.valid():
                return exp_condition

            exp_list.append(exp_condition.get_message())

        left_parenthese = ""
        right_parenthese = ""
        if is_sub_op:
            left_parenthese = "("
            right_parenthese = ")"

        return QueryHandler(True, left_parenthese + operator.join(map(str, exp_list)) + right_parenthese)

    def simple_select(self, dsl_loaded_query):
        """False is not valid query, True is good query """

        if not self.get_fields_from_table() or not self.get_sql_table_name() or not dsl_loaded_query or not isinstance(dsl_loaded_query, dict):
            return QueryHandler(False, "The format is not correct")

        if not "fields" in dsl_loaded_query:
            return QueryHandler(False, "The format is not correct, not found 'Field' keyword")

        if not isinstance(dsl_loaded_query["fields"], list) or len(dsl_loaded_query["fields"]) == 0:
            return QueryHandler(False, "The format is not correct, invalid fields")

        fields_in_table = self.get_fields_from_table()

        for field_element in dsl_loaded_query["fields"]:
            if not field_element in fields_in_table:
                return QueryHandler(False, "The element " + field_element + " in field is not correct")

        return QueryHandler(True, "SELECT " + ", ".join(map(str, dsl_loaded_query["fields"])) + " FROM " + self.get_sql_table_name())

    def select_with_filter(self, dsl_loaded_query):
        """False is not valid query, True is good query """

        res = self.simple_select(dsl_loaded_query)

        if not res.valid():
            return res

        if not "filters" in dsl_loaded_query or not (isinstance(dsl_loaded_query["filters"], dict)) or len(dsl_loaded_query["filters"]) == 0:
            return res

        if not "field" in dsl_loaded_query["filters"] or not "value" in dsl_loaded_query["filters"]:
            if "and" in dsl_loaded_query["filters"] or "or" in dsl_loaded_query["filters"]:
                return QueryHandler(True, res.get_message(), True)
            else:
                return QueryHandler(False, "The filter format is not correct")

        condition = self.get_condition(dsl_loaded_query["filters"])

        if not condition.valid():
            return condition

        return QueryHandler(True, res.get_message() + " WHERE " + condition.get_message())

    def select_with_filter_compounding(self, dsl_loaded_query):
        """False is not valid query, True is good query """
        res = self.select_with_filter(dsl_loaded_query)

        if not res.valid() or not res.is_filter_compounding():
            return res

        exp_condition = self.get_condition_expression(dsl_loaded_query["filters"])

        if not exp_condition.valid():
            return exp_condition

        return QueryHandler(True, res.get_message() + " WHERE " + exp_condition.get_message(), True)

