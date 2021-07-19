from api.DSLJsonToSQL import DSLJsonToQuery
import json
import unittest

engineDSLToQuery = DSLJsonToQuery("", "")


def test_Query(jsonInputStr, step):
    jsonInput = json.loads(jsonInputStr)
    if (step == 1):
        res = engineDSLToQuery.simple_select(jsonInput)
    elif (step == 2):
        res = engineDSLToQuery.select_with_filter(jsonInput)
    elif (step == 3):
        res = engineDSLToQuery.select_with_filter_compounding(jsonInput)

    print("input: " + jsonInputStr)

    print("is_valid: " + str(res.valid()))

    print(res.get_message())

    return res


class TestStringMethods(unittest.TestCase):

    def test_step_1(self):
        res = test_Query('{"fields": ["name", "population"]}', 1)
        self.assertEqual(res.get_message(), 'SELECT name, population FROM towns')

    def test_step_1_2(self):
        res = test_Query('{"fields": ["nom", "population"]}', 1)
        self.assertEqual(res.valid(), False)

    def test_step_2(self):
        res = test_Query('{"fields": ["name"],"filters": {"field": "distr_code", "value": 1}}', 2)
        self.assertEqual(res.get_message(), 'SELECT name FROM towns WHERE distr_code = 1')

    def test_step_2_2(self):
        res = test_Query('{"fields": ["name"],"filters": {"field": "population", "value": 1000, "predicate":"gt"}}', 2)
        self.assertEqual(res.get_message(), 'SELECT name FROM towns WHERE population > 1000')

    def test_step_3(self):
        res = test_Query('{"fields": ["name"],"filters": {"and": [{"field": "population","value": 10000,"predicate": "gt"},{"field": "region_name","value": "Hauts-de","predicate": "contains"}]}}', 3)
        self.assertEqual(res.get_message(), "SELECT name FROM towns WHERE population > 10000 AND region_name LIKE '%Hauts-de%'")

    def test_step_3_1(self):
        res = test_Query('{"fields": ["name"],"filters": {"and": [{"field": "population","value": 10000,"predicate": "gt"},{"field": "regional_name","value": "Hauts-de","predicate": "contains"}]}}', 3)
        self.assertEqual(res.valid(), False)

    def test_step_4(self):
        res = test_Query('{"fields": ["name"],"filters": {"and": [{"field": "population","value": 1000,"predicate": "gt"},{"or": [{"field": "name","predicate": "contains","value": "seine"},{"field": "name","predicate": "contains","value": "loire"}]}]}}', 3)
        self.assertEqual(res.get_message(), "SELECT name FROM towns WHERE population > 1000 AND (name LIKE '%seine%' OR name LIKE '%loire%')")

    def test_step_4_2(self):
        res = test_Query('{"fields": ["name"],"filters": {"and": [{"field": "population","value": 1000,"predicate": "gt"},{"or": [{"field": "name","predicate": "contains","value": "seine"}, {"and": [{"field": "name","predicate": "contains","value": "loire"}, {"field": "region_name","predicate": "contains","value": "loire"}]}]}]}}', 3)
        self.assertEqual(res.get_message(), "SELECT name FROM towns WHERE population > 1000 AND (name LIKE '%seine%' OR (name LIKE '%loire%' AND region_name LIKE '%loire%'))")
if __name__ == '__main__':
    unittest.main()

