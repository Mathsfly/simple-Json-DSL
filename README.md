
[![Build Status](https://img.shields.io/endpoint.svg?url=https%3A%2F%2Factions-badge.atrox.dev%2FDavid-Wobrock%2Fsqlvalidator%2Fbadge%3Fref%3Dmain&style=popout)](https://github.com/Mathsfly/test_botify/tree/master/api)

# Json DSL for SQLQuery

## Command line usage

Simple DSL for a json format to SQL Code

### Input
```  

{
  “fields”: [“name”],
  “filters”: {“and”: [
      {
      “field”: “population”,
      “value”: 1000,
      “predicate”: “gt”
      },
      {
      “or”: [
        {
          “field”: “name”,
          “predicate”: “contains”,
          “value”: “seine”
        },
        {
          “field”: “name”,
          “predicate”: “contains”,
          “value”: “loire”
        }
        ]
      }
    ]
  }
}

```
### Output
```  
SELECT name
FROM towns
WHERE population > 1000 AND (name LIKE
‘%seine%’ OR name LIKE ‘%loire%’)
```

## Run unitest
py test_dsl.py

## Dependencies: 
Python 3

