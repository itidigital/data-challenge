import json
import pathlib
import os

_ATHENA_CLIENT = None
    
def create_hive_table_with_athena(query):
    '''
    Função necessária para criação da tabela HIVE na AWS
    :param query: Script SQL de Create Table (str)
    :return: None
    '''
    print(f"Query: {query}")
    _ATHENA_CLIENT.start_query_execution(
        QueryString=query,
        ResultConfiguration={'OutputLocation': 's3://iti-query-results/'},
    )

def get_schema(filename) -> dict:
    with open(filename, 'r') as f:
        data = json.load(f)
        res = {}
        for k,v in data['properties'].items():
            if v["type"] != 'object':
                values = {k:v["type"]}
                res |= values
    return res


def get_schema_fields() -> list:
    schema = get_schema(os.path.join(pathlib.Path().absolute(),"./schema.json"))
    res = []
    for k,v in schema.items():
        if isinstance(v, dict):
            for m,n in v.items():
                x = schema.get(k).get(m)
                res.append(f"{m} {x}")
        else:
            x = schema.get(k)
            res.append(f"{k} {v}")
    return res

def handler():
    data = get_schema_fields()
    query = f"CREATE EXTERNAL TABLE IF NOT EXISTS client ({', '.join(data)})"
    create_hive_table_with_athena(query)