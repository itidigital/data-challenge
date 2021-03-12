import json
from jinja2 import Template

_ATHENA_CLIENT = None
JSON_SCHEMA = json.load(open('schema.json',))


def create_hive_table_with_athena(query):
    '''
    Função necessária para criação da tabela HIVE na AWS
    :param query: Script SQL de Create Table (str)
    :return: None
    '''

    print(f"Query: {query}")
    _ATHENA_CLIENT.start_query_execution(
        QueryString=query,
        ResultConfiguration={
            'OutputLocation': f's3://iti-query-results/'
        }
    )


def handler():
    '''
    #  Função principal
    Aqui você deve começar a implementar o seu código
    Você pode criar funções/classes à vontade
    Utilize a função create_hive_table_with_athena para te auxiliar
        na criação da tabela HIVE, não é necessário alterá-la
    '''
    schema = {}
    for key, value in JSON_SCHEMA['properties'].items():
        try:
            field = JSON_SCHEMA['properties'].get(key)
            if field['type'] == 'object':
                _dict = {}
                for k, v in JSON_SCHEMA['properties'][key]['properties'].items():
                    _dict.update({k: JSON_SCHEMA['properties'][
                        key]['properties'][k]['type']})
                field = {key: _dict}
                schema.update(field)
            else:
                field = {key: JSON_SCHEMA['properties'].get(key)['type']}
                schema.update(field)
        except Exception as e:
            print(e)
            return False

    template_str = """
    CREATE EXTERNAL TABLE xpto(
        {%- for key, value in schema.items() %}
            {%- if value.__class__ != dict %}
                {{ key }} {{ value }},
            {%- else %}
                {{ key }} struct<
                {%- for k, v in value.items() %}
                    {{ k }} {{ v }} {{ "," if not loop.last }}
                {%- endfor %}
                >
            {%- endif %}
        {%- endfor %}
    )
    """

    template = Template(template_str)
    table_with_template = template.render(schema=schema)
    create_hive_table_with_athena(table_with_template)
