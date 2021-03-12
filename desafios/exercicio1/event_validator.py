import json
import boto3

_SQS_CLIENT = None
JSON_SCHEMA = json.load(open('schema.json',))

def send_event_to_queue(event, queue_name):
    '''
     Responsável pelo envio do evento para uma fila
    :param event: Evento  (dict)
    :param queue_name: Nome da fila (str)
    :return: None
    '''
    
    sqs_client = boto3.client("sqs", region_name="us-east-1")
    response = sqs_client.get_queue_url(
        QueueName=queue_name
    )
    queue_url = response['QueueUrl']
    response = sqs_client.send_message(
        QueueUrl=queue_url,
        MessageBody=json.dumps(event)
    )
    print(f"Response status code: [{response['ResponseMetadata']['HTTPStatusCode']}]")

def handler(event):
    schema_valid = validate_fields_type(event)
    required_valid = validate_required_fields(event)
    print(schema_valid, required_valid)

    if schema_valid == True and required_valid == True:
        print('sending event')
        #send_event_to_queue(event, 'teste')
    else:
        print('houston we have a problem')


def validate_fields_type(event, event_properties=None):
    for key, value in event.items():
        try:
            field_schema = JSON_SCHEMA['properties'].get(key) if event_properties is None else event_properties.get(key)
            if field_schema['type'] == 'object':
                [validate_fields_type(event.get(key), field_schema['properties']) for k, v in value.items()]
            else:
                if isinstance(value, field_schema.get('examples')[0].__class__):
                    print(f"Field {key} {value.__class__} is correct")
                    schema_valid = True
                else:
                    print(f"Field {key} is {value.__class__} expecting type: {field_schema.get('examples')[0].__class__}")
                    schema_valid = False
        except Exception as e:
            print(e)
            schema_valid = False
    return schema_valid
    
def validate_required_fields(event, event_schema=None):
    for key, _ in event.items():
        field_type = JSON_SCHEMA['properties'].get(key)['type'] if event_schema is None else event_schema['properties'].get(key)['type']
        if field_type == 'object':
            validate_required_fields(event[key], JSON_SCHEMA['properties'].get(key))
        else:
            required_fields = JSON_SCHEMA['required'] if event_schema is None else event_schema['required']
            validate = set(required_fields) - set(list(event.keys()))
            if not validate:
                print('Required columns are OK!')
                required_valid = True
            else:
                print('There are missing columns', validate)
                required_valid = False
    return required_valid