import json
import boto3
from pathlib import Path

_SQS_CLIENT = None

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
    try:
        def get_schema(filename) -> dict:
            with open(filename, 'r') as f:
                        data = json.load(f)
                        res = {}
                        for k,v in data['properties'].items():
                            if v["type"] != 'object':
                                values = {k:v["type"]}
                                res.update(values)
                            elif v["type"] == "object":
                                res.update({k:{}})
                                for x in v["properties"].keys():
                                    values = {x: v['properties'].get(x).get("type")}
                                    res[k].update(values)
            return res

        def validate_schema(schema_path, event) -> dict:
            schema = get_schema(Path(schema_path))
            for k,v in event.items():
                if k not in schema.keys():
                    raise ValueError("Not a valid Event")
                
                if isinstance(v, dict):
                    for m,n in v.items():
                        try: 
                            x = schema.get(k).get(m)
                            z = type(n)
                            w = str if x == 'string' else int if x == 'integer' else bool if x == 'boolean' else dict
                            if w == z:
                                continue
                            else:
                                raise ValueError(f"{n} must be {w}, not {z}")
                        except Exception as e:
                            raise ValueError("Not a valid Event") from e
                else:
                    try:
                        x = schema.get(k)
                        z = type(v)
                        w = str if x == 'string' else int if x == 'integer' else bool if x == 'boolean' else dict
                        if w == z:
                            continue
                        else:
                            raise ValueError(f"{n} must be {w}, not {z}")
                    except Exception as e:
                        raise ValueError("Not a valid Event") from e
            return event
        data = validate_schema("./schema.json",event)
        send_event_to_queue(data, "valid-events-queue")
    except Exception as e:
        raise ValueError("Invalid event") from e