#/usr/bin/python

def parse_line_into_schema(row, schema):
    record = dict()
    fields = schema['fields']
    if len(row) != len(fields):
        print('ERROR: # OF FIELDS IN PARSED LINE DOES NOT MATCH # OF FIELDS IN SCHEMA')
        print('row: {0}\nschema: {1}'.format(row, schema))
        assert False
    for field, value in zip(fields, row):
        name = field['name']
        ftype = field['type']
        if ftype == 'int':
            value_out = int(value)
        elif ftype == 'float':
            value_out = float(value)
        else:
            value_out = str(value)
        record[name] = value_out
    return record

