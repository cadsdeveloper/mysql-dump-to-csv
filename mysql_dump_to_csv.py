import ast
import csv
import os
import sys
import re

SCHEMAS = {}
VALUES = {}


def is_create_statement(line):
    return line.startswith('CREATE TABLE')


def is_field_definition(line):
    return line.strip().startswith('`')


def is_insert_statement(line):
    return line.startswith('INSERT INTO')


def get_mysql_name_value(line):
    value = None
    result = re.search(r'\`([^\`]*)\`', line)
    if result:
        value = result.groups()[0]
    return value


def get_value_tuples(line):
    values = line.partition(' VALUES ')[-1].strip().replace('NULL', "''")
    if values[-1] == ';':
        values = values[:-1]

    return ast.literal_eval(values)


def write_file(output_directory, table_name, schema, values):
    file_name = os.path.join(output_directory, '%s.csv' % (table_name,))
    with open(file_name, 'w') as write_file:
        writer = csv.DictWriter(write_file, fieldnames=schema)
        writer.writeheader()
        for value in values:
            writer.writerow(dict(zip(schema, value)))


def parse_file(file_name, output_directory):
    current_table_name = None
    with open(file_name, 'r') as read_file:
        for line in read_file:
            if is_create_statement(line):
                current_table_name = get_mysql_name_value(line)
                SCHEMAS[current_table_name] = []
            elif current_table_name and is_field_definition(line):
                field_name = get_mysql_name_value(line)
                SCHEMAS[current_table_name].append(field_name)
            elif is_insert_statement(line):
                current_table_name = get_mysql_name_value(line)
                current_schema = SCHEMAS[current_table_name]
                values = get_value_tuples(line)

                # append these values to the table's existing ones, if any exist
                if not current_table_name in VALUES:
                    VALUES[current_table_name] = []
                for value in values:
                    VALUES[current_table_name].append(value)

        # we've stored the entire set of schemas in SCHEMAS and the entire set
        # of data in VALUES, both indexed by table name -- now write them
        for tableName,schema in SCHEMAS.items():
            write_file(output_directory, tableName, schema, VALUES[tableName])
if __name__ == '__main__':
    parse_file(sys.argv[1], sys.argv[2])

