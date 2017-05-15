import MySQLdb


STAGING_DB_HANDLE = MySQLdb.connect(host="127.0.0.1",port=3306,user="root",passwd="root",db="staging")
PROD_DB_HANDLE = MySQLdb.connect(host="127.0.0.1",port=3306,user="root",passwd="root",db="prod")

STAGING_CURSOR = STAGING_DB_HANDLE.cursor()
PROD_CURSOR = PROD_DB_HANDLE.cursor()

def get_migrations():

  staging_tables = map(lambda x: list(x)[0], _get_all_tables(STAGING_CURSOR))
  prod_tables = map(lambda x: list(x)[0], _get_all_tables(PROD_CURSOR))

  less = _get_less(staging_tables, prod_tables)
  extra = _get_extra(staging_tables, prod_tables)
  common = _get_common(staging_tables, prod_tables)

  migrations = []
  _process_extra_tables(extra, migrations)
  _process_less_tables(less, migrations)
  _process_common_tables(common, migrations)

  return migrations


def _process_less_tables(less_tables, migrations):
  # create these missing tables in prod which are in staging
  for table in less_tables:
    migration = "CREATE TABLE %s ("%table
    schema = _get_table_schema(table, STAGING_CURSOR)
    for column_definition in schema:
      migration += "\n\t" + get_column_migration(column_definition) + ","
    migration = migration[0:-1]

    migration += get_primary_keys(schema)
    migration += "\n);"
    migrations.append(migration)


def get_primary_keys(schema):
  for column_definition in schema:
    if "PRI" == column_definition[3]:
      return "PRI %s" + column_definition[0]
  return ""

def _process_extra_tables(extra_tables, migrations):
  # delete these extra tables from prod which are not in staging
  for table in extra_tables:
    migration = "drop table %s;"%table
    migrations.append(migration)
  

def _process_common_tables(common_tables, migrations):

  # get fields , get columns of a table
  for table in common_tables:
    staging_table_schema = _get_table_schema(table, STAGING_CURSOR)
    prod_table_schema = _get_table_schema(table, PROD_CURSOR) 
    staging_table_columns = extract_columns(staging_table_schema)
    prod_table_columns = extract_columns(prod_table_schema)

    # compare these list of strings
    less = _get_less(staging_table_columns, prod_table_columns)
    extra = _get_extra(staging_table_columns, prod_table_columns)
    common = _get_common(staging_table_columns, prod_table_columns)

    _process_extra_columns(extra, table, migrations)
    _process_less_columns(less, table, migrations)
    final = _process_common_columns(common, table, migrations)



def extract_columns(table_schema):
  return map(lambda x: list(x)[0], table_schema)

  # purpose of this function is to come up with a final schema for the input table 
  # which will have the changes if any from staging version of the table 
  # return the final version of the table schema

  # 1. get table schema from

def _process_common_columns(common_columns, table_name, migrations):
  Ts = {}
  Tp = {}  
  staging_table_schema  = _get_table_schema(table_name, STAGING_CURSOR)
  prod_table_schema = _get_table_schema(table_name, PROD_CURSOR)
  for schema_field_tuple in staging_table_schema:
    if schema_field_tuple[0] in common_columns:
      Ts[str(schema_field_tuple[0])] = list(schema_field_tuple)

  for schema_field_tuple in prod_table_schema:
    if schema_field_tuple[0] in common_columns:
      Tp[str(schema_field_tuple[0])] = list(schema_field_tuple)

  for common_column in common_columns:
    staging_column = Ts[common_column]
    production_column = Tp[common_column]
    if are_different_column(staging_column, production_column):
      migrations.append(get_column_diff_migration(table_name, staging_column, production_column))

def are_different_column(staging_column, production_column):
  for i in xrange(0, len(staging_column)):
    if staging_column[i] != production_column[i]:
      print staging_column[i], production_column[i]
      return True
  return False

def get_column_diff_migration(table, staging_column, production_column):
  migration = "ALTER TABLE %s CHANGE COLUMN %s %s %s"%(table, staging_column[0], staging_column[0], staging_column[1])
  if (staging_column[2] != production_column[2]):
    if "NO" == staging_column[2]:
      migration += " NOT NULL"
    else:
      migration += " NULL"
  if (staging_column[4] != production_column[4]):
    migration += " DEFAULT %s"%staging_column[4]
  return migration + ";"

def _process_less_columns(less_column_names, table_name, migrations):
  staging_table_schema = _get_table_schema(table_name, STAGING_CURSOR)
  less_columns = [schema_field_tuple for schema_field_tuple in staging_table_schema if schema_field_tuple[0] in less_column_names]
  for column in less_columns:
    migrations.append("ALTER TABLE %s ADD COLUMN %s;"%(table_name, get_column_migration(column)))
  

def _process_extra_columns(columns, table, migrations):
  # detele these columns from prod table `table`
  for column in columns:
    migrations.append("ALTER TABLE %s DROP COLUMN %s;"%(table, column))


def get_column_migration(column_definition):
    migration = "%s %s"%(column_definition[0], column_definition[1])
    if "NO" == column_definition[2]:
      migration += " NOT NULL"
    if column_definition[4]:
      migration += "DEFAULT %s"%column_definition[4]
    return migration

def _get_all_tables(db_cursor):
  db_cursor.execute("show tables;")
  return db_cursor.fetchall()


def _get_table_schema(table, db_cursor):
  if not table:
    return ()

  db_cursor.execute("desc %s;"%table)
  return db_cursor.fetchall()


def _get_extra(staging_list, prod_list):
  return list(set(prod_list) - set(staging_list))

def _get_common(staging_list, prod_list):
  return filter(lambda x: x in staging_list, prod_list)

def _get_less(staging_list, prod_list):
  return list(set(staging_list) - set(prod_list))


if __name__ == "__main__":

  print "\n".join(get_migrations())