import MySQLdb


class MigrationGenerator():

  """
  Collection of all db configs and tables' schema processing methods.
  """

  def __init__(self, base_connection_params, target_connection_params):
    """
    :param base_connection_params and target_connection_params are the dicts 
    containing databse connection params such as host, port, username, pwd, and db name.
    """
    self.BASE_DB_HANDLE = MySQLdb.connect(
      host=base_connection_params.get("host"),
      port=base_connection_params.get("port"),
      user=base_connection_params.get("user"),
      passwd=base_connection_params.get("passwd"),
      db=base_connection_params.get("db")
    )
    self.TARGET_DB_HANDLE = MySQLdb.connect(
      host=target_connection_params.get("host"),
      port=target_connection_params.get("port"),
      user=target_connection_params.get("user"),
      passwd=target_connection_params.get("passwd"),
      db=target_connection_params.get("db")
    )

    self.BASE_CURSOR = self.BASE_DB_HANDLE.cursor()
    self.TARGET_CURSOR = self.TARGET_DB_HANDLE.cursor()

  def get_migrations(self):

    """
    Returns all migrations
    """

    base_tables = map(lambda x: list(x)[0], _get_all_tables(self.BASE_CURSOR))
    target_tables = map(lambda x: list(x)[0], _get_all_tables(self.TARGET_CURSOR))

    less = _get_less(base_tables, target_tables)
    extra = _get_extra(base_tables, target_tables)
    common = _get_common(base_tables, target_tables)

    migrations = []
    migrations.extend(self._process_extra_tables(extra))
    migrations.extend(self._process_less_tables(less))
    migrations.extend(self._process_common_tables(common))

    return migrations

  def _process_less_tables(self, less_tables):
    """
    :param less_tables is the list of string names of additional
    tables present in base
    """
    migrations = []
    for table in less_tables:
      migration = "CREATE TABLE %s ("%table
      schema = _get_table_schema(table, self.BASE_CURSOR)
      for column_definition in schema:
        migration += "\n\t" + get_column_migration(column_definition) + ","
      migration = migration[0:-1]

      migration += get_primary_keys(schema)
      migration += "\n);"
      migrations.append(migration)
    return migrations

  def _process_common_tables(self, common_tables):

    """
    Creates relevant migrating sql statements changed columns if any from base and target
    :param common_tables is the list of string names of common
    tables present in both base and target
    
    """
    migrations = []

    for table in common_tables:
      base_table_schema = _get_table_schema(table, self.BASE_CURSOR)
      target_table_schema = _get_table_schema(table, self.TARGET_CURSOR) 
      base_table_columns = extract_columns(base_table_schema)
      target_table_columns = extract_columns(target_table_schema)

      # compare these list of strings
      less = _get_less(base_table_columns, target_table_columns)
      extra = _get_extra(base_table_columns, target_table_columns)
      common = _get_common(base_table_columns, target_table_columns)

      migrations.extend(self._process_extra_columns(extra, table))
      migrations.extend(self._process_less_columns(less, table))
      migrations.extend(self._process_common_columns(common, table))
    return migrations

  def _process_extra_tables(self, extra_tables):
    
    """
    Drops the extra tables from target which are not present in base.
    :param extra_tables is the list of string names of additional
    tables present in target
    
    """
    migrations = []
    for table in extra_tables:
      migration = "drop table %s;"%table
      migrations.append(migration)
    return migrations


  def _process_common_columns(self, common_columns, table_name):

    """

    Generate relevant migrations by comparing base and target columns.
    :param common_columns is the list of common fields from a table.
    :param table_name is the table name string.
    """

    Ts = {}
    Tp = {}  
    migrations = []
    base_table_schema  = _get_table_schema(table_name, self.BASE_CURSOR)
    target_table_schema = _get_table_schema(table_name, self.TARGET_CURSOR)
    for schema_field_tuple in base_table_schema:
      if schema_field_tuple[0] in common_columns:
        Ts[str(schema_field_tuple[0])] = list(schema_field_tuple)

    for schema_field_tuple in target_table_schema:
      if schema_field_tuple[0] in common_columns:
        Tp[str(schema_field_tuple[0])] = list(schema_field_tuple)

    for common_column in common_columns:
      base_column = Ts[common_column]
      target_column = Tp[common_column]
      if are_different_column(base_column, target_column):
        migrations.append(get_column_diff_migration(table_name, base_column, target_column))
    return migrations

  def _process_less_columns(self, less_column_names, table_name):
    """
    Returns migrations for missing columns from target.
    :param less_column_names missing in table columns in target
    """
    migrations = []
    base_table_schema = _get_table_schema(table_name, self.BASE_CURSOR)
    less_columns = [schema_field_tuple for schema_field_tuple in base_table_schema if schema_field_tuple[0] in less_column_names]
    for column in less_columns:
      migrations.append("ALTER TABLE %s ADD COLUMN %s;"%(table_name, get_column_migration(column)))
    return migrations

  def _process_extra_columns(self, extra_columns, table):
    """
    Returns migrations for extra columns from base.
    :param less_column_names missing in table columns in target
    """
    migrations = []
    for column in extra_columns:
      migrations.append("ALTER TABLE %s DROP COLUMN %s;"%(table, column))
    return migrations
    

######## Helpers

def get_primary_keys(schema):
  for column_definition in schema:
    if "PRI" == column_definition[3]:
      return "PRI %s" + column_definition[0]
  return ""


def extract_columns(table_schema):
  return map(lambda x: list(x)[0], table_schema)


def are_different_column(base_column, target_column):
  for i in xrange(0, len(base_column)):
    if base_column[i] != target_column[i]:
      return True
  return False


def get_column_diff_migration(table, base_column, target_column):
  migration = "ALTER TABLE %s CHANGE COLUMN %s %s %s"%(table, base_column[0], base_column[0], base_column[1])
  if (base_column[2] != target_column[2]):
    if "NO" == base_column[2]:
      migration += " NOT NULL"
    else:
      migration += " NULL"
  if (base_column[4] != target_column[4]):
    migration += " DEFAULT %s"%base_column[4]
  return migration + ";"


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
  db_cursor.execute("desc %s;"%table)
  return db_cursor.fetchall()


def _get_extra(base_list, target_list):
  return list(set(target_list) - set(base_list))


def _get_common(base_list, target_list):
  return filter(lambda x: x in base_list, target_list)


def _get_less(base_list, target_list):
  return list(set(base_list) - set(target_list))


if __name__ == "__main__":

  # In case of accessing remote dbs, you will have to grant the permission to access.

  staging_connection_params = {
    "host": "127.0.0.1",
    "port": 3306,
    "user": "root",
    "passwd": "root",
    "db": "staging"
  }
  production_connection_params = {
    "host": "127.0.0.1",
    "port": 3306,
    "user": "root",
    "passwd": "root",
    "db": "prod"
  }

  obj = MigrationGenerator(staging_connection_params, production_connection_params)

  print "\n".join(obj.get_migrations())