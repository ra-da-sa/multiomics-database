import sqlite3

'''
DatabaseCreator is a class created for BIOL4292: Programming and Databases for Biologists at the University of Glasgow.

Objectives
1. DatabaseCreator creates and initialises a SQLite database.
2. DatabseCreator reads an SQL schema file, enables foreign key constraints, and executes the schema to create all required tables.
'''

class DatabaseCreator:
    def __init__(self, schema_filename, database_filename):
        self.schema_filename = schema_filename
        self.database_filename = database_filename 

    def create(self):
        try: 
            with open(self.schema_filename, 'r') as sql_file:
                sql_script = sql_file.read()
        # if the SQL schema file is not found, the user will be notified with a FileNotFoundError
        except FileNotFoundError: 
            raise FileNotFoundError(f'SQL schema file {self.schema_filename} not found')
        
        con = sqlite3.connect(self.database_filename)
        con.execute('PRAGMA foreign_keys = ON') # enables FK constraints
        con.executescript(sql_script)
        con.commit()
        con.close()
