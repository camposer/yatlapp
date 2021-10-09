import os
from sqlalchemy import (
    create_engine, 
    Boolean,
    Column, 
    ForeignKey,
    Integer, 
    MetaData, 
    String,
    Table
)

class Schema:
    def __init__(self):
        self.engine = create_engine(os.getenv("DB_URI"), echo=True, future=True)
        self.metadata = MetaData()
        self.tables = self.__generate_tables()

    def create_transaction(self):
        return TransactionManager(self)        

    def create_all_tables(self):
        self.metadata.create_all(self.engine)

    def drop_all_tables(self):
        self.metadata.drop_all(self.engine)

    def __generate_tables(self):
        return {
            'todo': Table('todo', self.metadata,
                Column('id', Integer, primary_key=True , autoincrement=True),
                Column('user_id', Integer, ForeignKey('user.id')),
                Column('description', String(500)),
                Column('active', Boolean)
            ),
            'user': Table('user', self.metadata,
                Column('id', Integer, primary_key=True , autoincrement=True),
                Column('email', String(50)),
                Column('fullname', String(50))
            ),
        }

class TransactionManager:
    def __init__(self, schema):
        self.schema = schema
        self.conn = self.schema.engine.connect()

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.conn.commit()
        self.conn.close()

    def drop_all_tables(self):
        self.schema.metadata.drop_all(self.engine)

    def create_all_tables(self):
        self.schema.metadata.create_all(self.engine)
