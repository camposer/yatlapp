from sqlalchemy import insert, select

class TodoRepo:
    def __init__(self, schema):
        self.schema = schema

    def insert(self, todo):
        return insert(self.__get_table()).values(todo)

    def find_all(self):
        return select(self.__get_table())

    def __get_table(self):
        return self.schema.tables['todo']