from base_repo import BaseRepo

class TodoRepo(BaseRepo):
    def _get_table(self):
        return self.schema.tables['todo']
