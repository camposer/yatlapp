from base_repo import BaseRepo

class UserRepo(BaseRepo):
    def _get_table(self):
        return self.schema.tables['user']
