from abc import ABC, abstractmethod
from sqlalchemy import insert, select, update
from sqlalchemy.exc import InvalidRequestError

class BaseRepo(ABC):
    def __init__(self, tx):
        self.conn = tx.conn
        self.schema = tx.schema

    def find_all(self):
        stmt = select(self._get_table())
        return self.conn.execute(stmt)

    def find_by_id(self, id):
        table = self._get_table()
        stmt = select(table).where(table.c.id == id)
        res = self.conn.execute(stmt)
        if res:
            return res.fetchone()

    def insert(self, user):
        table = self._get_table()
        stmt = insert(table).values(user)
        res = self.conn.execute(stmt)
        return res.inserted_primary_key[0]

    def update(self, user):
        prev_user = self.find_by_id(user['id'])
        if not prev_user:
            raise InvalidRequestError('Invalid id')
        stmt = update(self._get_table()).values(user)
        self.conn.execute(stmt)

    @abstractmethod
    def _get_table(self):
        pass
