import pytest
from sqlalchemy import insert, select
from sqlalchemy.exc import IntegrityError, InvalidRequestError

from schema import Schema
from user_repo import UserRepo

@pytest.fixture
def schema(monkeypatch):
    monkeypatch.setenv("DB_URI", "sqlite://")
    schema = Schema()
    schema.drop_all_tables()
    schema.create_all_tables()
    return schema

class TestUserRepo_find_all:
    def test_when_multiple_users(self, schema):
        with schema.create_transaction() as tx:
            insert_user(tx, { 'email': 'a@test.com', 'fullname': 'fullname a' })
            insert_user(tx, { 'email': 'b@test.com', 'fullname': 'fullname b' })
            users = UserRepo(tx).find_all().fetchall()
            assert len(users) == 2
            assert users[0]['id'] == 1
            assert users[0]['email'] == 'a@test.com'
            assert users[0]['fullname'] == 'fullname a'
            assert users[1]['id'] == 2
            assert users[1]['email'] == 'b@test.com'
            assert users[1]['fullname'] == 'fullname b'

    def test_when_empty(self, schema):
        with schema.create_transaction() as tx:
            users = UserRepo(tx).find_all().fetchall()
            assert len(users) == 0

class TestUserRepo_find_by_id:
    def test_when_user_exists(self, schema):
        with schema.create_transaction() as tx:
            insert_user(tx, { 'email': 'a@test.com', 'fullname': 'fullname a' })
            insert_user(tx, { 'email': 'b@test.com', 'fullname': 'fullname b' })
            user = UserRepo(tx).find_by_id(1)
            assert user['id'] == 1
            assert user['email'] == 'a@test.com'
            assert user['fullname'] == 'fullname a'

    def test_when_user_does_not_exists(self, schema):
        with schema.create_transaction() as tx:
            user = UserRepo(tx).find_by_id(1)
            assert user is None

class TestUserRepo_insert:
    def test_when_auto_increment_id(self, schema):
        with schema.create_transaction() as tx:
            id = UserRepo(tx).insert({ 'email': 'a@test.com', 'fullname': 'fullname' })
            users = tx.conn.execute(select(get_table(schema))).fetchall()
            assert len(users) == 1
            assert users[0]['id'] == id
            assert users[0]['email'] == 'a@test.com'
            assert users[0]['fullname'] == 'fullname'

    def test_when_set_id(self, schema):
        with schema.create_transaction() as tx:
            UserRepo(tx).insert({ 'id': 99, 'email': 'a@test.com', 'fullname': 'fullname' })
            users = tx.conn.execute(select(get_table(schema))).fetchall()
            assert len(users) == 1
            assert users[0]['id'] == 99
            assert users[0]['email'] == 'a@test.com'
            assert users[0]['fullname'] == 'fullname'

    def test_when_invalid_id(self, schema):
        with schema.create_transaction() as tx, pytest.raises(IntegrityError):
            UserRepo(tx).insert({ 'id': 'invalid', 'email': 'a@test.com', 'fullname': 'fullname' })

    def test_when_duplicated_id(self, schema):
        with schema.create_transaction() as tx, pytest.raises(IntegrityError):
            repo = UserRepo(tx)
            repo.insert({ 'id': 1, 'email': 'a@test.com', 'fullname': 'fullname a' })
            repo.insert({ 'id': 1, 'email': 'b@test.com', 'fullname': 'fullname b' })

class TestUserRepo_update:
    def test_when_user_exists(self, schema):
        with schema.create_transaction() as tx:
            insert_user(tx, { 'id': 1, 'email': 'a@test.com', 'fullname': 'fullname a' })
            UserRepo(tx).update({ 'id': 1, 'email': 'updated@test.com', 'fullname': 'updated' })
            users = tx.conn.execute(select(get_table(schema))).fetchall()
            assert len(users) == 1
            assert users[0]['id'] == 1
            assert users[0]['email'] == 'updated@test.com'
            assert users[0]['fullname'] == 'updated'

    def test_when_user_does_not_exists(self, schema):
        with schema.create_transaction() as tx, pytest.raises(InvalidRequestError):
            UserRepo(tx).update({ 'id': 1, 'email': 'updated@test.com', 'fullname': 'updated' })

def get_table(schema):
    return schema.tables['user']

def insert_user(tx, user):
    stmt = insert(get_table(tx.schema)).values(user)
    tx.conn.execute(stmt)

