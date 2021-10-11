import pytest
from sqlalchemy import event, insert, select
from sqlalchemy.exc import IntegrityError, InvalidRequestError

from schema import Schema
from todo_repo import TodoRepo

@pytest.fixture
def schema(monkeypatch):
    monkeypatch.setenv("DB_URI", "sqlite://")
    schema = Schema()
    _enable_foreign_constraints(schema)
    schema.create_all_tables()
    return schema

def _enable_foreign_constraints(schema):
    def _fk_pragma_on_connect(dbapi_con, con_record):
        dbapi_con.execute('pragma foreign_keys=ON')
    event.listen(schema.engine, 'connect', _fk_pragma_on_connect)

class TestTodoRepo_find_all:
    def test_when_multiple_todos(self, schema):
        with schema.create_transaction() as tx:
            insert_obj(tx, 'user', { 'id': 1, 'email': 'a@test.com', 'fullname': 'fullname a' })
            insert_obj(tx, 'todo', { 'id': 1, 'user_id': 1, 'description': 'description a', 'active': True })
            insert_obj(tx, 'todo', { 'id': 2, 'user_id': 1, 'description': 'description b', 'active': False })
            todos = TodoRepo(tx).find_all().fetchall()
            assert len(todos) == 2
            assert todos[0]['id'] == 1
            assert todos[0]['user_id'] == 1
            assert todos[0]['description'] == 'description a'
            assert todos[0]['active'] == True
            assert todos[1]['id'] == 2
            assert todos[1]['user_id'] == 1
            assert todos[1]['description'] == 'description b'
            assert todos[1]['active'] == False

    def test_when_empty(self, schema):
        with schema.create_transaction() as tx:
            todos = TodoRepo(tx).find_all().fetchall()
            assert len(todos) == 0

class TestTodoRepo_find_by_id:
    def test_when_todo_exists(self, schema):
        with schema.create_transaction() as tx:
            insert_obj(tx, 'user', { 'id': 1, 'email': 'a@test.com', 'fullname': 'fullname a' })
            insert_obj(tx, 'todo', { 'id': 1, 'user_id': 1, 'description': 'description a', 'active': True })
            insert_obj(tx, 'todo', { 'id': 2, 'user_id': 1, 'description': 'description b', 'active': False })
            todo = TodoRepo(tx).find_by_id(1)
            assert todo['id'] == 1
            assert todo['user_id'] == 1
            assert todo['description'] == 'description a'
            assert todo['active'] == True

    def test_when_todo_does_not_exists(self, schema):
        with schema.create_transaction() as tx:
            todo = TodoRepo(tx).find_by_id(1)
            assert todo is None

class TestTodoRepo_insert:
    def test_when_auto_increment_id(self, schema):
        with schema.create_transaction() as tx:
            insert_obj(tx, 'user', { 'id': 1, 'email': 'a@test.com', 'fullname': 'fullname a' })
            id = TodoRepo(tx).insert({ 'user_id': 1, 'description': 'description a', 'active': True })
            table = tx.schema.tables['todo']
            todos = tx.conn.execute(select(table)).fetchall()
            assert len(todos) == 1
            assert todos[0]['id'] == id
            assert todos[0]['user_id'] == 1
            assert todos[0]['description'] == 'description a'
            assert todos[0]['active'] == True

    def test_when_set_id(self, schema):
        with schema.create_transaction() as tx:
            insert_obj(tx, 'user', { 'id': 1, 'email': 'a@test.com', 'fullname': 'fullname a' })
            TodoRepo(tx).insert({ 'id': 99, 'user_id': 1, 'description': 'description a', 'active': True })
            table = tx.schema.tables['todo']
            todos = tx.conn.execute(select(table)).fetchall()
            assert len(todos) == 1
            assert todos[0]['id'] == 99
            assert todos[0]['user_id'] == 1
            assert todos[0]['description'] == 'description a'
            assert todos[0]['active'] == True

    def test_when_invalid_id(self, schema):
        with schema.create_transaction() as tx, pytest.raises(IntegrityError):
            insert_obj(tx, 'user', { 'id': 1, 'email': 'a@test.com', 'fullname': 'fullname a' })
            TodoRepo(tx).insert({ 'id': 'invalid', 'user_id': 1, 'description': 'description a', 'active': True })

    def test_when_invalid_user_id(self, schema):
        with schema.create_transaction() as tx, pytest.raises(IntegrityError):
            TodoRepo(tx).insert({ 'id': 1, 'user_id': 1, 'description': 'description a', 'active': True })

    def test_when_duplicated_id(self, schema):
        with schema.create_transaction() as tx, pytest.raises(IntegrityError):
            repo = TodoRepo(tx)
            repo.insert({ 'id': 1, 'user_id': 1, 'description': 'description a', 'active': True })
            repo.insert({ 'id': 1, 'user_id': 1, 'description': 'description a', 'active': True })

class TestTodoRepo_update:
    def test_when_todo_exists(self, schema):
        with schema.create_transaction() as tx:
            insert_obj(tx, 'user', { 'id': 1, 'email': 'a@test.com', 'fullname': 'fullname a' })
            insert_obj(tx, 'user', { 'id': 2, 'email': 'b@test.com', 'fullname': 'fullname b' })
            insert_obj(tx, 'todo', { 'id': 1, 'user_id': 1, 'description': 'description a', 'active': True })
            TodoRepo(tx).update({ 'id': 1, 'user_id': 2, 'description': 'description b', 'active': False })
            table = tx.schema.tables['todo']
            todos = tx.conn.execute(select(table)).fetchall()
            assert len(todos) == 1
            assert todos[0]['id'] == 1
            assert todos[0]['user_id'] == 2
            assert todos[0]['description'] == 'description b'
            assert todos[0]['active'] == False

    def test_when_todo_does_not_exists(self, schema):
        with schema.create_transaction() as tx, pytest.raises(InvalidRequestError):
            TodoRepo(tx).update({ 'id': 1, 'user_id': 1, 'description': 'description b', 'active': False })

def insert_obj(tx, table_name, obj):
    table = tx.schema.tables[table_name]
    stmt = insert(table).values(obj)
    tx.conn.execute(stmt)
