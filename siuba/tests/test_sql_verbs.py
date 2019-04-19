from siuba.sql import mutate, LazyTbl, collect
from siuba.siu import _
from siuba.sql.translate import funcs


from sqlalchemy import sql
from sqlalchemy import Table, Column, Integer, String, MetaData, ForeignKey
from sqlalchemy import create_engine

from pandas.testing import assert_frame_equal
import pytest

metadata = MetaData()
users = Table('users', metadata,
    Column('id', Integer, primary_key=True),
    Column('name', String),
    Column('fullname', String),
)


addresses = Table('addresses', metadata,
    Column('id', Integer, primary_key=True),
    Column('user_id', None, ForeignKey('users.id')),
    Column('email_address', String, nullable=False)
)


@pytest.fixture
def db():
    engine = create_engine('sqlite:///:memory:', echo=False)

    metadata.create_all(engine)

    conn = engine.connect()

    ins = users.insert().values(name='jack', fullname='Jack Jones')
    result = conn.execute(ins)


    ins = users.insert()
    conn.execute(ins, id=2, name='wendy', fullname='Wendy Williams')
    yield conn



def test_sql_mutate(db):
    tbl = LazyTbl(db, addresses, funcs = funcs)
    f = mutate(user_id2 = _.user_id + 1)
    out1 = tbl >> f >> collect()
    out2 = tbl >> collect() >> f

    assert_frame_equal(out1, out2)

    

