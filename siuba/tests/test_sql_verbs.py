from siuba import group_by, mutate, collect
from siuba.siu import _
from siuba.sql import LazyTbl 
from siuba.sql.dialects.base import translator


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


@pytest.fixture(scope = "module")
def db():
    engine = create_engine('sqlite:///:memory:', echo=False)

    metadata.create_all(engine)

    conn = engine.connect()

    ins = users.insert().values(name='jack', fullname='Jack Jones')
    result = conn.execute(ins)


    ins = users.insert()
    conn.execute(ins, id=2, name='wendy', fullname='Wendy Williams')
    yield conn

# LazyTbl ---------------------------------------------------------------------

def test_lazy_tbl_table_string(db):
    tbl = LazyTbl(db, 'addresses')
    tbl.tbl.columns.user_id

def test_lazy_tbl_manual_columns(db):
    tbl = LazyTbl(db, 'addresses', columns = ('user_id', 'wrong_name'))
    tbl.tbl.columns.wrong_name
    tbl.tbl.columns.user_id

    with pytest.raises(AttributeError):
        tbl.tbl.columns.email_address

# SqlFunctionLookupError ------------------------------------------------------

from siuba import _, arrange, filter, mutate, summarize
from siuba.sql import SqlFunctionLookupError
from siuba.siu import strip_symbolic

def test_lazy_tbl_shape_call_error(db):
    tbl = LazyTbl(db, 'addresses')

    call = strip_symbolic(_.id.asdkfjsdf())
    with pytest.raises(SqlFunctionLookupError) as err:
        tbl.shape_call(call)

        # suppresses context for shorter stack trace
        assert err.__suppress_context__ == True



# TODO: remove these old tests? should be redundant ===========================


# mutate ----------------------------------------------------------------------

def test_sql_mutate(db):
    tbl = LazyTbl(db, addresses, translator = translator)
    f = mutate(user_id2 = _.user_id + 1)
    out1 = tbl >> f >> collect()
    out2 = tbl >> collect() >> f

    assert_frame_equal(out1, out2)


# group_by --------------------------------------------------------------------

@pytest.mark.parametrize("group_vars", [
    ["id",],                                # string syntax
    ["id", "user_id"],                      # string syntax multiple
    [_.id],                                 # _ syntax
    [_.id, _.user_id],                      # _ syntax multiple
    ])
def test_sql_group_by(db, group_vars):
    tbl = LazyTbl(db, addresses, translator = translator)
    group_by(tbl, *group_vars)


@pytest.mark.parametrize("group_var, error", [
    (_.id + 1, NotImplementedError),        # complex expressions
    (_.notacol, KeyError)                   # missing columns
    ])
def tets_sql_group_by_fail(db, group_var, error):
    tbl = LazyTbl(db, addresses, translator = translator)
    with pytest.raises(error):
        group_by(tbl, group_var)
    
