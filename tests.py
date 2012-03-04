import unittest

from sqlalchemy import MetaData, create_engine
from sqlalchemy import Table, Column, Integer, Text
from sqlalchemy.orm import sessionmaker
from sqlalchemyro import mapper

m = MetaData()
t = Table("t", m,
    Column("id", Integer, primary_key=True),
    Column("text", Text))

class T(object):
    def __init__(self, id, text=None):
        self.id = id
        self.text = text

    def __repr__(self):
        return "<T at 0x%s id=%d text=%s>" % (id(self), self.id, self.text)

mapper(T, t)

class TestCase(unittest.TestCase):

    def setUp(self):
        self.e = create_engine("sqlite://")
        m.bind = self.e
        m.create_all()
        self.Session = sessionmaker(bind=self.e)

    def tearDown(self):
        self.e.dispose()

class TestSimple(TestCase):

    def setUp(self):
        super(TestSimple, self).setUp()
        s = self.Session()
        s.add(T(1, "text1"))
        s.add(T(2, "text2"))
        s.commit()
        s.close()

    def test_smoke(self):
        s = self.Session()

        t = s.query(T).get(1)
        self.assertEqual(t.id, 1)
        self.assertEqual(t.text, "text1")

        t = s.query(T).get(1)
        self.assertEqual(t.id, 1)
        self.assertEqual(t.text, "text1")

        ts = s.query(T).order_by(T.id.asc()).all()
        self.assertEqual(len(ts), 2)
        t1, t2 = ts
        self.assertEqual(t1.id, 1)
        self.assertEqual(t1.text, "text1")
        self.assertEqual(t2.id, 2)
        self.assertEqual(t2.text, "text2")

    def test_pickle(self):
        import pickle
        s = self.Session()
        t1 = s.query(T).get(1)
        s.close()
        data = pickle.dumps(t1)
        t2 = pickle.loads(data)
        self.assertEqual(t1.id, t2.id)
        self.assertEqual(t1.text, t2.text)
