import unittest

from sqlalchemy import MetaData, create_engine
from sqlalchemy import Table, Column, Integer, Text, ForeignKey
from sqlalchemy.orm import sessionmaker, relationship, subqueryload
from saimmutable import mapper

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
        self.e = create_engine("sqlite://", echo=True)
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


a = Table("a", m,
    Column("id", Integer, primary_key=True),
    Column("text", Text))

b = Table("b", m,
    Column("aid", Integer, ForeignKey("a.id"), primary_key=True))

class A(object):
    def __init__(self, id, text=None):
        self.id = id
        self.text = text

    def __repr__(self):
        return "<A at 0x%s id=%d text=%s>" % (id(self), self.id, self.text)

class B(object):
    def __init__(self, aid):
        self.aid = aid

    def __repr__(self):
        return "<B at 0x%s aid=%d>" % (id(self), self.aid)


mapper(A, a)
mapper(B, b, properties={
    "a": relationship(A, backref="b"),
    })

class TestWithRels(TestCase):

    def setUp(self):
        super(TestWithRels, self).setUp()
        s = self.Session()
        s.add(A(1, "text1"))
        s.add(B(1))
        s.commit()
        s.close()

    def test_smoke(self):
        s = self.Session()

        a = s.query(A).get(1)
        self.assertTrue(a is not None)
        self.assertIsInstance(a, A)
        self.assertTrue(a.b is not None)
        self.assertIsInstance(a.b, list)
        self.assertEqual(len(a.b), 1)

        b = a.b[0]
        self.assertIsInstance(b, B)
        self.assertEqual(b.aid, 1)

    def test_subquery_load(self):
        s = self.Session()
        a = s.query(A).options(subqueryload(A.b)).all()
        self.assertEqual(len(a), 1)
        a = a[0]
        self.assertTrue("b" in a.__dict__)
        self.assertIsInstance(a.__dict__["b"], list)

if __name__ == "__main__":
    e = create_engine("sqlite://")
    m.bind = e
    m.create_all()
    Session = sessionmaker(bind=e)

    s = Session()
    s.add_all([T(x) for x in range(10000)])
    s.commit()
    s.close()

    import cPickle as pickle
    s = Session()
    ret = s.query(T).all()
    data = pickle.dumps(list(ret))

    def p():
        pickled = pickle.dumps(list(ret))
        unpickled = pickle.loads(data)

    import profile
    profile.run('p()')
