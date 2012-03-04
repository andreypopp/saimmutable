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
        return "<T id=%d text=%s>" % (self.id, self.text)

mapper(T, t)

class TestCase(unittest.TestCase):

    def setUp(self):
        self.e = create_engine("sqlite://", echo=True)
        m.bind = self.e
        m.create_all()
        self.Session = sessionmaker(bind=self.e)

    def tearDown(self):
        self.e.dispose()

    def test_smole(self):
        s = self.Session()
        s.add(T(1, "text1"))
        s.add(T(2, "text2"))
        s.commit()
        s = self.Session()
        t = s.query(T).first()
        print t
