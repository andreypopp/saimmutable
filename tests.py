from sqlalchemy import MetaData, create_engine
from sqlalchemy import Table, Column, Integer, Text
from sqlalchemy.orm import sessionmaker
from sqlalchemyro import mapper

e = create_engine("sqlite://", echo=True)
m = MetaData(bind=e)
t = Table("t", m,
    Column("id", Integer, primary_key=True),
    Column("text", Text))
m.create_all()

S = sessionmaker(bind=e)

class T(object):
    def __init__(self, id, text=None):
        self.id = id
        self.text = text

    def __repr__(self):
        return "<T id=%d text=%s>" % (self.id, self.text)

mapper(T, t)

s = S()
s.add(T(1))
s.add(T(2))
s.commit()

s = S()
print s.query(T).all()
t =  s.query(T).first()
print t
print t.__dict__
