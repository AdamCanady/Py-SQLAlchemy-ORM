# Py-SQLAlchemy-ORM
# Adam Canady, 2013

''' Database Connection '''
from sqlalchemy import *
from sqlalchemy.orm import *
from sqlalchemy.ext.declarative import *
from sqlalchemy.dialects.mysql import TEXT

engine = create_engine('mysql://username:password@host:port/database_name?charset=utf8', encoding="utf-8")
Session = sessionmaker(bind=engine)
session = Session()
SQLAlchemyBase = declarative_base()

''' Tables '''
class ORMBase:
    def __init__(self, final):
        for key in final:
            self[key] = final[key]

        session.add(self)

        session.flush()
        session.commit()

    def __init__(self, spec, final, upsert = False):
        for key in final:
            self[key] = final[key]

        q = session.query(self.__class__)
        for attr, value in spec.items():
            if value == None: q.filter(getattr(self.__class__, attr) == None)
            else: q = q.filter(getattr(self.__class__, attr).like("%s" % value))
        q = q.all()

        # If there is an old object and we want to replace old object, lets do that,
        # or, if there are no old objects and we want to only add new ones, lets do that
        if (q and upsert == True) or (len(q) == 0):
            if q:
                for key in final:
                    if key != None:
                        q[0][key] = final[key]
                session.add(q[0])
            else:
                for key in final:
                    self[key] = final[key]
                session.add(self)

            session.flush()
            session.commit()

    def __setitem__(self, key, value):
        setattr(self, key, value)

    def __getitem__(self, key):
        return getattr(self, key)

    def __repr__(self):
        import pprint
        pprint.pprint(self.__dict__)

    def find(self, attr, value):
        return session.query(self.__class__).filter(getattr(self.__class__, attr).like(value)).all()

    def find_by_attr(self, other_object, attr):
        return self.find(attr, getattr(other_object, attr))

    def update(self, attr, value):
        self['attr'] = value
        session.add(self)

        session.flush()
        session.commit()

# Example table classes to demonstrate features. If you implement this,
# make sure to create classes for your own tables.
class Customer(ORMBase, SQLAlchemyBase):
    __tablename__ = 'customers'

    __table_args__ = (Index('spec', 'phone','email','first_name','last_name' ), )


class Transaction(Lead, Base):
    __tablename__ = 'cmn'

Base.metadata.create_all(engine)

class Query:

    def find_unmatched(self):
        return session.query(ClientLead).filter(getattr(ClientLead, 'lead_id') == None).all()

    def find_by_attr(self, current_object, other_type, attr):
        return session.query(other_type).filter(getattr(other_type, attr).like(current_object[attr])).all()


# For debug
def printquery(statement, bind=None):
    """
    print a query, with values filled in
    for debugging purposes *only*
    for security, you should always separate queries from their values
    please also note that this function is quite slow
    """
    import sqlalchemy.orm
    if isinstance(statement, sqlalchemy.orm.Query):
        if bind is None:
            bind = statement.session.get_bind(
                    statement._mapper_zero_or_none()
            )
        statement = statement.statement
    elif bind is None:
        bind = statement.bind

    dialect = bind.dialect
    compiler = statement._compiler(dialect)
    class LiteralCompiler(compiler.__class__):
        def visit_bindparam(
                self, bindparam, within_columns_clause=False,
                literal_binds=False, **kwargs
        ):
            return super(LiteralCompiler, self).render_literal_bindparam(
                    bindparam, within_columns_clause=within_columns_clause,
                    literal_binds=literal_binds, **kwargs
            )

    compiler = LiteralCompiler(dialect, statement)
    print compiler.process(statement)

''' Tests '''
if __name__ == "__main__":


    a = ClientLead(spec, leadstuff, upsert = True)

    print a['phone']
    print a['enrolled_date']
    print a, dir(a)
    print a.phone
