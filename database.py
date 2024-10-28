"""
docstring for database.py
"""

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, joinedload
from models import Base, Query, Table, Refer

DATABASE_URL = "sqlite:///graph.db"

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(bind=engine)


def init_db():
    """Create all tables in the database."""
    Base.metadata.create_all(engine)


def get_session():
    """Get a new session instance."""
    return SessionLocal()


def create_sample_data():
    """Populate the database with sample data."""
    session = get_session()
    try:
        # Sample data
        table1 = Table(name="table_1")
        table1.set("columns", ["id", "name", "value"])

        query1 = Query(name="query_1")
        query1.set("sql", "SELECT * FROM table_1")
        query1.set("description", "This query retrieves all data from table_1.")

        refer1 = Refer(from_node=query1, to_node=table1)

        session.add_all([table1, query1, refer1])
        session.commit()
    finally:
        session.close()


def get_all_nodes():
    """Retrieve all nodes with related edges preloaded."""
    session = get_session()
    try:
        queries = (
            session.query(Query)
            .options(joinedload(Query.edges_from), joinedload(Query.edges_to))
            .all()
        )
        tables = (
            session.query(Table)
            .options(joinedload(Table.edges_from), joinedload(Table.edges_to))
            .all()
        )
        refers = (
            session.query(Refer)
            .options(joinedload(Refer.from_node), joinedload(Refer.to_node))
            .all()
        )
        return queries, tables, refers
    finally:
        session.close()
