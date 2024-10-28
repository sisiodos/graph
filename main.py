"""
docstring for main.py
"""

from database import init_db, create_sample_data, get_all_nodes

init_db()

create_sample_data()

queries, tables, refers = get_all_nodes()
print("Queries:", [query.name for query in queries])
print("Tables:", [table.name for table in tables])

# refers
for query in queries:
    for table in query.refers():
        print(query.name, "referes", table.name)

# refered
for table in tables:
    for query in table.refered():
        print(table.name, "is refered by", query.name)
