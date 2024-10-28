from sqlalchemy import Column, Integer, ForeignKey, JSON, String, create_engine
from sqlalchemy.orm import relationship, sessionmaker, declarative_base

Base = declarative_base()


class JSONModel:
    """
    A base class for models with JSON properties, allowing regular dictionary-style
    access and automatically handling JSON serialization and deserialization
    when stored in or retrieved from the database.
    """

    _properties_storage = None  # Database storage for JSON serialized data

    @property
    def properties(self):
        """Return properties as a dictionary, deserialized from JSON if necessary."""
        if self._properties_storage is None:
            self._properties = {}  # Initialize as an empty dictionary if None
        else:
            self._properties = self._properties_storage  # Load from storage
        return self._properties

    @properties.setter
    def properties(self, value):
        """Set properties as a dictionary, which will be serialized to JSON on storage."""
        self._properties = value
        self._properties_storage = value  # Store as JSON for database

    def get(self, key):
        """Retrieve a value from the properties by key."""
        return self.properties.get(key)

    def set(self, key, value):
        """Set a value in the properties dictionary."""
        self.properties[key] = value  # Update properties dictionary
        self._properties_storage = self._properties  # Sync with storage


class Node(Base, JSONModel):
    """
    Represents a generic node in a graph, with properties stored as JSON.

    Attributes:
        id (int): Primary key of the node.
        properties (dict): Dictionary of properties for the node.
    """

    __tablename__ = "nodes"

    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False)
    type = Column(String, nullable=False)  # Column for polymorphic identity
    _properties_storage = Column(
        "properties", JSON, nullable=True
    )  # Internal storage for JSON

    edges_from = relationship(
        "Edge", foreign_keys="[Edge.from_node_id]", back_populates="from_node"
    )
    edges_to = relationship(
        "Edge", foreign_keys="[Edge.to_node_id]", back_populates="to_node"
    )

    __mapper_args__ = {"polymorphic_on": type, "polymorphic_identity": "node"}

    def get_related_nodes(self, direction="out", related_type=None, edge_type="Refer"):
        """
        Generalized method to retrieve related nodes based on specified direction, type, and edge.

        :param direction: 'out' for edges_from (from_node) or 'in' for edges_to (to_node).
        :param related_type: Class type of related nodes to filter (e.g., Table, Query).
        :param edge_type: Edge class type to filter by (e.g., Refer).
        :return: Set of related nodes.
        """
        if direction == "out":
            edges = self.edges_from
        elif direction == "in":
            edges = self.edges_to
        else:
            raise ValueError("direction must be 'out' or 'in'")

        # Collect related nodes based on specified edge type and related node type
        return {
            edge.to_node if direction == "out" else edge.from_node
            for edge in edges
            if edge.__class__.__name__ == edge_type
            and (
                related_type is None
                or isinstance(
                    edge.to_node if direction == "out" else edge.from_node, related_type
                )
            )
        }


class Query(Node):
    __mapper_args__ = {"polymorphic_identity": "query"}

    def refers(self, recursive=False, visited=None):
        """
        Returns all tables referenced by this Query node.

        :param recursive: If True, performs a recursive traversal to find all referenced tables.
        :param visited: Set of visited node IDs to avoid infinite loops in cyclic graphs.
        :return: Set of all referenced Table nodes.
        """
        if recursive:
            # Recursive traversal
            if visited is None:
                visited = set()

            # Add current query ID to visited
            visited.add(self.id)

            # Initialize set for storing all referenced tables
            tables = set()

            # Traverse all references, recursively if needed
            for refer in self.edges_from:
                to_node = refer.to_node
                if isinstance(to_node, Table):
                    tables.add(to_node)
                elif isinstance(to_node, Query) and to_node.id not in visited:
                    # Recursively retrieve tables from nested queries
                    tables.update(to_node.refers(recursive=True, visited=visited))

            return tables
        else:
            # Directly referenced tables (non-recursive)
            # return {edge.to_node for edge in self.edges_from if isinstance(edge, Refer) and isinstance(edge.to_node, Table)}
            return self.get_related_nodes(
                direction="out", related_type=Table, edge_type="Refer"
            )


class Table(Node):
    """
    Represents a table node in the database.

    Attributes:
        name (str): The name of the table.
        properties (dict): Dictionary of properties specific to the table.
    """

    __mapper_args__ = {"polymorphic_identity": "table"}

    def refered(self):
        """Returns all Query nodes that directly reference this Table."""
        return self.get_related_nodes(
            direction="in", related_type=Query, edge_type="Refer"
        )


class Edge(Base, JSONModel):
    """
    Represents a generic edge in a graph, connecting nodes with specific relationship types.

    Attributes:
        id (int): Primary key of the edge.
        from_node_id (int): Foreign key to the originating node.
        to_node_id (int): Foreign key to the destination node.
        properties (dict): Dictionary of properties for the edge.
    """

    __tablename__ = "edges"

    id = Column(Integer, primary_key=True)
    from_node_id = Column(Integer, ForeignKey("nodes.id"), nullable=False)
    to_node_id = Column(Integer, ForeignKey("nodes.id"), nullable=False)
    type = Column(String, nullable=False)  # Column for polymorphic identity
    _properties_storage = Column(
        "properties", JSON, nullable=True
    )  # Internal storage for JSON

    from_node = relationship(
        "Node", foreign_keys=[from_node_id], back_populates="edges_from"
    )
    to_node = relationship("Node", foreign_keys=[to_node_id], back_populates="edges_to")

    __mapper_args__ = {"polymorphic_on": type, "polymorphic_identity": "edge"}


class Refer(Edge):
    """
    Represents a specific type of edge where a Query refers to a Table.

    Attributes:
        id (int): Primary key of the edge.
        from_node_id (int): Foreign key to the originating Query.
        to_node_id (int): Foreign key to the destination Table.
        properties (dict): Dictionary of properties for the refer edge.
    """

    __mapper_args__ = {"polymorphic_identity": "refer"}


# SQLiteエンジンの作成
engine = create_engine("sqlite:///graph.db")
Base.metadata.create_all(engine)

# セッションの準備
Session = sessionmaker(bind=engine)

# データの追加例
session = Session()

# Tableの作成
table1 = Table(name="table_1")
table1.set("columns", ["id", "name", "value"])

# Queryの作成
query1 = Query(name="query_1")
query1.set("sql", "SELECT * FROM table_1")
query1.set("description", "This query retrieves all data from table_1.")

# ReferでQueryとTableをリンクし、「query refers table」の関係を表現
refer1 = Refer(from_node=query1, to_node=table1)

# プロパティの確認
print("Table properties:", table1.properties)
print("Query properties:", query1.properties)
print("Refer properties:", refer1.properties)

# refers
for t in query1.refers():
    print("query1 referes", t.name)

# refered
for q in table1.refered():
    print("table1 is refered by", q.name)

# セッションに追加
session.add_all([table1, query1, refer1])
session.commit()

# セッションを閉じる
session.close()
