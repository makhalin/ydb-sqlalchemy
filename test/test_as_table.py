import sqlalchemy as sa
import ydb
from sqlalchemy import Column, Integer, String, Boolean, Table
from sqlalchemy.testing.fixtures import TablesTest

def execute_as_table(connection, table, data, operation="UPSERT"):
    """
    Generic helper to execute UPSERT or INSERT using AS_TABLE.
    Constructs the YQL query and TypedValue dynamically based on the table schema
    using the dialect's type compiler.
    """
    dialect = connection.dialect

    # In the current environment, dialect.type_compiler is an instance of YqlTypeCompiler
    compiler = dialect.type_compiler

    struct_members = {}
    yql_struct_fields = []
    select_fields = []

    for col in table.columns:
        # Get YQL type string (e.g., "Int64", "Utf8")
        yql_type = compiler.process(col.type)

        # Get YDB SDK type (e.g., ydb.PrimitiveType.Int64, ydb.OptionalType(...))
        # get_ydb_type handles the optional wrapping if is_optional is True
        ydb_type = compiler.get_ydb_type(col.type, is_optional=col.nullable)

        struct_members[col.name] = ydb_type

        # Construct YQL declaration field
        # Append '?' for nullable fields
        if col.nullable:
            yql_struct_fields.append(f"{col.name}:{yql_type}?")
        else:
            yql_struct_fields.append(f"{col.name}:{yql_type}")

        select_fields.append(col.name)

    ydb_struct_type = ydb.StructType()
    for name, type_ in struct_members.items():
        ydb_struct_type.add_member(name, type_)

    data_type = ydb.ListType(ydb_struct_type)
    typed_data = ydb.TypedValue(data, data_type)

    yql_struct_def = ", ".join(yql_struct_fields)
    select_cols = ", ".join(select_fields)

    # Use explicit DECLARE as required for AS_TABLE with parameters
    # sa.text handles the named parameters.
    stmt = sa.text(
        f"""
        DECLARE :data AS List<Struct<{yql_struct_def}>>;
        {operation} INTO `{table.name}` SELECT {select_cols} FROM AS_TABLE(:data);
        """
    )

    connection.execute(stmt, {"data": typed_data})

class TestUpsertAsTable(TablesTest):
    __backend__ = True

    @classmethod
    def define_tables(cls, metadata):
        Table(
            "test_upsert_as_table",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("val", Integer),
        )

        Table(
            "test_generic_table",
            metadata,
            Column("id", Integer, primary_key=True),
            Column("name", String),
            Column("is_active", Boolean),
        )

    def test_upsert_as_table(self, connection):
        tb = self.tables.test_upsert_as_table

        data_value = [
            {"id": 1, "val": 10},
            {"id": 2, "val": None},
            {"id": 3, "val": 30},
        ]

        execute_as_table(connection, tb, data_value, operation="UPSERT")

        rows = connection.execute(sa.select(tb).order_by(tb.c.id)).fetchall()
        assert rows == [(1, 10), (2, None), (3, 30)]

    def test_insert_as_table(self, connection):
        tb = self.tables.test_upsert_as_table

        connection.execute(sa.delete(tb))

        data_value = [
            {"id": 4, "val": 40},
            {"id": 5, "val": None},
        ]

        execute_as_table(connection, tb, data_value, operation="INSERT")

        rows = connection.execute(sa.select(tb).order_by(tb.c.id)).fetchall()
        assert rows == [(4, 40), (5, None)]

    def test_generic_table(self, connection):
        tb = self.tables.test_generic_table

        data_value = [
            {"id": 1, "name": "Alice", "is_active": True},
            {"id": 2, "name": None, "is_active": False},
            {"id": 3, "name": "Bob", "is_active": None},
        ]

        execute_as_table(connection, tb, data_value, operation="UPSERT")

        rows = connection.execute(sa.select(tb).order_by(tb.c.id)).fetchall()
        assert rows == [
            (1, "Alice", True),
            (2, None, False),
            (3, "Bob", None),
        ]
