from datetime import date
import sqlalchemy as sa

from . import YqlDialect, types


def test_casts():
    dialect = YqlDialect()
    expr = sa.literal_column("1/2")

    res_exprs = [
        sa.cast(expr, types.UInt32),
        sa.cast(expr, types.UInt64),
        sa.cast(expr, types.UInt8),
        sa.func.String.JoinFromList(
            sa.func.ListMap(sa.func.TOPFREQ(expr, 5), types.Lambda(lambda x: sa.cast(x, sa.Text))),
            ", ",
        ),
    ]

    strs = [str(res_expr.compile(dialect=dialect, compile_kwargs={"literal_binds": True})) for res_expr in res_exprs]

    assert strs == [
        "CAST(1/2 AS UInt32)",
        "CAST(1/2 AS UInt64)",
        "CAST(1/2 AS UInt8)",
        "String::JoinFromList(ListMap(TOPFREQ(1/2, 5), ($x) -> { RETURN CAST($x AS UTF8) ;}), ', ')",
    ]


def test_ydb_types():
    dialect = YqlDialect()

    query = sa.literal(date(1996, 11, 19))
    compiled = query.compile(dialect=dialect, compile_kwargs={"literal_binds": True})

    assert str(compiled) == "Date('1996-11-19')"


def test_struct_type_generation():
    dialect = YqlDialect()
    type_compiler = dialect.type_compiler

    # Test default (non-optional)
    struct_type = types.StructType({
        "id": sa.Integer,
        "val_int": sa.Integer,
    })
    ydb_type = type_compiler.get_ydb_type(struct_type, is_optional=False)
    # Keys are sorted
    assert str(ydb_type) == "Struct<id:Int64,val_int:Int64>"

    # Test optional
    struct_type_opt = types.StructType({
        "id": sa.Integer,
        "val_int": types.Optional(sa.Integer),
    })
    ydb_type_opt = type_compiler.get_ydb_type(struct_type_opt, is_optional=False)
    assert str(ydb_type_opt) == "Struct<id:Int64,val_int:Int64?>"


def test_types_compilation():
    dialect = YqlDialect()

    def compile_type(type_):
        return dialect.type_compiler.process(type_)

    assert compile_type(types.UInt64()) == "UInt64"
    assert compile_type(types.UInt32()) == "UInt32"
    assert compile_type(types.UInt16()) == "UInt16"
    assert compile_type(types.UInt8()) == "UInt8"

    assert compile_type(types.Int64()) == "Int64"
    assert compile_type(types.Int32()) == "Int32"
    assert compile_type(types.Int16()) == "Int32"
    assert compile_type(types.Int8()) == "Int8"

    assert compile_type(types.ListType(types.Int64())) == "List<Int64>"

    struct = types.StructType({"a": types.Int32(), "b": types.ListType(types.Int32())})
    # Ordered by key: a, b
    assert compile_type(struct) == "Struct<a:Int32,b:List<Int32>>"
