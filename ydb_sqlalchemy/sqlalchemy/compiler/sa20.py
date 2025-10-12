import sqlalchemy as sa
import ydb

from sqlalchemy.exc import CompileError
from sqlalchemy.sql import literal_column
from sqlalchemy.util.compat import inspect_getfullargspec

from .base import (
    BaseYqlCompiler,
    BaseYqlDDLCompiler,
    BaseYqlIdentifierPreparer,
    BaseYqlTypeCompiler,
)
from typing import Union


class YqlTypeCompiler(BaseYqlTypeCompiler):
    def visit_uuid(self, type_: sa.Uuid, **kw):
        return "UTF8"

    def get_ydb_type(
        self, type_: sa.types.TypeEngine, is_optional: bool
    ) -> Union[ydb.PrimitiveType, ydb.AbstractTypeBuilder]:
        if isinstance(type_, sa.TypeDecorator):
            type_ = type_.impl

        if isinstance(type_, sa.Uuid):
            ydb_type = ydb.PrimitiveType.Utf8
            if is_optional:
                return ydb.OptionalType(ydb_type)
            return ydb_type

        if isinstance(type_, sa.Double):
            ydb_type = ydb.PrimitiveType.Double
            if is_optional:
                return ydb.OptionalType(ydb_type)
            return ydb_type

        return super().get_ydb_type(type_, is_optional)


class YqlIdentifierPreparer(BaseYqlIdentifierPreparer):
    ...


class YqlCompiler(BaseYqlCompiler):
    _type_compiler_cls = YqlTypeCompiler

    def visit_json_getitem_op_binary(self, binary: sa.BinaryExpression, operator, **kw) -> str:
        json_field = self.process(binary.left, **kw)
        index = self.process(binary.right, **kw)
        return self._yson_convert_to(f"{json_field}[{index}]", binary.type)

    def visit_json_path_getitem_op_binary(self, binary: sa.BinaryExpression, operator, **kw) -> str:
        json_field = self.process(binary.left, **kw)
        path = self.process(binary.right, **kw)
        return self._yson_convert_to(f"Yson::YPath({json_field}, {path})", binary.type)

    def visit_regexp_match_op_binary(self, binary, operator, **kw):
        return self._generate_generic_binary(binary, " REGEXP ", **kw)

    def visit_not_regexp_match_op_binary(self, binary, operator, **kw):
        return self._generate_generic_binary(binary, " NOT REGEXP ", **kw)

    def visit_lambda(self, lambda_, **kw):
        func = lambda_.func
        spec = inspect_getfullargspec(func)

        if spec.varargs:
            raise CompileError("Lambdas with *args are not supported")
        if spec.varkw:
            raise CompileError("Lambdas with **kwargs are not supported")

        args = [literal_column("$" + arg) for arg in spec.args]
        text = f'({", ".join("$" + arg for arg in spec.args)}) -> ' f"{{ RETURN {self.process(func(*args), **kw)} ;}}"

        return text

    def _yson_convert_to(self, statement: str, target_type: sa.types.TypeEngine) -> str:
        type_name = target_type.compile(self.dialect)
        if isinstance(target_type, sa.Numeric) and not isinstance(target_type, (sa.Float, sa.Double)):
            # Since Decimal is stored in JSON either as String or as Float
            string_value = f"Yson::ConvertTo({statement}, Optional<String>, Yson::Options(true AS AutoConvert))"
            return f"CAST({string_value} AS Optional<{type_name}>)"
        return f"Yson::ConvertTo({statement}, Optional<{type_name}>)"

    def visit_insert(self, insert_stmt, **kw):
        # YDB does not support `INSERT ... SELECT FROM <target_table>`.
        # The target table should be excluded from the `FROM` clause
        # of the `SELECT` statement.
        if not insert_stmt.from_select:
            # `super().visit_insert` is not available in SA 1.3,
            # so we use `super(YqlCompiler, self).visit_insert` for compatibility.
            return super(YqlCompiler, self).visit_insert(insert_stmt, **kw)

        # We need to suppress the top-level table from being rendered in the
        # FROM clause of the SELECT.
        self.stack.append(
            {"from_clauses": [], "top_from_clause": None, "is_subquery": True}
        )

        try:
            preparer = self.preparer
            text = "INSERT INTO "
            text += preparer.format_table(insert_stmt.table)

            if insert_stmt.crud_cols:
                text += " ("
                text += ", ".join([
                    self.preparer.quote(c.name) for c in insert_stmt.crud_cols
                ])
                text += ")"

            select_text = self.process(insert_stmt.from_select, **kw)
            text += " " + select_text

            return text
        finally:
            self.stack.pop()

    def visit_upsert(self, insert_stmt, **kw):
        return self.visit_insert(insert_stmt, **kw).replace("INSERT", "UPSERT", 1)


class YqlDDLCompiler(BaseYqlDDLCompiler):
    ...
