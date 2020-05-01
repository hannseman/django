import warnings

from django.db import NotSupportedError
from django.db.backends.utils import names_digest, split_identifier
from django.db.models.expressions import BaseExpression, F
from django.db.models.query_utils import Q
from django.db.models.sql import Query

__all__ = ['Index']


class Index:
    suffix = 'idx'
    # The max length of the name of the index (restricted to 30 for
    # cross-database compatibility with Oracle)
    max_name_length = 30

    def __init__(
        self,
        *expressions,
        fields=(),
        name=None,
        db_tablespace=None,
        opclasses=(),
        condition=None,
        include=None,
    ):
        if opclasses and not name:
            raise ValueError('An index must be named to use opclasses.')
        if not isinstance(condition, (type(None), Q)):
            raise ValueError('Index.condition must be a Q instance.')
        if condition and not name:
            raise ValueError('An index must be named to use condition.')
        if not isinstance(fields, (list, tuple)):
            raise ValueError('Index.fields must be a list or tuple.')
        if not isinstance(opclasses, (list, tuple)):
            raise ValueError('Index.opclasses must be a list or tuple.')
        if opclasses and len(fields) != len(opclasses):
            raise ValueError('Index.fields and Index.opclasses must have the same number of elements.')
        if include and not name:
            raise ValueError('A covering index must be named.')
        if not expressions and not fields:
            raise ValueError('At least one field is required to define an index.')
        if expressions and fields:
            raise ValueError("'fields' cannot be used together with expressions.")
        if expressions and not name:
            raise ValueError('Index.name needs to be set when passed expressions.')

        self.fields = list(fields)
        self.expressions = list(expressions)
        self.name = name or ''
        self.db_tablespace = db_tablespace
        self.opclasses = opclasses
        self.condition = condition
        self.include = tuple(include) if include else ()

        self.field_name_expressions = [
            field_name
            for field_name in self.fields
            if isinstance(field_name, str)
        ]
        self.field_names = [
            field_name.lstrip('-')
            for field_name in self.field_name_expressions
        ]
        self.expressions = [
            field for field in self.fields if isinstance(field, (BaseExpression, F))
        ]

        self.field_names = [
            field_name.lstrip('-')
            for field_name in self.fields
        ]

        self._expressions = [
            F(expression) if isinstance(expression, str) else expression
            for expression in self.expressions
        ]

    def _get_condition_sql(self, model, schema_editor):
        if self.condition is None:
            return None
        query = Query(model=model, alias_cols=False)
        where = query.build_where(self.condition)
        compiler = query.get_compiler(connection=schema_editor.connection)
        sql, params = where.as_sql(compiler, schema_editor.connection)
        return sql % tuple(schema_editor.quote_value(p) for p in params)

    def _get_ordered_expression(self, model, schema_editor, using, expression):
        """
        Return the SQL for supplied expression and an optional ordering.
        """
        suffix = ''
        query = Query(model, alias_cols=False)
        connection = schema_editor.connection
        compiler = connection.ops.compiler('SQLCompiler')(query, connection, using)
        expression = expression.resolve_expression(query)
        # Check if expression is ordered and extract the ordering as a suffix
        if expression.ordered:
            suffix = 'DESC' if expression.descending else 'ASC'
            expression = expression.get_source_expressions()[0]
        expression_sql, params = compiler.compile(expression)
        params = tuple(map(schema_editor.quote_value, params))
        # Wrap expression in parentheses
        expression_sql = '(%s)' % expression_sql
        return expression_sql % params, suffix

    def _get_expression_orders(self, model, schema_editor, using):
        expressions = []
        for expression in self._expressions:
            expression_sql, order = self._get_ordered_expression(
                model, schema_editor, using, expression
            )
            expressions.append((expression_sql, order))
        return expressions

    def _get_ordered_field(self, model, field_name):
        """
        Return the field for supplied name and an optional ordering
        """
        field_name, order = (
            (field_name[1:], 'DESC') if field_name.startswith('-') else (field_name, '')
        )
        field = model._meta.get_field(field_name)
        return field, order

    def _get_field_orders(self, model):
        """
        Return fields together with their ordering
        """
        fields = []
        for field in self.fields:
            field, order = self._get_ordered_field(model, field)
            fields.append((field, order))
        return fields

    def _validate_supports_expression_indexes(self, schema_editor):
        """
        Validate that database supports supplied expressions
        """
        connection = schema_editor.connection
        supports_expression_indexes = connection.features.supports_expression_indexes
        for column_expression in self._expressions:
            if (
                not supports_expression_indexes and
                hasattr(column_expression, 'flatten') and
                any(
                    isinstance(expr, (BaseExpression, F))
                    for expr in column_expression.flatten()
                )
            ):
                raise NotSupportedError(
                    (
                        'Not creating expression index:\n'
                        '   {expression}\n'
                        'Expression indexes are not supported on {vendor}.'
                    ).format(
                        expression=column_expression, vendor=connection.display_name
                    )
                )

    def create_sql(self, model, schema_editor, using="", **kwargs):
        try:
            self._validate_supports_expression_indexes(schema_editor)
        except NotSupportedError as e:
            # While it's an error on platforms w/o support for expression
            # indexes to create one, the presence of an index is often not
            # required. Thus, we're raising a warning instead of blowing up.
            # That also seems to be the only way to allow 3rd party packages
            # using expression indexes while supporting all databases for
            # everything else.
            warnings.warn(str(e), RuntimeWarning)
            return None
        col_suffixes = fields = None
        if self.fields:
            field_orders = self._get_field_orders(model)
            fields = [field[0] for field in field_orders]
            col_suffixes = [order[1] for order in field_orders]
        include = [model._meta.get_field(field_name).column for field_name in self.include]
        condition = self._get_condition_sql(model, schema_editor)
        return schema_editor._create_index_sql(
            model, fields=fields, name=self.name, using=using, db_tablespace=self.db_tablespace,
            col_suffixes=col_suffixes, opclasses=self.opclasses, condition=condition,
            include=include, expressions=self._expressions, **kwargs,
        )

    def remove_sql(self, model, schema_editor, **kwargs):
        return schema_editor._delete_index_sql(model, self.name, **kwargs)

    def deconstruct(self):
        path = '%s.%s' % (self.__class__.__module__, self.__class__.__name__)
        path = path.replace('django.db.models.indexes', 'django.db.models')
        # TODO: expressions
        kwargs = {'fields': self.fields, 'name': self.name}
        if self.db_tablespace is not None:
            kwargs['db_tablespace'] = self.db_tablespace
        if self.opclasses:
            kwargs['opclasses'] = self.opclasses
        if self.condition:
            kwargs['condition'] = self.condition
        if self.include:
            kwargs['include'] = self.include
        return (path, (), kwargs)

    def clone(self):
        """Create a copy of this Index."""
        _, _, kwargs = self.deconstruct()
        return self.__class__(**kwargs)

    def set_name_with_model(self, model):
        """
        Generate a unique name for the index.

        The name is divided into 3 parts - table name (12 chars), field name
        (8 chars) and unique hash + suffix (10 chars). Each part is made to
        fit its size by truncating the excess length.
        """
        _, table_name = split_identifier(model._meta.db_table)
        column_names = [model._meta.get_field(field_name).column for field_name in self.field_names]
        column_names_with_order = [
            (('-%s' if field.startswith('-') else '%s') % column_name)
            for column_name, field in zip(column_names, self.fields
            )
        ]
        # The length of the parts of the name is based on the default max
        # length of 30 characters.
        hash_data = [table_name] + column_names_with_order + [self.suffix]
        self.name = '%s_%s_%s' % (
            table_name[:11],
            column_names[0][:7],
            '%s_%s' % (names_digest(*hash_data, length=6), self.suffix),
        )
        assert len(self.name) <= self.max_name_length, (
            'Index too long for multiple database support. Is self.suffix '
            'longer than 3 characters?'
        )
        if self.name[0] == '_' or self.name[0].isdigit():
            self.name = 'D%s' % self.name[1:]

    def __repr__(self):
        return "<%s: fields='%s'%s%s%s>" % (
            self.__class__.__name__, ', '.join(map(str, self.fields)),
            '' if self.condition is None else ' condition=%s' % self.condition,
            '' if not self.include else " include='%s'" % ', '.join(self.include),
            '' if not self.opclasses else " opclasses='%s'" % ', '.join(self.opclasses),
        )

    def __eq__(self, other):
        if self.__class__ == other.__class__:
            return self.deconstruct() == other.deconstruct()
        return NotImplemented
