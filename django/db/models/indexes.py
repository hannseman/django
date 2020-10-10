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
        if not expressions and not fields:
            raise ValueError('At least one field or expression is required to define an index.')
        if fields and not all(isinstance(field, str) for field in fields):
            raise ValueError('Index.fields must only contain strings.')
        if expressions and fields:
            raise ValueError("'fields' cannot be used together with expressions.")
        if expressions and not name:
            raise ValueError('Index.name needs to be set when passed expressions.')
        if opclasses:
            if expressions and len(expressions) != len(opclasses):
                raise ValueError('Index arguments and Index.opclasses must have the same number of elements.')
            elif fields and len(fields) != len(opclasses):
                raise ValueError('Index.fields and Index.opclasses must have the same number of elements.')
        if include and not name:
            raise ValueError('A covering index must be named.')
        if not isinstance(include, (type(None), list, tuple)):
            raise ValueError('Index.include must be a list or tuple.')

        self.fields = list(fields)
        self.name = name or ''
        self.db_tablespace = db_tablespace
        self.opclasses = opclasses
        self.condition = condition
        self.include = tuple(include) if include else ()

        self.field_names = [
            field_name.lstrip('-')
            for field_name in self.fields
        ]
        # TODO: Do we need to keep original args for `deconstruct()` or can we convert strings to `F()`?
        # TODO: Should we check supports_expression_indexes_on_columns?
        self.expressions = tuple(
            F(expression) if isinstance(expression, str) else expression
            for expression in expressions
        )

    def _get_condition_sql(self, model, schema_editor):
        if self.condition is None:
            return None
        query = Query(model=model, alias_cols=False)
        where = query.build_where(self.condition)
        compiler = query.get_compiler(connection=schema_editor.connection)
        sql, params = where.as_sql(compiler, schema_editor.connection)
        return sql % tuple(schema_editor.quote_value(p) for p in params)

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
            fields.append(self._get_ordered_field(model, field))
        return fields

    def _validate_supports_expression_indexes(self, schema_editor):
        """
        Validate that database supports supplied expressions
        """
        connection = schema_editor.connection
        supports_expression_indexes = connection.features.supports_expression_indexes
        for column_expression in self.expressions:
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
            fields, col_suffixes = zip(*field_orders)
        include = [model._meta.get_field(field_name).column for field_name in self.include]
        condition = self._get_condition_sql(model, schema_editor)
        return schema_editor._create_index_sql(
            model, fields=fields, name=self.name, using=using, db_tablespace=self.db_tablespace,
            col_suffixes=col_suffixes, opclasses=self.opclasses, condition=condition,
            include=include, expressions=self.expressions, **kwargs,
        )

    def remove_sql(self, model, schema_editor, **kwargs):
        return schema_editor._delete_index_sql(model, self.name, **kwargs)

    def deconstruct(self):
        path = '%s.%s' % (self.__class__.__module__, self.__class__.__name__)
        path = path.replace('django.db.models.indexes', 'django.db.models')
        kwargs = {'name': self.name}
        if self.fields:
            kwargs['fields'] = self.fields
        if self.db_tablespace is not None:
            kwargs['db_tablespace'] = self.db_tablespace
        if self.opclasses:
            kwargs['opclasses'] = self.opclasses
        if self.condition:
            kwargs['condition'] = self.condition
        if self.include:
            kwargs['include'] = self.include
        return (path, self.expressions, kwargs)

    def clone(self):
        """Create a copy of this Index."""
        _, args, kwargs = self.deconstruct()
        return self.__class__(*args, **kwargs)

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
            for column_name, field in zip(column_names, self.fields)
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
        return "<%s:%s%s%s%s%s>" % (
            self.__class__.__name__,
            '' if not self.fields else " fields='%s'" % ', '.join(self.fields),
            '' if not self.expressions else " expressions='%s'" % ', '.join(map(str, self.expressions)),
            '' if self.condition is None else ' condition=%s' % self.condition,
            '' if not self.include else " include='%s'" % ', '.join(self.include),
            '' if not self.opclasses else " opclasses='%s'" % ', '.join(self.opclasses),
        )

    def __eq__(self, other):
        if self.__class__ == other.__class__:
            return self.deconstruct() == other.deconstruct()
        return NotImplemented
