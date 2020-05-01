class a:
    def b(self):
        return Statement(
            sql_create_index,
            table=Table(table, self.quote_name),
            name=IndexName(table, columns, suffix, create_index_name),
            using=using,
            columns=(
                self._index_columns(table, columns, col_suffixes, opclasses)
                if not columns
                else Expressions(
                    table, expressions, compiler, self.quote_value, opclasses
                )
            ),
            extra=tablespace_sql,
            condition=(" WHERE " + condition) if condition else "",
        )
