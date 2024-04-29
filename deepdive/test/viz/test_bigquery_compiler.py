from pypika import functions as fn
from pypika.terms import Field, LiteralValue

from deepdive.schema import Binner, VizSpec, XAxis
from deepdive.sql.parser.sql_tree import SqlTree
from deepdive.test.sql.sql_test_case import SqlTestCase, compile_viz_spec
from deepdive.viz.compiler.bigquery_complier import BigQueryCompiler
from deepdive.schema import DatabaseSchema, SqlDialect


DB_SCHEMA = DatabaseSchema(sql_dialect=SqlDialect.GOOGLE_SQL, tables=[])
COMPILER = BigQueryCompiler(DB_SCHEMA)


def compile_viz_spec(viz_spec):
    return COMPILER.compile(viz_spec)


class TestBigQueryCompiler(SqlTestCase):
    """
    Tests that we can compile a viz spec into a SqlTree
    """

    def test_select_datetime_x(self):
        self.assertTreeEquals(
            SqlTree(
                sql_dialect=SqlDialect.GOOGLE_SQL,
                select_terms=[
                    fn.Function("FORMAT_DATE", "%Y-%m-%d %H:%M:%S", Field("started_at"))
                ],
                groupby_terms=[
                    fn.Function("FORMAT_DATE", "%Y-%m-%d %H:%M:%S", Field("started_at"))
                ],
                from_term="customers",
            ),
            compile_viz_spec(
                VizSpec(
                    x_axis=XAxis(
                        name="started_at",
                        binner=Binner(binner_type="datetime", time_unit="second"),
                    ),
                    tables=["customers"],
                )
            ),
        )

    def test_select_datetime_x_alias(self):
        self.assertTreeEquals(
            SqlTree(
                sql_dialect=SqlDialect.GOOGLE_SQL,
                select_terms=[
                    fn.Function(
                        "FORMAT_DATE", "%Y-%m-%d %H:%M:%S", Field("started_at")
                    ).as_("started_at_seconds")
                ],
                groupby_terms=[Field("started_at_seconds")],
                from_term="customers",
            ),
            compile_viz_spec(
                VizSpec(
                    x_axis=XAxis(
                        name="started_at",
                        binner=Binner(binner_type="datetime", time_unit="second"),
                        alias="started_at_seconds",
                    ),
                    tables=["customers"],
                )
            ),
        )

    def test_select_datetime_x_minutes(self):
        self.assertTreeEquals(
            SqlTree(
                sql_dialect=SqlDialect.GOOGLE_SQL,
                select_terms=[
                    fn.Function("FORMAT_DATE", "%Y-%m-%d %H:%M", Field("started_at"))
                ],
                groupby_terms=[
                    fn.Function("FORMAT_DATE", "%Y-%m-%d %H:%M", Field("started_at"))
                ],
                from_term="customers",
            ),
            compile_viz_spec(
                VizSpec(
                    x_axis=XAxis(
                        name="started_at",
                        binner=Binner(binner_type="datetime", time_unit="minute"),
                    ),
                    tables=["customers"],
                )
            ),
        )

    def test_select_datetime_x_hours(self):
        self.assertTreeEquals(
            SqlTree(
                sql_dialect=SqlDialect.GOOGLE_SQL,
                select_terms=[
                    fn.Function("FORMAT_DATE", "%Y-%m-%d %H", Field("started_at"))
                ],
                groupby_terms=[
                    fn.Function("FORMAT_DATE", "%Y-%m-%d %H", Field("started_at"))
                ],
                from_term="customers",
            ),
            compile_viz_spec(
                VizSpec(
                    x_axis=XAxis(
                        name="started_at",
                        binner=Binner(binner_type="datetime", time_unit="hour"),
                    ),
                    tables=["customers"],
                )
            ),
        )

    def test_select_datetime_x_days(self):
        self.assertTreeEquals(
            SqlTree(
                sql_dialect=SqlDialect.GOOGLE_SQL,
                select_terms=[
                    fn.Function("FORMAT_DATE", "%Y-%m-%d", Field("started_at"))
                ],
                groupby_terms=[
                    fn.Function("FORMAT_DATE", "%Y-%m-%d", Field("started_at"))
                ],
                from_term="customers",
            ),
            compile_viz_spec(
                VizSpec(
                    x_axis=XAxis(
                        name="started_at",
                        binner=Binner(binner_type="datetime", time_unit="day"),
                    ),
                    tables=["customers"],
                )
            ),
        )

    def test_select_datetime_x_weeks(self):
        self.assertTreeEquals(
            SqlTree(
                sql_dialect=SqlDialect.GOOGLE_SQL,
                select_terms=[
                    fn.Function(
                        "DATE_TRUNC",
                        Field("started_at"),
                        LiteralValue("WEEK"),
                    )
                ],
                groupby_terms=[
                    fn.Function(
                        "DATE_TRUNC",
                        Field("started_at"),
                        LiteralValue("WEEK"),
                    )
                ],
                from_term="customers",
            ),
            compile_viz_spec(
                VizSpec(
                    x_axis=XAxis(
                        name="started_at",
                        binner=Binner(binner_type="datetime", time_unit="week"),
                    ),
                    tables=["customers"],
                )
            ),
        )

    def test_select_month_of_year(self):
        self.assertTreeEquals(
            SqlTree(
                sql_dialect=SqlDialect.GOOGLE_SQL,
                select_terms=[fn.Extract("MONTH", Field("started_at"))],
                groupby_terms=[fn.Extract("MONTH", Field("started_at"))],
                from_term="customers",
            ),
            compile_viz_spec(
                VizSpec(
                    x_axis=XAxis(
                        name="started_at",
                        binner=Binner(
                            binner_type="datetime", time_unit="month_of_year"
                        ),
                    ),
                    tables=["customers"],
                )
            ),
        )

    def test_select_x_axis_domain_between_computed_alias(self):
        self.assertTreeEquals(
            SqlTree(
                sql_dialect=SqlDialect.GOOGLE_SQL,
                select_terms=[
                    fn.Function("FORMAT_DATE", "%Y", Field("started_at")).as_(
                        "started_at_year"
                    )
                ],
                groupby_terms=[Field("started_at_year")],
                from_term="customers",
                where_term=(
                    fn.Function("FORMAT_DATE", "%Y", Field("started_at"))[10:20]
                ),
            ),
            compile_viz_spec(
                VizSpec(
                    x_axis=XAxis(
                        name="started_at",
                        alias="started_at_year",
                        binner=Binner(binner_type="datetime", time_unit="year"),
                        domain=[10, 20],
                    ),
                    tables=["customers"],
                )
            ),
        )
