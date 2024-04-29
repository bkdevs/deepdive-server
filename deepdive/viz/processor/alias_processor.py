from deepdive.schema import Binner, VizSpec
from deepdive.viz.processor.viz_spec_processor import VizSpecProcessor


class AliasProcessor(VizSpecProcessor):
    """
    Appends/replaces alias for aggregated, binned, and unparsed columns

    This is necessary so that the data we return to the client does not contain raw expressions, e.g
    rows:
    {
      AVG(`Annual_Salary`): 123,
      MAX(`Annual_Salary`): 234,
      strftime("%Y-%m-%d", created_at): 2023-05-01
    }

    If we do so - we need logic on the front-end to map between the column we're specifying and the aggregation applied.
    That's a lot of duplication, so instead, ensure that all unaggregated columns (where we can't match exactly with the name)
    have an alias, such that the data we return looks like:
    {
      Annual_Salary_avg : 123,
      Annual_Salary_max: 234,
      created_at_year: 2023-05-01
    }
    """

    def process(self, viz_spec: VizSpec) -> VizSpec:
        if not viz_spec:
            return None

        if viz_spec.x_axis and viz_spec.x_axis.unparsed and not viz_spec.x_axis.alias:
            viz_spec.x_axis.alias = "computed_x_axis"
        elif viz_spec.x_axis and viz_spec.x_axis.binner:
            viz_spec.x_axis.alias = viz_spec.x_axis.name + self._get_binner_suffix(
                viz_spec.x_axis.binner
            )

        num_unparsed = 0
        if viz_spec.y_axises:
            for y_axis in viz_spec.y_axises:
                if y_axis.unparsed and not y_axis.alias:
                    y_axis.alias = f"computed_column_{num_unparsed + 1}"
                    num_unparsed += 1
                elif y_axis.aggregation and not y_axis.unparsed:
                    name = "ROWS" if y_axis.name == "*" else y_axis.name
                    y_axis.alias = f"{y_axis.aggregation}_{name}"

        return viz_spec

    def _get_binner_suffix(self, binner: Binner) -> str:
        if binner.binner_type == "datetime":
            return f"_{binner.time_unit.upper()}"
        return "_bins"
