TIME_UNIT_TO_FORMAT_STRING = {
    "second": "%Y-%m-%d %H:%M:%S",
    "minute": "%Y-%m-%d %H:%M",
    "hour": "%Y-%m-%d %H",
    "hour_of_day": "%H",
    "day": "%Y-%m-%d",
    "day_of_week": "%w",
    "day_of_month": "%d",
    "week_of_year_long": "%Y-%W",
    "week_of_year": "%W",
    "month": "%Y-%m",
    "year": "%Y",
}
FORMAT_STRING_TO_TIME_UNIT = {v: k for k, v in TIME_UNIT_TO_FORMAT_STRING.items()}
