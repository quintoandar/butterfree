from butterfree.dataframe_service import IncrementalStrategy


class TestIncrementalStrategy:
    def test_from_milliseconds(self):
        # arrange
        incremental_strategy = IncrementalStrategy().from_milliseconds("ts")
        target_expression = "date(from_unixtime(ts/ 1000.0)) >= date('2020-01-01')"

        # act
        result_expression = incremental_strategy.get_expression(start_date="2020-01-01")

        # assert
        assert target_expression.split() == result_expression.split()

    def test_from_string(self):
        # arrange
        incremental_strategy = IncrementalStrategy().from_string(
            "dt", mask="dd/MM/yyyy"
        )
        target_expression = "date(to_date(dt, 'dd/MM/yyyy')) >= date('2020-01-01')"

        # act
        result_expression = incremental_strategy.get_expression(start_date="2020-01-01")

        # assert
        assert target_expression.split() == result_expression.split()

    def test_from_year_month_day_partitions(self):
        # arrange
        incremental_strategy = IncrementalStrategy().from_year_month_day_partitions(
            year_column="y", month_column="m", day_column="d"
        )
        target_expression = (
            "date(concat(string(y), "
            "'-', string(m), "
            "'-', string(d))) >= date('2020-01-01')"
        )

        # act
        result_expression = incremental_strategy.get_expression(start_date="2020-01-01")

        # assert
        assert target_expression.split() == result_expression.split()

    def test_get_expression_with_just_end_date(self):
        # arrange
        incremental_strategy = IncrementalStrategy(column="dt")
        target_expression = "date(dt) <= date('2020-01-01')"

        # act
        result_expression = incremental_strategy.get_expression(end_date="2020-01-01")

        # assert
        assert target_expression.split() == result_expression.split()

    def test_get_expression_with_start_and_end_date(self):
        # arrange
        incremental_strategy = IncrementalStrategy(column="dt")
        target_expression = (
            "date(dt) >= date('2019-12-30') and date(dt) <= date('2020-01-01')"
        )

        # act
        result_expression = incremental_strategy.get_expression(
            start_date="2019-12-30", end_date="2020-01-01"
        )

        # assert
        assert target_expression.split() == result_expression.split()
