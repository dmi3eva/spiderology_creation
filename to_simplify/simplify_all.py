from configure import *
from to_simplify.simplifier.changer import *


def simplify(name: str, tag: Tag, queries_should_be_equal: bool = False):
    markup_path = os.path.join(MARKUP_PATH, f"{name}.csv")
    spider_samples_folder = SPIDER_DB_PATH
    source_tables_path = SPIDER_PROCESSED_DB_SCHEMES
    db_folders_path = SPIDER_PROCESSED_DB_FOLDERS
    target_folder = SIMPLY_SET_PATH
    changer = Changer(markup_path, spider_samples_folder, source_tables_path, name, tag,
                      db_folders_path, target_folder, queries_should_be_equal=queries_should_be_equal)
    changer.simplify_csv()


if __name__ == "__main__":
    simplify("binary", Tag.BINARY, False)
    simplify("aggregation_in_column", Tag.AGGREGATION_IN_COLUMN,False)
    simplify("math_minus", Tag.MATH_MINUS, False)
    simplify("math_plus", Tag.MATH_PLUS, False)
    simplify("column_overlapping", Tag.COLUMN_OVERLAPPING, False)
    simplify("datetime", Tag.DATETIME, False)
    simplify("values_without_columns", Tag.VALUES_WITHOUT_COLUMNS, False)

    simplify("group_by", Tag.GROUP_BY, False)
    simplify("multi_aggregation", Tag.MULTI_AGGREGATION, False)
    simplify("hidden_avg", Tag.HIDDEN_AVG, False)
    simplify("hidden_sum", Tag.HIDDEN_SUM, False)
    simplify("hidden_min", Tag.HIDDEN_MIN, False)
    simplify("hidden_max", Tag.HIDDEN_MAX, False)

    simplify("how_many", Tag.HOW_MANY, False)
    simplify("non_strict_inequality", Tag.NON_STRICT_INEQUALITY, False)
    simplify("and_in_nl", Tag.AND_IN_NL, False)
    simplify("or_in_nl", Tag.OR_IN_NL, False)
    simplify("and_in_sql_but_not_in_nl", Tag.AND_IN_SQL_BUT_NOT_IN_NL, False)
    simplify("and_with_or_in_nl", Tag.AND_WITH_OR_IN_NL, False)
    simplify("any_some_in_nl", Tag.ANY_SOME_IN_NL, False)
    simplify("changed_logical_connectives", Tag.CHANGED_LOGICAL_CONNECTIVES, False)

    simplify("like_in_sql", Tag.LIKE_IN_SQL, False)
    simplify("having_in_sql", Tag.HAVING_IN_SQL, False)
    simplify("order_by_count", Tag.ORDER_BY_COUNT, False)
    simplify("synonyms", Tag.SYNONYMS, False)
    simplify("order_by", Tag.ORDER_BY, False)
    simplify("where_mono", Tag.WHERE_MONO, False)
    simplify("aggregation_mono", Tag.AGGREGATION_MONO, False)
    simplify("negation", Tag.NEGATION, False)
    simplify("negation_with_any_all", Tag.NEGATION_WITH_ANY_ALL, False)
