from atomic import *
from configure import *

# Join

DB_WITH_PROBLEMS = [
    # "icfp_1",  # No scheme
    # "voter_1",  # No scheme
    # "world_1",  # No scheme
    # "chinook_1",  # No scheme
    # "small_bank_1",  # No scheme
    # "restaurant_1",  # No aliases in query
    # "baseball_1",  # Very long
    # "mountain_photos",  # Partly without aliases
    # "activity_1",  # Partly without aliases
    # "flight_4",  # No scheme
    # "flight_2",  # Or in JOIN
    # "wta_1",
    # "tvshow",  # 18_49
    # "orchestra",  # Brackets in the name of column
    # "dog_kennels",  # No scheme
    # "twitter_1",
    # "soccer_1",  # Very long
    # "epinions_1",   # No scheme
    # "company_1"  # No scheme
]

SAMPLES_WITH_PROBLEMS = [
    # "s4697",  # Not ordered joins
    # "s4698",  # Not ordered joins
    # "s5778",  # ON-absense
    # "s5779",  # ON-absense
    # "s5933",
    # "s5934",
    # "s5963",
    # "s5964",
    # "s5965",
    # "s5966",
    # "s6514",  # Something with value
    # "d0505",
    # "d0900",  # JOINS-01, network_1
    # "d0901",
    # "d0944",  # No scheme
    # "d1018",  # No table song
    # "s0316",
    # "s0773",
    # "s0774",
    # "s1468",
    # "s6513",  # line_stripped.index("values(") + 7 JOIN-1, scientist_1
    # "d1019",  # JOIN-1, singer, near FROM
    # "d1020",  # JOIN-1, singer, near FROM
    # "d1021",  # JOIN-1, singer, near FROM
    # "d1022",
    # "d1023",
    # "d1024",
    # "d1025",
    # "s1469",  # JOIN-1, college_2, if toks[idx] == '(':
    # "s2110",  # JOIN-1, cre_Doc_Control_Systems, if toks[idx] == '(':
    # "s2111",  # JOIN-1, cre_Doc_Control_Systems, if toks[idx] == '(':
    # "s2115",   # JOIN-1, cre_Doc_Control_Systems, if toks[idx] == '(':
    # "s2116",  # JOIN-1, cre_Doc_Control_Systems, if toks[idx] == '(':
    # "s2117",  # JOIN-1, cre_Doc_Control_Systems, if toks[idx] == '(':
    # "s2119",  # JOIN-1, cre_Doc_Control_Systems, if toks[idx] == '(':
    # "s2121",  # JOIN-1, cre_Doc_Control_Systems, if toks[idx] == '(':
    # "s2122",  # JOIN-1, college_2, if toks[idx] == '(':
    # "s2127",  # JOIN-1, cre_Doc_Control_Systems, if toks[idx] == '(':
    # "s2128",  # JOIN-1, cre_Doc_Control_Systems, if toks[idx] == '(':
    # "s2129",  # JOIN-1, cre_Doc_Control_Systems, if toks[idx] == '(':
    # "s2207",  # JOIN-1, formula_1, KeyError
    # "s2208",  # JOIN-1, formula_1, KeyError
    # "s2209",  # JOIN-1, formula_1, KeyError
    # "s2210",  # JOIN-1, formula_1, KeyError
    # "s2309",  # JOIN-1, perpretator, KeyError hometown
    # "s2310",  # JOIN-1, perpretator, KeyError hometown
    # "s2309",  # JOIN-1, perpretator, KeyError hometown
    # "s2309",  # JOIN-1, perpretator, KeyError hometown
    # "s2209",  # JOIN-1, formula_1, KeyError
    # "s2311",  # perpetrator
    # "s2312",  # perpetrator
    # "s2313",  # perpetrator
    # "s2314",  # perpetrator
    # "s2318",  # perpetrator
    # "s2322",  # perpetrator
    # "s2360",  # csu_1
    # "s2361",  # csu_1
    # "s2368",  # csu_1
    # "s2369",  # csu_1
    # "s2370",  # csu_1
    # "s2371",  # csu_1
    # "s2374",  # csu_1
    # "s2375",  # csu_1
    # "s2376",  # csu_1
    # "s2377",  # csu_1
    # "s2460",  # movie_1
    # "s2461",  # movie_1
    # "s2758",  # election
    # "s2759",  # election
    # "s2760",  # election
    # "s2761",  # election
    # "s2762",  # election
    # "s2763",  # election
    # "s2764",  # election
    # "s2765",  # election
    # "s2766",  # election
    # "s2767",  # election
    # "s2768",  # election
    # "s2769",  # election
    # "s2770",  # election
    # "s2771",  # election
    # "s2772",  # election
    # "s2773",  # election
    # "s2774",  # election
    # "s2775",  # election
    # "s2776",  # election
    # "s2777",  # election
    # "s2786",  # election
    # "s2787",  # election
    # "s2790",  # election
    # "s2791",  # election
    # "s2798",  # election
    # "s2799",  # election
    # "s2800",  # election
    # "s2801",  # election
    # "s3108",  # behavior_monitoring
    # "s3117",  # behavior_monitoring
    # "s3125",  # behavior_monitoring
    # "s3126",  # assets_maintenance
    # "s3127",  # assets_maintenance
    # "s3128",  # assets_maintenance
    # "s3131",  # assets_maintenance
    # "s3153",  # assets_maintenance
    # "s3173",  # college_1
    # "s3174",  # college_1
    # "s3205",  # college_1
    # "s3206",  # college_1
    # "s3233",  # college_1
    # "s3234",  # college_1
    # "s3267",  # college_1
    # "s3268",  # college_1
    # "s3465",  # hr_1
    # "s3466",  # hr_1
    # "s3525",  # hr_1
    # "s3526",  # hr_1
    # "s3595",  # music_1
    # "s3596",  # music_1
    # "s3850",  # insurance_policies
    # "s3851",  # insurance_policies
    # "s3904",  # hospital_1
    # "s3905",  #
    # "s4845",  # local_govt_and_lot
    # "s4850",  # local_govt_and_lot
    # "s4994",  # soccer_2
    # "s4995",  # soccer_2
    # "s5158",  # cre_Drama_Workshop_Groups
    # "s5159",  # cre_Drama_Workshop_Groups
    # "s5210",  # music_2
    # "s5211",  # music_2
    # "s5212",  # music_2
    # "s5213",  # music_2
    # "s5222",  # music_2
    # "s5223",  # music_2
    # "s5224",  # music_2
    # "s5225",  # music_2
    # "s5226",  # music_2
    # "s5227",  # music_2
    # "s5228",  # music_2
    # "s5229",  # music_2
    # "s5244",  # music_2
    # "s5245",  # music_2
    # "s5246",  # music_2
    # "s5247",  # music_2
    # "s5248",  # music_2
    # "s5249",  # music_2
    # "s5250",  # music_2
    # "s5251",  # music_2
    # "s5254",  # music_2
    # "s5255",  # music_2
    # "s5260",  # music_2
    # "s5261",  # music_2
    # "s5318",  # manufactory_1
    # "s5319",  # manufactory_1
    # "s5338",  # manufactory_1
    # "s5339",  # manufactory_1
    # "s5486",  # voter_2
    # "s5487",  # voter_2
    # "s5488",  # voter_2
    # "s5489",  # voter_2
    # "s5490",  # voter_2
    # "s5491",  # voter_2
    # "s5492",  # voter_2
    # "s5493",  # voter_2
    # "s5496",  # voter_2
    # "s5497",  # voter_2
    # "s5498",  # voter_2
    # "s5499",  # voter_2
    # "s5640",  # railway
    # "s5641",  # railway
    # "s5642",  # railway
    # "s5643",  # railway
    # "s5756",  # dorm_1
    # "s5757",  # dorm_1
    # "s6499",  # scientist_1
    # "s6500",  # scientist_1
    # "s6503",  # scientist_1
    # "s6504",  # scientist_1
    # "s6955",  # architecture
    # "o0556"  # geo
]

files = {
    "from_4_to_3": {
        "source_scheme_path": SPIDER_PROCESSED_DB_SCHEMES,
        "source_db_folder": SPIDER_PROCESSED_DB_FOLDERS,
        "source_db_path": SPIDER_PROCESSED_DB_PATH,
        "db_with_problems": DB_WITH_PROBLEMS,
        "samples_with_problems": SAMPLES_WITH_PROBLEMS,
        "simply_set_path": SIMPLY_SET_PATH,
        "test_set_path": os.path.join(TEST_SET_PATH, "joins", "joins_04.json")
    },
    "from_4_to_2": {
        "source_scheme_path": os.path.join(SIMPLY_SET_PATH, "tables", "joins_from_4_to_3", "joins_from_4_to_3.json"),
        "source_db_folder": os.path.join(SIMPLY_SET_PATH, "dbs", "joins_from_4_to_3"),
        "source_db_path": os.path.join(SIMPLY_SET_PATH, "dbs", "joins_from_4_to_3"),
        "db_with_problems": DB_WITH_PROBLEMS,
        "samples_with_problems": SAMPLES_WITH_PROBLEMS,
        "simply_set_path": SIMPLY_SET_PATH,
        "test_set_path": os.path.join(SIMPLY_SET_PATH, "samples", "joins_from_4_to_3", "joins_from_4_to_3.json")
    },
    "from_4_to_1": {
        "source_scheme_path": os.path.join(SIMPLY_SET_PATH, "tables", "joins_from_4_to_2", "joins_from_4_to_2.json"),
        "source_db_folder": os.path.join(SIMPLY_SET_PATH, "dbs", "joins_from_4_to_2"),
        "source_db_path": os.path.join(SIMPLY_SET_PATH, "dbs", "joins_from_4_to_2"),
        "db_with_problems": DB_WITH_PROBLEMS,
        "samples_with_problems": SAMPLES_WITH_PROBLEMS,
        "simply_set_path": SIMPLY_SET_PATH,
        "test_set_path": os.path.join(SIMPLY_SET_PATH, "samples", "joins_from_4_to_2", "joins_from_4_to_2.json")
    },
    "from_4_to_0": {
        "source_scheme_path": os.path.join(SIMPLY_SET_PATH, "tables", "joins_from_4_to_1", "joins_from_4_to_1.json"),
        "source_db_folder": os.path.join(SIMPLY_SET_PATH, "dbs", "joins_from_4_to_1"),
        "source_db_path": os.path.join(SIMPLY_SET_PATH, "dbs", "joins_from_4_to_1"),
        "db_with_problems": DB_WITH_PROBLEMS,
        "samples_with_problems": SAMPLES_WITH_PROBLEMS,
        "simply_set_path": SIMPLY_SET_PATH,
        "test_set_path": os.path.join(SIMPLY_SET_PATH, "samples", "joins_from_4_to_1", "joins_from_4_to_1.json")
    },
    "from_3_to_2": {
        "source_scheme_path": SPIDER_PROCESSED_DB_SCHEMES,
        "source_db_folder": SPIDER_PROCESSED_DB_FOLDERS,
        "source_db_path": SPIDER_PROCESSED_DB_PATH,
        "db_with_problems": DB_WITH_PROBLEMS,
        "samples_with_problems": SAMPLES_WITH_PROBLEMS,
        "simply_set_path": SIMPLY_SET_PATH,
        "test_set_path": os.path.join(TEST_SET_PATH, "joins", "joins_03.json")
    },
    "from_3_to_1": {
        "source_scheme_path": os.path.join(SIMPLY_SET_PATH, "tables", "joins_from_3_to_2", "joins_from_3_to_2.json"),
        "source_db_folder": os.path.join(SIMPLY_SET_PATH, "dbs", "joins_from_3_to_2"),
        "source_db_path": os.path.join(SIMPLY_SET_PATH, "dbs", "joins_from_3_to_2"),
        "db_with_problems": DB_WITH_PROBLEMS,
        "samples_with_problems": SAMPLES_WITH_PROBLEMS,
        "simply_set_path": SIMPLY_SET_PATH,
        "test_set_path": os.path.join(SIMPLY_SET_PATH, "samples", "joins_from_3_to_2", "joins_from_3_to_2.json")
    },
    "from_3_to_0": {
        "source_scheme_path": os.path.join(SIMPLY_SET_PATH, "tables", "joins_from_3_to_1", "joins_from_3_to_1.json"),
        "source_db_folder": os.path.join(SIMPLY_SET_PATH, "dbs", "joins_from_3_to_1"),
        "source_db_path": os.path.join(SIMPLY_SET_PATH, "dbs", "joins_from_3_to_1"),
        "db_with_problems": DB_WITH_PROBLEMS,
        "samples_with_problems": SAMPLES_WITH_PROBLEMS,
        "simply_set_path": SIMPLY_SET_PATH,
        "test_set_path": os.path.join(SIMPLY_SET_PATH, "samples", "joins_from_3_to_1", "joins_from_3_to_1.json")
    },
    "from_2_to_1": {
        "source_scheme_path": SPIDER_PROCESSED_DB_SCHEMES,
        "source_db_folder": SPIDER_PROCESSED_DB_FOLDERS,
        "source_db_path": SPIDER_PROCESSED_DB_PATH,
        "db_with_problems": DB_WITH_PROBLEMS,
        "samples_with_problems": SAMPLES_WITH_PROBLEMS,
        "simply_set_path": SIMPLY_SET_PATH,
        "test_set_path": os.path.join(TEST_SET_PATH, "joins", "joins_02.json")
    },
    "from_2_to_0": {
        "source_scheme_path": os.path.join(SIMPLY_SET_PATH, "tables", "joins_from_2_to_1", "joins_from_2_to_1.json"),
        "source_db_folder": os.path.join(SIMPLY_SET_PATH, "dbs", "joins_from_2_to_1"),
        "source_db_path": os.path.join(SIMPLY_SET_PATH, "dbs", "joins_from_2_to_1"),
        "db_with_problems": DB_WITH_PROBLEMS,
        "samples_with_problems": SAMPLES_WITH_PROBLEMS,
        "simply_set_path": SIMPLY_SET_PATH,
        "test_set_path": os.path.join(SIMPLY_SET_PATH, "samples", "joins_from_2_to_1", "joins_from_2_to_1.json")
    },
    "from_1_to_0": {
        "source_scheme_path": SPIDER_PROCESSED_DB_SCHEMES,
        "source_db_folder": SPIDER_PROCESSED_DB_FOLDERS,
        "source_db_path": SPIDER_PROCESSED_DB_PATH,
        "db_with_problems": DB_WITH_PROBLEMS,
        "samples_with_problems": SAMPLES_WITH_PROBLEMS,
        "simply_set_path": SIMPLY_SET_PATH,
        "test_set_path": os.path.join(TEST_SET_PATH, "joins", "joins_01.json")
    }
}

join_tags = {
    "from_4_to_3": [None, Tag._DEJOINED_TO_3, Tag._JOIN_4, Tag._JOIN_3, Tag.DEJOIN_MINUS_1, Tag.DEJOIN_TOTAL],
    "from_4_to_2": [Tag._DEJOINED_FROM_4, Tag._DEJOINED_TO_2, Tag._JOIN_3, Tag._JOIN_2, None, None],
    "from_4_to_1": [Tag._DEJOINED_FROM_4, Tag._DEJOINED_TO_1, Tag._JOIN_2, Tag._JOIN_1, None, None],
    "from_4_to_0": [Tag._DEJOINED_FROM_4, Tag._DEJOINED_TO_0, Tag._JOIN_1, Tag._JOIN_0, None, Tag.DEJOIN_TOTAL],
    "from_3_to_2": [None, Tag._DEJOINED_TO_2, Tag._JOIN_3, Tag._JOIN_2, Tag.DEJOIN_MINUS_1, Tag.DEJOIN_TOTAL],
    "from_3_to_1": [Tag._DEJOINED_FROM_3, Tag._DEJOINED_TO_1, Tag._JOIN_2, Tag._JOIN_1, None, None],
    "from_3_to_0": [Tag._DEJOINED_FROM_3, Tag._DEJOINED_TO_0, Tag._JOIN_1, Tag._JOIN_0, None, Tag.DEJOIN_TOTAL],
    "from_2_to_1": [None, Tag._DEJOINED_TO_1, Tag._JOIN_2, Tag._JOIN_1, Tag.DEJOIN_MINUS_1, Tag.DEJOIN_TOTAL],
    "from_2_to_0": [Tag._DEJOINED_FROM_2, Tag._DEJOINED_TO_0, Tag._JOIN_1, Tag._JOIN_0, None, Tag.DEJOIN_TOTAL],
    "from_1_to_0": [None, Tag._DEJOINED_TO_0, Tag._JOIN_1, Tag._JOIN_0, None, Tag.DEJOIN_TOTAL]
}

TAGS_MAPPING = {
    "binary_positive": Tag._BINARY_POSITIVE_VALUE,
    "binary_negative": Tag._BINARY_NEGATIVE_VALUE,
    "binary_unordered": Tag._BINARY_UNORDERED,
    "hidden_sum_total": Tag._HIDDEN_SUM_TOTAL,
    "hidden_sum_number": Tag._HIDDEN_SUM_NUMBER,
    "hidden_sum_all": Tag._HIDDEN_SUM_ALL,
    "hidden_sum_without_mentions": Tag._HIDDEN_SUM_WITHOUT_MENTIONS,
    "how_many_count": Tag._HOW_MANY_COUNT,
    "how_many_count_distinct": Tag._HOW_MANY_COUNT_DISTINCT,
    "how_many_sum": Tag._HOW_MANY_SUM,
    "how_many_avg": Tag._HOW_MANY_AVG,
    "how_many_max": Tag._HOW_MANY_MAX,
    "how_many_nothing": Tag._HOW_MANY_NOTHING,

    "and_to_filters": Tag._AND_TO_FILTERS,
    "and_to_columns": Tag._AND_TO_COLUMNS,
    "and_to_intersect": Tag._AND_TO_INTERSECT,

    "and_removed": Tag._AND_REMOVED,
    "and_added": Tag._AND_ADDED,
    "and_among": Tag._AND_AMONG,

    "and_with_or_agg_removed": Tag._AND_WITH_OR_AGG_REMOVED,
    "and_with_or_column_removed": Tag._AND_WITH_OR_COLUMN_REMOVED,
    "and_with_or_compare_removed": Tag._AND_WITH_OR_COMPARE_REMOVED,
    "and_with_or_filter_removed": Tag._AND_WITH_OR_FILTER_REMOVED,
    "and_with_or_in_column_removed": Tag._AND_WITH_OR_IN_COLUMN_REMOVED,
    "and_with_or_ordering_removed": Tag._AND_WITH_OR_ORDERING_REMOVED,
    "and_with_or_union_removed": Tag._AND_WITH_OR_UNION_REMOVED,

    "any_reformulation": Tag._ANY_REFORMULATION,
    "any_remove": Tag._ANY_REMOVE,

    "order_by_count_limit": Tag._ORDER_BY_COUNT_LIMIT,
    "order_by_count_limit_1": Tag._ORDER_BY_COUNT_LIMIT_1,

    "reformulation": Tag._REFORMULATION,
    "reformulation_short": Tag._REFORMULATION_SHORT,
    "reformulation_synonym": Tag._REFORMULATION_SYNONYM,
    "reformulation_abbreviation": Tag._REFORMULATION_ABBREVIATION,

    "negation_not_equal": Tag._NEGATION_NOT_EQUAL,
    "negation_except": Tag._NEGATION_EXCEPT,
    "negation_not_in": Tag._NEGATION_NOT_IN,
    "negation_other_words": Tag._NEGATION_OTHER_WORDS,
    "negation_redundant": Tag._NEGATION_REDUNDANT,
    "negation_opposite": Tag._NEGATION_OPPOSITE,

    "negation_any": Tag._NEGATION_ANY,
    "negation_all": Tag._NEGATION_ALL


    # "mono_to_all": Tag.MONO_TO_ALL,
    # "mono_to_part": Tag.MONO_TO_PART,
    # "rephrase": Tag.REPHRASE_OF_AGGREGATION,
    # "count": Tag.COUNT_IN_AGGREGATION,

}