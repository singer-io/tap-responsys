from functools import reduce
from unittest import TestCase
from tap_responsys.sftp import FileMatcher

import singer
LOGGER = singer.get_logger()

class TestFileMatcher(TestCase):
    stamps = [
        "20180911",
        "20180911_09",
        "20180911_0911",
        "20180911_091102",
        "2018-09-11",
        "2018-09-11_09",
        "2018-09-11_09-11",
        "2018-09-11_09-11-02",
        "20180911",
        "20180911_09",
        "20180911_0911",
        "20180911_091102",
        "2018-09-11",
        "2018-09-11_09",
        "2018-09-11_09-11",
        "2018-09-11_09-11-02",
    ]

    positive_no_stamp_file = "not_a_timestamped_export.csv"
    positive_files = {
        "prefix_csv": [p + "prefix_csv.csv" for p in stamps],
        "suffix_csv": ["suffix_csv" + s + ".csv" for s in stamps],
        "prefix_txt": [p + "prefix_txt.txt" for p in stamps],
        "suffix_txt": ["suffix_txt" + s + ".txt" for s in stamps],
        "both_prefix_and_suffix_csv": [s + "both_prefix_and_suffix_csv" + s + ".csv" for s in stamps],
        "both_prefix_and_suffix_txt": [s + "both_prefix_and_suffix_txt" + s + ".txt" for s in stamps],
        "not_a_timestamped_export": [positive_no_stamp_file]
    }

    all_positive_list = reduce(lambda x, y: x + y, positive_files.values(), [])

    wrong_extension = "12345678its_some_file.wav"
    no_extension = "12345678oops"

    all_negative_list = [wrong_extension,
                         no_extension]

    positive_table_names = {"prefix_csv", "suffix_csv", "prefix_txt", "suffix_txt", "both_prefix_and_suffix_txt", "both_prefix_and_suffix_csv", "not_a_timestamped_export"}
    
    def test_table_matcher_ready_files_without_extension(self):
        regex = FileMatcher()

        def get_file_name(f):
            file_split = f.split(".")
            if len(file_split) == 1:
                return file_split[0]
            return "".join(file_split[:-1])

        list_under_test = self.all_positive_list + self.all_negative_list
        ready_files = [get_file_name(f) + ".ready" for f in list_under_test]

        result = regex.match_available_tables(list_under_test + ready_files)
        self.assertEqual(self.positive_table_names, result)

    def test_table_matcher_ready_files_with_extension(self):
        regex = FileMatcher()

        list_under_test = self.all_positive_list + self.all_negative_list
        ready_files = [f + ".ready" for f in list_under_test]

        # Construct a list of positive and negative files, and assert they are all matched
        result = regex.match_available_tables(list_under_test + ready_files)
        self.assertEqual(self.positive_table_names, result)

    def test_table_matcher_no_ready_files(self):
        regex = FileMatcher()
        self.assertEqual(set(), regex.match_available_tables(self.all_positive_list))

    def test_table_file_matcher(self):
        regex = FileMatcher()
        files_available = [{"filepath": f} for f in self.all_positive_list + self.all_negative_list]
        for table_name in self.positive_table_names:
            table_files = regex.match_files_for_table(files_available, table_name)
            self.assertEqual(set(self.positive_files.get(table_name, [])), set([f["filepath"] for f in table_files]))
