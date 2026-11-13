"""Small tests for beam_pipeline.py. Run with:  pytest -q"""

import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to

from beam_pipeline import (by_count_then_name, link_targets, outgoing_count,
                           trigrams)

FILES = [
    ("A.txt", "B.txt\nC.txt\n"),
    ("B.txt", "C.txt\n"),
    ("C.txt", "A.txt\n\n"),        # blank line should be ignored
]


def test_outgoing_counts():
    assert [outgoing_count(f) for f in FILES] == [
        ("A.txt", 2), ("B.txt", 1), ("C.txt", 1)]


def test_incoming_counts_in_beam():
    with TestPipeline(runner="FnApiRunner") as p:
        counts = (p | beam.Create(FILES)
                    | beam.FlatMap(link_targets)
                    | beam.combiners.Count.PerElement())
        assert_that(counts, equal_to([("A.txt", 1), ("B.txt", 1), ("C.txt", 2)]))


def test_trigrams_stay_inside_one_file_and_ignore_case_and_punctuation():
    assert list(trigrams(("x", "The cat, sat on"))) == ["the cat sat", "cat sat on"]
    assert list(trigrams(("x", "too short"))) == []


def test_top5_picks_highest_counts():
    pairs = [("a", 1), ("b", 5), ("c", 3), ("d", 5), ("e", 2), ("f", 4)]
    with TestPipeline(runner="FnApiRunner") as p:
        top = (p | beam.Create(pairs)
                 | beam.combiners.Top.Of(5, key=by_count_then_name))
        assert_that(top, equal_to([[("d", 5), ("b", 5), ("f", 4), ("c", 3), ("e", 2)]]))
