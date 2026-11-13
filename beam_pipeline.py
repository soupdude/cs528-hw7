"""
Homework 3: Apache Beam / Cloud Dataflow analysis of the 12k linked files.

One pipeline reads every file once and computes three things:
  1. Top 5 files with the most INCOMING links
  2. Top 5 files with the most OUTGOING links
  3. Top 5 most frequent word trigrams (three consecutive words in the same file)

The same code runs on your laptop (DirectRunner) or on Google Cloud Dataflow
(DataflowRunner). Only the command-line flags change. See README.md.

Example (local, reading a local folder):
    python beam_pipeline.py --input "my_12k_files/*.txt" --output results/local

Example (Dataflow, reading the bucket):
    python beam_pipeline.py \
        --input "gs://YOUR_BUCKET/12k-files/*.txt" \
        --output "gs://YOUR_BUCKET/hw3-output/dataflow" \
        --runner DataflowRunner --project YOUR_PROJECT --region us-central1 \
        --temp_location gs://YOUR_BUCKET/tmp
"""

import argparse
import logging
import string
import time

import apache_beam as beam
from apache_beam.io import fileio
from apache_beam.io.filesystems import FileSystems
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions

TOP_N = 5


# ---------------------------------------------------------------------------
# Helper functions (module-level so Dataflow workers can pickle them)
# ---------------------------------------------------------------------------

def to_name_and_text(readable_file):
    """ReadableFile -> ("file_17.txt", "<whole file contents>")."""
    path = readable_file.metadata.path
    name = path.rsplit("/", 1)[-1]          # drop the bucket / folder prefix
    return name, readable_file.read_utf8()


def get_links(text):
    """Each non-empty line of a file is one link to another file."""
    return [line.strip() for line in text.splitlines() if line.strip()]


def outgoing_count(name_text):
    """(name, text) -> (name, number of links this file makes)."""
    name, text = name_text
    return name, len(get_links(text))


def link_targets(name_text):
    """(name, text) -> every file this file points at (one element per link)."""
    _, text = name_text
    return get_links(text)


def trigrams(name_text):
    """(name, text) -> every run of 3 consecutive words in that file.

    Words are split on whitespace, lower-cased, and stripped of punctuation at
    their ends. Trigrams never cross from one file into another.
    """
    _, text = name_text
    words = [w.strip(string.punctuation) for w in text.lower().split()]
    words = [w for w in words if w]
    for i in range(len(words) - 2):
        yield " ".join(words[i:i + 3])


def by_count_then_name(kv):
    """Sort key for Top: highest count first; ties broken by name so the
    answer is the same on every run."""
    key, count = kv
    return count, key


def format_rows(top_list):
    """[(key, count), ...] -> printable lines, rank 1 first."""
    return [f"{rank}. {key}\t{count}" for rank, (key, count) in enumerate(top_list, 1)]


def top_and_write(pcoll, label, output_prefix):
    """(key, count) PCollection -> top 5 written to <output_prefix>-<label>.txt."""
    return (
        pcoll
        | f"Top5 {label}" >> beam.combiners.Top.Of(TOP_N, key=by_count_then_name)
        | f"Format {label}" >> beam.FlatMap(format_rows)
        | f"Write {label}" >> beam.io.WriteToText(
            f"{output_prefix}-{label}",
            file_name_suffix=".txt",
            num_shards=1,
            shard_name_template="",      # exact file name, no -00000-of-00001
        )
    )


# ---------------------------------------------------------------------------
# Pipeline
# ---------------------------------------------------------------------------

def build_pipeline(p, input_pattern, output_prefix):
    files = (
        p
        | "Match files" >> fileio.MatchFiles(input_pattern)
        | "Read files" >> fileio.ReadMatches()
        | "Name and text" >> beam.Map(to_name_and_text)
    )

    # 1a. Incoming links: emit each link's target, then count per target.
    incoming = (
        files
        | "Link targets" >> beam.FlatMap(link_targets)
        | "Count incoming" >> beam.combiners.Count.PerElement()
    )

    # 1b. Outgoing links: just the number of lines in each file.
    outgoing = files | "Count outgoing" >> beam.Map(outgoing_count)

    # 2. Trigrams: emit every trigram, then count how often each appears.
    trigram_counts = (
        files
        | "Make trigrams" >> beam.FlatMap(trigrams)
        | "Count trigrams" >> beam.combiners.Count.PerElement()
    )

    top_and_write(incoming, "incoming", output_prefix)
    top_and_write(outgoing, "outgoing", output_prefix)
    top_and_write(trigram_counts, "trigrams", output_prefix)


def print_results(output_prefix):
    """Read the three small result files back and print them, so the output
    shows up in your terminal for both local and Dataflow runs."""
    titles = {
        "incoming": "Top 5 files by INCOMING links",
        "outgoing": "Top 5 files by OUTGOING links",
        "trigrams": "Top 5 word trigrams",
    }
    for label, title in titles.items():
        path = f"{output_prefix}-{label}.txt"
        print(f"\n{title}  ({path})")
        with FileSystems.open(path) as f:
            print(f.read().decode("utf-8").rstrip())


def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True,
                        help='Glob of input files, e.g. "gs://bucket/12k-files/*.txt"')
    parser.add_argument("--output", required=True,
                        help="Prefix for result files (local path or gs://...)")
    known_args, pipeline_args = parser.parse_known_args(argv)

    # Local default: Beam's in-process FnApiRunner. The newer default local
    # runner (Prism) used 5+ GB of RAM on the 1.36M trigrams and crashed in
    # testing; FnApiRunner finished in ~17 s using ~1 GB.
    if not any(a.startswith("--runner") for a in pipeline_args):
        pipeline_args.append("--runner=FnApiRunner")

    options = PipelineOptions(pipeline_args)
    # Ship this module's imports/functions to Dataflow workers.
    options.view_as(SetupOptions).save_main_session = True

    start = time.perf_counter()
    p = beam.Pipeline(options=options)
    build_pipeline(p, known_args.input, known_args.output)
    result = p.run()
    result.wait_until_finish()
    elapsed = time.perf_counter() - start

    print_results(known_args.output)
    runner = options.get_all_options().get("runner") or "DirectRunner"
    print(f"\nRunner: {runner}")
    print(f"Total wall-clock time (submit to finish): {elapsed:.1f} s")


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.WARNING)
    run()
