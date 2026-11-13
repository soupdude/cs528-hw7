# HW3: Apache Beam + Cloud Dataflow on the 12k linked files

`beam_pipeline.py` reads every file in the bucket once and finds:

1. the top 5 files with the most **incoming** links
2. the top 5 files with the most **outgoing** links
3. the top 5 most frequent **word trigrams** (three consecutive words in the same file)

The same script runs on your laptop and on Dataflow; only the flags change.

Replace these placeholders everywhere below:

| Placeholder | Your value (from HW2) |
| --- | --- |
| `PROJECT_ID` | `project-f131ec20-0df6-4cfa-bf0` |
| `BUCKET` | `cs528-12kfiles-bucket-thomas` |
| `REGION` | the region your bucket is in, e.g. `us-central1` |

## 1. One-time setup

```bash
# Python environment
python3 -m venv beam-env
source beam-env/bin/activate            # Windows: beam-env\Scripts\activate
pip install -r requirements.txt

# Google Cloud login (the second command lets Python code use your credentials)
gcloud auth login
gcloud auth application-default login
gcloud config set project PROJECT_ID

# Turn on the APIs Dataflow needs
gcloud services enable dataflow.googleapis.com compute.googleapis.com \
    storage.googleapis.com logging.googleapis.com

# Find your bucket's region (Dataflow should run in the same one)
gcloud storage buckets describe gs://BUCKET --format="value(location)"
```

Dataflow workers run as the **Compute Engine default service account**
(`PROJECT_NUMBER-compute@developer.gserviceaccount.com`). Give it the worker
role and access to the bucket:

```bash
PROJECT_NUMBER=$(gcloud projects describe PROJECT_ID --format="value(projectNumber)")
SA="${PROJECT_NUMBER}-compute@developer.gserviceaccount.com"

gcloud projects add-iam-policy-binding PROJECT_ID \
    --member="serviceAccount:${SA}" --role="roles/dataflow.worker"
gcloud storage buckets add-iam-policy-binding gs://BUCKET \
    --member="serviceAccount:${SA}" --role="roles/storage.objectAdmin"
```

## 2. Run locally (your laptop)

Read straight from the bucket, so the local and cloud runs process the same data:

```bash
python beam_pipeline.py \
    --input "gs://BUCKET/12k-files/*.txt" \
    --output results/local
```

Or, faster, read a local copy of the files:

```bash
python beam_pipeline.py --input "my_12k_files/*.txt" --output results/local
```

With no `--runner` flag the script uses Beam's in-process `FnApiRunner`.
(Beam's newer default local runner, Prism, ran out of memory on the
1.36 million trigrams in testing.)

## 3. Run on Cloud Dataflow

```bash
python beam_pipeline.py \
    --input "gs://BUCKET/12k-files/*.txt" \
    --output "gs://BUCKET/hw3-output/dataflow" \
    --runner DataflowRunner \
    --project PROJECT_ID \
    --region REGION \
    --temp_location gs://BUCKET/tmp \
    --job_name hw3-links-trigrams \
    --max_num_workers 2
```

The command waits until the job finishes, then prints the results and the
total time. You can watch the job graph at
**Console → Dataflow → Jobs → hw3-links-trigrams**.

If you get a quota error (common on free-trial accounts), keep
`--max_num_workers 2` and add `--machine_type e2-standard-2`.

## 4. Output

The script prints the three top-5 lists and also writes them to
`<output>-incoming.txt`, `<output>-outgoing.txt` and `<output>-trigrams.txt`
(on Dataflow these land in the bucket).

## 5. Runtimes to report

| Run | What to record |
| --- | --- |
| Local | "Total wall-clock time" printed by the script |
| Dataflow | "Total wall-clock time" printed by the script, **and** the job's *Elapsed time* in the Dataflow console |

Expect Dataflow to be slower for this job. Only ~48 MB of data is
processed, so most of Dataflow's time is spent starting and stopping worker
VMs (usually 3 to 5 minutes), not computing.

## 6. Tests

`test_beam_pipeline.py` checks each step on tiny hand-made inputs:

```bash
pip install pytest
pytest -q
```
