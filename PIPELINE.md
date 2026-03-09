# Cellpose Batch-Segmentation Pipeline

## Overview

`pipeline.py` is a high-throughput batch-segmentation script that runs
[Cellpose](https://github.com/MouseLand/cellpose) across an entire experiment's
worth of field-of-view (FOV) folders.  It is designed for the common microscopy
workflow where raw images live on a network server, but GPU processing must
happen on a local workstation with fast storage.

The script automatically:

1. Mounts any required network drives.
2. Copies raw images from the network to a fast local scratch disk.
3. Runs Cellpose on each FOV using the GPU.
4. Uploads the segmentation results back to the network.
5. Cleans up all temporary local files.

A JSON configuration file controls every tuneable parameter so that the
script itself never needs to be edited between experiments.

---

## Quick Start

```
python pipeline.py -c config.json
```

That single command is all you need.  Create one `config.json` per experiment
(or per batch of experiments) and point the script at it.

To abort at any time press **CTRL+C**.  All running Cellpose processes will be
terminated immediately and GPU memory will be freed.  If the pipeline is running
in detached mode (no interactive console), create a file called
`ABORT_PIPELINE.txt` in the output directory instead — see
[Aborting a Detached Run](#aborting-a-detached-run).

---

## How It Works

The pipeline uses a **producer/consumer** threading model with two pools of
workers and a bounded queue between them:

```
 Network           Local Scratch           Network
 (input)              (fast)               (output)
    |                    |                    |
    |   Fetch Workers    |    GPU Workers     |
    |------------------->|------------------->|
    |  copy raw images   | run Cellpose, then |
    |  to scratch disk   | upload results     |
    |                    |                    |
```

- **Fetch workers** pull FOV folders off a work queue, copy the matching image
  files to local scratch storage, and place a job descriptor onto the *ready
  queue*.
- **GPU workers** consume jobs from the ready queue, invoke Cellpose, and copy
  the finished output back to the network.
- The ready queue is **bounded** (controlled by `max_staged_folders`) so that
  the fetch workers do not fill the scratch disk faster than the GPU can
  process.

### Resumability

Before copying any files, the pipeline checks whether a hidden sentinel file
(`.pipeline_completed`) already exists in the FOV's output directory.  If it
does, the FOV is silently skipped.  This means you can safely **re-run the same
command** after an interruption or a partial failure and it will pick up exactly
where it left off.

### Output file renaming

Cellpose appends a `_cp_masks` suffix to every mask file it produces.  The
pipeline automatically strips this suffix so that output filenames match the
original input filenames (e.g. `image_w00.png` instead of
`image_w00_cp_masks.png`).

### VRAM safety

Every Cellpose subprocess is tracked in a global registry.  If the pipeline
exits for any reason (CTRL+C, crash, unhandled exception), an `atexit` handler
terminates all registered subprocesses so that GPU memory is released
immediately.

### Graceful abort via kill-switch file

When the pipeline runs in detached mode (no interactive console), CTRL+C
cannot reach the process.  Force-killing with `taskkill /F` bypasses all
Python cleanup handlers and leaves GPU memory locked.

Instead, the pipeline watches for a **kill-switch sentinel file** named
`ABORT_PIPELINE.txt` in the output directory.  When this file appears the
pipeline:

1. Stops downloading new FOV folders immediately.
2. Lets any Cellpose process that is already running finish its current image.
3. Uploads completed results, cleans up scratch storage, and exits gracefully.

See [Aborting a Detached Run](#aborting-a-detached-run) below for instructions.

---

## Configuration Reference

All settings are stored in a single JSON file.  Below is a fully annotated
example followed by a detailed explanation of every key.

### Example `config.json`

```json
{
    "network_drives": {
        "T:": "\\\\bs-hpsvr16\\TimelapseData"
    },
    "pipeline_tuning": {
        "fetch_workers": 4,
        "gpu_workers": 2,
        "max_staged_folders": 4
    },
    "input_root": "T:\\TimelapseData\\251118YZ18",
    "output_root": "T:\\TimelapseData\\251118YZ18\\Analysis\\Segmentation_251128",
    "local_scratch_root": "D:\\pipeline_scratch",
    "folder_filter": "*_p*",
    "cellpose": {
        "use_gpu": true,
        "img_filter": "w00",
        "flow_threshold": 0.4,
        "cellprob_threshold": 0.0,
        "norm_percentile_low": 1.0,
        "norm_percentile_high": 99.0,
        "save_png": true,
        "no_npy": true
    }
}
```

### Top-level keys

| Key | Type | Required | Description |
|---|---|---|---|
| `network_drives` | object | No | Drive letters to mount before processing. Keys are drive letters (e.g. `"T:"`), values are UNC paths (e.g. `"\\\\server\\share"`). Already-mounted drives are silently skipped. |
| `input_root` | string | **Yes** | Absolute path to the directory that contains the FOV sub-folders. |
| `output_root` | string | **Yes** | Absolute path where results are written. Created automatically if it does not exist. A copy of the config file is saved here as `run_config.json` for reproducibility. |
| `local_scratch_root` | string | **Yes** | Absolute path to fast local storage (e.g. an NVMe SSD) used for temporary staging. The pipeline creates `inputs/` and `outputs/` sub-directories here and removes them when each FOV is done. |
| `folder_filter` | string | No | Glob pattern to select which sub-folders inside `input_root` are treated as FOVs. Defaults to `"*_p*"`. |
| `pipeline_tuning` | object | No | Performance-tuning parameters (see below). |
| `cellpose` | object | **Yes** | Cellpose segmentation parameters (see below). |

### `pipeline_tuning`

These settings control the degree of parallelism.  The defaults are
conservative; increase them if your hardware allows it.

| Key | Type | Default | Description |
|---|---|---|---|
| `fetch_workers` | int | 2 | Number of threads that copy files from the network to scratch in parallel. Increase this if your network bandwidth is underutilised. |
| `gpu_workers` | int | 1 | Number of threads that run Cellpose concurrently. Set this to the number of GPUs available, or to 2 on a single GPU if individual FOVs do not saturate it. |
| `max_staged_folders` | int | 2 | Maximum number of FOV folders that may be staged on the scratch disk at once. Acts as back-pressure on the fetch workers so they do not fill the disk. |

### `cellpose`

These parameters are passed directly to the Cellpose CLI.  Only keys that are
present in your config file are forwarded; omitted keys fall back to Cellpose's
own defaults.

| Key | Type | Cellpose default | Description |
|---|---|---|---|
| `use_gpu` | bool | `false` | Enable GPU acceleration. Should almost always be `true` for batch processing. |
| `img_filter` | string | *(none)* | Only process files whose names contain this substring. For multi-channel acquisitions, use this to select a single wavelength (e.g. `"w00"` for the first channel). |
| `pretrained_model` | string | `"cyto3"` | Name of the pretrained Cellpose model to use (e.g. `"cyto3"`, `"nuclei"`, `"cyto2"`), or an absolute path to a custom-trained model file. |
| `diameter` | number | `30` | Expected cell diameter in pixels. Set to `0` to let Cellpose estimate it automatically. |
| `flow_threshold` | number | `0.4` | Maximum allowed error of the flow field. Increase to get more cells (at the risk of merging neighbours); decrease for stricter segmentation. |
| `cellprob_threshold` | number | `0.0` | Pixels with a cell probability below this value are excluded from masks. Decrease (towards -6) to include dimmer cells; increase (towards +6) to be more selective. |
| `norm_percentile_low` | number | `1.0` | Lower percentile for intensity normalisation. Pixel values below this percentile are clipped to black. |
| `norm_percentile_high` | number | `99.0` | Upper percentile for intensity normalisation. Pixel values above this percentile are clipped to white. Both `norm_percentile_low` and `norm_percentile_high` must be present for either to take effect. |
| `save_png` | bool | `false` | Save a PNG rendering of the segmentation masks alongside the default output. |
| `no_npy` | bool | `false` | Suppress the `.npy` output files that contain flows and other intermediate data. Enable this to save disk space when only the masks are needed. |

---

## Setting Up a New Experiment

1. **Create a copy** of `config.json` for your experiment (e.g.
   `exp_2026-03-09.json`).

2. **Set the paths.**  Point `input_root` at the top-level directory that
   contains your FOV sub-folders, and `output_root` at where you want the
   results.

3. **Adjust `folder_filter`** if your FOV folder names follow a different
   naming convention.  The value is a
   [glob pattern](https://docs.python.org/3/library/pathlib.html#pathlib.Path.glob),
   so `"*_p*"` matches any folder whose name contains `_p`.

4. **Set `img_filter`** to the wavelength string that identifies your channel
   of interest (e.g. `"w00"`, `"w01"`).  Leave it as an empty string `""` to
   process every file.

5. **Tune Cellpose parameters.**  Start with the defaults and adjust
   `flow_threshold`, `cellprob_threshold`, and the normalisation percentiles
   based on a representative FOV.  The
   [Cellpose GUI](https://cellpose.readthedocs.io/en/latest/gui.html) is
   convenient for interactive testing before committing values to the config.

6. **Run the pipeline:**
   ```
   python pipeline.py -c exp_2026-03-09.json
   ```

7. **Inspect results.**  Each FOV output directory will contain:
   - Segmentation mask files (with the `_cp_masks` suffix removed).
   - A `cellpose_run.log` with the full Cellpose console output.
   - A `.pipeline_completed` sentinel file.

---

## Aborting a Detached Run

If the pipeline is running without an interactive console (e.g. launched via
Task Scheduler, `Start-Process`, or a remote session), **do not force-kill the
process**.  Force-killing bypasses all Python cleanup, leaves orphaned files on
the scratch drive, and keeps GPU memory locked.

Instead, create an empty file named `ABORT_PIPELINE.txt` inside the
`output_root` directory for your run.  The pipeline checks for this file once
per second.

**PowerShell example** (adjust the path to match your `output_root`):

```powershell
New-Item -Path "T:\TimelapseData\251118YZ18\Analysis\Segmentation_251128\ABORT_PIPELINE.txt" -ItemType File
```

Once the file is detected the pipeline will:

1. Stop fetching new FOV folders immediately.
2. Let the currently running Cellpose process finish its image.
3. Upload the last completed results to the network.
4. Clean up all temporary files on the scratch drive.
5. Release GPU memory and exit.

The sentinel file is deleted automatically so it does not interfere with
subsequent runs.  Because every completed FOV is marked with
`.pipeline_completed`, you can simply re-run the same command later to resume
from where the abort occurred.

---

## Re-running and Recovery

Because the pipeline checks for `.pipeline_completed` before processing each
FOV, you can safely re-run the exact same command after an interruption.  Only
unfinished FOVs will be processed.

To **force reprocessing** of a specific FOV, delete its output folder (or just
the `.pipeline_completed` file inside it) and run the pipeline again.

To **reprocess everything from scratch**, delete the entire `output_root`
directory.

---

## Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| `Found 0 FOV(s)` | `folder_filter` does not match any sub-folders inside `input_root`. | Check the glob pattern.  Run `ls <input_root>` to see actual folder names. |
| `No files matching '...' found` | `img_filter` does not match any files in a FOV folder. | Verify the filter string against actual filenames. |
| `Cellpose exited with code 1` | Cellpose encountered an error. | Open the `cellpose_run.log` in the FOV's output directory for the full error message. |
| Pipeline hangs after CTRL+C | Should not happen.  The polling loop yields to `KeyboardInterrupt` and the `atexit` handler kills all subprocesses. | If a process survives, kill it manually via Task Manager and check for leftover files in `local_scratch_root`. |
| Need to stop a detached run | The process has no console to receive CTRL+C and `taskkill /F` skips cleanup. | Create `ABORT_PIPELINE.txt` in the `output_root` directory (see [Aborting a Detached Run](#aborting-a-detached-run)). |
| Scratch disk fills up | Too many FOVs staged at once, or previous runs left behind temporary directories. | Reduce `max_staged_folders` in `pipeline_tuning`.  Manually delete leftover `*_tmp` directories in `local_scratch_root`. |

---

## Reproducibility

Every pipeline run copies the configuration file to `output_root/run_config.json`.
This means you always have a record of exactly which parameters were used to
produce a given set of results, even if you later modify or delete the original
config file.
