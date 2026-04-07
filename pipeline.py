"""
pipeline.py
-----------
Cellpose batch-segmentation pipeline.

Processes a directory of field-of-view (FOV) sub-folders by staging input
images from a (possibly network-mounted) source to local scratch storage,
running Cellpose via its CLI, and uploading completed results back to a
network output directory.

A producer/consumer threading model is used: a pool of *fetch workers*
copies raw images to local scratch, while a pool of *GPU workers* runs
Cellpose on the staged data.  The ready queue between the two pools is
bounded to limit peak scratch-disk usage.

Configuration is supplied via a JSON file.  See ``config.json`` for a
documented example.

Usage
-----
    python pipeline.py -c config.json

Configuration keys
------------------
network_drives : dict, optional
    Mapping of drive letters (e.g. ``"T:"``) to UNC paths to mount before
    processing begins.  Already-mounted drives are silently skipped.
pipeline_tuning : dict
    fetch_workers : int
        Number of concurrent fetch threads (default 2).
    gpu_workers : int
        Number of concurrent GPU worker threads (default 1).
    max_staged_folders : int
        Upper bound on the number of FOV folders held in the staging queue
        at one time (default 2).
input_root : str
    Path to the directory that contains the FOV sub-folders.
output_root : str
    Path to the directory where completed results are written.  Created if
    it does not exist.
local_scratch_root : str
    Path to local fast storage used for input/output staging.
folder_filter : str
    Glob pattern used to select FOV sub-folders (default ``"*_p*"``).
cellpose : dict
    Cellpose CLI parameters forwarded verbatim.  Recognised keys:
    ``use_gpu``, ``img_filter``, ``pretrained_model``, ``diameter``,
    ``flow_threshold``, ``cellprob_threshold``, ``batch_size``, ``norm_percentile_low``,
    ``norm_percentile_high``, ``save_png``, ``no_npy``.
"""

import argparse
import atexit
import json
import logging
import queue
import shutil
import subprocess
import sys
import threading
import time
from pathlib import Path


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Subprocess registry used for VRAM cleanup on unexpected exit
# ---------------------------------------------------------------------------

_active_processes: set = set()
_proc_lock = threading.Lock()
_abort_event = threading.Event()


def _cleanup_active_processes() -> None:
    """Terminate any registered subprocesses and their child processes.

    On Windows, ``taskkill /F /T`` is used to kill the entire process tree
    so that child processes spawned by PyTorch (or similar frameworks) are
    also terminated and GPU memory is released.  On other platforms a
    simple ``proc.kill()`` is issued.

    Registered with :func:`atexit.register` so that GPU memory is released
    even when the pipeline exits abnormally (e.g. unhandled exception or
    SIGINT).
    """
    with _proc_lock:
        for proc in list(_active_processes):
            try:
                if sys.platform == "win32":
                    subprocess.run(
                        ["taskkill", "/F", "/T", "/PID", str(proc.pid)],
                        capture_output=True,
                        check=False,
                    )
                else:
                    proc.kill()
                # Wait for the process to fully exit so the OS releases all
                # file handles before worker finally-blocks attempt cleanup.
                try:
                    proc.wait(timeout=10)
                except Exception:
                    pass
                logger.info(
                    "Terminated process tree (PID %d).", proc.pid
                )
            except Exception:
                pass
        _active_processes.clear()


atexit.register(_cleanup_active_processes)


# ---------------------------------------------------------------------------
# Network-resilient path helpers
# ---------------------------------------------------------------------------

# Maximum time (seconds) to keep retrying a network path operation before
# giving up.  Covers typical IT backup snapshots and brief network drops.
NETWORK_RETRY_TIMEOUT: float = 120.0

# Interval (seconds) between successive retries.
NETWORK_RETRY_INTERVAL: float = 5.0


def _resilient_exists(path: Path) -> bool:
    """Check if *path* exists, retrying on transient network errors.

    On Windows, network drive hiccups (server snapshots, brief disconnects)
    can cause ``PermissionError`` or other ``OSError`` subclasses instead of
    a clean ``True``/``False`` answer.  This wrapper retries the check at
    :data:`NETWORK_RETRY_INTERVAL`-second intervals until either the call
    succeeds or :data:`NETWORK_RETRY_TIMEOUT` seconds have elapsed.

    If the timeout is exceeded the function assumes the path does **not**
    exist and returns ``False``, logging a warning so the event is visible
    in the pipeline log.
    """
    deadline = time.monotonic() + NETWORK_RETRY_TIMEOUT
    while True:
        try:
            return path.exists()
        except OSError as exc:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                logger.warning(
                    "Network path unreachable after %.0fs timeout: %s (%s). "
                    "Assuming path does not exist.",
                    NETWORK_RETRY_TIMEOUT, path, exc,
                )
                return False
            wait = min(NETWORK_RETRY_INTERVAL, remaining)
            logger.debug(
                "Transient network error checking %s: %s. "
                "Retrying in %.0fs (%.0fs remaining).",
                path, exc, wait, remaining,
            )
            time.sleep(wait)


def _resilient_unlink(path: Path) -> None:
    """Delete *path*, retrying on transient network errors.

    Same retry strategy as :func:`_resilient_exists`.  If the file
    disappears between retries (``FileNotFoundError``) the call
    succeeds silently.
    """
    deadline = time.monotonic() + NETWORK_RETRY_TIMEOUT
    while True:
        try:
            path.unlink(missing_ok=True)
            return
        except FileNotFoundError:
            return
        except OSError as exc:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                logger.warning(
                    "Could not delete network path after %.0fs timeout: "
                    "%s (%s). Continuing anyway.",
                    NETWORK_RETRY_TIMEOUT, path, exc,
                )
                return
            wait = min(NETWORK_RETRY_INTERVAL, remaining)
            logger.debug(
                "Transient network error deleting %s: %s. "
                "Retrying in %.0fs (%.0fs remaining).",
                path, exc, wait, remaining,
            )
            time.sleep(wait)


# ---------------------------------------------------------------------------
# Configuration helpers
# ---------------------------------------------------------------------------

def load_config(config_path: Path) -> dict:
    """Load and return the JSON pipeline configuration file.

    Parameters
    ----------
    config_path : Path
        Absolute or relative path to the JSON configuration file.

    Returns
    -------
    dict
        Parsed configuration dictionary.

    Raises
    ------
    FileNotFoundError
        If *config_path* does not exist.
    json.JSONDecodeError
        If the file is not valid JSON.
    """
    with open(config_path, "r") as fh:
        return json.load(fh)


# ---------------------------------------------------------------------------
# Network utilities
# ---------------------------------------------------------------------------

def mount_network_drives(drives: dict) -> None:
    """Mount Windows network drives that are not already connected.

    Parameters
    ----------
    drives : dict
        Mapping of drive letters (e.g. ``"T:"`` or ``"T"``) to UNC paths.
        A trailing colon is appended automatically if absent.  Drives whose
        root path already exists are silently skipped.
    """
    if not drives:
        return

    for letter, unc_path in drives.items():
        if not letter.endswith(":"):
            letter = letter + ":"
        if not Path(f"{letter}\\").exists():
            logger.info("Mounting network drive %s -> %s", letter, unc_path)
            subprocess.run(
                ["net", "use", letter, unc_path],
                capture_output=True,
                check=False,
            )


# ---------------------------------------------------------------------------
# Worker threads
# ---------------------------------------------------------------------------

def fetcher_worker(
    fov_queue: queue.Queue,
    ready_queue: queue.Queue,
    scratch_root: Path,
    output_root: Path,
    img_filter: str,
) -> None:
    """Stage raw images for one FOV at a time from the network to local scratch.

    Reads :class:`~pathlib.Path` objects from *fov_queue* until the queue is
    exhausted, copies matching image files to
    ``scratch_root/inputs/<fov_name>``, and places a job descriptor dict onto
    *ready_queue* for downstream GPU processing.

    A FOV is skipped without copying when a ``.pipeline_completed`` sentinel
    file already exists in the corresponding network output directory.

    Parameters
    ----------
    fov_queue : queue.Queue
        Queue of network-side FOV directories (:class:`pathlib.Path`).  The
        worker exits when the queue is exhausted.
    ready_queue : queue.Queue
        Downstream queue that receives job descriptor dicts with keys
        ``fov_name``, ``local_input_dir``, and ``network_output_dir``.
    scratch_root : Path
        Root of the local scratch directory.  The ``inputs/`` sub-directory
        is managed automatically.
    output_root : Path
        Root of the network output directory.
    img_filter : str
        Substring used to restrict which files are staged.  Pass an empty
        string to stage all files in the FOV directory.
    """
    while not _abort_event.is_set():
        try:
            network_input_dir: Path = fov_queue.get_nowait()
        except queue.Empty:
            break

        fov_name = network_input_dir.name
        network_output_dir = output_root / fov_name
        local_input_dir = scratch_root / "inputs" / fov_name
        temp_input_dir = scratch_root / "inputs" / f"{fov_name}_tmp"

        if (network_output_dir / ".pipeline_completed").exists():
            logger.info("Skipping %s: already completed.", fov_name)
            fov_queue.task_done()
            continue

        try:
            if temp_input_dir.exists():
                shutil.rmtree(temp_input_dir, ignore_errors=True)
            if local_input_dir.exists():
                shutil.rmtree(local_input_dir, ignore_errors=True)

            temp_input_dir.mkdir(parents=True, exist_ok=True)

            search_pattern = f"*{img_filter}*.*" if img_filter else "*.*"
            files_to_copy = list(network_input_dir.glob(search_pattern))

            if not files_to_copy:
                logger.warning(
                    "No files matching '%s' found in %s. Skipping.",
                    search_pattern,
                    fov_name,
                )
                continue

            for src_file in files_to_copy:
                shutil.copy2(src_file, temp_input_dir / src_file.name)

            temp_input_dir.rename(local_input_dir)
            logger.info("Staged %d file(s) for %s.", len(files_to_copy), fov_name)

            ready_queue.put({
                "fov_name": fov_name,
                "local_input_dir": local_input_dir,
                "network_output_dir": network_output_dir,
            })

        except Exception:
            if not _abort_event.is_set():
                logger.exception("Fetch error for %s.", fov_name)
            if temp_input_dir.exists():
                shutil.rmtree(temp_input_dir, ignore_errors=True)
            if local_input_dir.exists():
                shutil.rmtree(local_input_dir, ignore_errors=True)

        finally:
            fov_queue.task_done()


def _build_cellpose_command(
    local_input_dir: Path,
    temp_output_dir: Path,
    cp_config: dict,
) -> list:
    """Construct the Cellpose CLI argument list from the configuration.

    Parameters
    ----------
    local_input_dir : Path
        Directory containing the staged input images.
    temp_output_dir : Path
        Temporary directory where Cellpose should write its output.
    cp_config : dict
        ``cellpose`` section of the pipeline configuration.

    Returns
    -------
    list of str
        Argument list suitable for :func:`subprocess.Popen`.
    """
    cmd = [
        "cellpose",
        "--dir", str(local_input_dir),
        "--savedir", str(temp_output_dir),
    ]

    if cp_config.get("use_gpu"):
        cmd.append("--use_gpu")
    if cp_config.get("img_filter"):
        cmd.extend(["--img_filter", cp_config["img_filter"]])
    if cp_config.get("pretrained_model"):
        cmd.extend(["--pretrained_model", cp_config["pretrained_model"]])
    if cp_config.get("diameter"):
        cmd.extend(["--diameter", str(cp_config["diameter"])])
    if cp_config.get("flow_threshold"):
        cmd.extend(["--flow_threshold", str(cp_config["flow_threshold"])])
    if cp_config.get("cellprob_threshold"):
        cmd.extend(["--cellprob_threshold", str(cp_config["cellprob_threshold"])])
    if cp_config.get("batch_size"):
        cmd.extend(["--batch_size", str(cp_config["batch_size"])])
    if "norm_percentile_low" in cp_config and "norm_percentile_high" in cp_config:
        cmd.extend([
            "--norm_percentile",
            str(cp_config["norm_percentile_low"]),
            str(cp_config["norm_percentile_high"]),
        ])
    if cp_config.get("save_png"):
        cmd.append("--save_png")
    if cp_config.get("no_npy"):
        cmd.append("--no_npy")

    return cmd


def gpu_worker(
    ready_queue: queue.Queue,
    scratch_root: Path,
    cp_config: dict,
) -> None:
    """Run Cellpose on staged FOV data and upload results to the network.

    Reads job descriptor dicts from *ready_queue*.  A ``None`` value serves
    as a stop sentinel that causes the worker to exit cleanly.

    For each job the worker:

    1. Creates a temporary local output directory.
     2. Invokes Cellpose via its CLI, writing all output to a persistent
         log file under ``<scratch_root>/cellpose_logs/``.
    3. On success, renames the temporary directory to its final local path,
       copies results to the network output directory, and writes a
       ``.pipeline_completed`` sentinel file.
    4. Removes all local staging directories regardless of outcome.

    Parameters
    ----------
    ready_queue : queue.Queue
        Queue of job descriptor dicts produced by :func:`fetcher_worker`.
        Receives ``None`` as a stop signal.
    scratch_root : Path
        Root of the local scratch directory.  The ``outputs/`` sub-directory
        is managed automatically.
    cp_config : dict
        ``cellpose`` section of the pipeline configuration.
    """
    while True:
        job = ready_queue.get()

        # Exit on the natural None sentinel or on the global abort flag.
        # If a job slipped through the race window during an abort, clean
        # up its staged input directory before discarding it.
        if job is None or _abort_event.is_set():
            if job is not None:
                staged_dir = job.get("local_input_dir")
                if staged_dir and Path(staged_dir).exists():
                    shutil.rmtree(staged_dir, ignore_errors=True)
            ready_queue.task_done()
            break

        fov_name = job["fov_name"]
        local_input_dir: Path = job["local_input_dir"]
        network_output_dir: Path = job["network_output_dir"]

        local_output_dir = scratch_root / "outputs" / fov_name
        temp_output_dir = scratch_root / "outputs" / f"{fov_name}_tmp"
        persistent_log_dir = scratch_root / "cellpose_logs"

        try:
            if temp_output_dir.exists():
                shutil.rmtree(temp_output_dir, ignore_errors=True)
            if local_output_dir.exists():
                shutil.rmtree(local_output_dir, ignore_errors=True)

            temp_output_dir.mkdir(parents=True, exist_ok=True)
            persistent_log_dir.mkdir(parents=True, exist_ok=True)
            logger.info("Processing %s.", fov_name)

            cmd = _build_cellpose_command(local_input_dir, temp_output_dir, cp_config)
            log_path = persistent_log_dir / f"{fov_name}_{time.time_ns()}_cellpose_run.log"

            with open(log_path, "w") as log_file:
                process = subprocess.Popen(
                    cmd,
                    stdout=log_file,
                    stderr=subprocess.STDOUT,
                    text=True,
                )

                with _proc_lock:
                    _active_processes.add(process)

                try:
                    process.wait()
                finally:
                    with _proc_lock:
                        _active_processes.discard(process)

            if process.returncode != 0:
                if not _abort_event.is_set():
                    logger.error(
                        "Cellpose exited with code %d for %s. See log: %s",
                        process.returncode,
                        fov_name,
                        log_path,
                    )
                continue

            # Remove the _cp_masks suffix from mask files.
            for mask_file in temp_output_dir.glob("*_cp_masks*"):
                new_name = mask_file.name.replace("_cp_masks", "")
                mask_file.rename(temp_output_dir / new_name)

            temp_output_dir.rename(local_output_dir)

            network_output_dir.mkdir(parents=True, exist_ok=True)
            shutil.copytree(local_output_dir, network_output_dir, dirs_exist_ok=True)
            (network_output_dir / ".pipeline_completed").touch()

            logger.info("Results uploaded for %s.", fov_name)

        except Exception:
            if not _abort_event.is_set():
                logger.exception("Pipeline error for %s.", fov_name)

        finally:
            for cleanup_dir in [local_input_dir, temp_output_dir, local_output_dir]:
                if cleanup_dir.exists():
                    shutil.rmtree(cleanup_dir, ignore_errors=True)
            ready_queue.task_done()


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    """Parse command-line arguments, configure the pipeline, and process all FOVs."""
    parser = argparse.ArgumentParser(
        description="Cellpose batch-segmentation pipeline.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "-c", "--config",
        required=True,
        metavar="CONFIG",
        help="Path to the JSON pipeline configuration file.",
    )
    args = parser.parse_args()

    try:
        config = load_config(Path(args.config))
        mount_network_drives(config.get("network_drives", {}))

        input_root = Path(config["input_root"])
        output_root = Path(config["output_root"])
        scratch_root = Path(config["local_scratch_root"])
        tuning = config.get(
            "pipeline_tuning",
            {"fetch_workers": 2, "gpu_workers": 1, "max_staged_folders": 2},
        )

        output_root.mkdir(parents=True, exist_ok=True)
        shutil.copy2(args.config, output_root / "run_config.json")

        fovs = list(input_root.glob(config.get("folder_filter", "*_p*")))
        logger.info(
            "Found %d FOV(s). Starting pipeline "
            "(fetch_workers=%d, gpu_workers=%d, max_staged=%d).",
            len(fovs),
            tuning["fetch_workers"],
            tuning["gpu_workers"],
            tuning["max_staged_folders"],
        )

        fov_queue: queue.Queue = queue.Queue()
        ready_queue: queue.Queue = queue.Queue(maxsize=tuning["max_staged_folders"])

        for fov in fovs:
            fov_queue.put(fov)

        # Start GPU workers first so they are ready to consume staged jobs.
        gpu_threads = []
        for _ in range(tuning["gpu_workers"]):
            t = threading.Thread(
                target=gpu_worker,
                args=(ready_queue, scratch_root, config["cellpose"]),
                daemon=True,
            )
            t.start()
            gpu_threads.append(t)

        # Start fetch workers to stage FOV data into the ready queue.
        fetch_threads = []
        for _ in range(tuning["fetch_workers"]):
            t = threading.Thread(
                target=fetcher_worker,
                args=(
                    fov_queue,
                    ready_queue,
                    scratch_root,
                    output_root,
                    config["cellpose"].get("img_filter", ""),
                ),
                daemon=True,
            )
            t.start()
            fetch_threads.append(t)

        # Define the kill-switch sentinel file.  Creating this file in the
        # output directory triggers a graceful shutdown, which is the only
        # safe way to stop the pipeline when it runs in detached mode
        # (i.e. without an interactive console that can receive CTRL+C).
        kill_switch = output_root / "ABORT_PIPELINE.txt"

        def _check_for_abort() -> bool:
            """Broadcast abort, drain queues, and kill processes if the kill-switch exists.

            When the sentinel file is detected the function:

            1. Sets :data:`_abort_event` so every worker thread sees the
               signal immediately, closing the race window where a fetcher
               could sneak a job onto the ready queue after it was drained.
            2. Drains ``fov_queue`` so no new downloads start.
            3. Drains ``ready_queue`` and deletes staged input folders so
               they do not linger on the scratch disk.
            4. Calls :func:`_cleanup_active_processes` to terminate every
               running Cellpose process tree immediately.

            The ``gpu_worker`` ``finally`` blocks still execute after the
            subprocess is killed, so all remaining temporary directories
            are cleaned up automatically.
            """
            if not _resilient_exists(kill_switch):
                return False

            logger.critical("Abort file detected. Initiating immediate shutdown.")
            _resilient_unlink(kill_switch)

            # Broadcast the abort signal to all worker threads *first*,
            # before draining queues, to prevent the race condition where
            # a fetcher inserts a new job after the drain completes.
            _abort_event.set()

            # 1. Drain the fetch queue to stop new downloads.
            while not fov_queue.empty():
                try:
                    fov_queue.get_nowait()
                    fov_queue.task_done()
                except queue.Empty:
                    break

            # 2. Drain the ready queue and delete already-staged input folders.
            while not ready_queue.empty():
                try:
                    discarded_job = ready_queue.get_nowait()
                    if discarded_job is not None:
                        staged_dir = discarded_job.get("local_input_dir")
                        if staged_dir and Path(staged_dir).exists():
                            shutil.rmtree(staged_dir, ignore_errors=True)
                    ready_queue.task_done()
                except queue.Empty:
                    break

            # 3. Kill every running Cellpose process tree to free VRAM.
            _cleanup_active_processes()

            return True

        # Poll fetch threads, checking for both CTRL+C and the kill switch.
        abort_triggered = False
        while any(t.is_alive() for t in fetch_threads):
            if _check_for_abort():
                abort_triggered = True
                break
            time.sleep(1)

        # Send stop sentinels to GPU workers.  Use put_nowait so that a
        # full queue (possible after abort drained and re-filled by a
        # late fetcher) does not block the main thread indefinitely.
        for _ in gpu_threads:
            try:
                ready_queue.put_nowait(None)
            except queue.Full:
                pass

        # Poll GPU threads, continuing to check the kill switch so that
        # an abort requested after fetching has finished still takes effect.
        while any(t.is_alive() for t in gpu_threads):
            if not abort_triggered and _check_for_abort():
                abort_triggered = True
                # Re-send stop sentinels; the killed workers may have
                # already consumed the first batch before dying.
                for _ in gpu_threads:
                    try:
                        ready_queue.put_nowait(None)
                    except queue.Full:
                        pass
            time.sleep(1)

        if abort_triggered:
            # Final sweep: remove any leftover temporary directories on
            # the scratch drive that the worker finally-blocks could not
            # delete (e.g. due to Windows file-handle release delays).
            for subdir in ["inputs", "outputs"]:
                scratch_subdir = scratch_root / subdir
                if scratch_subdir.exists():
                    shutil.rmtree(scratch_subdir, ignore_errors=True)
            logger.info("Pipeline safely aborted by user.")
        else:
            logger.info("Pipeline execution complete.")

    except KeyboardInterrupt:
        _abort_event.set()
        _cleanup_active_processes()
        logger.critical("Interrupted by user. Aborting pipeline.")
        sys.exit(1)
    except Exception:
        logger.exception("Fatal pipeline error.")
        sys.exit(1)


if __name__ == "__main__":
    main()