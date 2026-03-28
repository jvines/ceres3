#!/usr/bin/env python3
import os
import sys
import json
import time
import signal
import logging
import traceback
import subprocess
from datetime import datetime

import redis

# Import settings
from config import settings
from shared.job_status import publish_event

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger('ceres_worker')

# Constants (derived from settings)
SPECTRA_DIR = settings.SPECTRA_DIR
RAW_SPECTRA_DIR = os.path.join(SPECTRA_DIR, 'raw_spectra')
REDUCED_DIR = os.path.join(SPECTRA_DIR, 'reduced')
STATUS_DIR = os.path.join(SPECTRA_DIR, 'status')
RAW_CALIBRATIONS_DIR = os.path.join(SPECTRA_DIR, 'raw_calibrations')
CALIBRATIONS_DIR = os.path.join(SPECTRA_DIR, 'calibrations')

# === Machine-Level Slot Manager ===
MACHINE_ID = os.environ.get('MACHINE_ID', 'default')
MACHINE_TOTAL_SLOTS = int(os.environ.get('MACHINE_TOTAL_SLOTS', '16'))
SLOT_COST = int(os.environ.get('SLOT_COST', '4'))

# Queue and key name constants
REDUCTION_QUEUE = 'queue:reduction:{0}'.format(MACHINE_ID)
REDUCTION_QUEUE_GLOBAL = 'reduction_queue'
STATUS_PREFIX = 'reduction:status:'
JOB_LIST_KEY = 'reduction:jobs'
EVENTS_CHANNEL = os.environ.get('EVENTS_CHANNEL', 'reduction:events')
import socket
import threading
import uuid as _uuid
WORKER_HOSTNAME = socket.gethostname()

WORKER_ID = 'ceres:{0}'.format(_uuid.uuid4().hex[:12])
HEARTBEAT_TTL = 30
HEARTBEAT_INTERVAL = 10

_MACHINE_KEY = 'slots:machine:{0}'.format(MACHINE_ID)
_HEARTBEAT_KEY = 'slots:heartbeat:{0}:{1}'.format(MACHINE_ID, WORKER_ID)
_CONFIG_KEY = 'slots:config:{0}'.format(MACHINE_ID)
_MACHINES_KEY = 'slots:machines'

# Lua scripts (executed server-side, Python version doesn't matter)
_LUA_ACQUIRE = """
local existing = redis.call('HGET', KEYS[1], ARGV[1])
if existing then
    redis.call('SET', KEYS[2], ARGV[5], 'EX', ARGV[4])
    return 1
end
redis.call('HSET', KEYS[1], ARGV[1], ARGV[2])
local vals = redis.call('HVALS', KEYS[1])
local used = 0
for _, v in ipairs(vals) do
    used = used + tonumber(v)
end
local total = tonumber(ARGV[3])
if used > total then
    redis.call('HDEL', KEYS[1], ARGV[1])
    return 0
end
redis.call('SET', KEYS[2], ARGV[5], 'EX', ARGV[4])
redis.call('HSET', KEYS[3], 'total_slots', ARGV[3])
redis.call('SADD', KEYS[4], string.match(KEYS[1], 'slots:machine:(.+)'))
return 1
"""

_LUA_RELEASE = """
redis.call('HDEL', KEYS[1], ARGV[1])
redis.call('DEL', KEYS[2])
return 1
"""

_LUA_CLEANUP = """
local workers = redis.call('HKEYS', KEYS[1])
local removed = 0
for _, worker_id in ipairs(workers) do
    local hb_key = ARGV[1] .. worker_id
    if redis.call('EXISTS', hb_key) == 0 then
        redis.call('HDEL', KEYS[1], worker_id)
        removed = removed + 1
    end
end
return removed
"""

_acquire_script = None
_release_script = None
_cleanup_script = None
_heartbeat_stop = None
_heartbeat_thread = None
_current_job_id = None


def _register_slot_scripts(redis_client):
    """Register Lua scripts with Redis (call once after connect)."""
    global _acquire_script, _release_script, _cleanup_script
    _acquire_script = redis_client.register_script(_LUA_ACQUIRE)
    _release_script = redis_client.register_script(_LUA_RELEASE)
    _cleanup_script = redis_client.register_script(_LUA_CLEANUP)


def _heartbeat_loop(redis_client):
    """Background thread to refresh heartbeat TTL (slot + job)."""
    while not _heartbeat_stop.is_set():
        try:
            metadata = json.dumps({
                "worker_id": WORKER_ID,
                "worker_type": "ceres",
                "slot_cost": SLOT_COST,
            })
            pipe = redis_client.pipeline()
            pipe.set(_HEARTBEAT_KEY, metadata, ex=HEARTBEAT_TTL)
            if _current_job_id:
                job_hb = json.dumps({
                    "worker_id": WORKER_ID,
                    "machine_id": MACHINE_ID,
                    "worker_type": "ceres",
                })
                pipe.set(
                    'job:heartbeat:{0}'.format(_current_job_id),
                    job_hb, ex=HEARTBEAT_TTL
                )
            pipe.execute()
        except Exception as e:
            logger.error("Heartbeat refresh failed: %s", str(e))
        _heartbeat_stop.wait(HEARTBEAT_INTERVAL)


def job_heartbeat_start(job_id):
    """Start tracking a job heartbeat."""
    global _current_job_id
    _current_job_id = job_id
    logger.debug("Started job heartbeat for %s", job_id)


def job_heartbeat_stop(redis_client):
    """Stop tracking the current job heartbeat and delete the key."""
    global _current_job_id
    job_id = _current_job_id
    _current_job_id = None
    if job_id:
        try:
            redis_client.delete('job:heartbeat:{0}'.format(job_id))
        except Exception as e:
            logger.error("Failed to delete job heartbeat key: %s", str(e))
        logger.debug("Stopped job heartbeat for %s", job_id)


def slot_acquire(redis_client):
    """Atomically acquire machine slots. Returns True on success."""
    global _heartbeat_stop, _heartbeat_thread

    metadata = json.dumps({
        "worker_id": WORKER_ID,
        "worker_type": "ceres",
        "slot_cost": SLOT_COST,
    })

    result = _acquire_script(
        keys=[_MACHINE_KEY, _HEARTBEAT_KEY, _CONFIG_KEY, _MACHINES_KEY],
        args=[WORKER_ID, SLOT_COST, MACHINE_TOTAL_SLOTS, HEARTBEAT_TTL, metadata]
    )

    if result == 1:
        _heartbeat_stop = threading.Event()
        _heartbeat_thread = threading.Thread(target=_heartbeat_loop, args=(redis_client,))
        _heartbeat_thread.daemon = True
        _heartbeat_thread.start()
        logger.info("Acquired %d slots on %s (worker: %s)", SLOT_COST, MACHINE_ID, WORKER_ID)
        return True
    return False


def slot_release(redis_client):
    """Release held slots and stop heartbeat."""
    global _heartbeat_stop, _heartbeat_thread
    if _heartbeat_stop is not None:
        _heartbeat_stop.set()
        if _heartbeat_thread is not None and _heartbeat_thread.is_alive():
            _heartbeat_thread.join(timeout=5)
        _heartbeat_stop = None
        _heartbeat_thread = None

    try:
        _release_script(
            keys=[_MACHINE_KEY, _HEARTBEAT_KEY],
            args=[WORKER_ID]
        )
        logger.info("Released %d slots on %s", SLOT_COST, MACHINE_ID)
    except Exception as e:
        logger.error("Failed to release slots: %s", str(e))


def slot_cleanup(redis_client):
    """Remove expired heartbeat entries."""
    hb_prefix = 'slots:heartbeat:{0}:'.format(MACHINE_ID)
    removed = _cleanup_script(
        keys=[_MACHINE_KEY],
        args=[hb_prefix]
    )
    if removed > 0:
        logger.info("Cleaned up %d dead worker(s) on %s", removed, MACHINE_ID)
    return removed


def slot_wait_and_acquire(redis_client, poll_interval=5):
    """Block until slots are acquired."""
    while True:
        slot_cleanup(redis_client)
        if slot_acquire(redis_client):
            return
        logger.info("Machine %s full, waiting %ds... (need %d slots)",
                     MACHINE_ID, poll_interval, SLOT_COST)
        time.sleep(poll_interval)

# Job status constants
STATUS_QUEUED = 'queued'
STATUS_PENDING = 'pending'
STATUS_RUNNING = 'running'
STATUS_COMPLETED = 'completed'
STATUS_FAILED = 'failed'

# Create required directories
for directory in [RAW_SPECTRA_DIR, REDUCED_DIR, STATUS_DIR,
                  RAW_CALIBRATIONS_DIR, CALIBRATIONS_DIR]:
    if not os.path.exists(directory):
        os.makedirs(directory)


class GracefulKiller:
    """Handle graceful shutdown on SIGINT or SIGTERM"""

    def __init__(self):
        self.kill_now = False
        signal.signal(signal.SIGINT, self.exit_gracefully)
        signal.signal(signal.SIGTERM, self.exit_gracefully)

    def exit_gracefully(self, signum, frame):
        if not self.kill_now:
            logger.info("Received signal %s, will shut down after current job completes.", signum)
            self.kill_now = True

    def suppress_signals(self):
        """Ignore SIGTERM during job execution (prevents Pool child noise)."""
        self._prev_handler = signal.getsignal(signal.SIGTERM)
        signal.signal(signal.SIGTERM, signal.SIG_IGN)

    def restore_signals(self):
        """Restore signal handling between jobs."""
        signal.signal(signal.SIGTERM, self.exit_gracefully)
        signal.signal(signal.SIGINT, self.exit_gracefully)


def connect_redis():
    """Connect to Redis with retries"""
    max_retries = 5
    base_delay = 5

    for attempt in range(max_retries):
        try:
            client = redis.Redis(
                host=settings.REDIS_HOST,
                port=settings.REDIS_PORT,
                db=settings.REDIS_DB,
                password=settings.REDIS_PASSWORD,
                socket_timeout=30,
                socket_connect_timeout=10
            )
            # Test connection
            client.ping()
            logger.info("Connected to Redis at %s:%s", settings.REDIS_HOST, settings.REDIS_PORT)
            return client
        except redis.RedisError as e:
            retry_delay = base_delay * (2 ** attempt)  # Exponential backoff
            logger.error("Redis connection failed (attempt %d/%d): %s",
                         attempt + 1, max_retries, str(e))
            if attempt < max_retries - 1:
                logger.info("Retrying in %d seconds...", retry_delay)
                time.sleep(retry_delay)

    logger.critical("Failed to connect to Redis after %d attempts", max_retries)
    sys.exit(1)


def update_job_status(redis_client, job_id, status, message=None, output=None):
    """Update job status in Redis and status directory"""
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    # Create status data
    status_data = {
        'job_id': job_id,
        'status': status,
        'timestamp': timestamp,
        'message': message or '',
        'output': output or '',
        'machine_id': MACHINE_ID
    }

    # Update Redis
    status_key = STATUS_PREFIX + job_id
    redis_client.set(status_key, json.dumps(status_data))

    # Keep status for 7 days
    redis_client.expire(status_key, 7 * 24 * 60 * 60)

    # Add to jobs list if not already present
    if not redis_client.sismember(JOB_LIST_KEY, job_id):
        redis_client.sadd(JOB_LIST_KEY, job_id)

    # Also write to status file for shared access
    status_file = os.path.join(STATUS_DIR, "{0}.json".format(job_id))
    with open(status_file, 'w') as f:
        json.dump(status_data, f, indent=2)

    logger.info("Updated job %s status: %s", job_id, status)

    # Publish event
    publish_event(redis_client, EVENTS_CHANNEL, job_id, status, "reduction", {
        "message": message,
        "output": output,
        "machine_id": MACHINE_ID,
    })


CERESPP_QUEUE = "queue:cerespp:{0}".format(MACHINE_ID)
CERESPP_EVENTS_CHANNEL = "cerespp:events"
CERESPP_STATUS_PREFIX = "cerespp:status:"


def run_postprocessing(redis_client, job_id, file_paths, mask='G2'):
    """Run activity indicator computation on already-reduced spectra (replaces cerespp worker)."""
    from ceres3.postprocess import process_spectrum

    update_job_status(redis_client, job_id, STATUS_RUNNING,
                      "Computing activity indicators for {0} files".format(len(file_paths)))

    results = []
    for fpath in file_paths:
        try:
            result = process_spectrum(fpath, save_1d=True, update_fits=True)
            results.append(result)

            # Publish per-file event (compatible with cerespp event handler)
            publish_event(redis_client, CERESPP_EVENTS_CHANNEL, job_id, "running", "cerespp:file_completed", {
                'file': os.path.basename(fpath),
                's_index': result.get('s_index', -999.0),
                'halpha': result.get('halpha', -999.0),
                'hei': result.get('hei', -999.0),
                'nai_d1d2': result.get('nai_d1d2', -999.0),
                'ca_irt': result.get('ca_irt', -999.0),
                'hbeta': result.get('hbeta', -999.0),
                'mg_ib': result.get('mg_ib', -999.0),
                'log_rhk': result.get('log_rhk', -999.0),
                'spectrum_1d_path': result.get('spectrum_1d_path', ''),
            })

        except Exception as e:
            logger.error("Post-processing failed for %s: %s", fpath, str(e))
            publish_event(redis_client, CERESPP_EVENTS_CHANNEL, job_id, "running", "cerespp:file_failed", {
                'file': os.path.basename(fpath),
                'error': str(e),
            })

    # Store final status
    status_data = json.dumps({
        'status': 'completed',
        'job_id': job_id,
        'results': [{k: v for k, v in r.items() if k != 'spectrum_1d_path'} for r in results],
        'n_files': len(file_paths),
        'n_success': len(results),
    })
    redis_client.set("{0}{1}".format(CERESPP_STATUS_PREFIX, job_id), status_data, ex=7*86400)

    # Write status file for durability
    status_path = os.path.join(SPECTRA_DIR, 'cerespp_status')
    os.makedirs(status_path, exist_ok=True)
    with open(os.path.join(status_path, "{0}.json".format(job_id)), 'w') as f:
        f.write(status_data)

    # Publish completion event
    publish_event(redis_client, CERESPP_EVENTS_CHANNEL, job_id, "completed", "cerespp", {
        'n_files': len(file_paths),
        'n_success': len(results),
    })

    update_job_status(redis_client, job_id, STATUS_COMPLETED,
                      "Activity indicators computed for {0}/{1} files".format(len(results), len(file_paths)))
    return results


def run_reduction(redis_client, job_id, date, npools=8, do_class=True,
                  job_type="science", calibration_reference_date=None, nsigmas=5.0):
    """Run CERES reduction pipeline via direct Python import (no subprocess)."""
    # Determine input directory based on job type
    if job_type == "calibration":
        input_dir = os.path.join(RAW_CALIBRATIONS_DIR, date)
    else:
        input_dir = os.path.join(RAW_SPECTRA_DIR, date)

    if not os.path.exists(input_dir):
        error_msg = "Input directory not found: {0}".format(input_dir)
        update_job_status(redis_client, job_id, STATUS_FAILED, error_msg)
        return False

    update_job_status(redis_client, job_id, STATUS_RUNNING,
                      "Processing {0} data for date {1}".format(job_type, date))

    # Determine instrument from config (e.g. "feros/ferospipe_fp.py" -> "feros")
    instrument = settings.CERES_PIPELINE_SCRIPT.split('/')[0] if '/' in settings.CERES_PIPELINE_SCRIPT else 'feros'

    # Build argv for the pipeline (it uses argparse internally)
    argv = [input_dir, "-npools", str(npools), "-nsigmas", str(nsigmas)]

    if do_class:
        argv.append("-do_class")

    if job_type == "science":
        calib_date = calibration_reference_date if calibration_reference_date else date
        if not calib_date:
            update_job_status(redis_client, job_id, STATUS_FAILED, "Missing calibration date")
            return False
        import re
        if not re.match(r'^\d{4}-\d{2}-\d{2}$', calib_date):
            update_job_status(redis_client, job_id, STATUS_FAILED,
                              "Invalid date format: {0}".format(calib_date))
            return False
        calib_dir = os.path.join(CALIBRATIONS_DIR, calib_date + "_red")
        if os.path.exists(calib_dir):
            argv.extend(["-calib_dir", calib_dir])
        else:
            error_msg = "Calibration directory not found: {0}".format(calib_dir)
            update_job_status(redis_client, job_id, STATUS_FAILED, error_msg)
            return False
    else:
        argv.append("-is_calib")

    logger.info("Running %s pipeline: %s %s", instrument, instrument, " ".join(argv))

    try:
        # Import and run the pipeline directly — no subprocess
        import importlib
        sys.argv = ["ceres3-" + instrument] + argv
        pipeline = importlib.import_module("ceres3.instruments." + {
            'feros': 'ferospipe_fp',
            'harps': 'harpspipe',
            'coralie': 'coraliepipe',
            'espadons': 'espadonspipe',
            'uves': 'uvespipe',
            'fideos': 'fideospipe',
            'fies': 'fiespipe',
            'hires': 'hirespipe',
            'dupont': 'dupontpipe',
            'mike': 'mikepipe',
            'pfs': 'pfspipe',
            'cafe': 'cafepipe',
            'pucheros': 'pucherospipe',
            'arces': 'arcespipe',
            'vbt': 'vbtpipe',
        }.get(instrument, 'ferospipe_fp'))
        # Pipeline runs on import (module-level code). Force re-execution:
        importlib.reload(pipeline)

        update_job_status(
            redis_client, job_id, STATUS_COMPLETED,
            "{0} reduction completed successfully".format(job_type.capitalize())
        )
        return True

    except Exception as e:
        error_msg = "Error running {0} reduction: {1}".format(job_type, str(e))
        logger.error(error_msg)
        logger.error(traceback.format_exc())
        update_job_status(redis_client, job_id, STATUS_FAILED, error_msg, traceback.format_exc())
        return False


def parse_job_data(job_data):
    """Parse job data from Redis queue"""
    # Default values
    job_id = None
    date = None
    npools = 8
    do_class = True
    job_type = "science"  # Default to science reduction
    calibration_reference_date = None
    nsigmas = settings.CERES_NSIGMAS  # Default to config value

    # Decode bytes to str (Redis returns bytes in Python 3)
    if isinstance(job_data, bytes):
        job_data = job_data.decode('utf-8')

    try:
        # Try to parse as JSON
        job = json.loads(job_data)

        # Extract fields
        job_id = job.get('job_id')
        date = job.get('date')
        npools = job.get('npools', 8)
        do_class = job.get('do_class', True)
        job_type = job.get('job_type', 'science')
        calibration_reference_date = job.get('calibration_reference_date')
        nsigmas = job.get('nsigmas', settings.CERES_NSIGMAS)

    except (ValueError, TypeError):
        # If not JSON, assume it's just a date string
        date = job_data.strip()
        job_id = "reduction_{0}_{1}".format(date, int(time.time()))
        calibration_reference_date = None

    return job_id, date, npools, do_class, job_type, calibration_reference_date, nsigmas


def main():
    """Main worker function"""
    logger.info("Starting CERES Redis Queue Worker")
    logger.info("Redis: %s:%s", settings.REDIS_HOST, settings.REDIS_PORT)
    logger.info("Raw spectra directory: %s", RAW_SPECTRA_DIR)
    logger.info("Raw calibrations directory: %s", RAW_CALIBRATIONS_DIR)
    logger.info("Reduction queue: %s", REDUCTION_QUEUE)
    logger.info("Default npools: %d", settings.CERES_NPOOLS)
    logger.info("Machine: %s, Total slots: %d, Slot cost: %d",
                MACHINE_ID, MACHINE_TOTAL_SLOTS, SLOT_COST)

    # Connect to Redis
    redis_client = connect_redis()

    # Register worker manifest for dynamic discovery
    _manifest_data = json.dumps({
        "worker_type": "ceres",
        "display_name": "CERES",
        "queue": REDUCTION_QUEUE,
        "events_channel": EVENTS_CHANNEL,
        "concurrency_key": "slots:machine:{0}".format(MACHINE_ID),
        "slot_cost": SLOT_COST,
        "job_storage": "redis",
        "status_prefix": STATUS_PREFIX,
        "capabilities": ["data_reduction", "calibration", "spectral_analysis", "activity_indicators"],
        "version": "1.0.0",
        "machine_id": MACHINE_ID,
    })
    _manifest_key = "worker:manifest:ceres:{0}".format(MACHINE_ID)
    _manifest_ttl = 60
    redis_client.set(_manifest_key, _manifest_data)
    redis_client.expire(_manifest_key, _manifest_ttl)
    logger.info("Registered worker manifest: ceres (key=%s, TTL=%ds)", _manifest_key, _manifest_ttl)

    _manifest_stop = threading.Event()

    def _refresh_manifest():
        while not _manifest_stop.is_set():
            try:
                redis_client.set(_manifest_key, _manifest_data)
                redis_client.expire(_manifest_key, _manifest_ttl)
            except Exception as e:
                logger.error("Manifest refresh failed: %s", str(e))
            _manifest_stop.wait(20)

    _mt = threading.Thread(target=_refresh_manifest)
    _mt.daemon = True
    _mt.start()

    # Register Lua scripts for slot management
    _register_slot_scripts(redis_client)

    # Setup graceful shutdown
    killer = GracefulKiller()

    logger.info("Waiting for jobs on %s and %s ...", REDUCTION_QUEUE, CERESPP_QUEUE)

    while not killer.kill_now:
        job_data = None
        job_source = None
        try:
            # Wait for a job on EITHER queue (reduction or cerespp)
            queue_data = redis_client.blpop([REDUCTION_QUEUE, CERESPP_QUEUE], timeout=5)

            if queue_data is None:
                continue

            queue_name, job_data = queue_data

            if isinstance(queue_name, bytes):
                queue_name = queue_name.decode('utf-8')
            if isinstance(job_data, bytes):
                job_data = job_data.decode('utf-8')

            # ── CERESPP JOB (activity indicators on existing spectra) ──
            if 'cerespp' in queue_name:
                job_source = 'cerespp'
                try:
                    data = json.loads(job_data)
                    pp_job_id = data.get('job_id', 'cerespp_{0}'.format(_uuid.uuid4().hex[:8]))
                    file_paths = data.get('file_paths', [])
                    mask = data.get('mask', 'G2')

                    if not file_paths:
                        logger.error("Cerespp job %s: no file_paths provided", pp_job_id)
                        continue

                    logger.info("Received cerespp job: %s (%d files)", pp_job_id, len(file_paths))

                    slot_wait_and_acquire(redis_client)
                    try:
                        job_heartbeat_start(pp_job_id)
                        killer.suppress_signals()
                        try:
                            run_postprocessing(redis_client, pp_job_id, file_paths, mask)
                        finally:
                            killer.restore_signals()
                    finally:
                        job_heartbeat_stop(redis_client)
                        slot_release(redis_client)

                except json.JSONDecodeError:
                    logger.error("Invalid cerespp job data: %s", job_data)
                continue

            # ── REDUCTION JOB ──────────────────────────────────────────
            job_source = 'reduction'
            job_id, date, npools, do_class, job_type, calibration_reference_date, nsigmas = parse_job_data(job_data)

            if not date:
                logger.error("Invalid job data: date is required")
                continue

            logger.info("Received reduction job: %s (date: %s, type: %s, nsigmas: %.1f)",
                        job_id, date, job_type, nsigmas)

            update_job_status(redis_client, job_id, STATUS_QUEUED,
                              "Job queued for date {0} (type: {1})".format(date, job_type))

            slot_wait_and_acquire(redis_client)

            try:
                job_heartbeat_start(job_id)
                effective_npools = settings.CERES_NPOOLS

                killer.suppress_signals()
                try:
                    success = run_reduction(redis_client, job_id, date, effective_npools, do_class,
                                            job_type, calibration_reference_date, nsigmas)

                    # After successful science reduction, auto-run post-processing
                    if success and job_type == "science":
                        try:
                            import glob as globmod
                            output_dir = os.path.join(SPECTRA_DIR, 'spectra', date + '_red', 'proc')
                            sp_files = sorted(globmod.glob(os.path.join(output_dir, '*_sp.fits')))
                            if sp_files:
                                logger.info("Auto-running post-processing on %d spectra", len(sp_files))
                                pp_job_id = "cerespp_auto_{0}".format(job_id)
                                run_postprocessing(redis_client, pp_job_id, sp_files)
                        except Exception as e:
                            logger.error("Auto post-processing failed: %s", str(e))
                finally:
                    killer.restore_signals()

            finally:
                job_heartbeat_stop(redis_client)
                slot_release(redis_client)

        except redis.RedisError as e:
            logger.error("Redis error: %s", str(e))
            time.sleep(5)
            redis_client = connect_redis()
            _register_slot_scripts(redis_client)

        except Exception as e:
            logger.error("Unexpected error: %s", str(e))
            logger.error(traceback.format_exc())
            if job_data is not None and job_source == 'reduction':
                try:
                    redis_client.rpush(REDUCTION_QUEUE, job_data)
                    logger.warning("Re-queued job to %s after error", REDUCTION_QUEUE)
                except Exception:
                    logger.error("CRITICAL: failed to re-queue job, job lost!")
            time.sleep(5)

    logger.info("Worker shutting down...")


if __name__ == "__main__":
    main()
