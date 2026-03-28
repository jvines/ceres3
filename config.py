import os
from multiprocessing import cpu_count


class Settings:
    def __init__(self):
        self.REDIS_HOST = os.environ.get('REDIS_HOST', 'redis')
        self.REDIS_PORT = int(os.environ.get('REDIS_PORT', 6379))
        self.REDIS_DB = int(os.environ.get('REDIS_DB', 0))
        self.REDIS_PASSWORD = os.environ.get('REDIS_PASSWORD', None)

        self.SPECTRA_DIR = os.environ.get('SPECTRA_DIR', '/data/spectra')
        self.CERES_PIPELINE_SCRIPT = os.environ.get(
            'CERES_PIPELINE_SCRIPT', 'feros/ferospipe_fp.py'
        )
        self.MAX_CONCURRENT_REDUCTIONS = int(
            os.environ.get('MAX_CONCURRENT_REDUCTIONS', 1)
        )

        slot_cost = int(os.environ.get('SLOT_COST', 4))
        self.CERES_NPOOLS = int(
            os.environ.get('CERES_NPOOLS', slot_cost)
        )
        self.CERES_NSIGMAS = float(
            os.environ.get('CERES_NSIGMAS', 5.0)
        )


settings = Settings()
