"""Instrument pipeline registry."""

INSTRUMENTS = {
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
}


def get_pipeline_module(instrument):
    """Import and return the pipeline module for the given instrument."""
    if instrument not in INSTRUMENTS:
        raise ValueError(f"Unknown instrument: {instrument}. "
                         f"Available: {', '.join(sorted(INSTRUMENTS))}")
    module_name = INSTRUMENTS[instrument]
    import importlib
    return importlib.import_module(f'ceres3.instruments.{module_name}')


def list_instruments():
    """Return sorted list of supported instrument names."""
    return sorted(INSTRUMENTS.keys())
