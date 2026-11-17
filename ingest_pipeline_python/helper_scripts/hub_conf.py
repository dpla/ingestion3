"""Shared i3.conf helpers, importable under a plain name.

The helpers live in 2_preflight.py. Python can't import a module whose name
starts with a digit, so the other pipeline scripts import them from here:

    from helper_scripts.hub_conf import lookup_hub_in_conf, list_scheduled_hubs
"""

import importlib.util
import os

_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "2_preflight.py")
_spec = importlib.util.spec_from_file_location("preflight", _path)
_preflight = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_preflight)

CONF_PATH = _preflight.CONF_PATH
MONTH_NAMES = _preflight.MONTH_NAMES
SPECIAL_CASE_NOTES = _preflight.SPECIAL_CASE_NOTES
list_scheduled_hubs = _preflight.list_scheduled_hubs
lookup_hub_in_conf = _preflight.lookup_hub_in_conf
