import os
import yaml


def load_config(path: str = None) -> dict:
    if path is None:
        path = os.path.join(os.path.dirname(__file__), '..', 'config', 'infrastructure.yaml')
    with open(path, 'r') as f:
        return yaml.safe_load(f)


def get_targets_for_event(event_type: str, config: dict) -> list[str]:
    """Return a list of enabled target names whose event_types include the given event_type."""
    targets = []
    for target_name, target_cfg in config.get('infrastructure', {}).items():
        if not target_cfg.get('enabled', False):
            continue
        if event_type in target_cfg.get('event_types', []):
            targets.append(target_name)
    return targets
