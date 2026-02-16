import logging
import requests
from plugins.vault_secrets_manager import VaultSecretsManager

logger = logging.getLogger(__name__)


class TargetNotifier:
    def __init__(self, vault: VaultSecretsManager, config: dict):
        self.vault = vault
        self.config = config['infrastructure']

    def notify(self, target_name: str, event: dict) -> None:
        dispatch = {
            'security_system': self.notify_security_system,
            'amazon_echo': self.notify_amazon_echo,
            'google_nest': self.notify_google_nest,
        }
        handler = dispatch.get(target_name)
        if handler is None:
            raise ValueError(f"Unknown target: {target_name}")
        handler(event)

    def notify_security_system(self, event: dict) -> None:
        cfg = self.config['security_system']
        api_key = self.vault.get_secret(cfg['vault_path'], 'api_key')
        url = f"{cfg['api_url']}/events"
        response = requests.post(
            url,
            json={'event': event},
            headers={'Authorization': f'Bearer {api_key}'},
        )
        logger.info("security_system status=%s body=%s", response.status_code, response.text)
        response.raise_for_status()

    def notify_amazon_echo(self, event: dict) -> None:
        cfg = self.config['amazon_echo']
        api_key = self.vault.get_secret(cfg['vault_path'], 'api_key')
        response = requests.post(
            cfg['api_url'],
            json={'event': event, 'device_id': cfg['device_id']},
            headers={'Authorization': f'Bearer {api_key}'},
        )
        logger.info("amazon_echo status=%s body=%s", response.status_code, response.text)
        response.raise_for_status()

    def notify_google_nest(self, event: dict) -> None:
        cfg = self.config['google_nest']
        api_key = self.vault.get_secret(cfg['vault_path'], 'api_key')
        url = f"{cfg['api_url']}/devices/{cfg['device_id']}:executeCommand"
        response = requests.post(
            url,
            json={'event': event},
            headers={'Authorization': f'Bearer {api_key}'},
        )
        logger.info("google_nest status=%s body=%s", response.status_code, response.text)
        response.raise_for_status()
