import unittest
from unittest.mock import MagicMock, patch, mock_open
import yaml

from plugins.event_router import get_targets_for_event, load_config
from plugins.target_notifier import TargetNotifier


SAMPLE_CONFIG = {
    'infrastructure': {
        'security_system': {
            'enabled': True,
            'api_url': 'https://security.example.com/api',
            'vault_path': 'telehandler/security_system',
            'event_types': ['motion', 'intrusion', 'alert', 'alarm'],
        },
        'amazon_echo': {
            'enabled': True,
            'device_id': 'echo-device-001',
            'api_url': 'https://api.amazonalexa.com/v3/events',
            'vault_path': 'telehandler/amazon_echo',
            'event_types': ['alert', 'alarm'],
        },
        'google_nest': {
            'enabled': True,
            'device_id': 'nest-device-001',
            'api_url': 'https://smartdevicemanagement.googleapis.com/v1',
            'vault_path': 'telehandler/google_nest',
            'event_types': ['climate', 'environment', 'alert', 'alarm'],
        },
    }
}


class TestEventRouter(unittest.TestCase):
    def test_motion_routes_to_security_only(self):
        targets = get_targets_for_event('motion', SAMPLE_CONFIG)
        self.assertEqual(targets, ['security_system'])

    def test_intrusion_routes_to_security_only(self):
        targets = get_targets_for_event('intrusion', SAMPLE_CONFIG)
        self.assertEqual(targets, ['security_system'])

    def test_climate_routes_to_nest_only(self):
        targets = get_targets_for_event('climate', SAMPLE_CONFIG)
        self.assertEqual(targets, ['google_nest'])

    def test_environment_routes_to_nest_only(self):
        targets = get_targets_for_event('environment', SAMPLE_CONFIG)
        self.assertEqual(targets, ['google_nest'])

    def test_alert_routes_to_all(self):
        targets = get_targets_for_event('alert', SAMPLE_CONFIG)
        self.assertIn('security_system', targets)
        self.assertIn('amazon_echo', targets)
        self.assertIn('google_nest', targets)

    def test_alarm_routes_to_all(self):
        targets = get_targets_for_event('alarm', SAMPLE_CONFIG)
        self.assertIn('security_system', targets)
        self.assertIn('amazon_echo', targets)
        self.assertIn('google_nest', targets)

    def test_unknown_event_type_routes_to_none(self):
        targets = get_targets_for_event('unknown_event', SAMPLE_CONFIG)
        self.assertEqual(targets, [])

    def test_disabled_target_is_excluded(self):
        config = {
            'infrastructure': {
                'security_system': {
                    'enabled': False,
                    'event_types': ['motion'],
                    'api_url': '',
                    'vault_path': '',
                }
            }
        }
        targets = get_targets_for_event('motion', config)
        self.assertEqual(targets, [])

    def test_load_config(self):
        yaml_content = yaml.dump(SAMPLE_CONFIG)
        with patch('builtins.open', mock_open(read_data=yaml_content)):
            config = load_config('/fake/path.yaml')
        self.assertIn('infrastructure', config)


class TestTargetNotifier(unittest.TestCase):
    def setUp(self):
        self.vault = MagicMock()
        self.vault.get_secret.return_value = 'test-api-key'
        self.notifier = TargetNotifier(vault=self.vault, config=SAMPLE_CONFIG)

    @patch('plugins.target_notifier.requests.post')
    def test_notify_security_system(self, mock_post):
        mock_post.return_value = MagicMock(status_code=200, text='OK')
        mock_post.return_value.raise_for_status = MagicMock()
        event = {'type': 'motion', 'data': 'front-door'}
        self.notifier.notify_security_system(event)
        mock_post.assert_called_once()
        args, kwargs = mock_post.call_args
        self.assertIn('https://security.example.com/api/events', args)
        self.assertEqual(kwargs['json'], {'event': event})
        self.assertEqual(kwargs['headers']['Authorization'], 'Bearer test-api-key')

    @patch('plugins.target_notifier.requests.post')
    def test_notify_amazon_echo(self, mock_post):
        mock_post.return_value = MagicMock(status_code=200, text='OK')
        mock_post.return_value.raise_for_status = MagicMock()
        event = {'type': 'alarm', 'data': 'fire'}
        self.notifier.notify_amazon_echo(event)
        mock_post.assert_called_once()
        args, kwargs = mock_post.call_args
        self.assertIn('https://api.amazonalexa.com/v3/events', args)
        self.assertEqual(kwargs['json']['device_id'], 'echo-device-001')

    @patch('plugins.target_notifier.requests.post')
    def test_notify_google_nest(self, mock_post):
        mock_post.return_value = MagicMock(status_code=200, text='OK')
        mock_post.return_value.raise_for_status = MagicMock()
        event = {'type': 'climate', 'temperature': 22}
        self.notifier.notify_google_nest(event)
        mock_post.assert_called_once()
        args, kwargs = mock_post.call_args
        self.assertIn('nest-device-001', args[0])

    def test_notify_dispatch_unknown_target_raises(self):
        with self.assertRaises(ValueError):
            self.notifier.notify('unknown_target', {})

    @patch('plugins.target_notifier.requests.post')
    def test_notify_dispatch_routes_correctly(self, mock_post):
        mock_post.return_value = MagicMock(status_code=200, text='OK')
        mock_post.return_value.raise_for_status = MagicMock()
        self.notifier.notify('security_system', {'type': 'motion'})
        self.notifier.notify('amazon_echo', {'type': 'alarm'})
        self.notifier.notify('google_nest', {'type': 'climate'})
        self.assertEqual(mock_post.call_count, 3)


if __name__ == '__main__':
    unittest.main()
