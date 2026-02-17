"""
alf — Harvesting client.

Connects to configured third-party auction APIs, retrieves sold prices,
reserves, and start prices, and stores them as JSON flat files organised
by manufacturer/model/date.

Usage
-----
    # Run continuously (interval configured in config/settings.json)
    python main.py

    # Run a single batch then exit
    python main.py --run-once

    # Override config and data directories
    python main.py --config-dir /etc/alf/config --data-dir /var/alf/data

    # Verbose (DEBUG) logging
    python main.py --verbose
"""

import argparse
import logging
import sys

from src.scheduler import Scheduler


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="alf",
        description="Auction data harvesting client",
    )
    parser.add_argument(
        "--run-once",
        action="store_true",
        default=False,
        help="Execute a single batch then exit (default: run continuously)",
    )
    parser.add_argument(
        "--config-dir",
        default="config",
        metavar="DIR",
        help="Directory containing sites.json and settings.json (default: config/)",
    )
    parser.add_argument(
        "--data-dir",
        default=None,
        metavar="DIR",
        help="Override data output directory (default: value from settings.json)",
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        default=False,
        help="Enable DEBUG logging",
    )
    return parser.parse_args()


def _configure_logging(verbose: bool, log_level: str = "INFO") -> None:
    level = logging.DEBUG if verbose else getattr(logging, log_level.upper(), logging.INFO)
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)-7s %(name)s — %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def main() -> int:
    args = _parse_args()
    _configure_logging(args.verbose)

    try:
        scheduler = Scheduler(
            config_dir=args.config_dir,
            data_dir=args.data_dir,
        )
    except FileNotFoundError as exc:
        logging.error("Config file not found: %s", exc)
        return 1
    except KeyError as exc:
        logging.error("Missing required environment variable: %s", exc)
        return 1
    except Exception as exc:
        logging.error("Startup error: %s", exc)
        return 1

    if args.run_once:
        stats = scheduler.run_once()
        return 0 if stats["sites_failed"] == 0 else 1

    scheduler.run_forever()
    return 0


if __name__ == "__main__":
    sys.exit(main())
