"""
alf — Harvesting client.

Connects to configured third-party APIs, retrieves vehicle pricing data,
and stores results as JSON flat files organised by manufacturer/model/date.

Modules
-------
  auctions    — Auction sold/reserve/start prices (default)
  classifieds — Classified listing prices (AutoTrader, Exchange & Mart, Motors.co.uk)

Usage
-----
    # Run auctions continuously (interval from config/settings.json)
    python main.py

    # Run classifieds once then exit
    python main.py --module classifieds --run-once

    # Override config and data directories
    python main.py --module auctions --config-dir /etc/alf/config --data-dir /var/alf/data

    # Verbose (DEBUG) logging
    python main.py --verbose
"""

import argparse
import logging
import sys

from src.scheduler import Scheduler

# Default config directories per module
_DEFAULT_CONFIG_DIR = {
    "auctions":    "config",
    "classifieds": "config/classifieds",
}


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="alf",
        description="Vehicle data harvesting client",
    )
    parser.add_argument(
        "--module",
        choices=["auctions", "classifieds"],
        default="auctions",
        help="Harvesting module to run (default: auctions)",
    )
    parser.add_argument(
        "--run-once",
        action="store_true",
        default=False,
        help="Execute a single batch then exit (default: run continuously)",
    )
    parser.add_argument(
        "--config-dir",
        default=None,
        metavar="DIR",
        help=(
            "Directory containing sites.json and settings.json "
            "(default: config/ for auctions, config/classifieds/ for classifieds)"
        ),
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

    config_dir = args.config_dir or _DEFAULT_CONFIG_DIR[args.module]

    client_class = None
    if args.module == "classifieds":
        from src.classifieds.client import ClassifiedHarvestClient
        client_class = ClassifiedHarvestClient

    try:
        scheduler = Scheduler(
            config_dir=config_dir,
            data_dir=args.data_dir,
            client_class=client_class,
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
