#!/usr/bin/env python3
"""
Simple CLI entry point for CHT web interface.

Usage:
    python -m cht.web [options]

Options:
    --host HOST        Host to bind to (default: 0.0.0.0)
    --port PORT        Port to listen on (default: 8000)
    --ch-host HOST     ClickHouse host (default: localhost)
    --ch-port PORT     ClickHouse port (default: 8123)
    --ch-user USER     ClickHouse user (default: developer)
    --ch-password PWD  ClickHouse password (default: developer)
    --ch-secure        Use secure connection
    --ch-verify        Verify SSL certificates
    --ch-read-only     Connect in read-only mode
    --reload           Enable auto-reload (for development)
    --help             Show this help message

Examples:
    python -m cht.web
    python -m cht.web --port 9000 --ch-host production.clickhouse.com
    python -m cht.web --reload --ch-user admin --ch-password secret
"""

import argparse
import os
import sys
from typing import Optional

import uvicorn

from .api.app import create_app
from .api.cluster_store import ClusterSettings, ClusterStore
from .api.services import ClickHouseMetadataService


def _env_bool(name: str, default: bool = False) -> bool:
    """Parse boolean from environment variable."""
    val = os.getenv(name)
    if val is None:
        return default
    return val.lower() in {"1", "true", "yes", "on"}


def create_cluster_store(
    ch_host: str = "localhost",
    ch_port: int = 8123,
    ch_user: str = "developer",
    ch_password: str = "developer",
    ch_secure: bool = False,
    ch_verify: bool = False,
    ch_read_only: bool = False,
) -> ClusterStore:
    """Create a cluster store with default settings."""
    store = ClusterStore()
    settings = ClusterSettings(
        host=ch_host,
        port=ch_port,
        user=ch_user,
        password=ch_password,
        secure=ch_secure,
        verify=ch_verify,
        read_only=ch_read_only,
    )
    store.add_cluster("default", settings, make_active=True)
    return store


def parse_args() -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="CHT Web Interface - ClickHouse metadata explorer",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s
  %(prog)s --port 9000 --ch-host production.clickhouse.com
  %(prog)s --reload --ch-user admin --ch-password secret
        """,
    )

    # Web server options
    parser.add_argument(
        "--host",
        default=os.getenv("HOST", "0.0.0.0"),
        help="Host to bind to (default: %(default)s)",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=int(os.getenv("PORT", "8000")),
        help="Port to listen on (default: %(default)s)",
    )

    # ClickHouse connection options
    parser.add_argument(
        "--ch-host",
        default=os.getenv("CH_HOST", "localhost"),
        help="ClickHouse host (default: %(default)s)",
    )
    parser.add_argument(
        "--ch-port",
        type=int,
        default=int(os.getenv("CH_PORT", "8123")),
        help="ClickHouse port (default: %(default)s)",
    )
    parser.add_argument(
        "--ch-user",
        default=os.getenv("CH_USER", "developer"),
        help="ClickHouse user (default: %(default)s)",
    )
    parser.add_argument(
        "--ch-password",
        default=os.getenv("CH_PASSWORD", "developer"),
        help="ClickHouse password (default: %(default)s)",
    )
    parser.add_argument(
        "--ch-secure",
        action="store_true",
        default=_env_bool("CH_SECURE", False),
        help="Use secure connection (default: %(default)s)",
    )
    parser.add_argument(
        "--ch-verify",
        action="store_true",
        default=_env_bool("CH_VERIFY", False),
        help="Verify SSL certificates (default: %(default)s)",
    )
    parser.add_argument(
        "--ch-read-only",
        action="store_true",
        default=_env_bool("CH_READ_ONLY", False),
        help="Connect in read-only mode (default: %(default)s)",
    )

    # Development options
    parser.add_argument(
        "--reload",
        action="store_true",
        default=_env_bool("RELOAD", False),
        help="Enable auto-reload for development (default: %(default)s)",
    )

    return parser.parse_args()


def main() -> None:
    """Main entry point."""
    args = parse_args()

    # Create cluster store with provided settings
    cluster_store = create_cluster_store(
        ch_host=args.ch_host,
        ch_port=args.ch_port,
        ch_user=args.ch_user,
        ch_password=args.ch_password,
        ch_secure=args.ch_secure,
        ch_verify=args.ch_verify,
        ch_read_only=args.ch_read_only,
    )

    # Create the metadata service
    service = ClickHouseMetadataService(cluster_store)

    # Create the FastAPI app
    app = create_app(service, cluster_store=cluster_store)

    # Print startup information
    print(f"🚀 Starting CHT Web Interface")
    display_host = "127.0.0.1" if args.host == "0.0.0.0" else args.host
    print(f"   📍 Web UI: http://{display_host}:{args.port}/ui")
    print(f"   📊 API Docs: http://{display_host}:{args.port}/docs")
    print(f"   🔌 ClickHouse: {args.ch_user}@{args.ch_host}:{args.ch_port}")
    if args.ch_secure:
        print(f"   🔒 Secure: Yes (verify={args.ch_verify})")
    if args.ch_read_only:
        print(f"   👀 Read-only: Yes")
    print()

    # Start the server
    uvicorn.run(
        app,
        host=args.host,
        port=args.port,
        reload=args.reload,
    )


if __name__ == "__main__":
    main()
