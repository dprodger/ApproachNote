#!/usr/bin/env python3
"""
Reset a user's password.

Usage:
    python scripts/reset_password.py <email>                 # prompt for password
    python scripts/reset_password.py <email> --password ...  # pass inline (discouraged)
    python scripts/reset_password.py <email> --yes           # skip the confirmation prompt

Prints the target database host before committing, so you don't accidentally
stomp a prod password thinking you're in dev. Exits non-zero if the email is
unknown, the passwords don't match, or the user declines the prompt.
"""

import argparse
import getpass
import os
import sys
from pathlib import Path

# Add parent directory to path so we can import db_utils (mirrors grant_admin.py).
sys.path.insert(0, str(Path(__file__).parent.parent))

# Load .env from backend/ so DB_* vars are available.
try:
    from dotenv import load_dotenv
    env_path = Path(__file__).parent.parent / '.env'
    if env_path.exists():
        load_dotenv(env_path)
except ImportError:
    pass

from core.auth_utils import hash_password  # noqa: E402
from db_utils import get_db_connection  # noqa: E402

MIN_PASSWORD_LENGTH = 8


def parse_args(argv=None):
    parser = argparse.ArgumentParser(
        description="Reset a user's password.",
    )
    parser.add_argument('email', help="Email address of the user.")
    parser.add_argument(
        '--password',
        help=(
            "New password. If omitted, you will be prompted interactively "
            "(preferred, so the plaintext doesn't land in shell history)."
        ),
    )
    parser.add_argument(
        '--yes', '-y',
        action='store_true',
        help="Skip the interactive confirmation prompt.",
    )
    return parser.parse_args(argv)


def read_password_interactively() -> str:
    first = getpass.getpass("New password: ")
    second = getpass.getpass("Confirm password: ")
    if first != second:
        print("Passwords do not match.", file=sys.stderr)
        sys.exit(1)
    return first


def main(argv=None):
    args = parse_args(argv)

    new_password = args.password or read_password_interactively()
    if len(new_password) < MIN_PASSWORD_LENGTH:
        print(
            f"Password must be at least {MIN_PASSWORD_LENGTH} characters.",
            file=sys.stderr,
        )
        return 1

    db_host = os.environ.get('DB_HOST') or '(unset)'
    db_name = os.environ.get('DB_NAME') or '(unset)'

    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id, email, display_name, is_active "
                "FROM users WHERE email = %s",
                (args.email,),
            )
            user = cur.fetchone()

            if not user:
                print(f"No user found with email: {args.email}", file=sys.stderr)
                return 1

            print(f"Target DB:  {db_host} / {db_name}")
            print(f"User:       {user['email']}")
            print(f"  id:         {user['id']}")
            print(f"  name:       {user['display_name'] or '(unset)'}")
            print(f"  is_active:  {user['is_active']}")

            if not args.yes:
                answer = input(
                    f"Proceed to reset password for {args.email} "
                    f"on {db_host}? [y/N] "
                )
                if answer.strip().lower() != 'y':
                    print("Cancelled.")
                    return 1

            cur.execute(
                "UPDATE users SET password_hash = %s, updated_at = NOW() "
                "WHERE id = %s",
                (hash_password(new_password), user['id']),
            )
            conn.commit()

    print(f"Done. Password updated for {args.email}.")
    return 0


if __name__ == '__main__':
    sys.exit(main())
