"""Reset login password for an identity (global email). Run from backend/ with DATABASE_URL set.

Usage (from repo root or backend/):
  cd backend && uv run python scripts/reset_identity_password.py <email> <new_password>

Or on server after deploy:
  cd /data/iDataMate/backend && uv run python scripts/reset_identity_password.py user@example.com 'NewSecret123'

Plaintext passwords are never stored; this only writes a new bcrypt hash to identities.password_hash.
"""
from __future__ import annotations

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from sqlalchemy import func, select

from app.core.security import hash_password
from app.database import async_session
from app.models.user import Identity


async def main() -> None:
    if len(sys.argv) != 3:
        print(
            "Usage: uv run python scripts/reset_identity_password.py <email> <new_password>",
            file=sys.stderr,
        )
        sys.exit(2)
    raw_email, new_password = sys.argv[1].strip(), sys.argv[2]
    if not raw_email or not new_password:
        print("Email and password must be non-empty.", file=sys.stderr)
        sys.exit(2)
    if len(new_password) < 6:
        print("Password must be at least 6 characters.", file=sys.stderr)
        sys.exit(2)

    email_lower = raw_email.lower()

    async with async_session() as db:
        result = await db.execute(
            select(Identity).where(func.lower(Identity.email) == email_lower)
        )
        ident = result.scalar_one_or_none()
        if ident is None:
            print(f"No identity found for email: {raw_email}", file=sys.stderr)
            sys.exit(1)
        ident.password_hash = hash_password(new_password)
        await db.commit()
        print(f"Password updated for identity email: {ident.email}")


if __name__ == "__main__":
    asyncio.run(main())
