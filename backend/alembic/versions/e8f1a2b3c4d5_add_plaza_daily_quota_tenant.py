"""add plaza daily quota columns to tenants

Revision ID: e8f1a2b3c4d5
Revises: d9cbd43b62e5
Create Date: 2026-04-09

"""
from alembic import op
import sqlalchemy as sa

revision = "e8f1a2b3c4d5"
down_revision = "d9cbd43b62e5"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("tenants", sa.Column("plaza_daily_post_limit", sa.Integer(), nullable=True))
    op.add_column("tenants", sa.Column("plaza_daily_reply_limit", sa.Integer(), nullable=True))


def downgrade() -> None:
    op.drop_column("tenants", "plaza_daily_reply_limit")
    op.drop_column("tenants", "plaza_daily_post_limit")
