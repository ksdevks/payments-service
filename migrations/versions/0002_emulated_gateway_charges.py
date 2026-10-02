"""emulated gateway charge ledger

Revision ID: 0002
Revises: 0001
Create Date: 2026-10-03
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0002"
down_revision: str | None = "0001"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "emulated_gateway_charges",
        sa.Column("payment_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("outcome", sa.String(16), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("payment_id", name=op.f("pk_emulated_gateway_charges")),
        sa.CheckConstraint(
            "outcome IN ('succeeded', 'declined')",
            name=op.f("ck_emulated_gateway_charges_outcome_valid"),
        ),
    )


def downgrade() -> None:
    op.drop_table("emulated_gateway_charges")
