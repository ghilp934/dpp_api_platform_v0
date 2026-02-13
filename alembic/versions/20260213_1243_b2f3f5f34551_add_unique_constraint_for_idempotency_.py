"""Add unique constraint for idempotency key

Revision ID: b2f3f5f34551
Revises: c5404f7a7462
Create Date: 2026-02-13 12:43:23.759536

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'b2f3f5f34551'
down_revision = 'c5404f7a7462'
branch_labels = None
depends_on = None


def upgrade() -> None:
    # INT-01: Add UNIQUE constraint on (tenant_id, idempotency_key)
    # This prevents race conditions from creating duplicate runs with same idempotency key
    op.create_unique_constraint(
        'uq_runs_tenant_idempotency',
        'runs',
        ['tenant_id', 'idempotency_key']
    )


def downgrade() -> None:
    op.drop_constraint('uq_runs_tenant_idempotency', 'runs', type_='unique')
