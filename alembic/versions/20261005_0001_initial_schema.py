"""initial schema: tenants, provider accounts, endpoints, events, deliveries, dead letters

Revision ID: 0001
Revises:
Create Date: 2026-10-05
"""
from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql as pg

revision: str = "0001"
down_revision: str | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

delivery_status = pg.ENUM(
    "pending", "in_flight", "retrying", "succeeded", "dead", name="delivery_status",
    create_type=False,
)


def upgrade() -> None:
    delivery_status.create(op.get_bind(), checkfirst=True)

    op.create_table(
        "tenants",
        sa.Column("id", pg.UUID(as_uuid=True), primary_key=True),
        sa.Column("name", sa.String(200), nullable=False),
        sa.Column("api_key_id", sa.String(64), nullable=False, unique=True),
        sa.Column("api_key_hash", sa.Text, nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(),
                  nullable=False),
    )

    op.create_table(
        "provider_accounts",
        sa.Column("id", pg.UUID(as_uuid=True), primary_key=True),
        sa.Column("tenant_id", pg.UUID(as_uuid=True),
                  sa.ForeignKey("tenants.id", ondelete="CASCADE"), nullable=False),
        sa.Column("provider", sa.String(32), nullable=False),
        sa.Column("external_account_id", sa.String(128), nullable=False),
        sa.UniqueConstraint("provider", "external_account_id"),
    )
    op.create_index("ix_provider_accounts_tenant_id", "provider_accounts", ["tenant_id"])

    op.create_table(
        "endpoints",
        sa.Column("id", pg.UUID(as_uuid=True), primary_key=True),
        sa.Column("tenant_id", pg.UUID(as_uuid=True),
                  sa.ForeignKey("tenants.id", ondelete="CASCADE"), nullable=False),
        sa.Column("url", sa.Text, nullable=False),
        sa.Column("secret", sa.Text, nullable=False),
        sa.Column("event_types", pg.ARRAY(sa.String(64)), nullable=False),
        sa.Column("active", sa.Boolean, server_default=sa.text("true"), nullable=False),
        sa.Column("rate_per_second", sa.Float),
        sa.Column("burst", sa.Integer),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(),
                  nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(),
                  nullable=False),
    )
    op.create_index("ix_endpoints_tenant_id", "endpoints", ["tenant_id"])

    op.create_table(
        "events",
        sa.Column("id", pg.UUID(as_uuid=True), primary_key=True),
        sa.Column("tenant_id", pg.UUID(as_uuid=True),
                  sa.ForeignKey("tenants.id", ondelete="CASCADE"), nullable=False),
        sa.Column("provider", sa.String(32), nullable=False),
        sa.Column("provider_event_id", sa.String(128), nullable=False),
        sa.Column("event_type", sa.String(64), nullable=False),
        sa.Column("call_id", sa.String(128), nullable=False),
        sa.Column("occurred_at", sa.DateTime(timezone=True)),
        sa.Column("payload", pg.JSONB, nullable=False),
        sa.Column("received_at", sa.DateTime(timezone=True),
                  server_default=sa.text("clock_timestamp()"), nullable=False),
        sa.Column("fanned_out_at", sa.DateTime(timezone=True)),
        sa.UniqueConstraint("tenant_id", "provider_event_id",
                            name="uq_events_tenant_provider_event"),
    )
    # Keyset pagination: tenant scoped, newest first, id as the tie breaker.
    op.create_index(
        "ix_events_tenant_received", "events",
        ["tenant_id", sa.text("received_at DESC"), sa.text("id DESC")],
    )
    op.create_index("ix_events_tenant_call", "events", ["tenant_id", "call_id"])
    # Partial index: only rows still waiting for fanout, so it stays tiny.
    op.create_index(
        "ix_events_unfanned", "events", ["received_at"],
        postgresql_where=sa.text("fanned_out_at IS NULL"),
    )

    op.create_table(
        "deliveries",
        sa.Column("id", pg.UUID(as_uuid=True), primary_key=True),
        sa.Column("tenant_id", pg.UUID(as_uuid=True),
                  sa.ForeignKey("tenants.id", ondelete="CASCADE"), nullable=False),
        sa.Column("event_id", pg.UUID(as_uuid=True),
                  sa.ForeignKey("events.id", ondelete="CASCADE"), nullable=False),
        sa.Column("endpoint_id", pg.UUID(as_uuid=True),
                  sa.ForeignKey("endpoints.id", ondelete="CASCADE"), nullable=False),
        sa.Column("status", delivery_status, server_default="pending", nullable=False),
        sa.Column("attempt_count", sa.Integer, server_default="0", nullable=False),
        sa.Column("next_retry_at", sa.DateTime(timezone=True)),
        sa.Column("claimed_at", sa.DateTime(timezone=True)),
        sa.Column("last_response_code", sa.Integer),
        sa.Column("last_error", sa.Text),
        sa.Column("delivered_at", sa.DateTime(timezone=True)),
        sa.Column("created_at", sa.DateTime(timezone=True),
                  server_default=sa.text("clock_timestamp()"), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(),
                  nullable=False),
        sa.UniqueConstraint("event_id", "endpoint_id", name="uq_deliveries_event_endpoint"),
    )
    # Dispatcher and reaper scan by status then due time.
    op.create_index("ix_deliveries_status_next_retry", "deliveries", ["status", "next_retry_at"])
    op.create_index(
        "ix_deliveries_tenant_created", "deliveries",
        ["tenant_id", sa.text("created_at DESC"), sa.text("id DESC")],
    )
    op.create_index("ix_deliveries_endpoint_id", "deliveries", ["endpoint_id"])

    op.create_table(
        "dead_letters",
        sa.Column("id", pg.UUID(as_uuid=True), primary_key=True),
        sa.Column("delivery_id", pg.UUID(as_uuid=True),
                  sa.ForeignKey("deliveries.id", ondelete="CASCADE"), nullable=False),
        sa.Column("tenant_id", pg.UUID(as_uuid=True),
                  sa.ForeignKey("tenants.id", ondelete="CASCADE"), nullable=False),
        sa.Column("event_id", pg.UUID(as_uuid=True), nullable=False),
        sa.Column("endpoint_id", pg.UUID(as_uuid=True), nullable=False),
        sa.Column("attempts", sa.Integer, nullable=False),
        sa.Column("last_response_code", sa.Integer),
        sa.Column("last_error", sa.Text),
        sa.Column("reason", sa.String(64), nullable=False),
        sa.Column("dead_at", sa.DateTime(timezone=True), server_default=sa.func.now(),
                  nullable=False),
        sa.Column("replayed_at", sa.DateTime(timezone=True)),
    )
    op.create_index("ix_dead_letters_delivery_id", "dead_letters", ["delivery_id"])
    op.create_index("ix_dead_letters_tenant_id", "dead_letters", ["tenant_id"])

    op.create_table(
        "circuit_events",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("endpoint_id", pg.UUID(as_uuid=True),
                  sa.ForeignKey("endpoints.id", ondelete="CASCADE"), nullable=False),
        sa.Column("state", sa.String(16), nullable=False),
        sa.Column("consecutive_failures", sa.Integer, nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(),
                  nullable=False),
    )
    op.create_index("ix_circuit_events_endpoint_id", "circuit_events", ["endpoint_id"])


def downgrade() -> None:
    op.drop_table("circuit_events")
    op.drop_table("dead_letters")
    op.drop_table("deliveries")
    op.drop_table("events")
    op.drop_table("endpoints")
    op.drop_table("provider_accounts")
    op.drop_table("tenants")
    delivery_status.drop(op.get_bind(), checkfirst=True)
