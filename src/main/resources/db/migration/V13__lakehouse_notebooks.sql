CREATE TABLE IF NOT EXISTS lakehouse_notebook (
    id UUID PRIMARY KEY,
    workspace_id UUID REFERENCES workspace(id) ON DELETE SET NULL,
    title VARCHAR(200) NOT NULL,
    description TEXT,
    cells JSONB NOT NULL DEFAULT '[]'::jsonb CHECK (jsonb_typeof(cells) = 'array'),
    version BIGINT NOT NULL DEFAULT 1 CHECK (version > 0),
    owner_token_hash CHAR(64) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    deleted_at TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS ix_lakehouse_notebook_updated
    ON lakehouse_notebook(updated_at DESC) WHERE deleted_at IS NULL;
CREATE TABLE IF NOT EXISTS lakehouse_notebook_revision (
    id BIGSERIAL PRIMARY KEY,
    notebook_id UUID NOT NULL REFERENCES lakehouse_notebook(id) ON DELETE CASCADE,
    version BIGINT NOT NULL,
    title VARCHAR(200) NOT NULL,
    description TEXT,
    cells JSONB NOT NULL CHECK (jsonb_typeof(cells) = 'array'),
    change_summary VARCHAR(500),
    changed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE(notebook_id, version)
);
CREATE INDEX IF NOT EXISTS ix_lakehouse_notebook_revision_history
    ON lakehouse_notebook_revision(notebook_id, version DESC);
CREATE TABLE IF NOT EXISTS lakehouse_notebook_share (
    id UUID PRIMARY KEY,
    notebook_id UUID NOT NULL REFERENCES lakehouse_notebook(id) ON DELETE CASCADE,
    token_hash CHAR(64) NOT NULL UNIQUE,
    permission VARCHAR(10) NOT NULL CHECK (permission IN ('READ', 'WRITE')),
    label VARCHAR(120),
    expires_at TIMESTAMPTZ,
    revoked_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_lakehouse_notebook_share_notebook
    ON lakehouse_notebook_share(notebook_id, created_at DESC);
CREATE TABLE IF NOT EXISTS lakehouse_notebook_event (
    id BIGSERIAL PRIMARY KEY,
    notebook_id UUID NOT NULL REFERENCES lakehouse_notebook(id) ON DELETE CASCADE,
    event_type VARCHAR(40) NOT NULL,
    actor_kind VARCHAR(12) NOT NULL CHECK (actor_kind IN ('OWNER','SHARE')),
    details JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_lakehouse_notebook_event_history
    ON lakehouse_notebook_event(notebook_id, created_at DESC);
COMMENT ON TABLE lakehouse_notebook IS 'Persistent collaborative Lakehouse notebooks with capability-token access.';
COMMENT ON TABLE lakehouse_notebook_revision IS 'Immutable notebook snapshots for version history and restore.';
COMMENT ON TABLE lakehouse_notebook_share IS 'Revocable read/write share capabilities; only token hashes are stored.';
