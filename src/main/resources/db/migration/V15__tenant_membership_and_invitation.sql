CREATE TABLE tenant_memberships (
    tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    subject VARCHAR(200) NOT NULL,
    member_role VARCHAR(20) NOT NULL CHECK (member_role IN ('OWNER','ADMIN','MEMBER','READ_ONLY')),
    status VARCHAR(20) NOT NULL DEFAULT 'ACTIVE' CHECK (status IN ('INVITED','ACTIVE','SUSPENDED')),
    invited_by VARCHAR(200) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY(tenant_id,subject)
);
CREATE INDEX idx_tenant_memberships_subject ON tenant_memberships(subject,status,tenant_id);
-- No implicit memberships are granted to existing accounts. A platform administrator
-- must explicitly add each subject to a tenant before it can use tenant-scoped APIs.
