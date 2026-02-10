-- Authentication and authorization tables

-- API keys for machine-to-machine authentication
CREATE TABLE api_keys (
    key_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    key_hash BYTEA NOT NULL,  -- bcrypt hash of the API key
    name VARCHAR(255) NOT NULL,
    principal VARCHAR(255) NOT NULL,  -- The identity this key represents
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    expires_at TIMESTAMPTZ,
    revoked_at TIMESTAMPTZ,
    last_used_at TIMESTAMPTZ
);

-- Index for key lookups
CREATE INDEX idx_api_keys_principal ON api_keys (principal);

-- Access control lists
CREATE TABLE acls (
    acl_id SERIAL PRIMARY KEY,
    principal VARCHAR(255) NOT NULL,  -- User or service identity
    resource_type VARCHAR(50) NOT NULL,  -- 'topic', 'group', 'cluster'
    resource_name VARCHAR(255) NOT NULL,  -- Specific resource or '*' for wildcard
    operation VARCHAR(50) NOT NULL,  -- 'produce', 'consume', 'admin', etc.
    permission VARCHAR(10) NOT NULL CHECK (permission IN ('allow', 'deny')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Index for ACL lookups
CREATE INDEX idx_acls_principal ON acls (principal, resource_type, resource_name);

-- Unique constraint to prevent duplicate ACLs
CREATE UNIQUE INDEX idx_acls_unique
ON acls (principal, resource_type, resource_name, operation);

-- Insert default admin ACL (allow admin principal to do anything)
INSERT INTO acls (principal, resource_type, resource_name, operation, permission)
VALUES ('admin', 'cluster', '*', 'admin', 'allow');
