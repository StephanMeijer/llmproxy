ALTER TABLE provider_credentials DROP CONSTRAINT provider_credentials_provider_check,
    ADD CONSTRAINT provider_credentials_provider_check CHECK (provider IN ('anthropic', 'chatgpt'));
ALTER TABLE limit_definitions DROP CONSTRAINT limit_definitions_provider_check,
    ADD CONSTRAINT limit_definitions_provider_check CHECK (provider IN ('anthropic', 'chatgpt'));
