CREATE TABLE curation_state (
    id VARCHAR(255) PRIMARY KEY,
    processed_content TEXT NOT NULL,
    processed_version VARCHAR(255),
    pending_content TEXT,
    pending_version VARCHAR(255),
    pending_since TIMESTAMP WITH TIME ZONE
);
