-- Initial database schema for fdk-harvest-admin-service
-- This migration consolidates all previous migrations into a single file

-- ============================================================================
-- DATA SOURCES TABLE
-- ============================================================================
CREATE TABLE data_sources (
    id VARCHAR(36) PRIMARY KEY,
    data_source_type VARCHAR(50) NOT NULL,
    data_type VARCHAR(50) NOT NULL,
    url VARCHAR(2048) NOT NULL,
    accept_header VARCHAR(255),
    publisher_id VARCHAR(255) NOT NULL,
    description TEXT,
    CONSTRAINT chk_data_source_type CHECK (data_source_type IN ('SKOS-AP-NO', 'DCAT-AP-NO', 'CPSV-AP-NO', 'TBX', 'ModellDCAT-AP-NO')),
    CONSTRAINT chk_data_type CHECK (data_type IN ('concept', 'dataset', 'informationmodel', 'dataservice', 'publicService', 'event'))
);

CREATE INDEX idx_publisher_id ON data_sources(publisher_id);
CREATE INDEX idx_url_data_type ON data_sources(url, data_type);

-- ============================================================================
-- HARVEST PROGRESS EVENTS TABLE
-- ============================================================================
CREATE TABLE harvest_progress_events (
    id BIGSERIAL PRIMARY KEY,
    event_type VARCHAR(100) NOT NULL,
    data_source_id VARCHAR(255) NOT NULL,
    harvest_run_id VARCHAR(36),
    data_type VARCHAR(50) NOT NULL,
    data_source_url VARCHAR(2048),
    accept_header VARCHAR(255),
    fdk_id VARCHAR(255),
    resource_uri VARCHAR(2048),
    timestamp BIGINT NOT NULL,
    start_time VARCHAR(255),
    end_time VARCHAR(255),
    error_message TEXT,
    changed_resources_count INTEGER,
    unchanged_resources_count INTEGER,
    removed_resources_count INTEGER,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Basic indexes
CREATE INDEX idx_harvest_progress_data_source_id ON harvest_progress_events(data_source_id);
CREATE INDEX idx_harvest_progress_fdk_id ON harvest_progress_events(fdk_id);
CREATE INDEX idx_harvest_progress_event_type ON harvest_progress_events(event_type);
CREATE INDEX idx_harvest_progress_timestamp ON harvest_progress_events(timestamp);
CREATE INDEX idx_harvest_progress_events_harvest_run_id ON harvest_progress_events(harvest_run_id);

-- Composite indexes for better query performance
CREATE INDEX idx_harvest_events_data_source_type_timestamp ON harvest_progress_events(data_source_id, data_type, timestamp DESC);
CREATE INDEX idx_harvest_events_data_source_event_type_timestamp ON harvest_progress_events(data_source_id, event_type, timestamp DESC);
CREATE INDEX idx_harvest_events_data_source_type_event_type ON harvest_progress_events(data_source_id, data_type, event_type);

-- ============================================================================
-- HARVEST RUNS TABLE
-- ============================================================================
-- Table to track distinct harvest runs for performance analysis
-- Includes state tracking fields (consolidated from harvest_state table)
CREATE TABLE harvest_runs (
    id BIGSERIAL PRIMARY KEY,
    data_source_id VARCHAR(255) NOT NULL,
    data_type VARCHAR(50) NOT NULL,
    run_id VARCHAR(36) NOT NULL,
    run_started_at TIMESTAMP NOT NULL,
    run_ended_at TIMESTAMP,
    
    -- Phase timings (in milliseconds)
    init_duration_ms BIGINT,
    harvest_duration_ms BIGINT,
    reasoning_duration_ms BIGINT,
    rdf_parsing_duration_ms BIGINT,
    search_processing_duration_ms BIGINT,
    ai_search_processing_duration_ms BIGINT,
    api_processing_duration_ms BIGINT,
    sparql_processing_duration_ms BIGINT,
    total_duration_ms BIGINT,
    
    -- Resource counts
    total_resources INTEGER,
    changed_resources_count INTEGER,
    unchanged_resources_count INTEGER,
    removed_resources_count INTEGER,
    
    -- State tracking fields (consolidated from harvest_state)
    current_phase VARCHAR(100),
    phase_started_at TIMESTAMP,
    last_event_timestamp BIGINT,
    processed_resources INTEGER,
    remaining_resources INTEGER,
    
    -- Status
    status VARCHAR(50) NOT NULL, -- 'COMPLETED', 'FAILED', 'IN_PROGRESS'
    error_message TEXT,
    
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    
    CONSTRAINT uk_harvest_runs_run_id UNIQUE (run_id)
);

-- Indexes for harvest_runs
CREATE INDEX idx_harvest_runs_data_source_id ON harvest_runs(data_source_id);
CREATE INDEX idx_harvest_runs_data_type ON harvest_runs(data_type);
CREATE INDEX idx_harvest_runs_run_started_at ON harvest_runs(run_started_at);
CREATE INDEX idx_harvest_runs_status ON harvest_runs(status);
CREATE INDEX idx_harvest_runs_data_source_type ON harvest_runs(data_source_id, data_type);
CREATE INDEX idx_harvest_runs_run_id ON harvest_runs(run_id);

