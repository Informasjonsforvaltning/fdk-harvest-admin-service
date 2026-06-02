-- Composite indexes to support per-message lookups during harvest event processing.
CREATE INDEX idx_harvest_events_run_event ON harvest_events(harvest_run_id, event_type);
CREATE INDEX idx_harvest_events_run_fdk ON harvest_events(harvest_run_id, fdk_id);
CREATE INDEX idx_harvest_events_run_uri ON harvest_events(harvest_run_id, resource_uri);
