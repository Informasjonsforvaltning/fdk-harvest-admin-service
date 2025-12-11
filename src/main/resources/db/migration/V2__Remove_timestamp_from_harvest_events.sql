-- Remove timestamp column and rename table from harvest_progress_events to harvest_events
-- Since we now use startTime and endTime for event timing

-- Drop indexes that reference timestamp
DROP INDEX IF EXISTS idx_harvest_progress_timestamp;
DROP INDEX IF EXISTS idx_harvest_events_data_source_type_timestamp;
DROP INDEX IF EXISTS idx_harvest_events_data_source_event_type_timestamp;

-- Drop the timestamp column entirely
ALTER TABLE harvest_progress_events DROP COLUMN IF EXISTS timestamp;

-- Rename the table
ALTER TABLE harvest_progress_events RENAME TO harvest_events;

-- Drop old indexes (they will be automatically dropped with the table rename, but we'll recreate them with new names)
DROP INDEX IF EXISTS idx_harvest_progress_data_source_id;
DROP INDEX IF EXISTS idx_harvest_progress_fdk_id;
DROP INDEX IF EXISTS idx_harvest_progress_event_type;
DROP INDEX IF EXISTS idx_harvest_progress_events_harvest_run_id;
DROP INDEX IF EXISTS idx_harvest_events_data_source_type_event_type;

-- Recreate indexes with new names
CREATE INDEX idx_harvest_events_data_source_id ON harvest_events(data_source_id);
CREATE INDEX idx_harvest_events_fdk_id ON harvest_events(fdk_id);
CREATE INDEX idx_harvest_events_event_type ON harvest_events(event_type);
CREATE INDEX idx_harvest_events_harvest_run_id ON harvest_events(harvest_run_id);
CREATE INDEX idx_harvest_events_data_source_type_event_type ON harvest_events(data_source_id, data_type, event_type);

-- Create new indexes using created_at instead of timestamp
CREATE INDEX idx_harvest_events_data_source_type_created_at ON harvest_events(data_source_id, data_type, created_at DESC);
CREATE INDEX idx_harvest_events_data_source_event_type_created_at ON harvest_events(data_source_id, event_type, created_at DESC);
