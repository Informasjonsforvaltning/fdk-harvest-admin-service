-- Remove timestamp column and rename table from harvest_progress_events to harvest_events
-- Since we now use startTime and endTime for event timing

-- Step 1: Drop all indexes first to reduce lock contention
DROP INDEX IF EXISTS idx_harvest_progress_timestamp;
DROP INDEX IF EXISTS idx_harvest_events_data_source_type_timestamp;
DROP INDEX IF EXISTS idx_harvest_events_data_source_event_type_timestamp;
DROP INDEX IF EXISTS idx_harvest_progress_data_source_id;
DROP INDEX IF EXISTS idx_harvest_progress_fdk_id;
DROP INDEX IF EXISTS idx_harvest_progress_event_type;
DROP INDEX IF EXISTS idx_harvest_progress_events_harvest_run_id;
DROP INDEX IF EXISTS idx_harvest_events_data_source_type_event_type;

-- Step 2: First make timestamp nullable (faster than dropping directly)
-- This allows the column to be dropped more efficiently in the next step
ALTER TABLE harvest_progress_events ALTER COLUMN timestamp DROP NOT NULL;

-- Step 2b: Drop the timestamp column (now that it's nullable, this should be faster)
ALTER TABLE harvest_progress_events DROP COLUMN timestamp;

-- Step 3: Rename the table (requires exclusive lock, but should be quick)
ALTER TABLE harvest_progress_events RENAME TO harvest_events;

-- Step 4: Recreate indexes with new names
CREATE INDEX idx_harvest_events_data_source_id ON harvest_events(data_source_id);
CREATE INDEX idx_harvest_events_fdk_id ON harvest_events(fdk_id);
CREATE INDEX idx_harvest_events_event_type ON harvest_events(event_type);
CREATE INDEX idx_harvest_events_harvest_run_id ON harvest_events(harvest_run_id);
CREATE INDEX idx_harvest_events_data_source_type_event_type ON harvest_events(data_source_id, data_type, event_type);

-- Step 5: Create new indexes using created_at instead of timestamp
CREATE INDEX idx_harvest_events_data_source_type_created_at ON harvest_events(data_source_id, data_type, created_at DESC);
CREATE INDEX idx_harvest_events_data_source_event_type_created_at ON harvest_events(data_source_id, event_type, created_at DESC);

