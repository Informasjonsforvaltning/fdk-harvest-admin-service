-- Remove partially_processed_resources column
ALTER TABLE harvest_runs DROP COLUMN IF EXISTS partially_processed_resources;

-- Add event count columns for each phase
ALTER TABLE harvest_runs ADD COLUMN initiating_events_count INTEGER DEFAULT 0;
ALTER TABLE harvest_runs ADD COLUMN harvesting_events_count INTEGER DEFAULT 0;
ALTER TABLE harvest_runs ADD COLUMN reasoning_events_count INTEGER DEFAULT 0;
ALTER TABLE harvest_runs ADD COLUMN rdf_parsing_events_count INTEGER DEFAULT 0;
ALTER TABLE harvest_runs ADD COLUMN resource_processing_events_count INTEGER DEFAULT 0;
ALTER TABLE harvest_runs ADD COLUMN search_processing_events_count INTEGER DEFAULT 0;
ALTER TABLE harvest_runs ADD COLUMN ai_search_processing_events_count INTEGER DEFAULT 0;
ALTER TABLE harvest_runs ADD COLUMN sparql_processing_events_count INTEGER DEFAULT 0;

