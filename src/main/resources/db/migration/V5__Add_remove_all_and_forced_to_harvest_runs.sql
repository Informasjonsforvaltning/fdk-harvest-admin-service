-- Add removeAll and forced (INITIATING options) to harvest runs
ALTER TABLE harvest_runs ADD COLUMN remove_all BOOLEAN;
ALTER TABLE harvest_runs ADD COLUMN forced BOOLEAN;
