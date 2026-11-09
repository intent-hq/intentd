-- Native tabs stay on their physical host and never gain page automation.
ALTER TABLE browser_tab ADD COLUMN navigation_only INTEGER NOT NULL DEFAULT 0;
