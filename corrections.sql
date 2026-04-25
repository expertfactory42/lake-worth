-- Lake Worth Archive — Manual Corrections
-- Format: -- DATE: description
--         SQL statement(s)
-- Apply via: python apply_corrections.py
-- Or via the Librarian Tools tab > Corrections > Apply to DB
--
-- Rules:
--   - Fix canonical names in: entities.canonical_name, people.canonical_name
--   - Never modify: full_text, mention_text, quote_text (archival/verbatim)
--   - Always end statements with a semicolon

-- 2026-04-22: OCR error — MeCart should be McCart (City Attorney)
UPDATE entities SET canonical_name = 'McCart' WHERE canonical_name = 'MeCart';
