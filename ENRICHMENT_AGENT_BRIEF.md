# Enrichment Agent Build Brief
*For a fresh agent with no prior context.*

---

## What You Are Building

A Python script (`enrich_articles.py`) that reads newspaper articles from a SQLite database and enriches each one using a single Claude Haiku API call. This is for a historical newspaper archive project — Fort Worth Star-Telegram, 1909–1925. The data will be used to write books.

**This is a preservation project. Accuracy matters more than speed.**

---

## Files You Need To Read First

Before writing any code, read these files:

1. `c:\lake_worth\platform_plan.md` — full design decisions, era definitions, database schema
2. `c:\lake_worth\migrate_phase1.py` — shows exactly what new tables exist and their columns
3. `c:\lake_worth\agents_config.json` — the agent design specification
4. `c:\lake_worth\lake_worth.db` — the actual database (use sqlite3 to inspect schema and sample data)

---

## Database Location

`c:\lake_worth\lake_worth.db`

**Existing tables (DO NOT MODIFY their schema):**
- `articles` — id, date, newspaper, page, headline, full_text, has_photo, photo_description, search_term
- `quotes` — id, quote_text, speaker, speaker_role, context, article_id
- `people` — id, name, role, first_seen_date

**New tables (created by migrate_phase1.py — write to these):**
- `enrichment_runs` — id, agent_name, prompt_version, prompt_hash, model, started_at, completed_at, articles_processed, notes
- `article_type` — id, article_id, type, confidence, enrichment_run_id
- `article_flags` — id, article_id, importance, interesting, ironic, tragic, comic, cautionary, scandalous, notable_quote, enrichment_run_id
- `eras` — id, name, slug, date_start, date_end, description (already seeded with 7 eras)
- `article_eras` — id, article_id, era_id, confidence, enrichment_run_id
- `entities` — id, name, type, date_first, date_last, description, notes
- `article_entities` — id, article_id, entity_id, mention_text, source, verified, enrichment_run_id

---

## The 7 Eras (already in DB — query to get IDs)

| slug | name | dates |
|---|---|---|
| impoundment | Impoundment | 1909–1914 |
| discovery | Discovery | 1914–1917 |
| carnival | Carnival | 1917–1929 |
| depression_war | Depression & War | 1929–1945 |
| postwar_suburbs | Postwar Suburbs | 1945–1970 |
| decline | Decline | 1970–2000 |
| renewal | Renewal | 2000–present |

---

## Agent Design (Research-Backed — Do Not Change Without Reason)

### One combined Claude call per article

Research shows Haiku/Sonnet-class models perform better with combined multi-task prompts (7–12% improvement) due to the "look-ahead effect." Separate agents per article are NOT used.

### Prompt section order (stability order — most stable first, highest degradation risk last)

1. Article type classification
2. Importance + boolean flags
3. Era assignment
4. Entity extraction
5. Quote extraction ← last, gets the most prompt real estate

### Quote extraction rules (critical — this is preservation data)

- **Verbatim only** — never reconstruct, paraphrase, or clean up OCR noise
- **Return null speaker rather than guess** — hallucinated attribution is worse than no attribution
- **attribution_confidence field**: `explicit` (quotation marks present), `implied` (attribution language: "he said", "according to"), `inferred` (boundary inferred from context, no direct signal)
- Extract ALL quotes — over-extract rather than under-extract. We can filter later; we cannot recover missed quotes.

### Entity extraction rules

- Extract any named camp, establishment, organization, vessel, location, or event
- Store `mention_text` as the **exact text as it appeared** in the article
- Do not pre-define a rigid type taxonomy — use whatever type label fits (e.g., "camp", "pavilion", "steamer", "club", "park", "road")
- Entity deduplication (canonicalization) is a SEPARATE process — do not try to resolve duplicates here

### Article types

`news` | `editorial` | `letter` | `ad` | `social` | `legal` | `notice` | `obituary` | `stub` | `unknown`

### Stub handling

Articles with `full_text` under 50 characters should be auto-classified as `stub` and skipped for entity/quote extraction (saves API cost, nothing to extract).

---

## Technical Requirements

### Structured output

Use Claude's **native structured output** (`output_config` with JSON schema) — NOT just prompting for JSON. This uses constrained decoding and guarantees valid JSON matching your schema. Do not use tool_use as a workaround — use the proper structured output feature.

Wrap with **Pydantic validation** on top for semantic constraints (importance must be 1–5, confidence must be 0.0–1.0, etc.).

### Model

`claude-haiku-4-5-20251022` — pin to exact version, never use a floating alias.

### API approach for the first run (100 articles)

Use the **standard Messages API** (not Batch API) for the first 100-article test run so results come back immediately and can be reviewed. Add a `--batch` flag to switch to Batch API for large runs.

### Prompt caching

Cache the system prompt using the `cache_control` parameter (`type: "ephemeral"`). For a 100-article run this may not matter much, but design it in from the start.

### Enrichment run tracking

1. INSERT a row into `enrichment_runs` at the start of every run. Store the run ID.
2. Compute a SHA256 hash of the full system prompt + schema. Store in `prompt_hash`.
3. Every result record (article_type, article_flags, article_eras, article_entities, quotes, people) gets the `enrichment_run_id`.
4. UPDATE `enrichment_runs.completed_at` and `articles_processed` at the end.
5. If the run fails mid-way, `completed_at` stays NULL — this signals an incomplete run.

### Resumability

Before processing an article, check if it already has a record in `article_type` for the current run. If yes, skip it. This means a failed run can be resumed by re-running the script with the same run ID (add a `--resume-run N` flag).

### Skip already-enriched articles

Default behavior (no flags): process articles that have NEVER been enriched by ANY run. Use `--rerun` flag to re-process all articles regardless.

### enrichment_status.json

Write this file to `c:\lake_worth\enrichment_status.json` during processing so the Librarian dashboard can show live progress. Update it after each article. Delete (or set active_agent to null) when the run completes.

Format:
```json
{
  "active_agent": "enrichment",
  "run_id": 1,
  "articles_done": 45,
  "articles_total": 100,
  "current_articles": [
    {
      "id": 123,
      "headline": "FILES SUIT TO ENJOIN WORK AT LAKE WORTH",
      "date": "1914-07-03",
      "newspaper": "Fort Worth Star Telegram"
    }
  ]
}
```

`current_articles` should hold the last 5 processed articles (rolling window).

---

## Script Interface

```
python enrich_articles.py [options]

Options:
  --limit N         Process N articles (default: all unprocessed)
  --batch           Use Batch API instead of standard API (for large runs)
  --resume-run N    Resume an incomplete run by ID
  --rerun           Re-process articles even if already enriched
  --dry-run         Print what would be processed, make no API calls
```

---

## Claude API Key

Read from environment variable `ANTHROPIC_API_KEY`. The key is already set in the environment.

---

## Few-Shot Examples

Include 2-3 real examples from the database in the system prompt (query for articles with substantial text from early years). This is research-backed — few-shot examples consistently outperform zero-shot on historical OCR text.

Query to find good examples:
```sql
SELECT id, date, headline, full_text
FROM articles
WHERE length(full_text) > 500
AND date < '1916-01-01'
ORDER BY date
LIMIT 5
```

Pick 2-3 diverse ones (a news article, a social/event notice, a legal notice) and hand-craft their expected output as few-shot examples.

---

## Output Schema (Pydantic)

```python
class QuoteExtraction(BaseModel):
    quote_text: str                    # verbatim from source
    speaker: Optional[str]             # null if unknown — never guess
    speaker_role: Optional[str]
    context: Optional[str]             # 1 sentence describing when/why said
    attribution_confidence: str        # "explicit" | "implied" | "inferred"

class EntityExtraction(BaseModel):
    name: str                          # canonical name as you'd list it
    type: str                          # camp | establishment | vessel | org | location | event | etc.
    mention_text: str                  # exact text as printed in article

class PersonExtraction(BaseModel):
    name: str                          # full name as printed
    role: str                          # title/role as described in article

class ArticleEnrichment(BaseModel):
    article_type: str                  # news|editorial|letter|ad|social|legal|notice|obituary|stub|unknown
    type_confidence: float             # 0.0–1.0
    importance: int                    # 1–5
    interesting: bool
    ironic: bool
    tragic: bool
    comic: bool
    cautionary: bool
    scandalous: bool
    notable_quote: bool
    era_slug: str                      # one of the 7 era slugs
    era_confidence: float              # 0.0–1.0
    entities: List[EntityExtraction]
    quotes: List[QuoteExtraction]
    people: List[PersonExtraction]
```

---

## Writing Results to DB

After extracting enrichment for an article:

1. **article_type**: INSERT one row
2. **article_flags**: INSERT one row
3. **article_eras**: look up era_id from `eras` table by slug, INSERT one row
4. **article_entities**: for each entity, INSERT into `entities` table if new (check by name+type), get entity_id, INSERT into `article_entities`
5. **quotes**: INSERT into existing `quotes` table (article_id, quote_text, speaker, speaker_role, context) — add `attribution_confidence` and `enrichment_run_id` columns if they don't exist yet (use ALTER TABLE ADD COLUMN IF NOT EXISTS)
6. **people**: INSERT into existing `people` table (name, role, first_seen_date=article.date) — INSERT OR IGNORE since name is UNIQUE

Use transactions — wrap each article's writes in a BEGIN/COMMIT. If an article fails, roll back and log the error, continue to the next article.

---

## Logging

Print to stdout:
```
[RUN 1] Processing 100 articles | Model: claude-haiku-4-5-20251022 | Prompt hash: abc123
[  1/100] 1914-07-03 | FILES SUIT TO ENJOIN... | type=news era=impoundment imp=3 entities=2 quotes=1
[  2/100] 1914-07-07 | PLAN TO GIVE NOTES... | type=legal era=impoundment imp=2 entities=1 quotes=0
...
[100/100] Done. 100 processed, 2 failed, 98 successful.
[RUN 1] Completed in 4m 23s
```

---

## After Building

1. Update `agents_config.json` — set `prompt_template` to the actual system prompt used, set `status` to `"ready"` for the enrichment agent
2. Run `python enrich_articles.py --dry-run --limit 5` to verify it would process articles correctly
3. Report back with: the script, the prompt, any design decisions made, and dry-run output

---

## Important Constraints

- **Do not modify** the existing `articles`, `quotes` (schema), or `people` (schema) tables beyond adding new columns if needed
- **Do not drop** any tables or delete any data
- **Do not reorder** or rename existing columns
- The script must be idempotent — running it twice on the same articles produces no duplicates
- Use `INSERT OR IGNORE` or `INSERT OR REPLACE` carefully — never silently overwrite good data
- All file paths use `c:\lake_worth\` as base. Use `os.path` for cross-platform safety.
