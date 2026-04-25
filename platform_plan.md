# Lake Worth Knowledge Engine — Working Plan
*Last updated: 2026-04-22 | Status: Under Discussion*

---

## Vision

A private semantic knowledge engine built from 80-100k newspaper articles (1909–2026) plus historical books and documents. Goal: excellent control of information, links, causes, people, connections, causes, effects, stories, irony, tragedy — queryable on demand. Used to generate books, articles, blogs, timelines, maps, coloring books, videos, and other materials. Foundation for a non-profit movement to connect the Lake Worth community to its past and future.

---

## Source Data

- **Primary**: Fort Worth Star-Telegram, Fort Worth Record-Telegram (1909–1925, actively collecting)
- **Future**: Newspapers continuing to 2026, historical books, documents, city records, photos, maps
- **Current state**: ~1,784 articles compiled 1913–1918; extraction at ~1925; target 80-100k articles

---

## Three Reader Layers (Book Goals)

1. **Casual thumbing** — timelines, maps, short stories, visual arc of history
2. **Narrative readers** — carried along by story and character
3. **Reference / decision-makers** — quotable, citable, useful for policy and perspective

---

## Book Structure

### Decade sections
1910s · 1920s · 1930s · 1940s · 1950s · 1960s · 1970s · 1980s · 1990s · 2000s · 2010s · 2020s

### Era narrative arcs (cross-decade)

Two parallel structures coexist — decades (for navigation) and narrative eras (for meaning). An article belongs to a decade AND an era. They are separate dimensions.

| Era | Dates | Character |
|---|---|---|
| **Impoundment** | 1909–1914 | The fire, the fight. Reservoir vs. artesian wells. Property battles, dam construction, the lake fills. Defined by conflict and necessity. |
| **Discovery** | 1914–1917 | Lake is new, no plan for it. Organic recreation emerges — fishing, camping, swimming. First camps appear. Flooding lawsuits. City figures out what it has. |
| **Carnival** | 1917–1929 | Amusement park opens. Interurban brings day-trippers. Camps multiply. Prohibition + Jacksboro Highway drive bootlegging alongside dancing and boats. Shriners, VIPs, mashing, drama. Peak attendance, peak chaos. |
| **Depression & War** | 1929–1945 | Carnival fades. Depression cuts attendance. WWII shifts energy. Lake persists but commercial peak passes. |
| **Postwar Suburbs** | 1945–1970 | Fort Worth grows outward. Lake becomes a neighborhood lake. Siltation accumulates. Operators close. |
| **Decline** | 1970–2000 | Governance failures, inadequate dredging, encroachment. Community treasure quietly deteriorates. |
| **Renewal** | 2000–present | Dredging projects, community advocacy, growing awareness of historical and ecological value. |

*Note: Current articles (1913–1925) fall almost entirely in Impoundment, Discovery, and Carnival. Later eras will fill in as collection extends into the 20th century.*

*Prohibition / Jacksboro Highway is a cross-cutting thread within Carnival and into Depression & War — not its own era.*

### Long narrative threads (span multiple eras)
- The lake at war
- Amon Carter (publisher, lived and entertained at the lake)
- Community park, entertainment, and meeting place
- Governance and management through the years
- The interurban story
- School district destruction and horse-bus "bussing"
- Comparisons to similar water bodies worldwide (success, failure, reinvention)
- Fish, animals, wildlife
- Transportation (interurban, boats, cars, airplanes)
- Oil and gas
- Cautionary tale: neglect of a community treasure

### Topic threads (shorter, can appear in multiple eras)
- Naming the lake (Minnetonka → Lake Worth)
- Resistance to building / artesian wells
- The fire
- Dam construction and flood lawsuits
- Recreation: fishing, boating, bathing, dancing, camping
- "Mashing" / "smash the masher"
- Modern health crusade
- Cost of water / waterworks financials
- Civil war veterans
- VIPs visiting
- Athletic events
- Car wrecks / speeding arrests
- Drownings and deaths
- Camps (all names)
- Establishments (all names — hotels, pavilions, clubs, boats)
- Shriners and organizations
- Early aviation / airplanes
- City sued (flooding, car theft, etc.)
- Forger / bigamist stories
- Start of gambling, dancing, bootlegging
- Titanic effect on life preservers?
- Commercial fishing (100,000 lbs/month)
- 113lb turtle
- Animals: wolves, deer, ducks, turtles
- New law to close lake Sundays
- Welfare camp / work camps
- Political promises and proposals about the lake
- "Avoid the lake if you're full of jake"
- Important women and ethnic people/groups
- People whose names are on current streets/parks

### Book goal: connections
- People to nature
- Park to park
- Past to present
- Present to future

---

## Technical Stack (Agreed)

| Component | Purpose | Runs |
|---|---|---|
| **SQLite** (WAL mode) | Primary DB — all article data, enrichment, embeddings | Local |
| **sqlite-vec** | Vector embeddings stored in SQLite | Local |
| **NetworkX** (Python) | Graph queries built from SQLite on demand | Local |
| **Prefect** | Pipeline orchestration — resumable, observable enrichment runs | Local |
| **Alembic** | Schema migrations — versioned, rollback-capable | Local |
| **Claude API** (Sonnet) | Enrichment agents | API |
| **Voyage AI API** | Embeddings (~$5 one-time for full corpus) | API |

**Machine**: Dell Latitude 7400, i7-8665U, CPU-only (no GPU). All data stays local. Only Claude API and Voyage AI calls go out.

---

## Database Design

### Existing tables (DO NOT MODIFY)
- `articles` — core article content
- `quotes` — extracted direct quotes with speaker/role/context
- `people` — named people with roles and first-seen dates
- `tags` — 15-category topic tags per article
- `images` — cropped newspaper images with captions
- `processed_pdfs` — clipper state per page
- `clipper_state`, `clipper_instances`, `accounts` — operational state

### New tables to add (Phase 1)

**`article_type`**
```
article_id INTEGER REFERENCES articles(id)
type TEXT  -- news | editorial | letter | ad | social | legal | notice | obituary
confidence FLOAT
enrichment_run_id INTEGER REFERENCES enrichment_runs(id)
```

**`article_flags`**
```
article_id INTEGER REFERENCES articles(id)
importance INTEGER  -- 1-5
interesting BOOLEAN
ironic BOOLEAN
tragic BOOLEAN
comic BOOLEAN
cautionary BOOLEAN
scandalous BOOLEAN
notable_quote BOOLEAN
enrichment_run_id INTEGER REFERENCES enrichment_runs(id)
```

**`eras`**
```
id INTEGER PRIMARY KEY
name TEXT  -- impoundment | carnival | wartime | maximum_usage | decline | residents | dredging | awakening
date_start TEXT
date_end TEXT
description TEXT
```

**`article_eras`**
```
article_id INTEGER REFERENCES articles(id)
era_id INTEGER REFERENCES eras(id)
```

**`story_threads`**
```
id INTEGER PRIMARY KEY
name TEXT
description TEXT
era_id INTEGER REFERENCES eras(id)  -- primary era (can span multiple)
created_by TEXT  -- 'human' | 'ai'
created_at TEXT
```

**`article_threads`**
```
article_id INTEGER REFERENCES articles(id)
thread_id INTEGER REFERENCES story_threads(id)
role TEXT  -- origin | development | peak | resolution | footnote | parallel
confidence FLOAT
source TEXT  -- 'ai' | 'human'
verified INTEGER DEFAULT 0
enrichment_run_id INTEGER REFERENCES enrichment_runs(id)
```

**`entities`**
```
id INTEGER PRIMARY KEY
name TEXT
type TEXT  -- camp | establishment | organization | vessel | location | event | landmark
date_first TEXT
date_last TEXT
description TEXT
notes TEXT
```

**`article_entities`**
```
article_id INTEGER REFERENCES articles(id)
entity_id INTEGER REFERENCES entities(id)
mention_text TEXT  -- exact text as it appeared
source TEXT  -- 'ai' | 'human'
verified INTEGER DEFAULT 0
enrichment_run_id INTEGER REFERENCES enrichment_runs(id)
```

**`connections`**
```
id INTEGER PRIMARY KEY
article_a_id INTEGER REFERENCES articles(id)
article_b_id INTEGER REFERENCES articles(id)
relationship TEXT  -- cause | effect | irony | callback | contrast | parallel | continuation
description TEXT  -- why these are connected
source TEXT  -- 'ai' | 'human'
verified INTEGER DEFAULT 0
enrichment_run_id INTEGER REFERENCES enrichment_runs(id)
created_at TEXT
```

**`book_sections`**
```
id INTEGER PRIMARY KEY
title TEXT
type TEXT  -- era | thread | topic | sidebar | spotlight
era_id INTEGER REFERENCES eras(id)
thread_id INTEGER REFERENCES story_threads(id)
sort_order INTEGER
notes TEXT
```

**`article_section_notes`**
```
article_id INTEGER REFERENCES articles(id)
section_id INTEGER REFERENCES book_sections(id)
user_notes TEXT
sort_order INTEGER
created_at TEXT
```

**`enrichment_runs`**
```
id INTEGER PRIMARY KEY
agent_name TEXT
prompt_version TEXT
started_at TEXT
completed_at TEXT
articles_processed INTEGER
notes TEXT
```

**`source_docs`** *(for future: books, documents beyond newspapers)*
```
id INTEGER PRIMARY KEY
title TEXT
author TEXT
date TEXT
doc_type TEXT  -- book | report | map | photo | city_record | oral_history
file_path TEXT
ingested_at TEXT
notes TEXT
```

---

## Enrichment Run Tracking

Every enrichment result record (article_type, article_flags, article_eras, article_entities, quotes, people) has an `enrichment_run_id` column pointing to the `enrichment_runs` table.

This means you can always answer:

- **Which runs have touched article A?** — query any result table for records where article_id = A, join to enrichment_runs
- **Which articles has run N not touched yet?** — query articles WHERE id NOT IN (SELECT article_id FROM article_type WHERE enrichment_run_id = N) — this is how the agent knows what to resume after a partial run or failure
- **What prompt/model produced this result?** — every record traces back to a run with agent_name, prompt_version, and prompt_hash

**What you cannot infer:** whether a run was *supposed to* cover an article but skipped it vs. was never designed to touch it. Absence of a record simply means "not done."

**Re-run behavior:** improving a prompt creates a new run. Old results are not overwritten. Both versions exist and can be compared (e.g., "show me articles where run 1 and run 7 disagree on era assignment").

**Prompt hash tracking:** Every enrichment run stores a hash of the full prompt (system prompt + schema). When the prompt changes, the hash changes. Query `WHERE prompt_hash != current_hash` to find all articles needing re-enrichment. Model version is also stored (pinned, never floating — e.g., `claude-haiku-4-5-20251022` not `claude-haiku-latest`) so output distributions don't silently shift on provider upgrades.

---

## Enrichment Agent Design (Research-Based)

*Revised 2026-04-22 based on multi-task inference research and Anthropic batch API documentation.*

### Core finding: one combined agent per article, not separate agents

Research (arXiv 2402.11597, Zhao et al.) shows Haiku/Sonnet-class models gain 7–12% performance improvement from multi-task inference vs. single-task — the opposite of smaller models. The "look-ahead effect" (seeing all task requirements at once) helps larger models plan better. Combined with a 1.46x speedup, separate agents per article are not justified.

**Exception:** Canonicalization and embeddings remain separate (see below).

### Single enrichment agent — combined call per article (Phase 1)

- **Model**: Claude Sonnet (`claude-sonnet-4-6`) — Haiku was tested and found inadequate for the quality requirements of this preservation project (entity extraction, quote attribution, and era assignment all suffered)
- **Prompt caching**: system prompt cached at 1-hour TTL — ~85% savings on input tokens across a batch
- **Structured output**: Claude native `output_config.format` constrained decoding (guarantees valid JSON matching schema — not just "ask for JSON"). Pydantic validation layer on top for semantic constraints.
- **Input**: headline + full_text (skip articles under 50 chars — auto-classify as `stub`, skip extraction)
- **Few-shot examples**: 2–3 real examples from the corpus embedded in system prompt — consistently outperforms zero-shot on historical NER and OCR-degraded text

**Prompt section order** (research shows ordering affects multi-task output quality — stable tasks first, most sensitive last):
1. Summary (plain English index card entry)
2. Article type classification (most stable under multi-task)
3. Era assignment
4. Entity extraction
5. Quote extraction (highest degradation risk — give most prompt real estate, last position)

**Output fields per article (Phase 1 — Ingestion):**
- `summary`: 1-3 sentences of plain English describing what happened, who was involved, and what was decided or revealed. Researcher's index card entry.
- `article_type`: news | editorial | letter | ad | social | legal | notice | obituary | stub | unknown
- `era`: one of the 7 defined eras, with `era_confidence` float
- `entities`: list of {name, type, mention_text (exact as printed)}; types emerge from data — do not pre-define
- `quotes`: list of {quote_text (verbatim only — never paraphrase), speaker (null if unknown — never guess), speaker_role, context, attribution_confidence: explicit | implied | inferred}

*Note: `article_flags` table (importance, interesting, ironic, tragic, comic, cautionary, scandalous, notable_quote) is retained in the schema for Phase 2 interpretation use. These fields are NOT populated during ingestion.*

**Quote extraction rules** (research-backed):
- Verbatim extraction only — never reconstruct or paraphrase OCR-damaged text
- Return the raw OCR span around the quote as evidence
- `attribution_confidence`: `explicit` (quotation marks present), `implied` (attribution language: "he said," "according to"), `inferred` (boundary inferred from context, no direct signal)
- Return `null` speaker rather than guess — hallucinated attribution is worse than no attribution for a preservation project

**Cost estimate (Sonnet, prompt caching):**
- ~$300 for 12k articles (actual, observed)
- ~$2,000 for 80k articles (projected from actual)

### Canonicalizer — separate post-processing pass (Phase 1, runs after corpus)

Not per-article. Runs after the enrichment agent has processed the full corpus (or a large batch).

- **Why separate**: entity canonicalization requires seeing the full set of mentions to resolve variants ("Camp Barnard" vs. "the Barnard camp"). Per-article extraction can't do this.
- **Process**: (1) Haiku clusters entity mentions by text similarity into candidate groups, (2) Sonnet resolves ambiguous clusters — decides if variants are the same entity, (3) writes canonical records to `entities` table with `mention_variants` array
- **Runs**: nightly incremental job after new articles arrive

### Embedding generator — separate (Phase 1)

- **Model**: Voyage AI `voyage-large-2`
- **Input**: headline + full_text
- **Output**: vector stored in sqlite-vec
- **Cost**: ~$5 one-time for 80k articles
- **Timing**: after enrichment agent, enables semantic search in Article Browser

### Story Thread Tagger (Phase 2)

- **Model**: Claude Sonnet
- **Input**: article + defined thread library
- **Output**: article_threads assignments with role (origin | development | peak | resolution | footnote | parallel)
- **Note**: threads must be defined by hand first; agent matches articles to them

### Connection Builder (Phase 2)

- **Model**: Claude Sonnet
- **Input**: article pairs with shared people/entities/threads (targeted — not all-against-all)
- **Output**: connections with relationship type (cause | effect | irony | callback | contrast | parallel | continuation) and description
- **Note**: driven by thread membership to keep candidate pairs manageable

---

## Quality Evaluation (Research-Based)

*The 100-article first run is the permanent gold set. Do not discard it.*

**Gold set**: 100 articles run first, manually reviewed. Every time a prompt changes, Sonnet-as-judge evaluates Haiku outputs against gold set before re-enriching full corpus. This is cheaper than full human review and more accurate than string-matching metrics (LLM-as-judge achieves 0.85 Pearson correlation vs. humans on extractive tasks).

**Metrics to track per enrichment run:**
- Validation failure rate (% of responses failing Pydantic schema)
- Field population rate (% non-null per field — unexpected drops signal extraction problems)
- Average entities per article
- Average quotes per article
- Cost per record (by model)
- Gold set precision/recall (after any prompt change)

**Sample size guidance** (from literature): 50–100 manually annotated documents is standard for extractive IE evaluation. 2–5% spot-check on ongoing runs.

---

## Frontend (extends existing dashboard)

### New sections to add

**Article Browser**
- Filter by: era, type, flag, thread, entity, person, date range, keyword, semantic search
- Sort by: date, importance, flag type
- Show: headline, date, snippet, flags, thread memberships

**Story Threads**
- List all threads with article counts
- Thread view: articles in chronological order with roles
- Manual article assignment / override
- Thread notes / narrative summary

**People & Entities Directory**
- All people with article count, date range, connections
- All entities (camps, establishments, etc.) with same
- Cross-reference: people ↔ entities ↔ threads

**Quote Explorer**
- Filter by speaker, era, topic, thread, keyword
- Bulk export for book sections

**Connections View**
- Article pairs with relationship type
- Human verification queue (AI-suggested, awaiting review)
- Filter by: relationship type, thread, era, verified status

**Timeline**
- Article density by month/year
- Colored by era
- Clickable to open article browser filtered to that period

**Book Workspace**
- Define/edit sections and chapters
- Drag articles into sections
- Add annotations per article
- Export: article list, quotes, full text, outlines

---

## Risks & Mitigations (Resolved)

| Risk | Mitigation |
|---|---|
| SQLite concurrent write contention (clipper + agents) | WAL mode + agents batch-write in transactions |
| Dual-DB sync (original Neo4j proposal) | **Dropped** — use NetworkX in-memory graph from SQLite instead |
| No schema migration tooling | Alembic for all new table additions |
| Re-enrichment versioning | `enrichment_runs` table — every record links to a run ID |
| AI connection quality | `source` + `verified` fields — human review queue before trust |
| sqlite-vec maturity | Acceptable risk at this scale; fallback to Chroma if needed |

---

## Decisions Made (2026-04-22)

### Decision: Ingestion vs. Interpretation split (2026-04-22)

The enrichment pipeline is split into two fundamentally different kinds of work:

**Ingestion** — what is in this article? Factual, mechanical, context-free. Can be done article by article in isolation. Results are stable and don't change as you learn more about the corpus.

**Interpretation** — what does this mean? Requires knowing the whole corpus. Significance, irony, cautionary value, connections — these are hindsight judgments that require pattern recognition across the full dataset. An article is only "ironic" or "cautionary" once you know what comes after it.

**Ingestion produces:** summary, type, era, entities, quotes, people.

**Interpretation produces (Phase 2):** importance scores, flags (interesting, ironic, tragic, comic, cautionary, scandalous, notable_quote), story thread assignments, connections, significance ratings.

Conflating ingestion and interpretation at the per-article level produces arbitrary and misleading flags — the model cannot know what's significant without context. Flags are deferred to Phase 2 when the corpus is large enough to reason about patterns.

### Entity types
Do not pre-define a rigid taxonomy. Start the entity extractor with a broad "extract any named place, organization, or vessel" prompt. Let the real categories emerge from the first 100–300 articles, then refine the type vocabulary based on what actually shows up (camps, pavilions, steamers, organizations, etc.).

### Quote philosophy
Extract as many direct quotes as possible — over-extract rather than under-extract. Quotes have the most power for the book. Even partial or unattributed quotes should be captured. Add a `speaker_known` flag to distinguish attributed vs. unattributed. We can filter later; we can't recover missed quotes.

### Quality validation strategy
Do not run blind on 15k articles. The approach:
1. **First batch: 100 articles in date order** — run enrichment, review all results manually
2. **Review frontend first** — build a minimal "Enrichment" tab in the dashboard before scaling, so results are readable as they come in, not just at the end
3. **Distribution checks after every run** — look at type/era/flag distributions; unexpected skews signal prompt problems
4. **Prompt iteration** — adjust prompts based on review, re-run the same 100 articles as a new run, compare
5. **Scale only after satisfied** — do not run full 15k until the 100-article review passes

### Run cadence
Manual batch runs for now. Automated re-enrichment on new clipper articles deferred until prompt quality is validated.

### Enrichment agent design (single call per article)
One Claude Sonnet call per article extracts everything at once (Phase 1 — Ingestion only):
- Summary (1-3 sentences, plain English, researcher's index card)
- Article type (news / editorial / letter / ad / social / legal / notice / obituary / unknown)
- Era assignment (which of the 7 narrative eras, with confidence)
- Entities (name + type + exact text as it appeared in the article)
- Quotes (every direct quote — text + speaker if known + context)

Flags (importance 1–5 + boolean: interesting, ironic, tragic, comic, cautionary, scandalous, notable_quote) are Phase 2 — Interpretation only. See "Decision: Ingestion vs. Interpretation split" above.

---

## Build Order

### Current status: Planning complete, nothing built yet for Phase 1 (as of 2026-04-22)

### Next steps (immediate)
1. **Migration script** (`migrate_phase1.py`) — adds all new tables to existing DB; pre-seeds `eras` table with the 7 era definitions; no Alembic yet, plain Python
2. **Enrichment agent** (`enrich_articles.py`) — single Haiku call per article; `--limit N` flag; skips already-enriched articles; links all results to an `enrichment_run` record
3. **Review tab in dashboard** — new "Enrichment" tab: article list with type/era/flag badges, click to see full text + all enrichment side by side; filter by era/type/importance; read-only first pass
4. **Run on 100 articles** — oldest 100 by date; user reviews and gives feedback on prompt quality and frontend usability
5. **Iterate** — adjust prompts, re-run, compare runs

### Phase 1 — Foundation (revised)
1. ~~Enable SQLite WAL mode~~ (already on)
2. Migration script: add all new tables (no Alembic yet — add when schema stabilizes)
3. Build enrichment agent (combined classifier + entity extractor + quote extractor in one call)
4. Build review tab in dashboard
5. Run on 100 articles, review, iterate until satisfied
6. Run on full article corpus (~15k)
7. Install sqlite-vec + Voyage AI; run embedding generator
8. Expand Article Browser with semantic search

### Phase 2 — Threads & Connections
1. Thread definition UI in dashboard
2. Build Agent 4 (Story Thread Tagger) — Sonnet, uses defined thread library
3. Thread browser in dashboard
4. Build Agent 5 (Connection Builder) targeted at priority threads
5. Connections view + human verification queue in dashboard

### Phase 3 — Book Workspace
1. Section/chapter definition
2. Article annotation and placement
3. Export pipeline (text, quotes, outlines)
4. Timeline visualization
5. People/Entities directory

### Ongoing
- Enrichment agents re-run on new articles as clipper delivers them (after Phase 1 is validated)
- Source docs ingestion pipeline (books, documents) added in Phase 3

---

## Open Questions

- [ ] Street/park names around Lake Worth for cross-referencing — user will supply as heard
- [ ] Modern sources (post-1925 to 2026): newspapers, city records, what else?
- [ ] Priority threads to define first (which stories do you want to chase first?)
- [ ] Public portal / contribution features — defer until platform is stable
- [ ] Exact year amusement park opened (hard boundary between Discovery and Carnival eras)
- [ ] Era date refinements — current dates are estimates; adjust as articles reveal actual turning points
- [ ] Whether to add Alembic now or after schema stabilizes through 100-article iteration

---

*This document is the working plan. Update as decisions are made.*
*Last updated: 2026-04-22*
