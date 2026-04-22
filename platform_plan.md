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
- **Impoundment** — fire that caused the dam, resistance, artesian wells, construction, flooding lawsuits
- **Carnival** — peak recreation, boats, camps, entertainment, mashing, dancing, Shriners, VIPs
- **Wartime** — WWI, WWII, alien enemies banned, soldiers writing home about the lake
- **Maximum usage** — by 1922 lake had reached its limit; cash cow; governance failures
- **Decline** — what happens when you don't watch a community treasure
- **Residents era** — communities forming around the lake
- **Dredging** — efforts to restore
- **Awakening** — present day and future

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
| **Claude API** (Haiku/Sonnet) | Enrichment agents | API |
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

## Enrichment Agents (Phase 1 → Phase 2)

### Agent 1 — Classifier (Phase 1)
- **Input**: article full_text + headline
- **Output**: article_type + article_flags + era assignment
- **Model**: Claude Haiku
- **Cost estimate**: ~$8-10 for 80k articles

### Agent 2 — Entity Extractor (Phase 1)
- **Input**: article full_text
- **Output**: camps, establishments, organizations, vessels, locations, events
- **Model**: Claude Haiku
- **Cost estimate**: ~$10-12 for 80k articles

### Agent 3 — Embedding Generator (Phase 1)
- **Input**: headline + full_text
- **Output**: vector stored in sqlite-vec
- **Model**: Voyage AI `voyage-large-2`
- **Cost estimate**: ~$5 for 80k articles (one-time)

### Agent 4 — Story Thread Tagger (Phase 2)
- **Input**: article + defined thread library
- **Output**: article_threads assignments with role
- **Model**: Claude Sonnet (more reasoning needed)
- **Note**: You define threads first; agent matches articles to them

### Agent 5 — Connection Builder (Phase 2)
- **Input**: article pairs with shared people/entities/threads
- **Output**: connections with relationship type and description
- **Model**: Claude Sonnet
- **Note**: Targeted — not all-against-all; driven by thread membership

### Agent 6 — Quote/People Re-enrichment (Phase 2)
- **Input**: articles from early extraction (may have missed quotes/people)
- **Output**: additional quotes and people records
- **Model**: Claude Haiku

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

## Build Order

### Phase 1 — Foundation
1. Enable SQLite WAL mode
2. Install and configure sqlite-vec, Alembic, Prefect, Voyage AI, NetworkX
3. Run Alembic migration: add all new tables
4. Build Agent 1 (Classifier) + run on existing articles
5. Build Agent 2 (Entity Extractor) + run on existing articles
6. Build Agent 3 (Embedding Generator) + run on existing articles
7. Add Article Browser to dashboard (filter by type, flag, era, keyword, semantic search)

### Phase 2 — Threads & Connections
1. Thread definition UI in dashboard
2. Build Agent 4 (Story Thread Tagger)
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
- Enrichment agents re-run automatically on new articles as clipper delivers them
- Source docs ingestion pipeline (books, documents) added in Phase 3

---

## Open Questions

- [ ] Street/park names around Lake Worth for cross-referencing — user will supply as heard
- [ ] Modern sources (post-1925 to 2026): newspapers, city records, what else?
- [ ] Priority threads to define first (which stories do you want to chase first?)
- [ ] Public portal / contribution features — defer until platform is stable

---

*This document is the working plan. Update as decisions are made.*
