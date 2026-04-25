"""
enrich_articles.py — Enrichment agent for the Lake Worth newspaper archive.

Single Claude Sonnet call per article. Extracts everything in one pass.
Uses Claude native structured output (output_config with JSON schema) + Pydantic validation.
Writes enrichment_status.json for live dashboard monitoring.

Usage:
  python enrich_articles.py [--limit N] [--batch] [--resume-run N] [--rerun] [--dry-run]

Options:
  --limit N         Process N articles (default: all unprocessed)
  --batch           Use Batch API instead of standard API (for large runs)
  --resume-run N    Resume an incomplete run by ID
  --rerun           Re-process articles even if already enriched
  --dry-run         Print what would be processed, make no API calls
"""

import argparse
import hashlib
import json
import os
import sqlite3
import sys
import time
from collections import deque
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional

import anthropic
from pydantic import BaseModel, field_validator, model_validator

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

DB_PATH = Path(r"C:\lake_worth\lake_worth.db")
STATUS_FILE = Path(r"C:\lake_worth\enrichment_status.json")
MODEL = "claude-sonnet-4-6"
AGENT_NAME = "enrichment"
PROMPT_VERSION = "1.0"
STUB_THRESHOLD = 50   # characters — skip entity/quote extraction below this

VALID_TYPES = {"news", "editorial", "letter", "ad", "social", "legal",
               "notice", "obituary", "stub", "unknown"}
VALID_ATTR_CONF = {"explicit", "implied", "inferred"}
VALID_TONES = {"humorous", "polemical", "ceremonial", "somber", "neutral"}

# ---------------------------------------------------------------------------
# System prompt (cached)
# ---------------------------------------------------------------------------

SYSTEM_PROMPT = """\
You are a historical newspaper analyst working on the Fort Worth Star-Telegram archive (1909–1925), part of a preservation project for Lake Worth, Texas history.

Your task: analyze a newspaper article and extract structured enrichment data in a single pass.

=== SUMMARY ===
Write 1-3 sentences describing what this article is about. For complex articles — legal filings, engineering reports, financial disputes, or multi-party proceedings — you may write up to 5 sentences if needed to capture all distinct threads. Plain English. Who was involved, what happened, what was decided or revealed. Written as a researcher's index card — specific enough to distinguish this article from others on similar topics. Do not editorialize or judge importance.
CRITICAL: Use names, places, and facts EXACTLY as they appear in the article. Never substitute or paraphrase proper nouns.

=== KEY POINTS ===
List the main factual points of the article as concise bullet strings. Include every distinct fact, decision, position, proposal, figure, and outcome — as many points as the article contains. Each point should be a specific, citable fact or claim from the article — not a restatement of the summary. Think: what would a researcher or editor need to know from this article without reading the full text?

=== ARTICLE TYPES ===
news         — news report
editorial    — opinion or editorial
letter       — letter to the editor
ad           — advertisement
social       — social notice, personal item, party, visit
legal        — court filing, legal notice, lawsuit record
notice       — public notice, announcement
obituary     — death notice or obituary
stub         — too short or garbled to classify meaningfully
unknown      — cannot determine

=== ENTITY EXTRACTION ===
Extract every named entity that is NOT a person (people go in PEOPLE EXTRACTION).
- name: canonical name as you would list it in an index
- type: choose the most specific label from the vocabulary below — do NOT collapse everything into "location" or "organization"
- mention_text: the EXACT text as printed in the article (OCR warts and all)
Do not try to resolve duplicates — that is a separate canonicalization pass.

TYPE VOCABULARY — use the most specific term that fits:

  Water features
    lake        — named lake (e.g. "Lake Worth", "Lake Minnetonka")
    reservoir   — named reservoir or impoundment (e.g. "West Fork Reservoir")
    river       — named river or creek (e.g. "West Fork of the Trinity River")
    spring      — named spring or mineral well

  Infrastructure & engineering
    dam         — a dam structure (e.g. "West Fork Dam", "Lake Worth Dam")
    bridge      — named bridge
    canal       — named canal or ditch
    road        — named road, street, highway, or boulevard
    railway     — named rail line, interurban, or streetcar line
    building    — named building, hall, or structure not better described below

  Recreation & hospitality
    park        — named park, public grounds, or nature reserve
    pavilion    — pavilion, dance hall, or open-air venue at a lake or resort
    camp        — named campground or lakeside camp
    resort      — named resort or hotel
    beach       — named beach or bathing area
    amusement   — named amusement ride or attraction (loop-the-loop, shoot-the-chute, etc.)

  Vessels & transport
    steamer     — named steamboat or motor launch
    vessel      — other named watercraft

  Civic & governmental
    court       — named court (e.g. "District Court", "Court of Civil Appeals")
    commission  — named commission or regulatory body
    department  — named government department or agency
    municipality — city, town, or county government as an entity

  Organizations & associations
    club        — social, sporting, or professional club (e.g. "Lawyers' Luncheon Club", "Rotary Club")
    association — named association or society
    company     — named business, corporation, or stock company
    law_firm    — named law firm or legal partnership
    union       — named labor union or trade union
    church      — named church or religious congregation
    school      — named school, college, or university

  Media
    newspaper   — named newspaper or periodical (e.g. "Fort Worth Star-Telegram")
    publication — other named publication (book, magazine, report)

  Events & shows
    event       — named fair, exposition, show, carnival, or one-time public event
    celebration — named annual celebration or holiday observance
    election    — named election or referendum

  Legal & regulatory
    lawsuit     — named legal case or suit
    ordinance   — named ordinance, bill, or statute
    contract    — named contract or bond issuance

  Fallback (use only if nothing above fits)
    place       — named place that is none of the above geographic types
    organization — named organization that fits none of the categories above

ANTI-COLLAPSE RULES:
- A lake is "lake", NOT "location"
- A dam is "dam", NOT "location" or "organization"
- A reservoir is "reservoir", NOT "location"
- A newspaper is "newspaper", NOT "organization"
- A law firm is "law_firm", NOT "organization"
- A court is "court", NOT "organization" or "location"
- A social or professional club is "club", NOT "organization"
- A fair, exposition, or named show is "event", NOT "organization" or "location"
- A pavilion at a lake is "pavilion", NOT "location"
- A named interurban or rail line is "railway", NOT "location"

=== QUOTE EXTRACTION — CRITICAL ===
Extract ALL direct quotes. Over-extract rather than under-extract. We can filter; we cannot recover missed quotes.
- quote_text: VERBATIM from the source — never reconstruct, paraphrase, or clean OCR noise
- speaker: null if unknown — NEVER GUESS. Hallucinated attribution is worse than no attribution for a preservation project.
- speaker_role: the title or role as described in the article (null if not given)
- context: one sentence describing when or why this was said (null if not clear)
- attribution_confidence:
    explicit  — quotation marks are present
    implied   — attribution language present ("he said", "according to", "stated that")
    inferred  — boundary inferred from context, no direct signal

=== PEOPLE EXTRACTION ===
Extract every named person mentioned.
- name: full name as printed (e.g. "J. C. Lord", "Tillman Graham")
- role: title or role as described in this article (e.g. "superintendent of construction", "promoter")

=== MEASUREMENTS ===
Extract every specific numerical value mentioned: costs, distances, crowd sizes, quantities, votes, acreage, capacity, durations, speeds, weights, temperatures, or any other measurable quantity.
- value: the numeric value as a number (e.g. 4000, 30, 600)
- unit: the base unit of what is being measured (e.g. "dollars", "miles", "acres", "people", "gallons", "feet", "months", "votes", "rowboats")
- context: one sentence describing what this number refers to
For ranges (e.g. "$4,000 to $8,000"), extract each bound as a separate measurement.
For approximate values (e.g. "about 600"), use the stated number and note the approximation in context.
Over-extract rather than under-extract.
DO NOT extract times of day as measurements. Expressions like "8 o'clock", "4 o'clock", or "half past two" are times, not measurable quantities — skip them entirely.
For compound rates (e.g. "$3 per acre", "$0.10 per million gallons", "$500 per mile"), store only the base unit in the unit field (e.g. "dollars") and describe the denominator in the context field (e.g. "Cost per acre of land purchased for the reservoir").

=== FLAGS ===
After reading the full article, set each flag true or false:
- has_map: article references a map, plat, diagram, or survey of the area
- has_photo_ref: article mentions or describes a photograph
- has_illustration: article references a drawing, sketch, or engraving
- notable_quote: contains at least one direct quote that is vivid, historically revealing, or distinctively voiced — a person expressing their values, a witness describing a significant event, or a line so characteristic it identifies its speaker and era. Set true liberally: if in doubt, set true.
- is_tragic: involves death, serious loss, or ruined plans
- has_conflict: dispute, lawsuit, formal opposition, or denial of a request

Also set tone to one of the following values based on the dominant register of the article:
- humorous: playful, witty, or comic in intent — jokes, absurdist details, light mockery
- polemical: argumentative, accusatory, or politically charged — editorials, denunciations, campaigns
- ceremonial: formal celebration, tribute, dedication, commemoration — speeches, toasts, milestones
- somber: grief, mourning, disaster, tragedy — obituaries, accident reports, defeated hopes
- neutral: matter-of-fact reporting with no dominant emotional register

Choose the single best fit. If the article is a stub or too garbled to assess, use neutral.

=== STUB HANDLING ===
If the article text is very short (under 50 characters) or completely garbled, set article_type to "stub".
For stubs: set summary to "Article too short to summarize." and return empty lists for entities, quotes, and people.

=== FEW-SHOT EXAMPLES ===

--- EXAMPLE 1 ---
Date: 1915-06-12
Headline: PLEASURE BOATS CROWD LAKE WORTH SHORES
Text: The shores of Lake Worth were crowded Sunday with pleasure seekers as a fleet of nearly forty rowboats and two motor launches took to the water. The interurban carried an estimated 600 passengers to the lake during the afternoon hours. Camp operators reported strong business, and the pavilion at the north shore hosted a dance attended by more than 200 persons.

Expected output:
{
  "summary": "Lake Worth drew large crowds on Sunday, with nearly forty rowboats and two motor launches on the water and an estimated 600 interurban passengers arriving for recreation. Camp operators and the north shore pavilion reported strong attendance.",
  "key_points": [
    "Nearly forty rowboats and two motor launches were on the water at Lake Worth on Sunday",
    "The interurban carried an estimated 600 passengers to the lake during the afternoon",
    "Camp operators reported strong business",
    "The north shore pavilion hosted a dance attended by more than 200 persons"
  ],
  "article_type": "news",
  "type_confidence": 0.95,
  "entities": [
    {"name": "Lake Worth", "type": "lake", "mention_text": "Lake Worth"},
    {"name": "North Shore Pavilion", "type": "pavilion", "mention_text": "pavilion at the north shore"}
  ],
  "quotes": [],
  "people": [],
  "measurements": [
    {"value": 40, "unit": "rowboats", "context": "Fleet of nearly forty rowboats on the water at Lake Worth"},
    {"value": 600, "unit": "passengers", "context": "Estimated interurban passengers arriving at the lake during afternoon hours"},
    {"value": 200, "unit": "persons", "context": "Attendance at the north shore pavilion dance"}
  ],
  "flags": {
    "has_map": false,
    "has_photo_ref": false,
    "has_illustration": false,
    "notable_quote": false,
    "is_tragic": false,
    "has_conflict": false,
    "tone": "neutral"
  }
}

--- EXAMPLE 2 ---
Date: 1913-07-20
Headline: MASS MEETING TO ARRANGE FOR WATER CARNIVAL PROPOSED
Text: A mass meeting to discuss a big water carnival as a fall feature following the completion of the West Fork dam is proposed by Tillman Graham, formerly of Fort Worth and later a promoter the Waco Cotton Palace. Graham proposes a stock company, for which stock of $100.000 would be ample. This company would erect a pavilion for an exhibit of all Texas farm products. He also proposes the erection a &quot;loop the loop,&quot; a &quot;shoot the chute,&quot; circle swing and other amusement contrivances. &quot;The stock show is a great attraction,&quot; he said Saturday, &quot;and I have no idea of a competing proposition. I think the sort of thing I am advocating would merely increase the crowds that come to Fort Worth for it, and hold them here a longer time.&quot;

Expected output:
{
  "summary": "Tillman Graham, a Fort Worth promoter formerly involved with the Waco Cotton Palace, proposed forming a stock company to build a water carnival at the new West Fork reservoir, including a pavilion, loop-the-loop, and other amusements.",
  "key_points": [
    "Tillman Graham proposed a mass meeting to organize a water carnival at the West Fork reservoir",
    "Graham proposed a stock company capitalized at $100,000 to build the carnival",
    "Proposed attractions included a pavilion for Texas farm products, loop-the-loop, shoot-the-chute, and circle swing",
    "Graham positioned the carnival as complementary to, not competing with, the National Feeders and Breeders Show",
    "Graham was formerly a promoter of the Waco Cotton Palace"
  ],
  "article_type": "news",
  "type_confidence": 0.88,
  "entities": [
    {"name": "West Fork Dam", "type": "dam", "mention_text": "West Fork dam"},
    {"name": "Waco Cotton Palace", "type": "event", "mention_text": "Waco Cotton Palace"},
    {"name": "National Feeders and Breeders Show", "type": "event", "mention_text": "National Feeders and Breeders&apos; Show"}
  ],
  "quotes": [
    {
      "quote_text": "The stock show is a great attraction,",
      "speaker": "Tillman Graham",
      "speaker_role": "promoter",
      "context": "Graham advocating for a water carnival alongside the stock show",
      "attribution_confidence": "explicit"
    },
    {
      "quote_text": "and I have no idea of a competing proposition. I think the sort of thing I am advocating would merely increase the crowds that come to Fort Worth for it, and hold them here a longer time.",
      "speaker": "Tillman Graham",
      "speaker_role": "promoter",
      "context": "Graham explaining his vision for the water carnival",
      "attribution_confidence": "explicit"
    }
  ],
  "people": [
    {"name": "Tillman Graham", "role": "promoter, formerly of Fort Worth"}
  ],
  "measurements": [
    {"value": 100000, "unit": "dollars", "context": "Amount of stock Graham believed would be ample for the proposed stock company"}
  ],
  "flags": {
    "has_map": false,
    "has_photo_ref": false,
    "has_illustration": false,
    "notable_quote": false,
    "is_tragic": false,
    "has_conflict": false,
    "tone": "neutral"
  }
}

--- EXAMPLE 3 ---
Date: 1913-10-19
Headline: JUDGE PEDEN HOLDS JURY SYSTEM WRONG
Text: JUDGE PEDEN HOLDS JURY SYSTEM WRONG Lawyers' Luncheon Will Debate Advantages of Supplanting Unanimous Verdict. At the meeting of the Lawyers' Luncheon club next Saturday, four members of the bar will discuss pro and con the proposition of a verdict. This announcement was made at the meeting Saturday, at which time claude McCaleb, court stenographer and piscatorial stocker of Lake Minnetonka, addressed the members on "Fishing as an Exact Science." D. M. Alexander, president of the club, made McCaleb a colonel, and ordered that In referring to him he be always "Colonel McCaleb." "I am sure," he said, "that if more of you would spend your time fishing, the country would be better off." "Lawyers get up there to the courthouse and can't think a good thing about anyone, but let them learn the gentle art of fishing and they could think of something good to say about the lawyer who beat 'em to a frazzle in a big case the day before."

Expected output:
{
  "summary": "At the Lawyers' Luncheon Club, court stenographer claude McCaleb addressed members on 'Fishing as an Exact Science,' after which club president D. M. Alexander made him an honorary colonel. The meeting also announced an upcoming debate on the jury verdict system.",
  "key_points": [
    "claude McCaleb, court stenographer and 'piscatorial stocker of Lake Minnetonka,' addressed the Lawyers' Luncheon Club on 'Fishing as an Exact Science'",
    "Club president D. M. Alexander gave McCaleb an honorary colonel's commission and ordered he always be addressed as 'Colonel McCaleb'",
    "Justice R. F. Peden proposed a future debate on replacing the unanimous jury verdict requirement with a nine-of-twelve rule",
    "Four club members were selected to argue the jury reform question pro and con at the next Saturday meeting",
    "McCaleb argued that fishing improves lawyers' temperament and outlook toward their colleagues"
  ],
  "article_type": "social",
  "type_confidence": 0.80,
  "entities": [
    {"name": "Lawyers' Luncheon Club", "type": "club", "mention_text": "Lawyers&apos; Luncheon club"},
    {"name": "Lake Minnetonka", "type": "lake", "mention_text": "Lake Minnetonka"}
  ],
  "quotes": [
    {
      "quote_text": "Fishing as an Exact Science.",
      "speaker": "claude McCaleb",
      "speaker_role": "court stenographer and piscatorial stocker of Lake Minnetonka",
      "context": "Title of McCaleb's address to the Lawyers' Luncheon club",
      "attribution_confidence": "implied"
    },
    {
      "quote_text": "I am sure, that if more of you would spend your time fishing, the country would be better off.",
      "speaker": "claude McCaleb",
      "speaker_role": "court stenographer",
      "context": "McCaleb addressing the Lawyers' Luncheon club on the virtues of fishing",
      "attribution_confidence": "explicit"
    },
    {
      "quote_text": "Lawyers get up there to the courthouse and can't think a good thing about anyone, but let them learn the gentle art of fishing and they could think of something good to say about the lawyer who beat 'em to a frazzle in a big case the day before.",
      "speaker": "claude McCaleb",
      "speaker_role": "court stenographer",
      "context": "McCaleb arguing that fishing improves lawyers' dispositions",
      "attribution_confidence": "explicit"
    }
  ],
  "people": [
    {"name": "claude McCaleb", "role": "court stenographer and piscatorial stocker of Lake Minnetonka"},
    {"name": "D. M. Alexander", "role": "president of the Lawyers' Luncheon club"},
    {"name": "Justice R. F. Peden", "role": "judge"}
  ],
  "measurements": [],
  "flags": {
    "has_map": false,
    "has_photo_ref": false,
    "has_illustration": false,
    "notable_quote": true,
    "is_tragic": false,
    "has_conflict": false,
    "tone": "humorous"
  }
}
"""

# ---------------------------------------------------------------------------
# JSON schema for structured output
# ---------------------------------------------------------------------------

OUTPUT_SCHEMA = {
    "type": "object",
    "additionalProperties": False,
    "properties": {
        "summary": {"type": "string"},
        "key_points": {
            "type": "array",
            "items": {"type": "string"}
        },
        "article_type": {
            "type": "string",
            "enum": ["news", "editorial", "letter", "ad", "social", "legal",
                     "notice", "obituary", "stub", "unknown"]
        },
        "type_confidence": {"type": "number"},
        "entities": {
            "type": "array",
            "items": {
                "type": "object",
                "additionalProperties": False,
                "properties": {
                    "name": {"type": "string"},
                    "type": {"type": "string"},
                    "mention_text": {"type": "string"}
                },
                "required": ["name", "type", "mention_text"]
            }
        },
        "quotes": {
            "type": "array",
            "items": {
                "type": "object",
                "additionalProperties": False,
                "properties": {
                    "quote_text": {"type": "string"},
                    "speaker": {"type": ["string", "null"]},
                    "speaker_role": {"type": ["string", "null"]},
                    "context": {"type": ["string", "null"]},
                    "attribution_confidence": {
                        "type": "string",
                        "enum": ["explicit", "implied", "inferred"]
                    }
                },
                "required": ["quote_text", "speaker", "speaker_role", "context",
                             "attribution_confidence"]
            }
        },
        "people": {
            "type": "array",
            "items": {
                "type": "object",
                "additionalProperties": False,
                "properties": {
                    "name": {"type": "string"},
                    "role": {"type": "string"}
                },
                "required": ["name", "role"]
            }
        },
        "measurements": {
            "type": "array",
            "items": {
                "type": "object",
                "additionalProperties": False,
                "properties": {
                    "value": {"type": "number"},
                    "unit": {"type": "string"},
                    "context": {"type": "string"}
                },
                "required": ["value", "unit", "context"]
            }
        },
        "flags": {
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "has_map": {"type": "boolean"},
                "has_photo_ref": {"type": "boolean"},
                "has_illustration": {"type": "boolean"},
                "notable_quote": {"type": "boolean"},
                "is_tragic": {"type": "boolean"},
                "has_conflict": {"type": "boolean"},
                "tone": {
                    "type": "string",
                    "enum": ["humorous", "polemical", "ceremonial", "somber", "neutral"]
                }
            },
            "required": ["has_map", "has_photo_ref", "has_illustration", "notable_quote",
                         "is_tragic", "has_conflict", "tone"]
        }
    },
    "required": [
        "summary", "key_points", "article_type", "type_confidence",
        "entities", "quotes", "people", "measurements", "flags"
    ]
}

# ---------------------------------------------------------------------------
# Pydantic models (semantic validation on top of schema)
# ---------------------------------------------------------------------------

class QuoteExtraction(BaseModel):
    quote_text: str
    speaker: Optional[str]
    speaker_role: Optional[str]
    context: Optional[str]
    attribution_confidence: str

    @field_validator("attribution_confidence")
    @classmethod
    def validate_attr_conf(cls, v):
        if v not in VALID_ATTR_CONF:
            raise ValueError(f"attribution_confidence must be one of {VALID_ATTR_CONF}, got {v!r}")
        return v


class EntityExtraction(BaseModel):
    name: str
    type: str
    mention_text: str


class PersonExtraction(BaseModel):
    name: str
    role: str


class MeasurementExtraction(BaseModel):
    value: float
    unit: str
    context: str


class FlagsExtraction(BaseModel):
    has_map: bool = False
    has_photo_ref: bool = False
    has_illustration: bool = False
    notable_quote: bool = False
    is_tragic: bool = False
    has_conflict: bool = False
    tone: str = "neutral"

    @field_validator("tone")
    @classmethod
    def validate_tone(cls, v):
        if v not in VALID_TONES:
            raise ValueError(f"tone must be one of {VALID_TONES}, got {v!r}")
        return v


class ArticleEnrichment(BaseModel):
    summary: str
    key_points: List[str] = []
    article_type: str
    type_confidence: float
    entities: List[EntityExtraction]
    quotes: List[QuoteExtraction]
    people: List[PersonExtraction]
    measurements: List[MeasurementExtraction] = []
    flags: FlagsExtraction = FlagsExtraction()

    @field_validator("article_type")
    @classmethod
    def validate_type(cls, v):
        if v not in VALID_TYPES:
            raise ValueError(f"article_type must be one of {VALID_TYPES}, got {v!r}")
        return v

    @field_validator("type_confidence")
    @classmethod
    def validate_confidence(cls, v):
        if not (0.0 <= v <= 1.0):
            raise ValueError(f"confidence must be 0.0–1.0, got {v}")
        return v


# ---------------------------------------------------------------------------
# Prompt hash
# ---------------------------------------------------------------------------

def compute_prompt_hash() -> str:
    payload = SYSTEM_PROMPT + json.dumps(OUTPUT_SCHEMA, sort_keys=True)
    return hashlib.sha256(payload.encode()).hexdigest()[:16]


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------

def get_db() -> sqlite3.Connection:
    conn = sqlite3.connect(str(DB_PATH))
    conn.execute("PRAGMA journal_mode=WAL")
    conn.row_factory = sqlite3.Row
    return conn


def ensure_quotes_columns(conn: sqlite3.Connection):
    """Add attribution_confidence and enrichment_run_id to quotes if missing.
    Also ensures article_type has a summary column."""
    cur = conn.cursor()
    cols = {r[1] for r in cur.execute("PRAGMA table_info(quotes)")}
    if "attribution_confidence" not in cols:
        cur.execute("ALTER TABLE quotes ADD COLUMN attribution_confidence TEXT")
    if "enrichment_run_id" not in cols:
        cur.execute("ALTER TABLE quotes ADD COLUMN enrichment_run_id INTEGER")
    try:
        cur.execute("ALTER TABLE article_type ADD COLUMN summary TEXT")
    except Exception:
        pass  # column already exists
    conn.commit()


def get_unprocessed_articles(conn: sqlite3.Connection, limit: Optional[int],
                              rerun: bool, resume_run_id: Optional[int],
                              from_date: Optional[str] = None,
                              to_date: Optional[str] = None) -> list:
    cur = conn.cursor()
    date_clauses = []
    date_params = []
    if from_date:
        date_clauses.append("a.date >= ?")
        date_params.append(from_date)
    if to_date:
        date_clauses.append("a.date <= ?")
        date_params.append(to_date)
    date_and = (" AND " + " AND ".join(date_clauses)) if date_clauses else ""
    date_where = (" WHERE " + " AND ".join(date_clauses)) if date_clauses else ""

    if resume_run_id is not None:
        # Resume: articles not yet touched by THIS specific run
        sql = f"""
            SELECT a.id, a.date, a.headline, a.full_text, a.newspaper
            FROM articles a
            WHERE a.id NOT IN (
                SELECT article_id FROM article_type WHERE enrichment_run_id = ?
            ){date_and}
            ORDER BY a.date ASC, a.id ASC
        """
        params = [resume_run_id] + date_params
    elif rerun:
        sql = f"SELECT id, date, headline, full_text, newspaper FROM articles{date_where} ORDER BY date ASC, id ASC"
        params = date_params
    else:
        # Default: articles with no entry in article_type at all
        sql = f"""
            SELECT a.id, a.date, a.headline, a.full_text, a.newspaper
            FROM articles a
            WHERE a.id NOT IN (SELECT DISTINCT article_id FROM article_type){date_and}
            ORDER BY a.date ASC, a.id ASC
        """
        params = date_params

    if limit is not None:
        sql += f" LIMIT {int(limit)}"

    rows = cur.execute(sql, params).fetchall()
    return [dict(r) for r in rows]


def create_enrichment_run(conn: sqlite3.Connection, prompt_hash: str,
                           notes: Optional[str] = None) -> int:
    cur = conn.cursor()
    cur.execute(
        """INSERT INTO enrichment_runs
           (agent_name, prompt_version, prompt_hash, model, started_at, articles_processed, notes)
           VALUES (?, ?, ?, ?, ?, 0, ?)""",
        (AGENT_NAME, PROMPT_VERSION, prompt_hash, MODEL,
         datetime.now(timezone.utc).isoformat(), notes)
    )
    conn.commit()
    return cur.lastrowid


def complete_enrichment_run(conn: sqlite3.Connection, run_id: int, processed: int):
    conn.execute(
        """UPDATE enrichment_runs SET completed_at=?, articles_processed=? WHERE id=?""",
        (datetime.now(timezone.utc).isoformat(), processed, run_id)
    )
    conn.commit()


def write_article_results(conn: sqlite3.Connection, article: dict,
                           enrichment: ArticleEnrichment, run_id: int):
    """Write all enrichment results for one article in a single transaction."""
    cur = conn.cursor()
    article_id = article["id"]
    article_date = article.get("date")

    # Clean up any previous enrichment for this article (makes reruns idempotent)
    cur.execute("DELETE FROM article_type WHERE article_id = ?", (article_id,))
    cur.execute("DELETE FROM article_entities WHERE article_id = ?", (article_id,))
    cur.execute("DELETE FROM quotes WHERE article_id = ?", (article_id,))
    cur.execute("DELETE FROM article_people WHERE article_id = ?", (article_id,))
    cur.execute("DELETE FROM measurements WHERE article_id = ?", (article_id,))
    cur.execute("DELETE FROM article_key_points WHERE article_id = ?", (article_id,))
    cur.execute("DELETE FROM article_flags WHERE article_id = ?", (article_id,))

    # 1. article_type
    cur.execute(
        "INSERT INTO article_type (article_id, type, confidence, summary, enrichment_run_id) VALUES (?,?,?,?,?)",
        (article_id, enrichment.article_type, enrichment.type_confidence, enrichment.summary, run_id)
    )

    # 2. entities — insert entity if new (name+type), then link
    for ent in enrichment.entities:
        existing = cur.execute(
            "SELECT id FROM entities WHERE name=? AND type=?",
            (ent.name, ent.type)
        ).fetchone()
        if existing:
            entity_id = existing["id"]
            # Update date_last if this article is more recent
            if article_date:
                cur.execute(
                    "UPDATE entities SET date_last=? WHERE id=? AND (date_last IS NULL OR date_last < ?)",
                    (article_date, entity_id, article_date)
                )
        else:
            cur.execute(
                """INSERT INTO entities (name, type, date_first, date_last, description, notes)
                   VALUES (?,?,?,?,?,?)""",
                (ent.name, ent.type, article_date, article_date, None, None)
            )
            entity_id = cur.lastrowid

        cur.execute(
            """INSERT INTO article_entities
               (article_id, entity_id, mention_text, source, verified, enrichment_run_id)
               VALUES (?,?,?,'ai',0,?)""",
            (article_id, entity_id, ent.mention_text, run_id)
        )

    # 5. quotes — insert into existing quotes table
    for q in enrichment.quotes:
        cur.execute(
            """INSERT INTO quotes
               (article_id, quote_text, speaker, speaker_role, context,
                attribution_confidence, enrichment_run_id)
               VALUES (?,?,?,?,?,?,?)""",
            (article_id, q.quote_text, q.speaker, q.speaker_role, q.context,
             q.attribution_confidence, run_id)
        )

    # 6. people — per-article join table
    for p in enrichment.people:
        cur.execute(
            "INSERT INTO article_people (article_id, mention_text, role, enrichment_run_id) VALUES (?,?,?,?)",
            (article_id, p.name, p.role, run_id)
        )

    # 7. measurements
    for m in enrichment.measurements:
        cur.execute(
            "INSERT INTO measurements (article_id, value, unit, context, enrichment_run_id) VALUES (?,?,?,?,?)",
            (article_id, m.value, m.unit, m.context, run_id)
        )

    # 8. key points
    for idx, point in enumerate(enrichment.key_points):
        cur.execute(
            "INSERT INTO article_key_points (article_id, point, sort_order, enrichment_run_id) VALUES (?,?,?,?)",
            (article_id, point, idx, run_id)
        )

    # 9. flags
    f = enrichment.flags
    cur.execute(
        """INSERT INTO article_flags
           (article_id, has_map, has_photo_ref, has_illustration, notable_quote,
            tragic, has_conflict, tone, enrichment_run_id)
           VALUES (?,?,?,?,?,?,?,?,?)""",
        (article_id, f.has_map, f.has_photo_ref, f.has_illustration, f.notable_quote,
         f.is_tragic, f.has_conflict, f.tone, run_id)
    )


# ---------------------------------------------------------------------------
# Status file
# ---------------------------------------------------------------------------

def write_status(run_id: int, done: int, total: int, recent: deque):
    data = {
        "active_agent": "enrichment",
        "run_id": run_id,
        "articles_done": done,
        "articles_total": total,
        "current_articles": list(recent)
    }
    STATUS_FILE.write_text(json.dumps(data, indent=2))


def clear_status():
    data = {"active_agent": None}
    STATUS_FILE.write_text(json.dumps(data, indent=2))


# ---------------------------------------------------------------------------
# API call
# ---------------------------------------------------------------------------

def call_claude(client: anthropic.Anthropic, article: dict) -> ArticleEnrichment:
    """Make a single Sonnet call and return validated ArticleEnrichment."""
    headline = article.get("headline") or "(no headline)"
    full_text = article.get("full_text") or ""
    date = article.get("date") or "unknown"

    user_content = f"Date: {date}\nHeadline: {headline}\n\nFull text:\n{full_text}\n\nRespond with ONLY a valid JSON object. No explanation, no markdown, no code fences. Required fields: summary, key_points, article_type, type_confidence, entities, quotes, people, measurements, flags (including tone)."

    response = client.messages.create(
        model=MODEL,
        max_tokens=16384,
        system=[
            {
                "type": "text",
                "text": SYSTEM_PROMPT,
                "cache_control": {"type": "ephemeral"}
            }
        ],
        messages=[
            {"role": "user", "content": user_content}
        ]
    )

    raw_json = response.content[0].text
    data = json.loads(raw_json)
    enrichment = ArticleEnrichment(**data)
    return enrichment



# ---------------------------------------------------------------------------
# Dry-run display
# ---------------------------------------------------------------------------

def display_dry_run(articles: list):
    print(f"\n[DRY RUN] Would process {len(articles)} articles with model: {MODEL}")
    print(f"[DRY RUN] Prompt hash: {compute_prompt_hash()}")
    print()
    for i, a in enumerate(articles, 1):
        hl = (a.get("headline") or "(no headline)")[:60]
        ft = a.get("full_text") or ""
        stub_note = " [STUB — would skip extraction]" if len(ft) < STUB_THRESHOLD else ""
        print(f"[{i:>3}] {a.get('date','?')} | {hl}{stub_note}")
    print()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Enrichment agent for Lake Worth newspaper archive")
    parser.add_argument("--limit", type=int, default=None, help="Process N articles")
    parser.add_argument("--batch", action="store_true", help="Use Batch API (not yet implemented)")
    parser.add_argument("--resume-run", type=int, default=None, dest="resume_run",
                        help="Resume an incomplete run by ID")
    parser.add_argument("--rerun", action="store_true",
                        help="Re-process articles even if already enriched")
    parser.add_argument("--dry-run", action="store_true", dest="dry_run",
                        help="Print what would be processed, make no API calls")
    parser.add_argument("--from-date", type=str, default=None, dest="from_date",
                        help="Only process articles on or after this date (YYYY-MM-DD)")
    parser.add_argument("--to-date", type=str, default=None, dest="to_date",
                        help="Only process articles on or before this date (YYYY-MM-DD)")
    args = parser.parse_args()

    if args.batch:
        print("NOTE: --batch flag set but Batch API not yet implemented. Using standard API.")

    conn = get_db()

    # Ensure quotes table has the extra columns
    ensure_quotes_columns(conn)

    # Get articles to process
    articles = get_unprocessed_articles(conn, args.limit, args.rerun, args.resume_run,
                                         args.from_date, args.to_date)

    if not articles:
        print("No articles to process.")
        conn.close()
        return

    if args.dry_run:
        display_dry_run(articles)
        conn.close()
        return

    # --- Live run ---
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        print("ERROR: ANTHROPIC_API_KEY not set.")
        sys.exit(1)

    client = anthropic.Anthropic(api_key=api_key)
    prompt_hash = compute_prompt_hash()

    # Create or reuse enrichment run record
    if args.resume_run is not None:
        run_id = args.resume_run
        print(f"[RUN {run_id}] Resuming run {run_id} | {len(articles)} articles remaining | "
              f"Model: {MODEL} | Prompt hash: {prompt_hash}")
    else:
        run_id = create_enrichment_run(conn, prompt_hash)
        print(f"[RUN {run_id}] Processing {len(articles)} articles | "
              f"Model: {MODEL} | Prompt hash: {prompt_hash}")

    total = len(articles)
    processed = 0
    failed = 0
    recent: deque = deque(maxlen=5)
    start_time = time.time()

    for i, article in enumerate(articles, 1):
        article_id = article["id"]
        headline = (article.get("headline") or "(no headline)")[:50]
        date = article.get("date") or "?"
        full_text = article.get("full_text") or ""

        # Update status file before call so dashboard shows current article
        recent.append({
            "id": article_id,
            "headline": article.get("headline") or "",
            "date": date,
            "newspaper": article.get("newspaper") or ""
        })
        write_status(run_id, processed, total, recent)

        # Short-circuit stubs before API call
        if len(full_text.strip()) < STUB_THRESHOLD:
            try:
                conn.execute("BEGIN")
                conn.execute(
                    "INSERT INTO article_type (article_id, type, confidence, summary, enrichment_run_id) VALUES (?,?,?,?,?)",
                    (article_id, "stub", 1.0, "Article too short to summarize.", run_id)
                )
                conn.execute("COMMIT")
                print(f"[{i:>4}/{total}] {date} | {headline} | type=stub [skipped API]")
                processed += 1
            except Exception as e:
                conn.execute("ROLLBACK")
                print(f"[{i:>4}/{total}] {date} | {headline} | STUB WRITE FAILED: {e}")
                failed += 1
            continue

        # Call Claude
        try:
            enrichment = call_claude(client, article)

            conn.execute("BEGIN")
            write_article_results(conn, article, enrichment, run_id)
            conn.execute("COMMIT")

            entities_count = len(enrichment.entities)
            quotes_count = len(enrichment.quotes)
            measurements_count = len(enrichment.measurements)
            points_count = len(enrichment.key_points)
            print(f"[{i:>4}/{total}] {date} | {headline} | "
                  f"type={enrichment.article_type} "
                  f"entities={entities_count} quotes={quotes_count} measurements={measurements_count} points={points_count}")
            processed += 1

        except Exception as e:
            try:
                conn.execute("ROLLBACK")
            except Exception:
                pass
            print(f"[{i:>4}/{total}] {date} | {headline} | FAILED: {e}")
            failed += 1

        write_status(run_id, processed, total, recent)

    # Complete the run
    elapsed = time.time() - start_time
    mins = int(elapsed // 60)
    secs = int(elapsed % 60)

    complete_enrichment_run(conn, run_id, processed)
    conn.close()
    clear_status()

    successful = processed - failed if failed == 0 else processed
    print(f"\n[{total}/{total}] Done. {processed} processed, {failed} failed, "
          f"{processed - failed} successful.")
    print(f"[RUN {run_id}] Completed in {mins}m {secs}s")


if __name__ == "__main__":
    main()
