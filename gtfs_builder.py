#!/usr/bin/env python3
"""
gtfs_builder.py
===============
Discover, validate, merge and load one or more GTFS feeds into a single
SQLite database.

Steps
-----
1. Discover feeds: every sub-folder of --gtfs that contains GTFS .txt files.
2. Validate each feed (structure, required files/columns, field formats,
   referential integrity, cross-feed ID collisions, duplicates).
3. If any ERROR is found -> write a dated error report and abort.
   (--force overrides this.)
4. Merge the feeds. IDs are kept exactly as they appear in the GTFS files;
   optional --prefix rewrites them to `<feed>__<id>` when merging feeds that
   reuse the same IDs.
5. Load the merged tables into SQLite and index every *_id column.

Usage
-----
    python gtfs_builder.py                       # uses .env / environment
    python gtfs_builder.py --gtfs bus-gtfs/ --db gtfs.sqlite
    python gtfs_builder.py --gtfs bus-gtfs/ water-gtfs/ --db all.sqlite
    python gtfs_builder.py --feeds dubai,abudhabi
    python gtfs_builder.py --prefix
    python gtfs_builder.py --force

Configuration
-------------
Every default can be set in the environment or in a `.env` file next to this
script (real environment variables win over `.env`):

    GTFS_ROOTS   = bus-gtfs,water-gtfs     roots to scan for feeds
    GTFS_DB      = gtfs.sqlite             output SQLite file
    GTFS_FEEDS   = (empty = all)           comma-separated feed folder names
    GTFS_LOG_DIR = logs                    where validation reports go
    GTFS_PREFIX  = false                   prefix IDs with the feed name
    GTFS_FORCE   = false                   build even when validation fails

Command-line flags override the environment.

Folder layout
-------------
    bus-gtfs/
        dubai/          <- feed 1
            gtfs/       <- one level of nesting is also supported
                agency.txt
                ...
        abudhabi/       <- feed 2
            agency.txt

A folder that directly contains the .txt files is used as-is; otherwise a
single sub-folder containing them is used. A bare single feed (no sub-folders)
is also accepted, so the old `--gtfs gtfs/` usage still works.

Validation report
-----------------
When validation fails, a report is written to
    logs/gtfs_errors_YYYY-MM-DD_HHMMSS.txt
and the newest copy is also written to
    logs/gtfs_errors_latest.txt
The report lists every issue with feed, file, line and a concrete fix hint.
"""

import argparse
import csv
import io
import logging
import os
import sys
import re
if sys.platform == "win32":
    for _d in [r"C:\OSGeo4W\bin", os.environ.get("OSGEO4W_ROOT", "") + r"\bin"]:
        if os.path.isdir(_d):
            try:
                os.add_dll_directory(_d)
            except Exception:
                pass
import sqlite3
import zipfile
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Optional

# ── Defaults ──────────────────────────────────────────────────────────────────
BASE_DIR = Path(__file__).resolve().parent
ENV_FILE = BASE_DIR / ".env"

ID_SEP = "__"          # feed prefix separator:  dubai__00102A


# ── Configuration (.env + environment) ────────────────────────────────────────

def load_dotenv(path: Path = ENV_FILE) -> None:
    """
    Minimal .env loader (no third-party dependency).

    Existing environment variables are never overwritten, so real env vars
    always win over the file.
    """
    if not path.is_file():
        return
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = value


def _env(name: str, default: str) -> str:
    value = os.environ.get(name, "").strip()
    return value if value else default


def _env_bool(name: str, default: bool) -> bool:
    value = os.environ.get(name, "").strip().lower()
    if not value:
        return default
    return value in ("1", "true", "yes", "y", "on")


def _env_list(name: str) -> list[str]:
    value = os.environ.get(name, "").strip()
    return [v.strip() for v in value.split(",") if v.strip()]


def _resolve(path_str: str) -> Path:
    """Resolve *path_str* relative to the script directory when not absolute."""
    p = Path(path_str).expanduser()
    return p if p.is_absolute() else (BASE_DIR / p)


@dataclass
class Target:
    """One database to build: a name, the roots to scan, and the output file."""
    name: str
    roots: list[Path]
    db_path: Path
    feeds: list[str] = field(default_factory=list)


@dataclass
class Config:
    targets: list[Target]
    log_dir: Path
    prefix: bool
    force: bool

    @classmethod
    def from_env(cls) -> "Config":
        """
        Build the list of databases to produce.

        Transit, ferry and rail are always separate databases, each driven by
        its own environment variables:

            GTFS_ROOTS / GTFS_DB             -> transit (default bus-gtfs/gtfs.sqlite)
            WATER_GTFS_ROOTS / WATER_GTFS_DB -> ferry   (default water-gtfs/water-gtfs.sqlite)
            RAIL_GTFS_ROOTS / RAIL_GTFS_DB   -> rail    (default rail-gtfs/rail-gtfs.sqlite)
        """
        transit_roots = _env_list("GTFS_ROOTS") or ["bus-gtfs"]
        ferry_roots   = _env_list("WATER_GTFS_ROOTS") or ["water-gtfs"]
        rail_roots    = _env_list("RAIL_GTFS_ROOTS") or ["rail-gtfs"]

        targets = [
            Target(
                name="transit",
                roots=[_resolve(r) for r in transit_roots],
                db_path=_resolve(_env("GTFS_DB", "gtfs.sqlite")),
                feeds=_env_list("GTFS_FEEDS"),
            ),
            Target(
                name="ferry",
                roots=[_resolve(r) for r in ferry_roots],
                db_path=_resolve(_env("WATER_GTFS_DB", "water-gtfs.sqlite")),
                feeds=_env_list("WATER_GTFS_FEEDS"),
            ),
            Target(
                name="rail",
                roots=[_resolve(r) for r in rail_roots],
                db_path=_resolve(_env("RAIL_GTFS_DB", "rail-gtfs.sqlite")),
                feeds=_env_list("RAIL_GTFS_FEEDS"),
            ),
        ]
        return cls(
            targets=targets,
            log_dir=_resolve(_env("GTFS_LOG_DIR", "logs")),
            prefix=_env_bool("GTFS_PREFIX", False),
            force=_env_bool("GTFS_FORCE", False),
        )

# ── GTFS spec ─────────────────────────────────────────────────────────────────

# file -> (required?, required columns, all known columns)
GTFS_SPEC: dict = {
    "agency": (True,
        ("agency_name", "agency_url", "agency_timezone"),
        ("agency_id", "agency_name", "agency_url", "agency_timezone",
         "agency_lang", "agency_phone", "agency_fare_url", "agency_email")),
    "stops": (True,
        ("stop_id", "stop_name", "stop_lat", "stop_lon"),
        ("stop_id", "stop_code", "stop_name", "stop_desc", "stop_lat", "stop_lon",
         "zone_id", "stop_url", "location_type", "parent_station", "stop_timezone",
         "wheelchair_boarding", "level_id", "platform_code")),
    "routes": (True,
        ("route_id", "route_type"),
        ("route_id", "agency_id", "route_short_name", "route_long_name",
         "route_desc", "route_type", "route_url", "route_color",
         "route_text_color", "route_sort_order", "continuous_pickup",
         "continuous_drop_off")),
    "trips": (True,
        ("route_id", "service_id", "trip_id"),
        ("route_id", "service_id", "trip_id", "trip_headsign", "trip_short_name",
         "direction_id", "block_id", "shape_id", "wheelchair_accessible",
         "bikes_allowed")),
    "stop_times": (True,
        ("trip_id", "stop_id", "stop_sequence"),
        ("trip_id", "arrival_time", "departure_time", "stop_id", "stop_sequence",
         "stop_headsign", "pickup_type", "drop_off_type", "shape_dist_traveled",
         "timepoint")),
    "calendar": (False,
        ("service_id", "monday", "tuesday", "wednesday", "thursday", "friday",
         "saturday", "sunday", "start_date", "end_date"),
        ("service_id", "monday", "tuesday", "wednesday", "thursday", "friday",
         "saturday", "sunday", "start_date", "end_date")),
    "calendar_dates": (False,
        ("service_id", "date", "exception_type"),
        ("service_id", "date", "exception_type")),
    "shapes": (False,
        ("shape_id", "shape_pt_lat", "shape_pt_lon", "shape_pt_sequence"),
        ("shape_id", "shape_pt_lat", "shape_pt_lon", "shape_pt_sequence",
         "shape_dist_traveled")),
    "fare_attributes": (False,
        ("fare_id", "price", "currency_type", "payment_method", "transfers"),
        ("fare_id", "price", "currency_type", "payment_method", "transfers",
         "agency_id", "transfer_duration")),
    "fare_rules": (False,
        ("fare_id",),
        ("fare_id", "route_id", "origin_id", "destination_id", "contains_id")),
    "transfers": (False,
        ("from_stop_id", "to_stop_id", "transfer_type"),
        ("from_stop_id", "to_stop_id", "transfer_type", "min_transfer_time")),
    "translations": (False,
        ("table_name", "field_name", "language", "translation"),
        ("table_name", "field_name", "language", "translation", "record_id",
         "record_sub_id", "field_value")),
    "feed_info": (False,
        ("feed_publisher_name", "feed_publisher_url", "feed_lang"),
        ("feed_publisher_name", "feed_publisher_url", "feed_lang",
         "default_lang", "feed_start_date", "feed_end_date", "feed_version",
         "feed_contact_email", "feed_contact_url")),
}

# Load order: parents before children (matters for logging/summary only).
GTFS_FILES = list(GTFS_SPEC)

# Columns that hold IDs and therefore get a feed prefix when merging.
ID_COLUMNS = {
    "agency_id", "stop_id", "route_id", "trip_id", "service_id", "shape_id",
    "fare_id", "from_stop_id", "to_stop_id", "parent_station", "zone_id",
    "origin_id", "destination_id", "contains_id", "record_id", "record_sub_id",
}

# Columns holding references to another table's ID.
FK_RULES = [
    # (file, column, target file, target column, required?)
    ("trips",          "route_id",      "routes",            "route_id",   True),
    ("trips",          "service_id",    "service",           "service_id", True),
    ("trips",          "shape_id",      "shapes",            "shape_id",   False),
    ("stop_times",     "trip_id",       "trips",             "trip_id",    True),
    ("stop_times",     "stop_id",       "stops",             "stop_id",    True),
    ("stops",          "parent_station", "stops",            "stop_id",    False),
    ("fare_rules",     "fare_id",       "fare_attributes",   "fare_id",    True),
    ("fare_rules",     "route_id",      "routes",            "route_id",   False),
    ("routes",         "agency_id",     "agency",            "agency_id",  False),
    ("transfers",      "from_stop_id",  "stops",             "stop_id",    True),
    ("transfers",      "to_stop_id",    "stops",             "stop_id",    True),
    ("calendar_dates", "service_id",    "calendar",          "service_id", False),
]

# Columns that must be unique within their file.
UNIQUE_RULES = {
    "agency":          ["agency_id"],
    "stops":           ["stop_id"],
    "routes":          ["route_id"],
    "trips":           ["trip_id"],
    "calendar":        ["service_id"],
    "fare_attributes": ["fare_id"],
}

# Columns the API reads unconditionally. When a feed omits one of these, it is
# added as an empty column so every merged DB has a uniform schema.
API_REQUIRED_COLUMNS = {
    "stops":  ["stop_code", "zone_id", "platform_code"],
    "routes": ["route_url", "route_color", "route_text_color",
               "route_short_name", "route_long_name"],
    "trips":  ["trip_headsign", "direction_id", "shape_id", "block_id"],
    "agency": ["agency_id"],
    "stop_times": ["stop_headsign", "pickup_type", "drop_off_type",
                   "shape_dist_traveled"],
    "translations": ["record_id", "record_sub_id", "field_value"],
}

DATE_RE = re.compile(r"^\d{8}$")
TIME_RE = re.compile(r"^(\d{1,3}):([0-5]\d):([0-5]\d)$")
PADDED_TIME_RE = re.compile(r"^(\d{2,3}):([0-5]\d):([0-5]\d)$")

logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(message)s")
log = logging.getLogger(__name__)

SEVERITY_ORDER = {"ERROR": 0, "WARNING": 1, "INFO": 2}


# ── Issue reporting ───────────────────────────────────────────────────────────

@dataclass
class Issue:
    severity: str          # ERROR | WARNING | INFO
    feed: str
    file: str
    message: str
    line: Optional[int] = None
    hint: str = ""

    def sort_key(self) -> tuple:
        return (SEVERITY_ORDER.get(self.severity, 9), self.feed,
                self.file, self.line or 0)

    def render(self) -> str:
        loc = f"{self.file}:{self.line}" if self.line else self.file
        out = f"[{self.severity:<7}] {self.feed}/{loc}\n            {self.message}"
        if self.hint:
            out += f"\n            → fix: {self.hint}"
        return out


@dataclass
class Feed:
    name: str
    path: Path
    tables: dict[str, list[dict]] = field(default_factory=dict)
    headers: dict[str, list[str]] = field(default_factory=dict)
    is_zip: bool = False

    def ids(self, table: str, column: str) -> set[str]:
        return {r[column] for r in self.tables.get(table, []) if r.get(column)}


# ── Step 1 – Discover feeds ───────────────────────────────────────────────────

def open_feed_table(feed: Feed, table: str):
    """Return an open text file-like object for table.txt in feed, or None."""
    if feed.is_zip or (feed.path.is_file() and feed.path.suffix.lower() == ".zip"):
        zf = zipfile.ZipFile(feed.path, "r")
        name = f"{table}.txt"
        matching = [n for n in zf.namelist() if n == name or n.endswith("/" + name)]
        if matching:
            raw = zf.open(matching[0], "r")
            return io.TextIOWrapper(raw, encoding="utf-8-sig", newline="")
        return None
    else:
        fpath = feed.path / f"{table}.txt"
        if fpath.is_file():
            return fpath.open(encoding="utf-8-sig", newline="")
        return None


def _is_gtfs_dir(path: Path) -> bool:
    """True if *path* directly contains at least one GTFS .txt file."""
    if not path.is_dir():
        return False
    return any((path / f"{name}.txt").is_file() for name in GTFS_FILES)


def _is_gtfs_zip(path: Path) -> bool:
    """True if *path* is a zip containing GTFS .txt files."""
    if not path.is_file() or path.suffix.lower() != ".zip":
        return False
    try:
        with zipfile.ZipFile(path, "r") as zf:
            names = set(zf.namelist())
            return any(
                f"{name}.txt" in names or any(n.endswith("/" + f"{name}.txt") for n in names)
                for name in GTFS_FILES
            )
    except Exception:
        return False


def resolve_feed_dir(folder: Path) -> Optional[Path]:
    """
    Return the directory that actually holds the GTFS .txt files.

    Accepts either `folder/` itself or a single nested sub-folder such as
    `folder/gtfs/`.
    """
    if _is_gtfs_dir(folder):
        return folder
    nested = [p for p in sorted(folder.iterdir()) if p.is_dir() and _is_gtfs_dir(p)]
    if len(nested) == 1:
        return nested[0]
    return None


def discover_feeds(root: Path) -> tuple[list[Feed], list[Issue]]:
    """
    Find every GTFS feed under *root*.

    Can be a .zip file, a directory with .txt files, or a folder containing
    feed subfolders / zip files.
    """
    issues: list[Issue] = []
    feeds: list[Feed] = []

    if _is_gtfs_zip(root):
        feeds.append(Feed(name=root.stem, path=root, is_zip=True))
        return feeds, issues

    if not root.is_dir():
        issues.append(Issue("ERROR", "-", str(root), "GTFS root folder or .zip not found",
                            hint="Pass an existing folder or .zip file with --gtfs or GTFS_ROOTS"))
        return feeds, issues

    zip_files = [p for p in sorted(root.iterdir()) if p.is_file() and _is_gtfs_zip(p)]
    if zip_files and not _is_gtfs_dir(root):
        for zf in zip_files:
            feeds.append(Feed(name=zf.stem, path=zf, is_zip=True))
        return feeds, issues

    subdirs = [p for p in sorted(root.iterdir())
               if p.is_dir() and not p.name.startswith(".")]

    if _is_gtfs_dir(root) or (not subdirs and resolve_feed_dir(root)):
        feeds.append(Feed(name=root.name, path=resolve_feed_dir(root) or root))
        return feeds, issues

    for sub in subdirs:
        if _is_gtfs_zip(sub):
            feeds.append(Feed(name=sub.stem, path=sub, is_zip=True))
            continue
        resolved = resolve_feed_dir(sub)
        if resolved is None:
            issues.append(Issue(
                "ERROR", sub.name, str(sub.relative_to(root)),
                "No GTFS .txt files found in this folder",
                hint="Put agency.txt, stops.txt, routes.txt, trips.txt, "
                     "stop_times.txt in this folder (or in exactly one sub-folder).",
            ))
            continue
        feeds.append(Feed(name=sub.name, path=resolved))

    if not feeds and not issues:
        issues.append(Issue("ERROR", "-", str(root), "No GTFS feeds found",
                            hint="Create one sub-folder or .zip per feed under the root."))

    return feeds, issues


def discover_all(roots: list[Path]) -> tuple[list[Feed], list[Issue]]:
    """
    Discover feeds across every root.

    Feed names must be unique across roots (they become the ID prefix), so a
    duplicate name is reported as an error rather than silently merged.
    """
    feeds: list[Feed] = []
    issues: list[Issue] = []

    for root in roots:
        found, root_issues = discover_feeds(root)
        issues.extend(root_issues)
        feeds.extend(found)

    seen: dict[str, Path] = {}
    for feed in feeds:
        if feed.name in seen:
            issues.append(Issue(
                "ERROR", feed.name, str(feed.path),
                f"Feed name '{feed.name}' is used by more than one folder "
                f"({seen[feed.name]} and {feed.path})",
                hint="Feed names become the ID prefix, so they must be unique. "
                     "Rename one of the folders.",
            ))
        else:
            seen[feed.name] = feed.path

    return feeds, issues


# ── Step 2 – Validation ───────────────────────────────────────────────────────

def _inspect_feed_headers(feed: Feed, issues: list[Issue]) -> None:
    """Read headers of every known table in feed without loading rows into memory."""
    for name in GTFS_FILES:
        required, required_cols, known_cols = GTFS_SPEC[name]
        fh = open_feed_table(feed, name)
        if fh is None:
            if required:
                issues.append(Issue("ERROR", feed.name, f"{name}.txt",
                                    "Required GTFS file is missing",
                                    hint=f"Add {name}.txt to {feed.path}"))
            continue
        try:
            with fh:
                reader = csv.reader(fh)
                raw_headers = next(reader, None)
                if not raw_headers:
                    issues.append(Issue("ERROR", feed.name, f"{name}.txt",
                                        "File is empty (no header row)",
                                        hint="Add a CSV header with the GTFS column names."))
                    continue
                headers = [h.strip() for h in raw_headers]
                feed.headers[name] = headers

                missing = [c for c in required_cols if c not in headers]
                if missing:
                    issues.append(Issue(
                        "ERROR", feed.name, f"{name}.txt",
                        f"Missing required column(s): {', '.join(missing)}",
                        hint=f"Add the column(s) to the header. GTFS requires: "
                             f"{', '.join(required_cols)}",
                    ))

                unknown = [c for c in headers if c and c not in known_cols]
                if unknown:
                    issues.append(Issue(
                        "WARNING", feed.name, f"{name}.txt",
                        f"Unknown column(s) ignored: {', '.join(unknown)}",
                        hint="These are not GTFS columns and will be dropped.",
                    ))
        except UnicodeDecodeError as exc:
            issues.append(Issue("ERROR", feed.name, f"{name}.txt",
                                f"File is not valid UTF-8: {exc}",
                                hint="Re-save the file as UTF-8."))
        except csv.Error as exc:
            issues.append(Issue("ERROR", feed.name, f"{name}.txt",
                                f"Malformed CSV: {exc}",
                                hint="Check for unbalanced quotes or stray delimiters."))


def _read_feed(feed: Feed, issues: list[Issue]) -> None:
    """Read every known .txt file of *feed* into memory, recording issues."""
    for name in GTFS_FILES:
        required, required_cols, known_cols = GTFS_SPEC[name]
        fh = open_feed_table(feed, name)

        if fh is None:
            if required:
                issues.append(Issue("ERROR", feed.name, f"{name}.txt",
                                    "Required GTFS file is missing",
                                    hint=f"Add {name}.txt to {feed.path}"))
            continue

        try:
            with fh:
                reader = csv.DictReader(fh)
                headers = [h.strip() for h in (reader.fieldnames or [])]
                if not headers:
                    issues.append(Issue("ERROR", feed.name, f"{name}.txt",
                                        "File is empty (no header row)",
                                        hint="Add a CSV header with the GTFS column names."))
                    continue

                feed.headers[name] = headers

                missing = [c for c in required_cols if c not in headers]
                if missing:
                    issues.append(Issue(
                        "ERROR", feed.name, f"{name}.txt",
                        f"Missing required column(s): {', '.join(missing)}",
                        hint=f"Add the column(s) to the header. GTFS requires: "
                             f"{', '.join(required_cols)}",
                    ))

                unknown = [c for c in headers if c and c not in known_cols]
                if unknown:
                    issues.append(Issue(
                        "WARNING", feed.name, f"{name}.txt",
                        f"Unknown column(s) ignored: {', '.join(unknown)}",
                        hint="These are not GTFS columns and will be dropped.",
                    ))

                rows: list[dict] = []
                for lineno, row in enumerate(reader, start=2):
                    clean = {}
                    for h in headers:
                        v = row.get(h)
                        clean[h] = v.strip() if isinstance(v, str) else ""
                    clean["__line__"] = lineno
                    rows.append(clean)
                feed.tables[name] = rows

        except UnicodeDecodeError as exc:
            issues.append(Issue("ERROR", feed.name, f"{name}.txt",
                                f"File is not valid UTF-8: {exc}",
                                hint="Re-save the file as UTF-8."))
        except csv.Error as exc:
            issues.append(Issue("ERROR", feed.name, f"{name}.txt",
                                f"Malformed CSV: {exc}",
                                hint="Check for unbalanced quotes or stray delimiters."))


def _validate_fields(feed: Feed, issues: list[Issue]) -> None:
    """Check per-field formats (numbers, dates, times, enums)."""
    def bad(table, row, col, msg, hint):
        issues.append(Issue("ERROR", feed.name, f"{table}.txt", msg,
                            line=row.get("__line__"), hint=hint))

    for row in feed.tables.get("stops", []):
        for col, lo, hi in (("stop_lat", -90, 90), ("stop_lon", -180, 180)):
            val = row.get(col, "")
            if not val:
                continue
            try:
                num = float(val)
            except ValueError:
                bad("stops", row, col, f"{col} is not a number: '{val}'",
                    "Use a decimal number, e.g. 24.4571")
                continue
            if not (lo <= num <= hi):
                bad("stops", row, col, f"{col} out of range: {num}",
                    f"Must be between {lo} and {hi}")

    for row in feed.tables.get("shapes", []):
        for col, lo, hi in (("shape_pt_lat", -90, 90), ("shape_pt_lon", -180, 180)):
            val = row.get(col, "")
            if not val:
                continue
            try:
                num = float(val)
            except ValueError:
                bad("shapes", row, col, f"{col} is not a number: '{val}'",
                    "Use a decimal number.")
                continue
            if not (lo <= num <= hi):
                bad("shapes", row, col, f"{col} out of range: {num}",
                    f"Must be between {lo} and {hi}")

    for row in feed.tables.get("stop_times", []):
        for col in ("arrival_time", "departure_time"):
            val = row.get(col, "")
            if not val:
                continue
            if not TIME_RE.match(val):
                bad("stop_times", row, col,
                    f"{col} is not a valid HH:MM:SS time: '{val}'",
                    "Use HH:MM:SS (hours may exceed 23, e.g. 25:10:00).")
            elif not PADDED_TIME_RE.match(val):
                # '8:43:00' parses fine but sorts before '10:00:00' as a string,
                # which breaks departure ordering in the API.
                bad("stop_times", row, col,
                    f"{col} is not zero-padded: '{val}'",
                    "Use two-digit hours, e.g. '08:43:00' instead of '8:43:00'.")
        seq = row.get("stop_sequence", "")
        if seq and not seq.isdigit():
            bad("stop_times", row, "stop_sequence",
                f"stop_sequence is not an integer: '{seq}'",
                "Use a whole number, e.g. 1")

    for row in feed.tables.get("calendar", []):
        for col in ("start_date", "end_date"):
            val = row.get(col, "")
            if val and not DATE_RE.match(val):
                bad("calendar", row, col, f"{col} is not YYYYMMDD: '{val}'",
                    "Use YYYYMMDD, e.g. 20261009")
        for col in ("monday", "tuesday", "wednesday", "thursday",
                    "friday", "saturday", "sunday"):
            val = row.get(col, "")
            if val and val not in ("0", "1"):
                bad("calendar", row, col, f"{col} must be 0 or 1: '{val}'",
                    "Use 0 (not running) or 1 (running).")

    for row in feed.tables.get("calendar_dates", []):
        val = row.get("date", "")
        if val and not DATE_RE.match(val):
            bad("calendar_dates", row, "date", f"date is not YYYYMMDD: '{val}'",
                "Use YYYYMMDD, e.g. 20261009")
        val = row.get("exception_type", "")
        if val and val not in ("1", "2"):
            bad("calendar_dates", row, "exception_type",
                f"exception_type must be 1 or 2: '{val}'",
                "1 = service added, 2 = service removed.")

    for row in feed.tables.get("routes", []):
        val = row.get("route_type", "")
        if val and not val.isdigit():
            bad("routes", row, "route_type", f"route_type is not an integer: '{val}'",
                "Use a GTFS route type, e.g. 3 for bus.")

    for row in feed.tables.get("trips", []):
        val = row.get("direction_id", "")
        if val and val not in ("0", "1"):
            bad("trips", row, "direction_id", f"direction_id must be 0 or 1: '{val}'",
                "Use 0 or 1.")


def _validate_required_values(feed: Feed, issues: list[Issue]) -> None:
    """Check that required columns are non-empty on every row."""
    for table, (_, required_cols, _) in GTFS_SPEC.items():
        rows = feed.tables.get(table)
        if not rows:
            continue
        empty_counts: Counter = Counter()
        first_line: dict[str, int] = {}
        for row in rows:
            for col in required_cols:
                if col in row and not row[col]:
                    empty_counts[col] += 1
                    first_line.setdefault(col, row["__line__"])
        for col, count in empty_counts.items():
            issues.append(Issue(
                "ERROR", feed.name, f"{table}.txt",
                f"Required column '{col}' is empty in {count} row(s)",
                line=first_line[col],
                hint=f"Fill in '{col}' for every row.",
            ))


def _validate_uniqueness(feed: Feed, issues: list[Issue]) -> None:
    """Check ID columns that must be unique."""
    for table, cols in UNIQUE_RULES.items():
        rows = feed.tables.get(table)
        if not rows:
            continue
        for col in cols:
            if col not in feed.headers.get(table, []):
                continue
            seen: dict[str, int] = {}
            dups: list[tuple[str, int, int]] = []
            for row in rows:
                val = row.get(col, "")
                if not val:
                    continue
                if val in seen:
                    dups.append((val, seen[val], row["__line__"]))
                else:
                    seen[val] = row["__line__"]
            for val, first, again in dups[:20]:
                issues.append(Issue(
                    "ERROR", feed.name, f"{table}.txt",
                    f"Duplicate {col} '{val}' (first seen on line {first})",
                    line=again,
                    hint=f"{col} must be unique within {table}.txt.",
                ))
            if len(dups) > 20:
                issues.append(Issue(
                    "WARNING", feed.name, f"{table}.txt",
                    f"{len(dups) - 20} more duplicate '{col}' value(s) not listed",
                    hint="Fix the ones above; the rest will appear on the next run.",
                ))


def _validate_references(feed: Feed, issues: list[Issue]) -> None:
    """Check foreign-key style references between tables."""
    available = set(feed.tables)

    def target_ids(target_file: str, target_col: str) -> Optional[set]:
        if target_file == "service":
            if "calendar" not in available and "calendar_dates" not in available:
                return None
            return (feed.ids("calendar", "service_id")
                    | feed.ids("calendar_dates", "service_id"))
        if target_file not in available:
            return None
        return feed.ids(target_file, target_col)

    for src_file, src_col, tgt_file, tgt_col, required in FK_RULES:
        rows = feed.tables.get(src_file)
        if not rows or src_col not in feed.headers.get(src_file, []):
            continue
        targets = target_ids(tgt_file, tgt_col)
        if targets is None:
            if required:
                issues.append(Issue(
                    "WARNING", feed.name, f"{src_file}.txt",
                    f"Cannot verify '{src_col}': {tgt_file} data is missing",
                    hint=f"Add {tgt_file}.txt to validate this reference.",
                ))
            continue

        missing: dict[str, int] = {}
        for row in rows:
            val = row.get(src_col, "")
            if val and val not in targets:
                missing.setdefault(val, row["__line__"])

        if missing:
            sample = list(missing.items())[:5]
            listed = ", ".join(f"'{v}' (line {ln})" for v, ln in sample)
            more = (f" and {len(missing) - len(sample)} more"
                    if len(missing) > len(sample) else "")
            issues.append(Issue(
                "ERROR", feed.name, f"{src_file}.txt",
                f"{len(missing)} value(s) in '{src_col}' not found in "
                f"{tgt_file}.{tgt_col}: {listed}{more}",
                line=sample[0][1],
                hint=f"Every {src_col} must exist in {tgt_file}.txt.",
            ))


def _validate_service_coverage(feed: Feed, issues: list[Issue]) -> None:
    """Check that every service_id used by trips is defined by a calendar."""
    if "trips" not in feed.tables:
        return
    cal = feed.ids("calendar", "service_id") | feed.ids("calendar_dates", "service_id")
    if not cal:
        issues.append(Issue(
            "ERROR", feed.name, "calendar.txt",
            "No service calendar data: neither calendar.txt nor calendar_dates.txt "
            "defines any service_id",
            hint="A GTFS feed needs calendar.txt or calendar_dates.txt.",
        ))
        return
    orphan = feed.ids("trips", "service_id") - cal
    if orphan:
        issues.append(Issue(
            "ERROR", feed.name, "trips.txt",
            f"{len(orphan)} service_id(s) used by trips but not defined in "
            f"calendar/calendar_dates: {', '.join(sorted(orphan)[:5])}",
            hint="Add the missing service_id to calendar.txt.",
        ))


def _validate_cross_feed(feeds: list[Feed], issues: list[Issue]) -> None:
    """
    Report IDs that appear in more than one feed.

    Merging prefixes IDs with the feed name, so nothing is lost — but the same
    stop or route living in two feeds is usually a mistake worth flagging.
    """
    if len(feeds) < 2:
        return
    for table, col in (("stops", "stop_id"), ("routes", "route_id"),
                       ("agency", "agency_id")):
        owners: dict[str, list[str]] = defaultdict(list)
        for feed in feeds:
            for val in feed.ids(table, col):
                owners[val].append(feed.name)
        shared = {v: o for v, o in owners.items() if len(o) > 1}
        if shared:
            sample = list(shared.items())[:5]
            listed = ", ".join(f"'{v}' in {'+'.join(o)}" for v, o in sample)
            issues.append(Issue(
                "WARNING", "(all feeds)", f"{table}.txt",
                f"{len(shared)} {col} value(s) appear in more than one feed: {listed}",
                hint="They will be kept separate via the feed prefix. Check whether "
                     "the same entity is duplicated across feeds.",
            ))


def validate(feeds: list[Feed]) -> list[Issue]:
    """Run every validation pass and return the collected issues."""
    issues: list[Issue] = []

    for feed in feeds:
        log.info("  Validating feed '%s' (%s)", feed.name, feed.path)
        _read_feed(feed, issues)
        _validate_required_values(feed, issues)
        _validate_fields(feed, issues)
        _validate_uniqueness(feed, issues)
        _validate_references(feed, issues)
        _validate_service_coverage(feed, issues)

    _validate_cross_feed(feeds, issues)
    return sorted(issues, key=Issue.sort_key)


# ── Error report ──────────────────────────────────────────────────────────────

def write_error_report(issues: list[Issue], feeds: list[Feed], roots: list[Path],
                       db_path: Path, log_dir: Path) -> Path:
    """Write a dated validation report and return its path."""
    log_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y-%m-%d_%H%M%S")
    report = log_dir / f"gtfs_errors_{stamp}.txt"

    errors   = [i for i in issues if i.severity == "ERROR"]
    warnings = [i for i in issues if i.severity == "WARNING"]
    infos    = [i for i in issues if i.severity == "INFO"]

    lines: list[str] = []
    lines.append("=" * 78)
    lines.append("GTFS VALIDATION REPORT")
    lines.append("=" * 78)
    lines.append(f"Generated : {datetime.now().isoformat(timespec='seconds')}")
    lines.append(f"GTFS roots: {', '.join(str(r.resolve()) for r in roots)}")
    lines.append(f"Target DB : {db_path.resolve()}")
    lines.append(f"Result    : {len(errors)} ERROR(S), {len(warnings)} WARNING(S), "
                 f"{len(infos)} INFO")
    if errors:
        lines.append("            Database was NOT built.")
    lines.append("")
    lines.append(f"Feeds ({len(feeds)}):")
    for feed in feeds:
        tables = ", ".join(sorted(feed.tables)) or "none"
        lines.append(f"  - {feed.name:<20} {feed.path}")
        lines.append(f"    tables: {tables}")
    lines.append("")

    def section(title: str, items: list[Issue], note: str) -> None:
        if not items:
            return
        lines.append("-" * 78)
        lines.append(f"{title} ({len(items)}) — {note}")
        lines.append("-" * 78)
        for i, issue in enumerate(items, 1):
            lines.append(f"{i:>3}. {issue.render()}")
        lines.append("")

    section("ERRORS", errors, "the database is NOT built until these are fixed")
    section("WARNINGS", warnings, "build continues, but review these")
    section("INFO", infos, "informational")

    lines.append("=" * 78)
    lines.append("How to fix")
    lines.append("=" * 78)
    lines.append("1. Open each file:line listed above in the feed folder.")
    lines.append("2. Apply the 'fix:' hint shown under each issue.")
    lines.append("3. Re-run:  python3 gtfs_builder.py --gtfs <root> --db <db>")
    lines.append("")
    lines.append("To build anyway despite errors:  add --force")
    lines.append("")

    text = "\n".join(lines)
    report.write_text(text, encoding="utf-8")
    (log_dir / "gtfs_errors_latest.txt").write_text(text, encoding="utf-8")
    return report


# ── Step 4 – Merge ────────────────────────────────────────────────────────────

def merge_feeds(feeds: list[Feed], prefix: bool) -> dict[str, list[dict]]:
    """
    Concatenate all feeds table by table.

    With *prefix*, every ID value is rewritten to `<feed>__<id>` so feeds can
    never collide. Columns are unioned across feeds.
    """
    merged: dict[str, list[dict]] = {}

    for table in GTFS_FILES:
        rows_out: list[dict] = []
        columns: list[str] = []
        for feed in feeds:
            rows = feed.tables.get(table)
            if not rows:
                continue
            for col in feed.headers.get(table, []):
                if col not in columns:
                    columns.append(col)

            for row in rows:
                new_row = {}
                for col in columns:
                    val = row.get(col, "")
                    if prefix and col in ID_COLUMNS and val:
                        val = f"{feed.name}{ID_SEP}{val}"
                    new_row[col] = val
                new_row["feed"] = feed.name
                rows_out.append(new_row)

        if rows_out:
            # Backfill columns the API expects but this feed omitted, so the
            # merged table always has a uniform schema.
            for col in API_REQUIRED_COLUMNS.get(table, []):
                if col not in columns:
                    columns.append(col)
                    for row in rows_out:
                        row[col] = ""
            merged[table] = rows_out
            log.info("  %-18s %7d rows  (%d feed(s))",
                     table, len(rows_out), len({r["feed"] for r in rows_out}))
    return merged


# ── Step 5 – Load into SQLite ─────────────────────────────────────────────────

def stream_load_gtfs(conn: sqlite3.Connection, feeds: list[Feed], prefix: bool,
                     batch_size: int = 50000) -> None:
    """Stream merged feeds into conn in batches to avoid high memory usage."""
    for table in GTFS_FILES:
        columns: list[str] = []
        feeds_with_table = []
        for feed in feeds:
            hdrs = feed.headers.get(table)
            if hdrs:
                feeds_with_table.append(feed)
                for col in hdrs:
                    if col not in columns:
                        columns.append(col)

        for col in API_REQUIRED_COLUMNS.get(table, []):
            if col not in columns:
                columns.append(col)

        if not feeds_with_table:
            _, required_cols, _ = GTFS_SPEC[table]
            for col in required_cols:
                if col not in columns:
                    columns.append(col)
            cols_with_feed = list(columns) + ["feed"]
            col_defs = ", ".join(f'"{c}" TEXT' for c in cols_with_feed)
            conn.execute(f'DROP TABLE IF EXISTS "{table}"')
            conn.execute(f'CREATE TABLE "{table}" ({col_defs})')
            conn.commit()
            log.info("  %-22s 0 rows (empty: not in any feed)", table)
            continue

        cols_with_feed = list(columns) + ["feed"]
        col_defs = ", ".join(f'"{c}" TEXT' for c in cols_with_feed)
        placeholders = ", ".join("?" * len(cols_with_feed))
        conn.execute(f'DROP TABLE IF EXISTS "{table}"')
        conn.execute(f'CREATE TABLE "{table}" ({col_defs})')

        total_rows = 0
        insert_sql = f'INSERT INTO "{table}" VALUES ({placeholders})'

        for feed in feeds_with_table:
            in_memory_rows = feed.tables.get(table)
            if in_memory_rows:
                batch = []
                feed_name = feed.name
                for row in in_memory_rows:
                    row_vals = []
                    for col in columns:
                        val = row.get(col, "")
                        if prefix and col in ID_COLUMNS and val:
                            val = f"{feed_name}{ID_SEP}{val}"
                        row_vals.append(val)
                    row_vals.append(feed_name)
                    batch.append(row_vals)
                    if len(batch) >= batch_size:
                        conn.executemany(insert_sql, batch)
                        total_rows += len(batch)
                        batch.clear()
                if batch:
                    conn.executemany(insert_sql, batch)
                    total_rows += len(batch)
                    batch.clear()
            else:
                fh = open_feed_table(feed, table)
                if fh is None:
                    continue
                with fh:
                    reader = csv.DictReader(fh)
                    batch = []
                    feed_name = feed.name
                    for row in reader:
                        row_vals = []
                        for col in columns:
                            val = row.get(col, "")
                            if val is None:
                                val = ""
                            else:
                                val = val.strip()
                            if prefix and col in ID_COLUMNS and val:
                                val = f"{feed_name}{ID_SEP}{val}"
                            row_vals.append(val)
                        row_vals.append(feed_name)
                        batch.append(row_vals)

                        if len(batch) >= batch_size:
                            conn.executemany(insert_sql, batch)
                            total_rows += len(batch)
                            batch.clear()

                    if batch:
                        conn.executemany(insert_sql, batch)
                        total_rows += len(batch)
                        batch.clear()

        conn.commit()
        log.info("  %-22s %d rows", table, total_rows)


def load_gtfs(conn: sqlite3.Connection, merged: dict[str, list[dict]]) -> None:
    """Write the merged tables into *conn*."""
    for table, rows in merged.items():
        columns = [c for c in rows[0].keys() if c != "__line__"]
        col_defs = ", ".join(f'"{c}" TEXT' for c in columns)
        placeholders = ", ".join("?" * len(columns))

        conn.execute(f'DROP TABLE IF EXISTS "{table}"')
        conn.execute(f'CREATE TABLE "{table}" ({col_defs})')
        conn.executemany(
            f'INSERT INTO "{table}" VALUES ({placeholders})',
            [[row.get(c, "") for c in columns] for row in rows],
        )
        conn.commit()
        log.info("  %-22s %d rows", table, len(rows))

    # Optional tables a feed may omit must still exist: the API queries them
    # unconditionally (e.g. calendar_dates). Create them empty, including the
    # columns the API reads.
    for table, (_, required_cols, _) in GTFS_SPEC.items():
        if table in merged:
            continue
        columns = list(required_cols)
        for col in API_REQUIRED_COLUMNS.get(table, []):
            if col not in columns:
                columns.append(col)
        columns.append("feed")
        col_defs = ", ".join(f'"{c}" TEXT' for c in columns)
        conn.execute(f'DROP TABLE IF EXISTS "{table}"')
        conn.execute(f'CREATE TABLE "{table}" ({col_defs})')
        conn.commit()
        log.info("  %-22s 0 rows (empty: not in any feed)", table)


def print_summary(conn: sqlite3.Connection) -> None:
    """Print a short summary of every table in the database."""
    tables = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
    ).fetchall()
    log.info("")
    log.info("Database summary:")
    log.info("  %-30s %s", "table", "rows")
    log.info("  " + "-" * 40)
    for (tbl,) in tables:
        (cnt,) = conn.execute(f'SELECT COUNT(*) FROM "{tbl}"').fetchone()
        log.info("  %-30s %d", tbl, cnt)


def create_id_indexes(conn: sqlite3.Connection) -> None:
    """
    Create non-unique indexes for every non-primary-key column named `*_id`
    across all tables.

    This accelerates common API patterns that filter or join by GTFS IDs.
    """
    tables = conn.execute(
        """
        SELECT name
        FROM sqlite_master
        WHERE type = 'table'
          AND name NOT LIKE 'sqlite_%'
        ORDER BY name
        """
    ).fetchall()

    created = 0
    for (table_name,) in tables:
        cols = conn.execute(f'PRAGMA table_info("{table_name}")').fetchall()
        for _, col_name, _, _, _, pk in cols:
            if pk:
                continue
            if not str(col_name).lower().endswith("_id"):
                continue

            idx_name = f"idx_{table_name}_{col_name}"
            conn.execute(
                f'CREATE INDEX IF NOT EXISTS "{idx_name}" '
                f'ON "{table_name}" ("{col_name}")'
            )
            created += 1

    # Composite indexes for the API's hottest queries: route+service lookups
    # (schedules) and per-trip stop ordering (first/last stop of a trip).
    composite = [
        ("idx_trips_route_service", "trips", "route_id, service_id"),
        ("idx_stop_times_trip_seq", "stop_times", "trip_id, stop_sequence"),
    ]
    for idx_name, table, cols in composite:
        if conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)
        ).fetchone():
            conn.execute(
                f'CREATE INDEX IF NOT EXISTS "{idx_name}" ON "{table}" ({cols})'
            )
            created += 1

    conn.commit()
    log.info("  Created/ensured %d ID indexes", created)


# ── Entry point ───────────────────────────────────────────────────────────────

def build(target: Target, log_dir: Path, prefix: bool, force: bool,
          skip_validation: bool = False) -> int:
    """
    Validate, merge and load one database. Returns 0 on success, 1 on failure.

    Nothing is written to *target.db_path* unless validation passes (or
    *force* is set).
    """
    log.info("")
    log.info("=" * 70)
    log.info("BUILD: %s  ->  %s", target.name, target.db_path)
    log.info("=" * 70)

    log.info("── Step 1: Discovering GTFS feeds ────────────────────")
    log.info("  Roots: %s", ", ".join(str(r) for r in target.roots))
    feeds, issues = discover_all(target.roots)
    if not feeds:
        report = write_error_report(issues, feeds, target.roots,
                                    target.db_path, log_dir)
        for issue in issues:
            log.error("%s", issue.render())
        log.error("No usable GTFS feeds found. Report → %s", report)
        return 1

    if target.feeds:
        unknown = set(target.feeds) - {f.name for f in feeds}
        if unknown:
            log.error("Unknown feed(s): %s. Available: %s",
                      ", ".join(sorted(unknown)),
                      ", ".join(f.name for f in feeds))
            return 1
        feeds = [f for f in feeds if f.name in target.feeds]
        log.info("  Selected %d feed(s): %s", len(feeds),
                 ", ".join(f.name for f in feeds))
    else:
        log.info("  Found %d feed(s): %s", len(feeds),
                 ", ".join(f.name for f in feeds))

    log.info("── Step 2: Validating ────────────────────────────────")
    if skip_validation:
        log.warning("  Validation skipped (--skip-validation)")
        for feed in feeds:
            _inspect_feed_headers(feed, issues)
        issues = []
    else:
        issues = issues + validate(feeds)

    errors   = [i for i in issues if i.severity == "ERROR"]
    warnings = [i for i in issues if i.severity == "WARNING"]

    if errors:
        report = write_error_report(issues, feeds, target.roots,
                                    target.db_path, log_dir)
        log.error("")
        log.error("Validation FAILED: %d error(s), %d warning(s)",
                  len(errors), len(warnings))
        for issue in errors[:15]:
            log.error("%s", issue.render())
        if len(errors) > 15:
            log.error("... and %d more error(s)", len(errors) - 15)
        log.error("")
        log.error("Full report → %s", report)
        if not force:
            log.error("Database NOT built. Fix the errors above and re-run.")
            log.error("To build anyway: add --force or set GTFS_FORCE=true")
            return 1
        log.warning("--force given: building despite %d error(s)", len(errors))

    if warnings:
        log.warning("Validation passed with %d warning(s)", len(warnings))
        for issue in warnings[:10]:
            log.warning("%s", issue.render())
        if len(warnings) > 10:
            log.warning("... and %d more warning(s) (see report)", len(warnings) - 10)
    else:
        log.info("  Validation passed: no issues")

    log.info("── Step 3: Loading SQLite ────────────────────────────")
    if prefix:
        log.info("  Prefixing IDs as <feed>%s<id>", ID_SEP)
    elif len(feeds) > 1:
        log.info("  Keeping original IDs (no prefix) across %d feeds", len(feeds))

    if target.db_path.exists():
        target.db_path.unlink()
        log.info("  Removed existing database: %s", target.db_path)

    target.db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(target.db_path)
    conn.execute("PRAGMA journal_mode=OFF")
    conn.execute("PRAGMA synchronous=OFF")
    conn.execute("PRAGMA cache_size=-128000")
    conn.execute("PRAGMA temp_store=MEMORY")
    try:
        stream_load_gtfs(conn, feeds, prefix=prefix)

        log.info("── Step 4: Creating ID indexes ───────────────────────")
        create_id_indexes(conn)

        conn.execute("PRAGMA journal_mode=WAL")
        print_summary(conn)
    finally:
        conn.close()

    log.info("")
    log.info("Done → %s", target.db_path.resolve())
    return 0


def main() -> int:
    load_dotenv()
    cfg = Config.from_env()

    parser = argparse.ArgumentParser(
        description="Validate, merge and load GTFS feeds into SQLite. "
                    "By default builds BOTH databases from .env: "
                    "transit (GTFS_ROOTS/GTFS_DB), ferry "
                    "(WATER_GTFS_ROOTS/WATER_GTFS_DB) and rail "
                    "(RAIL_GTFS_ROOTS/RAIL_GTFS_DB)."
    )
    parser.add_argument(
        "--only", choices=["transit", "ferry", "rail", "all"], default="all",
        help="Build only one database (default: all)"
    )
    parser.add_argument(
        "--gtfs", nargs="+", default=None, metavar="ROOT",
        help="Override the root folder(s) for the selected build"
    )
    parser.add_argument(
        "--db", default=None,
        help="Override the output SQLite file for the selected build"
    )
    parser.add_argument(
        "--feeds", default=None,
        help="Comma-separated feed folder names to include (default: all)"
    )
    parser.add_argument(
        "--log-dir", default=None,
        help=f"Folder for validation reports (default: GTFS_LOG_DIR or {cfg.log_dir})"
    )
    parser.add_argument(
        "--prefix", action="store_true",
        help="Prefix IDs with the feed name (<feed>__<id>). Off by default so "
             "IDs stay exactly as they appear in the GTFS files."
    )
    parser.add_argument(
        "--force", action="store_true",
        help="Build the database even if validation reports errors"
    )
    parser.add_argument(
        "--skip-validation", action="store_true",
        help="Skip validation entirely (not recommended)"
    )
    args = parser.parse_args()

    if (args.gtfs or args.db or args.feeds) and args.only == "all":
        args.only = "transit"

    log_dir = _resolve(args.log_dir) if args.log_dir else cfg.log_dir
    prefix  = cfg.prefix or args.prefix
    force   = cfg.force or args.force

    targets = cfg.targets if args.only == "all" else [
        t for t in cfg.targets if t.name == args.only
    ]

    # Apply single-build overrides.
    if args.gtfs or args.db or args.feeds:
        t = targets[0]
        if args.gtfs:
            t.roots = [_resolve(r) for r in args.gtfs]
        if args.db:
            t.db_path = _resolve(args.db)
        if args.feeds:
            t.feeds = [f.strip() for f in args.feeds.split(",") if f.strip()]

    log.info("Building %d database(s): %s", len(targets),
             ", ".join(f"{t.name} -> {t.db_path.name}" for t in targets))

    results: list[tuple[str, int, Path]] = []
    for target in targets:
        code = build(target, log_dir, prefix, force, args.skip_validation)
        results.append((target.name, code, target.db_path))

    log.info("")
    log.info("=" * 70)
    log.info("BUILD SUMMARY")
    log.info("=" * 70)
    for name, code, path in results:
        status = "OK     " if code == 0 else "FAILED "
        log.info("  %s %-8s %s", status, name, path.resolve())

    failed = [r for r in results if r[1] != 0]
    if failed:
        log.error("%d of %d build(s) failed", len(failed), len(results))
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
