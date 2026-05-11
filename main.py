import json
import os
from datetime import datetime
from pathlib import Path
from typing import List, Optional

import psycopg2
import psycopg2.extras
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

DATABASE_URL = os.environ.get("DATABASE_URL", "")

CONDITION_CODES = ["V1", "V2", "V3", "G1", "G2", "G3", "S", "Stg", "K", "R", "M", "Sb"]


def get_db() -> psycopg2.extensions.connection:
    url = DATABASE_URL
    if url.startswith("postgres://"):
        url = url.replace("postgres://", "postgresql://", 1)
    return psycopg2.connect(url, cursor_factory=psycopg2.extras.RealDictCursor)


def row_to_dict(row: dict) -> dict:
    d = dict(row)
    if "created_at" in d and hasattr(d["created_at"], "isoformat"):
        d["created_at"] = d["created_at"].isoformat()
    if "conditions" in d and isinstance(d["conditions"], str):
        d["conditions"] = json.loads(d["conditions"])
    return d


def init_db():
    conn = get_db()
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS trees (
            id          SERIAL PRIMARY KEY,
            tree_code   TEXT NOT NULL UNIQUE,
            block       TEXT NOT NULL,
            row_num     INTEGER NOT NULL,
            plot_num    INTEGER NOT NULL,
            variety     TEXT NOT NULL
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS observations (
            id          SERIAL PRIMARY KEY,
            tree_code   TEXT NOT NULL,
            block       TEXT NOT NULL,
            conditions  TEXT NOT NULL,
            notes       TEXT,
            observed_at TEXT NOT NULL,
            created_at  TIMESTAMP NOT NULL DEFAULT NOW(),
            FOREIGN KEY (tree_code) REFERENCES trees(tree_code)
        )
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_obs_tree    ON observations(tree_code)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_obs_date    ON observations(observed_at)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_trees_block ON trees(block)")
    conn.commit()

    # Auto-seed trees on first deploy
    cur.execute("SELECT COUNT(*) AS n FROM trees")
    if cur.fetchone()["n"] == 0:
        from seed import TREES
        psycopg2.extras.execute_batch(
            cur,
            "INSERT INTO trees (tree_code, block, row_num, plot_num, variety) "
            "VALUES (%s, %s, %s, %s, %s) ON CONFLICT DO NOTHING",
            TREES,
        )
        conn.commit()
        print(f"Auto-seeded {len(TREES)} trees")

    cur.close()
    conn.close()


app = FastAPI(title="Meisari Avocado Farm", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

init_db()


# --- Schemas ---

class ObservationIn(BaseModel):
    tree_code: str
    conditions: List[str]
    notes: Optional[str] = None
    observed_at: Optional[str] = None  # YYYY-MM-DD


# --- Routes ---

@app.get("/blocks")
def list_blocks():
    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT DISTINCT block FROM trees ORDER BY block")
    rows = cur.fetchall()
    cur.close(); conn.close()
    return [r["block"] for r in rows]


@app.get("/trees")
def list_trees(block: Optional[str] = Query(None)):
    conn = get_db()
    cur = conn.cursor()
    if block:
        cur.execute(
            "SELECT tree_code, block, row_num, plot_num, variety "
            "FROM trees WHERE block = %s ORDER BY row_num, plot_num",
            (block,),
        )
    else:
        cur.execute(
            "SELECT tree_code, block, row_num, plot_num, variety "
            "FROM trees ORDER BY block, row_num, plot_num"
        )
    rows = cur.fetchall()
    cur.close(); conn.close()
    return [dict(r) for r in rows]


@app.get("/condition-codes")
def list_condition_codes():
    return CONDITION_CODES


@app.post("/observations", status_code=201)
def create_observation(obs: ObservationIn):
    if not obs.conditions:
        raise HTTPException(400, "At least one condition code is required")

    invalid = [c for c in obs.conditions if c not in CONDITION_CODES]
    if invalid:
        raise HTTPException(400, f"Invalid condition codes: {invalid}")

    conn = get_db()
    cur = conn.cursor()

    cur.execute("SELECT block FROM trees WHERE tree_code = %s", (obs.tree_code,))
    tree = cur.fetchone()
    if not tree:
        cur.close(); conn.close()
        raise HTTPException(404, f"Tree '{obs.tree_code}' not found")

    observed_at = obs.observed_at or datetime.now().date().isoformat()

    cur.execute(
        "INSERT INTO observations (tree_code, block, conditions, notes, observed_at) "
        "VALUES (%s, %s, %s, %s, %s) RETURNING *",
        (obs.tree_code, tree["block"], json.dumps(obs.conditions), obs.notes, observed_at),
    )
    row = cur.fetchone()
    conn.commit()
    cur.close(); conn.close()
    return row_to_dict(row)


@app.get("/observations")
def list_observations(
    block: Optional[str] = Query(None),
    tree_code: Optional[str] = Query(None),
    date_from: Optional[str] = Query(None),
    date_to: Optional[str] = Query(None),
    limit: int = Query(100, le=500),
):
    conn = get_db()
    cur = conn.cursor()

    query = "SELECT * FROM observations WHERE 1=1"
    params: list = []

    if block:
        query += " AND block = %s"
        params.append(block)
    if tree_code:
        query += " AND tree_code = %s"
        params.append(tree_code)
    if date_from:
        query += " AND observed_at >= %s"
        params.append(date_from)
    if date_to:
        query += " AND observed_at <= %s"
        params.append(date_to)

    query += " ORDER BY created_at DESC LIMIT %s"
    params.append(limit)

    cur.execute(query, params)
    rows = cur.fetchall()
    cur.close(); conn.close()
    return [row_to_dict(r) for r in rows]


@app.get("/stats")
def stats():
    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT block, COUNT(*) AS tree_count FROM trees GROUP BY block ORDER BY block")
    tree_rows = cur.fetchall()
    cur.execute("SELECT block, COUNT(*) AS obs_count FROM observations GROUP BY block ORDER BY block")
    obs_rows = cur.fetchall()
    cur.close(); conn.close()

    obs_map = {r["block"]: r["obs_count"] for r in obs_rows}
    return [
        {"block": r["block"], "trees": r["tree_count"], "observations": obs_map.get(r["block"], 0)}
        for r in tree_rows
    ]


@app.get("/health")
def health():
    return {"status": "ok"}


if Path("static").exists():
    app.mount("/", StaticFiles(directory="static", html=True), name="static")
