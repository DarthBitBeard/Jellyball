"""Schema migrations owned by the Jellyfin and guide lane (versions 100-199).

Add a (version, function) pair to MIGRATIONS; db.py folds every lane's list
into SCHEMA_MIGRATIONS and applies the pending ones in version order. Rules:

- Stay inside this lane's range (db.MIGRATION_RANGES). A test enforces it, so
  two lanes can never pick the same number.
- A function receives the open sqlite3 connection and must be idempotent
  (CREATE TABLE IF NOT EXISTS, a column check before ALTER TABLE): a fresh
  database runs every migration too.
- Never edit or renumber a migration that has shipped. Add a new one.
- This module must not import db (db imports it).
"""

MIGRATIONS: list = []
