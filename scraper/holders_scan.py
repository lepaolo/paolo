"""
Snapshot des DETENTEURS ACTUELS — raccourci "etat present" via /instances.

Pourquoi (raccourci du grand livre, decide avec Preda le 2026-07-10) :
le rejeu de l'archive des transferts CollectChain doit avaler le gros DUMP de
migration IMX->CollectChain (28/01/2026, des milliers de transferts a la meme
seconde) avant d'atteindre l'etat courant — plusieurs runs lents. Or l'explorer
Blockscout expose deja, pour chaque token, son PROPRIETAIRE ACTUEL et ses
metadonnees inline, via :

    GET https://collectscan.com/api/v2/tokens/<CONTRACT>/instances

Chaque item contient : id (=token_id), owner.hash (detenteur actuel),
image_url (=> categorie + veve_uuid), metadata (edition, totalEditions, name,
rarity, series, mintDate). On obtient donc DIRECTEMENT « qui detient quel
exemplaire aujourd'hui » — sans rejouer l'historique. C'est la source ideale
pour la cornerisation, les whales par HOLDINGS et les tailles de wallet.
Ce que ca NE donne PAS : l'historique comportemental (achats/ventes, duree de
detention, CollectorScore) — ca reste le role du scan des transferts, qui
continue en fond et enrichira plus tard.

Consistance : la pagination est un keyset DESCENDANT sur token_id (next_page_
params opaque de Blockscout). Chaque token est donc visite exactement une fois,
que son proprietaire change ou non pendant le scan (une vente ne deplace pas le
token dans l'ordre des id). Seule la valeur `owner` d'un token echange APRES son
passage peut etre perimee de quelques heures — corrige ensuite par le scan
incremental des transferts.

Fichiers produits :

    data/holders_scan_state.json          etat resumable (next_page_params,
                                          done, runs, pages, rows).
    archive/holders_runNNN.csv.gz         les lignes de la tranche NNN, uploadees
                                          en GitHub Release "holders-snapshot"
                                          par le workflow (PAS commite : repo
                                          leger). Colonnes :
                                          token_id, category, veve_uuid, edition,
                                          total_editions, owner, name, rarity,
                                          series, mint_date.

Reprise (CHECK-LIST scan resumable) : le curseur est RECHARGE depuis l'etat au
demarrage ; curseur nul => done ; checkpoint (etat + flush archive) tous les
SAVE_EVERY_PAGES ; un garde-fou verifie que le compteur de lignes PROGRESSE.

Env :
    HOLDERS_MINUTES     budget temps par run (defaut 280)
    HOLDERS_MAX_PAGES   budget pages par run (defaut 0 = illimite)
    HOLDERS_PAUSE       pause entre pages (defaut 0.15 s)
    HOLDERS_RESET       "true" = repartir de zero (ignore l'etat existant)
"""

from __future__ import annotations

import csv
import gzip
import io
import json
import os
import sys
import time
from typing import Any, Dict, List

from scraper import collectchain as cc

DATA_DIR = os.environ.get("HOLDERS_DATA_DIR", "data")
STATE_JSON = os.path.join(DATA_DIR, "holders_scan_state.json")
ARCHIVE_DIR = os.environ.get("HOLDERS_ARCHIVE_DIR", "archive")

INSTANCES_URL = f"{cc.API_BASE}/tokens/{cc.CONTRACT}/instances"
COUNTERS_URL = f"{cc.API_BASE}/tokens/{cc.CONTRACT}/counters"

HEADER = ["token_id", "category", "veve_uuid", "edition", "total_editions",
          "owner", "name", "rarity", "series", "mint_date"]
SAVE_EVERY_PAGES = 500       # checkpoint intermediaire (crash-safety)


# ---------------------------------------------------------------------------
# Extraction
# ---------------------------------------------------------------------------

def _row(item: Dict[str, Any]) -> List[Any]:
    """Un item /instances -> ligne snapshot (detenteur actuel + metadonnees)."""
    cat, uuid = cc._categorise(item)          # item a image_url + metadata
    md = item.get("metadata") or {}
    if not isinstance(md, dict):
        md = {}
    owner = ((item.get("owner") or {}).get("hash") or "").lower()
    ed = md.get("edition")
    tot = md.get("totalEditions")
    return [
        str(item.get("id") or ""),
        cat,
        uuid,
        ed if ed not in (None, "") else "",
        tot if tot not in (None, "") else "",
        owner,
        md.get("name") or "",
        md.get("rarity") or "",
        md.get("series") or "",
        md.get("mintDate") or md.get("dropDate") or "",
    ]


def _flush_archive(path: str, rows: List[List[Any]], write_header: bool) -> int:
    """Append rows to a .csv.gz (concatenation de membres gzip = valide)."""
    if not rows:
        return 0
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    buf = io.StringIO()
    w = csv.writer(buf, lineterminator="\n")
    if write_header:
        w.writerow(HEADER)
    w.writerows(rows)
    with open(path, "ab") as f:
        f.write(gzip.compress(buf.getvalue().encode("utf-8")))
    return len(rows)


# ---------------------------------------------------------------------------
# State
# ---------------------------------------------------------------------------

def _load_state() -> Dict[str, Any]:
    if not os.path.exists(STATE_JSON):
        return {}
    with open(STATE_JSON, encoding="utf-8") as f:
        return json.load(f)


def _save_state(state: Dict[str, Any]) -> None:
    os.makedirs(os.path.dirname(STATE_JSON) or ".", exist_ok=True)
    tmp = STATE_JSON + ".tmp"
    import datetime as _dt
    state["updated_at"] = _dt.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=1)
    os.replace(tmp, STATE_JSON)


def _total_instances() -> int:
    """Nombre total d'instances (pour l'estimation d'avancement)."""
    try:
        c = cc._get(cc._session(), COUNTERS_URL, {})
        # Blockscout expose parfois le total via le token lui-meme.
        return 0  # counters ne donne pas le total d'instances de facon fiable
    except Exception:
        return 0


# ---------------------------------------------------------------------------
# Scan (etat present, resumable)
# ---------------------------------------------------------------------------

def snapshot_scan() -> int:
    budget_s = float(os.environ.get("HOLDERS_MINUTES", "280")) * 60
    max_pages = int(os.environ.get("HOLDERS_MAX_PAGES", "0"))
    pause = float(os.environ.get("HOLDERS_PAUSE", "0.15"))
    reset = os.environ.get("HOLDERS_RESET", "false").strip().lower() == "true"

    state: Dict[str, Any] = {} if reset else _load_state()
    if state.get("done"):
        print("Snapshot deja termine (state.done=true) — rien a faire.", flush=True)
        return 0

    run_no = int(state.get("runs", 0)) + 1
    apath = os.path.join(ARCHIVE_DIR, f"holders_run{run_no:03d}.csv.gz")
    if os.path.exists(apath):
        os.remove(apath)   # rejeu du meme run apres crash : on repart propre

    # CHECK-LIST #1 : RECHARGER le curseur depuis l'etat (jamais None au depart).
    params: Dict[str, Any] = dict(state.get("next_page_params") or {})
    rows_before = int(state.get("rows", 0))
    print(f"Snapshot detenteurs — run #{run_no}. "
          f"Etat : pages={state.get('pages', 0)}, lignes={rows_before}, "
          f"curseur={'oui' if params else 'DEBUT'}.", flush=True)

    session = cc._session()
    t0 = time.time()
    pages = 0
    rows_run = 0
    archived_run = 0
    abuf: List[List[Any]] = []
    header_pending = True
    done = False

    while True:
        if max_pages and pages >= max_pages:
            print(f"Budget pages atteint ({max_pages}).", flush=True)
            break
        if time.time() - t0 > budget_s:
            print(f"Budget temps atteint ({budget_s / 60:.0f} min).", flush=True)
            break

        data = cc._get(session, INSTANCES_URL, params)
        items = data.get("items", [])
        for it in items:
            abuf.append(_row(it))
            rows_run += 1
        pages += 1

        nxt = data.get("next_page_params")
        state.update(next_page_params=nxt,
                     pages=int(state.get("pages", 0)) + 1,
                     rows=rows_before + rows_run)

        if pages % 100 == 0:
            rate = pages / max(1.0, time.time() - t0)
            print(f"    ... {pages} pages ce run ({rate:.1f}/s), "
                  f"{rows_before + rows_run} lignes cumulees.", flush=True)
        # CHECK-LIST #4 : sauver le curseur a CHAQUE checkpoint (pas qu'a la fin).
        if pages % SAVE_EVERY_PAGES == 0:
            archived_run += _flush_archive(apath, abuf, header_pending)
            header_pending = False
            abuf = []
            _save_state(state)
            print(f"    checkpoint ({rows_before + rows_run} lignes, "
                  f"{archived_run} ecrites ce run).", flush=True)

        # CHECK-LIST #3 : curseur nul = FIN -> done (jamais repositionner).
        if not nxt:
            done = True
            print("DERNIER TOKEN ATTEINT — snapshot termine.", flush=True)
            break
        params = dict(nxt)
        if pause:
            time.sleep(pause)

    archived_run += _flush_archive(apath, abuf, header_pending)
    state["done"] = done
    state["runs"] = run_no
    _save_state(state)

    # CHECK-LIST #6 : garde-fou progression (les lignes doivent augmenter).
    if not done and rows_run == 0:
        print("ALERTE : 0 ligne collectee ce run alors que done=false — "
              "curseur potentiellement bloque, verifier next_page_params.",
              file=sys.stderr, flush=True)
    print(f"Run termine : {pages} pages, {rows_run} lignes ce run "
          f"({archived_run} -> {apath}), total {rows_before + rows_run}, "
          f"done={done}, run #{run_no}, duree {time.time() - t0:.0f}s.", flush=True)
    return 0


def main() -> int:
    return snapshot_scan()


if __name__ == "__main__":
    sys.exit(main())
