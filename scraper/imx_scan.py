"""
IMX deep scan — historique VeVe sur Immutable X (ere pre-CollectChain).

VeVe a d'abord tout minte sur Immutable X (contrat
0xa7aefead2f25972d80516628417ac46b3f2604af) de fin 2021 a ~2026 avant/pendant
la migration vers CollectChain. L'API officielle Immutable X est morte (404),
mais le backend AWS AppSync GraphQL d'immutascan.io repond encore et pagine
TOUT l'historique des transferts. On l'aspire pour :

    - dater la VRAIE 1ere apparition des wallets (2022-2025), la ou le scan
      CollectChain plafonne a la date de migration -> vrais "anciens" wallets ;
    - archiver tous les transferts IMX (from, to, token, temps).

Sorties (memes formats que le scan CollectChain, fusion triviale ensuite) :
    data/wallet_registry_imx.csv    wallet, first_seen, last_active, tx_count
    data/imx_scan_state.json        curseur resumable
    archive/imx_transfers_runNNN.csv.gz  -> Release "imx-archive"
       colonnes : txn_id, txn_time_ms, date_pt, txn_type, from, to,
                  token_id, token_address

Curseur = maxTime (borne temporelle haute, ms). Chaque run repart de
maxTime=<plus vieux temps deja traite> et pagine vers le passe via nextToken
jusqu'a epuisement du budget ; robuste meme si le nextToken n'a pas survecu
entre deux runs GitHub. Doublons de frontiere dedupliques par txn_id au
moment de consommer l'archive.

Env : SCAN_MINUTES (defaut 280), SCAN_MAX_PAGES (0=illimite),
      SCAN_PAUSE (defaut 0.05), SCAN_ARCHIVE ("false" pour couper),
      SCAN_RESET ("true" = repartir de zero),
      IMX_GENESIS_MS (borne basse, defaut 1 jan 2021).
"""

from __future__ import annotations

import csv
import datetime as _dt
import gzip
import io
import json
import os
import sys
import time
from typing import Any, Dict, List, Optional
from zoneinfo import ZoneInfo

import requests

# --- source (backend public d'immutascan ; cle API publique du site) --------
API_URL = "https://qbolqfa7fnctxo3ooupoqrslem.appsync-api.us-east-2.amazonaws.com/graphql"
API_KEY = "da2-ceptv3udhzfmbpxr3eqisx3coe"
CONTRACT = "0xa7aefead2f25972d80516628417ac46b3f2604af"
QUERY = (
    "query L($address:String!,$pageSize:Int,$nextToken:String,$maxTime:Float){"
    "listTransactionsV2(address:$address,limit:$pageSize,nextToken:$nextToken,maxTime:$maxTime){"
    "items{txn_time txn_id txn_type transfers{from_address to_address "
    "token{type quantity token_address token_id}}} nextToken}}"
)

DATA_DIR = os.environ.get("WALLET_DATA_DIR", "data")
IMX_CSV = os.path.join(DATA_DIR, "wallet_registry_imx.csv")
STATE_JSON = os.path.join(DATA_DIR, "imx_scan_state.json")
ARCHIVE_DIR = os.environ.get("SCAN_ARCHIVE_DIR", "archive")

PT = ZoneInfo("America/Los_Angeles")
ZERO = "0x0000000000000000000000000000000000000000"
MARKET_ESCROW = "0xb1af72a77b9065c55cda0680b86655a79b62e42c"
_SKIP = {ZERO, MARKET_ESCROW, ""}

HEADER = ["wallet", "first_seen", "last_active", "tx_count"]
ARCHIVE_HEADER = ["txn_id", "txn_time_ms", "date_pt", "txn_type",
                  "from", "to", "token_id", "token_address"]
PAGE_SIZE = int(os.environ.get("IMX_PAGE_SIZE", "100"))
SAVE_EVERY_PAGES = 500
REQUEST_TIMEOUT = 60
MAX_RETRIES = 5
RETRY_BACKOFF = 3
GENESIS_MS = int(os.environ.get("IMX_GENESIS_MS", "1609459200000"))  # 2021-01-01


def _session() -> requests.Session:
    s = requests.Session()
    s.headers.update({"content-type": "application/json", "x-api-key": API_KEY,
                      "origin": "https://immutascan.io",
                      "referer": "https://immutascan.io/"})
    return s


def _post(session: requests.Session, variables: Dict[str, Any]) -> Dict[str, Any]:
    last: Optional[Exception] = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = session.post(API_URL, json={"operationName": "L", "query": QUERY,
                                            "variables": variables},
                             timeout=REQUEST_TIMEOUT)
            r.raise_for_status()
            j = r.json()
            if j.get("errors"):
                raise RuntimeError(f"GraphQL errors: {str(j['errors'])[:200]}")
            return (j.get("data") or {}).get("listTransactionsV2") or {}
        except Exception as e:
            last = e
            wait = RETRY_BACKOFF * attempt
            print(f"    request failed ({attempt}/{MAX_RETRIES}): {e} — retry {wait}s",
                  flush=True)
            time.sleep(wait)
    raise RuntimeError(f"Gave up: {last}")


def _pt_date(ms: int) -> str:
    return _dt.datetime.fromtimestamp(ms / 1000, _dt.timezone.utc)\
        .astimezone(PT).strftime("%Y-%m-%d")


# ---- registry I/O (format identique au scan CollectChain) -----------------

def load_registry(path: str) -> Dict[str, Dict[str, Any]]:
    reg: Dict[str, Dict[str, Any]] = {}
    if not os.path.exists(path):
        return reg
    with open(path, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            w = (row.get("wallet") or "").strip().lower()
            if w:
                reg[w] = {"first": row.get("first_seen") or "",
                          "last": row.get("last_active") or "",
                          "tx": int(row.get("tx_count") or 0)}
    return reg


def save_registry(path: str, reg: Dict[str, Dict[str, Any]]) -> None:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    tmp = path + ".tmp"
    with open(tmp, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f, lineterminator="\n")
        w.writerow(HEADER)
        for wallet in sorted(reg):
            e = reg[wallet]
            w.writerow([wallet, e["first"], e["last"], e["tx"]])
    os.replace(tmp, path)


def _update(reg: Dict[str, Dict[str, Any]], wallet: str, date: str) -> None:
    w = (wallet or "").strip().lower()
    if w in _SKIP or not w.startswith("0x"):
        return
    e = reg.get(w)
    if e is None:
        reg[w] = {"first": date, "last": date, "tx": 1}
        return
    if not e["first"] or date < e["first"]:
        e["first"] = date
    if date > e["last"]:
        e["last"] = date
    e["tx"] += 1


# ---- archive ---------------------------------------------------------------

def _flush_archive(path: str, rows: List[List[Any]], write_header: bool) -> int:
    if not rows:
        return 0
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    buf = io.StringIO()
    w = csv.writer(buf, lineterminator="\n")
    if write_header:
        w.writerow(ARCHIVE_HEADER)
    w.writerows(rows)
    with open(path, "ab") as f:
        f.write(gzip.compress(buf.getvalue().encode("utf-8")))
    return len(rows)


# ---- state -----------------------------------------------------------------

def _load_state() -> Dict[str, Any]:
    if not os.path.exists(STATE_JSON):
        return {}
    with open(STATE_JSON, encoding="utf-8") as f:
        return json.load(f)


def _save_state(state: Dict[str, Any]) -> None:
    os.makedirs(os.path.dirname(STATE_JSON) or ".", exist_ok=True)
    tmp = STATE_JSON + ".tmp"
    state["updated_at"] = _dt.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=1)
    os.replace(tmp, STATE_JSON)


# ---- deep scan -------------------------------------------------------------

def deep_scan() -> int:
    budget_s = float(os.environ.get("SCAN_MINUTES", "280")) * 60
    max_pages = int(os.environ.get("SCAN_MAX_PAGES", "0"))
    pause = float(os.environ.get("SCAN_PAUSE", "0.05"))
    archive_on = os.environ.get("SCAN_ARCHIVE", "true").strip().lower() != "false"
    reset = os.environ.get("SCAN_RESET", "false").strip().lower() == "true"

    if reset:
        print("RESET : etat et registre IMX repartent de zero.", flush=True)
        state: Dict[str, Any] = {}
        reg: Dict[str, Dict[str, Any]] = {}
    else:
        state = _load_state()
        if state.get("done"):
            print("Scan IMX deja termine (done=true) — rien a faire.", flush=True)
            return 0
        reg = load_registry(IMX_CSV)

    run_no = int(state.get("runs", 0)) + 1
    apath = os.path.join(ARCHIVE_DIR, f"imx_transfers_run{run_no:03d}.csv.gz")
    if archive_on and os.path.exists(apath):
        os.remove(apath)

    # Curseur inter-run : borne haute temporelle (ms). None au 1er run.
    cursor_max = state.get("cursor_max_ms")
    print(f"Registre IMX : {len(reg)} wallets. pages cumulees={state.get('pages', 0)}, "
          f"curseur={_pt_date(cursor_max) if cursor_max else 'present'}. "
          f"Archive : {'ON -> ' + apath if archive_on else 'OFF'}", flush=True)

    session = _session()
    next_token = None            # best-effort intra-run
    t0 = time.time()
    pages = 0
    transfers = 0
    archived_run = 0
    abuf: List[List[Any]] = []
    header_pending = True
    oldest_ms = cursor_max or 0
    done = False

    while True:
        if max_pages and pages >= max_pages:
            print(f"Budget pages atteint ({max_pages}).", flush=True)
            break
        if time.time() - t0 > budget_s:
            print(f"Budget temps atteint ({budget_s/60:.0f} min).", flush=True)
            break

        variables: Dict[str, Any] = {"address": CONTRACT, "pageSize": PAGE_SIZE}
        if next_token:
            variables["nextToken"] = next_token
        elif cursor_max:
            variables["maxTime"] = float(cursor_max)

        data = _post(session, variables)
        items = data.get("items") or []
        if not items:
            done = True
            print("Plus d'items — GENESE IMX atteinte.", flush=True)
            break

        for it in items:
            try:
                ms = int(it.get("txn_time"))
            except (TypeError, ValueError):
                continue
            d = _pt_date(ms)
            ttype = it.get("txn_type") or ""
            for tr in (it.get("transfers") or []):
                frm = (tr.get("from_address") or "").lower()
                to = (tr.get("to_address") or "").lower()
                tok = tr.get("token") or {}
                if archive_on:
                    abuf.append([it.get("txn_id"), ms, d, ttype, frm, to,
                                 tok.get("token_id") or "", tok.get("token_address") or ""])
                _update(reg, frm, d)
                _update(reg, to, d)
                transfers += 1
            if ms and (oldest_ms == 0 or ms < oldest_ms):
                oldest_ms = ms
        pages += 1

        next_token = data.get("nextToken")

        if pages % 100 == 0:
            rate = pages / max(1.0, time.time() - t0)
            print(f"    ... {pages} pages ce run ({rate:.1f}/s), {len(reg)} wallets, "
                  f"{archived_run + len(abuf)} archives, remonte a {_pt_date(oldest_ms)}",
                  flush=True)
        if pages % SAVE_EVERY_PAGES == 0:
            if archive_on:
                archived_run += _flush_archive(apath, abuf, header_pending)
                header_pending = False
                abuf = []
            state.update(cursor_max_ms=oldest_ms, next_token=next_token,
                         pages=int(state.get("pages", 0)) + 500)
            save_registry(IMX_CSV, reg)
            _save_state(state)
            print(f"    checkpoint ({len(reg)} wallets, {archived_run} archives).",
                  flush=True)

        if oldest_ms and oldest_ms <= GENESIS_MS:
            done = True
            print("Borne genese atteinte (IMX_GENESIS_MS).", flush=True)
            break
        if not next_token:
            # fin de la fenetre nextToken : on repositionne le curseur temporel
            # au plus vieux temps vu et on continuera au prochain tour de boucle.
            if oldest_ms and oldest_ms > GENESIS_MS:
                cursor_max = oldest_ms
                next_token = None
            else:
                done = True
                print("Plus de nextToken et borne atteinte — termine.", flush=True)
                break
        if pause:
            time.sleep(pause)

    if archive_on:
        archived_run += _flush_archive(apath, abuf, header_pending)
    # pages cumulees : approx (par blocs de 500 aux checkpoints) — on ajoute le reste
    state["cursor_max_ms"] = oldest_ms
    state["next_token"] = next_token
    state["done"] = done
    state["runs"] = run_no
    state["archived"] = int(state.get("archived", 0)) + archived_run
    save_registry(IMX_CSV, reg)
    _save_state(state)
    print(f"Run termine : {pages} pages, {transfers} transferts "
          f"({archived_run} archives -> {apath if archive_on else '-'}), "
          f"{len(reg)} wallets, remonte a {_pt_date(oldest_ms) if oldest_ms else '-'}, "
          f"done={done}, run #{run_no}, duree {time.time()-t0:.0f}s.", flush=True)
    return 0


def main() -> int:
    return deep_scan()


if __name__ == "__main__":
    sys.exit(main())
