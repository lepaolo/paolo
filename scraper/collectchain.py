"""
CollectChain (collectscan.com) on-chain activity collector.

VeVe mints every collectible & comic on its own chain ("Collect Blockchain"),
explorable at https://collectscan.com — a standard Blockscout instance with a
public REST API (/api/v2). Everything lives on a single ERC-721 contract:

    0xbcFEbA7A9dA14f5C9453bDA72E2098537867B3c7   (name "VeVe", ~706k holders)

Each token transfer returned by the API embeds the NFT metadata inline
(name, rarity, series, brand, edition #, totalEditions) plus an image URL of
the form:

    .../collectible_type_image.<UUID>...   -> it's a COLLECTIBLE, UUID = veve_uuid
    .../comic_cover.<UUID>...              -> it's a COMIC,       UUID = comic/cover id

so we can recognise the category on-chain AND join back to the catalogue
scraped from my-nft-tracker (`veve_uuid` / `series_uuid` columns).

Transfer kinds:
    from == 0x000...0  -> MINT   (drop)
    to   == 0x000...0  -> BURN
    otherwise          -> MARKET (wallet -> wallet: sale / trade / gift)

This module fetches transfers (newest first, keyset pagination) down to a
cutoff timestamp or a previously stored checkpoint, and aggregates them into
one row per (day, account) with mint / market-in / market-out counters split
by category. Accounts count as active whether they SEND or RECEIVE.
"""

from __future__ import annotations

import datetime as _dt
import re
import time
from typing import Any, Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

import requests

# Les journees on-chain sont decoupees en heure PACIFIQUE (fuseau metier VeVe),
# et plus en UTC (changement 2026-07-08 — re-backfill requis pour l'historique).
PT = ZoneInfo("America/Los_Angeles")

API_BASE = "https://collectscan.com/api/v2"
CONTRACT = "0xbcFEbA7A9dA14f5C9453bDA72E2098537867B3c7"
TRANSFERS_URL = f"{API_BASE}/tokens/{CONTRACT}/transfers"
STATS_URL = f"{API_BASE}/stats"

ZERO = "0x0000000000000000000000000000000000000000"
# VeVe secondary-market escrow wallet: listing a collectible transfers it here
# (seller -> escrow); a sale is escrow -> buyer; a cancel is escrow -> seller.
# The DEPOSIT transfer's `from` reveals the seller wallet behind a market listing.
MARKET_ESCROW = "0xb1af72a77b9065c55cda0680b86655a79b62e42c"
# VeVe burn/vault sink: recoit les burns/crafts des utilisateurs ET les
# "vault mints" (stock invendu minte directement au coffre, ex. 15 120
# Street Fighter V le 2026-07-01). 1 449 328 transferts ENTRANTS, zero
# sortant depuis toujours (verifie sur CollectScan le 2026-07-08).
BURN_SINK = "0x39e3816a8c549ec22cd1a34a8cf7034b3941d8b1"
# Wallets systeme : jamais comptes comme des comptes actifs dans les stats.
SYSTEM_WALLETS = {ZERO, MARKET_ESCROW, BURN_SINK}

REQUEST_TIMEOUT = 60
MAX_RETRIES = 5
RETRY_BACKOFF = 3          # seconds * attempt number
PAUSE_BETWEEN_PAGES = 0.15  # be polite to the public explorer
USER_AGENT = "veve-chain-tracker/1.0 (personal activity stats)"

_UUID_RE = re.compile(
    r"(collectible_type_image|comic_cover)\."
    r"([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})",
    re.I,
)


def _session() -> requests.Session:
    s = requests.Session()
    s.headers.update({"User-Agent": USER_AGENT, "Accept": "application/json"})
    return s


def _get(session: requests.Session, url: str, params: Dict[str, Any]) -> Dict[str, Any]:
    last: Optional[Exception] = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = session.get(url, params=params, timeout=REQUEST_TIMEOUT)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            last = e
            wait = RETRY_BACKOFF * attempt
            print(f"    request failed ({attempt}/{MAX_RETRIES}): {e} — retry in {wait}s",
                  flush=True)
            time.sleep(wait)
    raise RuntimeError(f"Gave up fetching {url} {params}: {last}")


def chain_totals() -> Dict[str, Any]:
    """Global chain stats (total addresses, total transactions, tx today)."""
    d = _get(_session(), STATS_URL, {})
    return {
        "total_addresses": d.get("total_addresses"),
        "total_transactions": d.get("total_transactions"),
        "transactions_today": d.get("transactions_today"),
    }


def _parse_ts(x: Any) -> Optional[_dt.datetime]:
    if not x:
        return None
    try:
        return _dt.datetime.fromisoformat(str(x).replace("Z", "+00:00")) \
                  .astimezone(_dt.timezone.utc).replace(tzinfo=None)
    except Exception:
        return None


def _categorise(instance: Dict[str, Any]) -> Tuple[str, str]:
    """Return (category, veve_uuid) from a token_instance.

    category: 'collectible' | 'comic' | 'unknown'
    veve_uuid: the UUID embedded in the image URL — joins with the catalogue
    sheet (`veve_uuid` for collectibles; cover id for comics).
    """
    img = (instance.get("image_url") or instance.get("media_url") or "")
    m = _UUID_RE.search(img)
    if m:
        cat = "collectible" if m.group(1).lower().startswith("collectible") else "comic"
        return cat, m.group(2).lower()
    md = instance.get("metadata") or {}
    if isinstance(md, dict):
        if "comicNumber" in md or "coverArtists" in md or "artists" in md:
            return "comic", ""
        if "rarity" in md or "editionType" in md:
            return "collectible", ""
    return "unknown", ""


def _flatten(item: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """One raw API transfer -> compact record (or None if unparseable)."""
    ts = _parse_ts(item.get("timestamp"))
    if ts is None:
        return None
    frm = ((item.get("from") or {}).get("hash") or "").lower()
    to = ((item.get("to") or {}).get("hash") or "").lower()
    total = item.get("total") or {}
    inst = (total.get("token_instance") or {}) if isinstance(total, dict) else {}
    cat, uuid = _categorise(inst)
    md = inst.get("metadata") or {}
    if frm == ZERO:
        # Mint direct au coffre = stock invendu "vaulte" par VeVe, pas un achat.
        kind = "vault_mint" if to == BURN_SINK else "mint"
    elif to == ZERO or to == BURN_SINK:
        kind = "burn"
    elif to == MARKET_ESCROW:
        kind = "listing"   # mise en vente (depot escrow), PAS une vente
    else:
        kind = "market"
    if not isinstance(md, dict):
        md = {}
    return {
        "ts": ts,
        # Date en PT : un "jour" = journee pacifique, pas UTC.
        "date": ts.replace(tzinfo=_dt.timezone.utc).astimezone(PT).strftime("%Y-%m-%d"),
        "block": item.get("block_number"),
        "log_index": item.get("log_index"),
        "tx_hash": item.get("transaction_hash") or item.get("tx_hash") or "",
        "from": frm,
        "to": to,
        "kind": kind,
        "category": cat,
        "veve_uuid": uuid,
        "token_id": str(total.get("token_id") or ""),
        "name": md.get("name") or "",
        "rarity": md.get("rarity") or "",
        "series": md.get("series") or "",
        "comic_number": str(md.get("comicNumber") or ""),
        "start_year": str(md.get("startYear") or ""),
        "total_editions": md.get("totalEditions") or "",
        # Edition / mint number of THIS token — joins to the Market issueNumber
        # (veve_uuid, edition) so we can attribute an offer's pseudo to a wallet.
        "edition": md.get("edition") if md.get("edition") not in (None, "") else "",
    }


def fetch_transfers(cutoff: _dt.datetime,
                    checkpoint: Optional[Tuple[int, int]] = None,
                    until: Optional[_dt.datetime] = None,
                    max_pages: int = 20000) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """Fetch transfers newest-first until `cutoff` (UTC) or the checkpoint.

    checkpoint: (block_number, log_index) of the newest transfer already
    processed by a previous run — we stop as soon as we reach it (incremental).

    until: exclusive upper bound (UTC). Transfers with ts >= until are SKIPPED
    and never advance the checkpoint. Pass today's 00:00 UTC to only ever process
    fully-finished days (the current, incomplete day is ignored until tomorrow).

    Returns (records, meta) where meta holds the new checkpoint (newest PROCESSED
    transfer) and counters.
    """
    session = _session()
    params: Dict[str, Any] = {}
    records: List[Dict[str, Any]] = []
    newest: Optional[Dict[str, Any]] = None
    pages = 0
    skipped_recent = 0
    stop = False

    while pages < max_pages and not stop:
        data = _get(session, TRANSFERS_URL, params)
        items = data.get("items", [])
        if not items:
            break
        for item in items:
            rec = _flatten(item)
            if rec is None:
                continue
            if checkpoint and rec["block"] is not None:
                if (rec["block"], rec["log_index"] or 0) <= checkpoint:
                    stop = True
                    break
            if rec["ts"] < cutoff:
                stop = True
                break
            if until is not None and rec["ts"] >= until:
                # current (incomplete) day — skip, and don't checkpoint past it.
                skipped_recent += 1
                continue
            if newest is None:
                newest = rec
            records.append(rec)
        pages += 1
        if pages % 50 == 0:
            oldest = records[-1]["ts"] if records else None
            print(f"    ... {pages} pages, {len(records)} transfers, at {oldest}",
                  flush=True)
        nxt = data.get("next_page_params")
        if stop or not nxt:
            break
        params = dict(nxt)
        time.sleep(PAUSE_BETWEEN_PAGES)

    meta = {
        "pages": pages,
        "count": len(records),
        "skipped_current_day": skipped_recent,
        "newest_block": newest["block"] if newest else None,
        "newest_log_index": (newest["log_index"] or 0) if newest else None,
        "newest_ts": newest["ts"].strftime("%Y-%m-%d %H:%M:%S") if newest else "",
    }
    print(f"Fetched {len(records)} transfers over {pages} pages "
          f"(newest processed block {meta['newest_block']}, "
          f"skipped {skipped_recent} from the current day).", flush=True)
    return records, meta


# ---------------------------------------------------------------------------
# Aggregation
# ---------------------------------------------------------------------------

# Counter columns of one (day, account) activity row.
ACTIVITY_FIELDS = [
    "mint_collectible", "mint_comic",
    "market_in_collectible", "market_in_comic",
    "market_out_collectible", "market_out_comic",
    "burn_collectible", "burn_comic",
]


def _bump(agg: Dict[Tuple[str, str], Dict[str, int]], date: str, account: str,
          field: str) -> None:
    if not account or account in SYSTEM_WALLETS:
        return
    row = agg.setdefault((date, account), {f: 0 for f in ACTIVITY_FIELDS})
    row[field] += 1


def aggregate_daily(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Records -> one row per (date, account) with activity counters.

    'unknown' category rows are counted with collectibles (rare; metadata
    occasionally missing on very fresh mints).
    """
    agg: Dict[Tuple[str, str], Dict[str, int]] = {}
    for r in records:
        cat = "comic" if r["category"] == "comic" else "collectible"
        if r["kind"] == "mint":
            _bump(agg, r["date"], r["to"], f"mint_{cat}")
        elif r["kind"] == "burn":
            _bump(agg, r["date"], r["from"], f"burn_{cat}")
        elif r["kind"] == "market":
            _bump(agg, r["date"], r["to"], f"market_in_{cat}")
            _bump(agg, r["date"], r["from"], f"market_out_{cat}")
        # "listing" (depot escrow = mise en vente) et "vault_mint" (stock
        # invendu minte au coffre) sont des mouvements systeme : ignores.
    rows = []
    for (date, account), counters in sorted(agg.items()):
        row = {"date": date, "account": account, **counters}
        row["total"] = sum(counters.values())
        rows.append(row)
    return rows


def escrow_listings(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Latest deposit INTO the market escrow per (veve_uuid, edition): its `from`
    is the seller wallet behind that listing. Returns
    [{veve_uuid, edition, seller_wallet, ts}]. Enables pseudo<->wallet by joining
    (veve_uuid, edition) to the Market issueNumber."""
    latest: Dict[tuple, Dict[str, Any]] = {}
    for r in records:
        uid = r.get("veve_uuid")
        ed = r.get("edition")
        if not uid or ed in (None, ""):
            continue
        if r.get("to") != MARKET_ESCROW:
            continue
        frm = r.get("from")
        if not frm or frm == ZERO or frm == MARKET_ESCROW:
            continue
        key = (uid, str(ed))
        cur = latest.get(key)
        if cur is None or r["ts"] > cur["_ts"]:
            latest[key] = {"veve_uuid": uid, "edition": str(ed),
                           "seller_wallet": frm, "_ts": r["ts"]}
    return [{"veve_uuid": v["veve_uuid"], "edition": v["edition"],
             "seller_wallet": v["seller_wallet"],
             "ts": v["_ts"].strftime("%Y-%m-%d %H:%M:%S")} for v in latest.values()]


def item_key(category: str, veve_uuid: str, name: str, rarity: str,
             comic_number: str = "", start_year: str = "") -> str:
    """Stable identity of one on-chain item (a collectible type, or a comic
    cover x rarity). UUID when we have it, else a normalised name key."""
    if veve_uuid:
        return f"{veve_uuid}|{rarity.lower()}" if category == "comic" else veve_uuid
    base = name.strip().lower()
    if category == "comic" and comic_number:
        base = f"{base} #{comic_number}"
        if start_year:
            base = f"{base} ({start_year})"
    return f"{category}:{base}|{rarity.lower()}"


def aggregate_items(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Records -> one row per (date, item): what was minted / traded / burnt,
    and by how many distinct wallets."""
    agg: Dict[Tuple[str, str], Dict[str, Any]] = {}
    for r in records:
        if r["kind"] in ("listing", "vault_mint"):
            continue   # mouvements systeme (mise en vente / stock vaulte)
        cat = "comic" if r["category"] == "comic" else "collectible"
        key = (r["date"], item_key(cat, r["veve_uuid"], r["name"], r["rarity"],
                                   r["comic_number"], r["start_year"]))
        row = agg.get(key)
        if row is None:
            row = agg[key] = {
                "date": r["date"], "category": cat,
                "veve_uuid": r["veve_uuid"], "name": r["name"],
                "rarity": r["rarity"], "series": r["series"],
                "comic_number": r["comic_number"], "start_year": r["start_year"],
                "total_editions": r["total_editions"],
                "mints": 0, "market": 0, "burns": 0,
                "_minters": set(), "_buyers": set(), "_sellers": set(),
            }
        if r["kind"] == "mint":
            row["mints"] += 1
            row["_minters"].add(r["to"])
        elif r["kind"] == "burn":
            row["burns"] += 1
        else:
            row["market"] += 1
            if r["to"] not in SYSTEM_WALLETS:
                row["_buyers"].add(r["to"])
            if r["from"] not in SYSTEM_WALLETS:
                row["_sellers"].add(r["from"])
    rows = []
    for (_, _), row in sorted(agg.items()):
        row["unique_minters"] = len(row.pop("_minters"))
        row["unique_buyers"] = len(row.pop("_buyers"))
        row["unique_sellers"] = len(row.pop("_sellers"))
        rows.append(row)
    return rows


# ---------------------------------------------------------------------------
# Window stats (24h / 7d / 30d) computed from (date, account) activity rows
# ---------------------------------------------------------------------------

# "total" = tout l'historique conservé dans le sheet (RETENTION_DAYS, ~35 j)
WINDOWS = [("24h", 1), ("48h", 2), ("7j", 7), ("30j", 30), ("total", 3650)]


def _sum_fields(rows: List[Dict[str, Any]], fields: List[str]) -> int:
    return sum(int(r.get(f, 0) or 0) for r in rows for f in fields)


def compute_window_stats(activity_rows: List[Dict[str, Any]],
                         today: Optional[_dt.date] = None) -> List[Dict[str, Any]]:
    """activity_rows: (date, account) rows — possibly with duplicate
    (date, account) pairs across runs; counters simply sum.

    Returns one stats row per (window x scope), scope in
    {all, mints, market} x {all, collectible, comic}.
    """
    today = today or _dt.datetime.utcnow().date()
    out: List[Dict[str, Any]] = []

    for label, days in WINDOWS:
        start = (today - _dt.timedelta(days=days - 1)).strftime("%Y-%m-%d")
        win = [r for r in activity_rows if str(r.get("date", "")) >= start]

        def scope_row(scope: str, cat: str, fields: List[str]) -> Dict[str, Any]:
            accounts = {r["account"] for r in win
                        if any(int(r.get(f, 0) or 0) > 0 for f in fields)}
            # market wallet->wallet moves are counted twice (in + out); a
            # "transfer" is the movement itself, so halve the market legs.
            tx = _sum_fields(win, fields)
            in_f = [f for f in fields if f.startswith("market_in")]
            out_f = [f for f in fields if f.startswith("market_out")]
            if in_f and out_f:
                tx -= min(_sum_fields(win, in_f), _sum_fields(win, out_f))
            n = len(accounts)
            return {
                "window": label,
                "scope": scope,
                "category": cat,
                "nft_transfers": tx,
                "unique_accounts": n,
                "tx_per_account": round(tx / n, 2) if n else 0,
            }

        mint_c = ["mint_collectible"]; mint_b = ["mint_comic"]
        mkt_c = ["market_in_collectible", "market_out_collectible"]
        mkt_b = ["market_in_comic", "market_out_comic"]
        burn_c = ["burn_collectible"]; burn_b = ["burn_comic"]

        out.append(scope_row("all", "all",
                             mint_c + mint_b + mkt_c + mkt_b + burn_c + burn_b))
        out.append(scope_row("all", "collectible", mint_c + mkt_c + burn_c))
        out.append(scope_row("all", "comic", mint_b + mkt_b + burn_b))
        out.append(scope_row("mints", "all", mint_c + mint_b))
        out.append(scope_row("mints", "collectible", mint_c))
        out.append(scope_row("mints", "comic", mint_b))
        out.append(scope_row("market", "all", mkt_c + mkt_b))
        out.append(scope_row("market", "collectible", mkt_c))
        out.append(scope_row("market", "comic", mkt_b))
    return out


def compute_top_accounts(activity_rows: List[Dict[str, Any]],
                         today: Optional[_dt.date] = None,
                         top_n: int = 20) -> List[Dict[str, Any]]:
    """Most active accounts per window (by total moves, mint+market+burn)."""
    today = today or _dt.datetime.utcnow().date()
    out: List[Dict[str, Any]] = []
    for label, days in WINDOWS:
        start = (today - _dt.timedelta(days=days - 1)).strftime("%Y-%m-%d")
        per: Dict[str, Dict[str, int]] = {}
        for r in activity_rows:
            if str(r.get("date", "")) < start:
                continue
            acc = r["account"]
            d = per.setdefault(acc, {f: 0 for f in ACTIVITY_FIELDS})
            for f in ACTIVITY_FIELDS:
                d[f] += int(r.get(f, 0) or 0)
        ranked = sorted(per.items(),
                        key=lambda kv: sum(kv[1].values()), reverse=True)[:top_n]
        for rank, (acc, d) in enumerate(ranked, 1):
            out.append({
                "window": label, "rank": rank, "account": acc,
                "total": sum(d.values()),
                "mints": d["mint_collectible"] + d["mint_comic"],
                "market_in": d["market_in_collectible"] + d["market_in_comic"],
                "market_out": d["market_out_collectible"] + d["market_out_comic"],
                "collectibles": sum(v for k, v in d.items() if k.endswith("collectible")),
                "comics": sum(v for k, v in d.items() if k.endswith("comic")),
                "explorer_url": f"https://collectscan.com/address/{acc}",
            })
    return out


if __name__ == "__main__":
    import sys
    hours = 2 if "--test" in sys.argv else 24
    cutoff = _dt.datetime.utcnow() - _dt.timedelta(hours=hours)
    recs, meta = fetch_transfers(cutoff)
    rows = aggregate_daily(recs)
    print(f"{len(recs)} transfers -> {len(rows)} (day, account) rows")
    for s in compute_window_stats(rows):
        print(s)
