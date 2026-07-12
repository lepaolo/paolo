"""Flux VeVe PUBLIC — revenue reel, ventes en gems, pseudos (12/07/2026).

UNE source pour trois besoins (endpoint public, AUCUN cookie) :
  GET https://www.stackr.world/api/trpc/publicVeve.getVeveTransactions
      ?input={"json":{"limit":100,"cursor":<page>}}

Sonde du 12/07 :
  * `cursor` = numero de PAGE (1 = plus recent, absent = page 1) et `limit` =
    taille de page ; cursor=2/limit=5 renvoie bien les items 6 a 10 ;
  * la pagination profonde MARCHE (page 100 x limit 100 = 10 000 tx = 6 jours
    en arriere) — pas de mur a ~750 comme getAllLatestSales_v2 ;
  * rythme observe : ~1 700 tx/jour ;
  * `price` est un decimal NORMALISE (gems ~ $, meme pour MARKET_STACKR : une
    Ultra Rare vendue « 14.00 » ne peut pas etre 14 OMI) — a re-valider au 1er
    run contre _MarketRevenue (memes jours, meme ordre de grandeur).

veve_type :
  * CART_FIAT   : achat boutique en monnaie fiat   -> REVENUE DROP reel
  * STORE_GEM   : achat boutique en gems           -> REVENUE DROP reel
  * MARKET_FIXED: vente marche VeVe (gems)         -> REVENUE MARKET (VeVe)
  * MARKET_STACKR : vente marche StackR            -> REVENUE MARKET (StackR)
  * NFT_TRANSFER: jambe de reglement d'un trade (MEME nft/prix, quelques
    secondes apres) -> EXCLU des revenus (sinon double comptage)
  * ADMIN_COLLECTIBLE_TRANSFER : livraison VeVe (support/rewards) -> EXCLU

Sorties :
  * onglet cache `_VeveRevenue` (upsert par jour PT, RAW natif) ;
  * `data/veve_tx_daily.csv` (commite, meme contenu — sert de repli/historique) ;
  * paires (wallet -> pseudo) recoltees au passage, fusionnees dans 🟣C-PSEUDOS
    (remplace peu a peu les lookups StackR sous cookie : ici c'est PUBLIC).

Deux modes :
  * QUOTIDIEN (defaut) : re-lit la fenetre des VEVE_TX_DAYS derniers jours (3)
    et REMPLACE ces jours -> idempotent, ~50 requetes/jour ;
  * BACKFILL (VEVE_TX_BACKFILL=true) : descend jusqu'a VEVE_TX_UNTIL (ou la
    genese), dedup par veve_id, un seul run (le decalage du curseur pendant le
    run est absorbe par la dedup). A lancer sur le repo PUBLIC (minutes
    illimitees).

Env : SHEET_ID, VEVE_TX_DAYS (3), VEVE_TX_LIMIT (100), VEVE_TX_MAX_PAGES
      (400 en quotidien, 30000 en backfill), VEVE_TX_UNTIL (YYYY-MM-DD),
      VEVE_TX_PAUSE (0.25), VEVE_TX_TIMEOUT (60), VEVE_TX_CSV, VEVE_TX_PSEUDOS
      (true), VEVE_TX_BACKFILL (false).
"""

from __future__ import annotations

import csv
import datetime as _dt
import json
import os
import sys
import time
import urllib.parse
from collections import Counter, defaultdict
from typing import Any, Dict, List, Tuple

import requests

# Le Sheet est OPTIONNEL (leçon du 12/07 : le backfill lance sur `paolo`
# plantait a l'import, ce repo n'ayant ni scraper/sheets.py ni les identifiants
# Google). Un backfill n'a pas besoin du Sheet : il produit son CSV, que le
# repo principal ira lire. Sans sheets, on tourne en MODE CSV SEUL.
try:
    from scraper.sheets import _client, _open_worksheet, append_log
    SHEETS_OK = True
except Exception:                                    # pragma: no cover
    SHEETS_OK = False

    def _client():
        raise RuntimeError("scraper.sheets indisponible")

    def _open_worksheet(*a, **k):
        raise RuntimeError("scraper.sheets indisponible")

    def append_log(*a, **k):
        return None

TRPC = ("https://www.stackr.world/api/trpc/publicVeve.getVeveTransactions"
        "?input=")
UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/126.0 Safari/537.36")

REV_TAB = "_VeveRevenue"
REV_HEADER = ["date", "drop_usd", "drop_tx", "market_veve_usd",
              "market_veve_tx", "market_stackr_usd", "market_stackr_tx",
              "transfers", "admin_tx", "other_usd", "other_tx",
              "outlier_usd", "outlier_tx"]

# Typologie COMPLETE, etablie sur l'inventaire du 1er run (12/07) :
#   STORE_GEM      603 tx  219 027 $  achat boutique en gems  -> DROP
#   CART_FIAT       67 tx      632 $  achat boutique en fiat  -> DROP
#   MARKET_FIXED  1066 tx   25 650 $  vente marche VeVe       -> MARKET VeVe
#   MARKET_AUCTION  56 tx      399 $  vente aux encheres VeVe -> MARKET VeVe
#   MARKET_STACKR  356 tx  124 485 $  vente marche StackR     -> MARKET StackR
#   NFT_TRANSFER   356 tx  124 485 $  <- MEME total au centime pres que
#       MARKET_STACKR : c'est la JAMBE DE REGLEMENT du meme trade. EXCLU
#       (le compter doublerait le marche). Preuve faite par l'inventaire.
#   ADMIN_COLLECTIBLE_TRANSFER / ADMIN_COMIC_TRANSFER : livraisons VeVe -> EXCLU
#   CRAFT / PROMO_ENTITY_TRANSACTION : craft & promos -> colonne `other` (ni
#       drop ni marche : ce ne sont pas des ventes, mais on ne les jette pas).
DROP_TYPES = ("CART_FIAT", "STORE_GEM")
MKT_VEVE_TYPES = ("MARKET_FIXED", "MARKET_AUCTION")
MKT_STACKR = "MARKET_STACKR"
TRANSFER = "NFT_TRANSFER"
ADMIN_TYPES = ("ADMIN_COLLECTIBLE_TRANSFER", "ADMIN_COMIC_TRANSFER")
OTHER_TYPES = ("CRAFT", "PROMO_ENTITY_TRANSACTION")

DAYS = int(os.environ.get("VEVE_TX_DAYS", "3"))
LIMIT = int(os.environ.get("VEVE_TX_LIMIT", "100"))
PAUSE = float(os.environ.get("VEVE_TX_PAUSE", "0.25"))
TIMEOUT = int(os.environ.get("VEVE_TX_TIMEOUT", "60"))
CSV_PATH = os.environ.get("VEVE_TX_CSV", "data/veve_tx_daily.csv")
BACKFILL = os.environ.get("VEVE_TX_BACKFILL", "false").lower() == "true"
# CSV du backfill produit par l'autre repo (public) : le quotidien de `preda`
# le fusionne dans l'onglet _VeveRevenue -> l'historique remonte tout seul.
REMOTE_CSV = os.environ.get(
    "VEVE_TX_REMOTE_CSV",
    "https://raw.githubusercontent.com/lepaolo/paolo/main/data/veve_tx_daily.csv")
UNTIL = os.environ.get("VEVE_TX_UNTIL", "").strip()
WITH_PSEUDOS = os.environ.get("VEVE_TX_PSEUDOS", "true").lower() != "false"
MAX_PAGES = int(os.environ.get("VEVE_TX_MAX_PAGES",
                               "30000" if BACKFILL else "400"))
# seuil d'affichage des grosses transactions dans le log (diagnostic des pics)
BIG_TX = float(os.environ.get("VEVE_TX_BIG", "2000"))
# GARDE-FOU PRIX ABERRANTS (constate au 1er run, 12/07) : trois lignes a
# EXACTEMENT 100 000,00 $ sur « Basim - Hidden One » (#26 et #32) — deux
# STORE_GEM et une MARKET_STACKR — gonflaient a elles seules le drop du 11/07
# de 200 000 $ et le marche de 100 000 $. Ce sont des prix placeholder :
#   * un achat BOUTIQUE a 100 000 $ n'existe pas ;
#   * la source independante (getAllLatestSales_v2, onglet _MarketRevenue) ne
#     voit que 40 M OMI (~6 750 $) de ventes StackR ce jour-la, alors qu'une
#     vraie vente a 100 000 $ aurait exige ~594 M OMI a elle seule.
# Au-dela du seuil, la transaction est EXCLUE des revenus mais COMPTEE a part
# (colonnes outlier_usd/outlier_tx) et affichee dans le log — jamais jetee en
# silence. Mettre VEVE_TX_MAX_PRICE=0 pour desactiver le garde-fou.
MAX_PRICE = float(os.environ.get("VEVE_TX_MAX_PRICE", "50000"))
# Etat du BACKFILL (reprise) — leçon du run #1 : 730 pages / 52 min perdues sur
# un HTTP 500 TRANSITOIRE (re-teste juste apres : la meme page repond tres
# bien). Desormais : (1) beaucoup plus de retries avec backoff, (2) si ca
# insiste, on REDUIT la taille de page au meme endroit (100 -> 50 -> 25 -> 10),
# (3) en dernier recours on GARDE la recolte et on s'arrete proprement,
# (4) l'etat est sauvegarde -> le run suivant reprend ou on s'est arrete.
STATE_PATH = os.environ.get("VEVE_TX_STATE", "data/veve_tx_state.json")
RETRIES = int(os.environ.get("VEVE_TX_RETRIES", "6"))
# FLUSH INCREMENTAL (trou trouve le 12/07) : la recolte n'etait ecrite qu'a la
# FIN du run. Un long backfill ANNULE (quota, timeout, coupure) perdait donc
# tout — mon garde-fou ne couvrait que les erreurs API, pas l'arret du job.
# Desormais on ECRIT tous les FLUSH_PAGES : CSV + onglet + etat de reprise.
# Annuler un run ne coute plus que les quelques pages en cours.
FLUSH_PAGES = int(os.environ.get("VEVE_TX_FLUSH_PAGES", "200"))
LIMIT_STEPS = [100, 50, 25, 10]
COOLDOWN = float(os.environ.get("VEVE_TX_COOLDOWN", "10"))  # pause apres panne

# wallets systeme VeVe (jamais des pseudos d'utilisateurs)
SYSTEM_ADDRS = {
    "0xc4817870a6a75704985be4f9933643a27739afc1",   # VeveStore
    "0xdb721de5f825fcb3d2dbe3a4778e34e43ae7c095",   # admin (livraisons)
    "0x7be178ba43a9828c22997a3ec3640497d88d2fd3",   # VeveCollection (officiel)
}


# ---------------------------------------------------------------------------
# Dates
# ---------------------------------------------------------------------------

def _pt(created_at: str) -> str:
    """created_at (ISO UTC) -> jour PACIFIQUE (comme tout le reste du projet)."""
    s = (created_at or "").strip().replace("Z", "+00:00")
    try:
        dt = _dt.datetime.fromisoformat(s)
    except ValueError:
        return ""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=_dt.timezone.utc)
    try:
        from zoneinfo import ZoneInfo
        return dt.astimezone(ZoneInfo("America/Los_Angeles")).date().isoformat()
    except Exception:
        return (dt - _dt.timedelta(hours=8)).date().isoformat()


def _today_pt() -> str:
    return _pt(_dt.datetime.now(_dt.timezone.utc).isoformat())


def _f(x) -> float:
    try:
        return float(str(x).replace(",", ".") or 0)
    except (TypeError, ValueError):
        return 0.0


# ---------------------------------------------------------------------------
# API
# ---------------------------------------------------------------------------

def fetch_page(cursor: int, session=None, limit: int = LIMIT,
               retries: int = RETRIES):
    """Une page du flux (cursor = numero de page, 1 = la plus recente).

    Renvoie la liste d'items, ou None si l'API refuse obstinement (le HTTP 500
    en profondeur est TRANSITOIRE : backoff genereux avant d'abandonner)."""
    payload = {"limit": limit}
    if cursor > 1:
        payload["cursor"] = cursor
        payload["direction"] = "forward"
    url = TRPC + urllib.parse.quote(
        json.dumps({"json": payload}, separators=(",", ":")))
    s = session or requests
    for attempt in range(retries):
        try:
            r = s.get(url, headers={"User-Agent": UA,
                                    "Accept": "application/json"},
                      timeout=TIMEOUT)
            if r.status_code >= 500:
                raise RuntimeError(f"HTTP {r.status_code}")
            r.raise_for_status()
            data = r.json()
            return (data.get("result", {}).get("data", {}).get("json")) or []
        except Exception as e:
            if attempt == retries - 1:
                print(f"    page {cursor} (limit {limit}) : {e} — abandon "
                      f"apres {retries} essais.", flush=True)
                return None
            wait = min(60, 3 * (2 ** attempt))
            print(f"    page {cursor} (limit {limit}) : {e} — nouvel essai "
                  f"dans {wait} s ({attempt + 1}/{retries})", flush=True)
            time.sleep(wait)
    return None


def load_state() -> Dict:
    try:
        with open(STATE_PATH, encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def save_state(st: Dict) -> None:
    os.makedirs(os.path.dirname(STATE_PATH) or ".", exist_ok=True)
    with open(STATE_PATH, "w", encoding="utf-8") as f:
        json.dump(st, f, indent=1)


# ---------------------------------------------------------------------------
# Agregation
# ---------------------------------------------------------------------------

def _blank() -> Dict[str, float]:
    return {"drop_usd": 0.0, "drop_tx": 0, "market_veve_usd": 0.0,
            "market_veve_tx": 0, "market_stackr_usd": 0.0,
            "market_stackr_tx": 0, "transfers": 0, "admin_tx": 0,
            "other_usd": 0.0, "other_tx": 0,
            "outlier_usd": 0.0, "outlier_tx": 0}


def aggregate(items: List[Dict], daily: Dict[str, Dict],
              pseudos: Dict[str, str], types: Dict = None,
              top: List = None, out: List = None) -> None:
    """Ventile une page dans {jour PT -> compteurs} et recolte les pseudos.

    Seules les transactions COMPLETE comptent dans les revenus (les PENDING
    seront recomptees au run suivant : la fenetre de 3 jours est REJOUEE)."""
    for it in items:
        day = _pt(it.get("created_at"))
        if not day:
            continue
        d = daily.setdefault(day, _blank())
        typ = str(it.get("veve_type") or "")
        if types is not None:
            # inventaire de TOUS les types vus (+ montant) : garde-fou contre
            # un type de vente qu'on ignorerait (enchere, panier gem, etc.)
            t = types.setdefault(typ or "(vide)", [0, 0.0])
            t[0] += 1
            t[1] += _f(it.get("price"))
        price = _f(it.get("price"))
        done = str(it.get("status") or "") == "COMPLETE"
        if MAX_PRICE and price > MAX_PRICE and typ != TRANSFER:
            # prix aberrant : hors des revenus, mais compte et trace
            d["outlier_usd"] += price
            d["outlier_tx"] += 1
            if out is not None:
                out.append((price, day, typ, str(it.get("name") or ""),
                            it.get("nft_issue")))
            continue
        if typ == TRANSFER:
            d["transfers"] += 1
        elif typ in ADMIN_TYPES:
            d["admin_tx"] += 1
        elif typ in DROP_TYPES and done:
            d["drop_usd"] += price
            d["drop_tx"] += 1
        elif typ in MKT_VEVE_TYPES and done:
            d["market_veve_usd"] += price
            d["market_veve_tx"] += 1
        elif typ == MKT_STACKR and done:
            d["market_stackr_usd"] += price
            d["market_stackr_tx"] += 1
        elif typ in OTHER_TYPES and done:
            d["other_usd"] += price
            d["other_tx"] += 1
        if done and price >= BIG_TX and typ != TRANSFER and top is not None:
            top.append((price, day, typ, str(it.get("name") or ""),
                        it.get("nft_issue")))
        if WITH_PSEUDOS:
            for who in ("buyer", "seller"):
                u = it.get(f"{who}_username")
                a = str(it.get(f"{who}_address") or "").strip().lower()
                if u and a and a not in SYSTEM_ADDRS:
                    pseudos[a] = str(u)


def walk(days: int = DAYS, until: str = "", max_pages: int = MAX_PAGES,
         session=None, state: Dict = None, flush=None):
    """Descend le flux page par page (cursor = numero de page).

    Quotidien : s'arrete des qu'on passe sous la fenetre de `days` jours.
    Backfill  : descend jusqu'a `until`, REPREND l'etat du run precedent
                (resume_day + offset) et sauvegarde le sien a la fin.

    Robustesse (leçon du run #1) : sur echec repete, on reduit la taille de
    page au MEME endroit (100 -> 50 -> 25 -> 10) ; si meme 10 ne passe pas, on
    S'ARRETE EN GARDANT tout ce qui a ete recolte (jamais de perte).
    """
    state = state if state is not None else {}
    stop = until
    if not stop:
        d0 = _dt.date.fromisoformat(_today_pt()) - _dt.timedelta(days=days - 1)
        stop = d0.isoformat()
    resume_day = str(state.get("resume_day") or "")   # deja ecrit -> a sauter
    offset = int(state.get("offset") or 0)
    if resume_day:
        print(f"  reprise : on saute tout ce qui est >= {resume_day} "
              f"(deja ecrit), depart offset ~{offset}.", flush=True)

    daily: Dict[str, Dict] = {}
    day_offset: Dict[str, int] = {}       # ou commence chaque jour (reprise)
    pseudos: Dict[str, str] = {}
    types: Dict[str, list] = {}
    top: List = []
    outliers: List = []
    seen: set = set()
    s = session or requests.Session()
    limit = LIMIT
    pages = dupes = kept = skipped = 0
    oldest = ""
    incomplet = False

    for _ in range(max_pages):
        cursor = offset // limit + 1
        items = fetch_page(cursor, s, limit)
        if items is None:                       # echec obstine
            nxt = [x for x in LIMIT_STEPS if x < limit]
            if nxt:
                limit = nxt[0]
                offset = (offset // limit) * limit   # on reste au meme endroit
                print(f"  -> on reduit la page a {limit} et on repart de "
                      f"l'offset {offset}.", flush=True)
                time.sleep(COOLDOWN)
                continue
            print("  API obstinement en erreur : on GARDE la recolte et on "
                  "s'arrete proprement (l'etat permettra de reprendre).",
                  flush=True)
            incomplet = True
            break
        pages += 1
        if not items:
            print(f"  page {cursor} vide -> genese atteinte.", flush=True)
            state["done"] = True
            break
        fresh = []
        for it in items:
            vid = str(it.get("veve_id") or "")
            if vid and vid in seen:
                dupes += 1
                continue
            if vid:
                seen.add(vid)
            day = _pt(it.get("created_at"))
            if resume_day and day >= resume_day:
                skipped += 1               # deja ecrit par un run precedent
                continue
            if day and day not in day_offset:
                day_offset[day] = offset
            fresh.append(it)
        aggregate(fresh, daily, pseudos, types, top, outliers)
        kept += len(fresh)
        offset += len(items)
        oldest = _pt(items[-1].get("created_at"))
        if pages % 25 == 0 or pages <= 3:
            print(f"  page {cursor} : {kept} tx, jusqu'au {oldest}", flush=True)
        # SAUVEGARDE INTERMEDIAIRE : les jours COMPLETS (tous sauf le plus
        # ancien, forcement partiel) sont ecrits et l'etat avance. Si le run est
        # annule juste apres, rien n'est perdu.
        if flush and pages % FLUSH_PAGES == 0 and len(daily) > 1:
            complets = {d: v for d, v in daily.items() if d != min(daily)}
            plus_vieux = min(daily)
            state["resume_day"] = plus_vieux
            state["offset"] = day_offset.get(plus_vieux, offset)
            state.setdefault("done", False)
            try:
                n = flush(complets, dict(pseudos), state)
                print(f"    ... sauvegarde intermediaire : {len(complets)} "
                      f"jour(s) ecrits ({n}), reprise au {plus_vieux}.",
                      flush=True)
                for d in complets:
                    daily.pop(d, None)
                    day_offset.pop(d, None)
                pseudos.clear()
            except Exception as e:
                print(f"    ... sauvegarde intermediaire KO ({e}) — on "
                      f"continue, rien n'est perdu.", flush=True)
        if oldest and oldest < stop:
            break
        time.sleep(PAUSE)

    # le jour le plus ancien atteint est PARTIEL (on s'est arrete au milieu) :
    # on ne l'ecrit pas, le prochain run le reprendra depuis son debut.
    if oldest and oldest in daily and (oldest < stop or incomplet):
        daily.pop(oldest, None)
        day_offset.pop(oldest, None)

    if until:                                  # mode BACKFILL : etat de reprise
        if daily:
            plus_vieux = min(daily)
            state["resume_day"] = plus_vieux
            state["offset"] = day_offset.get(plus_vieux, offset)
        state.setdefault("done", False)
        if not incomplet and oldest and until and oldest <= until:
            state["done"] = True

    known = (set(DROP_TYPES) | set(MKT_VEVE_TYPES) | set(ADMIN_TYPES)
             | set(OTHER_TYPES) | {MKT_STACKR, TRANSFER})
    print("  types rencontres (tx / somme des prix) :", flush=True)
    for t, (n, tot) in sorted(types.items(), key=lambda x: -x[1][0]):
        flag = "" if t in known else "   <-- TYPE INCONNU (non compte !)"
        print(f"    {t:32s} {n:6d}   {tot:12.2f} ${flag}", flush=True)
    if outliers:
        outliers.sort(reverse=True)
        print(f"  PRIX ABERRANTS (> {MAX_PRICE:.0f} $) — exclus des revenus, "
              f"comptes dans outlier_usd :", flush=True)
        for pr, day, typ, name, iss in outliers[:10]:
            print(f"    {day}  {pr:12.2f} $  {typ:15s} {name} #{iss}",
                  flush=True)
    if top:
        top.sort(reverse=True)
        print(f"  plus grosses transactions (>= {BIG_TX:.0f} $) :", flush=True)
        for pr, day, typ, name, iss in top[:8]:
            print(f"    {day}  {pr:10.2f} $  {typ:15s} {name} #{iss}",
                  flush=True)
    inconnus = {t: v[0] for t, v in types.items() if t not in known}
    stats = {"pages": pages, "tx": kept, "doublons": dupes,
             "jusqu_au": oldest, "jours": len(daily), "pseudos": len(pseudos),
             "aberrants": len(outliers), "limit_final": limit}
    if skipped:
        stats["deja_faits"] = skipped
    if incomplet:
        stats["INCOMPLET"] = "relancer le workflow pour continuer"
    if inconnus:
        stats["TYPES_INCONNUS"] = inconnus
    return daily, pseudos, stats


# ---------------------------------------------------------------------------
# Ecritures
# ---------------------------------------------------------------------------

def _rows(daily: Dict[str, Dict]) -> List[List]:
    out = []
    for d in sorted(daily):
        v = daily[d]
        out.append([d, round(v["drop_usd"], 2), v["drop_tx"],
                    round(v["market_veve_usd"], 2), v["market_veve_tx"],
                    round(v["market_stackr_usd"], 2), v["market_stackr_tx"],
                    v["transfers"], v["admin_tx"],
                    round(v["other_usd"], 2), v["other_tx"],
                    round(v["outlier_usd"], 2), v["outlier_tx"]])
    return out


def _read_csv() -> Dict[str, List]:
    out: Dict[str, List] = {}
    try:
        with open(CSV_PATH, encoding="utf-8") as f:
            for r in csv.reader(f):
                if r and r[0] != "date":
                    out[r[0]] = r
    except FileNotFoundError:
        pass
    return out


def save_csv(rows: List[List]) -> int:
    """Upsert par jour (les jours rejoues ECRASENT les anciens)."""
    keep = _read_csv()
    for r in rows:
        keep[str(r[0])] = [str(x) for x in r]
    os.makedirs(os.path.dirname(CSV_PATH) or ".", exist_ok=True)
    with open(CSV_PATH, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(REV_HEADER)
        for d in sorted(keep):
            w.writerow(keep[d])
    return len(keep)


def fetch_remote(url: str = REMOTE_CSV) -> List[List]:
    """Les jours recoltes par le BACKFILL de l'autre repo (CSV public brut)."""
    if not url:
        return []
    try:
        r = requests.get(url, timeout=60)
        r.raise_for_status()
        out = []
        for row in csv.reader(r.text.splitlines()):
            if row and row[0] != "date":
                out.append(row)
        print(f"  backfill distant : {len(out)} jour(s) repris de {url}",
              flush=True)
        return out
    except Exception as e:
        print(f"  backfill distant indisponible ({e}) — sans consequence.",
              flush=True)
        return []


def write_tab(sh, rows: List[List]) -> int:
    """Onglet cache _VeveRevenue : upsert par date, nombres RAW natifs."""
    ws = _open_worksheet(sh, REV_TAB, cols=len(REV_HEADER))
    keep: Dict[str, List] = {}
    try:
        from gspread.utils import ValueRenderOption
        vals = ws.get_all_values(
            value_render_option=ValueRenderOption.unformatted)
    except Exception:
        vals = ws.get_all_values()
    for r in vals[1:] if vals else []:
        if r and str(r[0]).strip() and str(r[0]) != "date":
            keep[str(r[0]).strip()] = r
    for r in rows:
        keep[str(r[0])] = r
    ws.clear()
    ws.update(range_name="A1",
              values=[list(REV_HEADER)] + [keep[d] for d in sorted(keep)],
              value_input_option="RAW")
    try:
        ws.hide()
    except Exception:
        pass
    return len(keep)


def merge_pseudos(sh, pairs: Dict[str, str]) -> Dict[str, int]:
    """Fusionne les paires wallet->pseudo PUBLIQUES dans 🟣C-PSEUDOS.

    Remplace progressivement les lookups StackR sous cookie : ici la source est
    publique. On ne DETRUIT jamais une ligne existante — on complete le wallet
    manquant et on ajoute les pseudos inconnus."""
    if not pairs:
        return {"nouveaux": 0, "wallets_completes": 0}
    try:
        from scraper.stackr import PSEUDOS_TAB, PSEUDOS_HEADER
    except Exception:
        PSEUDOS_TAB, PSEUDOS_HEADER = "🟣C-PSEUDOS", ["username", "wallet_imx"]
    ws = _open_worksheet(sh, PSEUDOS_TAB, cols=len(PSEUDOS_HEADER))
    vals = ws.get_all_values()
    if not vals:
        vals = [list(PSEUDOS_HEADER)]
    head = vals[0]
    i_user = head.index("username") if "username" in head else 0
    i_wal = head.index("wallet_imx") if "wallet_imx" in head else 1
    i_src = head.index("source") if "source" in head else None
    i_fs = head.index("first_seen") if "first_seen" in head else None
    by_user = {}
    for r in vals[1:]:
        if r and len(r) > i_user and str(r[i_user]).strip():
            by_user[str(r[i_user]).strip().lower()] = r
    today = _dt.date.today().isoformat()
    added = filled = 0
    for addr, user in pairs.items():
        row = by_user.get(user.lower())
        if row is None:
            row = [""] * len(head)
            row[i_user] = user
            row[i_wal] = addr
            if i_src is not None:
                row[i_src] = "veve_tx"
            if i_fs is not None:
                row[i_fs] = today
            vals.append(row)
            by_user[user.lower()] = row
            added += 1
        else:
            while len(row) < len(head):
                row.append("")
            if not str(row[i_wal]).strip():
                row[i_wal] = addr
                filled += 1
    ws.clear()
    ws.update(range_name="A1", values=vals, value_input_option="RAW")
    return {"nouveaux": added, "wallets_completes": filled}


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> int:
    t0 = time.time()
    sheet_id = os.environ.get("SHEET_ID", "").strip()
    if not sheet_id:
        print("SHEET_ID env var is not set.", file=sys.stderr)
        return 2
    mode = "BACKFILL" if BACKFILL else f"quotidien ({DAYS} j)"
    print(f"Flux VeVe public — mode {mode}, limit={LIMIT}, "
          f"max_pages={MAX_PAGES}", flush=True)
    state = load_state() if BACKFILL else {}
    if BACKFILL and state.get("done"):
        print(f"Backfill deja termine (jusqu'au {state.get('resume_day')}). "
              f"Supprimer {STATE_PATH} pour repartir de zero.", flush=True)
        return 0
    sh_cache = {}

    def _flush(jours: Dict[str, Dict], ps: Dict[str, str], st: Dict) -> int:
        """Ecrit les jours complets + l'etat, en cours de route."""
        rows_ = _rows(jours)
        n = save_csv(rows_)
        if "sh" not in sh_cache:
            sh_cache["sh"] = _client().open_by_key(sheet_id)
        write_tab(sh_cache["sh"], rows_)
        if WITH_PSEUDOS and ps:
            merge_pseudos(sh_cache["sh"], ps)
        save_state(st)
        return n

    try:
        daily, pseudos, stats = walk(DAYS, UNTIL if BACKFILL else "",
                                     state=state,
                                     flush=_flush if BACKFILL else None)
    except Exception as e:
        # on ne perd JAMAIS 50 minutes de recolte sur une erreur reseau :
        # walk() garde sa recolte ; ici on ne tombe que sur l'imprevisible.
        print(f"veve_tx FAILED: {e}", file=sys.stderr)
        try:
            append_log(sheet_id, "veve_tx", "FAILED", str(e)[:200])
        except Exception:
            pass
        return 1
    rows = _rows(daily)
    summary: Dict[str, Any] = dict(stats)
    # MODE CSV SEUL (repo sans Sheet, typiquement le backfill sur paolo)
    if not SHEETS_OK or not sheet_id:
        if rows:
            summary["csv_jours"] = save_csv(rows)
        if BACKFILL:
            save_state(state)
            summary["etat"] = ("TERMINE" if state.get("done") else
                               f"reprise au {state.get('resume_day')} "
                               f"(offset {state.get('offset')})")
        summary["mode"] = "CSV seul (pas de Sheet sur ce repo)"
        summary["duration"] = f"{time.time() - t0:.0f}s"
        print(f"veve_tx : {summary}", flush=True)
        return 0
    if BACKFILL:
        save_state(state)
        summary["etat"] = (f"reprise au {state.get('resume_day')} "
                           f"(offset {state.get('offset')})"
                           if not state.get("done") else "TERMINE")
    if rows or not BACKFILL:
        # le quotidien reprend aussi les jours du backfill distant (paolo) :
        # l'historique de l'onglet se remplit tout seul, sans secrets partages.
        distants = [] if BACKFILL else fetch_remote()
        if rows:
            summary["csv_jours"] = save_csv(rows)
        sh = _client().open_by_key(sheet_id)
        summary["tab_jours"] = write_tab(sh, rows + distants)
        if distants:
            summary["jours_backfill"] = len(distants)
        if WITH_PSEUDOS and pseudos:
            summary.update(merge_pseudos(sh, pseudos))
        # apercu (verification humaine dans les logs)
        for r in rows[-3:]:
            print(f"  {r[0]} : drop {r[1]} $ ({r[2]} tx) · market VeVe {r[3]} $ "
                  f"({r[4]}) · market StackR {r[5]} $ ({r[6]})", flush=True)
    summary["duration"] = f"{time.time() - t0:.0f}s"
    try:
        append_log(sheet_id, "veve_tx", "OK",
                   "; ".join(f"{k}={v}" for k, v in summary.items()))
    except Exception as e:
        print(f"log warning: {e}", flush=True)
    print(f"veve_tx : {summary}", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())

# FIN veve_tx.py v7 (Sheet optionnel + reprise du backfill distant)
