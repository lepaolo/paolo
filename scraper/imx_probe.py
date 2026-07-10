"""
Sonde IMX — verifie si le 14/12/2021 est bien la GENESE VeVe sur Immutable X.

Le scan s'est marque done=true en restant sur le 14/12/2021. On teste s'il
existe des transactions AVANT cette date : si oui, done=true est PREMATURE (il
reste a scanner) ; si tout est vide avant le 14/12, la genese est confirmee et
le scan est reellement complet.

N'ecrit rien.
"""

from __future__ import annotations

import datetime as _dt
import sys

import requests

API_URL = "https://qbolqfa7fnctxo3ooupoqrslem.appsync-api.us-east-2.amazonaws.com/graphql"
API_KEY = "da2-ceptv3udhzfmbpxr3eqisx3coe"
CONTRACT = "0xa7aefead2f25972d80516628417ac46b3f2604af"
QUERY = ("query L($address:String!,$pageSize:Int,$maxTime:Float){"
         "listTransactionsV2(address:$address,limit:$pageSize,maxTime:$maxTime){"
         "items{txn_time txn_id txn_type} nextToken}}")

HEAD = {"content-type": "application/json", "x-api-key": API_KEY,
        "origin": "https://immutascan.io", "referer": "https://immutascan.io/"}


def ms(y, m, d, h=23, mi=59):
    return int(_dt.datetime(y, m, d, h, mi, tzinfo=_dt.timezone.utc).timestamp() * 1000)


def probe(label, maxtime):
    body = {"operationName": "L", "query": QUERY,
            "variables": {"address": CONTRACT, "pageSize": 5, "maxTime": float(maxtime)}}
    try:
        r = requests.post(API_URL, json=body, headers=HEAD, timeout=30)
        j = r.json()
        items = ((j.get("data") or {}).get("listTransactionsV2") or {}).get("items") or []
        if items:
            dates = []
            for it in items:
                try:
                    t = int(it["txn_time"]) / 1000
                    dates.append(_dt.datetime.utcfromtimestamp(t).strftime("%Y-%m-%d %H:%M"))
                except Exception:
                    pass
            print(f"   {label} (maxTime<={_dt.datetime.utcfromtimestamp(maxtime/1000):%Y-%m-%d}) "
                  f": {len(items)} items — dates {dates}", flush=True)
        else:
            print(f"   {label} (maxTime<={_dt.datetime.utcfromtimestamp(maxtime/1000):%Y-%m-%d}) "
                  f": AUCUN item (rien a/avant cette date)", flush=True)
        return len(items)
    except Exception as e:
        print(f"   {label}: ERR {e}", flush=True)
        return -1


def main() -> int:
    print("Sonde genese IMX — y a-t-il des transactions AVANT le 14/12/2021 ?\n", flush=True)
    probe("REF 14/12/2021 (jour bloque)", ms(2021, 12, 14))
    probe("13/12/2021", ms(2021, 12, 13))
    probe("10/12/2021", ms(2021, 12, 10))
    probe("01/12/2021", ms(2021, 12, 1))
    probe("15/11/2021", ms(2021, 11, 15))
    probe("01/10/2021", ms(2021, 10, 1))
    print("\nSi '13/12' et avant = AUCUN item -> 14/12 = GENESE, scan COMPLET.",
          flush=True)
    print("Si des items apparaissent avant le 14/12 -> done premature, il reste a scanner.",
          flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
