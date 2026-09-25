# /// script
# requires-python = ">=3.11"
# dependencies = ["duckdb==1.5.5", "pyarrow", "pyyaml"]
# ///
"""Benchmark de requêtes DuckDB sur des fichiers (Geo)Parquet, distants ou locaux."""

import argparse
import json
import os
import platform
import re
import statistics
import sys
import threading
import time
from collections import defaultdict
from itertools import product
from pathlib import Path

import duckdb
import pyarrow  # noqa: F401  keeps the import out of the timings (noqa: F401 disables ruff warning)
import yaml

BBOX = ("xmin", "ymin", "xmax", "ymax")
GEOARROW = {"point", "linestring", "polygon", "multipoint", "multilinestring", "multipolygon"}
METRICS = ["planner", "system_peak_buffer_memory", "cumulative_row_groups_scanned", "cumulative_total_row_groups_to_scan"]
PLACEHOLDER = re.compile(r"\$(src|geom|bbox)\b")
DEFAULTS = {"threads": [os.cpu_count()], "runs": 3, "warm": True, "timeout": 600, "init": []}
MAX_QUERIES = 5

ENV_SQL = """
SELECT version() AS version,
       (SELECT string_agg(extension_name || ' ' || extension_version, ' · ' ORDER BY extension_name)
          FROM duckdb_extensions() WHERE loaded AND install_mode <> 'STATICALLY_LINKED') AS extensions,
       current_setting('http_proxy') AS proxy"""

FILE_SQL = """
WITH geo AS (SELECT decode(value) AS geo FROM parquet_kv_metadata($url) WHERE key = 'geo')
SELECT f.file_size_bytes AS size, f.footer_size, f.created_by, f.format_version, f.num_rows, m.*, (FROM geo) AS geo,
       (SELECT logical_type LIKE 'Geo%' FROM parquet_schema($url)
         WHERE name = (SELECT geo ->> 'primary_column' FROM geo)) AS native
FROM parquet_file_metadata($url) f, (
    SELECT string_agg(DISTINCT compression, ', ') AS codecs,
           sum(total_uncompressed_size) / sum(total_compressed_size) AS ratio,
           count(bloom_filter_offset) AS blooms,
           count(DISTINCT row_group_id) FILTER (geo_bbox IS NOT NULL) AS geo_stats
    FROM parquet_metadata($url)) m"""

COLUMNS_SQL = """
SELECT split_part(path_in_schema, ', ', 1) AS name,
       sum(total_compressed_size) / sum(sum(total_compressed_size)) OVER () AS share
FROM parquet_metadata($url) GROUP BY name ORDER BY share DESC LIMIT 3"""

# Row-group extents come from the covering column statistics, or else from native GEOMETRY statistics.
ROWGROUPS_SQL = """
WITH s AS (
    SELECT *, TRY_CAST(coalesce(stats_min_value, stats_min) AS DOUBLE) AS lo,
              TRY_CAST(coalesce(stats_max_value, stats_max) AS DOUBLE) AS hi
    FROM parquet_metadata($url)
), rg AS (
    SELECT row_group_id, any_value(row_group_num_rows) AS nrows, sum(total_compressed_size) AS bytes,
           coalesce(min(lo) FILTER (path_in_schema = $xmin), min(geo_bbox.xmin)) AS xmin,
           coalesce(min(lo) FILTER (path_in_schema = $ymin), min(geo_bbox.ymin)) AS ymin,
           coalesce(max(hi) FILTER (path_in_schema = $xmax), max(geo_bbox.xmax)) AS xmax,
           coalesce(max(hi) FILTER (path_in_schema = $ymax), max(geo_bbox.ymax)) AS ymax
    FROM s GROUP BY row_group_id
), b AS (SELECT *, ST_MakeEnvelope(xmin, ymin, xmax, ymax) AS env FROM rg)
SELECT count(*) AS n, min(nrows) AS rows_min, median(nrows) AS rows_med, max(nrows) AS rows_max,
       min(bytes) AS bytes_min, median(bytes) AS bytes_med, max(bytes) AS bytes_max,
       count(env) AS with_stats, median(xmax - xmin) AS width, median(ymax - ymin) AS height,
       sum(ST_Area(env)) / nullif(ST_Area(ST_Union_Agg(env)), 0) AS overlap
FROM b"""

# The log timestamp marks the end of a request to the µs; start_time + duration_ms is rounded to the ms.
IO_SQL = """
WITH http AS (
    SELECT request.type AS method, request.duration_ms AS ms, response.status AS status, request.headers['Range'] AS range,
           request.start_time AS t0, timestamp AS t1
    FROM duckdb_logs_parsed('HTTP')
), get AS (FROM http WHERE method = 'GET')
SELECT (SELECT count(*) FROM http WHERE method = 'HEAD') AS heads,
       count(*) AS gets,
       median(ms) AS ms_p50, quantile_cont(ms, 0.95) AS ms_p95, max(ms) AS ms_max,
       count(*) FILTER (status = 'INVALID') AS timeouts,
       count(*) FILTER (status NOT IN ('OK_200', 'PartialContent_206', 'INVALID')) AS errors,
       count(range) - count(DISTINCT range) AS repeats,
       (SELECT max(n) FROM (SELECT count(*) OVER (ORDER BY t0 RANGE INTERVAL 1 SECOND PRECEDING) AS n FROM get)) AS rps,
       (SELECT max((SELECT count(*) FROM get b WHERE b.t0 <= a.t0 AND a.t0 < b.t1)) FROM get a) AS concurrency,
       (SELECT coalesce(sum(bytes), 0) FROM duckdb_logs_parsed('FileSystem') WHERE op = 'READ') AS read_bytes
FROM get"""


def literal(s):
    return "'" + s.replace("'", "''") + "'"


def ident(s):
    return '"' + s.replace('"', '""') + '"'


def one(con, sql, **params):
    cur = con.execute(sql, params)
    return dict(zip([d[0] for d in cur.description], cur.fetchone()))


def render(query, f):
    return PLACEHOLDER.sub(lambda m: f[m[1]], query)


def num(value, spec=",.0f"):
    return f"{value:{spec}}".replace(",", " ")


def mib(b):
    return f"{num(b / 2**20, ',.1f')} Mio"


def connect(threads, init, geoarrow=False):
    con = duckdb.connect(config={"threads": threads})
    con.execute("INSTALL httpfs; LOAD httpfs; INSTALL spatial; LOAD spatial")
    if geoarrow:
        con.execute("INSTALL duck_geoarrow FROM community; LOAD duck_geoarrow")
    for sql in init:
        con.execute(sql)
    con.execute(f"SET custom_profiling_settings = '{json.dumps(dict.fromkeys(METRICS, 'true'))}'")
    con.execute("PRAGMA enable_profiling = 'no_output'")
    con.execute("CALL enable_logging(['HTTP', 'FileSystem'], storage = 'memory', storage_buffer_size = 0)")
    return con


def inspect(url, init):
    con = connect(os.cpu_count(), init)
    con.execute("SET parquet_metadata_cache = true")  # not measured: reuses the footer across the metadata queries
    f = one(con, FILE_SQL, url=url)
    f["geo"] = json.loads(f["geo"] or "{}")
    f["column"] = f["geo"].get("primary_column")
    f["meta"] = f["geo"].get("columns", {}).get(f["column"], {})
    f["geoarrow"] = f["meta"].get("encoding") in GEOARROW
    f["src"] = f"read_parquet({literal(url)})"
    f["geom"] = f["column"] and geometry(con, f)
    cover = f["meta"].get("covering", {}).get("bbox")
    f["bbox"] = cover and ident(cover["xmin"][0])
    f |= one(con, ROWGROUPS_SQL, url=url, **{k: cover and ", ".join(cover[k]) for k in BBOX})
    f["columns"] = con.execute(COLUMNS_SQL, {"url": url}).fetchall()
    return f


def geometry(con, f):
    col = ident(f["column"])
    if f["geoarrow"]:
        return f"ST_GeomFromGeoArrow('{f['meta']['encoding']}', {col})"
    return col if con.sql(f"SELECT {col} FROM {f['src']}").types[0].id == "geometry" else f"ST_GeomFromWKB({col})"


def measure(con, sql, timeout):
    con.execute("CALL truncate_duckdb_logs()")
    timer = threading.Timer(timeout, con.interrupt)
    timer.start()
    start = time.perf_counter()
    try:
        rows = con.execute(sql).to_arrow_table().num_rows
    except duckdb.InterruptException:
        # in-flight HTTP requests are not aborted, so the query can outlive the timeout
        return {"error": f"timeout après {time.perf_counter() - start:.0f} s"}
    except duckdb.Error as e:
        return {"error": str(e).splitlines()[0]}
    finally:
        timer.cancel()
    elapsed = time.perf_counter() - start
    # {"result": "error"} when DuckDB answers from metadata alone, e.g. count(*) or min/max
    profile = json.loads(con.get_profiling_information(format="json"))
    return {"time": elapsed, "rows": rows, **{k: profile.get(k) for k in METRICS}, **one(con, IO_SQL)}


def crs(meta):
    if meta:
        return "{authority}:{code}".format(**meta["crs"]["id"]) if "crs" in meta else "OGC:CRS84 (défaut)"


def cell(fn, *args):
    try:
        value = fn(*args)
    except (KeyError, TypeError, ValueError, ZeroDivisionError):
        value = None
    return "—" if value is None else str(value)


STRUCTURE = [
    ("Taille", lambda f: f"{mib(f['size'])} · footer {mib(f['footer_size'])} · {num(f['num_rows'])} lignes"),
    ("Écrit par", lambda f: f["created_by"]),
    ("Parquet", lambda f: f"v{f['format_version']} · {f['codecs']} · compression ×{f['ratio']:.2f}"),
    ("GeoParquet", lambda f: f["geo"]["version"]),
    ("Géométrie", lambda f: f"{f['column']} · {f['meta']['encoding']}" + (" · GEOMETRY natif" if f["native"] else "")),
    ("Types", lambda f: ", ".join(f["meta"]["geometry_types"]) or "non déclarés"),
    ("CRS", lambda f: crs(f["meta"])),
    ("`$geom`", lambda f: f["geom"] and f"`{f['geom']}`"),
    ("`$bbox`", lambda f: f["bbox"] and f"`{f['bbox']}`"),
    ("Row groups", lambda f: f"{f['n']} · {num(f['rows_min'])} / {num(f['rows_med'])} / {num(f['rows_max'])} lignes"),
    ("Taille RG", lambda f: " / ".join(mib(f[k]) for k in ("bytes_min", "bytes_med", "bytes_max"))),
    ("Stats bbox par RG", lambda f: f"{f['with_stats']}/{f['n']} · natives {f['geo_stats']}"),
    ("Bloom filters", lambda f: f"{num(f['blooms'])} chunks"),
    ("Emprise RG médiane", lambda f: f"{f['width']:.4g} × {f['height']:.4g}"),
    ("Recouvrement RG", lambda f: f"{f['overlap']:.2f}"),
    ("Colonnes lourdes", lambda f: " · ".join(f"{name} {share:.0%}" for name, share in f["columns"])),
]

PERF = [
    ("temps", lambda m, f: f"**{m['time']:.2f} s**" if m.get("best") else f"{m['time']:.2f} s"),
    ("min–max", lambda m, f: f"{m['min']:.2f}–{m['max']:.2f}"),
    ("lignes", lambda m, f: num(m["rows"])),
    ("RG lus", lambda m, f: f"{m['cumulative_row_groups_scanned']:.0f}/{m['cumulative_total_row_groups_to_scan']:.0f}"),
    ("lu", lambda m, f: mib(m["read_bytes"])),
    ("% fichier", lambda m, f: f"{m['read_bytes'] / f['size']:.1%}"),
    ("débit", lambda m, f: f"{m['read_bytes'] / 2**20 / m['time']:.1f} Mio/s"),
    ("planif.", lambda m, f: f"{m['planner']:.2f} s"),
    ("mém. pic", lambda m, f: mib(m["system_peak_buffer_memory"])),
    ("accél.", lambda m, f: f"×{m['base'] / m['time']:.2f}"),
]

NETWORK = [(h, lambda m, f, k=k: num(m[k])) for h, k in [
    ("HEAD", "heads"), ("GET", "gets"), ("p50", "ms_p50"), ("p95", "ms_p95"), ("max", "ms_max"), ("req/s max", "rps"),
    ("simult. max", "concurrency"), ("timeouts", "timeouts"), ("erreurs", "errors"), ("répétées", "repeats")]]


def table(header, rows, align="--:"):
    rows = [header, ["---"] + [align] * (len(header) - 1), *rows]
    return "\n".join("| " + " | ".join(str(c).replace("|", "\\|") for c in row) + " |" for row in rows)


def report(path, title, env, cfg, sources, results):
    stats = {}
    for key, rs in results.items():
        if rs := [r for r in rs if "time" in r]:
            times = [r["time"] for r in rs]
            m = {k: statistics.median(v) for k in rs[0] if (v := [r[k] for r in rs if r[k] is not None])}
            stats[key] = m | {"min": min(times), "max": max(times)}
    for (query, source, _, kind), m in stats.items():
        m["base"] = stats.get((query, source, cfg["threads"][0], kind), {}).get("time")
    for query, kind in {(q, k) for q, _, _, k in stats}:
        min((m for (q, _, _, k), m in stats.items() if (q, k) == (query, kind)), key=lambda m: m["time"])["best"] = True
    settings = (f"threads {', '.join(map(str, cfg['threads']))} · {cfg['runs']} run(s) · "
                f"{'froid + chaud' if cfg['warm'] else 'froid'} · timeout {cfg['timeout']} s")
    md = [f"# {title}", env, settings, "## Sources", table(["", "URL"], cfg["sources"].items(), "---")]
    if cfg["init"]:
        md += ["## init", "```sql\n" + "\n".join(cfg["init"]) + "\n```"]
    md += ["## Requêtes", *(f"### {name}\n\n```sql\n{query}\n```" for name, query in cfg["queries"].items())]
    md += ["## Structure", table([""] + list(sources), [[name] + [cell(fn, f) for f in sources.values()] for name, fn in STRUCTURE], "---")]
    for heading, columns in (("Performances (médianes)", PERF), ("Réseau (médianes, durées en ms)", NETWORK)):
        md.append(f"## {heading}")
        for query in cfg["queries"]:
            if rows := [[source, threads, kind] + [cell(fn, m, sources[source]) for _, fn in columns]
                        for (q, source, threads, kind), m in stats.items() if q == query]:
                md += [f"### {query}", table(["source", "threads", "run"] + [h for h, _ in columns], rows)]
    if errors := sorted({(*key, r["error"]) for key, rs in results.items() for r in rs if "error" in r}):
        md += ["## Erreurs", "\n".join("- {} · {} · {} threads · {} : {}".format(*e) for e in errors)]
    path.write_text("\n\n".join(md) + "\n")
    print(f"\nRapport : {path}")


def load(path):
    raw = yaml.safe_load(path.read_text()) or {}
    if unknown := raw.keys() - DEFAULTS.keys() - {"sources", "queries"}:
        sys.exit(f"{path} : clé(s) inconnue(s) : {', '.join(sorted(map(str, unknown)))}")
    cfg = DEFAULTS | {k: v for k, v in raw.items() if v is not None}
    sources, queries = cfg.get("sources"), cfg.get("queries")
    if not (isinstance(sources, dict) and sources and isinstance(queries, dict) and 0 < len(queries) <= MAX_QUERIES):
        sys.exit(f"{path} : 'sources' (nom: url) et 'queries' (nom: requête, {MAX_QUERIES} au plus) sont requis")
    if missing := [str(name) for name, query in queries.items() if "src" not in PLACEHOLDER.findall(query or "")]:
        sys.exit(f"{path} : requête(s) sans $src : {', '.join(missing)}")
    # YAML reads names such as 2025 or 2026-06-15 as numbers or dates; local paths are relative to the config file
    cfg["sources"] = {str(name): url if "://" in str(url) else str(path.parent / str(url)) for name, url in sources.items()}
    cfg["queries"] = {str(name): query.strip() for name, query in queries.items()}
    for key in ("threads", "init"):
        cfg[key] = cfg[key] if isinstance(cfg[key], list) else [cfg[key]]
    if not all(type(n) is int and n > 0 for n in [*cfg["threads"], cfg["runs"]]):
        sys.exit(f"{path} : 'threads' et 'runs' doivent être des entiers positifs")
    return cfg


def parse_args():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("config", type=Path, help="fichier YAML : sources, queries, threads, runs, warm, timeout, init")
    return p.parse_args()


def main():
    args = parse_args()
    path = args.config
    cfg, stamp = load(path), time.localtime()

    # Inspect each source URL and gather metadata using the `inspect` function
    sources, sw, qw = {}, max(map(len, cfg["sources"])), max(map(len, cfg["queries"]))
    for label, url in cfg["sources"].items():
        print(f"{label:{sw}}  {url}")
        try:
            sources[label] = inspect(url, cfg["init"])
        except duckdb.Error as e:
            print("  ✗", error := str(e).splitlines()[0])
            cfg["sources"][label] = f"{url} · ✗ {error}"
    if not sources:
        sys.exit("Aucune source lisible")

    # Get environment information
    env = one(connect(1, cfg["init"], any(f["geoarrow"] for f in sources.values())), ENV_SQL)
    env = (f"DuckDB {env['version']} · {env['extensions']} · Python {platform.python_version()} · "
           f"{platform.system()} {platform.machine()} · {os.cpu_count()} CPU" + (f" · proxy {env['proxy']}" if env["proxy"] else ""))
   
    results = defaultdict(list)
    try:
        for run, (name, query), (label, f), threads in product(
                range(1, cfg["runs"] + 1), cfg["queries"].items(), sources.items(), cfg["threads"]):
            if missing := [k for k in PLACEHOLDER.findall(query) if not f[k]]:
                results[name, label, threads, "froid"].append({"error": f"${missing[0]} indisponible pour cette source"})
                continue
            con = connect(threads, cfg["init"], f["geoarrow"])
            for kind in ("froid", "chaud")[: 1 + cfg["warm"]]:
                r = measure(con, render(query, f), cfg["timeout"])
                if kind == "chaud":
                    r.pop("system_peak_buffer_memory", None)  # never reset within a connection
                results[name, label, threads, kind].append(r)
                print(f"{run}/{cfg['runs']} {name:{qw}} {label:{sw}} {threads:>3} threads {kind:5}", r.get("error") or
                      f"{r['time']:8.2f} s · {num(r['rows'])} lignes · {r['gets']} GET · {mib(r['read_bytes'])}")
                if "error" in r:
                    break
            con.close()
    finally:
        report(path.with_name(f"{path.stem}-{time.strftime('%Y%m%d-%H%M%S', stamp)}.md"),
               f"Benchmark {path.stem} — {time.strftime('%Y-%m-%d %H:%M', stamp)}", env, cfg, sources, results)


if __name__ == "__main__":
    main()
