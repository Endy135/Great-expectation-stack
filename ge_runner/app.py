"""
Great Expectations + PySpark Backend API
Sources supportées : Local, MinIO (S3), PostgreSQL
Credentials via variables d'environnement (.env)
"""

import os
import json
import traceback
from datetime import datetime
from flask import Flask, request, jsonify
from flask_cors import CORS

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from great_expectations.dataset import SparkDFDataset

# ─────────────────────────────────────────────────────────────────────────────
app = Flask(__name__)
CORS(app)

GE_ROOT     = "/app/ge_project"
RESULTS_DIR = "/app/ge_results"
os.makedirs(RESULTS_DIR, exist_ok=True)
os.makedirs(GE_ROOT, exist_ok=True)

SUPPORTED_EXT = {".csv", ".parquet", ".json", ".jsonl"}

# ─────────────────────────────────────────────────────────────────────────────
# DATASOURCE REGISTRY — état actif partagé
# ─────────────────────────────────────────────────────────────────────────────

_active_source = {
    "type":  "local",   # "local" | "minio" | "postgres"
    "file":  None,      # chemin/clé du fichier actif (local + minio)
    "table": None,      # table ou requête SQL active (postgres)
}

# ── Credentials depuis l'environnement ───────────────────────────────────────

def get_minio_config():
    return {
        "endpoint":   os.environ.get("MINIO_ENDPOINT",   "http://minio:9000"),
        "access_key": os.environ.get("MINIO_ACCESS_KEY", ""),
        "secret_key": os.environ.get("MINIO_SECRET_KEY", ""),
        "bucket":     os.environ.get("MINIO_BUCKET",     ""),
    }

def get_postgres_config():
    return {
        "host":     os.environ.get("PG_HOST",     "localhost"),
        "port":     os.environ.get("PG_PORT",     "5432"),
        "database": os.environ.get("PG_DATABASE", ""),
        "user":     os.environ.get("PG_USER",     ""),
        "password": os.environ.get("PG_PASSWORD", ""),
    }

# ─────────────────────────────────────────────────────────────────────────────
# SPARK SESSION (singleton résilient)
# ─────────────────────────────────────────────────────────────────────────────

_spark = None

def get_spark():
    global _spark
    if _spark is not None:
        try:
            _spark.sparkContext.statusTracker().getActiveStageIds()
        except Exception:
            _spark = None

    if _spark is None:
        cfg = get_minio_config()
        builder = (
            SparkSession.builder
            .master("local[2]")
            .appName("GE-Validator")
            .config("spark.sql.shuffle.partitions", "4")
            .config("spark.ui.enabled", "false")
            .config("spark.driver.memory", "1g")
            .config("spark.executor.memory", "512m")
            .config("spark.driver.maxResultSize", "256m")
            .config("spark.sql.execution.arrow.pyspark.enabled", "true")
            .config("spark.jars.packages",
                    "org.apache.hadoop:hadoop-aws:3.3.4,"
                    "com.amazonaws:aws-java-sdk-bundle:1.12.262,"
                    "org.postgresql:postgresql:42.6.0")
        )
        if cfg["access_key"]:
            builder = (
                builder
                .config("spark.hadoop.fs.s3a.endpoint",               cfg["endpoint"])
                .config("spark.hadoop.fs.s3a.access.key",             cfg["access_key"])
                .config("spark.hadoop.fs.s3a.secret.key",             cfg["secret_key"])
                .config("spark.hadoop.fs.s3a.path.style.access",      "true")
                .config("spark.hadoop.fs.s3a.impl",
                        "org.apache.hadoop.fs.s3a.S3AFileSystem")
                .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
            )
        _spark = builder.getOrCreate()
        _spark.sparkContext.setLogLevel("ERROR")
    return _spark

# ─────────────────────────────────────────────────────────────────────────────
# CONNECTEUR LOCAL
# ─────────────────────────────────────────────────────────────────────────────

DATA_DIR = "/data"

def local_list_files():
    result = []
    if not os.path.isdir(DATA_DIR):
        return result
    for name in sorted(os.listdir(DATA_DIR)):
        if os.path.splitext(name)[1].lower() not in SUPPORTED_EXT:
            continue
        full = os.path.join(DATA_DIR, name)
        stat = os.stat(full)
        result.append({
            "name":    name,
            "path":    full,
            "ext":     os.path.splitext(name)[1].lstrip(".").lower(),
            "size_kb": round(stat.st_size / 1024, 1),
            "active":  full == _active_source.get("file"),
        })
    return result

def local_read_df(path):
    spark = get_spark()
    ext = os.path.splitext(path)[1].lower()
    if ext == ".csv":
        return spark.read.csv(path, header=True, inferSchema=True)
    if ext == ".parquet":
        return spark.read.parquet(path)
    if ext in (".json", ".jsonl"):
        return spark.read.json(path)
    raise ValueError(f"Format non supporté : {ext}")

# ─────────────────────────────────────────────────────────────────────────────
# CONNECTEUR MINIO
# ─────────────────────────────────────────────────────────────────────────────

def _s3_client():
    import boto3
    from botocore.client import Config
    cfg = get_minio_config()
    return boto3.client(
        "s3",
        endpoint_url=cfg["endpoint"],
        aws_access_key_id=cfg["access_key"],
        aws_secret_access_key=cfg["secret_key"],
        config=Config(signature_version="s3v4"),
    )

def minio_list_buckets():
    resp = _s3_client().list_buckets()
    return [b["Name"] for b in resp.get("Buckets", [])]

def minio_list_files(bucket=None):
    cfg = get_minio_config()
    b   = bucket or cfg["bucket"]
    resp = _s3_client().list_objects_v2(Bucket=b)
    result = []
    for obj in resp.get("Contents", []):
        key = obj["Key"]
        ext = os.path.splitext(key)[1].lower()
        if ext not in SUPPORTED_EXT:
            continue
        s3_path = f"s3a://{b}/{key}"
        result.append({
            "name":    key,
            "path":    s3_path,
            "ext":     ext.lstrip("."),
            "size_kb": round(obj["Size"] / 1024, 1),
            "active":  _active_source.get("file") == s3_path,
            "bucket":  b,
        })
    return result

def minio_read_df(s3_path):
    spark = get_spark()
    ext   = os.path.splitext(s3_path.split("?")[0])[1].lower()
    if ext == ".csv":
        return spark.read.csv(s3_path, header=True, inferSchema=True)
    if ext == ".parquet":
        return spark.read.parquet(s3_path)
    if ext in (".json", ".jsonl"):
        return spark.read.json(s3_path)
    raise ValueError(f"Format non supporté : {ext}")

# ─────────────────────────────────────────────────────────────────────────────
# CONNECTEUR POSTGRESQL
# ─────────────────────────────────────────────────────────────────────────────

def _pg_options():
    cfg = get_postgres_config()
    url = f"jdbc:postgresql://{cfg['host']}:{cfg['port']}/{cfg['database']}"
    return url, cfg["user"], cfg["password"]

def pg_list_tables():
    spark = get_spark()
    url, user, pwd = _pg_options()
    df = spark.read.format("jdbc").options(
        url=url, user=user, password=pwd,
        dbtable="(SELECT table_name FROM information_schema.tables "
                "WHERE table_schema='public' ORDER BY table_name) t",
        driver="org.postgresql.Driver",
    ).load()
    return [r["table_name"] for r in df.collect()]

def pg_read_df(table_or_query):
    spark = get_spark()
    url, user, pwd = _pg_options()
    dbtable = (
        f"({table_or_query}) t"
        if table_or_query.strip().upper().startswith("SELECT")
        else table_or_query
    )
    return spark.read.format("jdbc").options(
        url=url, user=user, password=pwd,
        dbtable=dbtable,
        driver="org.postgresql.Driver",
    ).load()

# ─────────────────────────────────────────────────────────────────────────────
# DISPATCHER
# ─────────────────────────────────────────────────────────────────────────────

def read_active_df():
    src = _active_source["type"]
    if src == "local":
        path = _active_source.get("file")
        if not path:
            files = local_list_files()
            if not files:
                raise FileNotFoundError("Aucun fichier dans /data")
            path = files[0]["path"]
            _active_source["file"] = path
        return local_read_df(path)
    elif src == "minio":
        path = _active_source.get("file")
        if not path:
            raise FileNotFoundError("Aucun fichier MinIO sélectionné")
        return minio_read_df(path)
    elif src == "postgres":
        table = _active_source.get("table")
        if not table:
            raise ValueError("Aucune table PostgreSQL sélectionnée")
        return pg_read_df(table)
    raise ValueError(f"Source inconnue : {src}")

def active_label():
    src = _active_source["type"]
    if src == "postgres":
        return _active_source.get("table", "postgres")
    return os.path.basename(_active_source.get("file", "") or "")

# ─────────────────────────────────────────────────────────────────────────────
# EXPECTATION CATALOGUE
# ─────────────────────────────────────────────────────────────────────────────

EXPECTATION_CATALOGUE = {
    "expect_column_to_exist":                  {"label": "Column exists",             "params": ["column"],                        "description": "Vérifie que la colonne existe."},
    "expect_column_values_to_not_be_null":     {"label": "Pas de valeurs nulles",     "params": ["column"],                        "description": "Vérifie l'absence de NULL."},
    "expect_column_values_to_be_unique":       {"label": "Valeurs uniques",           "params": ["column"],                        "description": "Toutes les valeurs sont uniques."},
    "expect_column_values_to_be_between":      {"label": "Valeurs dans un intervalle","params": ["column", "min_value","max_value"],"description": "Valeurs numériques dans [min, max]."},
    "expect_column_values_to_match_regex":     {"label": "Regex match",               "params": ["column", "regex"],               "description": "Valeurs correspondant à une regex."},
    "expect_column_values_to_be_in_set":       {"label": "Valeurs dans un ensemble",  "params": ["column", "value_set"],           "description": "Valeurs dans un ensemble défini."},
    "expect_table_row_count_to_be_between":    {"label": "Nombre de lignes",          "params": ["min_value", "max_value"],        "description": "Nombre de lignes dans [min, max]."},
    "expect_column_mean_to_be_between":        {"label": "Moyenne dans un intervalle","params": ["column", "min_value","max_value"],"description": "Moyenne dans [min, max]."},
}

# ─────────────────────────────────────────────────────────────────────────────
# PERSISTANCE
# ─────────────────────────────────────────────────────────────────────────────

def load_suites():
    p = os.path.join(RESULTS_DIR, "suites.json")
    return json.load(open(p)) if os.path.exists(p) else {}

def save_suites(s):
    json.dump(s, open(os.path.join(RESULTS_DIR, "suites.json"), "w"), indent=2)

def load_results():
    p = os.path.join(RESULTS_DIR, "results.json")
    return json.load(open(p)) if os.path.exists(p) else []

def save_results(r):
    json.dump(r, open(os.path.join(RESULTS_DIR, "results.json"), "w"), indent=2)

def parse_param(key, val):
    if key in ("min_value", "max_value"):
        try:
            return float(val)
        except (ValueError, TypeError):
            return val
    if key == "value_set":
        return val if isinstance(val, list) else [v.strip() for v in str(val).split(",")]
    return val

# ─────────────────────────────────────────────────────────────────────────────
# ROUTES — HEALTH
# ─────────────────────────────────────────────────────────────────────────────

@app.route("/api/health", methods=["GET"])
def health():
    return jsonify({"status": "ok"})

# ─────────────────────────────────────────────────────────────────────────────
# ROUTES — SOURCE
# ─────────────────────────────────────────────────────────────────────────────

@app.route("/api/source", methods=["GET"])
def get_source():
    src  = _active_source["type"]
    info = {"type": src, "file": _active_source.get("file"), "table": _active_source.get("table")}
    if src == "minio":
        cfg = get_minio_config()
        info.update({"endpoint": cfg["endpoint"], "bucket": cfg["bucket"], "configured": bool(cfg["access_key"])})
    elif src == "postgres":
        cfg = get_postgres_config()
        info.update({"host": cfg["host"], "port": cfg["port"], "database": cfg["database"],
                     "user": cfg["user"], "configured": bool(cfg["database"] and cfg["user"])})
    return jsonify(info)

@app.route("/api/source/switch", methods=["POST"])
def switch_source():
    src = (request.json or {}).get("type", "").lower()
    if src not in ("local", "minio", "postgres"):
        return jsonify({"error": f"Source inconnue : {src}"}), 400
    _active_source.update({"type": src, "file": None, "table": None})
    return jsonify({"message": f"Source active : {src}", "type": src})

# ─────────────────────────────────────────────────────────────────────────────
# ROUTES — LOCAL
# ─────────────────────────────────────────────────────────────────────────────

@app.route("/api/data/files", methods=["GET"])
def data_files():
    return jsonify({"files": local_list_files(), "source": "local"})

@app.route("/api/data/select", methods=["POST"])
def data_select():
    name = (request.json or {}).get("name", "").strip()
    if not name:
        return jsonify({"error": "Paramètre 'name' manquant"}), 400
    full = os.path.join(DATA_DIR, name)
    if not os.path.exists(full):
        return jsonify({"error": f"Fichier introuvable : {name}"}), 404
    if os.path.splitext(name)[1].lower() not in SUPPORTED_EXT:
        return jsonify({"error": f"Format non supporté"}), 400
    _active_source.update({"type": "local", "file": full, "table": None})
    return jsonify({"message": f"Fichier actif : {name}", "path": full})

# ─────────────────────────────────────────────────────────────────────────────
# ROUTES — MINIO
# ─────────────────────────────────────────────────────────────────────────────

@app.route("/api/minio/buckets", methods=["GET"])
def route_minio_buckets():
    try:
        return jsonify({"buckets": minio_list_buckets()})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/minio/files", methods=["GET"])
def route_minio_files():
    bucket = request.args.get("bucket") or get_minio_config()["bucket"]
    try:
        return jsonify({"files": minio_list_files(bucket), "bucket": bucket})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/minio/select", methods=["POST"])
def route_minio_select():
    data   = request.json or {}
    bucket = data.get("bucket") or get_minio_config()["bucket"]
    key    = data.get("key", "").strip()
    if not key:
        return jsonify({"error": "Paramètre 'key' manquant"}), 400
    s3_path = f"s3a://{bucket}/{key}"
    _active_source.update({"type": "minio", "file": s3_path, "table": None})
    return jsonify({"message": f"Fichier MinIO actif : {key}", "path": s3_path})

# ─────────────────────────────────────────────────────────────────────────────
# ROUTES — POSTGRESQL
# ─────────────────────────────────────────────────────────────────────────────

@app.route("/api/postgres/tables", methods=["GET"])
def route_pg_tables():
    try:
        return jsonify({"tables": pg_list_tables()})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/postgres/select", methods=["POST"])
def route_pg_select():
    table = (request.json or {}).get("table", "").strip()
    if not table:
        return jsonify({"error": "Paramètre 'table' manquant"}), 400
    _active_source.update({"type": "postgres", "table": table, "file": None})
    return jsonify({"message": f"Table active : {table}", "table": table})

# ─────────────────────────────────────────────────────────────────────────────
# ROUTES — DATA (agnostique à la source)
# ─────────────────────────────────────────────────────────────────────────────

@app.route("/api/data/preview", methods=["GET"])
def data_preview():
    try:
        df   = read_active_df()
        rows = [row.asDict() for row in df.limit(10).collect()]
        return jsonify({
            "columns":    df.columns,
            "rows":       rows,
            "total_rows": df.count(),
            "source":     _active_source["type"],
            "label":      active_label(),
        })
    except Exception as e:
        return jsonify({"error": str(e), "trace": traceback.format_exc()}), 500

@app.route("/api/data/stats", methods=["GET"])
def data_stats():
    try:
        df    = read_active_df()
        total = df.count()
        stats = []
        for col in df.columns:
            dtype        = str(dict(df.dtypes)[col])
            null_count   = df.filter(F.col(col).isNull()).count()
            unique_count = df.select(col).distinct().count()
            stat = {"column": col, "type": dtype, "nulls": null_count,
                    "unique": unique_count, "total": total}
            if dtype in ("int", "double", "bigint", "float", "long"):
                agg = df.select(F.min(col).alias("min"), F.max(col).alias("max"),
                                F.avg(col).alias("mean")).collect()[0]
                stat.update({
                    "min":  float(agg["min"])  if agg["min"]  is not None else None,
                    "max":  float(agg["max"])  if agg["max"]  is not None else None,
                    "mean": float(agg["mean"]) if agg["mean"] is not None else None,
                })
            stats.append(stat)
        return jsonify(stats)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/data/columns", methods=["GET"])
def data_columns():
    try:
        df   = read_active_df()
        cols = [{"name": c, "type": str(dict(df.dtypes)[c])} for c in df.columns]
        return jsonify({"columns": cols, "label": active_label()})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# ─────────────────────────────────────────────────────────────────────────────
# ROUTES — EXPECTATIONS / SUITES / VALIDATION / RÉSULTATS
# ─────────────────────────────────────────────────────────────────────────────

@app.route("/api/expectations/catalogue", methods=["GET"])
def catalogue():
    return jsonify(EXPECTATION_CATALOGUE)

@app.route("/api/suites", methods=["GET"])
def list_suites():
    return jsonify(load_suites())

@app.route("/api/suites", methods=["POST"])
def create_suite():
    name = (request.json or {}).get("name", "").strip()
    if not name:
        return jsonify({"error": "Suite name required"}), 400
    suites = load_suites()
    if name in suites:
        return jsonify({"error": "Suite already exists"}), 409
    suites[name] = {"expectations": [], "created_at": datetime.utcnow().isoformat()}
    save_suites(suites)
    return jsonify({"message": f"Suite '{name}' créée.", "suite": suites[name]})

@app.route("/api/suites/<n>", methods=["GET"])
def get_suite(n):
    suites = load_suites()
    if n not in suites:
        return jsonify({"error": "Suite introuvable"}), 404
    return jsonify(suites[n])

@app.route("/api/suites/<n>", methods=["DELETE"])
def delete_suite(n):
    suites = load_suites()
    if n not in suites:
        return jsonify({"error": "Suite introuvable"}), 404
    del suites[n]
    save_suites(suites)
    return jsonify({"message": f"Suite '{n}' supprimée."})

@app.route("/api/suites/<n>/expectations", methods=["POST"])
def add_expectation(n):
    suites = load_suites()
    if n not in suites:
        return jsonify({"error": "Suite introuvable"}), 404
    exp      = request.json
    exp_type = exp.get("expectation_type")
    if exp_type not in EXPECTATION_CATALOGUE:
        return jsonify({"error": f"Type '{exp_type}' non reconnu"}), 400
    kwargs = {k: parse_param(k, v) for k, v in exp.get("kwargs", {}).items()}
    entry  = {"expectation_type": exp_type, "kwargs": kwargs}
    suites[n]["expectations"].append(entry)
    save_suites(suites)
    return jsonify({"message": "Expectation ajoutée.", "expectation": entry})

@app.route("/api/suites/<n>/expectations/<int:idx>", methods=["DELETE"])
def delete_expectation(n, idx):
    suites = load_suites()
    if n not in suites:
        return jsonify({"error": "Suite introuvable"}), 404
    exps = suites[n]["expectations"]
    if idx < 0 or idx >= len(exps):
        return jsonify({"error": "Index invalide"}), 400
    removed = exps.pop(idx)
    save_suites(suites)
    return jsonify({"message": "Expectation supprimée.", "removed": removed})

@app.route("/api/validate/<suite_name>", methods=["POST"])
def validate(suite_name):
    suites = load_suites()
    if suite_name not in suites:
        return jsonify({"error": "Suite introuvable"}), 404
    expectations = suites[suite_name].get("expectations", [])
    if not expectations:
        return jsonify({"error": "La suite ne contient aucune expectation"}), 400
    try:
        df               = read_active_df()
        df_columns       = set(df.columns)
        df_columns_lower = {c.lower(): c for c in df.columns}
        warnings         = []
        for exp in expectations:
            col = exp.get("kwargs", {}).get("column")
            if col and col not in df_columns:
                sug = df_columns_lower.get(col.lower())
                msg = f"Colonne '{col}' absente" + (f" — vouliez-vous dire '{sug}' ?" if sug else "")
                warnings.append({"column": col, "message": msg})

        ge_dataset = SparkDFDataset(df)
        individual = []
        passed = failed = 0

        for exp in expectations:
            kwargs    = exp.get("kwargs", {}).copy()
            col = kwargs.get("column")
            if col and col not in df_columns:
                resolved = df_columns_lower.get(col.lower())
                if resolved:
                    kwargs["column"] = resolved
            success = False
            error_msg = None
            try:
                result  = getattr(ge_dataset, exp["expectation_type"])(**kwargs)
                success = bool(result["success"])
            except AttributeError:
                error_msg = f"Expectation non supportée"
            except Exception as ex:
                error_msg = str(ex)
            passed += success
            failed += not success
            entry = {"expectation_type": exp["expectation_type"], "kwargs": kwargs,
                     "success": success, "status": "passed" if success else "failed"}
            if error_msg:
                entry["error"] = error_msg
            individual.append(entry)

        total      = passed + failed
        run_result = {
            "suite_name":        suite_name,
            "source_type":       _active_source["type"],
            "source_label":      active_label(),
            "available_columns": sorted(df_columns),
            "run_time":          datetime.utcnow().isoformat(),
            "warnings":          warnings,
            "summary": {"total": total, "passed": passed, "failed": failed,
                        "success_rate": round(passed / total * 100, 1) if total else 0},
            "results": individual,
        }
        all_results = load_results()
        all_results.insert(0, run_result)
        save_results(all_results[:50])
        return jsonify(run_result)
    except Exception as e:
        return jsonify({"error": str(e), "trace": traceback.format_exc()}), 500

@app.route("/api/results", methods=["GET"])
def get_results():
    return jsonify(load_results())

@app.route("/api/results", methods=["DELETE"])
def clear_results():
    save_results([])
    return jsonify({"message": "Historique effacé."})

# ─────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print("🚀  GE-Stack API démarrage …")
    try:
        get_spark()
        print("✅  Spark initialisé")
    except Exception as e:
        print(f"⚠️  Spark init error: {e}")
    app.run(host="0.0.0.0", port=5000, debug=False)
