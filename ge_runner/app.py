"""
Great Expectations + PySpark Backend API
Provides endpoints for:
- Managing Expectation Suites
- Running Validations on CSV data via Spark
- Retrieving validation results

Architecture:
  GE valide directement sur le SparkDataFrame via great_expectations.dataset.SparkDFDataset
  (API legacy mais stable). Aucune conversion Pandas, aucun contexte éphémère —
  la SparkSession singleton reste vivante pour toute la durée du process Flask.
"""

import os
import json
import traceback
from datetime import datetime
from flask import Flask, request, jsonify
from flask_cors import CORS

# ── PySpark ───────────────────────────────────────────────────────────────────
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# ── Great Expectations — dataset API (native Spark, pas de contexte éphémère) ─
from great_expectations.dataset import SparkDFDataset

# ─────────────────────────────────────────────────────────────────────────────
app = Flask(__name__)
CORS(app)

DATA_DIR    = "/data"
GE_ROOT     = "/app/ge_project"
RESULTS_DIR = "/app/ge_results"
os.makedirs(RESULTS_DIR, exist_ok=True)
os.makedirs(GE_ROOT, exist_ok=True)

# ── Active file (mutable shared state) ────────────────────────────────────
SUPPORTED_EXT = {".csv", ".parquet", ".json", ".jsonl"}
_active_file  = {"path": None}

def list_data_files():
    """List all supported files in DATA_DIR with metadata."""
    result = []
    if not os.path.isdir(DATA_DIR):
        return result
    for name in sorted(os.listdir(DATA_DIR)):
        if os.path.splitext(name)[1].lower() not in SUPPORTED_EXT:
            continue
        full = os.path.join(DATA_DIR, name)
        stat = os.stat(full)
        result.append({
            "name": name,
            "path": full,
            "ext":  os.path.splitext(name)[1].lstrip(".").lower(),
            "size_kb": round(stat.st_size / 1024, 1),
            "active": full == _active_file["path"],
        })
    return result

def get_active_path():
    """Return active file path; auto-select first available file if none set."""
    if _active_file["path"] and os.path.exists(_active_file["path"]):
        return _active_file["path"]
    files = list_data_files()
    if files:
        _active_file["path"] = files[0]["path"]
        return _active_file["path"]
    return None

def read_as_spark_df(path):
    """Read CSV, Parquet or JSON into a Spark DataFrame."""
    spark = get_spark()
    ext = os.path.splitext(path)[1].lower()
    if ext == ".csv":
        return spark.read.csv(path, header=True, inferSchema=True)
    if ext == ".parquet":
        return spark.read.parquet(path)
    if ext in (".json", ".jsonl"):
        return spark.read.json(path)
    raise ValueError(f"Format non supporté : {ext}")

# ── Spark Session (resilient singleton) ───────────────────────────────────
_spark = None

def get_spark():
    global _spark
    # Test if existing session is still alive
    if _spark is not None:
        try:
            _spark.sparkContext.statusTracker().getActiveStageIds()
        except Exception:
            _spark = None  # JVM died, force recreate

    if _spark is None:
        _spark = (
            SparkSession.builder
            .master("local[2]")
            .appName("GE-Validator")
            .config("spark.sql.shuffle.partitions", "4")
            .config("spark.ui.enabled", "false")
            .config("spark.driver.memory", "1g")
            .config("spark.executor.memory", "512m")
            .config("spark.driver.maxResultSize", "256m")
            .config("spark.sql.execution.arrow.pyspark.enabled", "true")
            .getOrCreate()
        )
        _spark.sparkContext.setLogLevel("ERROR")
    return _spark

# ── GE Context (singleton) ─────────────────────────────────────────────────
_context = None

def get_context():
    global _context
    if _context is None:
        _context = gx.get_context(
            context_root_dir=GE_ROOT,
            mode="file",
        )
    return _context


def build_in_memory_context():
    """Return a fresh ephemeral context for each run (avoids datasource conflicts)."""
    return gx.get_context(mode="ephemeral")


# ─────────────────────────────────────────────────────────────────────────────
# BUILT-IN EXPECTATION CATALOGUE
# ─────────────────────────────────────────────────────────────────────────────
EXPECTATION_CATALOGUE = {
    "expect_column_to_exist": {
        "label": "Column exists",
        "params": ["column"],
        "description": "Vérifie que la colonne existe dans le dataset."
    },
    "expect_column_values_to_not_be_null": {
        "label": "Pas de valeurs nulles",
        "params": ["column"],
        "description": "Vérifie l'absence de valeurs NULL dans la colonne."
    },
    "expect_column_values_to_be_unique": {
        "label": "Valeurs uniques",
        "params": ["column"],
        "description": "Vérifie que toutes les valeurs de la colonne sont uniques."
    },
    "expect_column_values_to_be_between": {
        "label": "Valeurs dans un intervalle",
        "params": ["column", "min_value", "max_value"],
        "description": "Vérifie que les valeurs numériques sont dans [min, max]."
    },
    "expect_column_values_to_match_regex": {
        "label": "Regex match",
        "params": ["column", "regex"],
        "description": "Vérifie que les valeurs correspondent à une expression régulière."
    },
    "expect_column_values_to_be_in_set": {
        "label": "Valeurs dans un ensemble",
        "params": ["column", "value_set"],
        "description": "Vérifie que les valeurs font partie d'un ensemble défini."
    },
    "expect_table_row_count_to_be_between": {
        "label": "Nombre de lignes",
        "params": ["min_value", "max_value"],
        "description": "Vérifie que le nombre de lignes est dans un intervalle."
    },
    "expect_column_mean_to_be_between": {
        "label": "Moyenne dans un intervalle",
        "params": ["column", "min_value", "max_value"],
        "description": "Vérifie que la moyenne d'une colonne est dans [min, max]."
    },
}


# ─────────────────────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def load_suites():
    path = os.path.join(RESULTS_DIR, "suites.json")
    if os.path.exists(path):
        with open(path) as f:
            return json.load(f)
    return {}


def save_suites(suites):
    path = os.path.join(RESULTS_DIR, "suites.json")
    with open(path, "w") as f:
        json.dump(suites, f, indent=2)


def load_results():
    path = os.path.join(RESULTS_DIR, "results.json")
    if os.path.exists(path):
        with open(path) as f:
            return json.load(f)
    return []


def save_results(results):
    path = os.path.join(RESULTS_DIR, "results.json")
    with open(path, "w") as f:
        json.dump(results, f, indent=2)


def parse_param(key, val):
    """Auto-cast param strings to proper types."""
    if key in ("min_value", "max_value"):
        try:
            return float(val)
        except (ValueError, TypeError):
            return val
    if key == "value_set":
        if isinstance(val, list):
            return val
        return [v.strip() for v in str(val).split(",")]
    return val


# ─────────────────────────────────────────────────────────────────────────────
# ROUTES — DATA
# ─────────────────────────────────────────────────────────────────────────────

@app.route("/api/data/columns", methods=["GET"])
def data_columns():
    """Return columns of the active file — used by the UI to populate dropdowns."""
    path = get_active_path()
    if not path:
        return jsonify({"columns": [], "file": None})
    try:
        df = read_as_spark_df(path)
        cols = [{"name": c, "type": str(dict(df.dtypes)[c])} for c in df.columns]
        return jsonify({"columns": cols, "file": os.path.basename(path)})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/data/files", methods=["GET"])
def data_files():
    """List all files available in DATA_DIR."""
    files = list_data_files()
    active = get_active_path()
    return jsonify({"files": files, "active": active})


@app.route("/api/data/select", methods=["POST"])
def data_select():
    """Set the active file by filename."""
    name = (request.json or {}).get("name", "").strip()
    if not name:
        return jsonify({"error": "Paramètre 'name' manquant"}), 400
    full = os.path.join(DATA_DIR, name)
    if not os.path.exists(full):
        return jsonify({"error": f"Fichier introuvable : {name}"}), 404
    ext = os.path.splitext(name)[1].lower()
    if ext not in SUPPORTED_EXT:
        return jsonify({"error": f"Format non supporté : {ext}"}), 400
    _active_file["path"] = full
    return jsonify({"message": f"Fichier actif : {name}", "path": full})


@app.route("/api/data/preview", methods=["GET"])
def data_preview():
    path = get_active_path()
    if not path:
        return jsonify({"error": "Aucun fichier dans /data. Déposez un CSV/Parquet/JSON."}), 404
    try:
        df = read_as_spark_df(path)
        rows = [row.asDict() for row in df.limit(10).collect()]
        return jsonify({
            "columns": df.columns,
            "rows": rows,
            "total_rows": df.count(),
            "file": os.path.basename(path),
            "file_path": path,
        })
    except Exception as e:
        return jsonify({"error": str(e), "trace": traceback.format_exc()}), 500


@app.route("/api/data/stats", methods=["GET"])
def data_stats():
    path = get_active_path()
    if not path:
        return jsonify({"error": "Aucun fichier actif"}), 404
    try:
        df = read_as_spark_df(path)
        total = df.count()
        stats = []
        for col in df.columns:
            null_count   = df.filter(F.col(col).isNull()).count()
            unique_count = df.select(col).distinct().count()
            dtype = str(dict(df.dtypes)[col])
            stat = {
                "column": col,
                "type": dtype,
                "nulls": null_count,
                "unique": unique_count,
                "total": total,
            }
            if dtype in ("int", "double", "bigint", "float", "long"):
                agg = df.select(
                    F.min(col).alias("min"),
                    F.max(col).alias("max"),
                    F.avg(col).alias("mean"),
                ).collect()[0]
                stat.update({
                    "min":  float(agg["min"])  if agg["min"]  is not None else None,
                    "max":  float(agg["max"])  if agg["max"]  is not None else None,
                    "mean": float(agg["mean"]) if agg["mean"] is not None else None,
                })
            stats.append(stat)
        return jsonify(stats)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ─────────────────────────────────────────────────────────────────────────────
# ROUTES — EXPECTATION CATALOGUE
# ─────────────────────────────────────────────────────────────────────────────

@app.route("/api/expectations/catalogue", methods=["GET"])
def catalogue():
    return jsonify(EXPECTATION_CATALOGUE)


# ─────────────────────────────────────────────────────────────────────────────
# ROUTES — SUITES CRUD
# ─────────────────────────────────────────────────────────────────────────────

@app.route("/api/suites", methods=["GET"])
def list_suites():
    return jsonify(load_suites())


@app.route("/api/suites", methods=["POST"])
def create_suite():
    data   = request.json
    name   = data.get("name", "").strip()
    if not name:
        return jsonify({"error": "Suite name required"}), 400
    suites = load_suites()
    if name in suites:
        return jsonify({"error": "Suite already exists"}), 409
    suites[name] = {"expectations": [], "created_at": datetime.utcnow().isoformat()}
    save_suites(suites)
    return jsonify({"message": f"Suite '{name}' créée.", "suite": suites[name]})


@app.route("/api/suites/<name>", methods=["GET"])
def get_suite(name):
    suites = load_suites()
    if name not in suites:
        return jsonify({"error": "Suite introuvable"}), 404
    return jsonify(suites[name])


@app.route("/api/suites/<name>", methods=["DELETE"])
def delete_suite(name):
    suites = load_suites()
    if name not in suites:
        return jsonify({"error": "Suite introuvable"}), 404
    del suites[name]
    save_suites(suites)
    return jsonify({"message": f"Suite '{name}' supprimée."})


@app.route("/api/suites/<name>/expectations", methods=["POST"])
def add_expectation(name):
    suites = load_suites()
    if name not in suites:
        return jsonify({"error": "Suite introuvable"}), 404
    exp    = request.json
    exp_type = exp.get("expectation_type")
    if exp_type not in EXPECTATION_CATALOGUE:
        return jsonify({"error": f"Type '{exp_type}' non reconnu"}), 400
    # cast params
    kwargs = {k: parse_param(k, v) for k, v in exp.get("kwargs", {}).items()}
    entry  = {"expectation_type": exp_type, "kwargs": kwargs}
    suites[name]["expectations"].append(entry)
    save_suites(suites)
    return jsonify({"message": "Expectation ajoutée.", "expectation": entry})


@app.route("/api/suites/<name>/expectations/<int:idx>", methods=["DELETE"])
def delete_expectation(name, idx):
    suites = load_suites()
    if name not in suites:
        return jsonify({"error": "Suite introuvable"}), 404
    exps = suites[name]["expectations"]
    if idx < 0 or idx >= len(exps):
        return jsonify({"error": "Index invalide"}), 400
    removed = exps.pop(idx)
    save_suites(suites)
    return jsonify({"message": "Expectation supprimée.", "removed": removed})


# ─────────────────────────────────────────────────────────────────────────────
# ROUTES — VALIDATION
# ─────────────────────────────────────────────────────────────────────────────

@app.route("/api/validate/<suite_name>", methods=["POST"])
def validate(suite_name):
    suites = load_suites()
    if suite_name not in suites:
        return jsonify({"error": "Suite introuvable"}), 404

    suite_cfg    = suites[suite_name]
    expectations = suite_cfg.get("expectations", [])
    if not expectations:
        return jsonify({"error": "La suite ne contient aucune expectation"}), 400

    try:
        path = get_active_path()
        if not path:
            return jsonify({"error": "Aucun fichier actif sélectionné"}), 404

        # ── Lire le fichier en Spark DataFrame natif ──────────────────────
        spark_df    = read_as_spark_df(path)
        df_columns  = set(spark_df.columns)
        df_columns_lower = {c.lower(): c for c in spark_df.columns}

        # ── Pré-vérification : colonnes référencées vs colonnes réelles ───
        # Détecte les erreurs de nommage AVANT d'envoyer à Spark.
        # Tente aussi une correspondance insensible à la casse.
        column_warnings = []
        for exp in expectations:
            col = exp.get("kwargs", {}).get("column")
            if col and col not in df_columns:
                suggestion = df_columns_lower.get(col.lower())
                msg = f"Colonne '{col}' absente du fichier '{os.path.basename(path)}'"
                if suggestion:
                    msg += f" — vouliez-vous dire '{suggestion}' ?"
                else:
                    msg += f". Colonnes disponibles : {sorted(df_columns)}"
                column_warnings.append({"column": col, "message": msg})

        # ── Encapsuler dans SparkDFDataset ────────────────────────────────
        ge_dataset = SparkDFDataset(spark_df)

        individual = []
        passed = 0
        failed = 0

        for exp in expectations:
            exp_type  = exp["expectation_type"]
            kwargs    = exp.get("kwargs", {})
            error_msg = None
            success   = False

            # Résolution insensible à la casse sur le nom de colonne
            col = kwargs.get("column")
            if col and col not in df_columns:
                resolved = df_columns_lower.get(col.lower())
                if resolved:
                    kwargs = {**kwargs, "column": resolved}

            try:
                method = getattr(ge_dataset, exp_type)
                result = method(**kwargs)
                success = bool(result["success"])
            except AttributeError:
                error_msg = f"Expectation '{exp_type}' non supportée par SparkDFDataset"
            except Exception as ex:
                error_msg = str(ex)

            if success:
                passed += 1
            else:
                failed += 1

            entry = {
                "expectation_type": exp_type,
                "kwargs":           kwargs,
                "success":          success,
                "status":           "passed" if success else "failed",
            }
            if error_msg:
                entry["error"] = error_msg
            individual.append(entry)

        total      = passed + failed
        run_result = {
            "suite_name":       suite_name,
            "source_file":      os.path.basename(path),
            "available_columns": sorted(df_columns),
            "run_time":         datetime.utcnow().isoformat(),
            "warnings":         column_warnings,
            "summary": {
                "total":        total,
                "passed":       passed,
                "failed":       failed,
                "success_rate": round(passed / total * 100, 1) if total else 0,
            },
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
    # warm-up Spark
    try:
        get_spark()
        print("✅  Spark initialisé")
    except Exception as e:
        print(f"⚠️  Spark init error: {e}")
    app.run(host="0.0.0.0", port=5000, debug=False)
