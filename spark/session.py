# spark/session.py
import os
import atexit
from pathlib import Path
from pyspark.sql import SparkSession

# Intenta cargar variables de entorno desde .env (ignora fallo silenciosamente)
try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

# Guardamos una sola SparkSession para evitar crear varias accidentalmente
_SPARK_SINGLETON = {"spark": None}

def _abs_path(p: str) -> str:
    """Devuelve ruta absoluta con separador '/' (evita problemas en Windows con Spark)."""
    pth = Path(p).resolve()
    return str(pth).replace("\\", "/")

def _ensure_dir(path_str: str) -> str:
    """Crea el directorio si falta y devuelve la ruta normalizada."""
    p = Path(path_str)
    p.mkdir(parents=True, exist_ok=True)
    return _abs_path(str(p))

def get_spark(app_name: str = "pharma_pipeline_spark"):
    """
    Crea/reutiliza SparkSession con ajustes básicos (jars, tmp dirs, logging).
    En Windows aplica mitigaciones de Hadoop NativeIO; en Linux (Docker) no las aplica.
    """
    if _SPARK_SINGLETON["spark"] is not None:
        return _SPARK_SINGLETON["spark"]

    is_windows = (os.name == "nt")

    pkgs = os.getenv("SPARK_JARS_PACKAGES", "org.postgresql:postgresql:42.7.4")
    wh_dir = _ensure_dir(os.getenv("SPARK_WAREHOUSE_DIR", "./data/spark-warehouse"))
    local_dirs_env = os.getenv("SPARK_LOCAL_DIRS", "./data/spark-tmp")
    local_dirs = _ensure_dir(local_dirs_env)
    os.environ["SPARK_LOCAL_DIRS"] = local_dirs  # algunos componentes nativos lo revisan

    builder = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.jars.packages", pkgs)
        .config("spark.sql.warehouse.dir", wh_dir)
        .config("spark.local.dir", local_dirs)
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.cleaner.referenceTracking", "true")
        .config("spark.cleaner.periodicGC.interval", "1min")
    )

    # Mitigaciones SOLO en Windows (en Docker/Linux no hacen falta)
    if is_windows:
        builder = (
            builder
            .config("spark.hadoop.io.native.lib.available", "false")
            .config("spark.hadoop.hadoop.native.lib", "false")
            .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
            .config("spark.hadoop.mapreduce.fileoutputcommitter.cleanup-failures.ignored", "true")
            .config("spark.hadoop.fs.file.impl.disable.cache", "true")
        )

    spark = builder.getOrCreate()

    try:
        spark.sparkContext.setLogLevel("WARN")
    except Exception:
        pass

    # Refuerzo post-creación (algunos builds solo obedecen aquí)
    if is_windows:
        try:
            hconf = spark.sparkContext._jsc.hadoopConfiguration()
            hconf.set("io.native.lib.available", "false")
            hconf.set("hadoop.native.lib", "false")
            hconf.set("mapreduce.fileoutputcommitter.algorithm.version", "2")
            hconf.set("mapreduce.fileoutputcommitter.cleanup-failures.ignored", "true")
            hconf.set("fs.file.impl.disable.cache", "true")
        except Exception:
            pass

    # Asegurar parada limpia al finalizar el proceso (evita hooks a medias)
    def _stop_spark():
        try:
            if _SPARK_SINGLETON["spark"] is not None:
                _SPARK_SINGLETON["spark"].stop()
        except Exception:
            pass

    atexit.register(_stop_spark)
    _SPARK_SINGLETON["spark"] = spark
    return spark
