"""Bronze -> silver : CSV SNCF brut -> table Parquet typée, nettoyée et annotée (drapeaux qualité).

Lit la dernière partition `ingest_date=` du bronze (ou celle de $INGEST_DATE), applique un schéma explicite,
corrige ou marque les problèmes de qualité mesurés, puis écrit (écrasement = idempotent) :
  s3://$BUCKET/project/silver/regularite_tgv/        Parquet partitionné par annee
  s3://$BUCKET/project/silver/_qualite/regularite_tgv/  rapport qualité (règle, lignes touchées, traitement)

Usage (racine du dépôt, après `source src/setup/onyxia_init.sh`) :
    python src/processing/bronze_to_silver.py
"""

import os
import sys
import time
from datetime import datetime
from zoneinfo import ZoneInfo

from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

sys.path.insert(0, os.path.dirname(__file__))
from spark_session import BUCKET, get_spark, s3_path  # noqa: E402

DATASET = "regularite-mensuelle-tgv-aqst"
EXPECTED_ROWS = int(os.environ.get("EXPECTED_ROWS", "12907"))  # contrôle de complétude (0 = désactivé)

# Schéma explicite, dans l'ordre des colonnes du CSV (pas d'inferSchema).
COUNTS = [
    "nb_train_prevu", "nb_annulation", "nb_train_depart_retard", "nb_train_retard_arrivee",
    "nb_train_retard_sup_15", "nb_train_retard_sup_30", "nb_train_retard_sup_60",
]
CAUSES = [
    "prct_cause_externe", "prct_cause_infra", "prct_cause_gestion_trafic",
    "prct_cause_materiel_roulant", "prct_cause_gestion_gare", "prct_cause_prise_en_charge_voyageurs",
]
SCHEMA = StructType([
    StructField("date", StringType()),                      # AAAA-MM
    StructField("service", StringType()),                   # National / International
    StructField("gare_depart", StringType()),
    StructField("gare_arrivee", StringType()),
    StructField("duree_moyenne", IntegerType()),            # minutes
    StructField("nb_train_prevu", IntegerType()),
    StructField("nb_annulation", IntegerType()),
    StructField("commentaire_annulation", StringType()),
    StructField("nb_train_depart_retard", IntegerType()),
    StructField("retard_moyen_depart", DoubleType()),
    StructField("retard_moyen_tous_trains_depart", DoubleType()),
    StructField("commentaire_retards_depart", StringType()),
    StructField("nb_train_retard_arrivee", IntegerType()),
    StructField("retard_moyen_arrivee", DoubleType()),
    StructField("retard_moyen_tous_trains_arrivee", DoubleType()),
    StructField("commentaires_retard_arrivee", StringType()),
    StructField("nb_train_retard_sup_15", IntegerType()),
    StructField("retard_moyen_trains_retard_sup15", DoubleType()),
    StructField("nb_train_retard_sup_30", IntegerType()),
    StructField("nb_train_retard_sup_60", IntegerType()),
    *[StructField(c, DoubleType()) for c in CAUSES],
    StructField("_corrupt_record", StringType()),            # lignes illisibles (mode PERMISSIVE)
])


def latest_bronze_file(spark) -> str:
    """Chemin du CSV de la partition ingest_date demandée ($INGEST_DATE) ou, à défaut, la plus récente."""
    base = s3_path("bronze", DATASET)
    wanted = os.environ.get("INGEST_DATE")
    if not wanted:
        jvm = spark.sparkContext._jvm
        path = jvm.org.apache.hadoop.fs.Path(base)
        fs = path.getFileSystem(spark.sparkContext._jsc.hadoopConfiguration())
        parts = sorted(
            st.getPath().getName() for st in fs.listStatus(path) if st.getPath().getName().startswith("ingest_date=")
        )
        if not parts:
            raise SystemExit(f"Aucune partition ingest_date= sous {base} : lancer d'abord src/ingestion/download_bronze.sh")
        wanted = parts[-1].split("=", 1)[1]
    return f"{base}/ingest_date={wanted}/{DATASET}.csv"


def read_bronze(spark, path: str) -> DataFrame:
    return (
        spark.read.schema(SCHEMA)
        .option("header", True)
        .option("sep", ";")
        .option("encoding", "UTF-8")
        .option("multiLine", True)     # commentaires multi-lignes entre guillemets
        .option("quote", '"')
        .option("escape", '"')
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .csv(path)
    )


def clean(raw: DataFrame) -> DataFrame:
    df = raw.where(F.col("_corrupt_record").isNull()).drop("_corrupt_record")

    # Texte : espaces superflus, chaînes vides -> NULL ; colonnes de commentaires toujours vides supprimées.
    for c in ["service", "gare_depart", "gare_arrivee"]:
        df = df.withColumn(c, F.upper(F.regexp_replace(F.trim(c), r"\s+", " ")))
    df = df.withColumn("commentaires_retard_arrivee",
                       F.when(F.trim("commentaires_retard_arrivee") != "", F.trim("commentaires_retard_arrivee")))
    df = df.drop("commentaire_annulation", "commentaire_retards_depart")

    # Dates : 'AAAA-MM' -> premier jour du mois.
    df = (df.withColumn("mois", F.to_date(F.concat("date", F.lit("-01")), "yyyy-MM-dd"))
            .withColumn("annee", F.year("mois"))
            .drop("date"))

    # Comptages négatifs : impossibles -> NULL. Retard moyen « tous trains » négatif = avance, conservé.
    for c in COUNTS:
        df = df.withColumn(c, F.when(F.col(c) >= 0, F.col(c)))
    # Retards moyens des seuls trains en retard : ne peuvent pas être négatifs -> NULL.
    for c in ["retard_moyen_depart", "retard_moyen_arrivee", "retard_moyen_trains_retard_sup15"]:
        df = df.withColumn(c, F.when(F.col(c) >= 0, F.col(c)))
    # Colonne recopiée à la source (valeur identique au retard moyen de tous les trains) -> NULL.
    df = df.withColumn(
        "retard_moyen_trains_retard_sup15",
        F.when(F.col("retard_moyen_trains_retard_sup15") != F.col("retard_moyen_tous_trains_arrivee"),
               F.col("retard_moyen_trains_retard_sup15")),
    )
    # Durée nulle (mois sans circulation) -> NULL.
    df = df.withColumn("duree_moyenne", F.when(F.col("duree_moyenne") > 0, F.col("duree_moyenne")))

    # Colonnes dérivées et drapeaux qualité (les lignes sont conservées, gold filtre sur les drapeaux).
    somme_causes = sum(F.coalesce(F.col(c), F.lit(0.0)) for c in CAUSES)
    df = (
        df.withColumn("nb_train_circule", F.col("nb_train_prevu") - F.col("nb_annulation"))
        .withColumn("somme_causes", F.round(somme_causes, 4))
        .withColumn("causes_valides", F.col("somme_causes").between(99.0, 101.0))
        .withColumn(
            "comptages_valides",
            (F.col("nb_train_prevu") > 0)
            & (F.col("nb_annulation") <= F.col("nb_train_prevu"))
            & F.col("nb_train_retard_sup_15").isNotNull()
            & (F.col("nb_train_retard_sup_15") <= F.col("nb_train_retard_arrivee"))
            & (F.col("nb_train_retard_arrivee") <= F.col("nb_train_circule")),
        )
        .withColumn("liaison", F.concat_ws(" -> ", "gare_depart", "gare_arrivee"))
    )
    first = ["mois", "annee", "service", "gare_depart", "gare_arrivee", "liaison"]
    return df.select(*first, *[c for c in df.columns if c not in first])


def quality_report(spark, raw: DataFrame, silver: DataFrame) -> DataFrame:
    """Nombre de lignes touchées par chaque règle, calculé sur les données brutes typées."""
    r = raw.where(F.col("_corrupt_record").isNull())
    neg_counts = " OR ".join(f"{c} < 0" for c in COUNTS)
    causes_sum = " + ".join(CAUSES)
    key = ["date", "service", "gare_depart", "gare_arrivee"]
    rules = [
        ("Ligne illisible (CSV corrompu)", raw.where(F.col("_corrupt_record").isNotNull()).count(), "ligne écartée"),
        ("Clé (date, gare_depart, gare_arrivee) en double, services National et International distincts",
         r.groupBy("date", "gare_depart", "gare_arrivee").count().where("count > 1").count() * 2,
         "conservées : la clé réelle inclut service"),
        ("Clé (date, service, gare_depart, gare_arrivee) en double",
         r.groupBy(*key).count().where("count > 1").count(), "aucune"),
        ("Commentaires multi-lignes (lecture multiLine nécessaire)",
         r.where(F.col("commentaires_retard_arrivee").contains("\n")).count(), "multiLine=true, escape='\"'"),
        ("Comptage négatif", r.where(neg_counts).count(), "valeur -> NULL"),
        ("Retard moyen « tous trains » négatif (avance)",
         r.where("retard_moyen_tous_trains_depart < 0 OR retard_moyen_tous_trains_arrivee < 0").count(), "conservé"),
        ("Retard moyen des trains en retard négatif",
         r.where("retard_moyen_depart < 0 OR retard_moyen_arrivee < 0 OR retard_moyen_trains_retard_sup15 < 0").count(),
         "valeur -> NULL"),
        ("retard_moyen_trains_retard_sup15 = retard_moyen_tous_trains_arrivee (colonne recopiée)",
         r.where("retard_moyen_trains_retard_sup15 = retard_moyen_tous_trains_arrivee").count(), "valeur -> NULL"),
        ("Aucun train prévu (nb_train_prevu = 0)", r.where("nb_train_prevu = 0").count(), "comptages_valides = false"),
        ("Annulations > trains prévus", r.where("nb_annulation > nb_train_prevu").count(), "comptages_valides = false"),
        ("Retards > 15 min > trains en retard à l'arrivée",
         r.where("nb_train_retard_sup_15 > nb_train_retard_arrivee").count(), "comptages_valides = false"),
        ("Retards > 60 min > retards > 30 min", r.where("nb_train_retard_sup_60 > nb_train_retard_sup_30").count(),
         "signalé, non utilisé"),
        ("Somme des 6 causes hors [99 ; 101] %", r.where(f"NOT ({causes_sum}) BETWEEN 99 AND 101").count(),
         "causes_valides = false"),
        ("Colonnes de commentaires toujours vides (commentaire_annulation, commentaire_retards_depart)",
         r.where("commentaire_annulation IS NULL AND commentaire_retards_depart IS NULL").count(), "colonnes supprimées"),
        ("Lignes silver exploitables pour le taux > 15 min", silver.where("comptages_valides").count(), "-"),
        ("Lignes silver exploitables pour les causes", silver.where("comptages_valides AND causes_valides").count(), "-"),
    ]
    return spark.createDataFrame(rules, "regle string, lignes_touchees long, traitement string")


def main() -> None:
    t0 = time.perf_counter()
    spark = get_spark("bronze-to-silver", iceberg=False)
    now = datetime.now(ZoneInfo("Europe/Paris")).strftime("%Y-%m-%d %H:%M:%S %Z")
    print(f"[{now}] namespace={BUCKET} spark={spark.version}")

    src = latest_bronze_file(spark)
    raw = read_bronze(spark, src).cache()
    n_raw = raw.count()
    t_read = time.perf_counter()
    print(f"Lecture bronze : {src} -> {n_raw} lignes ({t_read - t0:.1f} s depuis le démarrage)")
    if EXPECTED_ROWS and n_raw != EXPECTED_ROWS:
        raise SystemExit(f"ERREUR : {n_raw} lignes lues, {EXPECTED_ROWS} attendues (lecture CSV à revoir)")

    silver = clean(raw).cache()
    n_silver = silver.count()
    dest = s3_path("silver", "regularite_tgv")
    silver.write.mode("overwrite").partitionBy("annee").parquet(dest)
    t_write = time.perf_counter()
    print(f"Écriture silver : {dest} -> {n_silver} lignes ({t_write - t_read:.1f} s)")

    report = quality_report(spark, raw, silver)
    report.coalesce(1).write.mode("overwrite").option("header", True).csv(s3_path("silver", "_qualite", "regularite_tgv"))
    report.show(truncate=False)

    # Relecture de contrôle depuis S3.
    back = spark.read.parquet(dest)
    print("Relecture silver :", back.count(), "lignes,", len(back.columns), "colonnes")
    back.groupBy("annee").agg(F.count("*").alias("lignes"), F.sum(F.col("comptages_valides").cast("int")).alias("valides")) \
        .orderBy("annee").show()
    back.printSchema()
    print(f"Durée totale : {time.perf_counter() - t0:.1f} s")
    spark.stop()


if __name__ == "__main__":
    main()
