#!/usr/bin/env bash
# Ingestion bronze : télécharge le dataset SNCF "Régularité mensuelle TGV par liaisons" (ODbL)
# et le dépose tel quel sur S3, partitionné par date d'ingestion. Idempotent : relancer le même
# jour écrase la même partition.
# Usage (racine du dépôt, après `source src/setup/onyxia_init.sh`) : bash src/ingestion/download_bronze.sh
set -euo pipefail

: "${BUCKET:?lancer au préalable : source src/setup/onyxia_init.sh}"
: "${S3_PREFIX:=project}"

DATASET="regularite-mensuelle-tgv-aqst"
API="https://ressources.data.sncf.com/api/explore/v2.1/catalog/datasets/${DATASET}"
INGEST_DATE="$(TZ=Europe/Paris date +%F)"
LOCAL_DIR="data/raw/${DATASET}/ingest_date=${INGEST_DATE}"
DEST="s3://${BUCKET}/${S3_PREFIX}/bronze/${DATASET}/ingest_date=${INGEST_DATE}/"

mkdir -p "$LOCAL_DIR"
echo "Téléchargement -> $LOCAL_DIR"
curl -fsSL -o "$LOCAL_DIR/${DATASET}.csv" "${API}/exports/csv?delimiter=%3B"
curl -fsSL -o "$LOCAL_DIR/metadata.json" "${API}"   # licence, date de mise à jour, schéma

# Contrôle de la contrainte de l'énoncé : 1 Mo <= taille brute <= 5 Mo
SIZE=$(du -cb "$LOCAL_DIR" | tail -n 1 | cut -f1)
echo "Taille brute : ${SIZE} octets ; sha256 du CSV : $(sha256sum "$LOCAL_DIR/${DATASET}.csv" | cut -d' ' -f1)"
if (( SIZE < 1000000 || SIZE > 5000000 )); then
  echo "ERREUR : taille hors de [1 Mo ; 5 Mo]" >&2; exit 1
fi

aws s3 cp "$LOCAL_DIR/" "$DEST" --recursive
TZ=Europe/Paris date '+Vérification S3 (%Y-%m-%d %H:%M:%S %Z) :'
echo "Namespace : $BUCKET"
aws s3 ls "s3://${BUCKET}/${S3_PREFIX}/bronze/" --recursive --human-readable --summarize
