# Ingestion (bronze)

Deux façons de déposer les données brutes sur S3 — on peut documenter les deux dans le rapport (alternative + mesure).

Dataset : SNCF « Régularité mensuelle TGV par liaisons » (`regularite-mensuelle-tgv-aqst`, ODbL), export CSV
séparé par `;`, ~2,9 Mo. Destination : `s3://$BUCKET/project/bronze/regularite-mensuelle-tgv-aqst/ingest_date=AAAA-MM-JJ/`.

## A. Depuis le terminal du service (méthode retenue)

```bash
source src/setup/onyxia_init.sh
bash src/ingestion/download_bronze.sh   # télécharge, contrôle 1–5 Mo, dépose sur S3, liste le bucket
```

→ capture d'écran de la sortie (namespace + date affichés) : `evidence/01_bronze_s3.png`.

## B. Depuis un Job Kubernetes (`k8s/job-upload-bronze.yaml`)

```bash
kubectl auth can-i create jobs   # doit répondre yes
# Le CSV fait ~2,9 Mo > 1 MiB : une ConfigMap ne suffit pas, d'où le choix de la méthode A (à citer comme
# alternative rejetée dans le rapport), sauf à découper le fichier en plusieurs ConfigMaps.
kubectl create configmap raw-data --from-file=data/raw/            # max 1 MiB par ConfigMap
kubectl create configmap s3-config \
  --from-literal=AWS_ENDPOINT_URL="$S3_ENDPOINT_URL" \
  --from-literal=AWS_DEFAULT_REGION="${AWS_DEFAULT_REGION:-us-east-1}" \
  --from-literal=BUCKET="$BUCKET"
aws configure export-credentials --format env-no-export \
  | grep -E '^AWS_(ACCESS_KEY_ID|SECRET_ACCESS_KEY|SESSION_TOKEN)=' > s3.env
kubectl create secret generic s3-credentials --from-env-file=s3.env && rm s3.env

kubectl apply -f k8s/job-upload-bronze.yaml
kubectl wait --for=condition=complete job/upload-bronze --timeout=120s
kubectl logs job/upload-bronze          # <- capture d'écran (trace d'exécution)

# Nettoyage
kubectl delete -f k8s/job-upload-bronze.yaml
kubectl delete configmap raw-data s3-config && kubectl delete secret s3-credentials
```

Si les fichiers dépassent 1 MiB, découper en plusieurs ConfigMaps ou utiliser la méthode A.
