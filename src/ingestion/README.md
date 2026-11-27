# Ingestion (bronze)

Deux façons de déposer les données brutes sur S3 — on peut documenter les deux dans le rapport (alternative + mesure).

## A. Depuis le terminal du service

```bash
source src/setup/onyxia_init.sh
mkdir -p data/raw && cd data/raw
# curl -fsSLO <URL du dataset>        # à compléter une fois le dataset choisi
du -ch . | tail -n 1                   # doit être entre 1 Mo et 5 Mo
cd ../..
aws s3 --profile default cp data/raw/ "s3://$BUCKET/$S3_PREFIX/bronze/" --recursive
aws s3 --profile default ls "s3://$BUCKET/$S3_PREFIX/bronze/" --recursive --human-readable --summarize
```

## B. Depuis un Job Kubernetes (`k8s/job-upload-bronze.yaml`)

```bash
kubectl auth can-i create jobs   # doit répondre yes
kubectl create configmap raw-data --from-file=data/raw/            # max 1 MiB par ConfigMap
kubectl create configmap s3-config \
  --from-literal=AWS_ENDPOINT_URL="$S3_ENDPOINT_URL" \
  --from-literal=AWS_DEFAULT_REGION="$(aws configure get region --profile default)" \
  --from-literal=BUCKET="$BUCKET"
aws configure export-credentials --profile default --format env-no-export \
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
