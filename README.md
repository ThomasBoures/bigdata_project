# Framework Big Data — Ponctualité des TGV par liaison (2018–2026)

## Auteurs

- BOURES Thomas
- <NOM Prénom>

## Prérequis

- Compte Onyxia avec un service `vscode-pyspark` (4 CPU, 4 Go RAM) et son bucket S3 personnel
- Outils fournis par l'image : `aws` CLI, `kubectl`, `uv`, `pyspark` (Spark 4.1), `duckdb`
- Accès réseau à Maven Central (téléchargement d'Iceberg au démarrage de Spark) et à la source du dataset

## Dataset

| Nom | URL | Licence | Date de téléchargement | Taille brute |
|-----|-----|---------|------------------------|--------------|
| SNCF Voyageurs — Régularité mensuelle TGV par liaisons (`regularite-mensuelle-tgv-aqst`), 12 907 lignes, 2018-01 → 2026-09 | https://ressources.data.sncf.com/explore/dataset/regularite-mensuelle-tgv-aqst/ | ODbL | 2026-11-27 | 2,9 Mo (CSV `;`) |

## Problématique

Entre 2018-2019 et 2024-2025, quelles liaisons TGV ont le plus dégradé leur taux de trains arrivés avec plus de 15 minutes de retard ? Pour ces liaisons, quelle catégorie de cause (externe, infrastructure, gestion du trafic, matériel roulant, gare, voyageurs) pèse le plus dans les retards ? Le résultat est une table gold par liaison (taux par période, écart, cause dominante), recalculée indépendamment avec DuckDB.

## Chemins S3

- Bucket : `s3://user-<username>` (= namespace Kubernetes Onyxia)
- Brut : `s3://user-<username>/project/bronze/`
- Traité : `s3://user-<username>/project/silver/`, `s3://user-<username>/project/gold/`

## Reproduction

```bash
# 1. Cloner le dépôt dans le service Onyxia et initialiser l'environnement
cd /home/onyxia/work && git clone https://github.com/ThomasBoures/bigdata_project.git
cd bigdata_project && source src/setup/onyxia_init.sh

# 2. Ingestion : télécharger les données brutes et les déposer sur S3 (bronze)
bash src/ingestion/download_bronze.sh

# 3. Traitement : bronze -> silver -> gold
#    _à compléter_

# 4. Vérification indépendante d'un résultat
#    _à compléter_
```

Relance : chaque étape est idempotente (écrasement des sorties) et peut être relancée seule. En cas d'erreur
`ExpiredToken`/`403`, redémarrer le service Onyxia pour renouveler les identifiants S3 temporaires.
