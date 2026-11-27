# Framework Big Data — <titre du projet>

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
| _à compléter_ | _à compléter_ | _à compléter_ | _AAAA-MM-JJ_ | _x,x Mo (entre 1 et 5 Mo)_ |

## Problématique

_À compléter en 3 phrases maximum : la question posée, pourquoi, et le résultat vérifiable attendu._

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
#    _à compléter_

# 3. Traitement : bronze -> silver -> gold
#    _à compléter_

# 4. Vérification indépendante d'un résultat
#    _à compléter_
```

Relance : chaque étape est idempotente (écrasement des sorties) et peut être relancée seule. En cas d'erreur
`ExpiredToken`/`403`, redémarrer le service Onyxia pour renouveler les identifiants S3 temporaires.
