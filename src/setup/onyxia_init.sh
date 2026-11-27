#!/usr/bin/env bash
# Initialisation de l'environnement à chaque (re)démarrage d'un service vscode-pyspark Onyxia.
# Usage (depuis la racine du dépôt) :  source src/setup/onyxia_init.sh
# "source" et non "bash" : les variables exportées doivent rester dans le terminal courant.

# --- Variables du projet -------------------------------------------------------------
export BUCKET="${KUBERNETES_NAMESPACE:?KUBERNETES_NAMESPACE absent : es-tu bien dans un service Onyxia ?}"
export S3_PREFIX="project"
export S3_ENDPOINT_URL="https://${AWS_S3_ENDPOINT:?AWS_S3_ENDPOINT absent}"
# Le service n'a pas toujours de profil ~/.aws "default" : la CLI aws lit alors les AWS_* de l'environnement.
# AWS_ENDPOINT_URL lui indique l'endpoint S3 d'Onyxia (sinon elle vise AWS).
export AWS_ENDPOINT_URL="$S3_ENDPOINT_URL"
unset AWS_PROFILE
export PATH="$HOME/.local/bin:$PATH"

echo "Namespace / bucket : $BUCKET"
echo "Endpoint S3        : $S3_ENDPOINT_URL"
echo "Préfixe projet     : s3://$BUCKET/$S3_PREFIX/"
TZ=Europe/Paris date '+Date/heure        : %Y-%m-%d %H:%M:%S %Z'

# --- Vérification de l'accès S3 ------------------------------------------------------
if aws s3 ls "s3://$BUCKET/" >/dev/null 2>&1; then
  echo "Accès S3           : OK"
  aws s3 ls "s3://$BUCKET/$S3_PREFIX/" --recursive --human-readable --summarize 2>/dev/null | tail -n 2
else
  echo "Accès S3           : ÉCHEC -> copier le script 'S3 profile' du portail Onyxia, ou redémarrer le service" >&2
fi

# --- Git -----------------------------------------------------------------------------
git config --global init.defaultBranch main
git config --global pull.rebase true
echo "Git                : $(git config user.name) <$(git config user.email)> sur $(git branch --show-current 2>/dev/null)"
git fetch --quiet 2>/dev/null && git status -sb | head -n 1
echo "Rappel : commit + push avant de supprimer le service."
