# Reprendre le projet sur Onyxia

## 1. Lancer le service

Portail Onyxia → *Mes services* → **vscode-pyspark**. Dans la configuration, mettre les limites `cpu: 4000m` et
`memory: 4000Mi`, et activer le rôle Kubernetes **admin** si on veut lancer des Jobs (`kubectl auth can-i create jobs`).

## 2. Cloner le dépôt (chaque membre, à chaque nouveau service)

```bash
cd /home/onyxia/work
git clone https://github.com/ThomasBoures/bigdata_project.git
cd bigdata_project
# Chaque membre utilise SON identité GitHub (contributions visibles exigées) :
git config user.name  "<pseudo GitHub>"
git config user.email "<id>+<pseudo>@users.noreply.github.com"   # adresse noreply : voir github.com/settings/emails
source src/setup/onyxia_init.sh
```

Authentification pour `git push` : accepter la popup de l'extension GitHub de VS Code, ou utiliser un
Personal Access Token (github.com/settings/tokens, portée `repo`) comme mot de passe. Le binôme doit être ajouté en
collaborateur : *Settings → Collaborators* du dépôt.

## 3. Avant de fermer / supprimer le service

```bash
git add -A && git commit -m "..." && git push
aws s3 --profile default ls "s3://$BUCKET/project/" --recursive   # les données sont bien sur S3
```
