# Journal de bord (matière première du rapport de 4 pages)

À remplir au fil de l'eau : chaque section correspond à une exigence du rapport.

## Choix du dataset et de la problématique

- Dataset retenu : SNCF Voyageurs, « Régularité mensuelle TGV par liaisons » (AQST), export CSV `;` téléchargé le
  2026-11-27 (mise à jour source : 2026-10-02), sha256 `ea357724…8e5a559`.
- Pourquoi : 2,9 Mo (dans [1 ; 5] Mo), licence ODbL, données réelles et non vues en module ; 9 ans × 130 liaisons
  permettent une comparaison avant/après Covid ; nombreux défauts de qualité réels (voir plus bas).
- Schéma : 12 907 lignes, 26 colonnes. Clé = (`date` AAAA-MM, `gare_depart`, `gare_arrivee`) ; `service`
  (National 11 339 / International 1 568) ; volumes (`nb_train_prevu`, `nb_annulation`, `nb_train_retard_arrivee`,
  `nb_train_retard_sup_15/30/60`) ; retards moyens en min ; 6 parts de causes `prct_cause_*` (%) ; 3 colonnes de
  commentaires libres. 59 gares, 2018-01 → 2026-09.
- Problématique : voir README (dégradation du taux de retard > 15 min entre 2018-19 et 2024-25, cause dominante).

## Choix techniques et alternatives (avec NOS mesures)

| Étape | Choix retenu | Alternative testée | Mesure (temps, taille, ...) | Raison du rejet |
|-------|--------------|--------------------|-----------------------------|-----------------|
|       |              |                    |                             |                 |

## Problèmes de qualité des données

| Problème | Exemple réel | Lignes touchées | Traitement appliqué |
|----------|--------------|-----------------|---------------------|
| Commentaires multi-lignes entre guillemets (casse une lecture ligne à ligne) | 2018-01 LE MANS → PARIS MONTPARNASSE | 351 | Spark `multiLine=true`, `escape='"'` (à mesurer) |
| Clé dupliquée avec valeurs différentes | 2025-07 PARIS LYON → MACON LOCHE | 42 clés (84 lignes) | à décider (garder la version max `nb_train_prevu` ?) |
| Somme des 6 causes hors [99 ; 101] % | 2018-03 MARSEILLE ST CHARLES → TOURCOING : 0 % | 281 | exclure des calculs de causes |
| Annulations > trains prévus | 2020-04 PARIS MONTPARNASSE → ST MALO : 0 prévu, 8 annulés | 63 | exclure / marquer invalide |
| Valeurs négatives (comptages, retards) | 2025-01 LAUSANNE → PARIS LYON : `nb_train_retard_sup_30` = -4 | 345 valeurs (5 colonnes) | comptage négatif → NULL ; retard moyen « tous trains » négatif = avance, conservé |
| Mois sans trains prévus | 73 lignes, toutes en 2020 (confinement) | 73 | exclues (taux non défini) |
| Colonnes de commentaires vides | `commentaire_annulation`, `commentaire_retards_depart` | 12 907 / 12 907 | colonnes supprimées en silver |

Mesures faites hors Spark (Python `csv`, 2026-11-27) : à recompter en Spark sur Onyxia pour le rapport.

## Résultats et vérification indépendante

- Résultat principal :
- Vérification (autre moteur / calcul manuel) et écart constaté :

## Limites

-

## Et si le dataset était 100× plus gros ?

-

## Index des preuves (`evidence/`)

| Fichier | Contenu | Composant | Date |
|---------|---------|-----------|------|
| 01_bronze_s3.png | Ingestion : téléchargement, contrôle taille, dépôt et listing bronze (namespace + heure) | S3 (aws CLI) | 2026-11-27 10:32 |
