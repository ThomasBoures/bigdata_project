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
| Parallélisme Spark (silver) | `local[2]` = quota CPU du pod (cgroup `cpu.max`) | `local[*]` | `os.cpu_count()` = 24 (cœurs du nœud) → 24 tâches dans un pod de 2 cœurs / 2 Go : pod tué (OOM), service redémarré | dépasse la limite mémoire du pod |
| Rapport qualité silver | 1 seule agrégation (`sum(when(règle))` par règle) + 2 `groupBy` | 1 `count()` par règle (16 jobs Spark) | 16 jobs : OOM après l'écriture silver ; 1 passe : run complet en 62,2 s, mémoire conteneur médiane 1 959 Mo, cache disque compris (limite 2 000 Mo) | chaque job alloue plans + codegen ; trop lourd sur 2 Go |
| Mémoire JVM driver | heap 512m + hors heap plafonné (`SerialGC`, `TieredStopAtLevel=1`, métaspace 320m, code cache 96m, direct 128m) | heap 768m par défaut | JVM ≈ 950 Mo de RSS pour 512m de heap ; l'IDE (code-server) occupe déjà ≈ 1 Go | le heap n'est qu'une partie de la mémoire réelle |
| Format silver | Parquet snappy partitionné par `annee` (9 fichiers) | CSV brut | CSV 2,8 Mo → Parquet 1,1 Mio (÷ 2,5) ; typé, colonnes lisibles séparément | CSV non typé, relu en entier à chaque requête |

## Problèmes de qualité des données

| Problème | Exemple réel | Lignes touchées | Traitement appliqué |
|----------|--------------|-----------------|---------------------|
| Commentaires multi-lignes entre guillemets (casse une lecture ligne à ligne) | 2018-01 LE MANS → PARIS MONTPARNASSE | 351 | Spark `multiLine=true`, `escape='"'` : 12 907 lignes lues, 0 corrompue |
| Faux doublon sur (mois, gare_depart, gare_arrivee) : même liaison en service National ET International | 2025-07 PARIS LYON → MACON LOCHE | 84 (42 clés, 2025-07 à 09) | conservées : la vraie clé inclut `service` (0 doublon sur la clé à 4 colonnes) |
| Colonne recopiée : `retard_moyen_trains_retard_sup15` = `retard_moyen_tous_trains_arrivee` | valeur identique à la colonne voisine | 2 135 | valeur → NULL |
| Somme des 6 causes hors [99 ; 101] % | 2018-03 MARSEILLE ST CHARLES → TOURCOING : 0 % | 281 (dont 272 à somme nulle) | `causes_valides = false` |
| Retard moyen « tous trains » négatif | avance moyenne à l'arrivée | 278 | conservé (une avance est plausible) |
| Retards > 60 min > retards > 30 min (incohérent) | | 354 | signalé, colonne non utilisée |
| Aucun train prévu (`nb_train_prevu = 0`) | 73 lignes, toutes en 2020 (confinement) | 73 | `comptages_valides = false` (taux non défini) |
| Annulations > trains prévus | 2020-04 PARIS MONTPARNASSE → ST MALO : 0 prévu, 8 annulés | 63 | `comptages_valides = false` |
| Comptage négatif | 2025-01 LAUSANNE → PARIS LYON : `nb_train_retard_sup_30` = -4 | 43 | valeur → NULL |
| Retards > 15 min > trains en retard à l'arrivée | | 31 | `comptages_valides = false` |
| Retard moyen des trains en retard négatif | | 10 | valeur → NULL |
| Colonnes de commentaires toujours vides | `commentaire_annulation`, `commentaire_retards_depart` | 12 907 / 12 907 | colonnes supprimées en silver |

Comptes produits par Spark (`bronze_to_silver.py`, run du 2026-11-27 13:17, rapport écrit dans
`silver/_qualite/regularite_tgv/`). Lignes silver exploitables : **12 803** pour le taux > 15 min, **12 597** pour les
causes (sur 12 907). Lignes valides par année : 2018 1 554/1 560, 2019 1 469/1 476, 2020 1 385/1 476 (Covid),
2021 1 449/1 449, puis 100 % de 2022 à 2026.

## Incidents d'exécution

- Pod `vscode-pyspark` réellement limité à **2 cœurs / 2 Go** (`/sys/fs/cgroup/cpu.max`, `memory.max`) et non
  4 cœurs / 4 Go. Spark ne voit que le nœud (24 cœurs). 3 runs tués par OOM avant correctif (voir tableau des
  alternatives). Le service redémarre et `/tmp` est perdu : logs à écrire dans `~/work`.
- Image : Spark **4.3.0** (et non 4.1) → adapter la version du paquet Iceberg avant de l'utiliser.
- Temps du run silver (2 cœurs) : démarrage + lecture bronze 23,4 s ; nettoyage + écriture silver 25,2 s ;
  total 62,2 s (relecture de contrôle comprise).

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
| 02_silver_s3.png | Listing silver : 9 Parquet par année + rapport qualité, 12 objets, 1,1 MiB (namespace + heure) | S3 (aws CLI) | 2026-11-27 13:35 |
| 03_spark_silver_run.png | Trace du job Spark bronze → silver du 13:17 (namespace, Spark 4.3.0, rapport qualité, lignes valides par année, 62,2 s), affichée à 13:35 | Spark | 2026-11-27 13:35 |
