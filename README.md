# geoparquet-benchmark

Benchmark d'interrogation de fichiers (Geo)Parquet avec DuckDB, pensé pour les fichiers distants.

Pour chaque fichier, l'outil :

- décrit sa structure en lisant uniquement les métadonnées : GeoParquet, géométrie, row groups, statistiques, tri spatial ;
- exécute jusqu'à 5 requêtes, pour un ou plusieurs nombres de threads, et mesure le temps, les row groups lus, les octets et le détail des requêtes HTTP ;
- compare les fichiers entre eux, requête par requête.

```sh
uv run geoparquet_bench.py bdtopo-batiment.yaml
```

La console affiche la progression. Le rapport complet est écrit en Markdown à côté du fichier de configuration, par exemple `bdtopo-batiment-20260925-183012.md`. Il contient l'environnement, les requêtes, la structure des fichiers, les performances et le réseau. Il est horodaté, donc les benchmarks successifs ne s'écrasent pas, et il est écrit même si on interrompt le benchmark avec Ctrl-C.

## Configuration

Tout se règle dans un fichier YAML, par exemple [bdtopo-batiment.yaml](bdtopo-batiment.yaml), qui compare plusieurs hébergements de la BD TOPO. Les clés :

```yaml
sources:              # obligatoire : nom affiché → URL ou chemin local
  gpf: https://data.geopf.fr/…/batiment.parquet
  ovh: https://prd-ign-mut-platreo.s3.sbg.io.cloud.ovh.net/public/batiment.parquet
queries:              # obligatoire : nom affiché → requête, 5 au plus
  bbox: |
    SELECT count(*) FROM $src
    WHERE $bbox.xmin <= 3.4 AND $bbox.xmax >= 3.3 AND $bbox.ymin <= 47.4 AND $bbox.ymax >= 47.3
  hauteur: SELECT avg(hauteur) FROM $src
threads: [8, 16]      # un nombre ou une liste ; défaut : nombre de CPU
runs: 1               # défaut : 3 runs par requête, source et nombre de threads
warm: true            # défaut : true, un run chaud après chaque run froid
timeout: 600          # défaut : 600 s par requête SQL
init: []              # une requête ou une liste, exécutées à chaque connexion
```

Une clé inconnue est une erreur, ce qui évite qu'une faute de frappe passe inaperçue.

Une source correspond à un seul fichier Parquet (pas de glob ni de liste), et les chemins locaux sont relatifs au fichier de configuration. Une source illisible est signalée dans la section Sources du rapport.

**La durée se multiplie vite.** Chaque run exécute toutes les combinaisons source × requête × threads, avec un run froid et un run chaud. Par exemple, 4 sources × 5 requêtes × 2 nombres de threads × 2 font 80 requêtes par run.

## Requêtes

Chaque requête doit lire le fichier via `$src`. Les variables permettent d'appliquer une même requête à des fichiers dont les colonnes ou l'encodage diffèrent :

| Variable | Valeur |
|---|---|
| `$src` | `read_parquet('<url>')` (obligatoire) |
| `$geom` | la géométrie : la colonne elle-même si DuckDB la lit en GEOMETRY (type natif, ou WKB GeoParquet, converti par défaut), sinon `ST_GeomFromWKB(…)` ; `ST_GeomFromGeoArrow('<encodage>', …)` pour GeoArrow |
| `$bbox` | la colonne de couverture bbox déclarée dans les métadonnées `geo` |

Pour qu'un filtre spatial élimine des row groups :

- `$geom && <emprise>` exploite les statistiques GEOMETRY natives ; `ST_Intersects` seul ne le fait pas ;
- les prédicats sur `$bbox` exploitent les statistiques de la colonne de couverture.

La requête `geometries`, commentée dans [bdtopo-batiment.yaml](bdtopo-batiment.yaml), combine les deux.

Si une requête utilise `$geom` ou `$bbox` sur une source qui n'en a pas, seul ce couple source × requête est sauté, et il est signalé à la fin du rapport.

## init

SQL exécuté au début de chaque connexion DuckDB, avant la requête mesurée. L'outil n'applique aucun réglage de lui-même : sans `init`, DuckDB tourne avec ses valeurs par défaut.

```yaml
init:
  - SET auto_fallback_to_full_download = false          # jamais de téléchargement intégral (défaut DuckDB : true)
  - SET http_timeout = 120                              # défaut DuckDB : 30 s
  - SET validate_external_file_cache = 'NO_VALIDATION'  # garder le cache même sans ETag / Last-Modified
  - CREATE SECRET (TYPE s3, PROVIDER credential_chain)  # identifiants S3
```

Les deux premières lignes sont recommandées pour des fichiers distants volumineux. Elles figurent dans [bdtopo-batiment.yaml](bdtopo-batiment.yaml).

L'outil ajoute seulement les extensions (`httpfs`, `spatial`, et `duck_geoarrow` du dépôt community pour les fichiers GeoArrow) et son instrumentation, indispensable aux métriques :

- le profiling (`enable_profiling`), pour les row groups lus, la planification et la mémoire ;
- les logs HTTP et FileSystem (`enable_logging`), pour les GET, les octets et les durées.

Ces deux réglages ne changent pas la façon dont DuckDB lit les fichiers, mais ils ont un léger coût.

## Méthode

- **Run froid** : nouvelle base DuckDB en mémoire. Les caches (fichiers distants, métadonnées) sont donc vides.
- **Run chaud** : même requête, relancée aussitôt sur la même connexion. Il est sauté si le run froid a échoué.
- **Ordre des runs** : run, puis requête, puis source, puis threads. Pour une même requête, les sources sont donc mesurées côte à côte, ce qui répartit les variations du réseau entre elles.
- **Temps mesuré** : il va de l'exécution à la récupération complète du résultat en Arrow. La connexion (extensions, `init`) n'y est pas comptée.
- **Agrégation** : les tableaux donnent la médiane des runs, avec un tableau par requête. Le temps en gras est le meilleur de la requête, séparément pour les runs froids et chauds. La colonne `accél.` est relative au premier nombre de `threads`, et vaut `—` si celui-ci a échoué.

## Lecture du rapport

**Structure** (métadonnées seules)

| Ligne | Signification |
|---|---|
| Stats bbox par RG | row groups dont l'emprise est connue (statistiques de la colonne de couverture, sinon statistiques GEOMETRY) ; « natives » = row groups avec statistiques du type GEOMETRY Parquet |
| Emprise RG médiane | largeur × hauteur médiane des row groups |
| Recouvrement RG | somme des aires des bbox de row groups, divisée par l'aire de leur union. Vaut 1 si les row groups ne se chevauchent pas (fichier bien trié spatialement) ; plus c'est haut, moins le filtrage par bbox est efficace |

**Performances**

| Colonne | Signification |
|---|---|
| `RG lus` | row groups réellement lus, sur le total : c'est l'efficacité de l'élagage |
| `lu`, `% fichier`, `débit` | octets lus par le lecteur Parquet. En distant, c'est ce qui a été téléchargé : les lectures servies par le cache ne comptent pas |
| `planif.` | temps de planification. En distant, c'est surtout la lecture du footer : c'est le coût des métadonnées |
| `mém. pic` | pic du buffer manager DuckDB, affiché pour les runs froids uniquement (DuckDB ne le remet pas à zéro dans une connexion) |

**Réseau**

| Colonne | Signification |
|---|---|
| `p50`, `p95`, `max` | durée des GET, en ms |
| `req/s max` | nombre maximal de GET sur une fenêtre glissante d'une seconde |
| `simult. max` | nombre maximal de GET en cours au même moment |
| `timeouts` | GET au statut `INVALID` (échec de transport) |
| `répétées` | plages `Range` demandées plusieurs fois, ce qui trahit des nouvelles tentatives |

## À savoir

- DuckDB lit le proxy dans `HTTP_PROXY`. Le proxy est affiché dans l'en-tête, car il pèse lourdement sur les résultats.
- Une requête à laquelle DuckDB répond à partir des seules métadonnées (`count(*)`, parfois `min`/`max`) ne produit pas de profil. Les colonnes concernées affichent alors `—`.
- **Le cache ne sert à rien sur data.geopf.fr.** Le serveur n'envoie ni `ETag` ni `Last-Modified`, donc DuckDB ne peut pas valider son cache de fichiers distants : le run chaud retélécharge tout. Avec `SET validate_external_file_cache = 'NO_VALIDATION'` dans `init`, le run chaud tombe de 26 s à 0,2 s (0 GET) sur la requête bbox de gpf.
- **Le timeout n'est pas strict.** `timeout` interrompt la requête, mais pas une requête HTTP déjà en cours. Le message d'erreur donne donc la durée réelle.
- Le temps CPU n'est pas mesuré : pendant les lectures distantes, il suit le temps écoulé et ne veut donc rien dire.
