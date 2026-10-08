# Mobilisations étudiantes en France : sentiment médiatique et retour à la normale

Projet du cours de **Natural Language Processing** (M2).

Nous comparons la mobilisation étudiante actuelle en France à des mobilisations
étudiantes passées, à partir de la base [GDELT](https://www.gdeltproject.org/)
interrogée via Google BigQuery, selon deux axes :

1. **Sentiment des médias** : comment la presse (française et internationale)
   couvre-t-elle chaque mouvement ? (ton GDELT, puis analyse NLP des articles)
2. **Retour à la normale** : à quelle vitesse l'intensité de la protestation
   revient-elle à son niveau de base ? Nous modélisons la série journalière
   des événements avec un **modèle autorégressif (AR)**.

## Épisodes étudiés

| Clé                | Mouvement                         | Période (indicative)    | Source GDELT |
|--------------------|-----------------------------------|-------------------------|--------------|
| `devaquet_1986`    | Loi Devaquet                      | nov. – déc. 1986        | 1.0 (events) |
| `cpe_2006`         | Contrat première embauche (CPE)   | janv. – mai 2006        | 1.0 (events) |
| `loi_travail_2016` | Loi Travail / Nuit debout         | févr. – sept. 2016      | 2.0 + GKG    |
| `parcoursup_2018`  | Loi ORE / Parcoursup              | févr. – juin 2018       | 2.0 + GKG    |
| `actuel_2026`      | Mobilisation étudiante actuelle   | sept. 2026 – aujourd'hui| 2.0 + GKG    |

Les dates sont définies dans `EPISODES` ([`gdelt_import.py`](gdelt_import.py)).

## Données

| Table BigQuery                         | Couverture    | Usage |
|----------------------------------------|---------------|-------|
| `gdelt-bq.full.events`                 | 1979 →        | Événements des épisodes antérieurs à 2015 |
| `gdelt-bq.gdeltv2.events_partitioned`  | févr. 2015 →  | Événements (protestations, acteurs, ton) |
| `gdelt-bq.gdeltv2.gkg_partitioned`     | févr. 2015 →  | Articles : thèmes, lieux, ton (V2Tone) |

Filtres principaux : `EventRootCode = '14'` (CAMEO *PROTEST*),
`ActionGeo_CountryCode = 'FR'`, et acteur de type `EDU` pour isoler les
mobilisations étudiantes. Côté GKG, on garde les articles qui citent la France
et portent les thèmes `PROTEST` et `TAX_FNCACT_STUDENT(S)` / `EDUCATION`.

Fichiers produits pour chaque épisode, dans `data/raw/<épisode>/` (non versionnés) :

- `events_daily.parquet` : série journalière (avec ±60 jours de marge) → modèle AR
- `events_student.parquet` : détail des événements étudiants (acteurs, lieu, ton, URL)
- `gkg_articles.parquet` : articles et leur ton → analyse de sentiment

## Installation

Pré-requis : [uv](https://docs.astral.sh/uv/) et le
[SDK Google Cloud](https://cloud.google.com/sdk/docs/install) (`gcloud`).

```bash
git clone <url-du-depot>
cd <nom-du-depot>
uv sync                                   # crée .venv et installe les dépendances (uv.lock)

gcloud auth application-default login     # identifiants Google Cloud pour BigQuery
cp .env.example .env                      # puis renseigner GOOGLE_CLOUD_PROJECT
```

Le projet GCP sert uniquement à facturer les requêtes. Les données GDELT sont
publiques, et le
[bac à sable BigQuery](https://cloud.google.com/bigquery/docs/sandbox) gratuit
suffit (1 To de requêtes par mois).

## Utilisation

```bash
# 1. Estimer le volume scanné, sans rien télécharger
uv run --env-file .env python gdelt_import.py --dry-run

# 2. Télécharger les données (mises en cache en Parquet)
uv run --env-file .env python gdelt_import.py

# Options utiles
uv run --env-file .env python gdelt_import.py --episodes cpe_2006 actuel_2026
uv run --env-file .env python gdelt_import.py --skip-gkg      # sans le GKG (le plus coûteux)
uv run --env-file .env python gdelt_import.py --max-gb 300    # relever la limite de coût par requête
uv run --env-file .env python gdelt_import.py --force         # re-télécharger
```

> **Coûts :** chaque requête est d'abord estimée (dry run), puis refusée si elle
> dépasse `--max-gb` (150 Go par défaut). Les requêtes GKG sont les plus lourdes :
> lancer un `--dry-run` avant chaque nouvel import.

## Structure du dépôt

```
.
├── gdelt_import.py     # extraction GDELT via BigQuery → data/raw/
├── data/
│   └── raw/            # données téléchargées (ignorées par git)
├── pyproject.toml      # dépendances (gérées par uv)
├── uv.lock             # versions figées, pour la reproductibilité
├── .python-version
├── .env.example        # modèle de configuration (copier en .env)
└── README.md
```

## Limites connues

- GDELT commence en 1979 : **Mai 68 n'est pas couvert**. La couverture de 1986
  est très faible (peu de sources numérisées).
- Avant 2015 (GDELT 1.0), pas de GKG : ni articles détaillés ni ton par article,
  seulement le ton moyen par événement.
- Le codage CAMEO `EDU` regroupe étudiants, enseignants et institutions
  éducatives. Les thèmes GKG `EDUCATION` peuvent aussi capter des grèves
  d'enseignants.

## Feuille de route

- [x] Extraction des données GDELT (événements + GKG)
- [ ] Analyse exploratoire et nettoyage
- [ ] Analyse de sentiment des articles (ton GDELT vs modèle NLP)
- [ ] Modèle AR sur les séries journalières : estimation du retour à la normale
- [ ] Comparaison entre épisodes et rédaction du rapport

## Auteurs

- Simon — M2
