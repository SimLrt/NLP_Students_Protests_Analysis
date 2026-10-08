"""
Import des données GDELT (BigQuery) pour le projet NLP :
comparaison des mobilisations étudiantes actuelles en France avec des
mobilisations passées (sentiment médiatique + retour à la normale via un AR).

Pour chaque épisode, le script extrait :
  1. events_daily   : série journalière des événements de protestation en France
                      (toutes / étudiantes), avec une marge avant/après l'épisode
                      -> base pour le modèle AR (retour à la normale).
  2. events_student : détail des événements de protestation étudiante
                      (acteurs, lieu, ton, URL source).
  3. gkg_articles   : articles (GKG 2.x, >= 2015) qui évoquent manifestations +
                      étudiants/éducation en France, avec le ton (V2Tone)
                      -> base pour l'analyse de sentiment des médias.

Tables GDELT utilisées (projet public `gdelt-bq`, région US) :
  - gdelt-bq.full.events                  GDELT 1.0 (1979 -> ), pour les épisodes anciens
  - gdelt-bq.gdeltv2.events_partitioned   GDELT 2.0 (02/2015 -> ), partitionnée par jour
  - gdelt-bq.gdeltv2.gkg_partitioned      GKG 2.x  (02/2015 -> ), partitionnée par jour

Pré-requis :
  gcloud auth application-default login
  cp .env.example .env   # puis renseigner GOOGLE_CLOUD_PROJECT (projet facturé pour les requêtes)

Usage (depuis la racine du dépôt) :
  uv run --env-file .env python gdelt_import.py --dry-run     # estime le volume scanné, ne télécharge rien
  uv run --env-file .env python gdelt_import.py               # télécharge tout (cache Parquet)
  uv run --env-file .env python gdelt_import.py --episodes cpe_2006 actuel_2026 --skip-gkg
"""

from __future__ import annotations

import argparse
import logging
import os
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
from google.cloud import bigquery

# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #

PROJECT_DIR = Path(__file__).resolve().parent
RAW_DIR = PROJECT_DIR / "data" / "raw"

GDELT_V1_EVENTS = "gdelt-bq.full.events"
GDELT_V2_EVENTS = "gdelt-bq.gdeltv2.events_partitioned"
GDELT_V2_GKG = "gdelt-bq.gdeltv2.gkg_partitioned"
GDELT_V2_START = date(2015, 2, 19)  # début de GDELT 2.0 / GKG 2.x

# Codes CAMEO / filtres
PROTEST_ROOT_CODE = "14"  # EventRootCode CAMEO "PROTEST"
COUNTRY_FIPS = "FR"  # GDELT utilise les codes pays FIPS (France = FR)
STUDENT_ACTOR_CODE = "EDU"  # CAMEO : "Education" (étudiants, enseignants, universités)

# Filtres de thèmes GKG (colonne `Themes`, séparateur ';'). À affiner si besoin.
GKG_PROTEST_REGEX = r"(^|;)PROTEST(;|$)"
GKG_STUDENT_REGEX = r"(^|;)(TAX_FNCACT_STUDENTS?|EDUCATION)(;|$)"

# Garde-fou de coût : au-delà, la requête est refusée par BigQuery (pas facturée).
DEFAULT_MAX_GB = 150


@dataclass(frozen=True)
class Episode:
    """Une période de mobilisation étudiante à étudier."""

    key: str
    label: str
    start: date
    end: date

    @property
    def uses_v2(self) -> bool:
        return self.start >= GDELT_V2_START


# Dates indicatives : à ajuster selon la chronologie retenue dans le rapport.
EPISODES: dict[str, Episode] = {
    e.key: e
    for e in [
        # Couverture GDELT très faible avant les années 2000 : utile surtout comme point de comparaison.
        Episode("devaquet_1986", "Loi Devaquet", date(1986, 11, 1), date(1986, 12, 31)),
        Episode("cpe_2006", "Contrat première embauche (CPE)", date(2006, 1, 15), date(2006, 5, 15)),
        Episode("loi_travail_2016", "Loi Travail / Nuit debout", date(2016, 2, 15), date(2016, 9, 30)),
        Episode("parcoursup_2018", "Loi ORE / Parcoursup", date(2018, 2, 1), date(2018, 6, 30)),
        Episode("actuel_2026", "Mobilisation étudiante actuelle", date(2026, 9, 1), date.today()),
    ]
}

log = logging.getLogger("gdelt_import")

# --------------------------------------------------------------------------- #
# Requêtes SQL
# --------------------------------------------------------------------------- #

STUDENT_FLAG_SQL = f"""IFNULL('{STUDENT_ACTOR_CODE}' IN (
    Actor1Type1Code, Actor1Type2Code, Actor1Type3Code,
    Actor2Type1Code, Actor2Type2Code, Actor2Type3Code), FALSE)"""


def _events_source(episode: Episode) -> tuple[str, str]:
    """Retourne (table, filtre de partition) selon la version de GDELT."""
    if episode.uses_v2:
        # Partition = jour d'ajout (DATEADDED) ; +7 jours pour les événements rapportés en retard.
        partition = (
            "_PARTITIONTIME >= TIMESTAMP(@start_date) "
            "AND _PARTITIONTIME < TIMESTAMP(DATE_ADD(@end_date, INTERVAL 8 DAY))"
        )
        return GDELT_V2_EVENTS, partition
    return GDELT_V1_EVENTS, "TRUE"  # table non partitionnée


def events_daily_sql(episode: Episode) -> str:
    table, partition = _events_source(episode)
    return f"""
    WITH protests AS (
      SELECT
        SQLDATE, NumMentions, AvgTone, GoldsteinScale,
        {STUDENT_FLAG_SQL} AS is_student
      FROM `{table}`
      WHERE {partition}
        AND SQLDATE BETWEEN @start_int AND @end_int
        AND EventRootCode = '{PROTEST_ROOT_CODE}'
        AND ActionGeo_CountryCode = '{COUNTRY_FIPS}'
    )
    SELECT
      PARSE_DATE('%Y%m%d', CAST(SQLDATE AS STRING)) AS date,
      COUNT(*)                                       AS n_events,
      COUNTIF(is_student)                            AS n_events_student,
      SUM(NumMentions)                               AS n_mentions,
      SUM(IF(is_student, NumMentions, 0))            AS n_mentions_student,
      AVG(AvgTone)                                   AS avg_tone,
      AVG(IF(is_student, AvgTone, NULL))             AS avg_tone_student,
      AVG(GoldsteinScale)                            AS avg_goldstein
    FROM protests
    GROUP BY date
    ORDER BY date
    """


def events_student_sql(episode: Episode) -> str:
    table, partition = _events_source(episode)
    return f"""
    SELECT
      GLOBALEVENTID,
      PARSE_DATE('%Y%m%d', CAST(SQLDATE AS STRING)) AS date,
      Actor1Name, Actor1Type1Code, Actor2Name, Actor2Type1Code,
      EventCode, QuadClass, GoldsteinScale,
      NumMentions, NumSources, NumArticles, AvgTone,
      ActionGeo_FullName, ActionGeo_ADM1Code, ActionGeo_Lat, ActionGeo_Long,
      SOURCEURL
    FROM `{table}`
    WHERE {partition}
      AND SQLDATE BETWEEN @start_int AND @end_int
      AND EventRootCode = '{PROTEST_ROOT_CODE}'
      AND ActionGeo_CountryCode = '{COUNTRY_FIPS}'
      AND {STUDENT_FLAG_SQL}
    ORDER BY date, NumMentions DESC
    """


def gkg_articles_sql() -> str:
    # On filtre sur `Themes`/`Locations` (format V1, plus compact que V2*) pour limiter les octets scannés.
    return f"""
    SELECT
      GKGRECORDID,
      SAFE.PARSE_TIMESTAMP('%Y%m%d%H%M%S', CAST(DATE AS STRING)) AS datetime,
      SourceCommonName AS source,
      DocumentIdentifier AS url,
      IFNULL(REGEXP_EXTRACT(TranslationInfo, r'srclc:([a-z]+)'), 'eng') AS source_lang,
      SAFE_CAST(SPLIT(V2Tone, ',')[SAFE_OFFSET(0)] AS FLOAT64) AS tone,
      SAFE_CAST(SPLIT(V2Tone, ',')[SAFE_OFFSET(1)] AS FLOAT64) AS positive_score,
      SAFE_CAST(SPLIT(V2Tone, ',')[SAFE_OFFSET(2)] AS FLOAT64) AS negative_score,
      SAFE_CAST(SPLIT(V2Tone, ',')[SAFE_OFFSET(3)] AS FLOAT64) AS polarity,
      SAFE_CAST(SPLIT(V2Tone, ',')[SAFE_OFFSET(6)] AS INT64)   AS word_count,
      Themes
    FROM `{GDELT_V2_GKG}`
    WHERE _PARTITIONTIME >= TIMESTAMP(@start_date)
      AND _PARTITIONTIME < TIMESTAMP(DATE_ADD(@end_date, INTERVAL 1 DAY))
      AND Locations LIKE '%#{COUNTRY_FIPS}#%'
      AND REGEXP_CONTAINS(Themes, @protest_regex)
      AND REGEXP_CONTAINS(Themes, @student_regex)
    """


# --------------------------------------------------------------------------- #
# Exécution BigQuery
# --------------------------------------------------------------------------- #


def date_params(start: date, end: date) -> list[bigquery.ScalarQueryParameter]:
    return [
        bigquery.ScalarQueryParameter("start_date", "DATE", start),
        bigquery.ScalarQueryParameter("end_date", "DATE", end),
        bigquery.ScalarQueryParameter("start_int", "INT64", int(start.strftime("%Y%m%d"))),
        bigquery.ScalarQueryParameter("end_int", "INT64", int(end.strftime("%Y%m%d"))),
    ]


def run_query(
    client: bigquery.Client,
    sql: str,
    params: list[bigquery.ScalarQueryParameter],
    *,
    dry_run: bool,
    max_bytes: int,
) -> pd.DataFrame | None:
    """Estime le coût (dry run) puis, si autorisé, exécute la requête et renvoie un DataFrame."""
    estimate = client.query(
        sql,
        job_config=bigquery.QueryJobConfig(
            query_parameters=params, dry_run=True, use_query_cache=False
        ),
    )
    scanned = estimate.total_bytes_processed or 0
    log.info("    volume estimé : %.2f Go", scanned / 1e9)

    if dry_run:
        return None
    if scanned > max_bytes:
        log.warning(
            "    ignorée : %.2f Go > limite de %.0f Go (augmenter --max-gb si voulu)",
            scanned / 1e9,
            max_bytes / 1e9,
        )
        return None

    job = client.query(
        sql,
        job_config=bigquery.QueryJobConfig(
            query_parameters=params, maximum_bytes_billed=max_bytes
        ),
    )
    df = job.result().to_dataframe(create_bqstorage_client=True)
    log.info("    %d lignes récupérées", len(df))
    return df


def fetch_to_parquet(
    client: bigquery.Client,
    name: str,
    sql: str,
    params: list[bigquery.ScalarQueryParameter],
    out_path: Path,
    *,
    dry_run: bool,
    force: bool,
    max_bytes: int,
) -> None:
    if out_path.exists() and not force and not dry_run:
        log.info("  [%s] déjà en cache -> %s", name, out_path.relative_to(PROJECT_DIR))
        return
    log.info("  [%s]", name)
    df = run_query(client, sql, params, dry_run=dry_run, max_bytes=max_bytes)
    if df is not None:
        out_path.parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(out_path, index=False)
        log.info("    sauvegardé -> %s", out_path.relative_to(PROJECT_DIR))


def import_episode(
    client: bigquery.Client,
    episode: Episode,
    *,
    pad_days: int,
    skip_gkg: bool,
    dry_run: bool,
    force: bool,
    max_bytes: int,
) -> None:
    log.info(
        "== %s (%s -> %s, GDELT %s)",
        episode.label,
        episode.start,
        episode.end,
        "2.0" if episode.uses_v2 else "1.0",
    )
    out_dir = RAW_DIR / episode.key
    common = dict(dry_run=dry_run, force=force, max_bytes=max_bytes)

    # 1. Série journalière élargie (avant/après) pour mesurer le retour à la normale.
    padded_start = episode.start - timedelta(days=pad_days)
    padded_end = min(episode.end + timedelta(days=pad_days), date.today())
    fetch_to_parquet(
        client, "events_daily", events_daily_sql(episode),
        date_params(padded_start, padded_end), out_dir / "events_daily.parquet", **common,
    )

    # 2. Détail des événements étudiants pendant l'épisode.
    fetch_to_parquet(
        client, "events_student", events_student_sql(episode),
        date_params(episode.start, episode.end), out_dir / "events_student.parquet", **common,
    )

    # 3. Articles GKG (ton des médias), uniquement disponible à partir de 2015.
    if skip_gkg:
        return
    if not episode.uses_v2:
        log.info("  [gkg_articles] indisponible avant %s (GKG 2.x)", GDELT_V2_START)
        return
    gkg_params = date_params(episode.start, episode.end) + [
        bigquery.ScalarQueryParameter("protest_regex", "STRING", GKG_PROTEST_REGEX),
        bigquery.ScalarQueryParameter("student_regex", "STRING", GKG_STUDENT_REGEX),
    ]
    fetch_to_parquet(
        client, "gkg_articles", gkg_articles_sql(),
        gkg_params, out_dir / "gkg_articles.parquet", **common,
    )


# --------------------------------------------------------------------------- #
# CLI
# --------------------------------------------------------------------------- #


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "--project",
        default=os.environ.get("GOOGLE_CLOUD_PROJECT"),
        help="Projet GCP facturé pour les requêtes (défaut : $GOOGLE_CLOUD_PROJECT).",
    )
    parser.add_argument(
        "--episodes", nargs="+", choices=sorted(EPISODES), default=sorted(EPISODES),
        help="Épisodes à importer (défaut : tous).",
    )
    parser.add_argument("--pad-days", type=int, default=60, help="Marge (jours) autour de l'épisode pour la série journalière.")
    parser.add_argument("--skip-gkg", action="store_true", help="Ne pas interroger le GKG (le plus coûteux).")
    parser.add_argument("--dry-run", action="store_true", help="Estimer les volumes sans rien télécharger.")
    parser.add_argument("--force", action="store_true", help="Re-télécharger même si le fichier Parquet existe.")
    parser.add_argument("--max-gb", type=float, default=DEFAULT_MAX_GB, help="Volume max scanné par requête (Go).")
    return parser.parse_args()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", datefmt="%H:%M:%S")
    args = parse_args()

    client = bigquery.Client(project=args.project)
    log.info("Projet de facturation BigQuery : %s", client.project)

    for key in args.episodes:
        import_episode(
            client,
            EPISODES[key],
            pad_days=args.pad_days,
            skip_gkg=args.skip_gkg,
            dry_run=args.dry_run,
            force=args.force,
            max_bytes=int(args.max_gb * 1e9),
        )


if __name__ == "__main__":
    main()
