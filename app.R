options(encoding = "UTF-8")

# ---- packages ----
library(shiny)
library(shinydashboard)
library(shinydashboardPlus)
library(fresh)
library(shinyjs)
library(rhandsontable)
# Hinweis: rmarkdown / knitr / kableExtra / pdftools werden nicht mehr
# benoetigt. Der PDF-Export erfolgt seit Version 1.5.0 direkt ueber
# cairo_pdf (siehe render_wartungsplan_pdf), also ohne LaTeX und ohne
# .Rmd-Vorlage.
library(digest)
library(DBI)
library(RPostgres)
library(jsonlite)
library(bcrypt)
library(dplyr)
library(stringr)

options(encoding = "UTF-8")

# ---- Anwendungsversion ------------------------------------------------------
# Single source of truth for the version shown on the login page, in the
# sidebar and stamped into every audit-log row. Bump this on each release and
# add a matching entry to CHANGELOG.md.
APP_VERSION <- "1.6.1"
APP_RELEASE_DATE <- "2026-10-01"

# ---- Release-Historie -------------------------------------------------------
# Gespiegelt aus CHANGELOG.md. Wird beim Start in das Änderungsprotokoll
# geschrieben, damit dort neben den fachlichen Änderungen auch nachvollziehbar
# ist, welcher Programmstand wann produktiv ging -- vergleichbar mit der
# Commit-/Release-Historie auf GitHub.
#
# Bei einem neuen Release: hier einen Eintrag ergänzen, APP_VERSION erhöhen
# und denselben Abschnitt in CHANGELOG.md pflegen.
APP_RELEASES <- list(
  list(
    version = "1.6.1", date = "2026-10-01",
    title   = "Rubriken in der Monats\u00fcbersicht wieder sichtbar",
    changes = c(
      "Behoben: In der Monats\u00fcbersicht wurden die Rubrikzeilen (\u201eT\u00e4glich\u201c, \u201eW\u00f6chentlich (Mittwoch)\u201c, \u201eQuartalsweise\u201c \u2026) seit dem Fixieren der Aufgabenspalte wei\u00df dargestellt. Ursache war eine CSS-Regel, die die Farbe der fixierten Spalte \u00fcberschrieben hat.",
      "Behoben: Rubriken ohne hinterlegten Zeitplannamen (z. B. \u201eArbeitst\u00e4glich\u201c, \u201ePFA (SN 00398)\u201c, \u201eO neg-Konserven \u2026\u201c) wurden gar nicht als Rubrik erkannt. Die Erkennung l\u00e4uft jetzt \u00fcber die Zeilenstruktur statt \u00fcber den Text, damit jede Rubrik erfasst ist.",
      "Ge\u00e4ndert: Alle Rubrikzeilen erscheinen einheitlich in der Hausfarbe Blau statt in unterschiedlichen Kategoriefarben."
    )
  ),
  list(
    version = "1.6.0", date = "2026-09-30",
    title   = "R\u00fcckmeldungen aus dem Labor umgesetzt",
    changes = c(
      "Neu: Schaltfl\u00e4che \u201eNachtrag\u201c in der Monats\u00fcbersicht -- Eintr\u00e4ge f\u00fcr einen beliebigen Tag der letzten 120 Tage k\u00f6nnen nachgetragen werden. Jeder Nachtrag wird im \u00c4nderungsprotokoll festgehalten.",
      "Ge\u00e4ndert: In der Monats\u00fcbersicht bleiben die Spalte \u201eAufgabe\u201c und die Tages-Kopfzeile beim Scrollen stehen.",
      "Ge\u00e4ndert: Probenannahmeplatz -- \u201eZentrifugen: Temperatursensor s\u00e4ubern\u201c und \u201eBestellung Magazin (Probenannahme)\u201c sind jetzt zwei getrennte Aufgaben.",
      "Ge\u00e4ndert: Euroimmun Analyzer I -- w\u00f6chentliche Wartung jetzt dienstags (monatlich war bereits dienstags).",
      "Ge\u00e4ndert: Hydrasys -- w\u00f6chentliche und monatliche Wartung jetzt mittwochs.",
      "Ge\u00e4ndert: Phadia 250 -- w\u00f6chentliche Wartung jetzt freitags (monatlich war bereits freitags)."
    )
  ),
  list(
    version = "1.5.0", date = "2026-09-29",
    title   = "PDF-Export ohne LaTeX",
    changes = c(
      "Behoben: PDF-Export funktioniert wieder -- er ben\u00f6tigt kein LaTeX (xelatex) und keine .Rmd-Vorlage mehr.",
      "Ge\u00e4ndert: Das PDF zeigt jetzt eine Monats\u00fcbersicht -- alle Tage des Monats nebeneinander auf einer Seite (vorher eine Seite je Kalenderwoche).",
      "Ge\u00e4ndert: In erledigten Tagesfeldern steht das Namensk\u00fcrzel der Person statt eines Hakens -- im gef\u00fcllten wie im leeren Plan.",
      "Behoben: Aufgaben, die nicht auf eine Seite passen, werden auf Folgeseiten fortgesetzt statt weggelassen.",
      "Neu: Monat und Jahr sind beim leeren Wartungsplan-PDF w\u00e4hlbar.",
      "Behoben: CSV-Export wurde mit der Endung .pdf angeboten.",
      "Ge\u00e4ndert: Pakete rmarkdown, knitr, kableExtra und pdftools werden nicht mehr ben\u00f6tigt."
    )
  ),
  list(
    version = "1.4.0", date = "2026-09-22",
    title   = "Audit-Trail, neues Dashboard, Login-Redesign",
    changes = c(
      "Neu: \u00c4nderungsprotokoll (Audit-Trail) mit Filtern und CSV-Export.",
      "Neu: Dashboard mit Kachel \u201e\u00dcberf\u00e4llig von gestern\u201c und Tagesfortschritt.",
      "Neu: \u201eAls gelesen markieren\u201c f\u00fcr Hinweise -- wirkt nur f\u00fcr die jeweilige Person.",
      "Ge\u00e4ndert: Anmeldeseite in einer einheitlichen Akzentfarbe.",
      "Entfernt: Kachel \u201eGer\u00e4te erledigt\u201c (z\u00e4hlte Ger\u00e4te statt Aufgaben).",
      "Entfernt: Men\u00fcpunkt \u201eCheckliste\u201c -- Zugang \u00fcber die Ger\u00e4tekarten."
    )
  ),
  list(
    version = "1.3.0", date = "2026-09-15",
    title   = "COBAS Pro: Mittwochs-Zeitpl\u00e4ne, Hinweis nennt die Aufgabe",
    changes = c(
      "Neu: COBAS Pro I/II mit \u201e14-t\u00e4gig (Mittwoch)\u201c und \u201eMonatlich (Mittwoch, alle 4 Wochen)\u201c.",
      "Behoben: Hinweis zeigte \u201eAufgabe 18\u201c ohne Namen -- jetzt mit Ger\u00e4t, Datum und Abschnitt."
    )
  ),
  list(
    version = "1.2.0", date = "2026-09-08",
    title   = "Nur \u201eNicht erledigt\u201c erzeugt noch Hinweise",
    changes = c(
      "Ge\u00e4ndert: Dokumentationscodes (WE, FT, \u00d8, W.e., ne, D, sQ) l\u00f6sen keinen Hinweis mehr aus.",
      "Neu: Offene \u201eNE\u201c-Meldungen werden rot hervorgehoben.",
      "Neu: Ein sp\u00e4terer abweichender Eintrag schlie\u00dft die offene NE-Meldung automatisch.",
      "Ge\u00e4ndert: \u201eErledigt\u201c l\u00f6scht die Bemerkung nicht mehr, sondern schlie\u00dft sie."
    )
  )
)

# The Änderungsprotokoll uses DT for sorting/filtering when it is available.
# It is an optional dependency: if the package is missing the viewer falls
# back to a plain table instead of preventing the whole app from starting.
HAS_DT <- requireNamespace("DT", quietly = TRUE)


#----------------------- Arbeitsplatz 1 – Hämatologie--------------------------
# "g7 = Hämatologie
# g8 = Hydrasys
# g12 = Sysmex XP300 MVZ Onko Ambulanz
# g13 = Sysmex XP300 MVZ-DEL
# g14 = Sysmex XQ-320 Onko-Ambulanz, Kinderambulanz
# g15 = Sysmex XQ-320 ZL



# set German locale 
suppressWarnings(tryCatch(Sys.setlocale("LC_TIME", "de_DE.UTF-8"), error = function(e) NULL))
suppressWarnings(tryCatch(Sys.setlocale("LC_TIME", "German_Germany.1252"), error = function(e) NULL))

# Reliable German weekday/month names (independent of OS locale).
.DE_WEEKDAYS <- c(Monday="Montag", Tuesday="Dienstag", Wednesday="Mittwoch",
                  Thursday="Donnerstag", Friday="Freitag", Saturday="Samstag",
                  Sunday="Sonntag")
.DE_MONTHS <- c(January="Januar", February="Februar", March="März", April="April",
                May="Mai", June="Juni", July="Juli", August="August",
                September="September", October="Oktober", November="November",
                December="Dezember")

to_german_date_str <- function(s) {
  if (is.null(s) || is.na(s)) return(s)
  for (en in names(.DE_WEEKDAYS)) s <- gsub(en, .DE_WEEKDAYS[[en]], s, fixed = TRUE)
  for (en in names(.DE_MONTHS))   s <- gsub(en, .DE_MONTHS[[en]],   s, fixed = TRUE)
  s
}

# Format a Date as "Wochentag, dd. Monat YYYY" in German.
format_de_date <- function(d = Sys.Date(), with_weekday = TRUE) {
  d <- as.Date(d)
  fmt <- if (with_weekday) "%A, %d. %B %Y" else "%d. %B %Y"
  to_german_date_str(format(d, fmt))
}

# Format a POSIXct as "dd.mm.YYYY HH:MM" (German style, 24h).
format_de_datetime <- function(t, tz = "Europe/Berlin") {
  if (is.null(t) || (length(t) == 1 && is.na(t))) return("")
  format(as.POSIXct(t, tz = tz), "%d.%m.%Y %H:%M")
}

# First working day (Mon-Fri) of the month containing date `d`. Used as the
# canonical reminder date for monthly maintenance tasks: the team gets a clear
# heads-up at the start of the work month without losing the reminder when the
# 1st falls on a weekend.
first_workday_of_month <- function(d = Sys.Date()) {
  first <- as.Date(format(as.Date(d), "%Y-%m-01"))
  while (weekdays(first) %in% c("Saturday", "Sunday")) first <- first + 1
  first
}
is_first_workday_of_month <- function(d = Sys.Date()) {
  identical(as.Date(d), first_workday_of_month(d))
}
# First working day of the current quarter (Jan/Apr/Jul/Oct).
is_first_workday_of_quarter <- function(d = Sys.Date()) {
  d <- as.Date(d)
  m <- as.integer(format(d, "%m"))
  if (!(m %in% c(1, 4, 7, 10))) return(FALSE)
  is_first_workday_of_month(d)
}

# ---- 28-day cycle reminders ------------------------------------------------
# Some "Monatlich" / "Am ersten Dienstag im Monat" tasks use a fixed 28-day
# cadence rather than calendar-month start, so the next reminder is always
# exactly 4 weeks after the previous one. The reminder fires on the cycle's
# due date, sliding forward to the next working day if it lands on a weekend.
MONTHLY_CYCLE_ANCHOR  <- as.Date("2024-01-01")  # Monday – baseline for Monatlich
TUESDAY_CYCLE_ANCHOR  <- as.Date("2024-01-02")  # Tuesday – baseline for "Am ersten Dienstag im Monat"
FRIDAY_CYCLE_ANCHOR   <- as.Date("2024-01-05")  # Friday – baseline for "Am ersten Freitag im Monat"
# Device-specific 4-week (28-day) maintenance anchors, aligned to a real
# known due date so the cadence lands on the right weekday/phase:
#   Phadia 250 (g10): monthly maint. every 4 weeks on a FRIDAY, next 11.09.2026
#   Euroimmun Analyzer I (g2): every 4 weeks on a TUESDAY, last was 01.09.2026
PHADIA_FRIDAY_ANCHOR   <- as.Date("2026-09-11")
ANALYZER_TUESDAY_ANCHOR <- as.Date("2026-09-01")
# COBAS Pro I/II (g4, g5): the 4-week block "Monatlich (Mittwoch, alle 4
# Wochen)" is performed on a WEDNESDAY; 09.09.2026 is a confirmed due date.
COBAS_WEDNESDAY_ANCHOR <- as.Date("2026-09-09")

is_due_28day_cycle <- function(d = Sys.Date(), anchor) {
  d <- as.Date(d); anchor <- as.Date(anchor)
  if (is.na(d) || is.na(anchor) || d < anchor) return(FALSE)
  cycle_due <- anchor + (as.integer(d - anchor) %/% 28L) * 28L
  shifted <- cycle_due
  while (weekdays(shifted) %in% c("Saturday", "Sunday")) shifted <- shifted + 1L
  identical(d, shifted)
}

# Next due date in a 28-day cycle (>= today, weekend slid forward to Mon).
next_28day_due <- function(d = Sys.Date(), anchor) {
  d <- as.Date(d); anchor <- as.Date(anchor)
  if (is.na(d) || is.na(anchor)) return(NA)
  k <- if (d <= anchor) 0L else as.integer(d - anchor) %/% 28L
  repeat {
    cand <- anchor + k * 28L
    while (weekdays(cand) %in% c("Saturday", "Sunday")) cand <- cand + 1L
    if (cand >= d) return(cand)
    k <- k + 1L
  }
}

# Next Monday on or after `d`.
next_monday <- function(d = Sys.Date()) {
  d <- as.Date(d)
  off <- (1L - as.integer(format(d, "%u"))) %% 7L
  d + off
}

# Format a Date as short German "Mo, 03.06." (weekday + dd.mm.).
format_de_short <- function(d) {
  d <- as.Date(d)
  if (is.na(d)) return("")
  # Use ISO weekday number (1=Mon..7=Sun) so we don't depend on locale.
  iso <- as.integer(format(d, "%u"))
  wd_short <- c("Mo","Di","Mi","Do","Fr","Sa","So")[iso]
  if (is.na(wd_short)) wd_short <- ""
  sprintf("%s, %s", wd_short, format(d, "%d.%m."))
}

# Even ISO-week Monday (used as the bi-weekly reminder day).
is_biweekly_monday <- function(d = Sys.Date()) {
  d <- as.Date(d)
  if (weekdays(d) != "Monday") return(FALSE)
  wk <- as.integer(format(d, "%V"))
  (wk %% 2L) == 0L
}

# Bi-weekly Wednesday (COBAS Pro I/II "14-tägig (Mittwoch)"). Anchored to the
# ODD ISO weeks so that KW 37 (09.09.2026) is a due date, next 23.09.2026.
is_biweekly_wednesday <- function(d = Sys.Date()) {
  d <- as.Date(d)
  if (is.na(d) || as.integer(format(d, "%u")) != 3L) return(FALSE)
  wk <- as.integer(format(d, "%V"))
  (wk %% 2L) == 1L
}

# Next bi-weekly Wednesday on or after `d`.
next_biweekly_wednesday <- function(d = Sys.Date()) {
  d <- as.Date(d)
  if (is.na(d)) return(as.Date(NA))
  cand <- next_wednesday(d)
  if (is_biweekly_wednesday(cand)) cand else cand + 7L
}

# Most recent working day (Mon-Fri) strictly before `d`. If today is Monday,
# returns the previous Friday so the "Vortagsaufgaben" banner skips weekends
# rather than nagging about Sunday daily tasks that were never expected.
previous_workday <- function(d = Sys.Date()) {
  d <- as.Date(d) - 1L
  # Locale-independent: %u gives 1=Mon..7=Sun.
  while (as.integer(format(d, "%u")) >= 6L) d <- d - 1L
  d
}

# ---- German public holidays (Berlin / federal) ------------------------------
# Easter Sunday via Gauss/Meeus algorithm; rest derived from it. Covers the
# fixed dates and the Easter-relative dates that affect lab operations.
easter_sunday <- function(year) {
  a <- year %% 19L
  b <- year %/% 100L
  c <- year %% 100L
  d <- b %/% 4L
  e <- b %% 4L
  f <- (b + 8L) %/% 25L
  g <- (b - f + 1L) %/% 3L
  h <- (19L*a + b - d - g + 15L) %% 30L
  i <- c %/% 4L
  k <- c %% 4L
  l <- (32L + 2L*e + 2L*i - h - k) %% 7L
  m <- (a + 11L*h + 22L*l) %/% 451L
  month <- (h + l - 7L*m + 114L) %/% 31L
  day   <- ((h + l - 7L*m + 114L) %% 31L) + 1L
  as.Date(sprintf("%04d-%02d-%02d", year, month, day))
}
de_holidays_for_year <- function(year) {
  year <- as.integer(year)
  es   <- easter_sunday(year)
  c(
    as.Date(sprintf("%04d-01-01", year)),  # Neujahr
    es - 2L,                                # Karfreitag
    es + 1L,                                # Ostermontag
    as.Date(sprintf("%04d-05-01", year)),  # Tag der Arbeit
    es + 39L,                               # Christi Himmelfahrt
    es + 50L,                               # Pfingstmontag
    as.Date(sprintf("%04d-10-03", year)),  # Tag der Deutschen Einheit
    as.Date(sprintf("%04d-10-31", year)),  # Reformationstag (Niedersachsen)
    as.Date(sprintf("%04d-12-25", year)),  # 1. Weihnachtstag
    as.Date(sprintf("%04d-12-26", year))   # 2. Weihnachtstag
  )
}
is_de_holiday <- function(d) {
  d <- as.Date(d)
  if (length(d) == 0L || any(is.na(d))) return(rep(FALSE, length(d)))
  yrs <- unique(as.integer(format(d, "%Y")))
  hols <- do.call(c, lapply(yrs, de_holidays_for_year))
  d %in% hols
}
is_workday <- function(d) {
  d <- as.Date(d)
  iso <- as.integer(format(d, "%u"))
  !is.na(iso) & iso < 6L & !is_de_holiday(d)
}

# Slide a date forward to the next working day if it falls on a weekend or
# public holiday. Used so a Wednesday reminder is moved to the next Thursday
# (or further) when Wednesday is a holiday like 25.12. or 01.05.
shift_to_next_workday <- function(d) {
  d <- as.Date(d)
  while (!is_workday(d)) d <- d + 1L
  d
}

# Wednesday on or after `d` (ISO Wednesday = 3). Holiday/weekend handling is
# applied by the caller via shift_to_next_workday() so the helper itself stays
# pure.
next_wednesday <- function(d = Sys.Date()) {
  d <- as.Date(d)
  off <- (3L - as.integer(format(d, "%u"))) %% 7L
  d + off
}

# Thursday on or after `d` (ISO Thursday = 4).
next_thursday <- function(d = Sys.Date()) {
  d <- as.Date(d)
  off <- (4L - as.integer(format(d, "%u"))) %% 7L
  d + off
}

# Wednesday in the calendar month of `d`, shifted to the next workday if it
# falls on a public holiday. By default we use the FIRST Wednesday of that
# month, which is the canonical reminder day.
wednesday_of_month <- function(d, which = 1L) {
  d  <- as.Date(d)
  m1 <- as.Date(format(d, "%Y-%m-01"))
  w  <- next_wednesday(m1) + (as.integer(which) - 1L) * 7L
  shift_to_next_workday(w)
}

# ----------------------------- THEME (fresh/AdminLTE) -------------------------
DARK_BLUE <- "#003B73"
MID_TEAL  <- "#136377"
LIGHT_BG  <- "#eaeaea"

apptheme <- create_theme(
  adminlte_color(light_blue = DARK_BLUE),
  adminlte_sidebar(
    width = "250px",
    dark_bg = MID_TEAL,
    dark_hover_bg = "#0f5262",
    dark_color = "#FFFFFF"
  ),
  adminlte_global(content_bg = LIGHT_BG)
)

# ----------------------------- DB CONFIG  ----------------------------
# Zugangsdaten kommen ausschliesslich aus Umgebungsvariablen (~/.Renviron oder
# der systemd-Unit des Shiny-Servers) und stehen NIE im Quellcode -- damit
# koennen app.R und das Git-Repository gefahrlos geteilt werden.
#
# Benoetigt: DB_NAME, DB_HOST, DB_USER, DB_PASSWORD   (optional: DB_PORT)
DB_REQUIRED_VARS <- c("DB_NAME", "DB_HOST", "DB_USER", "DB_PASSWORD")

# Fail fast mit einer verstaendlichen Meldung, statt mit einem kryptischen
# libpq-Fehler abzubrechen, wenn eine Variable fehlt.
check_db_env <- function() {
  missing <- DB_REQUIRED_VARS[!nzchar(Sys.getenv(DB_REQUIRED_VARS))]
  if (length(missing))
    stop(paste0(
      "Datenbank-Zugangsdaten fehlen: ", paste(missing, collapse = ", "), ".\n",
      "Bitte in ~/.Renviron (oder in der Service-Konfiguration) setzen:\n",
      "  DB_NAME=wartungsplan\n  DB_HOST=localhost\n  DB_PORT=5433\n",
      "  DB_USER=wartungsplan_app\n  DB_PASSWORD=...\n",
      "Danach R bzw. den Shiny-Dienst neu starten."), call. = FALSE)
  invisible(TRUE)
}

# Opens a NEW database connection (connect + SET timezone).
pg_connect_new <- function() {
  check_db_env()
  con <- DBI::dbConnect(
    RPostgres::Postgres(),
    dbname   = Sys.getenv("DB_NAME"),
    host     = Sys.getenv("DB_HOST"),
    port     = as.integer(Sys.getenv("DB_PORT", "5433")),  
    user     = Sys.getenv("DB_USER"),
    password = Sys.getenv("DB_PASSWORD"),
    sslmode  = Sys.getenv("DB_SSLMODE", "disable")
  )
  
  DBI::dbExecute(con, "SET timezone = 'Europe/Berlin'")
  
  return(con)
}

# ---- Shared connection ---------------------------------------------------
# Rueckmeldung aus dem Labor: die App "haengt" beim Oeffnen eines Geraets.
# Bisher oeffnete JEDER Datenbankzugriff eine neue Verbindung (Verbindungs-
# aufbau + SET timezone) und schloss sie wieder -- beim Oeffnen eines
# Geraets mit seinen taeglichen Aufgaben rund 8-10 Mal, nach jedem Haken
# erneut. Jetzt teilen sich alle Zugriffe des R-Prozesses EINE Verbindung.
# Das ist sicher, weil R single-threaded ist: Shiny-Sitzungen laufen
# nacheinander, nie gleichzeitig auf derselben Verbindung.
#  * Vor jeder Nutzung prueft ein "SELECT 1", ob die Verbindung noch lebt
#    (DB-Neustart, Netzabbruch, abgebrochene Transaktion) -- sonst wird
#    automatisch neu verbunden.
#  * dbDisconnect() auf die geteilte Verbindung ist ein No-op (siehe unten),
#    damit die vorhandenen on.exit(dbDisconnect(con))-Aufrufe unveraendert
#    bleiben koennen.
#  * Abschalten (altes Verhalten): Umgebungsvariable DB_SHARED_CONNECTION=0.
.pg_shared <- new.env(parent = emptyenv())

pg_con <- function() {
  if (identical(Sys.getenv("DB_SHARED_CONNECTION", "1"), "0"))
    return(pg_connect_new())
  con <- .pg_shared$con
  if (!is.null(con)) {
    alive <- tryCatch({
      DBI::dbIsValid(con) && { DBI::dbGetQuery(con, "SELECT 1"); TRUE }
    }, error = function(e) FALSE)
    if (isTRUE(alive)) return(con)
    suppressWarnings(try(DBI::dbDisconnect(con), silent = TRUE))
    .pg_shared$con <- NULL
  }
  con <- pg_connect_new()
  .pg_shared$con <- con
  con
}

# Masks DBI::dbDisconnect for the unqualified dbDisconnect(con) calls in this
# app: the shared connection stays open, any other connection is closed.
dbDisconnect <- function(conn, ...) {
  if (!is.null(.pg_shared$con) && identical(conn, .pg_shared$con))
    return(invisible(TRUE))
  DBI::dbDisconnect(conn, ...)
}

# Close the shared connection when the app stops.
shiny::onStop(function() {
  if (!is.null(.pg_shared$con))
    try(DBI::dbDisconnect(.pg_shared$con), silent = TRUE)
  .pg_shared$con <- NULL
})


has_column <- function(con, table_name, column_name) {
  q <- "SELECT 1 FROM information_schema.columns
        WHERE table_schema='public' AND table_name=$1 AND column_name=$2"
  nrow(DBI::dbGetQuery(con, q, params = list(table_name, column_name))) > 0
}

ensure_schema <- function() {
  con <- pg_con()
  on.exit(dbDisconnect(con), add = TRUE)
  
  # NOTE: The block that used to sit here ran, on every restart:
  #   DELETE FROM device_tables WHERE device_id = 'gN'
  # for g1..g11, g15, g16, g17 -- "to force rebuild from template".
  #
  # This was the root cause of the "Monatsübersicht shows the wrong
  # person's initials" bug. Checkmarks live in device_cell_status keyed by
  # ROW POSITION, independently of device_tables. Wiping device_tables made
  # the next open rebuild the task structure from the hardcoded template --
  # and whenever a template had been restructured (rows added/split/moved,
  # as happened for g1 and g7), the rebuilt rows no longer lined up with the
  # old checkmarks' row positions. Result: a checkmark A genuinely made on
  # one task would re-appear, still bearing A's initials, on whatever task
  # now occupied that row number -- and shift again on every further
  # restructure. The updated_by/initials logic itself was always correct;
  # only the structure/history alignment was being broken here.
  #
  # The block is removed entirely. Structure now persists across restarts,
  # so history stays aligned with it, and admin task add/edit/delete edits
  # are no longer silently reverted. New devices seed themselves on first
  # open via create_initial_table(); genuine future template changes should
  # go through the admin task tools (which remap history safely) rather than
  # a blanket wipe.
  
  # Users
  dbExecute(con, "
    CREATE TABLE IF NOT EXISTS app_users (
      username      TEXT PRIMARY KEY,
      password_hash TEXT,
      must_reset    BOOLEAN NOT NULL DEFAULT TRUE,
      created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      last_login    TIMESTAMPTZ,
      role          TEXT NOT NULL DEFAULT 'user'
    );
  ")
  dbExecute(con, "ALTER TABLE app_users ADD COLUMN IF NOT EXISTS initials TEXT;")
  dbExecute(con, "ALTER TABLE app_users ADD COLUMN IF NOT EXISTS role TEXT NOT NULL DEFAULT 'user';")
  dbExecute(con, "ALTER TABLE app_users ADD COLUMN IF NOT EXISTS must_reset BOOLEAN NOT NULL DEFAULT TRUE;")
  dbExecute(con, "ALTER TABLE app_users ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ NOT NULL DEFAULT NOW();")
  dbExecute(con, "ALTER TABLE app_users ADD COLUMN IF NOT EXISTS last_login TIMESTAMPTZ;")
  dbExecute(con, "ALTER TABLE app_users ADD COLUMN IF NOT EXISTS security_question TEXT;")
  dbExecute(con, "ALTER TABLE app_users ADD COLUMN IF NOT EXISTS security_answer_hash TEXT;")
  try(dbExecute(con, "
    ALTER TABLE app_users
      ADD CONSTRAINT initials_format CHECK (initials IS NULL OR initials ~ '^[a-z]{2,5}$');
  "), silent = TRUE)
  
  # Devices
  dbExecute(con, "
    CREATE TABLE IF NOT EXISTS devices (
      device_id TEXT PRIMARY KEY,
      label     TEXT NOT NULL
    );
  ")
  
  # Base grids (no working cols)
  dbExecute(con, "
    CREATE TABLE IF NOT EXISTS device_tables (
      device_id  TEXT PRIMARY KEY REFERENCES devices(device_id),
      data_json  JSONB NOT NULL,
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_by TEXT
    );
  ")
  
  # Legacy row status
  dbExecute(con, "
    CREATE TABLE IF NOT EXISTS task_status (
      device_id  TEXT NOT NULL REFERENCES devices(device_id),
      row_index  INT  NOT NULL,
      done       BOOLEAN NOT NULL DEFAULT FALSE,
      comment    TEXT,
      updated_by TEXT,
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      PRIMARY KEY (device_id, row_index)
    );
  ")
  
  # Per-cell status (row x day)
  dbExecute(con, "
    CREATE TABLE IF NOT EXISTS device_cell_status (
      device_id  TEXT    NOT NULL REFERENCES devices(device_id) ON DELETE CASCADE,
      row_index  INT     NOT NULL,
      day        INT     NOT NULL CHECK (day BETWEEN 1 AND 31),
      value_text TEXT     NOT NULL,
      updated_by TEXT     NOT NULL,
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      PRIMARY KEY (device_id, row_index, day)
    );
  ")
  dbExecute(con, "CREATE INDEX IF NOT EXISTS idx_cell_status_device ON device_cell_status(device_id);")
  
  # ---- Migration: add month/year columns so historical months are preserved ----
  tryCatch({
    dbExecute(con, "ALTER TABLE device_cell_status ADD COLUMN IF NOT EXISTS month INT;")
    dbExecute(con, "ALTER TABLE device_cell_status ADD COLUMN IF NOT EXISTS year  INT;")
    # Backfill from updated_at (one-time)
    dbExecute(con, "UPDATE device_cell_status
                       SET month = EXTRACT(MONTH FROM updated_at)::int
                     WHERE month IS NULL;")
    dbExecute(con, "UPDATE device_cell_status
                       SET year = EXTRACT(YEAR FROM updated_at)::int
                     WHERE year IS NULL;")
    dbExecute(con, "ALTER TABLE device_cell_status ALTER COLUMN month SET NOT NULL;")
    dbExecute(con, "ALTER TABLE device_cell_status ALTER COLUMN year  SET NOT NULL;")
    
    # Swap PK to include month/year, if not already done
    has_new_pk <- tryCatch({
      r <- DBI::dbGetQuery(con, "
        SELECT a.attname
          FROM pg_index i
          JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
         WHERE i.indrelid = 'device_cell_status'::regclass AND i.indisprimary
      ")
      all(c("month","year") %in% r$attname)
    }, error = function(e) FALSE)
    if (!isTRUE(has_new_pk)) {
      dbExecute(con, "ALTER TABLE device_cell_status DROP CONSTRAINT IF EXISTS device_cell_status_pkey;")
      dbExecute(con, "ALTER TABLE device_cell_status
                       ADD PRIMARY KEY (device_id, row_index, day, month, year);")
    }
    dbExecute(con, "CREATE INDEX IF NOT EXISTS idx_cell_status_dev_my
                       ON device_cell_status(device_id, year, month);")
  }, error = function(e) message("device_cell_status migration: ", conditionMessage(e)))
  
  # Per-task / per-day remarks (Bemerkungen). Keyed by full date so previous
  # days' remarks can be surfaced when a user opens today's tasks.
  dbExecute(con, "
    CREATE TABLE IF NOT EXISTS device_task_remark (
      device_id   TEXT NOT NULL REFERENCES devices(device_id) ON DELETE CASCADE,
      row_index   INT  NOT NULL,
      remark_date DATE NOT NULL,
      remark_text TEXT,
      option_code TEXT,
      updated_by  TEXT NOT NULL,
      updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      PRIMARY KEY (device_id, row_index, remark_date)
    );
  ")
  dbExecute(con, "CREATE INDEX IF NOT EXISTS idx_task_remark_device_date ON device_task_remark(device_id, remark_date);")

  # Resolution state for "NE – Nicht erledigt" remarks. Only NE entries keep
  # nagging colleagues at every login; every other option code (WE, FT, Ø,
  # W.e., ne, D, sQ …) is pure documentation for the Monatsübersicht.
  # resolved_at is set as soon as somebody corrects the task to "erledigt"
  # (or explicitly closes the remark), which permanently silences the popup.
  dbExecute(con, "ALTER TABLE device_task_remark ADD COLUMN IF NOT EXISTS resolved_at TIMESTAMPTZ;")
  dbExecute(con, "ALTER TABLE device_task_remark ADD COLUMN IF NOT EXISTS resolved_by TEXT;")
  dbExecute(con, "
    CREATE INDEX IF NOT EXISTS idx_task_remark_open_ne
      ON device_task_remark(device_id, row_index)
     WHERE option_code = 'NE' AND resolved_at IS NULL;
  ")

  # Per-user "Als gelesen markieren" for open NE remarks. Acknowledging is
  # personal: it only silences the reminder for the user who clicked it, so
  # colleagues still get notified about the same open task.
  # `acked_update_at` stores the remark's updated_at at acknowledge time --
  # if someone later edits that remark (new text / re-opened), it becomes
  # newer than the stored value and the reminder resurfaces for everyone.
  dbExecute(con, "
    CREATE TABLE IF NOT EXISTS device_task_remark_ack (
      device_id       TEXT NOT NULL,
      row_index       INT  NOT NULL,
      remark_date     DATE NOT NULL,
      username        TEXT NOT NULL,
      acked_update_at TIMESTAMPTZ NOT NULL,
      acked_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      PRIMARY KEY (device_id, row_index, remark_date, username)
    );
  ")
  dbExecute(con, "CREATE INDEX IF NOT EXISTS idx_remark_ack_user
                     ON device_task_remark_ack(username, device_id);")
  
  # Per-task row-level metadata. Currently stores the "zuletzt getauscht am"
  # date for tasks that ask the user to record when a part was last replaced.
  dbExecute(con, "
    CREATE TABLE IF NOT EXISTS device_task_meta (
      device_id          TEXT NOT NULL REFERENCES devices(device_id) ON DELETE CASCADE,
      row_index          INT  NOT NULL,
      last_replaced_date DATE,
      updated_by         TEXT,
      updated_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      PRIMARY KEY (device_id, row_index)
    );
  ")
  dbExecute(con, "CREATE INDEX IF NOT EXISTS idx_task_meta_device ON device_task_meta(device_id);")
  
  # Per-device layout table
  dbExecute(con, "
    CREATE TABLE IF NOT EXISTS device_layout (
      device_id   TEXT PRIMARY KEY REFERENCES devices(device_id),
      title       TEXT,
      footer_text TEXT,
      footer_path TEXT,
      footer_mime TEXT,
      updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_by  TEXT
    );
  ")
  
  # Add columns to device_layout
  dbExecute(con, "ALTER TABLE device_layout ADD COLUMN IF NOT EXISTS version TEXT;")
  dbExecute(con, "ALTER TABLE device_layout ADD COLUMN IF NOT EXISTS valid_from DATE;")
  dbExecute(con, "ALTER TABLE device_layout ADD COLUMN IF NOT EXISTS serial_numbers JSONB;")
  
  # App-level images
  dbExecute(con, "
    CREATE TABLE IF NOT EXISTS app_images (
      id         TEXT PRIMARY KEY,
      img_path   TEXT,
      img_mime   TEXT,
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_by TEXT
    );
  ")
  
  dbExecute(con, "
    INSERT INTO app_images (id, img_path)
    VALUES ('hub_header', NULL)
    ON CONFLICT (id) DO NOTHING;
  ")
  
  # If no header logo has been set yet, default to hub-header.png (placed
  # directly in www/) so the Klinikum logo shows up without needing a
  # manual DB edit. Only fills in when empty -- never overwrites a path an
  # admin already set via the Layout tab.
  dbExecute(con, "
    UPDATE app_images
       SET img_path = 'hub-header.png'
     WHERE id = 'hub_header'
       AND (img_path IS NULL OR img_path = '');
  ")
  
  # Serial number history table
  dbExecute(con, "
    CREATE TABLE IF NOT EXISTS serial_number_history (
      id SERIAL PRIMARY KEY,
      device_id TEXT NOT NULL REFERENCES devices(device_id),
      device_name TEXT NOT NULL,
      old_serial TEXT,
      new_serial TEXT NOT NULL,
      changed_by TEXT NOT NULL,
      changed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
  ")
  dbExecute(con, "CREATE INDEX IF NOT EXISTS idx_serial_history_device ON serial_number_history(device_id);")
  dbExecute(con, "CREATE INDEX IF NOT EXISTS idx_serial_history_date ON serial_number_history(changed_at DESC);")
  
  # "Als gelesen markieren" for the open-tasks login popup: one row per day,
  # storing a fingerprint of which devices/counts were shown. As long as the
  # fingerprint doesn't change, the popup stays suppressed for the rest of
  # that day (across logins/users); a genuinely new/changed open task
  # produces a different fingerprint, so the popup reappears for that.
  dbExecute(con, "
    CREATE TABLE IF NOT EXISTS due_dialog_ack (
      ack_date    DATE PRIMARY KEY,
      fingerprint TEXT NOT NULL,
      acked_by    TEXT,
      acked_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
  ")
  
  # User feedback / issue reports about the Wartungsplan. Any user can submit;
  # only admins can view them (in the Admin "Feedback" tab). `device_id` is
  # optional context (which device the feedback is about).
  dbExecute(con, "
    CREATE TABLE IF NOT EXISTS app_feedback (
      id           SERIAL PRIMARY KEY,
      created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      created_by   TEXT,
      device_id    TEXT,
      category     TEXT,
      message      TEXT NOT NULL,
      resolved     BOOLEAN NOT NULL DEFAULT FALSE
    );
  ")
  dbExecute(con, "CREATE INDEX IF NOT EXISTS idx_feedback_date ON app_feedback(created_at DESC);")
  
  # ---- Audit-Trail / Änderungsprotokoll -------------------------------------
  # Append-only record of who changed what, when. Required for traceability in
  # an accredited lab (DIN EN ISO 15189): every entry in the Wartungsplan must
  # be attributable to a person and later corrections must remain visible.
  #
  # Design notes:
  #   * Append-only. Nothing in the app ever UPDATEs or DELETEs a row here.
  #   * `old_value` / `new_value` make a correction reconstructable: if
  #     somebody overwrites a "✓" with "NE", both states stay on record.
  #   * `entity_type` / `entity_id` keep it generic, so new features can log
  #     without a schema change (e.g. 'task' + 'g5#18@2026-09-14').
  #   * `details` is free-form JSON-ish text for anything that does not fit.
  dbExecute(con, "
    CREATE TABLE IF NOT EXISTS app_audit_log (
      id          BIGSERIAL PRIMARY KEY,
      ts          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      username    TEXT,
      user_role   TEXT,
      action      TEXT NOT NULL,
      entity_type TEXT,
      entity_id   TEXT,
      device_id   TEXT,
      task_name   TEXT,
      ref_date    DATE,
      row_index   INTEGER,
      old_value   TEXT,
      new_value   TEXT,
      details     TEXT,
      session_id  TEXT,
      client_ip   TEXT,
      app_version TEXT
    );
  ")
  # Idempotent für Installationen, die eine frühere Fassung der Tabelle haben.
  for (col in c("task_name TEXT", "ref_date DATE", "row_index INTEGER"))
    try(dbExecute(con, paste0("ALTER TABLE app_audit_log ADD COLUMN IF NOT EXISTS ", col)),
        silent = TRUE)
  dbExecute(con, "CREATE INDEX IF NOT EXISTS idx_audit_ts     ON app_audit_log(ts DESC);")
  dbExecute(con, "CREATE INDEX IF NOT EXISTS idx_audit_user   ON app_audit_log(username);")
  dbExecute(con, "CREATE INDEX IF NOT EXISTS idx_audit_device ON app_audit_log(device_id, ts DESC);")
  dbExecute(con, "CREATE INDEX IF NOT EXISTS idx_audit_action ON app_audit_log(action);")
  
  # ---- Release-Historie -----------------------------------------------------
  # Damit das Änderungsprotokoll auch die Software-Historie zeigt (vergleichbar
  # mit der Commit-Liste auf GitHub), werden die Versionen in derselben
  # Tabelle als Einträge vom Typ 'version' geführt. `ON CONFLICT DO NOTHING`
  # über einen eindeutigen Index sorgt dafür, dass jede Version bei jedem
  # Start höchstens einmal angelegt wird.
  dbExecute(con, "
    CREATE UNIQUE INDEX IF NOT EXISTS idx_audit_release_once
      ON app_audit_log (entity_id) WHERE action = 'version.veroeffentlicht';
  ")
  for (r in APP_RELEASES) {
    try(dbExecute(con, "
      INSERT INTO app_audit_log (ts, username, user_role, action, entity_type,
                                 entity_id, new_value, details, app_version)
      VALUES ($1::date, 'System', 'system', 'version.veroeffentlicht', 'version',
              $2, $3, $4, $2)
      ON CONFLICT DO NOTHING
    ", params = list(r$date, r$version, r$title,
                     paste(r$changes, collapse = "\n"))), silent = TRUE)
  }
  
  # ---- Einmalige Übernahme der bisherigen Historie --------------------------
  # Das Protokoll startet sonst leer, obwohl in `device_cell_status` und
  # `device_task_remark` bereits Monate an Einträgen liegen -- jeweils mit
  # `updated_by` und `updated_at`. Diese werden einmalig als Bestandseinträge
  # übernommen, klar als 'bestand' gekennzeichnet, damit sie nicht mit
  # lückenlos live protokollierten Änderungen verwechselt werden.
  #
  # Der Marker verhindert, dass die Übernahme bei jedem Start erneut läuft.
  already <- tryCatch(
    DBI::dbGetQuery(con, "SELECT 1 FROM app_audit_log
                           WHERE action = 'bestand.uebernommen' LIMIT 1"),
    error = function(e) data.frame())
  if (!nrow(already)) {
    try({
      dbExecute(con, "
        INSERT INTO app_audit_log (ts, username, user_role, action, entity_type,
                                   entity_id, details, app_version)
        SELECT NOW(), 'System', 'system', 'bestand.uebernommen', 'system',
               'Initiale Datenuebernahme',
               'Bestehende Eintraege und Bemerkungen wurden in das Protokoll uebernommen.',
               $1
      ", params = list(APP_VERSION))
      # Wartungseinträge
      dbExecute(con, "
        INSERT INTO app_audit_log (ts, username, user_role, action, entity_type,
                                   entity_id, device_id, ref_date, row_index,
                                   new_value, details, app_version)
        SELECT COALESCE(updated_at, NOW()), updated_by, 'user', 'eintrag.bestand',
               'wartungseintrag',
               'Zeile ' || row_index || ' | ' ||
                 to_char(make_date(year, month, day), 'DD.MM.YYYY'),
               device_id, make_date(year, month, day), row_index,
               value_text,
               'Aus dem Bestand uebernommen (vor Einfuehrung des Protokolls).',
               $1
          FROM device_cell_status
         WHERE value_text IS NOT NULL AND value_text <> ''
      ", params = list(APP_VERSION))
      # Bemerkungen
      dbExecute(con, "
        INSERT INTO app_audit_log (ts, username, user_role, action, entity_type,
                                   entity_id, device_id, ref_date, row_index,
                                   new_value, details, app_version)
        SELECT COALESCE(updated_at, NOW()), updated_by, 'user', 'bemerkung.bestand',
               'bemerkung',
               'Zeile ' || row_index || ' | ' || to_char(remark_date, 'DD.MM.YYYY'),
               device_id, remark_date, row_index,
               trim(both from COALESCE(option_code,'') || ' ' || COALESCE(remark_text,'')),
               'Aus dem Bestand uebernommen (vor Einfuehrung des Protokolls).',
               $1
          FROM device_task_remark
      ", params = list(APP_VERSION))
    }, silent = TRUE)
  }
  
  # Seed devices
  # Note: g12, g13, g14 (old Sysmex XP300/XQ-320 units) were decommissioned
  # -- their auto-rebuild lines above are already commented out and they're
  # not part of any Arbeitsplatz -- but they were never removed from this
  # seed list, so they kept being (re)created and inflating "Geräte gesamt".
  # Removed from the seed here, and actively cleaned up below.
  dbExecute(con, "
  INSERT INTO devices (device_id, label) VALUES
    ('g1',  'PFA, Cobas 411, Multiplate, MC1'),
    ('g2',  'Euroimmun Analyzer I'),
    ('g3',  'Cobas 8100'),
    ('g4',  'COBAS Pro I'),
    ('g5',  'COBAS Pro II'),
    ('g6',  'CS1'),
    ('g7',  'Hämatologie'),
    ('g8',  'Hydrasys'),                                         
    ('g9',  'Optilite'),
    ('g10', 'Phadia 250'),
    ('g11', 'ROTEM Sigma, Übersichtstabelle Kontrollen'),         
    ('g15', 'Sysmex XQ-320 ZL + Onko-Ambulanz, Kinderambulanz, DEL'),
    ('g16', 'CS2'),
    ('g17', 'Probenannahmeplatz')
  ON CONFLICT (device_id) DO UPDATE
    SET label = EXCLUDED.label;
")
  
  # Best-effort removal of the decommissioned g12/g13/g14 device rows. This
  # is wrapped in try(silent=TRUE) per device because a foreign key
  # violation (if any historical task/cell/remark data still references
  # them) would otherwise abort -- in that case the row is simply left in
  # place rather than losing that history, and "Geräte gesamt" will need a
  # manual look to fully resolve for that one device.
  for (.old_did in c("g12", "g13", "g14")) {
    try(dbExecute(con, "DELETE FROM devices WHERE device_id = $1", params = list(.old_did)),
        silent = TRUE)
  }
  
  # Seed default admin users (frwi, yaka, mabr). They keep their password if
  # it is already set; only the role is enforced to 'admin'. New seeded users
  # have must_reset = TRUE so they set their own password on first login.
  for (uname in c("frwi", "yaka", "mabr")) {
    dbExecute(con, "
      INSERT INTO app_users (username, role, must_reset, initials)
      VALUES ($1, 'admin', TRUE, $1)
      ON CONFLICT (username) DO UPDATE
        SET role = 'admin',
            initials = COALESCE(app_users.initials, EXCLUDED.initials);
    ", params = list(uname))
  }
}


# Helper functions for password reset
safe_equal <- function(a, b) {
  if (is.null(a) || is.null(b)) return(FALSE)
  identical(a, b)
}

force_reset_db <- function(con, username) {
  DBI::dbExecute(con, "UPDATE app_users SET must_reset = TRUE, password_hash = NULL WHERE username = $1", 
                 params = list(username))
}

# Helper to load device layout (includes footer text/path and timestamps)
load_device_layout <- function(con, device_id) {
  res <- DBI::dbGetQuery(con, "
    SELECT title, footer_text, footer_path, footer_mime, updated_at, updated_by,
           version, valid_from
    FROM device_layout
    WHERE device_id = $1
  ", params = list(device_id))
  if (nrow(res) == 0) {
    return(list(title=NULL, footer_text=NULL, footer_path=NULL, footer_mime=NULL,
                updated_at=NULL, updated_by=NULL, version=NULL, valid_from=NULL))
  }
  list(
    title       = res$title[1],
    footer_text = res$footer_text[1],
    footer_path = res$footer_path[1],
    footer_mime = res$footer_mime[1],
    updated_at  = res$updated_at[1],
    updated_by  = res$updated_by[1],
    version     = res$version[1],
    valid_from  = res$valid_from[1]
  )
}

# Helper to save device layout (upsert)
save_device_layout <- function(con, device_id, title=NULL, footer_text=NULL,
                               footer_path=NULL, footer_mime=NULL, who=NULL,
                               version=NULL, valid_from=NULL) {
  
  # Convert NULL to NA for database compatibility
  title <- if (is.null(title)) NA_character_ else as.character(title)
  footer_text <- if (is.null(footer_text)) NA_character_ else as.character(footer_text)
  footer_path <- if (is.null(footer_path)) NA_character_ else as.character(footer_path)
  footer_mime <- if (is.null(footer_mime)) NA_character_ else as.character(footer_mime)
  who <- if (is.null(who)) NA_character_ else as.character(who)
  version <- if (is.null(version)) NA_character_ else as.character(version)
  valid_from <- if (is.null(valid_from)) NA_character_ else as.character(valid_from)
  
  DBI::dbExecute(con, "
    INSERT INTO device_layout (device_id, title, footer_text, footer_path, footer_mime, updated_at, updated_by, version, valid_from)
    VALUES ($1,$2,$3,$4,$5,NOW(),$6,$7,$8)
    ON CONFLICT (device_id) DO UPDATE
      SET title       = COALESCE(EXCLUDED.title,       device_layout.title),
          footer_text = COALESCE(EXCLUDED.footer_text, device_layout.footer_text),
          footer_path = COALESCE(EXCLUDED.footer_path, device_layout.footer_path),
          footer_mime = COALESCE(EXCLUDED.footer_mime, device_layout.footer_mime),
          version     = COALESCE(EXCLUDED.version,     device_layout.version),
          valid_from  = COALESCE(EXCLUDED.valid_from,  device_layout.valid_from),
          updated_at  = NOW(),
          updated_by  = EXCLUDED.updated_by
  ", params = list(device_id, title, footer_text, footer_path, footer_mime, who, version, valid_from))
}

# Helper to load device serial numbers
load_device_serials <- function(con, device_id) {
  res <- DBI::dbGetQuery(con, "
    SELECT serial_numbers
    FROM device_layout
    WHERE device_id = $1
  ", params = list(device_id))
  
  if (nrow(res) == 0 || is.null(res$serial_numbers[[1]]) || is.na(res$serial_numbers[[1]])) {
    # Return default serial numbers based on device
    defaults <- list(
      "g1" = list(
        "Cobas u411" = list(label = "Cobas u411", sn = "5637", pattern = "^[0-9]{4,6}$"),
        "PFA" = list(label = "PFA", sn = "00398", pattern = "^[0-9]{5}$"),
        "MC1" = list(label = "MC1", sn = "", pattern = "^[0-9A-Z]{0,10}$"),
        "Multiplate" = list(label = "Multiplate", sn = "310071", pattern = "^[0-9]{6}$")
      ),
      "g2" = list(
        "Euroimmun Analyzer I" = list(label = "Euroimmun Analyzer I", sn = "", pattern = "^[0-9A-Z]{0,15}$")
      ),
      "g3" = list(
        "Cobas 8100" = list(label = "Cobas 8100", sn = "", pattern = "^[0-9]{4,10}$")
      ),
      "g4" = list(
        "COBAS Pro I" = list(label = "COBAS Pro I", sn = "", pattern = "^[0-9]{4,10}$")
      ),
      "g5" = list(
        "COBAS Pro II" = list(label = "COBAS Pro II", sn = "", pattern = "^[0-9]{4,10}$")
      ),
      "g6" = list(
        "CS 1" = list(label = "CS 1", sn = "", pattern = "^[0-9A-Z]{0,15}$")
      ),
      "g7" = list(
        "Hämatologie" = list(label = "Hämatologie", sn = "", pattern = "^[0-9A-Z]{0,15}$")
      ),
      "g8" = list(
        "Hydrasys" = list(label = "Hydrasys", sn = "", pattern = "^[0-9A-Z]{0,15}$")
      ),
      "g9" = list(
        "Optilite" = list(label = "Optilite", sn = "", pattern = "^[0-9A-Z]{0,15}$")
      ),
      "g10" = list(
        "Phadia 250" = list(label = "Phadia 250", sn = "", pattern = "^[0-9A-Z]{0,15}$")
      ),
      "g11" = list(
        "ROTEM Sigma" = list(label = "ROTEM Sigma, Übersichtstabelle Kontrollen", sn = "", pattern = "^[0-9A-Z]{0,15}$")
      ),
      #"g12" = list(
      #  "Sysmex XP300 MVZ Onko Ambulanz" = list(label = "Sysmex XP300 MVZ Onko Ambulanz", sn = "", pattern = "^[0-9A-Z]{0,15}$")
      #),
      #"g13" = list(
      #  "Sysmex XP300 MVZ-DEL" = list(label = "Sysmex XP300 MVZ-DEL", sn = "", pattern = "^[0-9A-Z]{0,15}$")
      #),
      #"g14" = list(
      #  "Sysmex XQ-320 Onko-Ambulanz, Kinderambulanz" = list(label = "Sysmex XQ-320 Onko-Ambulanz, Kinderambulanz", sn = "", pattern = "^[0-9A-Z]{0,15}$")
      #),
      "g15" = list(
        "Sysmex XQ-320 ZL" = list(label = "Sysmex XQ-320 ZL", sn = "", pattern = "^[0-9A-Z]{0,15}$")
      ),
      "g16" = list(
        "CS 2" = list(label = "CS 2", sn = "", pattern = "^[0-9A-Z]{0,15}$")
      ),
      "g17" = list(
        "Probenannahmeplatz" = list(label = "Probenannahmeplatz", sn = "", pattern = "^[0-9A-Z]{0,15}$")
      )
    )
    
    return(defaults[[device_id]] %||% list())
  }
  
  # Parse JSON from database
  tryCatch({
    parsed <- jsonlite::fromJSON(res$serial_numbers[[1]])
    
    # Ensure the structure is correct (nested lists with label, sn, pattern)
    if (is.list(parsed) && length(parsed) > 0) {
      return(parsed)
    } else {
      # If parsing failed or returned unexpected structure, return defaults
      defaults <- list(
        "g1" = list(
          "Cobas u411" = list(label = "Cobas u411", sn = "5637", pattern = "^[0-9]{4,6}$"),
          "PFA" = list(label = "PFA", sn = "00398", pattern = "^[0-9]{5}$"),
          "MC1" = list(label = "MC1", sn = "", pattern = "^[0-9A-Z]{0,10}$"),
          "Multiplate" = list(label = "Multiplate", sn = "310071", pattern = "^[0-9]{6}$")
        )
      )
      return(defaults[[device_id]] %||% list())
    }
  }, error = function(e) {
    warning("Error parsing serial numbers from database: ", e$message)
    # Return defaults on error
    defaults <- list(
      "g1" = list(
        "Cobas u411" = list(label = "Cobas u411", sn = "5637", pattern = "^[0-9]{4,6}$"),
        "PFA" = list(label = "PFA", sn = "00398", pattern = "^[0-9]{5}$"),
        "MC1" = list(label = "MC1", sn = "", pattern = "^[0-9A-Z]{0,10}$"),
        "Multiplate" = list(label = "Multiplate", sn = "310071", pattern = "^[0-9]{6}$")
      )
    )
    return(defaults[[device_id]] %||% list())
  })
}

# Helper to save device serial numbers
save_device_serials <- function(con, device_id, serials_list, who = NULL) {
  serials_json <- jsonlite::toJSON(serials_list, auto_unbox = TRUE)
  
  DBI::dbExecute(con, "
    INSERT INTO device_layout (device_id, serial_numbers, updated_at, updated_by)
    VALUES ($1, $2::jsonb, NOW(), $3)
    ON CONFLICT (device_id) DO UPDATE
      SET serial_numbers = EXCLUDED.serial_numbers,
          updated_at = NOW(),
          updated_by = EXCLUDED.updated_by
  ", params = list(device_id, serials_json, who))
}

# Helper to validate serial number format
validate_serial_number <- function(serial, pattern, device_name) {
  # Empty serials are allowed
  if (is.null(serial) || !nzchar(serial)) {
    return(list(valid = TRUE, message = ""))
  }
  
  # Check pattern
  if (!grepl(pattern, serial)) {
    return(list(
      valid = FALSE, 
      message = sprintf("Ungültiges Format für %s. Erlaubt: Zahlen und Buchstaben.", device_name)
    ))
  }
  
  return(list(valid = TRUE, message = ""))
}

# Helper to log serial number changes
log_serial_change <- function(con, device_id, device_name, old_serial, new_serial, who) {
  # Only log if there's actually a change
  old_serial <- old_serial %||% ""
  new_serial <- new_serial %||% ""
  
  if (old_serial != new_serial) {
    DBI::dbExecute(con, "
      INSERT INTO serial_number_history (device_id, device_name, old_serial, new_serial, changed_by, changed_at)
      VALUES ($1, $2, $3, $4, $5, NOW())
    ", params = list(device_id, device_name, old_serial, new_serial, who))
  }
}


# Helpers for app-level images (hub header)
load_app_image <- function(con, id = "hub_header") {
  res <- DBI::dbGetQuery(con, "SELECT img_path, img_mime FROM app_images WHERE id = $1", params = list(id))
  if (nrow(res) == 0) return(list(img_path = NULL, img_mime = NULL))
  list(img_path = res$img_path[1], img_mime = res$img_mime[1])
}
save_app_image <- function(con, id = "hub_header", img_path = NULL, img_mime = NULL, who = NULL) {
  DBI::dbExecute(con, "
    INSERT INTO app_images (id, img_path, img_mime, updated_at, updated_by)
    VALUES ($1, $2, $3, NOW(), $4)
    ON CONFLICT (id) DO UPDATE
      SET img_path = COALESCE(EXCLUDED.img_path, app_images.img_path),
          img_mime = COALESCE(EXCLUDED.img_mime, app_images.img_mime),
          updated_at = NOW(),
          updated_by = EXCLUDED.updated_by
  ", params = list(id, img_path, img_mime, who))
}

# =================== Helpers ===================
# Helper: compute invalid days for selected month/year
calc_invalid_days <- function(year, month) {
  # Calculate the number of days in the month
  if (month == 12) {
    next_month_start <- as.Date(sprintf("%04d-01-01", year + 1))
  } else {
    next_month_start <- as.Date(sprintf("%04d-%02d-01", year, month + 1))
  }
  current_month_start <- as.Date(sprintf("%04d-%02d-01", year, month))
  days_in_month <- as.integer(next_month_start - current_month_start)
  setdiff(1:31, 1:days_in_month)
}

# ---- Device-specific template ----

# Probenannahmeplatz (g17): Druckerstatus je Dienst als eigene tägliche Aufgabe.
G17_DRUCKER_OLD_HEADER <- "Druckerstatus aller Etikettendrucker 2X täglich kontrollieren (s. VA Etikettendrucker initialisieren)"
G17_DRUCKER_TD <- "Druckerstatus aller Etikettendrucker täglich kontrollieren (s. VA Etikettendrucker initialisieren) (Tagdienst)"
G17_DRUCKER_ND <- "Druckerstatus aller Etikettendrucker täglich kontrollieren (s. VA Etikettendrucker initialisieren) (Nachtdienst)"

build_initial_table <- function(device_id = NULL) {
  mk_row <- function(header = "", task = "") {
    tmp <- data.frame(
      Header = header,
      Task   = task,
      matrix("", nrow = 1, ncol = 31),
      stringsAsFactors = FALSE,
      check.names = FALSE
    )
    colnames(tmp) <- c("Header", "Task", as.character(1:31))
    tmp
  }
  
  # Default builder for other devices
  default_builder <- function(headers, row_counts) {
    rows <- NULL
    for (i in seq_along(headers)) {
      header <- headers[i]; count <- row_counts[i]
      rows <- rbind(rows, mk_row(header = header, task = ""))
      for (j in seq_len(count)) {
        rows <- rbind(rows, mk_row(header = "", task = ""))
      }
    }
    rows
  }
  
  #------------------Cobas u411 (SN 5637) /Schnellteste-------------------------
  
  # Custom single-column sequence for g1
  if (identical(device_id, "g1")) {
    rows <- NULL
    
    # Top header
    rows <- rbind(rows, mk_row(header = "Cobas u411 (SN 5637) /Schnellteste", task = ""))
    
    # Täglich header with its tasks (in order)
    rows <- rbind(rows, mk_row(header = "Täglich", task = ""))
    rows <- rbind(rows, mk_row(header = "", task = "Abfall entleeren"))
    rows <- rbind(rows, mk_row(header = "", task = "Kontrolle 08:00 Uhr"))
    rows <- rbind(rows, mk_row(header = "", task = "Kontrolle 18:00 Uhr"))
    
    # Montag und Donnerstag header with its task
    rows <- rbind(rows, mk_row(header = "Montag und Donnerstag", task = ""))
    rows <- rbind(rows, mk_row(header = "", task = "Reinigung und Wechsel Transporteinheit"))
    
    # Monatlich header with its task
    rows <- rbind(rows, mk_row(header = "Monatlich", task = ""))
    rows <- rbind(rows, mk_row(header = "", task = "Kalibration  (wird monatlich vom Gerät eingefordert)"))
    
    # Montag header with its task
    rows <- rbind(rows, mk_row(header = "Montag", task = ""))
    rows <- rbind(rows, mk_row(header = "", task = "Drogentest-Kontrolle positiv/negativ im Wechsel"))
    
    # PFA section
    rows <- rbind(rows, mk_row(header = "PFA (SN 00398)", task = ""))
    rows <- rbind(rows, mk_row(header = "Täglich", task = ""))
    rows <- rbind(rows, mk_row(header = "", task = "Selbsttest"))
    rows <- rbind(rows, mk_row(header = "Monatlich", task = ""))
    rows <- rbind(rows, mk_row(header = "", task = "Trigger-Lösung erneuern"))
    
    # MC1 section
    rows <- rbind(rows, mk_row(header = "MC1", task = ""))
    rows <- rbind(rows, mk_row(header = "Monatlich", task = ""))
    rows <- rbind(rows, mk_row(header = "", task = "Kontrolle N+P"))
    
    # Multiplate section
    rows <- rbind(rows, mk_row(header = "Multiplate  (SN 310071)", task = ""))
    rows <- rbind(rows, mk_row(header = "Am ersten Dienstag im Monat", task = ""))
    rows <- rbind(rows, mk_row(header = "", task = "Liquid Control Set Level 1 und 2 laufen lassen (Durchführung siehe Anleitung Multiplate)"))
    
    # Kryoglobuline section
    rows <- rbind(rows, mk_row(header = "Täglich (Ablesen zwischen 12:00 und 14:00 Uhr)", task = ""))
    rows <- rbind(rows, mk_row(header = "", task = "Kryoglobuline ablesen und dokumentieren"))
    
    # Ensure column order: Header, Task, 1..31
    rows <- rows[, c("Header", "Task", as.character(1:31)), drop = FALSE]
    return(rows)
  }
  
  #---------------Hämatologie (g7)───────────────────────────────────────────────────
  
  if (identical(device_id, "g7")) {
    rows <- NULL
    
    # ── Täglich ──────────────────────────────────────────────────────────────
    rows <- rbind(rows, mk_row(header = "Täglich", task = ""))
    rows <- rbind(rows, mk_row(header = "", task = "XN1 / XN2 Shutdown + Neustart (6:00)"))
    rows <- rbind(rows, mk_row(header = "", task = "SP50 Herunterfahren 1 + Neustart (6:00)"))
    rows <- rbind(rows, mk_row(header = "", task = "Tosoh Herunterfahren + Neustart (6:00)"))
    rows <- rbind(rows, mk_row(header = "", task = "DI-60 Neustart + Objektive putzen (6:30)"))
    rows <- rbind(rows, mk_row(header = "", task = "XN Check Level 3 (2:00)"))
    rows <- rbind(rows, mk_row(header = "", task = "XN Check Level 1 (6:15)"))
    rows <- rbind(rows, mk_row(header = "", task = "XN Check BF Level 1 (Bis 7:00)"))
    rows <- rbind(rows, mk_row(header = "", task = "XN Check Level 2 (15:00)"))
    rows <- rbind(rows, mk_row(header = "", task = "XN Check BF Level 2 (Ab 15:00)"))
    rows <- rbind(rows, mk_row(header = "", task = "Tosoh Kontrollen Level 1+2 (Bis 07:00)"))
    rows <- rbind(rows, mk_row(header = "", task = "Tosoh Kontrollen Level 1+2 (Ab 15:00)"))
    rows <- rbind(rows, mk_row(header = "", task = "DI-60 Zelllokalisation (Diff-Platz)"))
    
    # ── Wöchentlich, je nach tatsächlichem Wochentag ─────────────────────────
    # These four tasks were previously bundled under one generic "Wöchentlich"
    # header, which is only due on Mondays -- so on any other day, ALL FOUR
    # were hidden from the daily list (correctly not due-on-Monday-only tasks
    # like the Mittwoch/Donnerstag ones), and since they're also never
    # actually due on a Monday specifically, they'd never be recognized as
    # "missed" either. Splitting them into their real schedules fixes both.
    rows <- rbind(rows, mk_row(header = "Montag und Donnerstag", task = ""))
    rows <- rbind(rows, mk_row(header = "", task = "Färbereihe erneuern"))
    
    rows <- rbind(rows, mk_row(header = "Wöchentlich (Montag)", task = ""))
    rows <- rbind(rows, mk_row(header = "", task = "Bestellung Diff-Platz"))
    
    rows <- rbind(rows, mk_row(header = "Wöchentlich (Mittwoch)", task = ""))
    rows <- rbind(rows, mk_row(header = "", task = "Wöchentliche Wartung SP-50 + Straße Neustart (ab 6:00)"))
    
    rows <- rbind(rows, mk_row(header = "Wöchentlich (Donnerstag)", task = ""))
    rows <- rbind(rows, mk_row(header = "", task = "Kontrollmaterial erneuern XN+XQ alle Level"))
    
    rows <- rows[, c("Header", "Task", as.character(1:31)), drop = FALSE]
    return(rows)
  }
  
  
  #-----------------------Hydrasys (g8)──────────────────────────────────────────────
  
  if (identical(device_id, "g8")) {
    rows <- NULL
    
    # ── Arbeitstäglich ───────────────────────────────────────────────────────
    rows <- rbind(rows, mk_row(header = "Arbeitstäglich", task = ""))
    rows <- rbind(rows, mk_row(header = "", task = "Farblösungen und Reagenzien überprüfen"))
    rows <- rbind(rows, mk_row(header = "Nach jeder Migration", task = ""))
    rows <- rbind(rows, mk_row(header = "", task = "Reinigung der Migrationsfläche"))
    rows <- rbind(rows, mk_row(header = "", task = "Reinigung des Elektrodenrahmens"))
    rows <- rbind(rows, mk_row(header = "", task = "Reinigung der Antiserenmaske unter fließendem Wasser, anschließendes Spülen mit A.dest."))
    
    # ── Wöchentlich (Mittwoch) ────────────────────────────────────────────────
    # Rückmeldung: wöchentliche UND monatliche Wartung laufen hier immer mittwochs.
    rows <- rbind(rows, mk_row(header = "Wöchentlich (Mittwoch)", task = ""))
    rows <- rbind(rows, mk_row(header = "", task = "Programm TANKREINIGUNG für den Färbetank starten"))
    rows <- rbind(rows, mk_row(header = "", task = "Schlauchsystem überprüfen"))
    rows <- rbind(rows, mk_row(header = "", task = "Gerät äußerlich reinigen"))
    
    # ── Monatlich (Mittwoch, alle 4 Wochen) ───────────────────────────────────
    rows <- rbind(rows, mk_row(header = "Monatlich (Mittwoch, alle 4 Wochen)", task = ""))
    rows <- rbind(rows, mk_row(header = "", task = "Einlesen des Scan Control Film (Überprüfung der densitometrischen Auswertung)"))
    rows <- rbind(rows, mk_row(header = "", task = "Auswahl Programm → Test Pattern → Scann-Icon"))
    rows <- rbind(rows, mk_row(header = "", task = "Technikereinsatz / Wartung"))
    
    rows <- rows[, c("Header", "Task", as.character(1:31)), drop = FALSE]
    return(rows)
  }
  
  
  #-----------------------Optilite (g9)──────────────────────────────────────────
  
  if (identical(device_id, "g9")) {
    rows <- NULL
    
    # ── Arbeitstäglich ───────────────────────────────────────────────────────
    rows <- rbind(rows, mk_row(header = "Arbeitstäglich", task = ""))
    rows <- rbind(rows, mk_row(header = "", task = "Morgens: Küvettenabfall, Flüssigabfall leeren"))
    rows <- rbind(rows, mk_row(header = "", task = "Füllstand der Reagenzien prüfen"))
    rows <- rbind(rows, mk_row(header = "", task = "Systemvorbereitung"))
    rows <- rbind(rows, mk_row(header = "", task = "Abends: Tagesdaten löschen"))
    rows <- rbind(rows, mk_row(header = "", task = "\u201eWaschen vor Beenden\u201c durchführen"))
    rows <- rbind(rows, mk_row(header = "", task = "Bildschirm ausschalten (Schalter a. Rückseite)"))
    
    # ── Wöchentlich (Freitags) ──────────────────────────────────────────────
    rows <- rbind(rows, mk_row(header = "Wöchentlich (Freitag)", task = ""))
    rows <- rbind(rows, mk_row(header = "", task = "Sichern der Datenbank auf USB-Stick"))
    rows <- rbind(rows, mk_row(header = "", task = "Küvettenabfallbehälter auswaschen"))
    rows <- rbind(rows, mk_row(header = "", task = "Reagenzteller entnehmen, Kondenswasser aufwischen; Reagenzien in die Kühlzelle"))
    rows <- rbind(rows, mk_row(header = "", task = "Reinigen der Nadeln und des Mischers"))
    rows <- rbind(rows, mk_row(header = "", task = "Reinigen der Waschstationen"))
    rows <- rbind(rows, mk_row(header = "", task = "Reinigung der Luftfilter, linke und rechte Seite"))
    rows <- rbind(rows, mk_row(header = "", task = "Erst Software, dann Optilite ausschalten"))
    
    # ── Monatlich ────────────────────────────────────────────────────────────
    rows <- rbind(rows, mk_row(header = "Monatlich", task = ""))
    rows <- rbind(rows, mk_row(header = "", task = "Reinigen des Inkubators"))
    rows <- rbind(rows, mk_row(header = "", task = "Behälter für Flüssigabfall ausspülen (s. Kurzanleitung)"))
    rows <- rbind(rows, mk_row(header = "", task = "Wasserbehälter reinigen (s. Kurzanleitung)"))
    
    # ── Gelegentlich (bei Bedarf) ────────────────────────────────────────────
    rows <- rbind(rows, mk_row(header = "Bei Bedarf", task = ""))
    rows <- rbind(rows, mk_row(header = "", task = "Touchscreen reinigen"))
    rows <- rbind(rows, mk_row(header = "", task = "Segmente reinigen"))
    rows <- rbind(rows, mk_row(header = "", task = "Oberfläche abwischen"))
    
    rows <- rows[, c("Header", "Task", as.character(1:31)), drop = FALSE]
    return(rows)
  }
  
  
  #-----------------------Phadia 250 (g10)──────────────────────────────────────────
  
  if (identical(device_id, "g10")) {
    rows <- NULL
    
    # ── Arbeitstäglich ───────────────────────────────────────────────────────
    rows <- rbind(rows, mk_row(header = "Arbeitstäglich", task = ""))
    rows <- rbind(rows, mk_row(header = "", task = "Küvettenabfall, Flüssigabfall leeren, Füllstände prüfen"))
    rows <- rbind(rows, mk_row(header = "", task = "Systemvorbereitung"))
    rows <- rbind(rows, mk_row(header = "", task = "Tägliches Spülen (Lauf beenden)"))
    rows <- rbind(rows, mk_row(header = "", task = "Außenfläche bei Bedarf reinigen: Außenfläche mit trockenem Tuch, Rack-Modul mit 70% Alkohol"))
    
    # ── Wöchentlich (Freitag) ────────────────────────────────────────────────
    # Rückmeldung: wöchentliche UND monatliche Wartung laufen hier immer freitags.
    rows <- rbind(rows, mk_row(header = "Wöchentlich (Freitag)", task = ""))
    rows <- rbind(rows, mk_row(header = "", task = "Erweitertes Spülen"))
    rows <- rbind(rows, mk_row(header = "", task = "Reinigen der Wasch-, Spüllösungs- und Abfallflaschen"))
    rows <- rbind(rows, mk_row(header = "", task = "Außenfläche reinigen: siehe tägl. Wartung"))
    rows <- rbind(rows, mk_row(header = "", task = "Phadia-Prime-PC herunterfahren"))
    
    # ── Monatlich: alle 4 Wochen, immer Freitags (nächste: 11.09.) ───────────
    rows <- rbind(rows, mk_row(header = "Monatlich (Freitag, alle 4 Wochen)", task = ""))
    rows <- rbind(rows, mk_row(header = "", task = "Erweitertes monatl. Spülen mit Maintenace Solution"))
    rows <- rbind(rows, mk_row(header = "", task = "Monatl. Wartung entsprechend Anleitung im PC"))
    rows <- rbind(rows, mk_row(header = "", task = "Wash- u. Rinse-Kanister gründlich reinigen u. trocken"))
    rows <- rbind(rows, mk_row(header = "", task = "Gerät initialisieren"))
    
    rows <- rows[, c("Header", "Task", as.character(1:31)), drop = FALSE]
    return(rows)
  }
  
  
  
  #-------Sysmex XP300 MVZ Onko Ambulanz (g12) ───────────────────────────────────────
  
  if (identical(device_id, "g12")) {
    rows <- NULL
    
    # ── Täglich ──────────────────────────────────────────────────────────────
    rows <- rbind(rows, mk_row(header = "Täglich", task = ""))
    rows <- rbind(rows, mk_row(header = "", task = "Shutdown (nach der Routine)"))
    rows <- rbind(rows, mk_row(header = "", task = "Wasserfalle kontrollieren + entleeren (nach der Routine)"))
    rows <- rbind(rows, mk_row(header = "", task = "morgens Kontrollmessung Level low"))
    rows <- rbind(rows, mk_row(header = "", task = "mittags Kontrollmessung Level normal oder high"))
    rows <- rbind(rows, mk_row(header = "", task = "(wöchentlicher Wechsel)"))
    
    
    # ── Wöchentlich ───────────────────────────────────────────────────────────
    rows <- rbind(rows, mk_row(header = "Wöchentlich (Freitag)", task = ""))
    rows <- rbind(rows, mk_row(header = "", task = "Auffangschale reinigen"))
    rows <- rbind(rows, mk_row(header = "", task = "Meßwandlerkammer reinigen"))
    
    
    # ── Wöchentlich ───────────────────────────────────────────────────────────
    rows <- rbind(rows, mk_row(header = "Monatlich oder alle 2500 Proben", task = ""))
    rows <- rbind(rows, mk_row(header = "", task = "Abfallkammer reinigen"))
    
    
    # ── Alle 3 Monaten ────────────────────────────────────────────────────────
    rows <- rbind(rows, mk_row(header = "Alle 3 Monate oder alle 7500 Proben", task = ""))
    rows <- rbind(rows, mk_row(header = "", task = "Probendosierventil reinigen"))
    rows <- rbind(rows, mk_row(header = "", task = "(Jan./April/Juli/Okt.)"))
    
    # ── Wartung bei Bedarf	────────────────────────────────────────────────────
    rows <- rbind(rows, mk_row(header = "Wartung bei Bedarf", task = ""))
    rows <- rbind(rows, mk_row(header = "", task = "Kapillare der Messwandler reinigen"))
    rows <- rbind(rows, mk_row(header = "", task = "Scherventil reinigen"))
    rows <- rbind(rows, mk_row(header = "", task = "Automat. Spülung"))
    
    
    rows <- rows[, c("Header", "Task", as.character(1:31)), drop = FALSE]
    return(rows)
  }
  
  
  #------- Sysmex XP300 MVZ-DEL(g13) ───────────────────────────────────
  
  if (identical(device_id, "g13")) {
    rows <- NULL
    
    # ── Täglich ──────────────────────────────────────────────────────────────
    rows <- rbind(rows, mk_row(header = "Täglich", task = ""))
    rows <- rbind(rows, mk_row(header = "", task = "Shutdown (nach der Routine)"))
    rows <- rbind(rows, mk_row(header = "", task = "Wasserfalle kontrollieren + entleeren (nach der Routine)"))
    rows <- rbind(rows, mk_row(header = "", task = "morgens Kontrollmessung Level low"))
    rows <- rbind(rows, mk_row(header = "", task = "mittags Kontrollmessung Level normal oder high"))
    rows <- rbind(rows, mk_row(header = "", task = "(wöchentlicher Wechsel)"))
    
    
    # ── Wöchentlich ───────────────────────────────────────────────────────────
    rows <- rbind(rows, mk_row(header = "Wöchentlich (Freitag)", task = ""))
    rows <- rbind(rows, mk_row(header = "", task = "Auffangschale reinigen"))
    rows <- rbind(rows, mk_row(header = "", task = "Meßwandlerkammer reinigen"))
    
    
    # ── Wöchentlich ───────────────────────────────────────────────────────────
    rows <- rbind(rows, mk_row(header = "Monatlich oder alle 2500 Proben", task = ""))
    rows <- rbind(rows, mk_row(header = "", task = "Abfallkammer reinigen"))
    
    
    # ── Alle 3 Monaten ────────────────────────────────────────────────────────
    rows <- rbind(rows, mk_row(header = "Alle 3 Monate oder alle 7500 Proben", task = ""))
    rows <- rbind(rows, mk_row(header = "", task = "Probendosierventil reinigen"))
    rows <- rbind(rows, mk_row(header = "", task = "(Jan./April/Juli/Okt.)"))
    
    # ── Wartung bei Bedarf	────────────────────────────────────────────────────
    rows <- rbind(rows, mk_row(header = "Wartung bei Bedarf", task = ""))
    rows <- rbind(rows, mk_row(header = "", task = "Kapillare der Messwandler reinigen"))
    rows <- rbind(rows, mk_row(header = "", task = "Scherventil reinigen"))
    rows <- rbind(rows, mk_row(header = "", task = "Automat. Spülung"))
    
    
    rows <- rows[, c("Header", "Task", as.character(1:31)), drop = FALSE]
    return(rows)
  }
  
  
  #--------Sysmex XQ-320 Onko-Ambulanz, Kinderambulanz (g14)────────────────────
  
  
  if (identical(device_id, "g14")) {
    rows <- NULL
    
    # ── Täglich ──────────────────────────────────────────────────────────────
    rows <- rbind(rows, mk_row(header = "Täglich", task = ""))
    rows <- rbind(rows, mk_row(header = "", task = "07:00 – 08:00 Uhr Kontrolle LOW
und zusätzlich NORMAL/HIGH im wöchentlichen Wechsel"))
    rows <- rbind(rows, mk_row(header = "", task = "12:00 – 13:00 Uhr Kontrolle LOW"))
    rows <- rbind(rows, mk_row(header = "", task = "Nach der Routine: Shutdown (ohne CellClean)"))
    rows <- rbind(rows, mk_row(header = "", task = "Abfallkanister leeren"))
    
    
    # ── Wöchentlich ───────────────────────────────────────────────────────────
    rows <- rbind(rows, mk_row(header = "Wöchentlich (Montag)", task = ""))
    rows <- rbind(rows, mk_row(header = "", task = "Kontrollen erneuen"))
    
    # ── Wöchentlich ───────────────────────────────────────────────────────────
    rows <- rbind(rows, mk_row(header = "Monatlich (Freitag)", task = ""))
    rows <- rbind(rows, mk_row(header = "", task = "Routinereinigung vor Herunterfahren (mit CellClean)"))
    
    
    # ── Wartung bei Bedarf	────────────────────────────────────────────────────
    rows <- rbind(rows, mk_row(header = "Wartung bei Bedarf", task = ""))
    rows <- rbind(rows, mk_row(header = "", task = "Austausch des Abfallbehälters"))
    rows <- rbind(rows, mk_row(header = "", task = "Spülen der Abfallkammer"))
    rows <- rbind(rows, mk_row(header = "", task = "Automatische Spülung"))
    rows <- rbind(rows, mk_row(header = "", task = "Ablassen der Probe aus RBC-Isolationskammer"))
    rows <- rbind(rows, mk_row(header = "", task = "Entfernen einer Verstopfung im RBC-Detektor"))
    rows <- rbind(rows, mk_row(header = "", task = "Spülen der Kapillare im RBC-Detektor"))
    rows <- rbind(rows, mk_row(header = "", task = "Spülen der Durchflusszelle"))
    rows <- rbind(rows, mk_row(header = "", task = "Entfernen der Luftblasen aus der Durchflusszelle"))
    rows <- rbind(rows, mk_row(header = "", task = "Einstellung Druck/Vakuum"))
    
    
    
    
    rows <- rows[, c("Header", "Task", as.character(1:31)), drop = FALSE]
    return(rows)
  }
  
  
  #----------Sysmex XQ-320 (g15)────────────────────────────────────────────────
  
  
  if (identical(device_id, "g15")) {
    rows <- NULL
    
    # ── Täglich (ZL) ─────────────────────────────────────────────────────────
    rows <- rbind(rows, mk_row(header = "Täglich (ZL)", task = ""))
    rows <- rbind(rows, mk_row(header = "", task = "Shutdown (6:00)"))
    rows <- rbind(rows, mk_row(header = "", task = "Abfallbehälter prüfen, ggf. leeren (6:00)"))
    rows <- rbind(rows, mk_row(header = "", task = "Kontrolle low (6:15)"))
    rows <- rbind(rows, mk_row(header = "", task = "Kontrolle normal (12:00)"))
    rows <- rbind(rows, mk_row(header = "", task = "Kontrolle high (15:00)"))
    
    # MVZ Del
    rows <- rbind(rows, mk_row(header = "", task = "MVZ Del QC Validation (Tel: 04221 994 019) – Low + high/normal (7–9 Uhr)"))
    rows <- rbind(rows, mk_row(header = "", task = "MVZ Del QC Validation (Tel: 04221 994 019) – Low (12–14 Uhr)"))
    
    # Tag.Kli. Onko
    rows <- rbind(rows, mk_row(header = "", task = "Tag.Kli. Onko QC Validation (Tel: 77211) – Low + high/normal (7–9 Uhr)"))
    rows <- rbind(rows, mk_row(header = "", task = "Tag.Kli. Onko QC Validation (Tel: 77211) – Low (12–14 Uhr)"))
    
    # Kikra Ambulanz
    rows <- rbind(rows, mk_row(header = "", task = "Kikra Ambulanz QC Validation (Tel: 77728) – Low + high/normal (7–9 Uhr)"))
    rows <- rbind(rows, mk_row(header = "", task = "Kikra Ambulanz QC Validation (Tel: 77728) – Low (12–14 Uhr)"))
    
    # ── Wöchentlich (Freitag) ────────────────────────────────────────────────
    rows <- rbind(rows, mk_row(header = "Wöchentlich (Freitag)", task = ""))
    rows <- rbind(rows, mk_row(header = "", task = "Shutdown + Routinereinigung (Freitag, 6:00)"))
    
    # ── Wartung bei Bedarf (ZL) ──────────────────────────────────────────────
    rows <- rbind(rows, mk_row(header = "Bei Bedarf", task = ""))
    rows <- rbind(rows, mk_row(header = "", task = "Austausch des Abfallbehälters"))
    rows <- rbind(rows, mk_row(header = "", task = "Spülen der Abfallkammer"))
    rows <- rbind(rows, mk_row(header = "", task = "Automatische Spülung"))
    rows <- rbind(rows, mk_row(header = "", task = "Ablassen der Probe aus RBC-Isolationskammer"))
    rows <- rbind(rows, mk_row(header = "", task = "Entfernen einer Verstopfung im RBC-Detektor"))
    rows <- rbind(rows, mk_row(header = "", task = "Spülen der Kapillare im RBC-Detektor"))
    rows <- rbind(rows, mk_row(header = "", task = "Spülen der Durchflusszelle"))
    rows <- rbind(rows, mk_row(header = "", task = "Entfernen der Luftblasen aus der Durchflusszelle"))
    rows <- rbind(rows, mk_row(header = "", task = "Einstellung Druck/Vakuum"))
    
    rows <- rows[, c("Header", "Task", as.character(1:31)), drop = FALSE]
    return(rows)
  }
  
  
  #----------ROTEM Sigma (g11) ─────────────────────────────────────────────────
  
  if (identical(device_id, "g11")) {
    rows <- NULL
    
    # ── Wöchentliche Kontrollzyklen (Mo / Di / Do / Sa) ───────────────────────
    rows <- rbind(rows, mk_row(header = "Montag", task = ""))
    rows <- rbind(rows, mk_row(header = "", task = "Start > System > QC (mechanische Kontrolle)"))
    rows <- rbind(rows, mk_row(header = "", task = "Rotrol N Sigma / HEPTEM"))
    
    rows <- rbind(rows, mk_row(header = "Dienstag", task = ""))
    rows <- rbind(rows, mk_row(header = "", task = "Rotrol N Sigma / APTEM"))
    
    rows <- rbind(rows, mk_row(header = "Donnerstag", task = ""))
    rows <- rbind(rows, mk_row(header = "", task = "Rotrol P Sigma / HEPTEM"))
    
    rows <- rbind(rows, mk_row(header = "Samstag", task = ""))
    rows <- rbind(rows, mk_row(header = "", task = "Rotrol P Sigma / APTEM"))
    
    # ── Monatlich ─────────────────────────────────────────────────────────────
    rows <- rbind(rows, mk_row(header = "Monatlich", task = ""))
    rows <- rbind(rows, mk_row(header = "", task = paste(
      "Datenbackup: Messmodul beenden \u2192 Ger\u00e4teeinstellung \u2192",
      "USB Stick rechts in das Ger\u00e4t stecken \u2192 Backup \u2192",
      "USB Backup starten \u2192 ok \u2192 warten bis es fertig ist \u2192",
      "beenden \u2192 USB Stick entfernen"
    )))
    
    rows <- rows[, c("Header", "Task", as.character(1:31)), drop = FALSE]
    return(rows)
  }
  
  
  #----------CS1 (g6)────────────────────────────────────────────────
  
  if (identical(device_id, "g6")) {
    rows <- NULL
    
    # ── Täglich ─────────────────────────────────────────────────────────
    rows <- rbind(rows, mk_row(header = "Täglich", task = ""))
    rows <- rbind(rows, mk_row(header = "", task = "Herunterfahren der IPU inklusive Pipettor spülen"))
    rows <- rbind(rows, mk_row(header = "", task = "Verbrauchte Küvetten entsorgen"))
    rows <- rbind(rows, mk_row(header = "", task = "Flüssigkeitsfalle überprüfen"))
    rows <- rbind(rows, mk_row(header = "", task = "Küvetten auffüllen"))
    rows <- rbind(rows, mk_row(header = "", task = "OVB erneuern"))
    rows <- rbind(rows, mk_row(header = "", task = "Kontrollen 08:00 Uhr"))
    rows <- rbind(rows, mk_row(header = "", task = "Kontrollen 18:00 Uhr"))
    
    
    # ── Wöchentlich (Montag) ─────────────────────────────────────────────────
    rows <- rbind(rows, mk_row(header = "Wöchentlich (Montag)", task = ""))
    rows <- rbind(rows, mk_row(header = "", task = "Bestellung Verbrauchsmaterial"))
    
    # ── Wöchentlich (Mittwoch) ───────────────────────────────────────────────
    rows <- rbind(rows, mk_row(header = "Wöchentlich (Mittwoch)", task = ""))
    rows <- rbind(rows, mk_row(header = "", task = "Abfallbeutel erneuern"))
    rows <- rbind(rows, mk_row(header = "", task = "Analysator reinigen"))
    rows <- rbind(rows, mk_row(header = "", task = "Filter reinigen"))
    
    # ── Montags Bestellung ───────────────────────────────────────────────────
    rows <- rbind(rows, mk_row(header = "Wöchentlich (Montag)", task = ""))
    rows <- rbind(rows, mk_row(header = "", task = "Montags Bestellung"))
    
    # ── Monatlich ─────────────────────────────────────────────────────────
    rows <- rbind(rows, mk_row(header = "Monatlich", task = ""))
    rows <- rbind(rows, mk_row(header = "", task = "PC herunterfahren und Gerät ausschalten"))
    rows <- rbind(rows, mk_row(header = "", task = "Daten-Back-Up erstellen"))
    rows <- rbind(rows, mk_row(header = "", task = "Spülkanister spülen"))
    rows <- rbind(rows, mk_row(header = "", task = "Fotometerlampe kalibrieren/wechseln (alle 1000 Std. / ca. alle 5 Wochen \u2013 Erinnerung ca. monatlich)"))
    
    # ── Bei Bedarf ───────────────────────────────────────────────────────────
    rows <- rbind(rows, mk_row(header = "Bei Bedarf", task = ""))
    rows <- rbind(rows, mk_row(header = "", task = "Technikereinsatz / Wartung, aktuelle Chargendaten laden"))
    
    
    rows <- rows[, c("Header", "Task", as.character(1:31)), drop = FALSE]
    return(rows)
  }
  
  
  #----------CS2 (g16)────────────────────────────────────────────────
  
  if (identical(device_id, "g16")) {
    rows <- NULL
    
    # ── Täglich ─────────────────────────────────────────────────────────
    rows <- rbind(rows, mk_row(header = "Täglich", task = ""))
    rows <- rbind(rows, mk_row(header = "", task = "Herunterfahren der IPU inklusive Pipettor spülen"))
    rows <- rbind(rows, mk_row(header = "", task = "Verbrauchte Küvetten entsorgen"))
    rows <- rbind(rows, mk_row(header = "", task = "Flüssigkeitsfalle überprüfen"))
    rows <- rbind(rows, mk_row(header = "", task = "Küvetten auffüllen"))
    rows <- rbind(rows, mk_row(header = "", task = "OVB erneuern"))
    rows <- rbind(rows, mk_row(header = "", task = "Kontrollen 08:00 Uhr"))
    rows <- rbind(rows, mk_row(header = "", task = "Kontrollen 18:00 Uhr"))
    
    
    # ── Wöchentlich (Montag) ─────────────────────────────────────────────────
    rows <- rbind(rows, mk_row(header = "Wöchentlich (Montag)", task = ""))
    rows <- rbind(rows, mk_row(header = "", task = "Bestellung Verbrauchsmaterial"))
    
    # ── Wöchentlich (Donnerstag) ─────────────────────────────────────────────
    rows <- rbind(rows, mk_row(header = "Wöchentlich (Donnerstag)", task = ""))
    rows <- rbind(rows, mk_row(header = "", task = "Abfallbeutel erneuern"))
    rows <- rbind(rows, mk_row(header = "", task = "Analysator reinigen"))
    rows <- rbind(rows, mk_row(header = "", task = "Filter reinigen"))
    
    # ── Monatlich ─────────────────────────────────────────────────────────
    rows <- rbind(rows, mk_row(header = "Monatlich", task = ""))
    rows <- rbind(rows, mk_row(header = "", task = "PC herunterfahren und Gerät ausschalten"))
    rows <- rbind(rows, mk_row(header = "", task = "Daten-Back-Up erstellen"))
    rows <- rbind(rows, mk_row(header = "", task = "Spülkanister spülen"))
    
    # ── Bei Bedarf ───────────────────────────────────────────────────────────
    rows <- rbind(rows, mk_row(header = "Bei Bedarf", task = ""))
    rows <- rbind(rows, mk_row(header = "", task = "Fotometerlampe kalibrieren/wechseln & Alle 1000 Std. (ca. 5 Wochen)"))
    
    
    rows <- rows[, c("Header", "Task", as.character(1:31)), drop = FALSE]
    return(rows)
  }
  
  
  #----------Cobas 8100 (g3) ──────────────────────────────────────────────
  
  if (identical(device_id, "g3")) {
    rows <- NULL
    
    # ── Täglich ──────────────────────────────────────────────────────
    rows <- rbind(rows, mk_row(header = "Täglich", task = ""))
    rows <- rbind(rows, mk_row(header = "", task = "BCL: Kontrolle u. ggf. Auffüllen des Behälters für Aliquotröhrchen"))
    rows <- rbind(rows, mk_row(header = "", task = "AQM: Überprüfen auf Undichtigkeiten („Leak-Check“)"))
    rows <- rbind(rows, mk_row(header = "", task = "RSS: Überprüfen der Verschlussanzahl u. ggf. Auffüllen des Behälters für Verschlüsse"))
    
    # ── Wöchentlich ─────────────────────────────────────────────────
    rows <- rbind(rows, mk_row(header = "Wöchentlich", task = ""))
    rows <- rbind(rows, mk_row(header = "", task = "Cobas 8100: Herunterfahren / Daten löschen, montags (in der Nacht auf Dienstag)"))
    rows <- rbind(rows, mk_row(header = "", task = "IPB: Kontrolle und ggf. Reinigung des Röhrchen-Greiferarms"))
    rows <- rbind(rows, mk_row(header = "", task = paste(
      "ACU1: Kontrolle und ggf. Reinigung des Staubfilters /",
      "Kontrolle und ggf. Reinigung des Röhrchen-Greiferarms /",
      "Kontrolle des Ablaufschlauchs und Entwässern der Rotorkammer"
    )))
    rows <- rbind(rows, mk_row(header = "", task = paste(
      "ACU2: Kontrolle und ggf. Reinigung des Staubfilters /",
      "Kontrolle und ggf. Reinigung des Röhrchen-Greiferarms /",
      "Kontrolle des Ablaufschlauchs und Entwässern der Rotorkammer"
    )))
    rows <- rbind(rows, mk_row(header = "", task = paste(
      "SCM: Kontrolle und ggf. Reinigung des Staubfilters /",
      "Kontrolle und ggf. Reinigung des Röhrchen-Greifarms"
    )))
    rows <- rbind(rows, mk_row(header = "", task = paste(
      "AQM: Reinigen der Tropfenfänger /",
      "Reinigen der Spitzenentferner /",
      "Reinigen des Festabfallbehälters"
    )))
    rows <- rbind(rows, mk_row(header = "", task = "OBS: Kontrolle und Reinigung des Röhrchen-Greifarms"))
    rows <- rbind(rows, mk_row(header = "", task = "P501: Abfallschacht reinigen"))
    
    # ── Monatlich ────────────────────────────────────────────────────
    rows <- rbind(rows, mk_row(header = "Monatlich", task = ""))
    rows <- rbind(rows, mk_row(header = "", task = "ACU1: Kontrolle und Reinigung der Zentrifugenbecher"))
    rows <- rbind(rows, mk_row(header = "", task = "ACU2: Kontrolle und Reinigung der Zentrifugenbecher"))
    rows <- rbind(rows, mk_row(header = "", task = paste(
      "DSP: Reinigen des Drehgreifers /",
      "Reinigen des Abfallschachts /",
      "Reinigen des Festabfallbehälters"
    )))
    rows <- rbind(rows, mk_row(header = "", task = paste(
      "Sonstige Wartungsaktionen: Reinigen von Racks /",
      "Reinigen von Racktrays /",
      "Reinigen von Probentrays /",
      "Reinigen der Außenfläche des Gerätes und der Verbindungskomponenten"
    )))
    
    # ── Bei Bedarf ───────────────────────────────────────────────────
    rows <- rbind(rows, mk_row(header = "Bei Bedarf", task = ""))
    rows <- rbind(rows, mk_row(header = "", task = "P501: Stopfenabfall austauschen / Oberflächen reinigen"))
    
    rows <- rows[, c("Header", "Task", as.character(1:31)), drop = FALSE]
    return(rows)
  }
  
  
  #----------COBAS Pro I (g4) und COBAS Pro II (g5) ────────────────────────────
  # Beide Geräte teilen sich denselben Wartungsplan, Unterschiede:
  #   Pro I  : grosses Waschrack + Herunterfahren = Mittwoch frueh
  #            kleines Waschrack entfaellt mittwochs
  #            BRF-Modul 1
  #   Pro II : grosses Waschrack + Herunterfahren = Montag frueh
  #            kleines Waschrack entfaellt montags
  #            BRF-Modul 2
  
  if (identical(device_id, "g4") || identical(device_id, "g5")) {
    is_pro1   <- identical(device_id, "g4")
    big_day   <- if (is_pro1) "Mittwoch" else "Montag"
    skip_day  <- if (is_pro1) "mittwochs" else "montags"
    brf_no    <- if (is_pro1) "1" else "2"
    
    rows <- NULL
    
    # ── Täglich ──────────────────────────────────────────────────────────────
    rows <- rbind(rows, mk_row(header = "Täglich", task = ""))
    rows <- rbind(rows, mk_row(header = "", task = "Datenarchiv, Routineabschluss vom Vortag (ND, Wartungspunkt 31)"))
    rows <- rbind(rows, mk_row(header = "", task = sprintf(
      "c503 kleines grünes Waschrack starten. Entfällt %s, da grosses Waschrack läuft. (ND)",
      skip_day)))
    rows <- rbind(rows, mk_row(header = "", task = "ISE + c503 Probennadel waschen (Wartungspunkt 18, ND)"))
    
    # ── Wöchentlich ──────────────────────────────────────────────────────────
    rows <- rbind(rows, mk_row(header = "Wöchentlich", task = ""))
    rows <- rbind(rows, mk_row(header = "", task = sprintf("Herunterfahren (%s früh, ND)", big_day)))
    rows <- rbind(rows, mk_row(header = "", task = sprintf(
      "c503 grosses grünes Waschrack starten (%s früh, ND)", big_day)))
    
    # ── Wöchentlich, mittwochs (TD) ──────────────────────────────────────────
    rows <- rbind(rows, mk_row(header = "Wöchentlich (Mittwoch, TD)", task = ""))
    rows <- rbind(rows, mk_row(header = "", task = "ISE + c503 + e801 Spülstationen reinigen"))
    rows <- rbind(rows, mk_row(header = "", task = "ISE: Sipper und Vakuum-Nadel reinigen"))
    rows <- rbind(rows, mk_row(header = "", task = "c503 Reagenz- und Probennadeln mit a.d. reinigen"))
    rows <- rbind(rows, mk_row(header = "", task = "c503 Küvettenwaschnadeln optisch prüfen, nur ggf. reinigen"))
    rows <- rbind(rows, mk_row(header = "", task = "c503 Küvettendeckel optisch prüfen, nur ggf. reinigen"))
    rows <- rbind(rows, mk_row(header = "", task = "c503 Inkubationsteller optisch prüfen, nur ggf. reinigen"))
    rows <- rbind(rows, mk_row(header = "", task = "c503 Abfluss für hochkonz. Abfall optisch prüfen, ggf. reinigen"))
    rows <- rbind(rows, mk_row(header = "", task = "e801 Pro-/CleanCell Ansaugrohre nur optisch prüfen"))
    rows <- rbind(rows, mk_row(header = "", task = "e801 Probennadel mit a.d. reinigen"))
    rows <- rbind(rows, mk_row(header = "", task = "e801 Inkubationsteller optisch prüfen, nur ggf. reinigen"))
    rows <- rbind(rows, mk_row(header = "", task = "e801 Assay-Cup-Mischer optisch prüfen, nur ggf. reinigen"))
    rows <- rbind(rows, mk_row(header = "", task = "e801 Mikropartikelmischer reinigen"))
    rows <- rbind(rows, mk_row(header = "", task = "Neue Chargen von Ko/Kal installieren + alte löschen"))
    rows <- rbind(rows, mk_row(header = "", task = sprintf(
      "BRF-Modul %s: Reinigung des Röhrchen-Greiferarms", brf_no)))
    
    # ── 14-tägig (Mittwoch) ──────────────────────────────────────────────────
    rows <- rbind(rows, mk_row(header = "14-tägig (Mittwoch)", task = ""))
    rows <- rbind(rows, mk_row(header = "", task = "e801 Messzellreinigung (Dauer ca. 25 Min.)"))
    rows <- rbind(rows, mk_row(header = "", task = "Verfallsdaten von Ko/Kal/Reagenzien in der Kühlzelle überprüfen"))
    
    # ── Monatlich (Mittwoch, alle 4 Wochen) ──────────────────────────────────
    rows <- rbind(rows, mk_row(header = "Monatlich (Mittwoch, alle 4 Wochen)", task = ""))
    rows <- rbind(rows, mk_row(header = "", task = "ISE Mischkammer nur optisch prüfen"))
    rows <- rbind(rows, mk_row(header = "", task = "c503 Austausch der Fotometerlampe und der Küvetten (gemäss Wartungspunkt Nr. 45)"))
    rows <- rbind(rows, mk_row(header = "", task = "c503 Reinigen der Luftfilter (Vordertüren + Rackbereich)"))
    rows <- rbind(rows, mk_row(header = "", task = "ISE Schlauchsystem waschen"))
    rows <- rbind(rows, mk_row(header = "", task = "NaCl-Kartusche tauschen (letzte Woche des Monats)"))
    
    # ── Elektrodenaustausch (Wartungspunkt 39) ───────────────────────────────
    rows <- rbind(rows, mk_row(header = "Elektrodenaustausch (Wartungspunkt 39)", task = ""))
    rows <- rbind(rows, mk_row(header = "", task = "c503 Austausch ISE-Na+, K+, Cl- Elektrode (ca. alle 2 Monate) – zuletzt getauscht am:"))
    rows <- rbind(rows, mk_row(header = "", task = "c503 Austausch ISE-Sipper- + Quetschschlauch (alle 3 Monate) – zuletzt getauscht am:"))
    rows <- rbind(rows, mk_row(header = "", task = "c503 Austausch ISE-Referenzelektrode (alle 6 Monate) – zuletzt getauscht am:"))
    
    rows <- rows[, c("Header", "Task", as.character(1:31)), drop = FALSE]
    return(rows)
  }
  
  
  #-----------------------Euroimmun Analyzer I (g2)─────────────────────────────
  
  if (identical(device_id, "g2")) {
    rows <- NULL
    
    # ── Arbeitstäglich ───────────────────────────────────────────────────────
    rows <- rbind(rows, mk_row(header = "Arbeitstäglich", task = ""))
    rows <- rbind(rows, mk_row(header = "", task = "Füllstand der Systemflüssigkeit (Kanister unter dem Tisch) prüfen, ggf. mit A.dest auffüllen"))
    rows <- rbind(rows, mk_row(header = "", task = "Vor Analyse: Washer Check starten"))
    rows <- rbind(rows, mk_row(header = "", task = "Nach Analyse: Behälter mit Puffer durch Ersatzgefäße mit A.dest ersetzen und Rinse daily starten"))
    rows <- rbind(rows, mk_row(header = "", task = "Flüssig-Abfallkanister kontrollieren/entleeren"))
    
    # ── Wöchentlich (Dienstag) ───────────────────────────────────────────────
    # Rückmeldung: wöchentliche UND monatliche Wartung laufen auf diesem Gerät
    # immer dienstags.
    rows <- rbind(rows, mk_row(header = "Wöchentlich (Dienstag)", task = ""))
    rows <- rbind(rows, mk_row(header = "", task = "Dekontaminieren der Waschstation + Selbsttes"))
    rows <- rbind(rows, mk_row(header = "", task = "Die 4 Vorratsbehälter reinigen, mit A.dest füllen. Maintenance weekly starten"))
    rows <- rbind(rows, mk_row(header = "", task = "Spitzenabwurframpe desinfizieren"))
    rows <- rbind(rows, mk_row(header = "", task = "Abfallkanister entleeren, desinfizieren und spülen"))
    rows <- rbind(rows, mk_row(header = "", task = "Reinigen und desinfizieren der Geräteoberflächen"))
    
    # ── Monatlich: alle 4 Wochen, immer Dienstags (letzte: 01.09.) ───────────
    rows <- rbind(rows, mk_row(header = "Monatlich (Dienstag, alle 4 Wochen)", task = ""))
    rows <- rbind(rows, mk_row(header = "", task = "Vorratsbehälter m. Reinigungslösung füllen + Maintenance monthly ausführen"))
    rows <- rbind(rows, mk_row(header = "", task = "Vorratsbehälter mit A.dest ausspülen und befüllen Rinse monthly"))
    rows <- rbind(rows, mk_row(header = "", task = "Systemflüssigkeitsbehälter desinfizieren, anschließend gründlich mit A.dest spülen + mit A.dest neu befüllen"))
    rows <- rbind(rows, mk_row(header = "", task = "Pipettorspitze m. fusselfreien Tuch und Ethanol reinigen"))
    
    rows <- rows[, c("Header", "Task", as.character(1:31)), drop = FALSE]
    return(rows)
  }
  
  #---------------------Probenannahmeplatz (g17)---------------------------------------
  
  if (identical(device_id, "g17")) {
    rows <- NULL
    
    # ── Täglich ────────────────────────────────────────────────────────────
    rows <- rbind(rows, mk_row(header = "Täglich", task = ""))
    rows <- rbind(rows, mk_row(header = "", task = "Kartenleser säubern"))
    rows <- rbind(rows, mk_row(header = "", task = "2 PC'S morgens herunterfahren und wieder starten"))
    rows <- rbind(rows, mk_row(header = "", task = "Kühlschranktemperaturen dokumentieren Annahme und Magazin"))
    
    # ── Wöchentlich (Mittwoch) ─────────────────────────────────────────────
    # Rückmeldung aus dem Labor: Bestellung (Probenannahme) und Wartung der
    # Zentrifugen sind zwei getrennte Tätigkeiten und werden deshalb als zwei
    # eigenständige Punkte geführt.
    rows <- rbind(rows, mk_row(header = "Wöchentlich (Mittwoch)", task = ""))
    rows <- rbind(rows, mk_row(header = "", task = "Zentrifugen: Temperatursensor säubern"))
    rows <- rbind(rows, mk_row(header = "", task = "Bestellung Magazin (Probenannahme)"))
    
    # ── Am ersten Freitag im Monat ───────────────────────────────────────────
    rows <- rbind(rows, mk_row(header = "Am ersten Freitag im Monat", task = ""))
    rows <- rbind(rows, mk_row(header = "", task = "Zentrifugen: Reinigung und Desinfektion"))
    
    # ── Quartalsweise (Jan/April/Juli/Oktober) ──────────────────────────────
    rows <- rbind(rows, mk_row(header = "Quartalsweise", task = ""))
    rows <- rbind(rows, mk_row(header = "", task = "Zentrifugen: Tragbolzen fetten"))
    
    # ── Druckerstatus: 2x täglich, je Dienst eine eigene Aufgabe ────────────
    # Rückmeldung aus dem Labor: der Druckerstatus wird je Dienst (Tagdienst /
    # Nachtdienst) einzeln abgehakt. Gleiche Zeilenpositionen wie der frühere
    # Hinweisblock, damit bestehende Haken und Bemerkungen erhalten bleiben
    # (Umbenennung gespeicherter Tabellen in load_device_table()).
    rows <- rbind(rows, mk_row(header = "Täglich", task = ""))
    rows <- rbind(rows, mk_row(header = "", task = G17_DRUCKER_TD))
    rows <- rbind(rows, mk_row(header = "", task = G17_DRUCKER_ND))
    
    # ── Always-visible notes (not tied to a due-date schedule) ──────────────
    rows <- rbind(rows, mk_row(header = "O neg-Konserven werden 14 Tage vor dem Verfall an das DRK zurückgeschickt. Datum bitte markieren.", task = ""))
    rows <- rbind(rows, mk_row(header = "", task = "Datum markieren"))
    
    rows <- rows[, c("Header", "Task", as.character(1:31)), drop = FALSE]
    return(rows)
  }
  
  default_builder(c("Täglich", "Wöchentlich", "14-tägig", "Monatlich", "Bei Bedarf"),
                  c(3, 13, 3, 5, 4))
}

# Templates are fixed code, so each device's template is built once per R
# process and then reused. load_device_table() and the dashboard call this
# for every device on every load; building one takes 6-35 ms (many rbind()
# calls), which added up noticeably. Data frames are copied on modification
# in R, so callers changing the returned table never touch the cache.
.template_cache <- new.env(parent = emptyenv())
create_initial_table <- function(device_id = NULL) {
  key <- if (is.null(device_id)) ".default" else as.character(device_id)
  if (!exists(key, envir = .template_cache, inherits = FALSE))
    assign(key, build_initial_table(device_id), envir = .template_cache)
  get(key, envir = .template_cache, inherits = FALSE)
}

# ---- Utilities ----
`%||%` <- function(a, b) if (is.null(a) || length(a) == 0 || (length(a) == 1 && is.na(a))) b else a

order_cols <- function(df) {
  day_cols <- intersect(as.character(1:31), names(df))
  front    <- intersect(c("Header", "Task", "Done", "Kommentar", "Benutzer"), names(df))
  other    <- setdiff(names(df), c(front, day_cols))
  df[, c(front, day_cols, other), drop = FALSE]
}

# Define task option tokens globally so all server functions can use them
TASK_OPTIONS <- c("WE (Wochenende)", "FT (Feiertag)", "Ø (An diesem Tag wurden keine Analysen gestartet)", 
                  "W.e. (Wartungspunkt ist in einer größeren Wartung enthalten)", "ne (Nicht erforderlich (für die Rubrik „ bei Bedarf“))", 
                  "D (Gerät / Modul defekt)", "NE (Nicht erledigt – bitte Bemerkung eintragen)",
                  "sB (Siehe Bemerkungen)", "sQ (Siehe Quasi)")

# Task-row flow helpers (Rueckmeldung: Ablauf erledigt / nicht erledigt war
# unklar). The reason dropdown only appears after "Nicht erledigt" and starts
# with an explicit prompt; the comment field's placeholder depends on the
# state, so users see whether they comment the completion or the reason.
TASK_REASON_PROMPT <- "Bitte auswählen, warum die Aufgabe nicht erledigt ist ..."
task_reason_choices <- function() c(setNames("", TASK_REASON_PROMPT), TASK_OPTIONS)
task_state_class <- function(is_done, selected_opt) {
  if (isTRUE(is_done)) "state-done"
  else if (nzchar(selected_opt %||% "")) "state-notdone"
  else "state-open"
}
task_remark_placeholder <- function(state) {
  switch(state,
         "state-done"    = "Kommentar zur Erledigung (optional) ...",
         "state-notdone" = "Bemerkung zum Grund (bei NE bitte ausfüllen) ...",
         "Bemerkung ...")
}

# Abbreviation code -> full German meaning (word form). Single source of
# truth used by the checklist legend and by any place that shows an option
# code, so users see e.g. "Nicht erledigt" instead of just "NE".
OPTION_MEANINGS <- c(
  "WE"   = "Wochenende",
  "FT"   = "Feiertag",
  "Ø"    = "An diesem Tag wurden keine Analysen gestartet",
  "W.e." = "Wartungspunkt ist in einer größeren Wartung enthalten",
  "ne"   = "Nicht erforderlich (für die Rubrik „bei Bedarf“)",
  "D"    = "Gerät / Modul defekt",
  "NE"   = "Nicht erledigt",
  "sB"   = "Siehe Bemerkungen",
  "sQ"   = "Siehe Quasi"
)

# Given a stored option code (e.g. "NE", "WE"), return "NE – Nicht erledigt".
# Falls back to the raw code if unknown.
option_full_label <- function(code) {
  code <- trimws(as.character(code %||% ""))
  if (!nzchar(code)) return("")
  meaning <- OPTION_MEANINGS[[code]]
  if (is.null(meaning) || is.na(meaning)) code else paste0(code, " \u2013 ", meaning)
}


# ---- Base grid JSON (load/save) ----
load_device_table <- function(con, device_id) {
  # Try to read stored JSON grid
  res <- DBI::dbGetQuery(
    con,
    "SELECT data_json FROM device_tables WHERE device_id = $1",
    params = list(device_id)
  )
  if (nrow(res) == 0) {
    # no saved table -> special-case g1: create & return template; otherwise return NULL
    if (identical(device_id, "g1")) {
      df_template <- create_initial_table("g1")
      # persist template so future loads pick it up
      save_device_table(con, "g1", df_template, "<system>")
      return(df_template)
    }
    return(NULL)
  }
  df <- as.data.frame(jsonlite::fromJSON(res$data_json[[1]]),
                      stringsAsFactors = FALSE, check.names = FALSE)
  bad <- intersect(c("Done", "Kommentar", "Benutzer"), names(df))
  if (length(bad)) df <- df[, setdiff(names(df), bad), drop = FALSE]
  
  # Ensure the g1 table is up-to-date with the current template.
  # GUARD: only attempt structural migration when the stored row count
  # differs from the template's. Once the structure matches, this block is
  # skipped entirely on every future load -- so it can never re-shuffle rows
  # underneath existing checkmark history (which is keyed by row position).
  if (identical(device_id, "g1") && nrow(df) != nrow(create_initial_table("g1"))) {
    df_template <- create_initial_table("g1")
    n_existing  <- nrow(df)
    n_template  <- nrow(df_template)
    needs_save  <- FALSE
    df_before   <- df   # to skip the DB write when nothing actually changed
    
    # Only inject task texts when NO data row has any task text yet (first-time migration)
    data_rows     <- which(df$Header == "")
    has_task_text <- length(data_rows) > 0 && any(nzchar(df$Task[data_rows]))
    if (!has_task_text) {
      n_min <- min(n_existing, n_template)
      for (i in seq_len(n_min)) {
        if (df$Header[i] == "" && nzchar(df_template$Task[i]) && !nzchar(df$Task[i])) {
          df$Task[i] <- df_template$Task[i]
          needs_save <- TRUE
        }
      }
    }
    
    # Replace PFA section with new template structure
    pfa_idx_stored <- which(df$Header == "PFA (SN 00398)")
    pfa_idx_template <- which(df_template$Header == "PFA (SN 00398)")
    if (length(pfa_idx_stored) > 0 && length(pfa_idx_template) > 0) {
      # Find the end of PFA section in stored df (until next header with non-empty content or MC1)
      pfa_start_stored <- pfa_idx_stored[1]
      pfa_end_stored <- pfa_start_stored + 1
      while (pfa_end_stored <= nrow(df) && df$Header[pfa_end_stored] == "" && df$Header[pfa_end_stored + 1] != "MC1") {
        if (pfa_end_stored == nrow(df)) break
        pfa_end_stored <- pfa_end_stored + 1
      }
      
      # Find the PFA section span in template
      pfa_start_template <- pfa_idx_template[1]
      pfa_end_template <- pfa_start_template
      while (pfa_end_template < nrow(df_template) && (df_template$Header[pfa_end_template + 1] == "" || df_template$Header[pfa_end_template + 1] == "Täglich" || df_template$Header[pfa_end_template + 1] == "Monatlich")) {
        pfa_end_template <- pfa_end_template + 1
      }
      
      # Replace PFA section
      pfa_rows_to_replace <- pfa_end_template - pfa_start_template + 1
      new_pfa_section <- df_template[pfa_start_template:pfa_end_template, , drop = FALSE]
      
      if (pfa_end_stored - pfa_start_stored + 1 == pfa_rows_to_replace) {
        # Same size, just replace in place
        df[pfa_start_stored:pfa_end_stored, ] <- new_pfa_section
        needs_save <- TRUE
      }
    }
    
    # Always append rows that exist in the template but are missing from the stored table
    if (n_template > n_existing) {
      extra <- df_template[(n_existing + 1):n_template, , drop = FALSE]
      missing_cols <- setdiff(names(df), names(extra))
      if (length(missing_cols)) for (col in missing_cols) extra[[col]] <- ""
      extra <- extra[, names(df), drop = FALSE]
      df <- rbind(df, extra)
      needs_save <- TRUE
    }
    
    # The PFA block above re-copies the template on EVERY load whenever the
    # stored row count differs from the template (e.g. after a task was
    # added in Admin) and flagged a save each time -- one DB write per load,
    # also from the dashboard. Only write when the table really changed.
    if (needs_save && !identical(df, df_before)) {
      df <- order_cols(df)
      save_device_table(con, "g1", df, "<system>")
    }
  }
  
  # ---- COBAS Pro I (g4) / COBAS Pro II (g5): Mittwochs-Rhythmus ----------
  # The "14-tägig" and "Monatlich" blocks are performed on WEDNESDAYS on these
  # two devices, but were originally stored under the generic headers, whose
  # schedules resolve to Monday / a non-Wednesday 28-day cycle. Those sections
  # therefore never appeared in the daily checklist on a Wednesday.
  # This renames the headers in place. It touches ONLY the Header column of
  # header rows, never row order or task texts, so all existing checkmarks and
  # Bemerkungen stay attached to exactly the same tasks. It also runs
  # independently of the row-count guard below, because a pure rename does not
  # change the number of rows.
  if (identical(device_id, "g4") || identical(device_id, "g5")) {
    # NOTE: matched with plain `==` against string literals rather than a
    # named lookup vector -- names of a named character vector can be
    # re-encoded to the native charset on Windows, which makes umlaut keys
    # like "14-tägig" silently stop matching UTF-8 header values.
    hdr_now <- as.character(df$Header)
    old_biweekly <- hdr_now == "14-t\u00e4gig"
    old_monthly  <- hdr_now == "Monatlich"
    if (any(old_biweekly) || any(old_monthly)) {
      df$Header[old_biweekly] <- "14-t\u00e4gig (Mittwoch)"
      df$Header[old_monthly]  <- "Monatlich (Mittwoch, alle 4 Wochen)"
      save_device_table(con, device_id, order_cols(df), "<system>")
    }
  }
  
  # ---- Euroimmun Analyzer I (g2) / Hydrasys (g8) / Phadia 250 (g10) ------
  # Rueckmeldung aus dem Labor (Feedback 25.09.2026): auf diesen Geraeten
  # laufen woechentliche und monatliche Wartung immer an einem festen
  # Wochentag -- g2 dienstags, g8 mittwochs, g10 freitags. Die generischen
  # Header "Woechentlich"/"Monatlich" loesen dagegen auf Montag bzw. einen
  # beliebigen 28-Tage-Zyklus auf, weshalb die Bloecke nie am richtigen Tag
  # in der Tagesliste erschienen.
  # Reines Umbenennen der Header-Zeilen: Zeilenzahl und Reihenfolge bleiben
  # unveraendert, dadurch bleiben alle Haken und Bemerkungen exakt an ihrer
  # Aufgabe haengen.
  weekday_rename <- list(
    g2  = list(c("W\u00f6chentlich", "W\u00f6chentlich (Dienstag)")),
    g8  = list(c("W\u00f6chentlich", "W\u00f6chentlich (Mittwoch)"),
               c("Monatlich", "Monatlich (Mittwoch, alle 4 Wochen)")),
    g10 = list(c("W\u00f6chentlich", "W\u00f6chentlich (Freitag)"))
  )
  if (!is.null(weekday_rename[[device_id]])) {
    hdr_now <- as.character(df$Header)
    changed <- FALSE
    for (pair in weekday_rename[[device_id]]) {
      hit <- hdr_now == pair[1]
      if (any(hit)) { df$Header[hit] <- pair[2]; changed <- TRUE }
    }
    if (changed) save_device_table(con, device_id, order_cols(df), "<system>")
  }
  
  # ---- Probenannahmeplatz (g17): kombinierte Aufgabe aufteilen -----------
  # Rueckmeldung aus dem Labor: "Bestellung in der Probenannahme" und
  # "Wartung der Zentrifugen" sind zwei getrennte Taetigkeiten und muessen
  # einzeln abgehakt werden koennen. Die alte Zeile fasste beides zusammen.
  # Beim Aufteilen entsteht eine zusaetzliche Zeile -- alle nachfolgenden
  # row_index-Werte verschieben sich um 1. Damit Haken und Bemerkungen nicht
  # auf die falsche Aufgabe zeigen, wird die Historie in einem zweiphasigen
  # UPDATE (ueber einen negativen Haltebereich) mitverschoben.
  if (identical(device_id, "g17")) {
    old_txt <- "Zentrifugen: Temperatursensor s\u00e4ubern Bestellung Magazin"
    hit <- which(as.character(df$Task) == old_txt)
    if (length(hit) == 1L) {
      p <- hit[1L]
      n_old <- nrow(df)
      df$Task[p] <- "Zentrifugen: Temperatursensor s\u00e4ubern"
      new_row <- df[p, , drop = FALSE]
      new_row$Header <- ""
      new_row$Task   <- "Bestellung Magazin (Probenannahme)"
      for (cn in setdiff(names(new_row), c("Header", "Task"))) new_row[[cn]] <- ""
      df <- rbind(df[seq_len(p), , drop = FALSE], new_row,
                  if (p < n_old) df[(p + 1L):n_old, , drop = FALSE] else df[0, , drop = FALSE])
      
      if (p < n_old) {
        tables <- c("task_status", "device_cell_status",
                    "device_task_remark", "device_task_meta")
        OFFSET <- 1000000L
        tryCatch({
          DBI::dbWithTransaction(con, {
            for (tbl in tables) {
              for (k in n_old:(p + 1L))
                DBI::dbExecute(con, sprintf(
                  "UPDATE %s SET row_index = $1 WHERE device_id = $2 AND row_index = $3", tbl),
                  params = list(-(k + OFFSET), device_id, k))
              for (k in n_old:(p + 1L))
                DBI::dbExecute(con, sprintf(
                  "UPDATE %s SET row_index = $1 WHERE device_id = $2 AND row_index = $3", tbl),
                  params = list(k + 1L, device_id, -(k + OFFSET)))
            }
          })
        }, error = function(e)
          message("g17 Aufgaben-Split: row_index-Remap fehlgeschlagen: ",
                  conditionMessage(e)))
      }
      save_device_table(con, device_id, order_cols(df), "<system>")
    }
  }
  
  # ---- Probenannahmeplatz (g17): Druckerstatus je Dienst -----------------
  # Rueckmeldung aus dem Labor: "Druckerstatus aller Etikettendrucker 2X
  # taeglich kontrollieren" soll zweimal als eigene Aufgabe erscheinen --
  # einmal fuer den Tagdienst, einmal fuer den Nachtdienst. Bisher war es ein
  # Hinweisblock mit den Unterzeilen "Tagdienst (morgens)" / "Nachtdienst".
  # Reines Umbenennen an Ort und Stelle (Kopfzeile -> "Taeglich", Unterzeilen
  # -> voller Aufgabentext): Zeilenzahl und Reihenfolge bleiben gleich, alle
  # Haken und Bemerkungen bleiben an ihrer Aufgabe.
  if (identical(device_id, "g17")) {
    hdr_now  <- as.character(df$Header)
    task_now <- as.character(df$Task)
    p <- which(hdr_now == G17_DRUCKER_OLD_HEADER)
    if (length(p) == 1L && p + 2L <= nrow(df) &&
        identical(task_now[p + 1L], "Tagdienst (morgens)") &&
        identical(task_now[p + 2L], "Nachtdienst")) {
      df$Header[p]    <- "Täglich"
      df$Task[p + 1L] <- G17_DRUCKER_TD
      df$Task[p + 2L] <- G17_DRUCKER_ND
      save_device_table(con, device_id, order_cols(df), "<system>")
      try(audit_clear_task_cache(device_id), silent = TRUE)
    }
  }
  
  # ---- COBAS Pro I (g4) / COBAS Pro II (g5) migration --------------------
  # Sync stored tables with the current template so newly added task texts
  # (Taeglich / Woechentlich / TD / 14-taegig / Monatlich / Elektrodenaustausch)
  # appear without dropping existing daily entries or comments. Mirrors the g1
  # logic above: inject missing task texts only into rows that don't already
  # have one, and append any extra template rows at the end.
  # GUARD (see g1 note above): only run when row counts differ, so it can't
  # re-shuffle rows under existing checkmark history on every load.
  if ((identical(device_id, "g4") || identical(device_id, "g5")) &&
      nrow(df) != nrow(create_initial_table(device_id))) {
    df_template <- create_initial_table(device_id)
    n_existing  <- nrow(df)
    n_template  <- nrow(df_template)
    needs_save  <- FALSE
    
    # Inject template task texts only where the stored row currently has none.
    # Safe to run repeatedly: existing tasks are left untouched.
    n_min <- min(n_existing, n_template)
    for (i in seq_len(n_min)) {
      tmpl_header <- df_template$Header[i]
      tmpl_task   <- df_template$Task[i]
      # Headers: fill if stored header is empty/missing
      if (nzchar(tmpl_header) && !nzchar(df$Header[i])) {
        df$Header[i] <- tmpl_header
        needs_save <- TRUE
      }
      # Tasks: fill if this is a data row (Header == "") and Task is empty
      if (df$Header[i] == "" && nzchar(tmpl_task) && !nzchar(df$Task[i])) {
        df$Task[i] <- tmpl_task
        needs_save <- TRUE
      }
    }
    
    # Append rows that exist in the template but are missing from the stored
    # table (e.g. new sections added later).
    if (n_template > n_existing) {
      extra <- df_template[(n_existing + 1):n_template, , drop = FALSE]
      missing_cols <- setdiff(names(df), names(extra))
      if (length(missing_cols)) for (col in missing_cols) extra[[col]] <- ""
      extra <- extra[, names(df), drop = FALSE]
      df <- rbind(df, extra)
      needs_save <- TRUE
    }
    
    if (needs_save) {
      df <- order_cols(df)
      save_device_table(con, device_id, df, "<system>")
    }
  }
  
  df
}


save_device_table <- function(con, device_id, df, username) {
  df <- order_cols(df)
  base_cols <- setdiff(names(df), c("Done", "Kommentar", "Benutzer"))
  js <- jsonlite::toJSON(df[, base_cols, drop = FALSE],
                         dataframe = "rows", auto_unbox = TRUE, na = "string")
  DBI::dbExecute(
    con,
    "INSERT INTO device_tables (device_id, data_json, updated_at, updated_by)
     VALUES ($1, $2::jsonb, NOW(), $3)
     ON CONFLICT (device_id) DO UPDATE
     SET data_json = EXCLUDED.data_json, updated_at = NOW(), updated_by = EXCLUDED.updated_by",
    params = list(device_id, js, username)
  )
  # The plan structure changed -> drop the cached task names used by the
  # audit trail so later entries resolve against the new layout.
  if (exists("audit_clear_task_cache"))
    try(audit_clear_task_cache(device_id), silent = TRUE)
}

# ---- Row-level status (kept for comments/overview if you still want it) ----
load_status <- function(con, device_id) {
  if (has_column(con, "task_status", "updated_by")) {
    DBI::dbGetQuery(
      con,
      "SELECT device_id, row_index, done, comment, updated_by
         FROM task_status
        WHERE device_id = $1",
      params = list(device_id)
    )
  } else {
    DBI::dbGetQuery(
      con,
      "SELECT device_id, row_index, done, comment, NULL::text AS updated_by
         FROM task_status
        WHERE device_id = $1",
      params = list(device_id)
    )
  }
}

save_status <- function(con, device_id, df, username) {
  non_header_idx <- which(df$Header == "")
  if (!length(non_header_idx)) return(invisible(TRUE))
  
  done_vec <- if ("Done" %in% names(df)) {
    as.logical(df$Done[non_header_idx])
  } else {
    rep(FALSE, length(non_header_idx))
  }
  
  comment_vec <- if ("Kommentar" %in% names(df)) {
    as.character(df$Kommentar[non_header_idx])
  } else {
    rep("", length(non_header_idx))
  }
  
  dat <- data.frame(
    device_id  = rep(device_id, length(non_header_idx)),
    row_index  = non_header_idx,
    done       = done_vec,
    comment    = comment_vec,
    updated_by = rep(username, length(non_header_idx)),
    stringsAsFactors = FALSE
  )
  
  DBI::dbBegin(con)
  on.exit({ try(DBI::dbRollback(con), silent = TRUE) }, add = TRUE)
  
  for (i in seq_len(nrow(dat))) {
    DBI::dbExecute(
      con,
      "INSERT INTO task_status (device_id, row_index, done, comment, updated_by, updated_at)
       VALUES ($1, $2, $3, $4, $5, NOW())
       ON CONFLICT (device_id, row_index) DO UPDATE
         SET done = EXCLUDED.done,
             comment = EXCLUDED.comment,
             updated_by = EXCLUDED.updated_by,
             updated_at = NOW()",
      params = unname(as.list(dat[i, c("device_id", "row_index", "done", "comment", "updated_by")]))
    )
  }
  
  DBI::dbCommit(con); on.exit(NULL, add = FALSE)
  TRUE
}

# ---- Per-cell status helpers ----
load_cell_status <- function(con, device_id, month = NULL, year = NULL) {
  if (!is.null(month) && !is.null(year) &&
      length(month) == 1L && length(year) == 1L &&
      !is.na(month) && !is.na(year)) {
    DBI::dbGetQuery(con, "
      SELECT device_id, row_index, day, value_text, updated_by, updated_at,
             month, year
        FROM device_cell_status
       WHERE device_id = $1 AND month = $2 AND year = $3
    ", params = list(device_id, as.integer(month), as.integer(year)))
  } else {
    DBI::dbGetQuery(con, "
      SELECT device_id, row_index, day, value_text, updated_by, updated_at,
             month, year
        FROM device_cell_status
       WHERE device_id = $1
    ", params = list(device_id))
  }
}

# ---- Audit-Trail: zentrale Logfunktion --------------------------------------
# One call logs one change. Deliberately fail-safe: logging must NEVER break a
# user action, so every error is swallowed and additionally written to stderr
# (which ends up in the Shiny Server log) so a broken audit trail is still
# noticeable to an administrator.
#
# Usage:
#   log_event(con, "task.update", entity_type = "cell",
#             entity_id = "18@2026-09-14", device_id = "g5",
#             old_value = "", new_value = "NE", who = rv$user)

# Resolve a row index to a readable "Abschnitt -> Aufgabentext" label, so the
# protocol says WHAT was changed instead of only "Zeile 18". Row indices are
# unstable over time (inserting a plan row shifts them), which is exactly why
# the name is frozen into the log entry at the time of the change.
# Cached per device for the lifetime of the R process; the plan structure
# changes rarely and only via the admin UI, which clears the cache.
.audit_task_cache <- new.env(parent = emptyenv())

audit_clear_task_cache <- function(device_id = NULL) {
  if (is.null(device_id)) rm(list = ls(.audit_task_cache), envir = .audit_task_cache)
  else if (exists(device_id, envir = .audit_task_cache))
    rm(list = device_id, envir = .audit_task_cache)
  invisible(TRUE)
}

audit_task_name <- function(con, device_id, row_index) {
  ri <- suppressWarnings(as.integer(row_index))
  if (is.na(ri) || is.null(device_id) || !nzchar(device_id)) return(NA_character_)
  df <- tryCatch({
    if (exists(device_id, envir = .audit_task_cache)) {
      get(device_id, envir = .audit_task_cache)
    } else {
      d <- load_device_table(con, device_id)
      if (!is.null(d)) assign(device_id, d, envir = .audit_task_cache)
      d
    }
  }, error = function(e) NULL)
  if (is.null(df) || !nrow(df) || ri < 1 || ri > nrow(df)) return(NA_character_)
  hdrs  <- as.character(df$Header)
  tasks <- if ("Task" %in% names(df)) as.character(df$Task) else rep("", nrow(df))
  txt <- trimws(tasks[ri] %||% "")
  # Nearest non-empty header at or above the row = the schedule section.
  sec <- ""
  for (i in seq(ri, 1L)) {
    h <- trimws(hdrs[i] %||% "")
    if (nzchar(h)) { sec <- h; break }
  }
  if (!nzchar(txt)) {
    # Row carries no task -- most likely a section header itself.
    hh <- trimws(hdrs[ri] %||% "")
    return(if (nzchar(hh)) paste0("[Abschnitt] ", hh) else NA_character_)
  }
  if (nzchar(sec)) paste0(sec, " \u2192 ", txt) else txt
}

log_event <- function(con, action,
                      entity_type = NA_character_, entity_id = NA_character_,
                      device_id = NA_character_,
                      task_name = NA_character_, ref_date = NA,
                      row_index = NA_integer_,
                      old_value = NA_character_, new_value = NA_character_,
                      details = NA_character_,
                      who = NULL, role = NULL, session_id = NULL,
                      client_ip = NULL) {
  as_txt <- function(x) {
    if (is.null(x) || length(x) == 0) return(NA_character_)
    x <- as.character(x)[1]
    if (is.na(x)) return(NA_character_)
    # Keep single entries readable in the viewer and bounded in size.
    if (nchar(x) > 2000) x <- paste0(substr(x, 1, 2000), " \u2026[gekuerzt]")
    x
  }
  as_int <- function(x) {
    if (is.null(x) || length(x) == 0) return(NA_integer_)
    x <- suppressWarnings(as.integer(x)[1]); if (is.na(x)) NA_integer_ else x
  }
  as_dat <- function(x) {
    if (is.null(x) || length(x) == 0) return(NA_character_)
    d <- suppressWarnings(as.Date(x[1]))
    if (is.na(d)) NA_character_ else as.character(d)
  }
  tryCatch({
    DBI::dbExecute(con, "
      INSERT INTO app_audit_log
        (username, user_role, action, entity_type, entity_id, device_id,
         task_name, ref_date, row_index,
         old_value, new_value, details, session_id, client_ip, app_version)
      VALUES ($1,$2,$3,$4,$5,$6,$7,$8::date,$9,$10,$11,$12,$13,$14,$15)
    ", params = list(as_txt(who), as_txt(role), as_txt(action),
                     as_txt(entity_type), as_txt(entity_id), as_txt(device_id),
                     as_txt(task_name), as_dat(ref_date), as_int(row_index),
                     as_txt(old_value), as_txt(new_value), as_txt(details),
                     as_txt(session_id), as_txt(client_ip),
                     as_txt(APP_VERSION)))
  }, error = function(e) {
    message(sprintf("[AUDIT-FEHLER] %s: %s", action, conditionMessage(e)))
    NULL
  })
  invisible(TRUE)
}

# Read the value a cell had before it is overwritten, so the audit entry can
# show old -> new. Returns "" when the cell was empty.
read_cell_value <- function(con, device_id, row_index, day, month, year) {
  tryCatch({
    r <- DBI::dbGetQuery(con, "
      SELECT value_text FROM device_cell_status
       WHERE device_id=$1 AND row_index=$2 AND day=$3 AND month=$4 AND year=$5
    ", params = list(device_id, as.integer(row_index), as.integer(day),
                     as.integer(month), as.integer(year)))
    if (nrow(r)) as.character(r$value_text[1]) %||% "" else ""
  }, error = function(e) "")
}

upsert_cell <- function(con, device_id, row_index, day, value_text, who,
                        month = NULL, year = NULL) {
  if (is.null(month) || is.na(month)) month <- as.integer(format(Sys.Date(), "%m"))
  if (is.null(year)  || is.na(year))  year  <- as.integer(format(Sys.Date(), "%Y"))
  # Capture the previous state first, so the audit entry is a real before/after.
  prev <- read_cell_value(con, device_id, row_index, day, month, year)
  DBI::dbExecute(con, "
    INSERT INTO device_cell_status (device_id, row_index, day, month, year,
                                    value_text, updated_by, updated_at)
    VALUES ($1,$2,$3,$4,$5,$6,$7,NOW())
    ON CONFLICT (device_id, row_index, day, month, year) DO UPDATE
      SET value_text = EXCLUDED.value_text,
          updated_by = EXCLUDED.updated_by,
          updated_at = NOW()
  ", params = list(device_id, row_index, day,
                   as.integer(month), as.integer(year),
                   value_text, who))

  # Central "override" hook: any cell value other than NE (a checkmark, WE,
  # FT, D, …) means the task was clarified for that day, so every still-open
  # NE remark of this task up to that date is closed and stops popping up.
  # Putting this here covers ALL write paths (Tagesliste, Vortagsbanner,
  # Monatsübersicht-Grid, Admin), not just the daily checklist.
  vt <- trimws(as.character(value_text %||% ""))
  if (nzchar(vt) && !identical(vt, "NE")) {
    cell_date <- suppressWarnings(as.Date(sprintf("%04d-%02d-%02d",
                                                  as.integer(year),
                                                  as.integer(month),
                                                  as.integer(day))))
    if (!is.na(cell_date))
      tryCatch(resolve_open_ne_remarks(con, device_id, row_index, cell_date, who),
               error = function(e) NULL)
  }
  # Audit: only record a row when the value actually changed, so repeatedly
  # re-saving the same state does not flood the protocol.
  if (!identical(trimws(prev), vt))
    log_event(con,
              action      = if (!nzchar(trimws(prev))) "eintrag.neu" else "eintrag.geaendert",
              entity_type = "wartungseintrag",
              entity_id   = sprintf("Zeile %s | %02d.%02d.%04d", row_index,
                                    as.integer(day), as.integer(month),
                                    as.integer(year)),
              device_id   = device_id,
              task_name   = audit_task_name(con, device_id, row_index),
              ref_date    = sprintf("%04d-%02d-%02d", as.integer(year),
                                    as.integer(month), as.integer(day)),
              row_index   = row_index,
              old_value   = prev, new_value = vt, who = who)
  invisible(TRUE)
}

delete_cell_if_owner <- function(con, device_id, row_index, day, who,
                                 is_admin = FALSE,
                                 month = NULL, year = NULL) {
  if (is.null(month) || is.na(month)) month <- as.integer(format(Sys.Date(), "%m"))
  if (is.null(year)  || is.na(year))  year  <- as.integer(format(Sys.Date(), "%Y"))
  prev <- read_cell_value(con, device_id, row_index, day, month, year)
  n <- 0L
  if (is_admin) {
    n <- DBI::dbExecute(con, "
      DELETE FROM device_cell_status
       WHERE device_id=$1 AND row_index=$2 AND day=$3 AND month=$4 AND year=$5
    ", params = list(device_id, row_index, day,
                     as.integer(month), as.integer(year)))
  } else {
    n <- DBI::dbExecute(con, "
      DELETE FROM device_cell_status
       WHERE device_id=$1 AND row_index=$2 AND day=$3 AND month=$4 AND year=$5
         AND updated_by=$6
    ", params = list(device_id, row_index, day,
                     as.integer(month), as.integer(year), who))
  }
  if (n > 0)
    log_event(con, action = "eintrag.geloescht",
              entity_type = "wartungseintrag",
              entity_id   = sprintf("Zeile %s | %02d.%02d.%04d", row_index,
                                    as.integer(day), as.integer(month),
                                    as.integer(year)),
              device_id = device_id,
              task_name = audit_task_name(con, device_id, row_index),
              ref_date  = sprintf("%04d-%02d-%02d", as.integer(year),
                                  as.integer(month), as.integer(day)),
              row_index = row_index,
              old_value = prev, new_value = "",
              details = if (is_admin) "durch Administrator" else NA_character_,
              who = who, role = if (is_admin) "admin" else "user")
  invisible(n)
}

# Convenience: load the live (current month/year) status snapshot.
load_cell_status_current <- function(con, device_id) {
  load_cell_status(con, device_id,
                   as.integer(format(Sys.Date(), "%m")),
                   as.integer(format(Sys.Date(), "%Y")))
}

# ---- Remarks (Bemerkungen) -------------------------------------------------
# Save/update a remark for one task on one calendar date.
upsert_remark <- function(con, device_id, row_index, remark_date, remark_text,
                          option_code, who) {
  rt <- if (is.null(remark_text)) "" else trimws(as.character(remark_text))
  oc <- if (is.null(option_code)) NA_character_ else as.character(option_code)
  # Previous state, for the audit trail.
  prev <- tryCatch({
    p <- DBI::dbGetQuery(con, "
      SELECT remark_text, option_code FROM device_task_remark
       WHERE device_id=$1 AND row_index=$2 AND remark_date=$3
    ", params = list(device_id, row_index, as.character(remark_date)))
    if (nrow(p)) trimws(paste0(p$option_code[1] %||% "", " ",
                               p$remark_text[1] %||% "")) else ""
  }, error = function(e) "")
  new_txt <- trimws(paste0(if (is.na(oc)) "" else oc, " ", rt))
  audit_id <- sprintf("Zeile %s | %s", row_index,
                      format(as.Date(remark_date), "%d.%m.%Y"))
  # If both fields are empty, treat as a deletion so we don't accumulate blanks.
  if (!nzchar(rt) && (is.na(oc) || !nzchar(oc))) {
    n <- DBI::dbExecute(con, "
      DELETE FROM device_task_remark
       WHERE device_id=$1 AND row_index=$2 AND remark_date=$3
    ", params = list(device_id, row_index, as.character(remark_date)))
    if (n > 0)
      log_event(con, "bemerkung.geloescht", entity_type = "bemerkung",
                entity_id = audit_id, device_id = device_id,
                task_name = audit_task_name(con, device_id, row_index),
                ref_date = remark_date, row_index = row_index,
                old_value = prev, new_value = "", who = who)
    return(invisible())
  }
  DBI::dbExecute(con, "
    INSERT INTO device_task_remark
      (device_id, row_index, remark_date, remark_text, option_code, updated_by, updated_at)
    VALUES ($1,$2,$3,$4,$5,$6,NOW())
    ON CONFLICT (device_id, row_index, remark_date) DO UPDATE
      SET remark_text = EXCLUDED.remark_text,
          option_code = EXCLUDED.option_code,
          updated_by  = EXCLUDED.updated_by,
          updated_at  = NOW(),
          -- Re-open only when the entry newly BECOMES 'NE'. Editing the text
          -- of an already-closed remark must not resurrect the popup.
          resolved_at = CASE WHEN EXCLUDED.option_code = 'NE'
                              AND device_task_remark.option_code IS DISTINCT FROM 'NE'
                             THEN NULL ELSE device_task_remark.resolved_at END,
          resolved_by = CASE WHEN EXCLUDED.option_code = 'NE'
                              AND device_task_remark.option_code IS DISTINCT FROM 'NE'
                             THEN NULL ELSE device_task_remark.resolved_by END
  ", params = list(device_id, row_index, as.character(remark_date),
                   rt, oc, who))
  if (!identical(prev, new_txt))
    log_event(con,
              action      = if (!nzchar(prev)) "bemerkung.neu" else "bemerkung.geaendert",
              entity_type = "bemerkung", entity_id = audit_id,
              device_id   = device_id,
              task_name   = audit_task_name(con, device_id, row_index),
              ref_date    = remark_date, row_index = row_index,
              old_value   = prev, new_value = new_txt, who = who,
              details     = if (identical(oc, "NE"))
                "Nicht erledigt - erscheint als Hinweis bis zur Korrektur" else NA_character_)
}

# Mark every still-open "NE – Nicht erledigt" remark of one task as resolved.
# Called whenever the same task gets a different entry (✓ erledigt, WE, FT, …)
# on `upto_date` or later: the newer entry overrides the old NE, so the login
# popup disappears for good. History stays intact -- nothing is deleted, the
# NE remains visible in the Monatsübersicht.
resolve_open_ne_remarks <- function(con, device_id, row_index, upto_date, who) {
  DBI::dbExecute(con, "
    UPDATE device_task_remark
       SET resolved_at = NOW(),
           resolved_by = $4
     WHERE device_id   = $1
       AND row_index   = $2
       AND remark_date <= $3
       AND option_code  = 'NE'
       AND resolved_at IS NULL
  ", params = list(device_id, as.integer(row_index),
                   as.character(upto_date), who %||% "system"))
}

# Close exactly one remark (the "Erledigt" button in the popup).
resolve_remark_entry <- function(con, device_id, row_index, remark_date, who) {
  DBI::dbExecute(con, "
    UPDATE device_task_remark
       SET resolved_at = NOW(),
           resolved_by = $4
     WHERE device_id = $1 AND row_index = $2 AND remark_date = $3
       AND resolved_at IS NULL
  ", params = list(device_id, as.integer(row_index),
                   as.character(remark_date), who %||% "system"))
}

# Explicitly (re-)open an NE remark -- used when a user actively clicks
# "Nicht erledigt" / picks NE again for a task that had been closed before.
reopen_ne_remark <- function(con, device_id, row_index, remark_date) {
  DBI::dbExecute(con, "
    UPDATE device_task_remark
       SET resolved_at = NULL,
           resolved_by = NULL
     WHERE device_id = $1 AND row_index = $2 AND remark_date = $3
  ", params = list(device_id, as.integer(row_index), as.character(remark_date)))
}

load_remarks_for_date <- function(con, device_id, remark_date) {
  DBI::dbGetQuery(con, "
    SELECT row_index, remark_text, option_code, updated_by, updated_at
      FROM device_task_remark
     WHERE device_id=$1 AND remark_date=$2
  ", params = list(device_id, as.character(remark_date)))
}

# ---- Per-task metadata (e.g. "zuletzt getauscht am") ----------------------
# Returns a named list (row_index -> Date) with the last-replaced date for
# every task row that has one stored.
load_task_meta_dates <- function(con, device_id) {
  res <- DBI::dbGetQuery(con, "
    SELECT row_index, last_replaced_date
      FROM device_task_meta
     WHERE device_id = $1
  ", params = list(device_id))
  out <- list()
  if (!nrow(res)) return(out)
  for (i in seq_len(nrow(res))) {
    d <- res$last_replaced_date[i]
    if (is.null(d) || is.na(d)) next
    out[[ as.character(res$row_index[i]) ]] <- as.Date(d)
  }
  out
}

save_task_meta_date <- function(con, device_id, row_index, last_replaced_date, who) {
  d <- if (is.null(last_replaced_date) || is.na(last_replaced_date)) NA_character_
  else as.character(as.Date(last_replaced_date))
  if (is.na(d)) {
    DBI::dbExecute(con, "
      DELETE FROM device_task_meta
       WHERE device_id = $1 AND row_index = $2
    ", params = list(device_id, row_index))
    return(invisible())
  }
  DBI::dbExecute(con, "
    INSERT INTO device_task_meta (device_id, row_index, last_replaced_date,
                                  updated_by, updated_at)
    VALUES ($1, $2, $3::date, $4, NOW())
    ON CONFLICT (device_id, row_index) DO UPDATE
      SET last_replaced_date = EXCLUDED.last_replaced_date,
          updated_by         = EXCLUDED.updated_by,
          updated_at         = NOW()
  ", params = list(device_id, row_index, d, who))
}

# Detect tasks that end with "zuletzt getauscht am:" -> render a date input.
is_last_replaced_task <- function(text) {
  if (is.null(text) || is.na(text)) return(FALSE)
  grepl("zuletzt\\s+getauscht\\s+am\\s*:", text, ignore.case = TRUE)
}

# Pull the replacement period in months from the task description, e.g.
# "alle 2 Monate" -> 2L. Returns NA if no period is mentioned.
extract_replacement_months <- function(text) {
  if (is.null(text) || is.na(text)) return(NA_integer_)
  m <- regmatches(text, regexpr("alle\\s+(\\d+)\\s+Monat", text, ignore.case = TRUE))
  if (!length(m) || !nzchar(m)) return(NA_integer_)
  as.integer(sub("\\D+", "", m))
}

# Compute the next due date for a "zuletzt getauscht am" replacement task.
# Rule (per CL request): the replacement always happens on the FIRST Wednesday
# of the target month (= last_replaced + period), shifted to the next workday
# if that Wednesday is a public holiday. Returns NA if either input is missing.
replacement_next_due <- function(last_replaced, months) {
  last_replaced <- suppressWarnings(as.Date(last_replaced))
  months        <- suppressWarnings(as.integer(months))
  if (length(last_replaced) != 1L || is.na(last_replaced) ||
      length(months) != 1L || is.na(months) || months <= 0L) return(as.Date(NA))
  lt <- as.POSIXlt(last_replaced)
  lt$mday <- 1L
  lt$mon  <- lt$mon + months
  target_month_first <- as.Date(lt)
  wednesday_of_month(target_month_first, which = 1L)
}

# Snap a chosen date to the most appropriate working Wednesday in the same
# calendar month (next Wednesday on/after `d`, falling back to previous
# Wednesday if that overflows the month, then shifted past holidays).
snap_to_working_wednesday <- function(d) {
  d <- suppressWarnings(as.Date(d))
  if (length(d) != 1L || is.na(d)) return(as.Date(NA))
  iso <- as.integer(format(d, "%u"))
  if (iso == 3L && is_workday(d)) return(d)
  cand_fwd <- next_wednesday(d)
  cand_bwd <- cand_fwd - 7L
  pick <- if (as.integer(format(cand_fwd, "%m")) == as.integer(format(d, "%m")))
    cand_fwd else cand_bwd
  shift_to_next_workday(pick)
}

# All remarks (Bemerkungen) for one calendar month — used to surface
# other users' comments in the Monatsübersicht table.
load_remarks_for_month <- function(con, device_id, month, year) {
  DBI::dbGetQuery(con, "
    SELECT row_index, remark_date,
           EXTRACT(DAY FROM remark_date)::int AS day,
           remark_text, option_code, updated_by, updated_at
      FROM device_task_remark
     WHERE device_id = $1
       AND EXTRACT(MONTH FROM remark_date) = $2
       AND EXTRACT(YEAR  FROM remark_date) = $3
  ", params = list(device_id, as.integer(month), as.integer(year)))
}

# Open items for the login reminder ("Nicht erledigte Aufgaben"). ONLY tasks
# explicitly marked "NE – Nicht erledigt" and not yet resolved are returned:
# every other option code (WE, FT, Ø, W.e., ne, D, sQ) is documentation for
# the Monatsübersicht and must never trigger a popup, and a plain remark
# without an option code is a note, not an open task.
# `for_user` additionally hides entries that this user personally marked as
# read ("Als gelesen markieren"), unless the remark was edited afterwards.
# Set only_open = FALSE to get the old behaviour (all remarks of the period).
load_recent_remarks <- function(con, device_id, today = Sys.Date(),
                                days_back = 14L, include_today = FALSE,
                                only_open = TRUE, for_user = NULL) {
  upper <- if (isTRUE(include_today)) today + 1L else today
  filter_sql <- if (isTRUE(only_open))
    "AND r.option_code = 'NE' AND r.resolved_at IS NULL"
  else
    "AND ( (r.remark_text IS NOT NULL AND r.remark_text <> '')
        OR (r.option_code IS NOT NULL AND r.option_code <> '') )"
  who <- if (is.null(for_user) || !nzchar(as.character(for_user)))
    "" else as.character(for_user)
  ack_sql <- if (nzchar(who))
    "AND NOT EXISTS (
       SELECT 1 FROM device_task_remark_ack a
        WHERE a.device_id   = r.device_id
          AND a.row_index   = r.row_index
          AND a.remark_date = r.remark_date
          AND a.username    = $4
          AND a.acked_update_at >= r.updated_at
     )" else ""
  prm <- list(device_id,
              as.character(today - as.integer(days_back)),
              as.character(upper))
  if (nzchar(who)) prm <- c(prm, list(who))
  DBI::dbGetQuery(con, sprintf("
    SELECT r.row_index, r.remark_date, r.remark_text, r.option_code,
           r.updated_by, r.updated_at
      FROM device_task_remark r
     WHERE r.device_id = $1
       AND r.remark_date >= $2
       AND r.remark_date <  $3
       %s
       %s
     ORDER BY r.remark_date DESC, r.row_index ASC
  ", filter_sql, ack_sql), params = prm)
}

# "Als gelesen markieren": personal acknowledgement of one open remark.
# Stores the remark's current updated_at, so a later edit to that remark
# makes it resurface for this user as well.
ack_remark_for_user <- function(con, device_id, row_index, remark_date, who) {
  DBI::dbExecute(con, "
    INSERT INTO device_task_remark_ack
      (device_id, row_index, remark_date, username, acked_update_at, acked_at)
    SELECT r.device_id, r.row_index, r.remark_date, $4, r.updated_at, NOW()
      FROM device_task_remark r
     WHERE r.device_id = $1 AND r.row_index = $2 AND r.remark_date = $3
    ON CONFLICT (device_id, row_index, remark_date, username) DO UPDATE
      SET acked_update_at = EXCLUDED.acked_update_at,
          acked_at        = NOW()
  ", params = list(device_id, as.integer(row_index),
                   as.character(remark_date), as.character(who)))
}

# Delete a single remark identified by (device_id, row_index, remark_date).
# Hard delete -- kept for admin/cleanup use only. The "Erledigt" button in the
# popup no longer deletes: it calls resolve_remark_entry() so the entry stays
# documented in the Monatsübersicht and only stops triggering the reminder.
delete_remark <- function(con, device_id, row_index, remark_date) {
  DBI::dbExecute(con, "
    DELETE FROM device_task_remark
     WHERE device_id = $1 AND row_index = $2 AND remark_date = $3
  ", params = list(device_id, as.integer(row_index), as.character(remark_date)))
}

# ---- PDF-Report ohne LaTeX (cairo_pdf) -------------------------------------
# Zeichnet den Wartungsplan direkt mit Base-R-Grafik in ein PDF (Querformat
# A4). Es wird weder LaTeX/xelatex noch pandoc oder eine .Rmd-Vorlage
# benoetigt, damit der Export auch auf einem Server ohne TeX funktioniert.
#
# Layout: Monatsuebersicht -- alle Tage des Monats stehen nebeneinander auf
# einer Seite. Passen nicht alle Aufgaben auf eine Seite, werden sie auf
# Folgeseiten fortgesetzt (die Abschnittsueberschrift wird wiederholt).
#
# In den Tagesfeldern stehen bei erledigten Aufgaben die Namenskuerzel der
# Person, die abgezeichnet hat (aus device_cell_status.updated_by), sonst der
# eingetragene Code (WE, FT, NE ...). Dafuer wird `initials_df` benoetigt --
# ein data.frame mit den Spalten row_index, day und updated_by.
render_wartungsplan_pdf <- function(file, table_data, device_label, month_str,
                                    report_date, serials = list(),
                                    footer_text = "", version_str = "",
                                    sel_month = NULL, sel_year = NULL,
                                    option_meanings = NULL,
                                    initials_df = NULL) {

  # Umlaute zuverlaessig in die PDF-Schrift bringen.
  old_lc <- Sys.getlocale("LC_CTYPE")
  suppressWarnings(tryCatch(Sys.setlocale("LC_CTYPE", "de_DE.UTF-8"),
                            error = function(e) NULL))
  if (Sys.getlocale("LC_CTYPE") %in% c("C", "POSIX"))
    suppressWarnings(tryCatch(Sys.setlocale("LC_CTYPE", "C.UTF-8"),
                              error = function(e) NULL))
  on.exit(suppressWarnings(try(Sys.setlocale("LC_CTYPE", old_lc), silent = TRUE)),
          add = TRUE)

  col_primary <- "#003B73"; col_accent  <- "#136377"
  col_row_alt <- "#eef3f8"; col_border  <- "#c2cede"
  col_done    <- "#0a7d34"; col_note    <- "#7a5d00"
  col_weekend <- "#0f2f52"

  if (is.null(table_data) || !nrow(table_data))
    table_data <- data.frame(Header = "", Task = "Keine Aufgaben hinterlegt.",
                             stringsAsFactors = FALSE)
  if (!"Header" %in% names(table_data)) table_data$Header <- ""
  if (!"Task"   %in% names(table_data)) table_data$Task   <- ""

  has_my <- !is.null(sel_month) && !is.null(sel_year) &&
            !is.na(sel_month) && !is.na(sel_year)
  day_date <- function(dnum) {
    if (!has_my) return(NA)
    tryCatch(as.Date(sprintf("%04d-%02d-%02d", sel_year, sel_month, dnum)),
             error = function(e) NA, warning = function(w) NA)
  }

  # Gueltige Tagesspalten fuer den gewaehlten Monat bestimmen.
  day_cols <- intersect(as.character(1:31), names(table_data))
  ndays_in_month <- 31L
  if (has_my) {
    nm <- if (sel_month == 12) as.Date(sprintf("%04d-01-01", sel_year + 1))
          else as.Date(sprintf("%04d-%02d-01", sel_year, sel_month + 1))
    ndays_in_month <- as.integer(nm - as.Date(sprintf("%04d-%02d-01", sel_year, sel_month)))
  }
  valid_days <- intersect(as.character(seq_len(ndays_in_month)), day_cols)
  vd_int <- suppressWarnings(as.integer(valid_days))
  vd_int <- vd_int[!is.na(vd_int)]
  if (!length(vd_int)) vd_int <- seq_len(ndays_in_month)

  wd_letter <- function(dnum) {
    dt <- day_date(dnum); if (is.na(dt)) return("")
    c("Mo","Di","Mi","Do","Fr","Sa","So")[as.integer(format(dt, "%u"))]
  }
  is_weekend <- function(dnum) {
    dt <- day_date(dnum); if (is.na(dt)) return(FALSE)
    as.integer(format(dt, "%u")) >= 6
  }
  day_week <- function(dnum) {
    dt <- day_date(dnum); if (is.na(dt)) return(NA_integer_)
    as.integer(format(dt, "%V"))
  }

  # Tage nicht mehr nach Kalenderwoche aufteilen: die Monatsuebersicht zeigt
  # alle Tage des Monats nebeneinander auf einer Seite.
  days_all <- sort(vd_int)

  # ---- Namenskuerzel je (Zeile, Tag) nachschlagbar machen -------------------
  ini_map <- new.env(parent = emptyenv())
  if (!is.null(initials_df) && is.data.frame(initials_df) && nrow(initials_df) &&
      all(c("row_index", "day") %in% names(initials_df))) {
    who_col <- if ("updated_by" %in% names(initials_df)) "updated_by" else NA_character_
    if (!is.na(who_col)) for (i in seq_len(nrow(initials_df))) {
      who <- trimws(as.character(initials_df[[who_col]][i]))
      if (is.na(who) || !nzchar(who)) next
      assign(paste0(initials_df$row_index[i], "_", initials_df$day[i]), who, envir = ini_map)
    }
  }
  lookup_ini <- function(ri, d) {
    k <- paste0(ri, "_", d)
    if (exists(k, envir = ini_map, inherits = FALSE)) get(k, envir = ini_map) else ""
  }

  # R ersetzt Zeichen, die es im eingestellten Zeichensatz nicht darstellen
  # kann, durch Platzhalter der Form <U+00D8>. Das passiert z. B. bei den
  # Namen benannter Vektoren (OPTION_MEANINGS) auf Servern ohne UTF-8-Locale.
  # Hier werden solche Platzhalter wieder in das echte Zeichen zurueckgewandelt.
  fix_uplus <- function(x) {
    x <- enc2utf8(as.character(x))
    m <- regmatches(x, gregexpr("<U\\+[0-9A-Fa-f]{4,6}>", x))
    for (i in seq_along(x)) {
      if (!length(m[[i]])) next
      for (tok in unique(m[[i]])) {
        cp <- strtoi(substr(tok, 4, nchar(tok) - 1), 16L)
        ch <- tryCatch(intToUtf8(cp), error = function(e) "")
        if (nzchar(ch)) x[i] <- gsub(tok, ch, x[i], fixed = TRUE)
      }
    }
    x
  }

  # Text zentriert in eine Tagesspalte schreiben und dabei so weit
  # verkleinern, dass er nicht in die Nachbarspalte laeuft. Notwendig, weil
  # Namenskuerzel unterschiedlich lang sind (z. B. "ab" gegenueber "mabr").
  fit_text <- function(x, y, txt, cell_w, col, font = 1, base_cex = 0.46,
                       min_cex = 0.24) {
    txt <- fix_uplus(txt)
    if (!nzchar(txt)) return(invisible(NULL))
    avail <- cell_w * 0.88
    cex <- base_cex
    w <- strwidth(txt, cex = cex, font = font)
    if (w > avail && w > 0) cex <- max(min_cex, cex * avail / w)
    # Passt es selbst beim kleinsten Schriftgrad nicht, wird gekuerzt.
    if (strwidth(txt, cex = cex, font = font) > avail) {
      while (nchar(txt) > 1 && strwidth(txt, cex = cex, font = font) > avail)
        txt <- substr(txt, 1, nchar(txt) - 1)
    }
    text(x, y, txt, cex = cex, col = col, font = font)
  }

  # ---- Zeilenumbruch: Aufgaben in Bloecke aufteilen, die auf eine Seite
  # passen. Leere Zeilen (weder Abschnitt noch Aufgabe) werden verworfen,
  # damit keine Luecken entstehen.
  keep <- nzchar(trimws(as.character(table_data$Header))) |
          nzchar(trimws(as.character(table_data$Task)))
  row_ids <- which(keep)
  if (!length(row_ids)) row_ids <- 1L

  y_top <- 86.5; foot_reserve <- 12
  avail_h <- y_top - (foot_reserve + 2)
  # Zeilenhoehe so waehlen, dass moeglichst wenige Seiten entstehen, ohne die
  # Schrift unleserlich zu machen (+1 fuer die Tages-Kopfzeile).
  row_h <- max(1.9, min(3.0, avail_h / (length(row_ids) + 1)))
  rows_per_page <- max(5L, floor(avail_h / row_h) - 1L)

  chunk_rows <- function(ids) {
    if (length(ids) <= rows_per_page) return(list(ids))
    split(ids, ceiling(seq_along(ids) / rows_per_page))
  }
  row_chunks <- chunk_rows(row_ids)

  # Zu welchem Abschnitt gehoert eine Zeile (fuer die Wiederholung der
  # Ueberschrift auf Folgeseiten).
  section_of <- character(nrow(table_data))
  cur <- ""
  for (i in seq_len(nrow(table_data))) {
    h <- trimws(as.character(table_data$Header[i]))
    if (!is.na(h) && nzchar(h)) cur <- h
    section_of[i] <- cur
  }

  opened <- tryCatch({
    cairo_pdf(file, width = 11.69, height = 8.27, onefile = TRUE); TRUE
  }, error = function(e) FALSE)
  if (!opened) pdf(file, width = 11.69, height = 8.27)
  on.exit(try(dev.off(), silent = TRUE), add = TRUE)

  n_chunks <- length(row_chunks)
  n_pages  <- n_chunks
  page_no  <- 0L

  draw_page <- function(days_vec, ids, chunk_idx) {
    page_no <<- page_no + 1L
    par(mar = c(0.4, 0.4, 0.4, 0.4), oma = c(0, 0, 0, 0))
    plot.new(); plot.window(xlim = c(0, 100), ylim = c(0, 100), xaxs = "i", yaxs = "i")

    # Kopfleiste
    rect(0, 93.5, 100, 100, col = col_primary, border = NA)
    text(1.5, 96.7, "WARTUNGSPLAN", col = "white", cex = 1.45, font = 2, adj = 0)
    text(98.5, 97.4, month_str, col = "white", cex = 1.05, font = 2, adj = 1)
    sub_lbl <- if (n_pages > 1) sprintf("Monats\u00fcbersicht \u2013 Teil %d/%d",
                                        chunk_idx, n_pages) else "Monats\u00fcbersicht"
    text(98.5, 94.8, sub_lbl, col = "white", cex = 0.72, adj = 1)

    # Geraet + Metadaten
    text(1.5, 91.6, device_label, cex = 1.02, font = 2, adj = 0, col = col_primary)
    meta <- paste0("Erstellt: ", report_date,
                   if (nzchar(version_str)) paste0("   Version: ", version_str) else "")
    text(98.5, 91.6, meta, cex = 0.78, adj = 1, col = "#555555")
    if (length(serials)) {
      sn_txt <- paste(vapply(names(serials), function(n) {
        sn <- serials[[n]]$sn %||% ""
        if (nzchar(sn)) paste0(n, ": SN ", sn) else n
      }, character(1)), collapse = "    |    ")
      if (nzchar(sn_txt)) text(1.5, 89.2, sn_txt, cex = 0.68, adj = 0, col = "#333333")
    }
    segments(1.5, 88.0, 98.5, 88.0, col = col_accent, lwd = 1.4)

    # Tabellengeometrie. Bei der Monatsuebersicht stehen bis zu 31 Spalten
    # nebeneinander, deshalb faellt die Aufgabenspalte schmaler aus.
    x_left <- 1.5; x_right <- 98.5
    task_w <- 36
    grid_x0 <- x_left + task_w
    nd <- length(days_vec)
    cell_w <- (x_right - grid_x0) / max(nd, 1)

    # Kopfzeile: Tagesnummern + Wochentagskuerzel
    rect(x_left, y_top - row_h, x_right, y_top, col = col_primary, border = NA)
    text(x_left + 0.8, y_top - row_h / 2, "Aufgabe", col = "white",
         cex = 0.7, font = 2, adj = 0)
    for (k in seq_len(nd)) {
      d  <- days_vec[k]
      cx <- grid_x0 + (k - 0.5) * cell_w
      if (is_weekend(d))
        rect(grid_x0 + (k - 1) * cell_w, y_top - row_h,
             grid_x0 + k * cell_w, y_top, col = col_weekend, border = NA)
      text(cx, y_top - row_h * 0.35, as.character(d), col = "white",
           cex = if (nd > 24) 0.5 else 0.6, font = 2)
      wl <- wd_letter(d)
      if (nzchar(wl)) text(cx, y_top - row_h * 0.75, wl, col = "#bcd3ea",
                           cex = if (nd > 24) 0.36 else 0.42)
    }

    # Trennlinien zwischen den Kalenderwochen: bei 31 Spalten nebeneinander
    # laesst sich die Tabelle sonst nur schwer zeilenweise lesen.
    wk_breaks <- integer(0)
    if (nd > 1) {
      wks <- vapply(days_vec, day_week, integer(1))
      if (!all(is.na(wks)))
        wk_breaks <- which(c(FALSE, wks[-1] != wks[-nd] & !is.na(wks[-1]) & !is.na(wks[-nd])))
    }

    yr <- y_top - row_h

    # Auf Folgeseiten den laufenden Abschnitt als Kontext wiederholen.
    first_id <- ids[1]
    lead_sec <- section_of[first_id]
    starts_with_header <- nzchar(trimws(as.character(table_data$Header[first_id])))
    if (chunk_idx > 1 && nzchar(lead_sec) && !starts_with_header) {
      yb <- yr - row_h
      rect(x_left, yb, x_right, yr, col = col_accent, border = NA)
      text(x_left + 0.8, (yr + yb) / 2, paste0(lead_sec, "  (Fortsetzung)"),
           col = "white", cex = 0.62, font = 2, adj = 0)
      yr <- yb
    }

    shade <- 0L
    for (ri in ids) {
      hdr  <- as.character(table_data$Header[ri]); if (is.na(hdr)) hdr <- ""
      task <- as.character(table_data$Task[ri]);   if (is.na(task)) task <- ""
      yb <- yr - row_h
      if (nzchar(trimws(hdr))) {
        rect(x_left, yb, x_right, yr, col = col_accent, border = NA)
        text(x_left + 0.8, (yr + yb) / 2, fix_uplus(hdr), col = "white",
             cex = 0.62, font = 2, adj = 0)
        shade <- 0L
      } else {
        shade <- shade + 1L
        fill <- if ((shade %% 2L) == 0L) col_row_alt else "white"
        rect(x_left, yb, x_right, yr, col = fill, border = col_border, lwd = 0.4)
        # Wochenendspalten leicht hinterlegen, damit sich die Spalten auf dem
        # Ausdruck leichter zuordnen lassen.
        for (k in seq_len(nd)) if (is_weekend(days_vec[k]))
          rect(grid_x0 + (k - 1) * cell_w, yb, grid_x0 + k * cell_w, yr,
               col = "#dde6f1", border = NA)
        lab <- fix_uplus(task)
        maxch <- 56
        if (nchar(lab) > maxch) lab <- paste0(substr(lab, 1, maxch - 1), "\u2026")
        text(x_left + 0.8, (yr + yb) / 2, lab, cex = 0.5, adj = 0, col = "#1a1a1a")
        for (k in seq_len(nd)) {
          d   <- days_vec[k]
          cxl <- grid_x0 + (k - 1) * cell_w
          segments(cxl, yb, cxl, yr, col = col_border, lwd = 0.3)
          dc  <- as.character(d)
          val <- if (dc %in% names(table_data)) as.character(table_data[[dc]][ri]) else ""
          if (!is.na(val) && nzchar(val)) {
            cx <- grid_x0 + (k - 0.5) * cell_w
            if (startsWith(val, "\u2713") || startsWith(val, "X")) {
              # Erledigt: statt eines Hakens das Namenskuerzel der Person
              # eintragen, die abgezeichnet hat. Ist ausnahmsweise kein
              # Kuerzel hinterlegt (z. B. Altbestand), bleibt ein Haken.
              who <- lookup_ini(ri, d)
              if (nzchar(who)) {
                fit_text(cx, (yr + yb) / 2, who, cell_w, col_done, font = 2)
              } else {
                cy <- (yr + yb) / 2
                hw <- min(cell_w * 0.22, row_h * 0.42)
                hh <- row_h * 0.30
                lines(c(cx - hw, cx - hw * 0.25, cx + hw),
                      c(cy + hh * 0.05, cy - hh, cy + hh),
                      col = col_done, lwd = 1.6, lend = 1, ljoin = 1)
              }
            } else {
              fit_text(cx, (yr + yb) / 2, sub(" .*$", "", val), cell_w, col_note)
            }
          }
        }
        segments(grid_x0 + nd * cell_w, yb, grid_x0 + nd * cell_w, yr,
                 col = col_border, lwd = 0.3)
      }
      yr <- yb
    }

    # Wochentrenner ueber die gesamte Tabellenhoehe nachziehen.
    if (length(wk_breaks)) {
      y_bot_tbl <- yr
      for (k in wk_breaks)
        segments(grid_x0 + (k - 1) * cell_w, y_bot_tbl,
                 grid_x0 + (k - 1) * cell_w, y_top, col = col_accent, lwd = 0.9)
    }
    segments(grid_x0, yr, grid_x0, y_top, col = col_accent, lwd = 0.9)

    # Legende (unten links)
    if (!is.null(option_meanings) && length(option_meanings)) {
      ly <- foot_reserve - 1.5
      text(x_left, ly + 1.2, "Legende:", cex = 0.6, font = 2, adj = 0, col = col_primary)
      text(x_left + 8.5, ly + 1.2,
           "Namensk\u00fcrzel im Tagesfeld = Aufgabe von dieser Person erledigt.",
           cex = 0.45, adj = 0, col = col_done, font = 3)
      codes <- fix_uplus(names(option_meanings))
      per_col <- ceiling(length(codes) / 2)
      col_w <- 34
      for (i in seq_along(codes)) {
        coln <- (i - 1) %/% per_col
        rown <- (i - 1) %%  per_col
        text(x_left + coln * col_w, ly - rown * 1.7,
             sprintf("%s = %s", codes[i], fix_uplus(option_meanings[[i]])),
             cex = 0.42, adj = 0, col = "#333333")
      }
    }

    # Unterschriftszeile (unten rechts)
    sig_y <- 3.5
    segments(72, sig_y, 88, sig_y, col = "#333333", lwd = 0.6)
    text(80, sig_y - 1.4, "Datum", cex = 0.5, col = "#555555")
    segments(90, sig_y, 98.5, sig_y, col = "#333333", lwd = 0.6)
    text(94.25, sig_y - 1.4, "Unterschrift", cex = 0.5, col = "#555555")

    if (nzchar(footer_text)) text(x_left, 0.8, footer_text, cex = 0.5, adj = 0, col = "#888888")
    text(98.5, 0.8, sprintf("Seite %d von %d", page_no, n_pages),
         cex = 0.5, adj = 1, col = "#888888")
  }

  for (cidx in seq_along(row_chunks))
    draw_page(days_all, row_chunks[[cidx]], cidx)

  invisible(file)
}

# ---- Overlay builder for export ----
build_overlayed_table <- function(df, cell_status_df) {
  out <- df
  if (!nrow(cell_status_df)) return(order_cols(out))
  
  for (i in seq_len(nrow(cell_status_df))) {
    r <- cell_status_df$row_index[i]
    d <- as.character(cell_status_df$day[i])
    if (r >= 1 && r <= nrow(out) && d %in% names(out) && out$Header[r] == "") {
      out[r, d] <- cell_status_df$value_text[i]
    }
  }
  
  # Clean up data for PDF export - remove empty columns and ensure proper encoding
  # Replace any problematic characters
  out[] <- lapply(out, function(x) {
    if (is.character(x)) {
      # Convert to UTF-8 and replace problematic characters
      x <- iconv(x, to = "UTF-8", sub = "")
      # Replace checkmarks with simpler character if needed
      x <- gsub("\u2713", "X", x)  # Replace checkmark with X
      return(x)
    }
    return(x)
  })
  
  order_cols(out)
}

# Every header text that represents an actual schedule. Kept in sync with
# is_header_due_on() in the server and the sched_styles map in the daily
# task list.
# (Defined at top level so the Monatsuebersicht grid can use it too.)
SCHEDULE_HEADER_NAMES <- c(
  "Täglich", "Täglich (ZL)", "Täglich (Ablesen zwischen 12:00 und 14:00 Uhr)",
  "Montag und Donnerstag",
  "Wöchentlich", "Wöchentlich (Montag)", "Wöchentlich (Dienstag)",
  "Wöchentlich (Mittwoch)",
  "Wöchentlich (Mittwoch, TD)",
  "Wöchentlich (Donnerstag)", "Wöchentlich (Freitag)",
  "Wöchentlich (Freitag, ZL)", "14-tägig", "14-tägig (Mittwoch)",
  "Monatlich", "Monatlich (Freitag)", "Monatlich oder alle 2500 Proben",
  "Quartalsweise", "Alle 3 Monate oder alle 7500 Proben",
  "Am ersten Dienstag im Monat", "Am ersten Freitag im Monat",
  "Monatlich (Freitag, alle 4 Wochen)", "Monatlich (Dienstag, alle 4 Wochen)",
  "Monatlich (Mittwoch, alle 4 Wochen)",
  "Bei Bedarf", "Wartung bei Bedarf", "Nach jeder Migration:",
  "Montag", "Dienstag", "Mittwoch", "Donnerstag", "Freitag", "Samstag", "Sonntag"
)
# Schedules with no real due date ("as needed") -- never due, never overdue.
SCHEDULE_HEADERS_NO_DUE_DATE <- c("Bei Bedarf", "Wartung bei Bedarf")

# ---- Handsontable renderer (per-cell locking) ----
cells_readonly_for_headers <- function(df, current_user_initials, cell_status_df,
                                       role = "user", invalid_days = integer(),
                                       month = as.integer(format(Sys.Date(), "%m")),
                                       year  = as.integer(format(Sys.Date(), "%Y")),
                                       read_only_all = FALSE,
                                       remarks_df = NULL) {
  df_show <- overlay_for_render(df, cell_status_df)
  
  # ---- Space-saving display merge: Header + Aufgabe -> single column --------
  # To reclaim horizontal width (so more day columns fit without scrolling),
  # the grid DISPLAYS one merged "Aufgabe" column: header rows show the
  # schedule header text, task rows show the task text. This is purely a
  # render-time transform -- the underlying df (Header, Task, 1..31) is
  # untouched, so all due-date logic, history alignment, save paths, admin
  # tools and the PDF export keep working exactly as before.
  #
  # df_show      : original 2-column structure -> drives all locking/coloring
  #                logic below (header_rows via df_show$Header, etc.)
  # df_render    : what rhandsontable actually shows -> merged single column
  #                + day columns. Column indices for hot_cell() must be taken
  #                relative to df_render, so day-column names are identical
  #                and a "Merge" column replaces the Header+Task pair.
  is_header_row <- nzchar(df_show$Header)
  merged_label  <- ifelse(is_header_row, df_show$Header, df_show$Task)
  day_cols      <- intersect(as.character(1:31), names(df_show))
  df_render <- data.frame(Aufgabe = merged_label, stringsAsFactors = FALSE,
                          check.names = FALSE)
  for (d in day_cols) df_render[[d]] <- df_show[[d]]
  
  # Build a per-cell tooltip map (row_col -> "von X · timestamp [+ Bemerkung]")
  # so the who/when info shows as a native cell hover-title anchored EXACTLY
  # to the day cell that holds the checkmark -- keeping the mark and its
  # attribution together in one cell, instead of the detached comment popup
  # that could drift to the top of the table. Keyed by 0-based row and the
  # day-column NAME, matched in the renderer via instance.getColHeader / prop.
  cell_tooltip <- list()
  if (!missing(cell_status_df) && !is.null(cell_status_df) && nrow(cell_status_df)) {
    # Format all timestamps in ONE call. Formatting them one by one (with a
    # timezone lookup each time) took ~80% of the grid build time -- about
    # a second for a full month of entries, on every re-render.
    when_all <- tryCatch(
      format(as.POSIXct(cell_status_df$updated_at, tz = "Europe/Berlin"),
             "%Y-%m-%d %H:%M %Z"),
      error = function(e) rep("", nrow(cell_status_df)))
    when_all[is.na(when_all)] <- ""
    for (i in seq_len(nrow(cell_status_df))) {
      rr <- cell_status_df$row_index[i]
      dd <- as.character(cell_status_df$day[i])
      who <- cell_status_df$updated_by[i] %||% ""
      if (rr < 1 || rr > nrow(df_render)) next
      if (!(dd %in% day_cols)) next
      cell_tooltip[[paste0(rr - 1, "_", dd)]] <- sprintf("von %s \u00b7 %s", who, when_all[i])
    }
  }
  
  # German-friendly column headers:
  #   merged "Aufgabe" column -> "Aufgabe"
  #   day columns 1..31 -> day number with the weekday BELOW it (as in the
  #   PDF report), using the selected month/year. Weekends are highlighted.
  if (length(month) != 1L || is.na(month)) month <- as.integer(format(Sys.Date(), "%m"))
  if (length(year)  != 1L || is.na(year))  year  <- as.integer(format(Sys.Date(), "%Y"))
  display_names <- names(df_render)
  display_names[display_names == "Aufgabe"] <- "Aufgabe"
  de_wd <- c("So", "Mo", "Di", "Mi", "Do", "Fr", "Sa")
  weekend_days <- character(0)   # day columns shaded like in the PDF
  for (d in 1:31) {
    key <- as.character(d)
    idx <- which(display_names == key)
    if (!length(idx)) next
    dt <- tryCatch(
      suppressWarnings(as.Date(sprintf("%04d-%02d-%02d", year, month, d))),
      error = function(e) as.Date(NA_character_)
    )
    if (length(dt) != 1L || is.na(dt) || as.integer(format(dt, "%d")) != d) {
      display_names[idx] <- key
    } else {
      wd <- as.POSIXlt(dt)$wday + 1L
      is_we <- wd %in% c(1L, 7L)   # So / Sa
      if (is_we) weekend_days <- c(weekend_days, key)
      display_names[idx] <- sprintf(
        "%d<br><span style=\"font-size:10px; font-weight:normal; color:%s;\">%s</span>",
        d, if (is_we) "#c62828" else "#6c757d", de_wd[wd])
    }
  }
  
  # Rueckmeldung aus dem Labor: beim Scrollen waren weder die Aufgabenspalte
  # noch die Tagesueberschriften noch sichtbar. Zwei Massnahmen:
  #  1) `height` -- damit scrollt Handsontable in seinem eigenen Viewport.
  #     Nur dann bleibt die Kopfzeile mit den Tagen oben haften. Ohne Hoehe
  #     rendert das Widget in voller Laenge und der aeussere Container
  #     scrollt, wodurch die Kopfzeile mit nach oben wegwandert.
  #  2) `fixedColumnsLeft = 1` -- friert die Spalte "Aufgabe" links ein, so
  #     dass beim horizontalen Scrollen ueber 31 Tage immer sichtbar bleibt,
  #     zu welcher Aufgabe eine Zelle gehoert.
  # Rubrikzeilen werden NICHT mehr ueber ihren Text erkannt. Frueher glich der
  # Renderer den Zellwert gegen eine fest verdrahtete Liste von Zeitplan-Namen
  # ab; jede Rubrik, die dort fehlte (z. B. Arbeitstaeglich, PFA (SN 00398),
  # O neg-Konserven ...), blieb deshalb weiss und war nicht als Rubrik
  # erkennbar. Jetzt liefert R die Zeilennummern direkt -- damit ist jede
  # Rubrik eingefaerbt, egal wie sie heisst.
  # Zusaetzlich: Zeilen, deren Text exakt ein Zeitplan-Name ist (z. B.
  # "Montag und Donnerstag"), die aber in der Spalte Aufgabe statt Header
  # gespeichert sind -- im Plan sind das Rubriken, also gleiche Hervorhebung.
  # Nur Anzeige; die gespeicherten Daten bleiben unveraendert.
  HEADER_ROW_BLUE   <- "#003b73"
  looks_like_header <- is_header_row |
    trimws(as.character(df_show$Task)) %in% SCHEDULE_HEADER_NAMES
  header_rows_json  <- jsonlite::toJSON(as.integer(which(looks_like_header) - 1L))

  # columnHeaderHeight: Platz fuer die zweizeilige Tageskopfzeile.
  rh <- rhandsontable(df_render, stretchH = "all",
                      fixedColumnsLeft = 1, height = 600,
                      columnHeaderHeight = 38) %>%
    # Zusammengefuehrte Spalte Aufgabe: Rubrikzeilen bekommen einen
    # einheitlichen blauen Balken in der Markenfarbe, Aufgabenzeilen bleiben
    # schlicht und leicht eingerueckt, damit sie sichtbar unter ihrer Rubrik
    # haengen. Sortierung ist fuer diese Spalte aus -- Sortieren wuerde die
    # Zuordnung Rubrik -> Aufgaben zerreissen.
    hot_col(
      "Aufgabe",
      readOnly = TRUE,
      renderer = htmlwidgets::JS(sprintf("
        function (instance, td, row, col, prop, value, cellProperties) {
          Handsontable.renderers.TextRenderer.apply(this, arguments);
          var headerRows = %s;
          var isHeader = headerRows.indexOf(row) !== -1;
          if (isHeader) {
            td.style.background = '%s';
            td.style.color = '#ffffff';
            td.style.fontWeight = 'bold';
            td.style.paddingLeft = '6px';
            td.style.whiteSpace = 'normal';
          } else {
            td.style.background = '#ffffff';
            td.style.color = '#212529';
            td.style.fontWeight = 'normal';
            td.style.paddingLeft = '18px';
          }
        }
      ", header_rows_json, HEADER_ROW_BLUE))
    ) %>%
    hot_cols(columnSorting = FALSE, manualColumnResize = TRUE) %>%
    hot_table(highlightCol = TRUE, highlightRow = TRUE, rowHeaders = FALSE, comments = TRUE,
              readOnly = isTRUE(read_only_all))
  
  # Attach native hover-title tooltips (who/when) to the day cells, anchored
  # exactly to each cell so the checkmark and its attribution stay together.
  tooltip_json <- if (length(cell_tooltip)) {
    jsonlite::toJSON(cell_tooltip, auto_unbox = TRUE)
  } else {
    "{}"   # empty object (not []) so hasOwnProperty is always safe
  }
  
  # Bemerkungen sichtbar machen (Rueckmeldung aus dem Labor: ein Kommentar
  # zu "erledigt" oder "nicht erledigt" soll auch in der Monatsuebersicht
  # erscheinen). Jede Zelle mit Bemerkung bekommt ein blaues Stift-Symbol,
  # der Text steht im Hover-Hinweis (zusammen mit wer/wann). Ersetzt die
  # frueheren Kommentar-Boxen (rotes Eck), die man leicht uebersehen hat.
  remark_tips <- list()
  if (!is.null(remarks_df) && is.data.frame(remarks_df) && nrow(remarks_df)) {
    for (i in seq_len(nrow(remarks_df))) {
      rr <- suppressWarnings(as.integer(remarks_df$row_index[i]))
      dd <- as.character(suppressWarnings(as.integer(remarks_df$day[i])))
      if (is.na(rr) || rr < 1 || rr > nrow(df_show) || !(dd %in% day_cols)) next
      if (nzchar(df_show$Header[rr])) next
      txt <- trimws(as.character(remarks_df$remark_text[i] %||% ""))
      opt <- trimws(as.character(remarks_df$option_code[i] %||% ""))
      if (is.na(txt)) txt <- ""
      if (is.na(opt)) opt <- ""
      if (!nzchar(txt) && !nzchar(opt)) next
      who <- as.character(remarks_df$updated_by[i] %||% "")
      if (is.na(who)) who <- ""
      label <- trimws(paste(if (nzchar(opt)) sprintf("[%s]", opt) else "", txt))
      remark_tips[[paste0(rr - 1L, "_", dd)]] <-
        if (nzchar(who)) sprintf("Bemerkung von %s: %s", who, label)
        else sprintf("Bemerkung: %s", label)
    }
  }
  remark_json <- if (length(remark_tips)) {
    jsonlite::toJSON(remark_tips, auto_unbox = TRUE)
  } else "{}"
  
  day_renderer <- htmlwidgets::JS(sprintf("
    function (instance, td, row, col, prop, value, cellProperties) {
      Handsontable.renderers.TextRenderer.apply(this, arguments);
      var tips = %s;
      var weekend = %s;
      var hdrRows = %s;
      var rem = %s;
      var key = row + '_' + prop;   // prop is the column name (the day number)
      td.title = '';
      td.style.cursor = '';
      if (hdrRows.indexOf(row) !== -1) {
        // Rubrikzeile: ganze Zeile blau hinterlegen (wie im PDF)
        td.style.background = '%s';
        td.style.textAlign = 'center';
        return;
      }
      if (tips && tips.hasOwnProperty(key)) {
        td.title = tips[key];
        td.style.cursor = 'help';
      }
      if (rem && rem.hasOwnProperty(key)) {
        // Bemerkung: blaues Stift-Symbol + Text im Hover-Hinweis
        td.title = (td.title ? td.title + '\\n' : '') + rem[key];
        td.style.cursor = 'help';
        var m = document.createElement('span');
        m.textContent = ' \u270e';
        m.style.color = '#1565c0';
        m.style.fontWeight = '700';
        td.appendChild(m);
      }
      // Wochenendspalten leicht hinterlegen (wie im PDF)
      td.style.background = (weekend.indexOf(String(prop)) !== -1) ? '#eef3f8' : '';
      td.style.textAlign = 'center';
    }
  ", tooltip_json, jsonlite::toJSON(weekend_days), header_rows_json, remark_json,
     HEADER_ROW_BLUE))
  for (d in day_cols) {
    rh <- rh %>% hot_col(d, renderer = day_renderer)
  }
  
  
  # ---- Per-cell settings (read-only locks + comment boxes) ----------------
  # Collected in one lookup and handed to Handsontable in a single step at
  # the end. Previously every lock/comment was a separate hot_cell() call;
  # each call copies the whole settings list, so with ~31 locks per header
  # row plus every entry the grid took 0.3-0.6 s to build -- on every
  # re-render, i.e. after every tick. Also, rhandsontable >= 0.3.8 treats
  # hot_cell()'s row as 1-based and subtracts 1 itself, while this code
  # passed an already 0-based row -> locks and Bemerkungs-Kommentare landed
  # one row too high. Here row/col are written directly as 0-based indices,
  # which is what Handsontable expects, independent of the package version.
  cell_cfg <- new.env(hash = TRUE, parent = emptyenv())
  day_col0 <- setNames(match(names(df_render), names(df_render)) - 1L, names(df_render))
  set_cell <- function(r, d, readOnly = NULL, comment = NULL) {
    d <- as.character(d)
    if (is.na(day_col0[d])) return(invisible(NULL))
    k <- paste0(r, "_", d)
    cur <- if (exists(k, envir = cell_cfg, inherits = FALSE))
      get(k, envir = cell_cfg, inherits = FALSE)
    else list(row = as.integer(r) - 1L, col = unname(day_col0[d]))
    if (!is.null(readOnly)) cur$readOnly <- readOnly
    if (!is.null(comment)) {
      prev <- cur$comment$value %||% ""
      cur$comment <- list(value = if (nzchar(prev)) paste(prev, comment, sep = "\n") else comment)
    }
    assign(k, cur, envir = cell_cfg)
    invisible(NULL)
  }
  
  # Header ROWS: day cells are read-only (no data entry there)
  header_rows <- which(df_show$Header != "")
  for (r in header_rows) for (d in day_cols) set_cell(r, d, readOnly = TRUE)
  
  # lock invalid days
  if (length(invalid_days)) {
    for (d in as.character(invalid_days)) {
      if (d %in% day_cols) {
        for (r in seq_len(nrow(df_show))) {
          set_cell(r, d, readOnly = TRUE, comment = "Ungültig für diesen Monat")
        }
      }
    }
  }
  
  # per-cell locks
  if (!missing(cell_status_df) && nrow(cell_status_df)) {
    for (i in seq_len(nrow(cell_status_df))) {
      rr  <- cell_status_df$row_index[i]
      dd  <- as.character(cell_status_df$day[i])
      who <- cell_status_df$updated_by[i]
      if (!(dd %in% names(df_show))) next
      if (rr < 1 || rr > nrow(df_show)) next
      if (df_show$Header[rr] != "") next
      if (!identical(who, current_user_initials) && role != "admin") {
        set_cell(rr, dd, readOnly = TRUE)
      }
      # who/when and any Bemerkung are shown as the cell's hover text and
      # a marker (see cell_tooltip / remark_tips above).
    }
  }
  
  cell_list <- unname(mget(ls(cell_cfg), envir = cell_cfg))
  if (length(cell_list)) {
    rh$x$cell <- c(rh$x$cell, cell_list)
    if (any(vapply(cell_list, function(x) !is.null(x$comment), logical(1))))
      rh <- rh %>% hot_table(enableComments = TRUE)
  }
  
  # Tageszahl + Wochentag als Spaltenkopf setzen. Bewusst erst ganz am Ende:
  # hot_col()/hot_cell() suchen Spalten ueber ihren Kopf ("1".."31") -- mit
  # den neuen Koepfen wuerden sie nichts mehr finden. Frueher wurde
  # colHeaders an hot_cols() uebergeben; das reicht den Wert aber nur an die
  # einzelnen Spalten weiter, wo Handsontable ihn ignoriert -- deshalb waren
  # die Wochentage nie zu sehen.
  rh$x$colHeaders <- display_names

  rh
}


# Overlay helper used above
overlay_for_render <- function(df, cell_status_df) {
  out <- df
  if (!nrow(cell_status_df)) return(order_cols(out))
  for (i in seq_len(nrow(cell_status_df))) {
    rr <- cell_status_df$row_index[i]
    dd <- as.character(cell_status_df$day[i])
    if (rr >= 1 && rr <= nrow(out) && dd %in% names(out) && out$Header[rr] == "") {
      out[rr, dd] <- cell_status_df$value_text[i]
    }
  }
  order_cols(out)
}

# ----------------------------- DASHBOARD UI -----------------------------------
header <- dashboardHeader(
  title = "Wartungsplan",
  tags$li(
    class = "dropdown",
    uiOutput("logout_ui", container = tags$span,
             style = "display:inline-block; padding: 10px 15px;")
  )
)

sidebar <- dashboardSidebar(
  sidebarMenuOutput("sidebar_menu")
)

body <- dashboardBody(
  useShinyjs(),
  use_theme(apptheme),
  tags$style(HTML(paste0("
    .btn.btn-primary { color: #fff !important; }
    .btn.btn-primary{ background:", DARK_BLUE, "; border-color:", DARK_BLUE, "; }
    .btn.btn-primary:hover{ filter:brightness(0.9); }
    .badge{ background:", DARK_BLUE, "; }
    h1,h2,h3,h4{ color:", DARK_BLUE, "; }

    /* Monatsuebersicht: Tageszahl mit Wochentag darunter (wie im PDF) */
    #tableRH .handsontable thead th { height: 38px; line-height: 1.15; vertical-align: middle; }
    #tableRH .handsontable thead th .colHeader { white-space: normal; }

    /* yellow highlight for header rows in the handsontable */
    .header-yellow {
      background-color: #fff7b2 !important;
      font-weight: 600;
    }

    .handsontable td.htYellowHeader {
      background-color: #fff7b2 !important;
      font-weight: 700 !important;
    }

    /* Split screen layout */
    .split-container {
      display: flex;
      gap: 20px;
      height: calc(100vh - 200px);
      margin-top: 20px;
    }

    .left-panel {
      flex: 0 0 550px;
      background: white;
      border-radius: 8px;
      box-shadow: 0 2px 8px rgba(0,0,0,0.1);
      padding: 20px;
      overflow-y: auto;
      border: 2px solid #3c8dbc;
    }

    .right-panel {
      flex: 1;
      background: white;
      border-radius: 8px;
      box-shadow: 0 2px 8px rgba(0,0,0,0.1);
      padding: 20px;
      overflow: auto;
      border: 2px solid #00a65a;
    }

    /* Task list styling */
    .task-header {
      background: linear-gradient(135deg, #FFC000 0%, #FFD700 100%);
      padding: 12px;
      margin: -5px 0 10px 0;
      border-radius: 5px;
      font-weight: 700;
      font-size: 14px;
      color: #333;
      box-shadow: 0 2px 4px rgba(0,0,0,0.1);
      border-left: 4px solid #FF8C00;
    }

    .task-header.device-header {
      background: linear-gradient(135deg, #003B73 0%, #136377 100%);
      color: white;
      border-left: 4px solid #003B73;
    }

    .task-header.device-header {
      background: linear-gradient(135deg, #003B73 0%, #136377 100%);
      color: white;
      border-left: 4px solid #003B73;
    }

    /* Click-to-expand section (currently used for the Cobas 8100 devices Bei Bedarf tasks): the header becomes a summary the user clicks to
       reveal the tasks underneath instead of them always taking up space. */
    .task-collapse { margin-bottom: 4px; }
    .task-collapse > summary {
      cursor: pointer;
      list-style: none;
      position: relative;
    }
    .task-collapse > summary::-webkit-details-marker { display: none; }
    .task-collapse > summary .task-header::after {
      content: \"▸\";
      float: right;
      font-size: 16px;
      transition: transform 0.15s ease;
    }
    .task-collapse[open] > summary .task-header::after { transform: rotate(90deg); }
    .task-collapse > summary .task-header {
      display: flex;
      align-items: center;
      justify-content: space-between;
    }

    .task-item {
      background: #f9f9f9;
      border-left: 3px solid #3c8dbc;
      margin-bottom: 12px;
      padding: 15px;
      border-radius: 5px;
      transition: all 0.3s ease;
    }

    .task-item:hover {
      background: #f0f8ff;
      box-shadow: 0 2px 6px rgba(60, 141, 188, 0.2);
      transform: translateX(3px);
    }

    .task-item.completed {
      border-left-color: #00a65a;
      background: #f0fff4;
    }

    .task-item.nicht-erledigt {
      border-left-color: #d32f2f !important;
      background: #fdecea;
    }
    .task-item.nicht-erledigt .task-name {
      color: #a02218;
    }

    .task-name {
      font-weight: 600;
      color: #333;
      margin-bottom: 10px;
      font-size: 13px;
    }

    .task-controls {
      display: flex;
      gap: 10px;
      align-items: center;
      flex-wrap: nowrap;
    }

    /* Ablauf je Aufgabe (Rueckmeldung aus dem Labor: der Ablauf war fuer
       die Nutzer nicht klar). Drei Zustaende, vom Server beim Aufbau und
       per JS sofort beim Klicken gesetzt:
         state-open    : nur 'Erledigt' oder 'Nicht erledigt' waehlen
         state-done    : erledigt -> optionales Kommentarfeld zur Erledigung
         state-notdone : nicht erledigt -> Grund (Pflicht) + Bemerkung      */
    .task-controls.state-open .task-select,
    .task-controls.state-open .task-remark { display: none; }
    .task-controls.state-done .task-select { display: none; }
    /* Grund noch nicht gewaehlt -> Auswahlfeld rot hervorheben */
    .task-controls.state-notdone .task-select.needs-reason select.form-control {
      border: 2px solid #d9534f;
      background-color: #fff5f5;
      box-shadow: 0 0 0 3px rgba(217, 83, 79, 0.15);
    }

    /* Compact dropdown so it doesn't dominate the row or visually
       clash with the rows below when the menu pops open. */
    .task-checkbox { flex: 0 0 auto; padding-left: 12px; padding-right: 8px; }
    .task-checkbox .form-group { margin-bottom: 0; }
    .task-nicht    { flex: 0 0 auto; }
    .task-select   { flex: 0 1 220px; min-width: 140px; }
    .task-select .form-group { margin-bottom: 0; }
    .task-remark   { flex: 1 1 110px; min-width: 0; display: flex; gap: 4px; align-items: center; }
    .task-remark .form-group { margin-bottom: 0; flex: 1; min-width: 0; }
    .task-remark input[type=text] {
      font-size: 13px;
      padding: 6px 10px;
      height: 36px;
    }
    .btn.btn-nicht-erledigt {
      background: #fff;
      color: #d9534f;
      border: 1px solid #d9534f;
      font-weight: 600;
      height: 36px;
      padding: 4px 8px;
      font-size: 12px;
      white-space: nowrap;
    }
    .btn.btn-nicht-erledigt:hover {
      background: #d9534f;
      color: #fff;
    }
    /* Active state: this task is currently marked NE. The button stays
       filled red so the open item is impossible to miss, and keeps that
       look until somebody sets the task to erledigt. */
    .btn.btn-nicht-erledigt.is-ne,
    .btn.btn-nicht-erledigt.is-ne:hover,
    .btn.btn-nicht-erledigt.is-ne:focus {
      background: #d32f2f;
      color: #fff;
      border-color: #b71c1c;
      box-shadow: 0 0 0 3px rgba(211, 47, 47, 0.22);
    }
    .btn.btn-save-remark {
      background: #28a745;
      color: #fff;
      border: 1px solid #218838;
      font-weight: 600;
      height: 36px;
      padding: 4px 8px;
      font-size: 12px;
      white-space: nowrap;
      flex: 0 0 auto;
    }
    .btn.btn-save-remark:hover { background: #218838; color: #fff; }

    /* Native <select> (selectize = FALSE) — opens the OS picker on
       tablets, so the menu can never overlap rows below. */
    .task-select select.form-control {
      height: 36px;
      padding: 4px 28px 4px 10px;
      font-size: 13px;
      line-height: 1.2;
      background-color: #fff;
      border: 1px solid #ccd5e0;
      border-radius: 4px;
      cursor: pointer;
      appearance: none;
      -webkit-appearance: none;
      background-image: url(\"data:image/svg+xml;utf8,<svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 16 16'><path fill='%23555' d='M4 6l4 5 4-5z'/></svg>\");
      background-repeat: no-repeat;
      background-position: right 8px center;
      background-size: 14px;
    }
    .task-select select.form-control:focus {
      border-color: #3c8dbc;
      outline: 0;
      box-shadow: 0 0 0 2px rgba(60,141,188,0.2);
    }

    /* When the user picks 'Nicht erledigt', highlight the remark box
       to make it obvious where the reason should be typed. */
    .task-remark.needs-remark input[type=text] {
      border: 2px solid #d9534f !important;
      background: #fff5f5;
      box-shadow: 0 0 0 3px rgba(217, 83, 79, 0.15);
    }
    .task-remark.needs-remark::before {
      content: \"Grund:\";
      font-size: 11px;
      color: #d9534f;
      font-weight: 600;
      margin-right: 4px;
      white-space: nowrap;
      flex: 0 0 auto;
    }
    .recent-remarks-alert {
      background: #fff3cd;
      border-left: 4px solid #f0ad4e;
      padding: 10px 14px;
      margin: 8px 0 14px 0;
      border-radius: 4px;
      font-size: 13px;
      color: #6b4f00;
    }
    .recent-remarks-alert h4 {
      margin: 0 0 6px 0;
      font-size: 13px;
      font-weight: 700;
      color: #8a6d00;
    }
    .recent-remarks-alert ul { margin: 0; padding-left: 20px; }
    .recent-remarks-alert li { margin-bottom: 2px; }

    .task-item input[type=checkbox] {
      width: 14px;
      height: 14px;
      cursor: pointer;
    }

    .task-item .checkbox {
      margin-top: 0;
      margin-bottom: 0;
      min-height: 0;
    }

    .task-item .checkbox label {
      font-size: 11px;
      font-weight: 500;
      color: #00a65a;
      padding-left: 4px;
      min-height: 0;
    }

    /* Date header styling */
    .date-header {
      background: linear-gradient(135deg, #3c8dbc 0%, #5dade2 100%);
      color: white;
      padding: 15px;
      border-radius: 8px;
      text-align: center;
      margin-bottom: 20px;
      box-shadow: 0 3px 6px rgba(0,0,0,0.15);
      font-size: 16px;
      font-weight: 700;
    }

    .visualization-header {
      background: linear-gradient(135deg, #00a65a 0%, #00c16e 100%);
      color: white;
      padding: 15px;
      border-radius: 8px;
      text-align: center;
      margin-bottom: 20px;
      box-shadow: 0 3px 6px rgba(0,0,0,0.15);
      font-size: 16px;
      font-weight: 700;
      position: relative;
    }

    /* Open-remarks dropdown anchored top-right of Monatsübersicht header */
    .open-remarks-wrap {
      position: absolute;
      top: 8px;
      right: 10px;
      z-index: 10;
    }
    .open-remarks-btn {
      background: rgba(255,255,255,0.18);
      color: #fff;
      border: 1px solid rgba(255,255,255,0.45);
      border-radius: 6px;
      padding: 5px 12px;
      font-size: 12px;
      font-weight: 600;
      cursor: pointer;
      display: inline-flex;
      align-items: center;
      gap: 6px;
      transition: background 0.2s ease;
    }
    .open-remarks-btn:hover { background: rgba(255,255,255,0.32); }
    .open-remarks-btn .badge-count {
      background: #d9534f;
      color: #fff;
      border-radius: 10px;
      padding: 1px 8px;
      font-size: 11px;
      font-weight: 700;
    }
    .open-remarks-panel {
      display: none;
      position: absolute;
      top: 38px;
      right: 0;
      width: 380px;
      max-height: 60vh;
      overflow-y: auto;
      background: #fff;
      color: #333;
      border: 1px solid #ccd5e0;
      border-radius: 8px;
      box-shadow: 0 8px 24px rgba(0,0,0,0.18);
      padding: 0;
      text-align: left;
      font-weight: 400;
      font-size: 12px;
    }
    .open-remarks-wrap.open .open-remarks-panel { display: block; }
    .open-remarks-section { border-bottom: 1px solid #eef1f5; }
    .open-remarks-section:last-child { border-bottom: none; }
    .open-remarks-section > .sec-head {
      padding: 8px 12px;
      font-weight: 700;
      font-size: 12px;
      color: #003B73;
      background: #f5f8fb;
      display: flex;
      justify-content: space-between;
      align-items: center;
      cursor: pointer;
      user-select: none;
    }
    .open-remarks-section > .sec-head .sec-count {
      background: #d9534f; color: #fff; border-radius: 10px;
      padding: 1px 7px; font-size: 11px;
    }
    .open-remarks-section > .sec-head .sec-count.zero {
      background: #b5bdc7;
    }
    .open-remarks-section .sec-body { padding: 6px 12px 10px 12px; }
    .open-remarks-section.collapsed .sec-body { display: none; }
    .open-remarks-section ul { margin: 0; padding-left: 18px; }
    .open-remarks-section li { margin-bottom: 3px; line-height: 1.35; }
    .open-remarks-empty { color: #888; font-style: italic; font-size: 11px; }

    .table-container {
      overflow: hidden;              /* Handsontable scrollt selbst -> Kopfzeile bleibt fixiert */
      border: 1px solid #ddd;
      border-radius: 5px;
    }

    /* Fixierte Aufgabenspalte und Tages-Kopfzeile optisch vom Rest abheben,
       damit erkennbar ist, dass sie beim Scrollen stehen bleiben.
       WICHTIG: hier KEIN background setzen. Die Aufgabenspalte wird von
       Handsontable in den Klon .ht_clone_left gespiegelt; eine Hintergrund-
       regel mit !important wuerde die Rubrikfarbe ueberschreiben, die der
       Zell-Renderer inline setzt (Rubriken erschienen sonst weiss). */
    .table-container .ht_clone_left td,
    .table-container .ht_clone_top_left_corner td {
      border-right: 2px solid #b0bec5 !important;
    }
    .table-container .ht_clone_top th {
      border-bottom: 2px solid #b0bec5 !important;
    }

    .quick-actions {
      display: flex;
      gap: 10px;
      margin-bottom: 15px;
      flex-wrap: wrap;
    }

    .quick-actions button {
      flex: 1;
      min-width: 150px;
    }

    .stats-bar {
      display: flex;
      gap: 15px;
      margin-bottom: 20px;
      flex-wrap: wrap;
    }

    .stat-box {
      flex: 1;
      background: linear-gradient(135deg, #f5f5f5 0%, #e8e8e8 100%);
      padding: 15px;
      border-radius: 8px;
      text-align: center;
      min-width: 100px;
      border: 2px solid #ddd;
    }

    .stat-number {
      font-size: 28px;
      font-weight: 700;
      color: #3c8dbc;
      display: block;
    }

    .stat-label {
      font-size: 11px;
      color: #666;
      text-transform: uppercase;
      font-weight: 600;
      margin-top: 5px;
    }

    /* Device Info Area Styling */
    .device-info-container {
      background: white;
      border-radius: 12px;
      box-shadow: 0 4px 12px rgba(0,0,0,0.08);
      padding: 25px;
      margin-bottom: 25px;
      border: 1px solid #e0e0e0;
    }

    .device-info-header {
      background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
      color: white;
      padding: 15px 20px;
      border-radius: 8px;
      margin: -25px -25px 20px -25px;
      display: flex;
      justify-content: space-between;
      align-items: center;
      box-shadow: 0 2px 8px rgba(102, 126, 234, 0.3);
    }

    .device-info-title {
      font-size: 18px;
      font-weight: 700;
      margin: 0;
    }

    .device-info-badge {
      background: rgba(255,255,255,0.2);
      padding: 5px 12px;
      border-radius: 20px;
      font-size: 11px;
      font-weight: 600;
      text-transform: uppercase;
    }

    .serial-section {
      background: #f8f9fa;
      border-radius: 8px;
      padding: 20px;
      margin-bottom: 20px;
      border: 1px solid #dee2e6;
    }

    .serial-section-title {
      color: #495057;
      font-size: 15px;
      font-weight: 700;
      margin-bottom: 15px;
      display: flex;
      align-items: center;
      gap: 8px;
    }

    .serial-grid {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
      gap: 15px;
    }

    .serial-input-wrapper {
      background: white;
      padding: 12px;
      border-radius: 6px;
      border: 1px solid #ced4da;
      transition: all 0.3s ease;
    }

    .serial-input-wrapper:hover {
      border-color: #667eea;
      box-shadow: 0 2px 8px rgba(102, 126, 234, 0.15);
    }

    .serial-input-wrapper label {
      color: #495057;
      font-weight: 600;
      font-size: 12px;
      margin-bottom: 5px;
    }

    .serial-input-wrapper input {
      border: 1px solid #e0e0e0;
      border-radius: 4px;
      font-family: 'Courier New', monospace;
      font-weight: 600;
      letter-spacing: 1px;
    }

    .info-section {
      background: #fff;
      border-radius: 8px;
      padding: 15px;
      margin-bottom: 15px;
    }

    .info-section label {
      color: #495057;
      font-weight: 600;
      font-size: 13px;
    }

    .device-image-preview {
      background: #f8f9fa;
      border-radius: 8px;
      padding: 15px;
      text-align: center;
      margin-bottom: 15px;
      border: 2px dashed #dee2e6;
    }

    .device-image-preview img {
      max-height: 120px;
      border-radius: 6px;
      box-shadow: 0 2px 8px rgba(0,0,0,0.1);
    }

    .action-buttons {
      display: flex;
      gap: 10px;
      flex-wrap: wrap;
      margin-top: 20px;
    }

    .action-buttons button {
      flex: 1;
      min-width: 150px;
      font-weight: 600;
      padding: 10px 20px;
      border-radius: 6px;
      transition: all 0.3s ease;
    }

    .action-buttons .btn-primary {
      background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
      border: none;
    }

    .action-buttons .btn-primary:hover {
      transform: translateY(-2px);
      box-shadow: 0 4px 12px rgba(102, 126, 234, 0.4);
    }

    .action-buttons .btn-info {
      background: linear-gradient(135deg, #3c8dbc 0%, #5dade2 100%);
      border: none;
      color: white;
    }

    .action-buttons .btn-info:hover {
      transform: translateY(-2px);
      box-shadow: 0 4px 12px rgba(60, 141, 188, 0.4);
    }

    .update-timestamp {
      color: #6c757d;
      font-size: 11px;
      font-style: italic;
      margin-top: 15px;
      padding-top: 15px;
      border-top: 1px solid #e0e0e0;
      text-align: right;
    }

    /* Scrollbar styling */
    .left-panel::-webkit-scrollbar,
    .right-panel::-webkit-scrollbar,
    .table-container::-webkit-scrollbar {
      width: 8px;
    }

    .left-panel::-webkit-scrollbar-track,
    .right-panel::-webkit-scrollbar-track,
    .table-container::-webkit-scrollbar-track {
      background: #f1f1f1;
      border-radius: 10px;
    }

    .left-panel::-webkit-scrollbar-thumb,
    .right-panel::-webkit-scrollbar-thumb,
    .table-container::-webkit-scrollbar-thumb {
      background: #888;
      border-radius: 10px;
    }

    .left-panel::-webkit-scrollbar-thumb:hover,
    .right-panel::-webkit-scrollbar-thumb:hover,
    .table-container::-webkit-scrollbar-thumb:hover {
      background: #555;
    }

    @media (max-width: 1200px) {
      .split-container {
        flex-direction: column;
        height: auto;
      }
      .left-panel {
        flex: 1;
        max-height: 600px;
      }
      .right-panel {
        flex: 1;
        min-height: 500px;
      }
    }

    /* -------- Tablet (portrait / landscape) tweaks -------- */
    @media (max-width: 1024px) {
      /* Stack the task controls so the dropdown and the remark
         textbox each get the full row width and the dropdown
         popup no longer visually overlaps the next row. */
      .task-controls {
        flex-direction: row;
        align-items: center;
        flex-wrap: nowrap;
        gap: 6px;
      }
      .task-checkbox { flex: 0 0 auto; padding-left: 10px; padding-right: 4px; }
      .task-nicht    { flex: 0 0 auto; }
      .task-select   { flex: 0 1 190px; min-width: 130px; }
      .task-remark   { flex: 1 1 100px; min-width: 0; }

      /* Bigger tap targets for finger use. */
      .task-select select.form-control {
        height: 46px;
        font-size: 15px;
        padding: 6px 32px 6px 12px;
      }
      .task-remark input[type=text] {
        height: 46px;
        font-size: 15px;
        padding: 8px 12px;
      }
      .btn.btn-nicht-erledigt,
      .btn.btn-save-remark { height: 46px; font-size: 14px; padding: 6px 14px; }
      .task-item input[type=checkbox] { width: 20px; height: 20px; }
      .task-item .checkbox label { font-size: 14px; padding-left: 8px; }

      /* Tighter card padding so more content fits on a tablet. */
      .task-item { padding: 12px; margin-bottom: 10px; }
      .task-name { font-size: 14px; }

      /* Hub grid: 2 columns instead of many on tablets. */
      .hub-grid { grid-template-columns: repeat(2, minmax(0, 1fr)) !important; }
      .sum-card { font-size: 14px; }

      /* Generic button tap targets. */
      .btn { min-height: 40px; }
    }

    .app-header, .app-footer { 
      text-align:center; 
      padding:8px 0; 
      background: #ffffff; 
    }
    
    .app-header img, .app-footer img { 
      max-height:100px; 
      display:inline-block; 
      margin:4px; 
    }

    /* ---------------- Login screen ---------------- */
    /* ---- Login: single brand accent -------------------------------------
       One place to change the look of the whole left panel. The wordmark
       and every icon use the same colour so the page reads as one brand
       rather than a collection of highlights. */
    .login-wrapper {
      --login-accent: #ffffff;
      min-height: calc(100vh - 100px);
      display: flex;
      align-items: flex-start;
      justify-content: center;
      padding: 30px 15px;
      background:
        radial-gradient(circle at 20% 20%, rgba(19,99,119,0.12), transparent 60%),
        radial-gradient(circle at 80% 80%, rgba(0,59,115,0.18), transparent 60%),
        linear-gradient(135deg, #eef3f8 0%, #dbe6f0 100%);
    }
    .login-card {
      width: 100%;
      max-width: 1000px;
      display: grid;
      grid-template-columns: 1.1fr 1fr;
      gap: 0;
      background: #ffffff;
      border-radius: 16px;
      overflow: hidden;
      box-shadow: 0 20px 50px rgba(0, 59, 115, 0.18);
    }
    .login-left {
      background: linear-gradient(160deg, #003B73 0%, #136377 100%);
      color: #ffffff;
      padding: 40px 36px;
      display: flex;
      flex-direction: column;
      justify-content: space-between;
    }
    /* Wordmark: both lines in the same colour. They are told apart by
       size, weight and letter-spacing instead of by colour or opacity. */
    .login-left .brand-mark {
      display: flex; align-items: center; gap: 14px; margin-bottom: 18px;
    }
    .login-left .brand-logo {
      width: 48px; height: 48px; border-radius: 12px; flex-shrink: 0;
      display: flex; align-items: center; justify-content: center;
      background: rgba(255,255,255,0.14);
      border: 1px solid rgba(255,255,255,0.22);
    }
    .login-left .brand-logo i {
      font-size: 22px;
      color: var(--login-accent);
    }
    .login-left .brand-eyebrow {
      font-size: 11px;
      letter-spacing: 2.5px;
      text-transform: uppercase;
      font-weight: 600;
      color: var(--login-accent);
      opacity: 1;
    }
    .login-left h2 {
      color: var(--login-accent);
      margin: 2px 0 0 0;
      font-weight: 700;
      letter-spacing: -0.3px;
      line-height: 1.1;
    }
    .login-left .subtitle {
      opacity: 0.85;
      font-size: 14px;
      margin-bottom: 24px;
    }
    .login-left ul.feature-list {
      list-style: none;
      padding: 0;
      margin: 0 0 20px 0;
    }
    .login-left ul.feature-list li {
      padding: 8px 0;
      font-size: 14px;
      display: flex;
      align-items: flex-start;
      gap: 12px;
    }
    /* Every icon on the panel -- feature bullets and the footer note --
       shares the single accent colour. */
    .login-left i.fa,
    .login-left i.fas,
    .login-left i.far,
    .login-left svg {
      color: var(--login-accent);
      flex-shrink: 0;
    }
    .login-left ul.feature-list li i.fa,
    .login-left ul.feature-list li svg {
      margin-top: 3px;
      width: 18px;
      text-align: center;
      opacity: 0.9;
    }
    .login-left .login-foot {
      font-size: 12px;
      opacity: 0.75;
      margin-top: 24px;
      border-top: 1px solid rgba(255,255,255,0.18);
      padding-top: 14px;
    }
    .login-left .login-version {
      font-size: 11px;
      opacity: 0.6;
      margin-top: 8px;
      letter-spacing: 0.5px;
    }

    .audit-hint {
      background: #eef5fb;
      border-left: 4px solid #003B73;
      border-radius: 6px;
      padding: 10px 14px;
      margin-bottom: 14px;
      font-size: 13px;
      color: #24425f;
    }

    /* ---- Änderungsprotokoll: Verlaufsansicht ---- */
    .audit-chips {
      display: flex; flex-wrap: wrap; gap: 10px; margin-bottom: 14px;
    }
    .audit-chip {
      background: #fff; border-radius: 10px; padding: 8px 14px;
      box-shadow: 0 2px 6px rgba(0,0,0,0.06);
      font-size: 13px; color: #6c757d;
      display: flex; align-items: center; gap: 7px;
    }
    .audit-chip b { color: #2c3e50; font-size: 16px; }
    .audit-chip i { color: #003B73; }

    .audit-timeline { font-size: 13px; }
    .au-daysep {
      display: flex; align-items: center; gap: 8px;
      font-weight: 700; color: #003B73; font-size: 13px;
      margin: 16px 0 8px 0; padding-bottom: 6px;
      border-bottom: 1px solid #e3ecf3;
    }
    .au-daycount {
      margin-left: auto; font-weight: 600; color: #8a949e; font-size: 12px;
    }
    .au-list {
      list-style: none; margin: 0; padding: 0 0 0 6px;
      border-left: 2px solid #e3ecf3;
    }
    .au-item {
      position: relative; padding: 10px 0 10px 30px;
    }
    .au-item + .au-item { border-top: 1px solid #f1f4f7; }
    .au-dot {
      position: absolute; left: -13px; top: 12px;
      width: 24px; height: 24px; border-radius: 50%;
      display: flex; align-items: center; justify-content: center;
      font-size: 11px; color: #fff; border: 2px solid #fff;
    }
    .au-dot.sl-ok      { background: #00b894; }
    .au-dot.sl-warn    { background: #f0932b; }
    .au-dot.sl-danger  { background: #d63031; }
    .au-dot.sl-info    { background: #0984e3; }
    .au-dot.sl-muted   { background: #b2bec3; }
    .au-dot.sl-version { background: #6c5ce7; }

    .au-head {
      display: flex; align-items: center; gap: 8px; flex-wrap: wrap;
    }
    .au-avatar {
      width: 22px; height: 22px; border-radius: 50%;
      background: linear-gradient(135deg, #003B73 0%, #136377 100%);
      color: #fff; font-size: 10px; font-weight: 700;
      display: flex; align-items: center; justify-content: center;
      flex-shrink: 0;
    }
    .au-title { color: #2c3e50; }
    .au-time  { margin-left: auto; color: #8a949e; font-size: 12px; cursor: help; }

    .au-meta { display: flex; flex-wrap: wrap; gap: 6px; margin: 5px 0 0 30px; }
    .au-badge {
      background: #eef2f7; color: #24425f; border-radius: 999px;
      padding: 1px 9px; font-size: 11px; font-weight: 700;
    }
    .au-chip {
      background: #f7f9fb; color: #6c757d; border: 1px solid #e8edf2;
      border-radius: 999px; padding: 1px 9px; font-size: 11px;
    }

    .au-diff {
      margin: 7px 0 0 30px; display: flex; align-items: center;
      gap: 8px; flex-wrap: wrap;
    }
    .au-old, .au-new {
      font-family: Consolas, Monaco, monospace; font-size: 12px;
      border-radius: 5px; padding: 2px 8px;
    }
    .au-old { background: #ffeaea; color: #a52626; text-decoration: line-through; }
    .au-new { background: #e7f8f0; color: #17694a; font-weight: 600; }
    .au-arrow { color: #b2bec3; }

    .au-details {
      margin: 6px 0 0 30px; font-size: 12px; color: #6c757d;
    }
    .au-release {
      margin: 7px 0 0 30px; padding-left: 18px; color: #4a4a68; font-size: 12.5px;
    }
    .au-release li { margin-bottom: 2px; }

    @media (max-width: 700px) {
      .au-time { margin-left: 0; }
      .au-meta, .au-diff, .au-details, .au-release { margin-left: 0; }
    }

    .login-right {
      padding: 40px 36px;
      background: #ffffff;
      display: flex;
      flex-direction: column;
      justify-content: center;
    }
    .login-right h3 {
      color: #003B73;
      margin: 0 0 4px 0;
      font-weight: 700;
    }
    .login-right .login-sub {
      color: #6c757d;
      font-size: 13px;
      margin-bottom: 22px;
    }
    .login-right .form-group label {
      font-weight: 600;
      color: #2c3e50;
      font-size: 13px;
    }
    .login-right .form-control {
      height: 42px;
      border-radius: 8px;
      border: 1px solid #d6dde4;
      transition: border-color .2s, box-shadow .2s;
    }
    .login-right .form-control:focus {
      border-color: #136377;
      box-shadow: 0 0 0 3px rgba(19,99,119,0.15);
    }
    .login-right .btn-login {
      width: 100%;
      height: 44px;
      font-weight: 700;
      border-radius: 8px;
      background: linear-gradient(135deg, #003B73 0%, #136377 100%);
      border: none;
      color: #fff;
      letter-spacing: 0.3px;
      margin-top: 6px;
      transition: transform .15s ease, box-shadow .2s ease, filter .2s ease;
    }
    .login-right .btn-login:hover {
      filter: brightness(1.05);
      transform: translateY(-1px);
      box-shadow: 0 6px 16px rgba(0,59,115,0.25);
    }
    .login-right .forgot-row {
      display: flex;
      justify-content: flex-end;
      margin: -6px 0 12px 0;
    }
    .login-right .forgot-row a {
      color: #136377;
      font-size: 12px;
      font-weight: 600;
      text-decoration: none;
    }
    .login-right .forgot-row a:hover { text-decoration: underline; }

    .login-help {
      margin-top: 18px;
      background: #f5f9fc;
      border: 1px solid #e3ecf3;
      border-radius: 10px;
      padding: 14px 16px;
      font-size: 13px;
      color: #2c3e50;
    }
    .login-help .login-help-title {
      font-weight: 700;
      color: #003B73;
      margin-bottom: 6px;
      display: flex;
      align-items: center;
      gap: 8px;
    }
    .login-help ol { padding-left: 18px; margin: 6px 0 0 0; }
    .login-help li { margin-bottom: 4px; }

    @media (max-width: 900px) {
      .login-card { grid-template-columns: 1fr; }
      .login-left { padding: 28px 24px; }
      .login-right { padding: 28px 24px; }
    }

    /* ---------------- Geräte-Übersicht (Hub) ---------------- */
    .hub-intro {
      background: linear-gradient(135deg, #003B73 0%, #136377 100%);
      color: #fff;
      border-radius: 16px;
      padding: 26px 30px;
      margin-bottom: 22px;
      box-shadow: 0 8px 22px rgba(0,59,115,0.18);
    }
    .hub-intro .hub-eyebrow {
      font-size: 12px; letter-spacing: 2px; text-transform: uppercase;
      opacity: 0.85; margin-bottom: 6px;
    }
    .hub-intro h2 {
      color: #fff; margin: 0 0 6px 0; font-weight: 800; font-size: 26px;
    }
    .hub-intro p {
      margin: 0; opacity: 0.92; font-size: 15px; max-width: 720px;
    }

    .hub-summary {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(210px, 1fr));
      gap: 14px;
      margin-bottom: 14px;
    }
    .hub-summary .sum-card {
      background: #fff; border-radius: 14px;
      padding: 18px 20px;
      box-shadow: 0 4px 12px rgba(0,0,0,0.06);
      border-left: 5px solid #003B73;
      display: flex; align-items: center; gap: 14px;
    }
    .hub-summary .sum-card.warn   { border-left-color: #ff6a3d; }
    .hub-summary .sum-card.ok     { border-left-color: #00b894; }
    .hub-summary .sum-card.danger { border-left-color: #d32f2f; }
    .hub-summary .sum-card.clickable {
      transition: transform .15s ease, box-shadow .2s ease;
    }
    .hub-summary .sum-card.clickable:hover {
      transform: translateY(-2px);
      box-shadow: 0 8px 18px rgba(0,0,0,0.12);
    }
    .hub-summary .sum-icon {
      width: 44px; height: 44px; border-radius: 12px;
      display: flex; align-items: center; justify-content: center;
      font-size: 20px; color: #fff; flex-shrink: 0;
      background: linear-gradient(135deg, #003B73 0%, #136377 100%);
    }
    .hub-summary .sum-card.warn .sum-icon {
      background: linear-gradient(135deg, #ff6a3d 0%, #c0392b 100%);
    }
    .hub-summary .sum-card.ok .sum-icon {
      background: linear-gradient(135deg, #00b894 0%, #00897b 100%);
    }
    .hub-summary .sum-card.danger .sum-icon {
      background: linear-gradient(135deg, #e53935 0%, #8e0000 100%);
    }
    .hub-summary .sum-num {
      font-size: 26px; font-weight: 800; color: #2c3e50; line-height: 1;
    }
    .hub-summary .sum-card.danger .sum-num { color: #b71c1c; }
    .hub-summary .sum-lbl {
      font-size: 12px; color: #6c757d; text-transform: uppercase;
      letter-spacing: 1px; font-weight: 600; margin-top: 4px;
    }
    .hub-summary .sum-hint {
      font-size: 11.5px; color: #8a949e; margin-top: 3px; line-height: 1.35;
    }

    .hub-progress {
      background: #fff; border-radius: 14px; padding: 14px 18px 16px 18px;
      box-shadow: 0 4px 12px rgba(0,0,0,0.06);
      margin-bottom: 22px;
    }
    .hub-progress .hp-head {
      display: flex; align-items: center; justify-content: space-between;
      gap: 12px; flex-wrap: wrap; margin-bottom: 9px;
    }
    .hub-progress .hp-title {
      font-size: 12px; font-weight: 700; color: #6c757d;
      text-transform: uppercase; letter-spacing: 1px;
    }
    .hub-progress .hp-val {
      font-size: 13px; font-weight: 700; color: #2c3e50;
    }
    .hub-progress .hp-track {
      height: 12px; border-radius: 999px; background: #eceff1; overflow: hidden;
    }
    .hub-progress .hp-fill {
      height: 100%; border-radius: 999px;
      background: linear-gradient(90deg, #ff6a3d 0%, #f6b93b 100%);
      transition: width .45s ease;
      min-width: 3px;
    }
    .hub-progress .hp-fill.full {
      background: linear-gradient(90deg, #00b894 0%, #00897b 100%);
    }

    /* The Checkliste tab is reached by clicking a device card on the hub,
       so it does not need its own sidebar entry. The menu item itself must
       stay in the DOM because updateTabItems() targets it. */
    .sidebar-menu > li > a[href='#shiny-tab-checklist'] { display: none !important; }

    .hub-section-title {
      font-size: 13px; font-weight: 700; color: #6c757d;
      letter-spacing: 1.5px; text-transform: uppercase;
      margin: 6px 4px 12px 4px; display: flex; align-items: center; gap: 8px;
    }

    .hub-grid {
      display: grid;
      grid-template-columns: repeat(auto-fill, minmax(280px, 1fr));
      gap: 14px;
    }
    /* The actionButton inside each card -> make the button itself look like the card */
    .hub-grid .hub-card-btn {
      all: unset;
      box-sizing: border-box;
      cursor: pointer;
      display: block;
      width: 100%;
      background: #fff;
      border-radius: 14px;
      padding: 18px;
      box-shadow: 0 4px 14px rgba(0,0,0,0.06);
      border: 1px solid #eef1f4;
      border-left: 5px solid #003B73;
      transition: transform .15s ease, box-shadow .2s ease, border-color .2s ease;
      min-height: 120px;
    }
    .hub-grid .hub-card-btn:hover {
      transform: translateY(-3px);
      box-shadow: 0 10px 24px rgba(0,59,115,0.15);
      border-color: #cfd8e3;
    }
    .hub-grid .hub-card-btn.warn { border-left-color: #ff6a3d; }
    .hub-grid .hub-card-btn.ok   { border-left-color: #00b894; }

    .hub-card .hc-row {
      display: flex; align-items: flex-start; gap: 12px;
    }
    .hub-card .hc-icon {
      width: 44px; height: 44px; border-radius: 12px;
      background: linear-gradient(135deg, #003B73 0%, #136377 100%);
      color: #fff; display: flex; align-items: center; justify-content: center;
      font-size: 18px; flex-shrink: 0;
    }
    .hub-card .hc-text { min-width: 0; flex: 1; text-align: left; }
    .hub-card .hc-name {
      font-weight: 700; color: #003B73; font-size: 15px; line-height: 1.25;
    }
    .hub-card .hc-id {
      font-size: 11px; color: #6c757d; letter-spacing: 1px;
      text-transform: uppercase; margin-top: 2px;
    }
    .hub-card .hc-bottom {
      display: flex; align-items: center; justify-content: space-between;
      margin-top: 12px;
    }
    .hub-card .hc-status {
      display: inline-flex; align-items: center; gap: 6px;
      padding: 6px 12px; border-radius: 999px;
      font-size: 12px; font-weight: 700;
    }
    .hub-card .hc-status.ok    { background: #e6f8f1; color: #00897b; }
    .hub-card .hc-status.warn  { background: #fff1ec; color: #c0392b; }
    .hub-card .hc-cta {
      font-size: 13px; font-weight: 600; color: #003B73;
      display: inline-flex; align-items: center; gap: 6px;
    }

    @media (max-width: 600px) {
      .hub-intro { padding: 20px 18px; }
      .hub-intro h2 { font-size: 22px; }
    }
  "))),
  
  # JS: when a task dropdown is changed to "Nicht erledigt", highlight
  # the sibling Bemerkung box and focus it so the user is prompted to
  # type the reason.
  tags$script(HTML("
    // ---- Ablauf je Aufgabe: offen -> erledigt / nicht erledigt ----------
    // Rueckmeldung aus dem Labor: der Ablauf war unklar. Pro Aufgabe gibt es
    // jetzt drei Zustaende (CSS-Klassen an .task-controls):
    //   state-open    : nur 'Erledigt' oder 'Nicht erledigt' sichtbar
    //   state-done    : Kommentarfeld zur Erledigung (optional)
    //   state-notdone : Grund-Auswahl (Pflicht) + Bemerkung zum Grund
    // Der Server setzt den Zustand beim Aufbau; hier wird er beim Klicken
    // sofort umgeschaltet, ohne auf den Server zu warten.
    window.wpSetTaskState = function(ctrls, st) {
      ctrls.removeClass('state-open state-done state-notdone').addClass('state-' + st);
      var inp = ctrls.find('.task-remark input[type=text]');
      if (st === 'done') {
        inp.attr('placeholder', 'Kommentar zur Erledigung (optional) ...');
      } else if (st === 'notdone') {
        inp.attr('placeholder', 'Bemerkung zum Grund (bei NE bitte ausfüllen) ...');
      }
      ctrls.find('.btn-nicht-erledigt').toggleClass('is-ne', st === 'notdone');
      var sel = ctrls.find('.task-select select');
      ctrls.find('.task-select').toggleClass('needs-reason',
        st === 'notdone' && !(sel.val() || ''));
      if (st !== 'notdone') ctrls.find('.task-remark').removeClass('needs-remark');
    };
    // Grund gewaehlt -> Zustand 'nicht erledigt'; bei NE ist die Bemerkung
    // Pflicht und wird hervorgehoben.
    $(document).on('change', '.task-select select', function() {
      var ctrls = $(this).closest('.task-controls');
      var label = $(this).find('option:selected').text() || '';
      var hasVal = !!($(this).val() || '');
      if (hasVal) wpSetTaskState(ctrls, 'notdone');
      ctrls.find('.task-select').toggleClass('needs-reason', !hasVal);
      var remark = ctrls.find('.task-remark');
      if (hasVal && label.indexOf('Nicht erledigt') !== -1) {
        remark.addClass('needs-remark');
        setTimeout(function(){ remark.find('input[type=text]').focus(); }, 50);
      } else {
        remark.removeClass('needs-remark');
      }
    });
    // 'Nicht erledigt' oeffnet die Grund-Auswahl. Gespeichert wird erst,
    // wenn ein Grund gewaehlt ist.
    $(document).on('click', '.btn-nicht-erledigt', function() {
      var ctrls = $(this).closest('.task-controls');
      ctrls.find('.task-checkbox input[type=checkbox]').prop('checked', false);
      wpSetTaskState(ctrls, 'notdone');
      setTimeout(function(){ ctrls.find('.task-select select').focus(); }, 50);
    });
    // 'Erledigt' an -> Kommentarfeld zur Erledigung; aus -> wieder offen
    // (ausser es wurde gerade 'Nicht erledigt' gewaehlt).
    $(document).on('change', '.task-checkbox input[type=checkbox]', function() {
      var ctrls = $(this).closest('.task-controls');
      if ($(this).is(':checked')) {
        wpSetTaskState(ctrls, 'done');
      } else if (!ctrls.hasClass('state-notdone')) {
        wpSetTaskState(ctrls, 'open');
      }
    });
    // Apply on initial render too (e.g. when reopening a device).
    $(document).on('shiny:value', function(e) {
      if (e.name === 'today_tasks') {
        setTimeout(function() {
          $('.task-select select').each(function() {
            var label = $(this).find('option:selected').text() || '';
            var ctrls = $(this).closest('.task-controls');
            if (label.indexOf('Nicht erledigt') !== -1) {
              ctrls.find('.task-remark').addClass('needs-remark');
              ctrls.find('.btn-nicht-erledigt').addClass('is-ne');
            }
          });
        }, 80);
      }
    });
    // Server -> client: focus a remark textbox after Nicht erledigt click.
    Shiny.addCustomMessageHandler('focusTaskRemark', function(msg) {
      setTimeout(function() {
        var el = document.getElementById(msg.id);
        if (el) { el.focus(); el.select && el.select(); }
      }, 60);
    });
    // Enter-to-login: pressing Enter in the username or password field should
    // trigger the Einloggen button, so users don't have to reach for the
    // mouse. Delegated so it works even though the login form is rendered
    // dynamically.
    $(document).on('keydown', '#login_user, #login_pass', function(e) {
      if (e.key === 'Enter' || e.keyCode === 13) {
        e.preventDefault();
        var btn = document.getElementById('login_btn');
        if (btn) btn.click();
      }
    });
    // Enter-to-submit for the set-new-password fields on first login.
    $(document).on('keydown', '#new_pass, #new_pass2', function(e) {
      if (e.key === 'Enter' || e.keyCode === 13) {
        e.preventDefault();
        var btn = document.getElementById('set_pass_btn');
        if (btn) btn.click();
      }
    });
    // Bemerkung fields auto-save while typing (debounced, silent). We only
    // want the save-confirmation toast once the user actually leaves the
    // field, not on every typing pause, so track blur separately here.
    $(document).on('blur', '.task-remark input[type=text]', function() {
      var id = $(this).attr('id');
      if (id) Shiny.setInputValue(id + '_blur', Math.random());
    });
    // Server -> client: scroll to a task row in 'Taegliche Aufgaben' and
    // briefly highlight it so the user can find the affected Bemerkung.
    Shiny.addCustomMessageHandler('scrollToTaskRow', function(msg) {
      var tryScroll = function(attempts) {
        var el = document.getElementById('task_row_' + msg.row);
        if (el) {
          // Make sure the Taegliche-Aufgaben tab is active.
          var tabLink = $('a[data-toggle=\"tab\"]:contains(\"T\u00e4gliche Aufgaben\")').first();
          if (tabLink.length && !tabLink.parent().hasClass('active')) {
            tabLink.tab('show');
          }
          el.scrollIntoView({behavior: 'smooth', block: 'center'});
          el.style.transition = 'box-shadow .3s ease, background-color .3s ease';
          var prevBg = el.style.backgroundColor;
          el.style.boxShadow = '0 0 0 3px #d32f2f';
          el.style.backgroundColor = '#fff5f5';
          setTimeout(function(){
            el.style.boxShadow = '';
            el.style.backgroundColor = prevBg;
          }, 2200);
        } else if (attempts > 0) {
          setTimeout(function(){ tryScroll(attempts - 1); }, 120);
        }
      };
      tryScroll(20);
    });
  ")),
  
  
  
  conditionalPanel(
    condition = "!output.is_authed",
    
    tags$div(class = "login-wrapper",
             tags$div(class = "login-card",
                      
                      # ---- Left: branding & app description ----
                      tags$div(class = "login-left",
                               tags$div(
                                 tags$div(class = "brand-mark",
                                          tags$div(class = "brand-logo",
                                                   tags$i(class = "fa fa-tools")
                                          ),
                                          tags$div(
                                            tags$div(class = "brand-eyebrow",
                                                     "Diagnostikzentrum"),
                                            tags$h2("Wartungsplan")
                                          )
                                 ),
                                 tags$p(class = "subtitle",
                                        "Digitale Wartungsplanung für die Geräte des Zentrallabors. ",
                                        "Aufgaben dokumentieren, Verantwortlichkeiten nachvollziehen und ",
                                        "Berichte erzeugen – an einem Ort."
                                 ),
                                 tags$ul(class = "feature-list",
                                         tags$li(tags$i(class = "fa fa-th-large"),
                                                 tags$span(tags$b("Übersicht: "),
                                                           "Alle Geräte auf einen Blick mit offenen Aufgaben des Tages.")),
                                         tags$li(tags$i(class = "fa fa-tasks"),
                                                 tags$span(tags$b("Checkliste: "),
                                                           "Tägliche Aufgaben abhaken und im Monatsplan dokumentieren.")),
                                         
                                         tags$li(tags$i(class = "fa fa-file-pdf"),
                                                 tags$span(tags$b("Export: "),
                                                           "Monatsplan als PDF oder CSV herunterladen.")),
                                         
                                         tags$li(tags$i(class = "fa fa-user-shield"),
                                                 tags$span(tags$b("Sicher: "),
                                                           "Persönlicher Zugang mit Initialen-Nachverfolgung pro Eintrag."))
                                 )
                               ),
                               tags$div(class = "login-foot",
                                        tags$i(class = "fa fa-info-circle"), " ",
                                        "Bei Fragen oder Zugangsproblemen wenden Sie sich an ",
                                        tags$b("Yadwinder"), ", ", tags$b("Frank"), " oder ", tags$b("Martina"), ".",
                                        tags$div(class = "login-version",
                                                 sprintf("Version %s \u00b7 Stand %s",
                                                         APP_VERSION,
                                                         format(as.Date(APP_RELEASE_DATE), "%d.%m.%Y")))
                               )
                      ),
                      
                      # ---- Right: login form ----
                      tags$div(class = "login-right",
                               tags$h3("Willkommen zurück"),
                               tags$div(class = "login-sub",
                                        "Bitte melden Sie sich mit Ihrem Benutzernamen und Passwort an."),
                               
                               textInput("login_user", "Benutzername",
                                         placeholder = "z. B. Ihre Initialen"),
                               passwordInput("login_pass", "Passwort",
                                             placeholder = "Bei Erstanmeldung leer lassen"),
                               
                               tags$div(class = "forgot-row",
                                        actionLink("forgot_pw_link", "Passwort vergessen?")),
                               
                               actionButton("login_btn",
                                            label = tagList(tags$i(class = "fa fa-sign-in-alt"),
                                                            " Einloggen"),
                                            class = "btn btn-login"),
                               
                               # Kurzanleitung Erstanmeldung
                               tags$div(class = "login-help",
                                        tags$div(class = "login-help-title",
                                                 tags$i(class = "fa fa-key"),
                                                 "Erstanmeldung – so legen Sie Ihr Passwort fest"
                                        ),
                                        tags$ol(
                                          tags$li("Benutzernamen eingeben (Initialen, die Ihnen mitgeteilt wurden)."),
                                          tags$li("Passwortfeld ", tags$b("leer lassen"), " und auf ",
                                                  tags$b("„Einloggen“"), " klicken."),
                                          tags$li("Im erscheinenden Feld zweimal ein neues Passwort ",
                                                  "(min. 8 Zeichen) eingeben und speichern.")
                                        )
                               ),
                               
                               uiOutput("password_setup_panel")
                      )
             )
    )
    
  ),
  conditionalPanel(
    condition = "output.is_authed",
    tabItems(
      tabItem(tabName = "hub",
              uiOutput("hub_header_image"),
              uiOutput("hub_intro"),
              uiOutput("hub_summary"),
              uiOutput("hub_buttons"),
              br(),br(),
              uiOutput("hub_table_area")
              
              
      ),
      tabItem(
        tabName = "checklist",
        
        # Back button: returns to the previous page (usually the device hub).
        tags$div(style = "margin-bottom: 8px;",
                 actionButton("nav_back_btn",
                              label = tagList(icon("arrow-left"), " Zurück"),
                              class = "btn btn-default btn-sm")),
        
        # Compact device header (always visible, slim)
        uiOutput("device_info_header_slim"),
        
        # Tabbed interface for tasks and visualization (THIS is what the user
        # should see first when opening a device)
        tabsetPanel(
          id = "checklist_tabs",
          tabPanel(
            "Tägliche Aufgaben",
            value = "daily",
            tags$div(
              class = "left-panel",
              style = "flex: none; width: auto; height: calc(100vh - 200px); margin-top: 20px;",
              
              box(width = 12, collapsible = TRUE, collapsed = FALSE,
                  title = "Tägliche Aufgaben",
                  
                  # Stats bar
                  uiOutput("task_stats"),
                  
                  tags$hr(),
                  
                  # Task list
                  uiOutput("today_tasks")
              ),
              
              # Legend explaining the option symbols (collapsible).
              box(width = 12, collapsible = TRUE, collapsed = TRUE,
                  title = "Legende \u2013 Erklärung der Symbole",
                  status = "info",
                  tags$table(
                    class = "legend-table",
                    style = "width:100%; border-collapse:collapse; font-size:13px;",
                    tags$tbody(
                      lapply(
                        list(
                          c("WE",   "Wochenende"),
                          c("FT",   "Feiertag"),
                          c("\u00d8",    "An diesem Tag wurden keine Analysen gestartet"),
                          c("W.e.", "Wartungspunkt ist in einer größeren Wartung enthalten"),
                          c("ne",   "Nicht erforderlich (für die Rubrik „bei Bedarf“)"),
                          c("D",    "Gerät / Modul defekt"),
                          c("NE",   "Nicht erledigt"),
                          c("sB",   "Siehe Bemerkungen"),
                          c("sQ",   "Siehe Quasi")
                        ),
                        function(pair) tags$tr(
                          tags$td(style = "padding:4px 10px 4px 0; white-space:nowrap;
                                           vertical-align:top;",
                                  tags$span(style = "display:inline-block; min-width:32px;
                                                     text-align:center; background:#003B73;
                                                     color:#fff; padding:1px 8px;
                                                     border-radius:4px; font-weight:700;",
                                            pair[1])),
                          tags$td(style = "padding:4px 0; vertical-align:top;", pair[2])
                        )
                      )
                    )
                  )
              )
            )
          ),
          # Eigener Tab fuer versaeumte Eintraege der letzten 14 Tage; der Titel
          # zeigt die Anzahl live: "Ueberfaellige Aufgaben (offen: N)".
          tabPanel(
            title = uiOutput("overdue_tab_title", inline = TRUE),
            value = "overdue",
            tags$div(
              class = "left-panel",
              style = "flex: none; width: auto; height: calc(100vh - 200px); margin-top: 20px;",
              uiOutput("overdue_tasks")
            )
          ),
          tabPanel(
            "Monatsübersicht",
            value = "monthly",
            tags$div(
              class = "right-panel",
              style = "flex: none; width: auto; height: calc(100vh - 200px); margin-top: 20px;",
              
              # Visualization header
              tags$div(class = "visualization-header",
                       icon("table"),
                       " Monatsübersicht - Wartungsplan",
                       uiOutput("monthly_remarks_dropdown", inline = TRUE)
              ),
              
              # Controls row
              fluidRow(
                column(3,
                       selectInput("month", "Monat:", 
                                   choices = setNames(1:12, unname(.DE_MONTHS)),
                                   selected = as.integer(format(Sys.Date(), "%m")))
                ),
                column(3,
                       numericInput("year", "Jahr:", 
                                    value = as.integer(format(Sys.Date(), "%Y")),
                                    min = 2020, max = 2030, step = 1)
                ),
                column(6,
                       tags$div(
                         style = "margin-top: 25px;",
                         downloadButton("download_table_pdf", "📄 PDF", 
                                        class = "btn btn-primary btn-sm",
                                        style = "margin-right: 5px;"),
                         actionButton("open_nachtrag", 
                                      label = tagList(icon("clock-rotate-left"), " Nachtrag"),
                                      class = "btn btn-warning btn-sm",
                                      title = "Eintrag für einen früheren Tag nachtragen")
                       )
                )
              ),
              
              tags$hr(),
              
              # Table container
              tags$div(
                class = "table-container",
                rHandsontableOutput("tableRH", height = "100%")
              )
            )
          )
        ),
        
        # Device footer
        uiOutput("device_footer_ui"),
        
        # Editable device info (collapsed at the bottom — admin / power-user only)
        tags$div(style = "margin-top: 30px;", uiOutput("device_info_area"))
      ),
      tabItem(tabName = "layout",
              h3("Kopf- / Fußzeile ändern"),
              fluidRow(
                column(width = 6,
                       box(width = 12, title = "Gerätespezifische Fußzeile (Admin)", status = "primary", solidHeader = TRUE,
                           selectInput("layout_device", "Gerät wählen:", choices = NULL),
                           textInput("layout_title", "Gerätetitel (wird in Checkliste angezeigt)"),
                           textInput("layout_footer_text", "Fußzeilentext"),
                           textInput("layout_version", "Version"),
                           dateInput("layout_valid_from", "Gültig ab", format = "yyyy-mm-dd"),
                           helpText("Hinweis: Bilder können manuell in www/uploads/ abgelegt werden. Der Pfad (z. B. 'uploads/g1-footer.png') kann in der DB gesetzt, ansonsten wird nur der Text verwendet.")
                       )
                ),
                column(width = 6,
                       box(width = 12, title = "Übersicht Header (global)", status = "primary", solidHeader = TRUE,
                           textInput("hub_header_path", "Pfad zum Logo (relativ zu www/)",
                                     placeholder = "hub-header.png"),
                           helpText("Die Bilddatei muss bereits im www/-Ordner der App liegen ",
                                    "(z. B. www/hub-header.png -> hier 'hub-header.png' eintragen, ",
                                    "oder www/uploads/logo.png -> 'uploads/logo.png')."),
                           actionButton("save_hub_header", "Logo-Pfad speichern (Admin)",
                                        class = "btn btn-primary")
                       ),
                       # save controls for admin
                       uiOutput("layout_save_controls")
                )
              ),
              fluidRow(
                column(width = 12,
                       helpText("Hinweis: Bilder werden nicht über die App hochgeladen. Legen Sie stattdessen eine Datei in www/uploads/ ab und verwenden Sie z. B. pgAdmin/psql, um app_images oder device_layout.footer_path zu setzen, oder nutzen Sie die Admin-Speicherfelder oben.")
                )
              )
      ),
      tabItem(tabName = "add_task",
              h3("Neue Aufgabe hinzufügen"),
              uiOutput("admin_add_task_box"),
              uiOutput("admin_edit_task_box"),
              uiOutput("admin_diag_box"),
              uiOutput("admin_rebuild_box")
      ),
      tabItem(tabName = "empty_plan",
              h3("Ungefülltes Wartungsplan herunterladen"),
              uiOutput("empty_plan_box")
      ),
      tabItem(tabName = "all_tasks",
              h3("Alle Aufgaben \u2013 Gesamtübersicht"),
              uiOutput("admin_all_tasks_box")
      ),
      tabItem(tabName = "feedback",
              h3("Feedback / Problem melden"),
              uiOutput("feedback_form_box")
      ),
      tabItem(tabName = "feedback_admin",
              h3("Feedback \u2013 Eingegangene Meldungen"),
              uiOutput("feedback_admin_box")
      ),
      tabItem(tabName = "audit",
              h3(icon("clipboard-list"), " \u00c4nderungsprotokoll (Audit-Trail)"),
              uiOutput("audit_panel")
      ),
      tabItem(tabName = "admin",
              uiOutput("admin_panel")
      )
    )
  )
)

ui <- dashboardPage(header, sidebar, body, title = "Wartungsplan")

# Run schema setup (table creation/migrations, device seeding, and the
# one-off "force rebuild from template" deletes) exactly ONCE when the R
# process starts -- NOT inside server(), which runs fresh for every new
# browser session. Calling it per-session meant every single login wiped
# and rebuilt every device's task-table row from the hardcoded template,
# silently discarding any admin edits (add/edit/delete task) made since
# the last login by anyone, and re-stamping updated_by/updated_at with
# whichever session happened to trigger the rebuild first.
ensure_schema()

# ----------------------------- SERVER -----------------------------------------
server <- function(input, output, session) {
  
  
  
  # ── Reminder helpers ──────────────────────────────────────────────────────────
  
  # Extract the first HH:MM time found in a task string e.g. "(6:00)" -> "06:00"
  extract_task_time <- function(task_text) {
    m <- regmatches(task_text,
                    regexpr("\\b([01]?[0-9]|2[0-3]):[0-5][0-9]\\b", task_text))
    if (!length(m) || !nzchar(m)) return(NA_character_)
    # normalise to HH:MM
    parts <- strsplit(m, ":")[[1]]
    sprintf("%02d:%02d", as.integer(parts[1]), as.integer(parts[2]))
  }
  
  # Given a task's time string and the current time, return one of:
  #   "overdue"  – window passed (> 60 min ago)
  #   "due_now"  – within ±60 min of the target time
  #   "upcoming" – within the next 120 min
  #   "later"    – more than 120 min away
  #   NA         – no time in task
  classify_task_urgency <- function(task_time_str,
                                    now = Sys.time(),
                                    tz  = "Europe/Berlin") {
    if (is.na(task_time_str)) return(NA_character_)
    today_str <- format(as.POSIXct(now, tz = tz), "%Y-%m-%d")
    target    <- as.POSIXct(paste(today_str, task_time_str),
                            format = "%Y-%m-%d %H:%M", tz = tz)
    diff_min  <- as.numeric(difftime(target, now, units = "mins"))
    if      (diff_min < -60)              "overdue"
    else if (diff_min >= -60 && diff_min <= 60) "due_now"
    else if (diff_min >  60  && diff_min <= 120) "upcoming"
    else                                  "later"
  }
  
  # Is a schedule header due today?
  is_header_due_today <- function(header) {
    today_wd  <- weekdays(Sys.Date())   # English weekday name
    wd_de_map <- c(Monday="Montag", Tuesday="Dienstag", Wednesday="Mittwoch",
                   Thursday="Donnerstag", Friday="Freitag",
                   Saturday="Samstag", Sunday="Sonntag")
    today_de  <- wd_de_map[[today_wd]]
    
    switch(header,
           "Täglich"                       = TRUE,
           "Täglich (ZL)"                  = TRUE,
           "Täglich (Ablesen zwischen 12:00 und 14:00 Uhr)" = TRUE,
           "Wöchentlich"                   = (today_wd == "Monday"),
           "Wöchentlich (Montag)"          = (today_wd == "Monday"),
           "Wöchentlich (Dienstag)"        = (today_wd == "Tuesday"),
           "Wöchentlich (Mittwoch)"        = (today_wd == "Wednesday"),
           "Wöchentlich (Mittwoch, TD)"    = (today_wd == "Wednesday"),
           "Wöchentlich (Donnerstag)"      = (today_wd == "Thursday"),
           "Wöchentlich (Freitag)"         = (today_wd == "Friday"),
           "Wöchentlich (Freitag, ZL)"     = (today_wd == "Friday"),
           "14-tägig"                      = is_biweekly_monday(),
           "14-tägig (Mittwoch)"           = is_biweekly_wednesday(),
           "Montag und Donnerstag"         = (today_wd %in% c("Monday", "Thursday")),
           "Montag"                        = (today_wd == "Monday"),
           "Dienstag"                      = (today_wd == "Tuesday"),
           "Mittwoch"                      = (today_wd == "Wednesday"),
           "Donnerstag"                    = (today_wd == "Thursday"),
           "Freitag"                       = (today_wd == "Friday"),
           "Samstag"                       = (today_wd == "Saturday"),
           "Sonntag"                       = (today_wd == "Sunday"),
           "Monatlich"                     = is_due_28day_cycle(Sys.Date(), MONTHLY_CYCLE_ANCHOR),
           "Monatlich (Freitag)"           = (today_wd == "Friday" && is_first_workday_of_month()),
           "Monatlich oder alle 2500 Proben" = is_due_28day_cycle(Sys.Date(), MONTHLY_CYCLE_ANCHOR),
           "Quartalsweise"                 = is_first_workday_of_quarter(),
           "Am ersten Dienstag im Monat"   = is_due_28day_cycle(Sys.Date(), TUESDAY_CYCLE_ANCHOR),
           "Am ersten Freitag im Monat"    = is_due_28day_cycle(Sys.Date(), FRIDAY_CYCLE_ANCHOR),
           "Monatlich (Freitag, alle 4 Wochen)"  = is_due_28day_cycle(Sys.Date(), PHADIA_FRIDAY_ANCHOR),
           "Monatlich (Dienstag, alle 4 Wochen)" = is_due_28day_cycle(Sys.Date(), ANALYZER_TUESDAY_ANCHOR),
           "Monatlich (Mittwoch, alle 4 Wochen)" = is_due_28day_cycle(Sys.Date(), COBAS_WEDNESDAY_ANCHOR),
           "Alle 3 Monate oder alle 7500 Proben" = is_first_workday_of_quarter(),
           "Nach jeder Migration:"         = TRUE,
           FALSE   # Bei Bedarf / Wartung bei Bedarf / unknown -> never auto-flagged
    )
  }
  
  # Same logic as is_header_due_today() but for an arbitrary date `d`. Used by
  # the "Vortagsaufgaben" banner to decide which schedule blocks were actually
  # expected on the previous workday.
  is_header_due_on <- function(header, d) {
    d <- as.Date(d)
    if (is.na(d)) return(FALSE)
    # Locale-independent weekday detection: %u -> 1=Mon..7=Sun. Avoids the
    # weekdays() trap where a German locale returns "Dienstag" and an English
    # name lookup fails with "subscript out of bounds".
    iso  <- as.integer(format(d, "%u"))
    en   <- c("Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday")
    wd_en <- if (!is.na(iso) && iso >= 1L && iso <= 7L) en[iso] else ""
    
    switch(header,
           "Täglich"                       = TRUE,
           "Täglich (ZL)"                  = TRUE,
           "Täglich (Ablesen zwischen 12:00 und 14:00 Uhr)" = TRUE,
           "Wöchentlich"                   = (wd_en == "Monday"),
           "Wöchentlich (Montag)"          = (wd_en == "Monday"),
           "Wöchentlich (Dienstag)"        = (wd_en == "Tuesday"),
           "Wöchentlich (Mittwoch)"        = (wd_en == "Wednesday"),
           "Wöchentlich (Mittwoch, TD)"    = (wd_en == "Wednesday"),
           "Wöchentlich (Donnerstag)"      = (wd_en == "Thursday"),
           "Wöchentlich (Freitag)"         = (wd_en == "Friday"),
           "Wöchentlich (Freitag, ZL)"     = (wd_en == "Friday"),
           "14-tägig"                      = is_biweekly_monday(d),
           "14-tägig (Mittwoch)"           = is_biweekly_wednesday(d),
           "Montag und Donnerstag"         = (wd_en %in% c("Monday", "Thursday")),
           "Montag"                        = (wd_en == "Monday"),
           "Dienstag"                      = (wd_en == "Tuesday"),
           "Mittwoch"                      = (wd_en == "Wednesday"),
           "Donnerstag"                    = (wd_en == "Thursday"),
           "Freitag"                       = (wd_en == "Friday"),
           "Samstag"                       = (wd_en == "Saturday"),
           "Sonntag"                       = (wd_en == "Sunday"),
           "Monatlich"                     = is_due_28day_cycle(d, MONTHLY_CYCLE_ANCHOR),
           "Monatlich (Freitag)"           = (wd_en == "Friday" && is_first_workday_of_month(d)),
           "Monatlich oder alle 2500 Proben" = is_due_28day_cycle(d, MONTHLY_CYCLE_ANCHOR),
           "Quartalsweise"                 = is_first_workday_of_quarter(d),
           "Am ersten Dienstag im Monat"   = is_due_28day_cycle(d, TUESDAY_CYCLE_ANCHOR),
           "Am ersten Freitag im Monat"    = is_due_28day_cycle(d, FRIDAY_CYCLE_ANCHOR),
           "Monatlich (Freitag, alle 4 Wochen)"  = is_due_28day_cycle(d, PHADIA_FRIDAY_ANCHOR),
           "Monatlich (Dienstag, alle 4 Wochen)" = is_due_28day_cycle(d, ANALYZER_TUESDAY_ANCHOR),
           "Monatlich (Mittwoch, alle 4 Wochen)" = is_due_28day_cycle(d, COBAS_WEDNESDAY_ANCHOR),
           "Alle 3 Monate oder alle 7500 Proben" = is_first_workday_of_quarter(d),
           "Nach jeder Migration:"         = TRUE,
           FALSE
    )
  }
  
  # Every header text that represents an actual schedule (used to decide,
  # while scanning rv$data top to bottom, whether a header row starts a new
  # schedule section or is just a device sub-grouping label that keeps
  # whatever schedule was active above it). Kept in sync with the switch
  # statements above and with the sched_styles map in the daily task list.
  # SCHEDULE_HEADER_NAMES / SCHEDULE_HEADERS_NO_DUE_DATE: see top level,
  # next to cells_readonly_for_headers().
  
  # Whether a task under `header` is something the user is actually expected
  # to act on for date `d` -- mirrors exactly what the daily checklist shows
  # (see the skip_section logic in the main render loop): a real schedule
  # that's due on `d`, or a non-schedule/always-visible header (device
  # sub-heading, standalone note). "Bei Bedarf"/"Wartung bei Bedarf" never
  # count here, since they have no due date and are shown purely for the
  # user's own judgement, not because they're "open" for today.
  is_task_relevant_on <- function(header, d) {
    if (!nzchar(header)) return(FALSE)
    if (!(header %in% SCHEDULE_HEADER_NAMES)) return(TRUE)
    if (header %in% SCHEDULE_HEADERS_NO_DUE_DATE) return(FALSE)
    isTRUE(is_header_due_on(header, d))
  }
  
  # Most recent date <= d on which `header`'s schedule was due. Used to find
  # the last time a (non-today) section should have been completed, so an
  # unfinished task from that date is surfaced no matter how long ago it
  # became due -- not just "yesterday". Returns NA if nothing found within
  # max_lookback days (covers even the quarterly cadence with headroom).
  last_due_on_or_before <- function(header, d, max_lookback = 400L) {
    d <- as.Date(d)
    if (is.na(d) || header %in% SCHEDULE_HEADERS_NO_DUE_DATE) return(as.Date(NA))
    for (i in 0:max_lookback) {
      cand <- d - i
      if (isTRUE(is_header_due_on(header, cand))) return(cand)
    }
    as.Date(NA)
  }
  
  # Human-readable description of WHEN a schedule header is due (frequency +
  # weekday), for the admin "Alle Aufgaben" overview. Returns a short German
  # phrase; unknown/device-grouping headers return "" (caller shows the raw
  # header). Kept in sync with is_header_due_on's switch.
  schedule_when_text <- function(header) {
    switch(header,
           "Täglich"                          = "Täglich",
           "Täglich (ZL)"                     = "Täglich",
           "Täglich (Ablesen zwischen 12:00 und 14:00 Uhr)" = "Täglich (12:00–14:00 Uhr)",
           "Arbeitstäglich"                   = "Jeden Arbeitstag",
           "Montag und Donnerstag"            = "Wöchentlich – Mo & Do",
           "Wöchentlich"                      = "Wöchentlich – Montag",
           "Wöchentlich (Montag)"             = "Wöchentlich – Montag",
           "Wöchentlich (Dienstag)"           = "Wöchentlich – Dienstag",
           "Wöchentlich (Mittwoch)"           = "Wöchentlich – Mittwoch",
           "Wöchentlich (Mittwoch, TD)"       = "Wöchentlich – Mittwoch",
           "Wöchentlich (Donnerstag)"         = "Wöchentlich – Donnerstag",
           "Wöchentlich (Freitag)"            = "Wöchentlich – Freitag",
           "Wöchentlich (Freitag, ZL)"        = "Wöchentlich – Freitag",
           "14-tägig"                         = "Alle 2 Wochen – Montag",
           "14-tägig (Mittwoch)"              = "Alle 2 Wochen – Mittwoch",
           "Monatlich"                        = "Monatlich (alle 4 Wochen)",
           "Monatlich (Freitag)"              = "Monatlich – 1. Freitag",
           "Monatlich (Freitag, alle 4 Wochen)"  = "Alle 4 Wochen – Freitag",
           "Monatlich (Dienstag, alle 4 Wochen)" = "Alle 4 Wochen – Dienstag",
           "Monatlich (Mittwoch, alle 4 Wochen)" = "Alle 4 Wochen – Mittwoch",
           "Monatlich oder alle 2500 Proben"  = "Monatlich / alle 2500 Proben",
           "Quartalsweise"                    = "Quartalsweise",
           "Alle 3 Monate oder alle 7500 Proben" = "Alle 3 Monate / 7500 Proben",
           "Am ersten Dienstag im Monat"      = "Alle 4 Wochen – Dienstag",
           "Am ersten Freitag im Monat"       = "Alle 4 Wochen – Freitag",
           "Bei Bedarf"                       = "Bei Bedarf",
           "Wartung bei Bedarf"               = "Bei Bedarf",
           "Nach jeder Migration:"            = "Nach jeder Migration",
           "Montag"                           = "Wöchentlich – Montag",
           "Dienstag"                         = "Wöchentlich – Dienstag",
           "Mittwoch"                         = "Wöchentlich – Mittwoch",
           "Donnerstag"                       = "Wöchentlich – Donnerstag",
           "Freitag"                          = "Wöchentlich – Freitag",
           "Samstag"                          = "Wöchentlich – Samstag",
           "Sonntag"                          = "Wöchentlich – Sonntag",
           ""
    )
  }
  
  # Next due date for a schedule header (or NA if none / on-demand). Reuses the
  # same anchors/logic as the daily list's "fällig am" badges.
  schedule_next_due <- function(header) {
    if (header %in% SCHEDULE_HEADERS_NO_DUE_DATE) return(as.Date(NA))
    nd <- switch(header,
                 "Monatlich"                        = next_28day_due(Sys.Date(), MONTHLY_CYCLE_ANCHOR),
                 "Monatlich oder alle 2500 Proben"  = next_28day_due(Sys.Date(), MONTHLY_CYCLE_ANCHOR),
                 "Am ersten Dienstag im Monat"      = next_28day_due(Sys.Date(), TUESDAY_CYCLE_ANCHOR),
                 "Am ersten Freitag im Monat"       = next_28day_due(Sys.Date(), FRIDAY_CYCLE_ANCHOR),
                 "Monatlich (Freitag, alle 4 Wochen)"  = next_28day_due(Sys.Date(), PHADIA_FRIDAY_ANCHOR),
                 "Monatlich (Dienstag, alle 4 Wochen)" = next_28day_due(Sys.Date(), ANALYZER_TUESDAY_ANCHOR),
                 "Monatlich (Mittwoch, alle 4 Wochen)" = next_28day_due(Sys.Date(), COBAS_WEDNESDAY_ANCHOR),
                 "14-tägig (Mittwoch)"              = next_biweekly_wednesday(Sys.Date()),
                 "Wöchentlich"                      = next_monday(Sys.Date()),
                 "Wöchentlich (Montag)"             = next_monday(Sys.Date()),
                 "Wöchentlich (Mittwoch)"           = next_wednesday(Sys.Date()),
                 "Wöchentlich (Mittwoch, TD)"       = next_wednesday(Sys.Date()),
                 "Wöchentlich (Donnerstag)"         = next_thursday(Sys.Date()),
                 NA
    )
    if (is.na(nd) || !inherits(nd, "Date")) {
      for (i in 0:400) {
        cand <- Sys.Date() + i
        if (isTRUE(is_header_due_on(header, cand))) return(cand)
      }
      return(as.Date(NA))
    }
    nd
  }
  
  # Build the urgency badge tag shown next to a task name
  urgency_badge <- function(urgency, task_time_str) {
    if (is.na(urgency)) return(NULL)
    cfg <- list(
      due_now  = list(bg = "#d32f2f", icon = "bell",       label = sprintf("Jetzt fällig (%s)", task_time_str)),
      upcoming = list(bg = "#f57c00", icon = "clock",      label = sprintf("Bald fällig (%s)", task_time_str)),
      overdue  = list(bg = "#6d1a1a", icon = "circle-exclamation", label = sprintf("Überfällig (%s)", task_time_str)),
      later    = list(bg = "#1565c0", icon = "hourglass-start",    label = sprintf("Heute (%s)",    task_time_str))
    )
    c <- cfg[[urgency]]
    if (is.null(c)) return(NULL)
    tags$span(
      style = sprintf(
        "display:inline-flex; align-items:center; gap:4px;
       background:%s; color:#fff; border-radius:999px;
       padding:2px 10px; font-size:11px; font-weight:700;
       margin-left:8px; vertical-align:middle;",
        c$bg),
      icon(c$icon), c$label
    )
  }
  
  
  # Task statistics
  # output$task_stats <- renderUI({
  #   req(rv$current_device, rv$data)
  #   today <- as.integer(format(Sys.Date(), "%d"))
  
  #   # Calculate statistics
  #   data_rows <- which(rv$data$Header == "")
  #   total_tasks <- length(data_rows)
  
  #   cs <- rv$table_status %||% data.frame()
  #   done_rows <- unique(cs$row_index[cs$day == today])
  #   completed_tasks <- length(done_rows)
  #   pending_tasks <- total_tasks - completed_tasks
  #   completion_pct <- if (total_tasks > 0) round((completed_tasks / total_tasks) * 100) else 0
  
  # tags$div(
  #   class = "stats-bar",
  #   tags$div(
  #     class = "stat-box",
  #     tags$span(class = "stat-number", total_tasks),
  #     tags$span(class = "stat-label", "Gesamt")
  #   ),
  #   tags$div(
  #     class = "stat-box",
  #     style = "border-color: #00a65a;",
  #     tags$span(class = "stat-number", style = "color: #00a65a;", completed_tasks),
  #     tags$span(class = "stat-label", "Erledigt")
  #   ),
  #   tags$div(
  #     class = "stat-box",
  #     style = "border-color: #dc3545;",
  #     tags$span(class = "stat-number", style = "color: #dc3545;", pending_tasks),
  #     tags$span(class = "stat-label", "Offen")
  #   ),
  #   tags$div(
  #     class = "stat-box",
  #     style = "border-color: #3c8dbc;",
  #     tags$span(class = "stat-number", style = "color: #3c8dbc;", paste0(completion_pct, "%")),
  #     tags$span(class = "stat-label", "Fortschritt")
  #   )
  # )
  # })
  
  rv <- reactiveValues(
    role = "user",
    authed = FALSE,
    user   = NULL,
    user_initials = NULL,
    must_reset = TRUE,
    current_device = NULL,
    current_device_title = NULL,
    data = NULL,
    table_status = data.frame(),
    invalid_days = integer(),
    task_obs_ids = character(0),
    prev_task_obs_ids = character(0),
    tasks_refresh = 0L,
    remarks_rev = 0L,   # bumped when a Bemerkung is saved -> refresh Monatsuebersicht
    pending_remarks = NULL,
    nav_history = character(0),  # stack of previously-visited tabs (for the Zurück button)
    feedback_refresh = 0L,       # bump to re-render the admin feedback inbox
    audit_refresh = 0L,          # bump to re-query the Änderungsprotokoll
    fb_obs = character(0)         # ids of feedback rows we've wired button observers for
  )
  
  # Ensure upload directory exists (www/uploads) so you can place images there manually
  if (!dir.exists(file.path("www", "uploads"))) dir.create(file.path("www", "uploads"), recursive = TRUE, showWarnings = FALSE)
  
  # ---- Back-button navigation history --------------------------------------
  # Track the sequence of visited tabs so the "Zurück" button can return to
  # the previous one. We push the PREVIOUS tab onto the stack each time the
  # tab changes (ignoring back-navigation itself, flagged via nav_going_back).
  rv$nav_current <- NULL
  rv$nav_going_back <- FALSE
  observeEvent(input$tabs, {
    new_tab <- input$tabs
    prev    <- isolate(rv$nav_current)
    if (isTRUE(isolate(rv$nav_going_back))) {
      # this change was caused by the back button itself -> don't record it
      rv$nav_going_back <- FALSE
    } else if (!is.null(prev) && !identical(prev, new_tab)) {
      rv$nav_history <- c(isolate(rv$nav_history), prev)
    }
    rv$nav_current <- new_tab
  }, ignoreInit = FALSE)
  
  observeEvent(input$nav_back_btn, {
    hist <- isolate(rv$nav_history)
    if (length(hist)) {
      target <- tail(hist, 1)
      rv$nav_history <- head(hist, -1)
    } else {
      # nothing recorded yet -> sensible default: the device hub
      target <- "hub"
    }
    rv$nav_going_back <- TRUE
    updateTabItems(session, "tabs", target)
  })
  
  # Update the is_authed output to respect bypass_auth
  output$is_authed <- reactive({
    rv$authed || bypass_auth  # Allow access if authenticated or bypass is enabled
  })
  
  # Ensure the reactive value is registered
  outputOptions(output, "is_authed", suspendWhenHidden = FALSE)
  
  # --- Login flow ---
  observeEvent(input$login_btn, {
    req(input$login_user)
    con <- pg_con(); on.exit(dbDisconnect(con), add = TRUE)
    
    role_exists     <- has_column(con, "app_users", "role")
    initials_exists <- has_column(con, "app_users", "initials")
    sql <- paste0(
      "SELECT username, password_hash, must_reset, ",
      if (role_exists) "COALESCE(role,'user')" else "'user'",
      " AS role, ",
      if (initials_exists) "initials" else "NULL",
      " AS initials FROM app_users WHERE username=$1"
    )
    u <- dbGetQuery(con, sql, params = list(input$login_user))
    if (nrow(u) == 0) { showNotification("Unbekannter Benutzer.", type = "error"); return() }
    
    rv$role <- u$role[1]
    rv$user <- u$username[1]
    rv$user_initials <- (u$initials %||% substr(rv$user, 1, 5))[1]
    
    if (isTRUE(u$must_reset[1]) || is.na(u$password_hash[1])) {
      rv$must_reset <- TRUE
      return()
    }
    
    ok <- bcrypt::checkpw(input$login_pass %||% "", u$password_hash[1] %||% "")
    if (!ok) {
      log_event(con, "anmeldung.fehlgeschlagen", entity_type = "benutzer",
                entity_id = input$login_user, who = input$login_user,
                details = "Falsches Passwort",
                session_id = session$token,
                client_ip = session$clientData$url_hostname)
      showNotification("Falsches Passwort.", type = "error"); return()
    }
    
    rv$authed <- TRUE; rv$must_reset <- FALSE
    dbExecute(con, "UPDATE app_users SET last_login = NOW() WHERE username=$1", params = list(rv$user))
    log_event(con, "anmeldung", entity_type = "benutzer", entity_id = rv$user,
              who = rv$user, role = rv$role, session_id = session$token,
              client_ip = session$clientData$url_hostname)
    updateTabItems(session, "tabs", "hub")
    showNotification(sprintf("Willkommen, %s!", rv$user), type = "message")
    show_due_tasks_dialog()
  })
  
  # Predefined security questions (users can also enter a custom one)
  SECURITY_QUESTIONS <- c(
    "Wann (Monat/Jahr) haben Sie in diesem Labor angefangen?" = "start_lab",
    "In welcher Stadt sind Sie geboren?"                       = "birth_city",
    "Wie hieß Ihr erstes Haustier?"                            = "first_pet",
    "Was ist der Mädchenname Ihrer Mutter?"                    = "mother_maiden",
    "Wie hieß Ihre Grundschule?"                               = "primary_school",
    "Eigene Frage definieren …"                                = "__custom__"
  )
  
  output$password_setup_panel <- renderUI({
    if (!isTRUE(rv$must_reset) || is.null(rv$user)) return(NULL)
    box(width = 12, title = "Passwort festlegen", status = "warning", solidHeader = TRUE,
        p("Erstmalige Anmeldung (oder Passwort wurde zurückgesetzt). ",
          "Bitte legen Sie ein Passwort sowie eine Sicherheitsfrage fest. ",
          "Die Sicherheitsfrage benötigen Sie später, falls Sie Ihr Passwort vergessen."),
        passwordInput("new_pass",  "Neues Passwort"),
        passwordInput("new_pass2", "Passwort bestätigen"),
        tags$hr(),
        tags$div(style = "font-weight:600; margin-bottom:6px; color:#003B73;",
                 icon("shield-alt"), " Sicherheitsfrage"),
        selectInput("sec_q_choice", "Frage wählen",
                    choices = SECURITY_QUESTIONS),
        conditionalPanel(
          condition = "input.sec_q_choice == '__custom__'",
          textInput("sec_q_custom", "Eigene Frage",
                    placeholder = "z. B. Wie hieß Ihr erster Lehrer?")
        ),
        textInput("sec_answer", "Ihre Antwort",
                  placeholder = "Antwort merken – wird für Passwort-Reset benötigt"),
        helpText("Hinweis: Groß-/Kleinschreibung und führende/nachfolgende Leerzeichen werden ignoriert."),
        actionButton("set_pass_btn", "Passwort & Sicherheitsfrage speichern",
                     class = "btn btn-primary")
    )
  })
  
  # Helper: normalize a security answer (lowercase, trim, collapse spaces)
  normalize_answer <- function(x) {
    x <- tolower(trimws(x %||% ""))
    gsub("\\s+", " ", x)
  }
  
  observeEvent(input$set_pass_btn, {
    req(input$new_pass, input$new_pass2, rv$user)
    if (nchar(input$new_pass) < 8) {
      showNotification("Passwort zu kurz (min. 8 Zeichen).", type = "error"); return()
    }
    if (input$new_pass != input$new_pass2) {
      showNotification("Passwörter stimmen nicht überein.", type = "error"); return()
    }
    
    # Resolve security question label
    q_key <- input$sec_q_choice %||% ""
    q_label <- if (identical(q_key, "__custom__")) {
      trimws(input$sec_q_custom %||% "")
    } else {
      # Reverse-lookup: SECURITY_QUESTIONS is named vector (label -> key)
      nm <- names(SECURITY_QUESTIONS)[match(q_key, SECURITY_QUESTIONS)]
      if (is.na(nm)) "" else nm
    }
    ans <- normalize_answer(input$sec_answer)
    if (!nzchar(q_label)) {
      showNotification("Bitte wählen Sie eine Sicherheitsfrage oder geben Sie eine eigene ein.",
                       type = "error"); return()
    }
    if (nchar(ans) < 2) {
      showNotification("Bitte geben Sie eine sinnvolle Antwort (min. 2 Zeichen).",
                       type = "error"); return()
    }
    
    con <- pg_con(); on.exit(dbDisconnect(con), add = TRUE)
    ph    <- bcrypt::hashpw(input$new_pass)
    a_h   <- bcrypt::hashpw(ans)
    dbExecute(con, "
      UPDATE app_users
         SET password_hash = $1,
             must_reset = FALSE,
             last_login = NOW(),
             security_question = $2,
             security_answer_hash = $3
       WHERE username = $4
    ", params = list(ph, q_label, a_h, rv$user))
    
    rv$authed <- TRUE; rv$must_reset <- FALSE
    showNotification("Passwort und Sicherheitsfrage gespeichert. Willkommen!",
                     type = "message")
    updateTabItems(session, "tabs", "hub")
    show_due_tasks_dialog()
  })
  
  # ---- Passwort vergessen: zweistufiger Dialog mit Sicherheitsfrage ----
  forgot_state <- reactiveValues(user = NULL, question = NULL)
  
  observeEvent(input$forgot_pw_link, {
    forgot_state$user <- NULL
    forgot_state$question <- NULL
    showModal(modalDialog(
      title = tagList(icon("key"), " Passwort zurücksetzen – Schritt 1/2"),
      tags$p("Geben Sie Ihren Benutzernamen ein. Im nächsten Schritt müssen Sie ",
             "Ihre persönliche Sicherheitsfrage beantworten."),
      textInput("forgot_user", "Benutzername", placeholder = "z. B. Ihre Initialen"),
      footer = tagList(
        modalButton("Abbrechen"),
        actionButton("forgot_lookup", "Weiter", class = "btn btn-primary")
      ),
      easyClose = TRUE
    ))
  })
  
  observeEvent(input$forgot_lookup, {
    uname <- trimws(input$forgot_user %||% "")
    if (!nzchar(uname)) {
      showNotification("Bitte einen Benutzernamen eingeben.", type = "error"); return()
    }
    con <- pg_con(); on.exit(dbDisconnect(con), add = TRUE)
    u <- dbGetQuery(con,
                    "SELECT username, security_question FROM app_users WHERE username=$1",
                    params = list(uname))
    if (!nrow(u)) {
      showNotification("Benutzer nicht gefunden.", type = "error"); return()
    }
    if (is.na(u$security_question[1]) || !nzchar(u$security_question[1])) {
      removeModal()
      showModal(modalDialog(
        title = "Keine Sicherheitsfrage hinterlegt",
        tags$p("Für diesen Benutzer wurde noch keine Sicherheitsfrage festgelegt. ",
               "Bitte wenden Sie sich an einen Administrator (Yadwinder, Frank oder Martina), ",
               "um Ihr Passwort zurücksetzen zu lassen."),
        footer = modalButton("Schließen"),
        easyClose = TRUE
      ))
      return()
    }
    forgot_state$user <- u$username[1]
    forgot_state$question <- u$security_question[1]
    removeModal()
    showModal(modalDialog(
      title = tagList(icon("shield-alt"), " Passwort zurücksetzen – Schritt 2/2"),
      tags$p(tags$b("Benutzer: "), forgot_state$user),
      tags$p(tags$b("Sicherheitsfrage:")),
      tags$div(style = "background:#f5f9fc; border-left:4px solid #003B73; padding:10px 12px; margin-bottom:14px; border-radius:4px;",
               forgot_state$question),
      textInput("forgot_answer", "Ihre Antwort"),
      helpText("Groß-/Kleinschreibung wird ignoriert."),
      footer = tagList(
        modalButton("Abbrechen"),
        actionButton("forgot_confirm", "Passwort zurücksetzen",
                     class = "btn btn-primary")
      ),
      easyClose = TRUE
    ))
  })
  
  observeEvent(input$forgot_confirm, {
    uname <- forgot_state$user
    if (is.null(uname) || !nzchar(uname)) { removeModal(); return() }
    ans <- normalize_answer(input$forgot_answer)
    if (!nzchar(ans)) {
      showNotification("Bitte beantworten Sie die Sicherheitsfrage.", type = "error"); return()
    }
    con <- pg_con(); on.exit(dbDisconnect(con), add = TRUE)
    u <- dbGetQuery(con,
                    "SELECT security_answer_hash FROM app_users WHERE username=$1",
                    params = list(uname))
    if (!nrow(u) || is.na(u$security_answer_hash[1])) {
      showNotification("Sicherheitsfrage konnte nicht überprüft werden.", type = "error"); return()
    }
    ok <- tryCatch(bcrypt::checkpw(ans, u$security_answer_hash[1]),
                   error = function(e) FALSE)
    if (!isTRUE(ok)) {
      showNotification("Antwort ist nicht korrekt.", type = "error"); return()
    }
    force_reset_db(con, uname)
    forgot_state$user <- NULL
    forgot_state$question <- NULL
    removeModal()
    showNotification(
      sprintf("Passwort für '%s' wurde zurückgesetzt. Bitte mit leerem Passwortfeld einloggen und ein neues Passwort festlegen.",
              uname),
      type = "message", duration = 10
    )
  })
  
  # Add a bypass mode for testing purposes
  bypass_auth <- FALSE  # Set to TRUE to skip login, FALSE for normal behavior
  
  # ---- Dynamic sidebar (Admin item visible only to admins) ----
  output$sidebar_menu <- renderMenu({
    items <- list(
      menuItem("Geräte Übersicht",     tabName = "hub",       icon = icon("th-large")),
      # The Checkliste tab is opened by clicking a device card on the hub, so
      # it gets no visible sidebar entry. The item must stay in the menu
      # because updateTabItems(session, "tabs", "checklist") activates it.
      tagAppendAttributes(
        menuItem("Checkliste",         tabName = "checklist", icon = icon("tasks")),
        style = "display:none !important;"
      ),
      menuItem("Feedback / Problem melden", tabName = "feedback", icon = icon("comment-dots"))
    )
    is_admin <- isTRUE(rv$authed) && identical(rv$role, "admin")
    can_edit_layout <- isTRUE(rv$authed) && (is_admin || identical(rv$user, "groe"))
    if (can_edit_layout) {
      items <- c(items, list(
        menuItem("Kopf-Fuß Zeile ändern", tabName = "layout", icon = icon("images"))
      ))
    }
    if (is_admin) {
      items <- c(items, list(
        menuItem("Neue Aufgabe hinzufügen (Admin)", tabName = "add_task", icon = icon("plus")),
        menuItem("Ungefülltes Wartungsplan herunterladen", tabName = "empty_plan", icon = icon("file-arrow-down")),
        menuItem("Alle Aufgaben sehen", tabName = "all_tasks", icon = icon("list-check")),
        menuItem("Feedback ansehen (Admin)", tabName = "feedback_admin", icon = icon("inbox")),
        menuItem("\u00c4nderungsprotokoll", tabName = "audit", icon = icon("clipboard-list")),
        menuItem("Admin", tabName = "admin", icon = icon("user-shield"))
      ))
    }
    do.call(sidebarMenu, c(list(id = "tabs"), items))
  })
  
  # ---- Logout button (top-right in header) ----
  output$logout_ui <- renderUI({
    if (!isTRUE(rv$authed)) return(NULL)
    who <- rv$user %||% ""
    tagList(
      tags$span(style = "color:#fff; margin-right:12px;",
                icon("user"), " ", who),
      actionLink(
        "logout_btn",
        label = tagList(icon("sign-out-alt"), " Abmelden"),
        style = "color:#fff; font-weight:600; text-decoration:none;"
      )
    )
  })
  
  observeEvent(input$logout_btn, {
    tryCatch({
      con_l <- pg_con(); on.exit(dbDisconnect(con_l), add = TRUE)
      log_event(con_l, "abmeldung", entity_type = "benutzer",
                entity_id = rv$user, who = rv$user, role = rv$role,
                session_id = session$token)
    }, error = function(e) NULL)
    rv$authed        <- FALSE
    rv$must_reset    <- TRUE
    rv$user          <- NULL
    rv$user_initials <- NULL
    rv$role          <- "user"
    rv$current_device <- NULL
    rv$current_device_title <- NULL
    rv$data          <- NULL
    rv$table_status  <- data.frame()
    rv$invalid_days  <- integer()
    updateTextInput(session, "login_user", value = "")
    updateTextInput(session, "login_pass", value = "")
    showNotification("Sie wurden abgemeldet.", type = "message")
    session$reload()
  })
  
  # ---- Admin panel: user management ------------------------------------------
  # Reactive trigger to refresh the user list after add/delete/role/reset actions
  admin_refresh <- reactiveVal(0)
  bump_admin_refresh <- function() admin_refresh(isolate(admin_refresh()) + 1L)
  
  load_app_users_df <- function() {
    con <- pg_con(); on.exit(dbDisconnect(con), add = TRUE)
    DBI::dbGetQuery(con, "
      SELECT username,
             COALESCE(initials, '')               AS initials,
             COALESCE(role, 'user')               AS role,
             must_reset,
             COALESCE(to_char(last_login, 'YYYY-MM-DD HH24:MI'), '-') AS last_login,
             to_char(created_at, 'YYYY-MM-DD')    AS created_at
      FROM app_users
      ORDER BY username
    ")
  }
  
  output$admin_panel <- renderUI({
    req(rv$authed)
    if (!identical(rv$role, "admin")) {
      return(div(class = "alert alert-danger",
                 "Zugriff verweigert. Nur Administratoren können diese Seite öffnen."))
    }
    fluidRow(
      column(width = 12,
             box(width = 12, title = "Neuen Benutzer anlegen", status = "success", solidHeader = TRUE,
                 fluidRow(
                   column(3, textInput("admin_new_user", "Benutzername (Login)",
                                       placeholder = "z. B. mmus")),
                   column(3, textInput("admin_new_initials", "Initialen",
                                       placeholder = "2-5 Kleinbuchstaben")),
                   column(2, selectInput("admin_new_role", "Rolle",
                                         choices = c("user", "admin"), selected = "user")),
                   column(4,
                          br(),
                          actionButton("admin_add_user", "Benutzer hinzufügen",
                                       class = "btn btn-success", icon = icon("user-plus"))
                   )
                 ),
                 helpText("Der neue Benutzer hat noch kein Passwort und wird beim ersten Login aufgefordert, eines festzulegen.")
             ),
             box(width = 12, title = "Benutzer verwalten", status = "primary", solidHeader = TRUE,
                 tableOutput("admin_users_table"),
                 hr(),
                 fluidRow(
                   column(4, uiOutput("admin_user_select_ui")),
                   column(8,
                          br(),
                          actionButton("admin_toggle_role", "Rolle umschalten (admin/user)",
                                       class = "btn btn-warning", icon = icon("user-shield")),
                          actionButton("admin_reset_pw", "Passwort zurücksetzen",
                                       class = "btn btn-info", icon = icon("key")),
                          actionButton("admin_delete_user", "Benutzer löschen",
                                       class = "btn btn-danger", icon = icon("user-times"))
                   )
                 ),
                 helpText("Hinweis: Sie können Ihren eigenen Account nicht löschen oder degradieren.")
             )
      )
    )
  })
  
  output$admin_users_table <- renderTable({
    req(rv$authed, identical(rv$role, "admin"))
    admin_refresh()  # take a dependency
    df <- load_app_users_df()
    if (!nrow(df)) return(data.frame(Hinweis = "Keine Benutzer vorhanden."))
    data.frame(
      Benutzer    = df$username,
      Initialen   = df$initials,
      Rolle       = df$role,
      `Passwort-Reset nötig` = ifelse(isTRUE(df$must_reset) | df$must_reset == TRUE, "ja", "nein"),
      `Letzter Login` = df$last_login,
      Erstellt    = df$created_at,
      check.names = FALSE,
      stringsAsFactors = FALSE
    )
  }, striped = TRUE, hover = TRUE, bordered = TRUE, spacing = "s", width = "100%")
  
  output$admin_user_select_ui <- renderUI({
    req(rv$authed, identical(rv$role, "admin"))
    admin_refresh()
    df <- load_app_users_df()
    choices <- if (nrow(df)) df$username else character(0)
    selectInput("admin_target_user", "Benutzer auswählen", choices = choices)
  })
  
  # -------------------------- Add user-----------------------------------------
  observeEvent(input$admin_add_user, {
    req(rv$authed, identical(rv$role, "admin"))
    uname <- trimws(input$admin_new_user %||% "")
    inits <- trimws(input$admin_new_initials %||% "")
    rolev <- input$admin_new_role %||% "user"
    if (!nzchar(uname)) {
      showNotification("Benutzername darf nicht leer sein.", type = "error"); return()
    }
    if (!grepl("^[A-Za-z0-9_.-]{2,32}$", uname)) {
      showNotification("Ungültiger Benutzername (2-32 Zeichen, A-Z/0-9/_.-).", type = "error"); return()
    }
    if (nzchar(inits) && !grepl("^[a-z]{2,5}$", inits)) {
      showNotification("Initialen müssen 2-5 Kleinbuchstaben (a-z) sein.", type = "error"); return()
    }
    if (!rolev %in% c("user", "admin")) rolev <- "user"
    if (!nzchar(inits)) inits <- substr(tolower(uname), 1, 5)
    
    con <- pg_con(); on.exit(dbDisconnect(con), add = TRUE)
    exists <- DBI::dbGetQuery(con, "SELECT 1 FROM app_users WHERE username=$1", params = list(uname))
    if (nrow(exists)) {
      showNotification("Benutzer existiert bereits.", type = "error"); return()
    }
    tryCatch({
      dbExecute(con, "
        INSERT INTO app_users (username, password_hash, must_reset, role, initials)
        VALUES ($1, NULL, TRUE, $2, $3)
      ", params = list(uname, rolev, inits))
      updateTextInput(session, "admin_new_user", value = "")
      updateTextInput(session, "admin_new_initials", value = "")
      updateSelectInput(session, "admin_new_role", selected = "user")
      bump_admin_refresh()
      showNotification(sprintf("Benutzer '%s' wurde angelegt.", uname), type = "message")
    }, error = function(e) {
      showNotification(paste("Fehler beim Anlegen:", conditionMessage(e)), type = "error")
    })
  })
  
  # ---- Delete user (with confirmation)
  observeEvent(input$admin_delete_user, {
    req(rv$authed, identical(rv$role, "admin"))
    target <- input$admin_target_user
    if (is.null(target) || !nzchar(target)) {
      showNotification("Bitte einen Benutzer auswählen.", type = "error"); return()
    }
    if (identical(target, rv$user)) {
      showNotification("Sie können Ihren eigenen Account nicht löschen.", type = "error"); return()
    }
    showModal(modalDialog(
      title = "Benutzer löschen",
      paste0("Möchten Sie den Benutzer '", target, "' wirklich endgültig löschen?"),
      footer = tagList(
        modalButton("Abbrechen"),
        actionButton("admin_delete_confirm", "Löschen", class = "btn btn-danger")
      ),
      easyClose = TRUE
    ))
  })
  
  observeEvent(input$admin_delete_confirm, {
    req(rv$authed, identical(rv$role, "admin"))
    target <- input$admin_target_user
    if (is.null(target) || !nzchar(target) || identical(target, rv$user)) {
      removeModal(); return()
    }
    con <- pg_con(); on.exit(dbDisconnect(con), add = TRUE)
    tryCatch({
      dbExecute(con, "DELETE FROM app_users WHERE username=$1", params = list(target))
      removeModal()
      bump_admin_refresh()
      showNotification(sprintf("Benutzer '%s' gelöscht.", target), type = "message")
    }, error = function(e) {
      removeModal()
      showNotification(paste("Fehler beim Löschen:", conditionMessage(e)), type = "error")
    })
  })
  
  # ---- Toggle role admin <-> user
  observeEvent(input$admin_toggle_role, {
    req(rv$authed, identical(rv$role, "admin"))
    target <- input$admin_target_user
    if (is.null(target) || !nzchar(target)) {
      showNotification("Bitte einen Benutzer auswählen.", type = "error"); return()
    }
    if (identical(target, rv$user)) {
      showNotification("Sie können Ihre eigene Rolle nicht ändern.", type = "error"); return()
    }
    con <- pg_con(); on.exit(dbDisconnect(con), add = TRUE)
    cur <- DBI::dbGetQuery(con,
                           "SELECT COALESCE(role,'user') AS role FROM app_users WHERE username=$1",
                           params = list(target))
    if (!nrow(cur)) {
      showNotification("Benutzer nicht gefunden.", type = "error"); return()
    }
    new_role <- if (identical(cur$role[1], "admin")) "user" else "admin"
    dbExecute(con, "UPDATE app_users SET role=$1 WHERE username=$2",
              params = list(new_role, target))
    bump_admin_refresh()
    showNotification(sprintf("Rolle von '%s' ist jetzt '%s'.", target, new_role),
                     type = "message")
  })
  
  # ---- Reset password (force user to set a new one on next login)
  observeEvent(input$admin_reset_pw, {
    req(rv$authed, identical(rv$role, "admin"))
    target <- input$admin_target_user
    if (is.null(target) || !nzchar(target)) {
      showNotification("Bitte einen Benutzer auswählen.", type = "error"); return()
    }
    con <- pg_con(); on.exit(dbDisconnect(con), add = TRUE)
    force_reset_db(con, target)
    bump_admin_refresh()
    showNotification(sprintf("Passwort für '%s' zurückgesetzt. Beim nächsten Login wird ein neues verlangt.", target),
                     type = "message")
  })
  
  # ---- Overview (hub) header image render ----
  output$hub_header_image <- renderUI({
    # read global hub header image path from DB
    con <- pg_con(); on.exit(dbDisconnect(con), add = TRUE)
    l <- load_app_image(con, id = "hub_header")
    if (!is.null(l$img_path) && nzchar(l$img_path)) {
      # img_path is relative to www/ (e.g. 'uploads/hub-20251029.png')
      tags$div(class = "app-header", tags$img(src = l$img_path, alt = "Hub Header"))
    } else {
      return(NULL)
    }
  })
  
  # ---- Layout tab: populate device selector choices ----
  observe({
    con <- pg_con(); on.exit(dbDisconnect(con), add = TRUE)
    devices <- DBI::dbGetQuery(con, "SELECT device_id, label FROM devices ORDER BY device_id")
    devices <- devices[!(devices$device_id %in% RETIRED_DEVICE_IDS), , drop = FALSE]
    if (nrow(devices)) {
      choices <- setNames(devices$device_id, devices$label)
      updateSelectInput(session, "layout_device", choices = choices, selected = devices$device_id[1])
    }
  })
  
  # Provide layout save controls only for admins (for the layout tab)
  output$layout_save_controls <- renderUI({
    if (is.null(rv$role) || rv$role != "admin") {
      div(class = "alert alert-info", "Nur Admins können hier Layout-Metadaten ändern.")
    } else {
      actionButton("save_layout", "Speichern (Admin)", class = "btn btn-primary")
    }
  })
  
  # ---- Save layout (admin-only) - note: we do not provide image upload in-app; images are expected to be placed in www/uploads/ manually ----
  observeEvent(input$save_layout, {
    req(rv$authed)
    if (is.null(rv$role) || rv$role != "admin") {
      showNotification("Nur Admins können Layouts speichern.", type = "error"); return()
    }
    con <- pg_con(); on.exit(dbDisconnect(con), add = TRUE)
    who <- rv$user %||% "system"
    if (!is.null(input$layout_device)) {
      did <- input$layout_device
      title <- if (!is.null(input$layout_title) && nzchar(input$layout_title)) input$layout_title else NULL
      footer_text <- if (!is.null(input$layout_footer_text) && nzchar(input$layout_footer_text)) input$layout_footer_text else NULL
      # Note: version and valid_from fields removed as they're not currently used
      save_device_layout(con, did,
                         title = title,
                         footer_text = footer_text,
                         footer_path = NULL,
                         footer_mime = NULL,
                         who = who)
      showNotification("Geräte-Metadaten gespeichert.", type = "message")
    }
  })
  
  # Pre-fill the global header-logo path field with whatever is currently set.
  observeEvent(input$tabs, {
    if (!identical(input$tabs, "layout")) return()
    con <- pg_con(); on.exit(dbDisconnect(con), add = TRUE)
    l <- load_app_image(con, id = "hub_header")
    updateTextInput(session, "hub_header_path", value = l$img_path %||% "")
  })
  
  # ---- Save global header logo path (admin-only) ----
  observeEvent(input$save_hub_header, {
    req(rv$authed)
    if (is.null(rv$role) || rv$role != "admin") {
      showNotification("Nur Admins können das Logo ändern.", type = "error"); return()
    }
    path <- trimws(input$hub_header_path %||% "")
    if (!nzchar(path)) {
      showNotification("Bitte einen Pfad angeben (z. B. 'hub-header.png').", type = "error"); return()
    }
    con <- pg_con(); on.exit(dbDisconnect(con), add = TRUE)
    tryCatch({
      save_app_image(con, id = "hub_header", img_path = path, who = rv$user %||% "system")
      showNotification(sprintf("Logo-Pfad gespeichert: %s", path), type = "message")
    }, error = function(e) {
      showNotification(paste("Fehler beim Speichern:", conditionMessage(e)), type = "error")
    })
  })
  
  # ---- Übersicht (hub) ----
  count_open_today <- function(con, device_id, df) {
    if (is.null(df) || !nrow(df)) return(0L)
    today <- Sys.Date()
    hdrs  <- as.character(df$Header)
    tasks <- if ("Task" %in% names(df)) as.character(df$Task) else rep("", nrow(df))
    cur_hdr <- ""
    data_rows <- integer(0)
    for (i in seq_len(nrow(df))) {
      h <- hdrs[i]
      if (nzchar(h)) { cur_hdr <- h; next }
      if (!nzchar(tasks[i])) next
      if (is_task_relevant_on(cur_hdr, today)) data_rows <- c(data_rows, i)
    }
    if (!length(data_rows)) return(0L)
    cur_m <- as.integer(format(today, "%m"))
    cur_y <- as.integer(format(today, "%Y"))
    cs <- load_cell_status(con, device_id, cur_m, cur_y)
    today_day <- as.integer(format(today, "%d"))
    done_rows <- unique(cs$row_index[cs$day == today_day])
    sum(!(data_rows %in% done_rows))
  }
  
  # Same idea but for yesterday's calendar day (handles month/year rollover).
  count_open_yesterday <- function(con, device_id, df) {
    if (is.null(df) || !nrow(df)) return(0L)
    yd    <- Sys.Date() - 1L
    hdrs  <- as.character(df$Header)
    tasks <- if ("Task" %in% names(df)) as.character(df$Task) else rep("", nrow(df))
    cur_hdr <- ""
    data_rows <- integer(0)
    for (i in seq_len(nrow(df))) {
      h <- hdrs[i]
      if (nzchar(h)) { cur_hdr <- h; next }
      if (!nzchar(tasks[i])) next
      if (is_task_relevant_on(cur_hdr, yd)) data_rows <- c(data_rows, i)
    }
    if (!length(data_rows)) return(0L)
    y_d <- as.integer(format(yd, "%d"))
    y_m <- as.integer(format(yd, "%m"))
    y_y <- as.integer(format(yd, "%Y"))
    cs <- load_cell_status(con, device_id, y_m, y_y)
    done_rows <- unique(cs$row_index[cs$day == y_d])
    sum(!(data_rows %in% done_rows))
  }
  
  # Generic counter: how many tasks are relevant on date `d` for this device,
  # and how many of them still have no entry. Returns c(total, open).
  count_tasks_on_date <- function(con, device_id, df, d, cs = NULL) {
    if (is.null(df) || !nrow(df)) return(c(total = 0L, open = 0L))
    hdrs  <- as.character(df$Header)
    tasks <- if ("Task" %in% names(df)) as.character(df$Task) else rep("", nrow(df))
    cur_hdr <- ""
    data_rows <- integer(0)
    for (i in seq_len(nrow(df))) {
      h <- hdrs[i]
      if (nzchar(h)) { cur_hdr <- h; next }
      if (!nzchar(tasks[i])) next
      if (is_task_relevant_on(cur_hdr, d)) data_rows <- c(data_rows, i)
    }
    if (!length(data_rows)) return(c(total = 0L, open = 0L))
    if (is.null(cs)) {
      cs <- load_cell_status(con, device_id,
                             as.integer(format(d, "%m")), as.integer(format(d, "%Y")))
    } else {
      cs <- cs[cs$device_id == device_id &
                 cs$month == as.integer(format(d, "%m")) &
                 cs$year  == as.integer(format(d, "%Y")), , drop = FALSE]
    }
    done_rows <- unique(cs$row_index[cs$day == as.integer(format(d, "%d"))])
    c(total = length(data_rows),
      open  = as.integer(sum(!(data_rows %in% done_rows))))
  }
  
  # Build a summary of devices with open tasks for today.
  # Returns a data.frame with columns: device_id, label, open_count,
  # open_count_yesterday.
  # Devices no longer in service. They may still physically exist in the
  # `devices` table if the DELETE in ensure_schema() was blocked by a
  # foreign-key reference to old historical data -- excluding them here
  # too means "Geräte gesamt" / "Geräte betroffen" are correct either way,
  # instead of depending on that delete having actually succeeded.
  RETIRED_DEVICE_IDS <- c("g12", "g13", "g14")
  
  compute_due_today <- function() {
    con <- pg_con(); on.exit(dbDisconnect(con), add = TRUE)
    devs <- DBI::dbGetQuery(con, "SELECT device_id, label FROM devices ORDER BY device_id")
    devs <- devs[!(devs$device_id %in% RETIRED_DEVICE_IDS), , drop = FALSE]
    if (!nrow(devs)) {
      devs$open_count <- integer(0)
      devs$open_count_yesterday <- integer(0)
      devs$total_count <- integer(0)
      devs$total_count_yesterday <- integer(0)
      return(devs)
    }
    yd <- Sys.Date() - 1L
    # Today's and yesterday's entries of ALL devices in one query, instead of
    # two queries per device (~30 round trips per dashboard refresh).
    day_parts <- function(d) as.integer(format(d, c("%Y", "%m", "%d")))
    td <- day_parts(Sys.Date()); ydp <- day_parts(yd)
    cs_all <- tryCatch(DBI::dbGetQuery(con, "
      SELECT device_id, row_index, day, month, year
        FROM device_cell_status
       WHERE (year = $1 AND month = $2 AND day = $3)
          OR (year = $4 AND month = $5 AND day = $6)
    ", params = list(td[1], td[2], td[3], ydp[1], ydp[2], ydp[3])),
    error = function(e) NULL)
    counts <- lapply(seq_len(nrow(devs)), function(i) {
      did <- devs$device_id[i]
      df_tmp <- tryCatch(load_device_table(con, did) %||% create_initial_table(did),
                         error = function(e) NULL)
      zero <- c(total = 0L, open = 0L)
      list(
        today     = tryCatch(count_tasks_on_date(con, did, df_tmp, Sys.Date(), cs_all),
                             error = function(e) zero),
        yesterday = tryCatch(count_tasks_on_date(con, did, df_tmp, yd, cs_all),
                             error = function(e) zero)
      )
    })
    devs$open_count            <- vapply(counts, function(x) as.integer(x$today[["open"]]), integer(1))
    devs$total_count           <- vapply(counts, function(x) as.integer(x$today[["total"]]), integer(1))
    devs$open_count_yesterday  <- vapply(counts, function(x) as.integer(x$yesterday[["open"]]), integer(1))
    devs$total_count_yesterday <- vapply(counts, function(x) as.integer(x$yesterday[["total"]]), integer(1))
    devs
  }
  
  # Show a friendly modal listing devices with open tasks today.
  # Each device with open tasks gets a button that opens its checklist.
  # `force = TRUE` is used when the user explicitly clicks a dashboard tile:
  # in that case neither the admin-only rule nor today's acknowledgement
  # should suppress the dialog.
  show_due_tasks_dialog <- function(force = FALSE) {
    # This login summary popup (Offene Wartungsaufgaben heute / nach
    # Arbeitsplatz) is for admins only. Regular users just go straight to
    # their checklist without the overview dialog.
    if (!force && !identical(rv$role, "admin")) return(invisible(NULL))
    devs <- tryCatch(compute_due_today(), error = function(e) NULL)
    if (is.null(devs) || !nrow(devs)) return(invisible(NULL))
    
    if (is.null(devs$open_count_yesterday))
      devs$open_count_yesterday <- 0L
    open_devs <- devs[devs$open_count > 0 | devs$open_count_yesterday > 0,
                      , drop = FALSE]
    if (!nrow(open_devs)) return(invisible(NULL))
    
    # Fingerprint of exactly what would be shown today; if the user already
    # acknowledged this same set today, skip the popup so it doesn't repeat
    # on every login. A change in the open tasks (new device, higher count)
    # yields a different fingerprint, so it surfaces again.
    fp_src <- paste(sort(paste0(open_devs$device_id, ":", open_devs$open_count,
                                ":", open_devs$open_count_yesterday)),
                    collapse = "|")
    fingerprint <- paste0(Sys.Date(), "#", fp_src)
    already_acked <- tryCatch({
      con_a <- pg_con(); on.exit(dbDisconnect(con_a), add = TRUE)
      ack <- dbGetQuery(con_a,
                        "SELECT fingerprint FROM due_dialog_ack WHERE ack_date = $1",
                        params = list(as.character(Sys.Date())))
      nrow(ack) && identical(ack$fingerprint[1], fingerprint)
    }, error = function(e) FALSE)
    if (isTRUE(already_acked) && !force) return(invisible(NULL))
    rv$due_dialog_fingerprint <- fingerprint
    
    today_str <- to_german_date_str(format(Sys.Date(), "%A, %d.%m.%Y"))
    yesterday_str <- to_german_date_str(format(Sys.Date() - 1L, "%A, %d.%m.%Y"))
    total_open <- sum(open_devs$open_count)
    total_yest <- sum(open_devs$open_count_yesterday)
    
    # Shared CSS for the modal (injected once via the dialog body)
    modal_css <- tags$style(HTML("
      .due-modal .modal-content {
        border: none;
        border-radius: 16px;
        overflow: hidden;
        box-shadow: 0 25px 60px rgba(0,0,0,0.25);
      }
      .due-modal .modal-header,
      .due-modal .modal-footer { display: none; }
      .due-modal .modal-body { padding: 0; }

      .due-hero {
        padding: 26px 28px 22px 28px;
        color: #fff;
        position: relative;
      }
      .due-hero.alert  { background: linear-gradient(135deg, #003B73 0%, #136377 100%); }
      .due-hero.ok     { background: linear-gradient(135deg, #00b894 0%, #00897b 100%); }

      .due-hero .hero-row {
        display: flex; align-items: center; gap: 18px;
      }
      .due-hero .hero-icon {
        width: 54px; height: 54px; border-radius: 14px;
        background: rgba(255,255,255,0.18);
        display: flex; align-items: center; justify-content: center;
        font-size: 26px;
        flex-shrink: 0;
      }
      .due-hero .hero-title {
        font-size: 20px; font-weight: 800; line-height: 1.15; margin: 0;
        color: #fff;
      }
      .due-hero .hero-sub {
        font-size: 12px; opacity: 0.9; letter-spacing: .8px;
        text-transform: uppercase; margin-bottom: 4px;
      }
      .due-hero .hero-stats {
        display: flex; gap: 14px; margin-top: 18px; flex-wrap: wrap;
      }
      .due-hero .hero-pill {
        background: rgba(255,255,255,0.18);
        padding: 8px 14px;
        border-radius: 999px;
        font-size: 13px; font-weight: 600;
      }
      .due-hero .hero-pill b { font-size: 15px; }
      .due-hero .hero-pill-warn {
        background: rgba(255, 193, 7, 0.28);
        border: 1px solid rgba(255, 235, 59, 0.55);
      }

      .due-body { padding: 18px 22px 8px 22px; background: #fafbfc; }
      .due-body .due-list-title {
        font-size: 12px; font-weight: 700; color: #6c757d;
        letter-spacing: 1.2px; text-transform: uppercase; margin: 4px 4px 10px 4px;
      }
      .due-list { max-height: 50vh; overflow-y: auto; padding-right: 4px; }

      .due-ap-group { margin-bottom: 18px; }
      .due-ap-title {
        display: flex; align-items: center; gap: 10px;
        font-size: 13px; font-weight: 800;
        letter-spacing: 1.2px; text-transform: uppercase;
        color: #fff;
        background: linear-gradient(135deg, #003B73 0%, #136377 100%);
        padding: 8px 14px; border-radius: 8px;
        margin: 4px 0 10px 0;
        box-shadow: 0 3px 8px rgba(0,59,115,0.18);
      }
      .due-ap-title .due-ap-icon {
        width: 26px; height: 26px; border-radius: 7px;
        background: rgba(255,255,255,0.18);
        display: inline-flex; align-items: center; justify-content: center;
        font-size: 13px;
      }
      .due-ap-title .due-ap-count {
        margin-left: auto;
        background: rgba(255,255,255,0.22);
        padding: 2px 10px; border-radius: 999px;
        font-size: 11px; font-weight: 700; letter-spacing: .5px;
      }
      .due-card {
        display: flex; align-items: center; justify-content: space-between;
        background: #ffffff;
        border: 1px solid #e9ecef;
        border-left: 4px solid #136377;
        border-radius: 10px;
        padding: 12px 14px;
        margin-bottom: 8px;
        transition: transform .15s ease, box-shadow .2s ease;
      }
      .due-card:hover {
        transform: translateY(-1px);
        box-shadow: 0 6px 14px rgba(0,59,115,0.12);
      }
      .due-card .due-info { display: flex; align-items: center; gap: 12px; min-width: 0; }
      .due-card .due-count {
        background: linear-gradient(135deg, #003B73 0%, #136377 100%);
        color: #fff;
        font-weight: 800;
        min-width: 38px; height: 38px;
        border-radius: 50%;
        display: flex; align-items: center; justify-content: center;
        font-size: 15px;
        box-shadow: 0 3px 8px rgba(0,59,115,0.35);
        flex-shrink: 0;
      }
      .due-card .due-name {
        font-weight: 600; color: #2c3e50; font-size: 14px;
        white-space: nowrap; overflow: hidden; text-overflow: ellipsis;
      }
      .due-card .due-meta {
        font-size: 11px; color: #6c757d;
      }
      .due-card .due-yest-badge {
        display: inline-flex; align-items: center; gap: 4px;
        margin-top: 4px;
        background: #fff8e1;
        color: #8d6e00;
        border: 1px solid #ffe082;
        border-radius: 999px;
        padding: 2px 10px;
        font-size: 11px; font-weight: 700;
      }
      .due-card .btn-open {
        background: #003B73; color: #fff; border: none;
        font-weight: 600; font-size: 12px;
        padding: 8px 14px; border-radius: 8px;
        white-space: nowrap;
      }
      .due-card .btn-open:hover { background: #136377; color:#fff; }

      .due-foot {
        padding: 12px 22px 18px 22px;
        background: #fafbfc;
        display: flex; align-items: center; justify-content: space-between;
        border-top: 1px solid #eef1f4;
      }
      .due-foot .foot-hint { font-size: 12px; color: #6c757d; }
      .due-foot .btn-later {
        background: transparent; border: 1px solid #ced4da;
        color: #495057; font-weight: 600; padding: 8px 16px; border-radius: 8px;
      }
      .due-foot .btn-later:hover { background:#f1f3f5; }
    "))
    
    if (!nrow(open_devs)) {
      showModal(modalDialog(
        size = "m", easyClose = TRUE, footer = NULL,
        class = "due-modal",
        modal_css,
        tags$div(class = "due-hero ok",
                 tags$div(class = "hero-row",
                          tags$div(class = "hero-icon", icon("check")),
                          tags$div(
                            tags$div(class = "hero-sub", today_str),
                            tags$h3(class = "hero-title", "Alles erledigt!")
                          )
                 ),
                 tags$p(style = "margin: 14px 0 0 0; opacity: 0.95;",
                        "Für heute sind keine offenen Wartungsaufgaben vorhanden. ",
                        "Schöner Tag!")
        ),
        tags$div(class = "due-foot",
                 tags$div(class = "foot-hint",
                          icon("info-circle"), " Sie können diese Übersicht später jederzeit öffnen."),
                 modalButton("Schließen") |> tagAppendAttributes(class = "btn-later")
        )
      ))
      return(invisible(NULL))
    }
    
    # ── Arbeitsplatz definitions (mirrors hub_buttons) ──────────────────────
    arbeitsplaetze <- list(
      list(name = "Arbeitsplatz 1 \u2013 H\u00e4matologie",
           icon = "tint",
           devices = c("g7", "g15")),
      list(name = "Arbeitsplatz 2 \u2013 Gerinnung",
           icon = "wave-square",
           devices = c("g1", "g6", "g16", "g11")),
      list(name = "Arbeitsplatz 3 \u2013 Klinische Chemie",
           icon = "flask",
           devices = c("g4", "g5", "g9")),
      list(name = "Arbeitsplatz 4 \u2013 Immunologie / Allergie",
           icon = "shield-alt",
           devices = c("g2", "g8", "g10")),
      list(name = "Arbeitsplatz 5 \u2013 Cobas 8100",
           icon = "vial", 
           devices = c("g3", "g17") 
      )
    )
    
    make_dev_card <- function(did, lbl, cnt, cnt_y = 0L) {
      meta_today <- if (cnt > 0)
        sprintf("%d offene Aufgabe%s heute", cnt, ifelse(cnt == 1, "", "n"))
      else NULL
      meta_yest <- if (isTRUE(cnt_y > 0))
        sprintf("%d offen seit gestern", cnt_y)
      else NULL
      meta <- paste(c(meta_today, meta_yest), collapse = " \u00b7 ")
      tags$div(class = "due-card",
               tags$div(class = "due-info",
                        tags$div(
                          tags$div(class = "due-name", lbl),
                          tags$div(class = "due-meta", meta),
                          if (isTRUE(cnt_y > 0))
                            tags$div(class = "due-yest-badge",
                                     icon("clock-rotate-left"),
                                     sprintf(" Gestern noch %d offen", cnt_y))
                        )
               ),
               actionButton(
                 inputId = paste0("due_open_", did),
                 label   = tagList(icon("arrow-right"), " \u00d6ffnen"),
                 class   = "btn-open"
               )
      )
    }
    
    # Build one section per Arbeitsplatz (only if it has open devices today)
    assigned_ids <- unlist(lapply(arbeitsplaetze, `[[`, "devices"))
    ap_sections <- lapply(arbeitsplaetze, function(ap) {
      sub <- open_devs[open_devs$device_id %in% ap$devices, , drop = FALSE]
      if (!nrow(sub)) return(NULL)
      ord <- match(ap$devices, sub$device_id)
      sub <- sub[ord[!is.na(ord)], , drop = FALSE]
      ap_total <- sum(sub$open_count)
      ap_yest  <- sum(sub$open_count_yesterday)
      cards <- lapply(seq_len(nrow(sub)), function(i)
        make_dev_card(sub$device_id[i], sub$label[i], sub$open_count[i],
                      sub$open_count_yesterday[i]))
      tags$div(class = "due-ap-group",
               tags$div(class = "due-ap-title",
                        tags$span(class = "due-ap-icon", icon(ap$icon)),
                        tags$span(ap$name),
                        tags$span(class = "due-ap-count",
                                  paste0(
                                    sprintf("%d offen", ap_total),
                                    if (ap_yest > 0) sprintf(" \u00b7 %d seit gestern", ap_yest) else ""
                                  ))
               ),
               cards
      )
    })
    
    # Devices with open tasks that aren't mapped to any Arbeitsplatz
    leftover <- open_devs[!(open_devs$device_id %in% assigned_ids), , drop = FALSE]
    if (nrow(leftover)) {
      cards <- lapply(seq_len(nrow(leftover)), function(i)
        make_dev_card(leftover$device_id[i], leftover$label[i],
                      leftover$open_count[i],
                      leftover$open_count_yesterday[i]))
      ap_sections <- c(ap_sections, list(
        tags$div(class = "due-ap-group",
                 tags$div(class = "due-ap-title",
                          tags$span(class = "due-ap-icon", icon("microscope")),
                          tags$span("Weitere Ger\u00e4te"),
                          tags$span(class = "due-ap-count",
                                    sprintf("%d offen", sum(leftover$open_count)))
                 ),
                 cards
        )
      ))
    }
    dev_rows <- Filter(Negate(is.null), ap_sections)
    
    showModal(modalDialog(
      size = "m", easyClose = TRUE, footer = NULL,
      class = "due-modal",
      modal_css,
      tags$div(class = "due-hero alert",
               tags$div(class = "hero-row",
                        tags$div(class = "hero-icon", icon("triangle-exclamation")),
                        tags$div(
                          tags$div(class = "hero-sub", today_str),
                          tags$h3(class = "hero-title", "Offene Wartungsaufgaben heute")
                        )
               ),
               tags$div(class = "hero-stats",
                        tags$div(class = "hero-pill",
                                 tags$b(total_open),
                                 sprintf(" Aufgabe%s offen heute", ifelse(total_open == 1, "", "n"))),
                        tags$div(class = "hero-pill",
                                 tags$b(nrow(open_devs)),
                                 sprintf(" Gerät%s betroffen", ifelse(nrow(open_devs) == 1, "", "e"))),
                        if (isTRUE(total_yest > 0))
                          tags$div(class = "hero-pill hero-pill-warn",
                                   icon("clock-rotate-left"), " ",
                                   tags$b(total_yest),
                                   sprintf(" Aufgabe%s noch offen von gestern",
                                           ifelse(total_yest == 1, "", "n")))
               )
      ),
      tags$div(class = "due-body",
               tags$div(class = "due-list-title",
                        icon("list-check"), " Offene Aufgaben \u2013 nach Arbeitsplatz"),
               tags$div(class = "due-list", dev_rows)
      ),
      tags$div(class = "due-foot",
               tags$div(class = "foot-hint",
                        icon("hand-pointer"),
                        " Klicken Sie auf „Öffnen“, um zur Checkliste zu springen."),
               tags$div(style = "display:flex; gap:8px;",
                        actionButton("due_dialog_ack_btn",
                                     label = tagList(icon("eye"), " Als gelesen markieren"),
                                     class = "btn-later"),
                        modalButton("Später") |> tagAppendAttributes(class = "btn-later")
               )
      )
    ))
  }
  
  # "Als gelesen markieren": remember today's fingerprint so the same open-
  # tasks popup doesn't reappear on every subsequent login today.
  observeEvent(input$due_dialog_ack_btn, {
    fp <- isolate(rv$due_dialog_fingerprint)
    if (nzchar(fp %||% "")) {
      tryCatch({
        con_a <- pg_con(); on.exit(dbDisconnect(con_a), add = TRUE)
        dbExecute(con_a, "
          INSERT INTO due_dialog_ack (ack_date, fingerprint, acked_by)
          VALUES ($1, $2, $3)
          ON CONFLICT (ack_date) DO UPDATE
            SET fingerprint = EXCLUDED.fingerprint,
                acked_by    = EXCLUDED.acked_by,
                acked_at    = NOW()
        ", params = list(as.character(Sys.Date()), fp, isolate(rv$user) %||% ""))
      }, error = function(e) NULL)
    }
    removeModal()
  })
  
  
  output$hub_intro <- renderUI({
    req(rv$authed)
    who <- rv$user %||% ""
    today_str <- format_de_date(Sys.Date())
    tags$div(class = "hub-intro",
             tags$div(class = "hub-eyebrow", "Zentrallabor – Wartungsplan"),
             tags$h2(sprintf("Willkommen, %s!", who)),
             tags$p(paste0("Heute ist ", today_str, ". Hier sehen Sie alle Geräte ",
                           "auf einen Blick. Klicken Sie auf eine Karte, um die ",
                           "Tagesaufgaben zu öffnen."))
    )
  })
  
  # Per-device counts for the overview, computed ONCE per refresh and shared
  # by the summary (hub_summary) and the device cards (hub_buttons). Before,
  # both computed everything on their own -- every device's plan and entries
  # were loaded twice on every visit to the overview and after every tick.
  hub_counts <- reactive({
    req(rv$authed)
    rv$tasks_refresh  # reactive dependency: refresh live when any task is toggled
    tryCatch(compute_due_today(), error = function(e) NULL)
  })
  
  output$hub_summary <- renderUI({
    req(rv$authed)
    devs <- hub_counts()
    if (is.null(devs) || !nrow(devs)) return(NULL)
    if (is.null(devs$total_count))           devs$total_count <- 0L
    if (is.null(devs$open_count_yesterday))  devs$open_count_yesterday <- 0L
    if (is.null(devs$total_count_yesterday)) devs$total_count_yesterday <- 0L
    
    n_total    <- nrow(devs)
    sum_open   <- sum(devs$open_count)
    sum_due    <- sum(devs$total_count)
    sum_done   <- max(0L, sum_due - sum_open)
    n_dev_open <- sum(devs$open_count > 0)
    sum_yest   <- sum(devs$open_count_yesterday)
    n_dev_yest <- sum(devs$open_count_yesterday > 0)
    pct        <- if (sum_due > 0) round(100 * sum_done / sum_due) else 100
    
    open_modal_js <- paste0("Shiny.setInputValue('hub_open_due_modal', ",
                            "Math.random(), {priority:'event'});")
    
    # One KPI tile. `tone` drives the accent colour, `hint` is the small
    # context line under the label so the number is self-explanatory.
    kpi <- function(tone, ico, num, lbl, hint, clickable = FALSE) {
      tags$div(
        class = paste("sum-card", tone, if (clickable) "clickable" else ""),
        title = if (clickable) "Klicken f\u00fcr die Aufschl\u00fcsselung pro Ger\u00e4t" else NULL,
        onclick = if (clickable) open_modal_js else NULL,
        style = if (clickable) "cursor:pointer;" else NULL,
        tags$div(class = "sum-icon", icon(ico)),
        tags$div(style = "min-width:0;",
                 tags$div(class = "sum-num", num),
                 tags$div(class = "sum-lbl", lbl),
                 tags$div(class = "sum-hint", hint)
        )
      )
    }
    
    tagList(
      tags$div(
        class = "hub-summary",
        kpi("neutral", "microchip", n_total, "Ger\u00e4te im Plan",
            sprintf("%d mit Aufgaben heute", sum(devs$total_count > 0))),
        kpi(if (sum_open > 0) "warn" else "ok",
            if (sum_open > 0) "triangle-exclamation" else "check",
            sum_open, "Offen heute",
            if (sum_open > 0)
              sprintf("auf %d Ger\u00e4t%s \u2013 von %d f\u00e4lligen Aufgaben",
                      n_dev_open, if (n_dev_open == 1) "" else "en", sum_due)
            else "Alles erledigt \u2013 super!",
            clickable = TRUE),
        kpi(if (sum_yest > 0) "danger" else "ok",
            if (sum_yest > 0) "clock-rotate-left" else "thumbs-up",
            sum_yest, "\u00dcberf\u00e4llig von gestern",
            if (sum_yest > 0)
              sprintf("%s \u2013 auf %d Ger\u00e4t%s",
                      format_de_date(Sys.Date() - 1L), n_dev_yest,
                      if (n_dev_yest == 1) "" else "en")
            else sprintf("%s war vollst\u00e4ndig", format_de_date(Sys.Date() - 1L)),
            clickable = sum_yest > 0)
      ),
      # Progress bar for today, so the team sees at a glance how far along
      # the day is without counting cards.
      tags$div(class = "hub-progress",
               tags$div(class = "hp-head",
                        tags$span(class = "hp-title",
                                  icon("chart-simple"), " Tagesfortschritt"),
                        tags$span(class = "hp-val",
                                  sprintf("%d von %d erledigt (%d%%)",
                                          sum_done, sum_due, pct))),
               tags$div(class = "hp-track",
                        tags$div(class = paste("hp-fill",
                                               if (pct >= 100) "full" else ""),
                                 style = sprintf("width:%d%%;", pct)))
      )
    )
  })
  
  output$hub_buttons <- renderUI({
    req(rv$authed)
    counts <- hub_counts()  # also re-renders live when any task is toggled
    con <- pg_con(); on.exit(dbDisconnect(con), add = TRUE)
    devices <- DBI::dbGetQuery(con, "SELECT device_id, label FROM devices ORDER BY device_id")
    
    # ── Arbeitsplatz definitions ──────────────────────────────────────────────
    # Colours follow the physical lid colours used in the lab, per the
    # colleague's feedback: Hämatologie rot, Gerinnung grün, Klinische Chemie
    # orange, Immunologie/Allergologie braun.
    arbeitsplaetze <- list(
      list(
        name    = "Arbeitsplatz 1 \u2013 H\u00e4matologie",
        icon    = "tint",
        color   = "#c62828",
        bg      = "#fdecea",
        border  = "#c62828",
        devices = c("g7", "g15")
      ),
      list(
        name    = "Arbeitsplatz 2 \u2013 Gerinnung",
        icon    = "wave-square",
        color   = "#2e7d32",
        bg      = "#eef7ee",
        border  = "#2e7d32",
        devices = c("g1", "g6", "g16", "g11")
      ),
      list(
        name    = "Arbeitsplatz 3 \u2013 Klinische Chemie",
        icon    = "flask",
        color   = "#ef6c00",
        bg      = "#fff4e5",
        border  = "#ef6c00",
        devices = c("g4", "g5", "g9")
      ),
      list(
        name    = "Arbeitsplatz 4 \u2013 Immunologie / Allergie",
        icon    = "shield-alt",
        color   = "#6d4c41",
        bg      = "#f5efec",
        border  = "#6d4c41",
        devices = c("g2", "g8", "g10")
      ),
      list(
        name    = "Arbeitsplatz 5 \u2013 Cobas 8100",
        icon    = "vial",
        color   = "#ff9800",
        bg      = "#fff3e0",
        border  = "#ff9800",
        devices = c("g3", "g17")
      )
    )
    
    # ── Helper: build one device card ─────────────────────────────────────────
    make_device_card <- function(did, lbl, open_count, ap_color, ap_bg, ap_border) {
      state_cls   <- if (open_count > 0) "warn" else "ok"
      status_lbl  <- if (open_count > 0) sprintf("%d offen", open_count) else "Erledigt"
      status_icon <- if (open_count > 0) icon("circle-exclamation") else icon("check")
      
      # Card border-left accent: red when tasks open, Arbeitsplatz colour when done
      card_border <- if (open_count > 0) "#e53935" else ap_border
      
      actionButton(
        inputId = paste0("open_", did),
        # Override .hub-card-btn background via inline style on the label wrapper
        class   = paste("hub-card-btn", state_cls),
        style   = sprintf(
          "background: %s !important;
         border-left: 5px solid %s !important;
         border-color: %s !important;",
          ap_bg, card_border, ap_border
        ),
        label   = tags$div(
          class = "hub-card",
          tags$div(
            class = "hc-row",
            tags$div(
              class = "hc-icon",
              style = sprintf("background: linear-gradient(135deg, %s 0%%, %s99 100%%);",
                              ap_color, ap_color),
              icon("microscope")
            ),
            tags$div(
              class = "hc-text",
              # Device name only — ID removed
              tags$div(class = "hc-name",
                       style = sprintf("color: %s;", ap_color),
                       lbl)
            )
          ),
          tags$div(
            class = "hc-bottom",
            tags$span(class = paste("hc-status", state_cls), status_icon, " ", status_lbl),
            tags$span(class = "hc-cta",
                      style = sprintf("color: %s;", ap_color),
                      "\u00d6ffnen ", icon("arrow-right"))
          )
        )
      )
    }
    
    # ── Render one Arbeitsplatz section ───────────────────────────────────────
    make_ap_section <- function(ap) {
      ap_rows <- devices[devices$device_id %in% ap$devices, , drop = FALSE]
      ord     <- match(ap$devices, ap_rows$device_id)
      ap_rows <- ap_rows[ord[!is.na(ord)], , drop = FALSE]
      if (!nrow(ap_rows)) return(NULL)
      
      cards <- lapply(seq_len(nrow(ap_rows)), function(i) {
        did        <- ap_rows$device_id[i]
        lbl        <- ap_rows$label[i]
        # Open count from the shared hub_counts() (see above) -- no extra
        # plan load / query per card.
        open_count <- if (!is.null(counts) && did %in% counts$device_id)
          as.integer(counts$open_count[match(did, counts$device_id)]) else 0L
        make_device_card(did, lbl, open_count, ap$color, ap$bg, ap$border)
      })
      
      tags$div(
        style = "margin-bottom: 28px;",
        
        # Section header bar
        tags$div(
          style = sprintf(
            "display:flex; align-items:center; gap:10px;
           padding: 10px 16px; margin-bottom: 14px;
           background: linear-gradient(135deg, %s22 0%%, %s11 100%%);
           border-left: 4px solid %s;
           border-radius: 8px;",
            ap$color, ap$color, ap$color
          ),
          tags$div(
            style = sprintf(
              "width:34px; height:34px; border-radius:10px;
             background:%s; display:flex; align-items:center;
             justify-content:center; color:#fff; font-size:15px; flex-shrink:0;",
              ap$color
            ),
            icon(ap$icon)
          ),
          tags$div(
            tags$div(
              style = sprintf(
                "font-weight:800; font-size:15px; color:%s; line-height:1.1;",
                ap$color
              ),
              ap$name
            ),
            tags$div(
              style = "font-size:11px; color:#6c757d; margin-top:2px; letter-spacing:.5px;",
              sprintf("%d Ger\u00e4t%s",
                      nrow(ap_rows),
                      if (nrow(ap_rows) == 1) "" else "e")
            )
          )
        ),
        
        # Device cards grid
        tags$div(class = "hub-grid", do.call(tagList, cards))
      )
    }
    
    tagList(
      lapply(arbeitsplaetze, make_ap_section)
    )
  })
  
  
  
  # observers for each device button
  lapply(1:17, function(i) {
    obs_id <- paste0("open_g", i)
    observeEvent(input[[obs_id]], {
      load_device(paste0("g", i))
    })
    # observers for the post-login "due tasks" dialog buttons
    due_id <- paste0("due_open_g", i)
    observeEvent(input[[due_id]], {
      removeModal()
      updateTabItems(session, "tabs", "checklist")
      load_device(paste0("g", i))
    })
  })
  
  # Reopen the "Offene Aufgaben heute" dialog from the hub summary card.
  observeEvent(input$hub_open_due_modal, {
    req(rv$authed)
    show_due_tasks_dialog(force = TRUE)
  })
  
  # ---- Recent remarks modal --------------------------------------------------
  # Persistent dialog that replaces the old toast notification. Lists each
  # open "NE – Nicht erledigt" entry of the last 14 days with two actions:
  #   * "Zur Aufgabe" – switches to the Tägliche-Aufgaben tab and scrolls to
  #     the relevant row so the user can address the remark in context.
  #   * "Erledigt" – closes the entry (it stays documented in der
  #     Monatsübersicht) and refreshes the modal.
  show_recent_remarks_modal <- function() {
    rr <- rv$pending_remarks
    if (is.null(rr) || !nrow(rr) || is.null(rv$current_device)) {
      removeModal()
      return(invisible(NULL))
    }
    task_lookup <- if (!is.null(rv$data) && "Task" %in% names(rv$data))
      as.character(rv$data$Task) else character(0)
    hdr_lookup  <- if (!is.null(rv$data) && "Header" %in% names(rv$data))
      as.character(rv$data$Header) else character(0)
    
    safe_at <- function(v, i) {
      if (!length(v) || is.na(i) || i < 1 || i > length(v)) return("")
      x <- v[i]
      if (is.na(x)) "" else trimws(x)
    }
    # Schedule block a row belongs to (nearest non-empty Header above it) --
    # gives the user context like "Wöchentlich (Donnerstag)".
    header_above <- function(ri) {
      if (!length(hdr_lookup) || is.na(ri) || ri < 1) return("")
      h <- hdr_lookup[seq_len(min(as.integer(ri), length(hdr_lookup)))]
      h <- h[!is.na(h) & nzchar(trimws(h))]
      if (!length(h)) "" else trimws(tail(h, 1))
    }
    # Readable name for a remark's row. Falls back to the section heading if
    # the row itself carries no task text (e.g. the plan was restructured
    # after the remark was written), so the popup never shows a bare
    # "Aufgabe 18" without any clue what it refers to.
    task_label <- function(ri) {
      t <- safe_at(task_lookup, ri)
      if (nzchar(t)) return(t)
      h <- safe_at(hdr_lookup, ri)
      if (nzchar(h)) return(paste0(h, " \u2013 gesamter Abschnitt"))
      paste0("Aufgabe ", ri, " (Bezeichnung im aktuellen Plan nicht gefunden)")
    }
    
    line_items <- lapply(seq_len(nrow(rr)), function(i) {
      ri   <- rr$row_index[i]
      d    <- as.Date(rr$remark_date[i])
      opt  <- rr$option_code[i] %||% ""
      txt  <- rr$remark_text[i] %||% ""
      usr  <- rr$updated_by[i]  %||% ""
      tname <- task_label(ri)
      sched <- header_above(ri)
      key <- sprintf("%d|%s", as.integer(ri), format(d, "%Y-%m-%d"))
      tags$li(
        style = "margin-bottom:10px; padding:10px 12px; border-radius:8px;
                 background:#fff5f5; border-left:4px solid #d32f2f;
                 list-style:none;",
        tags$div(
          style = "display:flex; align-items:flex-start; gap:10px; flex-wrap:wrap;",
          tags$div(style = "flex:1 1 240px; min-width:0;",
                   tags$div(style = "font-size:11px; color:#7a1f1f; margin-bottom:2px;",
                            tags$span(style = "background:#ffe0e0; padding:1px 6px;
                                       border-radius:4px; font-weight:700;
                                       margin-right:6px;",
                                      format(d, "%d.%m.%Y")),
                            if (nzchar(sched)) tags$span(
                              style = "background:#e3eaf3; color:#003B73; padding:1px 6px;
                                       border-radius:4px; font-weight:700;
                                       margin-right:6px;", sched) else NULL,
                            tags$span(style = "opacity:.7;",
                                      sprintf("Zeile %d", as.integer(ri)))),
                   tags$div(style = "font-weight:700; color:#003B73; font-size:13px;",
                            icon("clipboard-list"), " ", tname),
                   tags$div(style = "font-size:12px; color:#555; margin-top:3px;",
                            if (nzchar(opt)) tags$span(
                              style = "background:#003B73; color:#fff; padding:1px 6px;
                         border-radius:4px; font-weight:700; margin-right:6px;",
                              option_full_label(opt)) else NULL,
                            if (nzchar(txt)) txt else NULL,
                            if (nzchar(usr)) tags$span(style = "opacity:.7; margin-left:6px;",
                                                       paste0("(", usr, ")")) else NULL
                   )
          ),
          tags$div(style = "display:flex; gap:6px; flex-shrink:0;",
                   tags$button(
                     type = "button", class = "btn btn-sm btn-default",
                     onclick = sprintf(
                       "Shiny.setInputValue('jump_to_remark', '%s', {priority:'event'});", key),
                     icon("arrow-right"), " Zur Aufgabe"
                   ),
                   tags$button(
                     type = "button", class = "btn btn-sm btn-default",
                     title = paste0("Blendet diesen Hinweis nur f\u00fcr Sie aus. ",
                                    "Kolleginnen und Kollegen werden weiterhin informiert."),
                     onclick = sprintf(
                       "Shiny.setInputValue('ack_remark', '%s', {priority:'event'});", key),
                     icon("eye-slash"), " Als gelesen markieren"
                   ),
                   tags$button(
                     type = "button", class = "btn btn-sm btn-success",
                     onclick = sprintf(
                       "Shiny.setInputValue('resolve_remark', '%s', {priority:'event'});", key),
                     icon("check"), " Erledigt"
                   )
          )
        )
      )
    })
    
    showModal(modalDialog(
      title = NULL, size = "l", easyClose = TRUE, fade = FALSE,
      footer = tagList(
        tags$span(style = "font-size:12px; color:#6c757d; margin-right:auto;",
                  icon("info-circle"),
                  " \u201eErledigt\u201c schlie\u00dft die Meldung f\u00fcr alle, ",
                  "\u201eAls gelesen markieren\u201c blendet sie nur f\u00fcr Sie aus."),
        modalButton("Sp\u00e4ter")
      ),
      tags$div(
        style = "background: linear-gradient(135deg,#d32f2f 0%,#b71c1c 100%);
                 color:#fff; padding:14px 18px; margin:-15px -15px 12px -15px;
                 border-radius:6px 6px 0 0;",
        tags$div(style = "font-size:18px; font-weight:800;",
                 icon("triangle-exclamation"),
                 " Nicht erledigte Aufgaben \u2013 bitte pr\u00fcfen"),
        tags$div(style = "font-size:12px; opacity:.92; margin-top:2px;",
                 sprintf("%d offene \u201eNicht erledigt\u201c-Meldung%s \u2013 %s",
                         nrow(rr), if (nrow(rr) == 1) "" else "en",
                         rv$current_device_title %||% rv$current_device))
      ),
      tags$ul(style = "margin:0; padding-left:0;", line_items)
    ))
  }
  
  # User clicked "Zur Aufgabe" on a remark – jump to the task in the
  # Tägliche-Aufgaben list.
  observeEvent(input$jump_to_remark, {
    req(rv$current_device, input$jump_to_remark)
    parts <- strsplit(input$jump_to_remark, "|", fixed = TRUE)[[1]]
    if (length(parts) < 1) return()
    ri <- suppressWarnings(as.integer(parts[1]))
    if (is.na(ri)) return()
    removeModal()
    updateTabItems(session, "tabs", "checklist")
    # Switch to the "Tägliche Aufgaben" sub-tab and scroll to the row.
    session$sendCustomMessage("scrollToTaskRow", list(row = ri))
    # The row is only rendered when its schedule is due today. Say so instead
    # of silently doing nothing, and name the section it belongs to.
    task_txt <- if (!is.null(rv$data) && "Task" %in% names(rv$data) &&
                    ri >= 1 && ri <= nrow(rv$data))
      trimws(as.character(rv$data$Task[ri]) %||% "") else ""
    if (!nzchar(task_txt))
      showNotification(
        sprintf(paste0("Diese Meldung geh\u00f6rt zu Zeile %d, die im aktuellen ",
                       "Plan keine Aufgabe mehr enth\u00e4lt. Bitte in der ",
                       "Monats\u00fcbersicht pr\u00fcfen."), ri),
        type = "warning", duration = 10)
  })
  
  # User clicked "Erledigt" – close the remark and refresh the modal.
  observeEvent(input$resolve_remark, {
    req(rv$current_device, input$resolve_remark)
    parts <- strsplit(input$resolve_remark, "|", fixed = TRUE)[[1]]
    if (length(parts) < 2) return()
    ri <- suppressWarnings(as.integer(parts[1]))
    rd <- parts[2]
    if (is.na(ri) || !nzchar(rd)) return()
    ok <- tryCatch({
      con_d <- pg_con(); on.exit(dbDisconnect(con_d), add = TRUE)
      resolve_remark_entry(con_d, rv$current_device, ri, rd,
                           rv$user_initials %||% rv$user)
      TRUE
    }, error = function(e) FALSE)
    if (!ok) {
      showNotification("Bemerkung konnte nicht geschlossen werden.",
                       type = "error", duration = 5)
      return()
    }
    # If the modal popup is open, drop the resolved row from its state and
    # decide whether to re-render or close it.
    had_modal <- !is.null(rv$pending_remarks)
    if (had_modal && nrow(rv$pending_remarks)) {
      keep <- !(rv$pending_remarks$row_index == ri &
                  as.character(rv$pending_remarks$remark_date) == rd)
      rv$pending_remarks <- rv$pending_remarks[keep, , drop = FALSE]
    }
    # Refresh dropdown / today_tasks (depends on rv$tasks_refresh).
    rv$tasks_refresh <- isolate(rv$tasks_refresh) + 1L
    if (had_modal) {
      if (is.null(rv$pending_remarks) || !nrow(rv$pending_remarks)) {
        rv$pending_remarks <- NULL
        removeModal()
        showNotification("Alle Bemerkungen erledigt. Danke!",
                         type = "message", duration = 4)
      } else {
        show_recent_remarks_modal()
      }
    } else {
      showNotification("Bemerkung als erledigt markiert.",
                       type = "message", duration = 3)
    }
  })
  
  # User clicked "Als gelesen markieren" – hide this remark for THIS user
  # only. Colleagues keep getting the reminder until somebody enters the
  # task as erledigt. If the remark is edited later it resurfaces here too.
  observeEvent(input$ack_remark, {
    req(rv$current_device, input$ack_remark)
    parts <- strsplit(input$ack_remark, "|", fixed = TRUE)[[1]]
    if (length(parts) < 2) return()
    ri <- suppressWarnings(as.integer(parts[1]))
    rd <- parts[2]
    if (is.na(ri) || !nzchar(rd)) return()
    who <- rv$user %||% rv$user_initials
    if (is.null(who) || !nzchar(who)) {
      showNotification("Kein Benutzer angemeldet.", type = "error", duration = 5)
      return()
    }
    ok <- tryCatch({
      con_a <- pg_con(); on.exit(dbDisconnect(con_a), add = TRUE)
      ack_remark_for_user(con_a, rv$current_device, ri, rd, who)
      TRUE
    }, error = function(e) FALSE)
    if (!ok) {
      showNotification("Hinweis konnte nicht ausgeblendet werden.",
                       type = "error", duration = 5)
      return()
    }
    had_modal <- !is.null(rv$pending_remarks)
    if (had_modal && nrow(rv$pending_remarks)) {
      keep <- !(rv$pending_remarks$row_index == ri &
                  as.character(rv$pending_remarks$remark_date) == rd)
      rv$pending_remarks <- rv$pending_remarks[keep, , drop = FALSE]
    }
    if (had_modal && (is.null(rv$pending_remarks) || !nrow(rv$pending_remarks))) {
      rv$pending_remarks <- NULL
      removeModal()
    } else if (had_modal) {
      show_recent_remarks_modal()
    }
    showNotification(
      paste0("Als gelesen markiert \u2013 dieser Hinweis wird Ihnen nicht mehr ",
             "angezeigt. Die Aufgabe bleibt f\u00fcr andere weiterhin offen."),
      type = "message", duration = 6)
  })
  
  load_device <- function(did) {
    req(rv$authed)
    rv$data <- NULL
    con <- pg_con(); on.exit(dbDisconnect(con), add = TRUE)
    
    df <- load_device_table(con, did)
    if (is.null(df)) {
      df <- create_initial_table(did)
      save_device_table(con, did, df, rv$user)
    }
    
    # ensure Task column exists
    if (!"Task" %in% names(df)) df$Task <- ""
    
    cur_m <- as.integer(format(Sys.Date(), "%m"))
    cur_y <- as.integer(format(Sys.Date(), "%Y"))
    status <- load_cell_status(con, did, cur_m, cur_y)
    rv$current_device <- did
    rv$data <- order_cols(df)
    rv$table_status <- status
    # NOTE: rv$task_obs_ids / rv$prev_task_obs_ids are deliberately NOT reset
    # here any more. The per-row observers read rv$current_device when they
    # fire, so they work for every device. Resetting the list made each device
    # opening register a NEW full set of observers on the same input ids
    # while the old ones stayed alive -- after a few devices, a single tick
    # ran several identical observers (each with its own DB writes, reload
    # and list rebuild), which made the app hang more and more over time.
    
    # Load the device title BEFORE the reminder dialog so the popup can name
    # the device it is talking about.
    dl <- load_device_layout(con, did)
    rv$current_device_title <- dl$title
    
    # Remind the user about tasks that a colleague explicitly marked
    # "NE – Nicht erledigt" and that nobody has corrected yet. Documentation
    # codes (WE, FT, Ø, W.e., ne, D, sQ) and plain notes never appear here –
    # they only live in der Monatsübersicht. The reminder returns at every
    # login until the task is entered as erledigt (or closed via "Erledigt").
    tryCatch({
      rr <- load_recent_remarks(con, did, today = Sys.Date(),
                                days_back = 14L, include_today = TRUE,
                                for_user = rv$user %||% rv$user_initials)
      rv$pending_remarks <- if (nrow(rr) > 0) rr else NULL
      if (nrow(rr) > 0) show_recent_remarks_modal()
    }, error = function(e) NULL)
    
    updateTabItems(session, "tabs", "checklist")
    output$tableRH <- renderRHandsontable({
      cells_readonly_for_headers(rv$data, rv$user_initials, rv$table_status, rv$role, rv$invalid_days)
    })
    
    # Slim device header shown at the top of the checklist page (always visible).
    output$device_info_header_slim <- renderUI({
      req(rv$current_device)
      con2 <- pg_con(); on.exit(dbDisconnect(con2), add = TRUE)
      device_info <- DBI::dbGetQuery(con2, "SELECT label FROM devices WHERE device_id = $1",
                                     params = list(rv$current_device))
      device_label <- if (nrow(device_info) > 0) device_info$label[1] else rv$current_device
      tags$div(
        style = "display:flex; align-items:center; gap:12px;
                 padding:10px 14px; margin-bottom:8px;
                 background: linear-gradient(135deg, #003B73 0%, #136377 100%);
                 color:#fff; border-radius:8px;",
        tags$div(style = "font-size:18px;", icon("microchip")),
        tags$div(style = "font-weight:600; font-size:15px;", device_label),
        tags$div(style = "margin-left:auto; background:rgba(255,255,255,0.2);
                          padding:4px 10px; border-radius:999px; font-size:12px;
                          font-weight:600; letter-spacing:.5px;",
                 rv$current_device)
      )
    })
    
    # Render the device info area (editable by any authenticated user)
    output$device_info_area <- renderUI({
      req(rv$current_device)
      con2 <- pg_con(); on.exit(dbDisconnect(con2), add = TRUE)
      dl2 <- load_device_layout(con2, rv$current_device)
      
      # Get device label for display
      device_info <- DBI::dbGetQuery(con2, "SELECT label FROM devices WHERE device_id = $1", 
                                     params = list(rv$current_device))
      device_label <- if (nrow(device_info) > 0) device_info$label[1] else rv$current_device
      
      # Load serial numbers for this device
      serials <- load_device_serials(con2, rv$current_device)
      
      is_admin <- identical(rv$role, "admin")
      
      # Build serial number inputs dynamically
      serial_section <- NULL
      if (length(serials) > 0) {
        serial_fields <- lapply(names(serials), function(device_name) {
          input_id <- paste0("sn_", gsub("[^a-z0-9]", "_", tolower(device_name)))
          
          # Safely extract values
          device_data <- serials[[device_name]]
          if (is.list(device_data)) {
            sn_value <- device_data$sn %||% ""
            label_text <- device_data$label %||% device_name
          } else {
            sn_value <- ""
            label_text <- device_name
          }
          
          inp <- textInput(input_id,
                           label = label_text,
                           value = sn_value,
                           placeholder = if (is_admin) "SN eingeben..." else "—")
          if (!is_admin) {
            # Disable input visually + functionally for non-admins
            inp <- shinyjs::disabled(inp)
          }
          tags$div(class = "serial-input-wrapper", inp)
        })
        
        serial_header <- tags$div(
          class = "serial-section-title",
          icon("barcode"),
          " Seriennummern",
          if (!is_admin) {
            tags$span(style = "margin-left:auto; background:#fff3cd; color:#856404;
                              border:1px solid #ffeeba; border-radius:999px;
                              padding:3px 10px; font-size:11px; font-weight:600;",
                      icon("lock"), " Nur Admin")
          }
        )
        
        admin_hint <- if (!is_admin) {
          tags$div(style = "margin-top:8px; font-size:12px; color:#6c757d;
                          background:#f8f9fa; border-left:3px solid #ffc107;
                          padding:8px 12px; border-radius:4px;",
                   icon("info-circle"),
                   " Seriennummern können nur von einem Administrator (Yadwinder, Frank oder Martina) geändert werden.")
        } else NULL
        
        serial_section <- tags$div(
          class = "serial-section",
          serial_header,
          tags$div(class = "serial-grid", do.call(tagList, serial_fields)),
          admin_hint
        )
      }
      
      # Device image preview
      image_preview <- NULL
      if (!is.null(dl2$footer_path) && nzchar(dl2$footer_path)) {
        image_preview <- tags$div(
          class = "device-image-preview",
          tags$div(
            style = "color: #6c757d; font-size: 12px; margin-bottom: 10px;",
            icon("image"),
            " Gerätebild (manuelles Upload in www/uploads/)"
          ),
          tags$img(src = dl2$footer_path)
        )
      }
      
      # Update timestamp
      timestamp <- NULL
      if (!is.null(dl2$updated_at)) {
        timestamp <- tags$div(
          class = "update-timestamp",
          icon("clock"),
          sprintf(" Letzte Änderung: %s von %s", 
                  format_de_datetime(dl2$updated_at), 
                  dl2$updated_by)
        )
      }
      
      # Main container — entire block is collapsed by default so users see
      # the daily tasks first. Click to expand.
      tags$details(
        class = "device-info-details",
        style = "margin-top: 8px; border: 1px solid #e3e6ea; border-radius: 8px;
                 background: #fafbfc; padding: 10px 14px;",
        tags$summary(
          style = "cursor:pointer; font-size:13px; font-weight:600; color:#495057;
                   display:flex; align-items:center; gap:8px;",
          icon("cogs"),
          "Geräte-Informationen & Bild",
          tags$span(style = "margin-left:auto; font-size:11px; color:#6c757d;
                            font-weight:500;",
                    "(zum Ein-/Ausklappen klicken)")
        ),
        tags$div(
          class = "device-info-container",
          style = "margin-top: 12px;",
          
          # Header
          tags$div(
            class = "device-info-header",
            tags$div(
              class = "device-info-title",
              icon("cogs"),
              " ", device_label
            ),
            tags$div(
              class = "device-info-badge",
              rv$current_device
            )
          ),
          
          # Device image preview
          image_preview,
          
          # Info text section (kept compact)
          tags$div(
            class = "info-section",
            textAreaInput(
              "device_info_text",
              label = tagList(icon("info-circle"), " Informationen zum Gerät (sichtbar für alle)"),
              value = dl2$footer_text %||% "",
              rows = 2,
              placeholder = "Zusätzliche Informationen, Hinweise, Besonderheiten..."
            )
          ),
          
          # Action buttons
          tags$div(
            class = "action-buttons",
            actionButton(
              "save_device_info",
              tagList(icon("save"), " Speichern"),
              class = "btn btn-primary btn-sm"
            )
          ),
          
          # Timestamp
          timestamp,
          
          # Serial numbers - collapsed at the bottom (admin focus)
          if (!is.null(serial_section)) {
            tags$details(
              style = "margin-top: 18px; border-top: 1px solid #eef1f4; padding-top: 12px;",
              tags$summary(
                style = "cursor:pointer; font-size:12px; font-weight:600;
                         color:#6c757d; text-transform:uppercase; letter-spacing:1px;
                         padding:6px 0; display:flex; align-items:center; gap:8px;",
                icon("barcode"),
                "Seriennummern & Verlauf",
                if (!identical(rv$role, "admin")) {
                  tags$span(style = "background:#fff3cd; color:#856404;
                                    border:1px solid #ffeeba; border-radius:999px;
                                    padding:2px 8px; font-size:10px; font-weight:700;
                                    text-transform:uppercase; letter-spacing:.5px;",
                            icon("lock"), " Nur Admin")
                }
              ),
              tags$div(style = "margin-top: 12px;",
                       serial_section,
                       tags$div(style = "margin-top: 10px;",
                                actionButton(
                                  "view_serial_history",
                                  tagList(icon("history"), " Verlauf anzeigen"),
                                  class = "btn btn-default btn-sm"
                                )
                       )
              )
            )
          }
        )
      )
    })
    # render footer (visual) for this device (below the info box)
    output$device_footer_ui <- renderUI({
      if (!is.null(dl$footer_path) && nzchar(dl$footer_path)) {
        tagList(tags$hr(), tags$div(class = "app-footer", tags$img(src = dl$footer_path, alt = "Geräte-Fuß")))
      } else if (!is.null(dl$footer_text) && nzchar(dl$footer_text)) {
        tagList(tags$hr(), tags$div(class = "app-footer", dl$footer_text))
      } else NULL
    })
  }
  
  # Save device info (editable by any authenticated user)
  observeEvent(input$save_device_info, {
    req(rv$authed, rv$current_device)
    con <- pg_con(); on.exit(dbDisconnect(con), add = TRUE)
    who <- rv$user %||% "unknown"
    
    # Ensure txt is a single string
    txt <- if (!is.null(input$device_info_text) && length(input$device_info_text) == 1) {
      as.character(input$device_info_text)
    } else {
      ""
    }
    
    # Save footer_text for the device (this is the editable info box)
    save_device_layout(con, rv$current_device, 
                       title = NULL, 
                       footer_text = txt, 
                       footer_path = NULL, 
                       footer_mime = NULL, 
                       who = who,
                       version = NULL, 
                       valid_from = NULL)
    
    # Load serial number definitions for this device
    serial_defs <- load_device_serials(con, rv$current_device)
    
    # Save serial numbers if this device has any AND the current user is an admin.
    # Non-admins can update the info text, but Seriennummern bleiben unverändert.
    is_admin <- identical(rv$role, "admin")
    if (length(serial_defs) > 0 && !is_admin) {
      showNotification(
        "Hinweis: Seriennummern können nur von einem Administrator geändert werden. Geräte-Informationstext wurde gespeichert.",
        type = "warning", duration = 8
      )
    }
    if (length(serial_defs) > 0 && is_admin) {
      # Load old serials for comparison
      old_serials_data <- load_device_serials(con, rv$current_device)
      
      # Collect new serial numbers from inputs
      new_serials <- list()
      validation_errors <- c()
      
      for (device_name in names(serial_defs)) {
        input_id <- paste0("sn_", gsub("[^a-z0-9]", "_", tolower(device_name)))
        new_sn <- as.character(input[[input_id]] %||% "")[1]
        
        # Safely extract definition (may be a list OR a plain string for legacy entries)
        def <- serial_defs[[device_name]]
        if (!is.list(def)) def <- list(label = device_name, sn = as.character(def), pattern = NULL)
        
        # Validate format
        pattern <- def$pattern %||% "^[0-9A-Z]{0,15}$"
        validation <- validate_serial_number(new_sn, pattern, device_name)
        
        if (!validation$valid) {
          validation_errors <- c(validation_errors, validation$message)
        } else {
          new_serials[[device_name]] <- list(
            label = def$label %||% device_name,
            sn = new_sn,
            pattern = pattern
          )
          
          # Log change (also safe-extract old serial)
          old_def <- old_serials_data[[device_name]]
          old_sn <- if (is.list(old_def)) (old_def$sn %||% "") else as.character(old_def %||% "")
          log_serial_change(con, rv$current_device, device_name, old_sn, new_sn, who)
        }
      }
      
      # If there are validation errors, show them and stop
      if (length(validation_errors) > 0) {
        showNotification(
          paste(validation_errors, collapse = "\n"),
          type = "error",
          duration = 10
        )
        return()
      }
      
      # Save the new serials
      save_device_serials(con, rv$current_device, new_serials, who)
      
      # Update table headers with new serial numbers
      if (!is.null(rv$data)) {
        for (i in seq_len(nrow(rv$data))) {
          header <- rv$data$Header[i]
          
          # Check each device name to see if this header matches
          for (device_name in names(new_serials)) {
            # Extract the base name (without serial number)
            base_pattern <- paste0("^", gsub("\\s+", "\\\\s+", device_name))
            
            if (grepl(base_pattern, header)) {
              new_sn <- new_serials[[device_name]]$sn
              if (nzchar(new_sn)) {
                rv$data$Header[i] <- sprintf("%s (SN %s)", device_name, new_sn)
              } else {
                rv$data$Header[i] <- device_name
              }
              break
            }
          }
        }
        
        # Save updated table
        save_device_table(con, rv$current_device, rv$data, who)
        
        # Reload data to ensure consistency
        rv$data <- load_device_table(con, rv$current_device)
        rv$data <- order_cols(rv$data)
      }
    }
    
    # Refresh table display
    output$tableRH <- renderRHandsontable({
      cells_readonly_for_headers(rv$data, rv$user_initials, rv$table_status, rv$role, rv$invalid_days)
    })
    
    showNotification("Geräteinformationen und Seriennummern gespeichert.", type = "message")
  })
  
  
  # View serial number history
  observeEvent(input$view_serial_history, {
    req(rv$authed, rv$current_device)
    con <- pg_con(); on.exit(dbDisconnect(con), add = TRUE)
    
    history <- DBI::dbGetQuery(con, "
    SELECT device_name, old_serial, new_serial, changed_by, changed_at
    FROM serial_number_history
    WHERE device_id = $1
    ORDER BY changed_at DESC
    LIMIT 50
  ", params = list(rv$current_device))
    
    if (nrow(history) == 0) {
      showModal(modalDialog(
        title = "Seriennummern-Verlauf",
        "Keine Änderungen gefunden.",
        easyClose = TRUE,
        footer = modalButton("Schließen")
      ))
    } else {
      # Format the history as a table
      history$changed_at <- format(as.POSIXct(history$changed_at, tz = "UTC"), "%d.%m.%Y %H:%M")
      history$old_serial <- ifelse(is.na(history$old_serial) | history$old_serial == "", "(leer)", history$old_serial)
      
      history_table <- tags$table(
        class = "table table-striped",
        tags$thead(
          tags$tr(
            tags$th("Gerät"),
            tags$th("Alte SN"),
            tags$th("Neue SN"),
            tags$th("Geändert von"),
            tags$th("Datum")
          )
        ),
        tags$tbody(
          lapply(seq_len(nrow(history)), function(i) {
            tags$tr(
              tags$td(history$device_name[i]),
              tags$td(history$old_serial[i]),
              tags$td(history$new_serial[i]),
              tags$td(history$changed_by[i]),
              tags$td(history$changed_at[i])
            )
          })
        )
      )
      
      showModal(modalDialog(
        title = paste("Seriennummern-Verlauf für", rv$current_device),
        history_table,
        size = "l",
        easyClose = TRUE,
        footer = modalButton("Schließen")
      ))
    }
  })
  
  observeEvent(input$save_draft, {
    req(rv$data); saveRDS(rv$data, file = "draft_table.rds")
    showNotification("Draft gespeichert (lokal).", type = "message")
  })
  observeEvent(input$load_draft, {
    if (file.exists("draft_table.rds")) {
      rv$data <- readRDS("draft_table.rds")
      if (!"Task" %in% names(rv$data)) rv$data$Task <- ""
      showNotification("Draft geladen (lokal).", type = "message")
      output$tableRH <- renderRHandsontable({
        cells_readonly_for_headers(rv$data, rv$user_initials, rv$table_status, rv$role, rv$invalid_days)
      })
    } else showNotification("Keine lokale Draft-Datei gefunden.", type = "error")
  })
  
  observeEvent(input$save_db, {
    rv$data <- order_cols(rv$data)
    req(rv$data, rv$current_device, rv$user)
    con <- pg_con(); on.exit(dbDisconnect(con), add = TRUE)
    save_device_table(con, rv$current_device, rv$data, rv$user)
    try(save_status(con, rv$current_device, rv$data, rv$user), silent = TRUE)
    showNotification("In DB gespeichert.", type = "message")
  })
  
  ensure_task_observers <- function(rows) {
    for (r in rows) {
      rid <- as.character(r)
      if (rid %in% rv$task_obs_ids) next
      local({
        rr <- r
        cid <- paste0("task_done_", rr)
        oid <- paste0("task_opt_", rr)
        rmid <- paste0("task_rem_", rr)
        nid  <- paste0("task_nicht_", rr)
        dsid <- paste0("task_lastrepl_save_", rr)
        did  <- paste0("task_lastrepl_", rr)
        
        # when checkbox toggled
        #
        # GUARD against the "wrong initials" bug: these per-row observers are
        # created once, but the checkbox UI is RECREATED on every re-render
        # with value=is_done. When another user has already checked a task,
        # the freshly-rendered pre-checked checkbox fires this observer as if
        # the current viewer had just clicked it -- which previously re-saved
        # the cell with the VIEWER's initials, overwriting the real doer's.
        # ignoreInit only suppresses the first eval at observer-creation, not
        # these re-render events. So before writing, we compare the incoming
        # checkbox value against what's actually stored for this cell today;
        # if it already matches (someone checked it, we're just re-rendering),
        # we do nothing. A genuine human toggle always differs from the
        # stored state, so real clicks still go through.
        observeEvent(input[[cid]], {
          req(rv$current_device, rv$data)
          val <- isTRUE(input[[cid]])
          today <- as.integer(format(Sys.Date(), "%d"))
          who <- rv$user_initials %||% rv$user
          is_admin <- identical(rv$role, "admin")
          
          # What's currently stored for this cell today?
          cur_status  <- isolate(rv$table_status) %||% data.frame()
          stored_val  <- ""
          if (nrow(cur_status)) {
            hit <- cur_status[cur_status$row_index == rr & cur_status$day == today, , drop = FALSE]
            if (nrow(hit)) stored_val <- as.character(hit$value_text[1])
          }
          stored_is_check <- startsWith(trimws(stored_val), "\u2713")
          
          # No-op if the checkbox event merely reflects the already-stored
          # state (i.e. this is a re-render echo, not a real click).
          if (identical(val, stored_is_check)) return()
          # Open the DB connection only now: every list rebuild re-sends all
          # pre-ticked checkboxes, and each of those echoes used to open a
          # connection just to find out that nothing had changed.
          con <- pg_con(); on.exit(dbDisconnect(con), add = TRUE)
          
          if (val) {
            # If checkbox is checked, put checkmark with username and clear dropdown
            updateSelectInput(session, oid, selected = "")
            cell_val <- paste0("\u2713 (", who, ")")
            upsert_cell(con, rv$current_device, rr, today, cell_val, who)
            # Save a Bemerkung typed just before ticking right away -- the
            # tick rebuilds the list, and text still waiting for the 900 ms
            # auto-save was lost. Lets users note e.g. "positiv"/"negativ"
            # for an erledigte task (Rueckmeldung Drogentest).
            cur_text <- trimws(isolate(input[[rmid]]) %||% "")
            if (nzchar(cur_text))
              tryCatch(upsert_remark(con, rv$current_device, rr, Sys.Date(),
                                     cur_text, "", who),
                       error = function(e) NULL)
            
          } else {
            # delete only if present
            delete_cell_if_owner(con, rv$current_device, rr, today, who, is_admin)
          }
          # Simple update without complex reactive dependencies
          rv$table_status <- load_cell_status_current(con, rv$current_device)
          rv$tasks_refresh <- isolate(rv$tasks_refresh) + 1L
        }, ignoreInit = TRUE)
        
        # when select input changed -> uncheck checkbox and put abbreviation only
        observeEvent(input[[oid]], {
          req(rv$current_device, rv$data)
          sel <- input[[oid]]
          if (is.null(sel) || sel == "") return()  # Ignore empty selection
          
          # Extract abbreviation from full text (part before first space/paren).
          abbrev <- sub("^([A-Za-z.øØ]+).*", "\\1", sel)
          
          # GUARD (same re-render echo problem as the checkbox above): the
          # dropdown is recreated with selected=selected_opt on every render,
          # which re-fires this observer and would re-stamp the option with
          # the current viewer's initials. Skip if the incoming selection
          # already matches what's stored for this cell today.
          cur_status <- isolate(rv$table_status) %||% data.frame()
          stored_val <- ""
          if (nrow(cur_status)) {
            hit <- cur_status[cur_status$row_index == rr &
                                cur_status$day == as.integer(format(Sys.Date(), "%d")), , drop = FALSE]
            if (nrow(hit)) stored_val <- trimws(as.character(hit$value_text[1]))
          }
          if (identical(stored_val, abbrev)) return()
          
          # Uncheck the checkbox when dropdown is selected
          updateCheckboxInput(session, cid, value = FALSE)
          
          con <- pg_con(); on.exit(dbDisconnect(con), add = TRUE)
          today <- as.integer(format(Sys.Date(), "%d"))
          who <- rv$user_initials %||% rv$user
          
          upsert_cell(con, rv$current_device, rr, today, abbrev, who)
          
          # Persist the option code alongside any current remark text so
          # tomorrow's banner can show "[abbrev] reason".
          cur_text <- isolate(input[[rmid]]) %||% ""
          tryCatch(
            upsert_remark(con, rv$current_device, rr, Sys.Date(),
                          cur_text, abbrev, who),
            error = function(e) NULL
          )
          # NE picked from the dropdown = the task is openly unfinished ->
          # (re-)open it so it keeps appearing at every login. All other
          # codes are documentation only and were already closed by
          # upsert_cell().
          if (identical(abbrev, "NE"))
            tryCatch(reopen_ne_remark(con, rv$current_device, rr, Sys.Date()),
                     error = function(e) NULL)
          
          # Simple update without complex reactive dependencies
          rv$table_status <- load_cell_status_current(con, rv$current_device)
          rv$tasks_refresh <- isolate(rv$tasks_refresh) + 1L
        }, ignoreInit = TRUE)
        
        # Remark field auto-saves ~900ms after the user stops typing, same
        # as the Erledigt checkbox -- no separate "Speichern" click needed.
        # This part stays silent (no toast): typing naturally has pauses
        # longer than 900ms, and a toast on every pause would fire many
        # times while composing one remark.
        rem_debounced <- debounce(reactive(input[[rmid]]), 900)
        rem_save <- function(txt) {
          req(rv$current_device, rv$data)
          who <- rv$user_initials %||% rv$user
          sel <- isolate(input[[oid]]) %||% ""
          opt <- if (nzchar(sel)) sub("^([A-Za-z.øØ]+).*", "\\1", sel) else ""
          tryCatch({
            con <- pg_con(); on.exit(dbDisconnect(con), add = TRUE)
            upsert_remark(con, rv$current_device, rr, Sys.Date(), txt, opt, who)
            rv$remarks_rev <- isolate(rv$remarks_rev) + 1L
            TRUE
          }, error = function(e) FALSE)
        }
        observeEvent(rem_debounced(), {
          rem_save(rem_debounced() %||% "")
        }, ignoreInit = TRUE)
        
        # The confirmation toast fires once, when the user actually leaves
        # the field (blur) -- not on every debounced save while typing.
        observeEvent(input[[paste0(rmid, "_blur")]], {
          ok <- rem_save(isolate(input[[rmid]]) %||% "")
          if (isTRUE(ok)) {
            showNotification(
              "Ihre Eingabe wurde automatisch gespeichert.",
              id = paste0("save_notif_", rr),
              type = "message", duration = 60
            )
          }
        }, ignoreInit = TRUE)
        
        # "Nicht erledigt" button: marks the cell as NE, unchecks Erledigt,
        # and focuses the Bemerkung textbox so the user can type the reason.
        # "Nicht erledigt" only opens the reason dropdown (done in the
        # browser, see wpSetTaskState). Nothing is saved yet -- the entry is
        # written as soon as a reason is chosen (observer on the dropdown).
        # Before, the button saved "NE" immediately and pre-selected it, so
        # the prompt "Bitte auswaehlen, warum ..." was never visible.
        # Here: untick "Erledigt" (the observer above then removes the tick)
        # and put the focus on the reason dropdown.
        observeEvent(input[[nid]], {
          req(rv$current_device, rv$data)
          if (isTRUE(isolate(input[[cid]])))
            updateCheckboxInput(session, cid, value = FALSE)
          session$sendCustomMessage("focusTaskRemark", list(id = oid))
        }, ignoreInit = TRUE)
        
        # "Zuletzt getauscht am": save the chosen date for this task row.
        # COBAS Pro I/II electrode rows must always be a Wednesday (shifted
        # past public holidays); snap and inform the user when needed.
        observeEvent(input[[dsid]], {
          req(rv$current_device)
          who <- rv$user_initials %||% rv$user
          d   <- input[[did]]
          
          snapped <- FALSE
          if (rv$current_device %in% c("g4", "g5") && !is.null(d) && !is.na(d)) {
            d_in <- suppressWarnings(as.Date(d))
            d_ok <- snap_to_working_wednesday(d_in)
            if (!is.na(d_ok) && !identical(d_in, d_ok)) {
              d <- d_ok
              snapped <- TRUE
              # reflect the snapped value in the date picker
              updateDateInput(session, did, value = d_ok)
            }
          }
          
          ok <- tryCatch({
            con <- pg_con(); on.exit(dbDisconnect(con), add = TRUE)
            save_task_meta_date(con, rv$current_device, rr, d, who)
            TRUE
          }, error = function(e) FALSE)
          if (isTRUE(ok)) {
            showNotification(
              if (is.null(d) || is.na(d))
                "Datum gel\u00f6scht."
              else if (snapped)
                sprintf(
                  "Hinweis: Datum auf den n\u00e4chsten Mittwoch (Werktag) verschoben \u2192 %s",
                  format(as.Date(d), "%d.%m.%Y"))
              else sprintf("Datum gespeichert: %s",
                           format(as.Date(d), "%d.%m.%Y")),
              type = if (snapped) "warning" else "message",
              duration = if (snapped) 8 else 4
            )
            rv$tasks_refresh <- isolate(rv$tasks_refresh) + 1L
          } else {
            showNotification("Datum konnte nicht gespeichert werden.",
                             type = "error", duration = 6)
          }
        }, ignoreInit = TRUE)
        
      })
      rv$task_obs_ids <- unique(c(rv$task_obs_ids, rid))
    }
  }
  
  # Companion to ensure_task_observers() for the "Vortagsaufgaben" banner.
  # The inputs use a "prev_" prefix so they are independent from today's
  # controls, and every write is dated to whatever previous_workday() resolves
  # to at click time (so we still record the correct day if the user keeps
  # the session open across midnight). Behavioural parity with today: the
  # checkbox writes "✓ (user)", the dropdown writes the abbreviation, the
  # "Nicht erledigt" button writes "NE" and focuses the remark textbox, and
  # the Save button persists the typed Bemerkung.
  # Find the schedule header governing row `rr` by scanning rv$data top to
  # bottom and remembering the last SCHEDULE_HEADER_NAMES header seen.
  header_for_row <- function(rr) {
    if (is.null(rv$data) || nrow(rv$data) < rr) return("")
    hdrs <- as.character(rv$data$Header[seq_len(rr)])
    hdrs <- hdrs[nzchar(hdrs) & hdrs %in% SCHEDULE_HEADER_NAMES]
    if (!length(hdrs)) "" else tail(hdrs, 1)
  }
  
  # Input-id suffix for one overdue (row, due date) item in the banner, e.g.
  # "12_20260929". The same task can be overdue on several days, so the row
  # number alone is not unique any more.
  prev_item_key <- function(row, due) paste0(row, "_", format(as.Date(due), "%Y%m%d"))
  
  ensure_prev_task_observers <- function(items) {
    for (it in items) {
      rid <- prev_item_key(it$row, it$due)
      if (rid %in% rv$prev_task_obs_ids) next
      local({
        rr  <- it$row
        due <- as.Date(it$due)
        key <- prev_item_key(rr, due)
        cid  <- paste0("prev_task_done_",     key)
        oid  <- paste0("prev_task_opt_",      key)
        rmid <- paste0("prev_task_rem_",      key)
        nid  <- paste0("prev_task_nicht_",    key)
        
        # The date this item was due on is fixed per banner entry, so saving
        # always writes to exactly the day shown next to the task.
        date_parts <- function() {
          list(d = due,
               day = as.integer(format(due, "%d")),
               month = as.integer(format(due, "%m")),
               year = as.integer(format(due, "%Y")))
        }
        
        observeEvent(input[[cid]], {
          req(rv$current_device, rv$data)
          val <- isTRUE(input[[cid]])
          who <- rv$user_initials %||% rv$user
          is_admin <- identical(rv$role, "admin")
          dp <- date_parts()
          con <- pg_con(); on.exit(dbDisconnect(con), add = TRUE)
          if (val) {
            updateSelectInput(session, oid, selected = "")
            cell_val <- paste0("\u2713 (", who, ")")
            upsert_cell(con, rv$current_device, rr, dp$day, cell_val, who,
                        month = dp$month, year = dp$year)
            # Save a Bemerkung typed just before ticking (see daily list).
            cur_text <- trimws(isolate(input[[rmid]]) %||% "")
            if (nzchar(cur_text))
              tryCatch(upsert_remark(con, rv$current_device, rr, dp$d,
                                     cur_text, "", who),
                       error = function(e) NULL)
          } else {
            delete_cell_if_owner(con, rv$current_device, rr, dp$day, who,
                                 is_admin, month = dp$month, year = dp$year)
          }
          rv$table_status <- load_cell_status_current(con, rv$current_device)
          rv$tasks_refresh <- isolate(rv$tasks_refresh) + 1L
        }, ignoreInit = TRUE)
        
        observeEvent(input[[oid]], {
          req(rv$current_device, rv$data)
          sel <- input[[oid]]
          if (is.null(sel) || sel == "") return()
          updateCheckboxInput(session, cid, value = FALSE)
          who <- rv$user_initials %||% rv$user
          dp <- date_parts()
          abbrev <- sub("^([A-Za-z.øØ]+).*", "\\1", sel)
          con <- pg_con(); on.exit(dbDisconnect(con), add = TRUE)
          upsert_cell(con, rv$current_device, rr, dp$day, abbrev, who,
                      month = dp$month, year = dp$year)
          cur_text <- isolate(input[[rmid]]) %||% ""
          tryCatch(
            upsert_remark(con, rv$current_device, rr, dp$d,
                          cur_text, abbrev, who),
            error = function(e) NULL
          )
          if (identical(abbrev, "NE"))
            tryCatch(reopen_ne_remark(con, rv$current_device, rr, dp$d),
                     error = function(e) NULL)
          rv$table_status <- load_cell_status_current(con, rv$current_device)
          rv$tasks_refresh <- isolate(rv$tasks_refresh) + 1L
        }, ignoreInit = TRUE)
        
        # Remark field auto-saves ~900ms after the user stops typing, same
        # as the Erledigt checkbox -- no separate "Speichern" click needed.
        # This part stays silent (no toast): typing naturally has pauses
        # longer than 900ms, and a toast on every pause would fire many
        # times while composing one remark.
        rem_debounced_b <- debounce(reactive(input[[rmid]]), 900)
        rem_save_b <- function(txt) {
          req(rv$current_device, rv$data)
          who <- rv$user_initials %||% rv$user
          sel <- isolate(input[[oid]]) %||% ""
          opt <- if (nzchar(sel)) sub("^([A-Za-z.øØ]+).*", "\\1", sel) else ""
          dp <- date_parts()
          tryCatch({
            con <- pg_con(); on.exit(dbDisconnect(con), add = TRUE)
            upsert_remark(con, rv$current_device, rr, dp$d, txt, opt, who)
            rv$remarks_rev <- isolate(rv$remarks_rev) + 1L
            TRUE
          }, error = function(e) FALSE)
        }
        observeEvent(rem_debounced_b(), {
          rem_save_b(rem_debounced_b() %||% "")
        }, ignoreInit = TRUE)
        
        # The confirmation toast fires once, when the user actually leaves
        # the field (blur) -- not on every debounced save while typing.
        observeEvent(input[[paste0(rmid, "_blur")]], {
          ok <- rem_save_b(isolate(input[[rmid]]) %||% "")
          if (isTRUE(ok)) {
            showNotification(
              "Ihre Eingabe wurde automatisch gespeichert.",
              id = paste0("save_notif_prev_", key),
              type = "message", duration = 60
            )
          }
        }, ignoreInit = TRUE)
        
        # "Nicht erledigt" only opens the reason dropdown (done in the
        # browser, see wpSetTaskState). Nothing is saved yet -- the entry is
        # written as soon as a reason is chosen (observer on the dropdown).
        # Before, the button saved "NE" immediately and pre-selected it, so
        # the prompt "Bitte auswaehlen, warum ..." was never visible.
        # Here: untick "Erledigt" (the observer above then removes the tick)
        # and put the focus on the reason dropdown.
        observeEvent(input[[nid]], {
          req(rv$current_device, rv$data)
          if (isTRUE(isolate(input[[cid]])))
            updateCheckboxInput(session, cid, value = FALSE)
          session$sendCustomMessage("focusTaskRemark", list(id = oid))
        }, ignoreInit = TRUE)
      })
      rv$prev_task_obs_ids <- unique(c(rv$prev_task_obs_ids, rid))
    }
  }
  
  # ---- Monatsübersicht: top-right "open remarks" dropdown ----------------
  # Shows the still-open "NE – Nicht erledigt" entries (documentation codes
  # such as WE / FT / D are not listed here), grouped into three buckets:
  #   * Tägliche Wartung
  #   * Wöchentliche / Monatliche Wartung
  #   * Bedarfs-Wartung (Bei Bedarf / Start-Up)
  # The schedule for each remark is derived from the most recent non-empty
  # Header row above the remark's row_index in rv$data.
  output$monthly_remarks_dropdown <- renderUI({
    req(rv$current_device, rv$data)
    rv$tasks_refresh
    
    # Classify a schedule header into one of three buckets.
    classify_schedule <- function(h) {
      if (is.null(h) || !nzchar(h)) return("other")
      if (h %in% c("Täglich", "Täglich (ZL)", "Täglich (Ablesen zwischen 12:00 und 14:00 Uhr)")) return("daily")
      if (h %in% c("Bei Bedarf", "Wartung bei Bedarf", "Start-Up")) return("ondemand")
      if (h %in% c("Wöchentlich", "Wöchentlich (Montag)", "Wöchentlich (Mittwoch)",
                   "Wöchentlich (Mittwoch, TD)", "Wöchentlich (Donnerstag)",
                   "Wöchentlich (Freitag)", "Wöchentlich (Freitag, ZL)",
                   "Monatlich", "Monatlich (Freitag)", "Monatlich oder alle 2500 Proben",
                   "14-tägig", "14-tägig (Mittwoch)",
                   "Quartalsweise", "Alle 3 Monate oder alle 7500 Proben",
                   "Montag und Donnerstag", "Am ersten Dienstag im Monat",
                   "Am ersten Freitag im Monat",
                   "Monatlich (Freitag, alle 4 Wochen)", "Monatlich (Dienstag, alle 4 Wochen)",
                   "Monatlich (Mittwoch, alle 4 Wochen)",
                   "Montag", "Dienstag", "Mittwoch", "Donnerstag",
                   "Freitag", "Samstag", "Sonntag")) return("weekmonth")
      "other"
    }
    
    # Build row_index -> schedule header by walking forward through rv$data.
    headers_vec <- as.character(rv$data$Header)
    row_sched <- character(length(headers_vec))
    cur <- ""
    for (i in seq_along(headers_vec)) {
      if (nzchar(headers_vec[i])) {
        if (headers_vec[i] %in% KNOWN_SCHEDULES) cur <- headers_vec[i]
      }
      row_sched[i] <- cur
    }
    task_lookup <- if ("Task" %in% names(rv$data))
      as.character(rv$data$Task) else character(length(headers_vec))
    
    rr <- tryCatch({
      con_r <- pg_con(); on.exit(dbDisconnect(con_r), add = TRUE)
      load_recent_remarks(con_r, rv$current_device,
                          today = Sys.Date(), days_back = 14L,
                          include_today = TRUE)
    }, error = function(e) data.frame())
    if (is.null(rr)) rr <- data.frame()
    
    buckets <- list(daily = list(), weekmonth = list(), ondemand = list())
    if (nrow(rr)) {
      for (i in seq_len(nrow(rr))) {
        ri  <- rr$row_index[i]
        if (ri < 1 || ri > length(row_sched)) next
        cat <- classify_schedule(row_sched[ri])
        if (!cat %in% names(buckets)) next
        d   <- as.Date(rr$remark_date[i])
        opt <- rr$option_code[i] %||% ""
        txt <- rr$remark_text[i] %||% ""
        usr <- rr$updated_by[i]  %||% ""
        tname <- if (nzchar(task_lookup[ri])) task_lookup[ri] else paste0("Aufgabe ", ri)
        key <- sprintf("%d|%s", as.integer(ri), format(d, "%Y-%m-%d"))
        text_parts <- c(
          format(d, "%d.%m."),
          paste0("\u00BB", tname, "\u00AB"),
          if (nzchar(opt)) paste0("[", opt, "]") else NULL,
          if (nzchar(txt)) txt else NULL,
          if (nzchar(usr)) paste0("(", usr, ")") else NULL
        )
        item <- tags$li(
          style = "display:flex; align-items:flex-start; gap:6px;
                   margin-bottom:4px; line-height:1.35;",
          tags$span(style = "flex:1 1 auto; min-width:0;",
                    paste(text_parts, collapse = " ")),
          tags$button(
            type = "button",
            class = "btn btn-xs btn-success open-remarks-resolve",
            title = "Bemerkung als erledigt markieren",
            style = "padding:1px 6px; font-size:10px; line-height:1.3; flex-shrink:0;",
            onclick = sprintf(
              "event.stopPropagation(); Shiny.setInputValue('resolve_remark', '%s', {priority:'event'});",
              key),
            icon("check"), " Erledigt"
          ),
          tags$button(
            type = "button",
            class = "btn btn-xs btn-default open-remarks-jump",
            title = "Zur Aufgabe springen",
            style = "padding:1px 6px; font-size:10px; line-height:1.3; flex-shrink:0;",
            onclick = sprintf(
              "event.stopPropagation(); Shiny.setInputValue('jump_to_remark', '%s', {priority:'event'});",
              key),
            icon("arrow-right")
          )
        )
        buckets[[cat]] <- append(buckets[[cat]], list(item))
      }
    }
    
    n_total <- length(buckets$daily) + length(buckets$weekmonth) + length(buckets$ondemand)
    
    make_section <- function(title, items) {
      n <- length(items)
      body <- if (n) do.call(tags$ul, items)
      else tags$div(class = "open-remarks-empty",
                    "Keine offenen Bemerkungen.")
      tags$div(
        class = if (n) "open-remarks-section" else "open-remarks-section collapsed",
        tags$div(class = "sec-head",
                 onclick = "this.parentNode.classList.toggle('collapsed');",
                 tags$span(title),
                 tags$span(class = if (n) "sec-count" else "sec-count zero", n)),
        tags$div(class = "sec-body", body)
      )
    }
    
    tags$div(
      class = "open-remarks-wrap",
      tags$button(
        type = "button",
        class = "open-remarks-btn",
        onclick = "this.parentNode.classList.toggle('open');",
        icon("triangle-exclamation"),
        tags$span("Offene Bemerkungen"),
        tags$span(class = "badge-count", n_total)
      ),
      tags$div(
        class = "open-remarks-panel",
        make_section("Tägliche Wartung", buckets$daily),
        make_section("Wöchentliche / Monatliche Wartung", buckets$weekmonth),
        make_section("Bedarfs-Wartung", buckets$ondemand)
      )
    )
  })
  
  # ── Ueberfaellige Aufgaben (eigener Tab) ─────────────────────────────────
  # Computed once per refresh and shared by the tab title
  # ("Ueberfaellige Aufgaben (offen: N)"), the tab content and the short
  # notice in the daily list. Returns list(items, remarks).
  overdue_info <- reactive({
    req(rv$current_device, rv$data)
    rv$tasks_refresh
    nrows <- nrow(rv$data)
    if (is.null(nrows) || nrows == 0) return(list(items = list(), remarks = list()))
    # ── Überfällige Aufgaben: any earlier missed due date, not just yesterday ──
    # For every schedule that is NOT due today, look up its most recent due
    # date before today (whatever that was -- yesterday for "Täglich", last
    # Monday for "Wöchentlich", last month for "Monatlich", ...) and surface
    # any task on it that still has no recorded value_text, so a forgotten
    # entry stays visible until it's filled in -- no matter how long ago it
    # became due. "Bei Bedarf" / "Wartung bei Bedarf" have no due date and
    # are never flagged. Headers due today are handled live in the main list
    # below and are skipped here to avoid showing them twice.
    headers_all  <- as.character(rv$data$Header)
    header_of_row <- character(nrows)
    {
      cur_hdr_scan <- ""
      for (rr_s in seq_len(nrows)) {
        h_s <- headers_all[rr_s]
        if (nzchar(h_s)) {
          if (h_s %in% SCHEDULE_HEADER_NAMES) cur_hdr_scan <- h_s
          # device sub-headers keep whatever schedule was active above them
          next
        }
        header_of_row[rr_s] <- cur_hdr_scan
      }
    }
    schedule_headers_present <- setdiff(
      unique(header_of_row[nzchar(header_of_row)]),
      SCHEDULE_HEADERS_NO_DUE_DATE
    )
    # Rueckmeldung aus dem Labor (01.10.2026): in der Haematologie fehlten
    # die taeglichen Eintraege vom 24./25./28.-30.09., tauchten aber nie
    # unter "Ueberfaellige Aufgaben" auf. Ursache: Abschnitte, die HEUTE
    # faellig sind, wurden hier komplett uebersprungen -- "Taeglich" ist
    # jeden Tag faellig, also wurden versaeumte Tagesaufgaben nie gemeldet.
    # Ausserdem wurde je Abschnitt nur EIN Termin geprueft und unterdrueckt,
    # sobald er am naechsten Tag wieder faellig war (bei Taeglich immer).
    # Jetzt wird fuer jeden Tag der letzten OVERDUE_WINDOW_DAYS Tage
    # geprueft, ob der Abschnitt faellig war und ob ein Eintrag fehlt -- jede
    # fehlende (Aufgabe, Datum)-Kombination erscheint einzeln. Aeltere
    # Luecken lassen sich ueber "Nachtrag" in der Monatsuebersicht fuellen.
    # 14 Tage = gleicher Zeitraum wie die NE-Erinnerung (load_recent_remarks).
    OVERDUE_WINDOW_DAYS <- 14L
    window_dates <- rev(Sys.Date() - seq_len(OVERDUE_WINDOW_DAYS))  # oldest first
    due_map <- list()   # header -> vector of past due dates within the window
    for (h in schedule_headers_present) {
      dd <- window_dates[vapply(window_dates, function(d)
        isTRUE(is_header_due_on(h, d)), logical(1))]
      if (length(dd)) due_map[[h]] <- dd
    }
  
    overdue_missing  <- list()
    overdue_remarks_map <- list()
    if (length(due_map)) {
      all_due <- sort(unique(do.call(c, unname(due_map))))
      my_keys <- unique(format(all_due, "%Y-%m"))
      status_by_my <- list()
      tryCatch({
        con_o <- pg_con(); on.exit(dbDisconnect(con_o), add = TRUE)
        for (k in my_keys) {
          yy <- as.integer(substr(k, 1, 4)); mm <- as.integer(substr(k, 6, 7))
          status_by_my[[k]] <- load_cell_status(con_o, rv$current_device, mm, yy)
        }
        # One query for the whole window instead of one per day.
        pr <- DBI::dbGetQuery(con_o, "
          SELECT row_index, remark_date, remark_text, option_code
            FROM device_task_remark
           WHERE device_id = $1 AND remark_date BETWEEN $2 AND $3
        ", params = list(rv$current_device, as.character(min(all_due)), as.character(max(all_due))))
        if (is.data.frame(pr) && nrow(pr))
          for (i in seq_len(nrow(pr)))
            overdue_remarks_map[[ paste0(pr$row_index[i], "|", as.Date(pr$remark_date[i])) ]] <-
          list(text = pr$remark_text[i] %||% "", opt = pr$option_code[i] %||% "")
      }, error = function(e) NULL)
    
      # Fast lookup of filled cells: "<row>|<YYYY-MM-DD>"
      filled_o <- new.env(hash = TRUE, parent = emptyenv())
      for (k in names(status_by_my)) {
        cs_o <- status_by_my[[k]]
        if (!is.data.frame(cs_o) || !nrow(cs_o)) next
        for (i in seq_len(nrow(cs_o))) {
          v <- as.character(cs_o$value_text[i])
          if (is.na(v) || !nzchar(trimws(v))) next
          dkey <- sprintf("%s-%02d", k, as.integer(cs_o$day[i]))
          assign(paste0(cs_o$row_index[i], "|", dkey), TRUE, envir = filled_o)
        }
      }
    
      # Ordered by date (oldest first), then by position in the plan.
      for (due_d in as.list(all_due)) {
        for (rr_o in seq_len(nrows)) {
          h_o <- header_of_row[rr_o]
          if (!nzchar(h_o) || is.null(due_map[[h_o]])) next
          if (!(due_d %in% due_map[[h_o]])) next
          task_o <- if ("Task" %in% names(rv$data)) as.character(rv$data$Task[rr_o]) else ""
          if (is.na(task_o) || !nzchar(task_o)) next
          if (exists(paste0(rr_o, "|", format(due_d, "%Y-%m-%d")),
                     envir = filled_o, inherits = FALSE)) next
          overdue_missing[[length(overdue_missing) + 1L]] <- list(
            row = rr_o, header = h_o, task = task_o, due = due_d
          )
        }
      }
    }
  
    list(items = overdue_missing, remarks = overdue_remarks_map)
  })
  
  output$overdue_tab_title <- renderUI({
    n <- if (is.null(rv$current_device) || is.null(rv$data)) 0L else
      tryCatch(length(overdue_info()$items), error = function(e) 0L)
    tags$span(
      "Überfällige Aufgaben ",
      tags$span(style = if (n > 0L)
                  "background:#d32f2f; color:#fff; border-radius:999px; padding:1px 8px; font-weight:700; font-size:12px;"
                else "color:#2e7d32; font-weight:600;",
                sprintf("(offen: %d)", n))
    )
  })
  
  observeEvent(input$goto_overdue_tab, {
    updateTabsetPanel(session, "checklist_tabs", selected = "overdue")
  })
  
  output$overdue_tasks <- renderUI({
    req(rv$current_device, rv$data)
    info <- overdue_info()
    overdue_missing     <- info$items
    overdue_remarks_map <- info$remarks
    if (!length(overdue_missing))
      return(tags$div(class = "alert alert-success", style = "margin-top:10px;",
                      icon("circle-check"),
                      " Keine überfälligen Aufgaben in den letzten 14 Tagen."))
    ensure_prev_task_observers(overdue_missing)
    build_prev_row <- function(item) {
      rr_b <- item$row
      task_name_b <- if (nzchar(item$task)) item$task else paste0("Aufgabe ", rr_b)
      key_b   <- prev_item_key(rr_b, item$due)
      done_id <- paste0("prev_task_done_",     key_b)
      opt_id  <- paste0("prev_task_opt_",      key_b)
      rem_id  <- paste0("prev_task_rem_",      key_b)
      nid_b   <- paste0("prev_task_nicht_",    key_b)
      rem_val_b <- overdue_remarks_map[[ paste0(rr_b, "|", item$due) ]]$text %||% ""
      opt_b     <- overdue_remarks_map[[ paste0(rr_b, "|", item$due) ]]$opt  %||% ""
      is_ne_b   <- identical(trimws(opt_b), "NE")
      tags$div(
        class = if (is_ne_b) "task-item nicht-erledigt" else "task-item",
        style = if (is_ne_b)
          "border-left-color:#d32f2f !important; background:#fdecea;"
        else "border-left-color:#f57c00 !important; background:#fff8f0;",
        tags$div(class = "task-name",
                 icon("clipboard"), " ",
                 tags$span(style = "display:inline-block; padding:1px 6px;
                                     margin-right:6px; border-radius:4px;
                                     background:#ffd699; color:#7f4f00;
                                     font-size:10px; font-weight:700;
                                     text-transform:uppercase; letter-spacing:0.5px;",
                           item$header),
                 tags$span(style = "display:inline-block; padding:1px 6px;
                                     margin-right:6px; border-radius:4px;
                                     background:#ffe0b2; color:#7f4f00;
                                     font-size:10px; font-weight:700;",
                           icon("calendar-xmark"), " ", format_de_short(item$due)),
                 task_name_b),
        tags$div(class = "task-controls state-open",
                 tags$div(class = "task-checkbox",
                          checkboxInput(done_id, label = "Erledigt", value = FALSE)),
                 tags$div(class = "task-nicht",
                          actionButton(nid_b,
                                       label = tagList(icon("xmark"), " Nicht erledigt"),
                                       class = paste("btn btn-sm btn-nicht-erledigt",
                                                     if (is_ne_b) "is-ne" else ""))),
                 tags$div(class = "task-select",
                          selectInput(opt_id, label = NULL,
                                      choices  = task_reason_choices(),
                                      selected = "",
                                      selectize = FALSE)),
                 tags$div(class = if (is_ne_b) "task-remark needs-remark" else "task-remark",
                          textInput(rem_id, label = NULL, value = rem_val_b,
                                    placeholder = task_remark_placeholder("state-open")))
        )
      )
    }
    prev_banner_block <- tags$div(
      style = "margin-bottom:16px;",
      tags$div(
        class = "date-header",
        style = "background: linear-gradient(135deg, #b26500 0%, #f57c00 100%);",
        icon("triangle-exclamation"),
        sprintf(" \u00dcberf\u00e4llige Aufgaben (offen: %d)", length(overdue_missing))
      ),
      tags$div(
        style = "padding:10px; background:#fff8f0;
                 border:1px solid #ffd699;
                 border-top:none;
                 border-radius:0 0 8px 8px;",
        tags$div(style = "font-size:12px; color:#7f4f00; margin-bottom:8px;",
                 icon("info-circle"),
                 " Diese Aufgaben waren bereits f\u00e4llig und wurden noch nicht ",
                 "eingetragen \u2013 bitte nachtragen."),
        # Grouped by due date (newest first); only the newest day is
        # expanded, older days can be opened with a click. Keeps the
        # banner short even when a whole week of daily tasks is missing.
        {
          due_keys <- vapply(overdue_missing,
                             function(x) format(as.Date(x$due), "%Y-%m-%d"),
                             character(1))
          day_keys <- sort(unique(due_keys), decreasing = TRUE)
          do.call(tagList, lapply(seq_along(day_keys), function(i_k) {
            dk    <- day_keys[i_k]
            items <- overdue_missing[due_keys == dk]
            tags$details(
              open = if (i_k == 1L) NA else NULL,
              style = "margin-bottom:6px;",
              tags$summary(
                style = "cursor:pointer; font-weight:700; color:#7f4f00;
                         padding:4px 2px;",
                icon("calendar-xmark"), " ",
                format_de_short(as.Date(dk)),
                sprintf(" – %d offen", length(items))
              ),
              do.call(tagList, lapply(items, build_prev_row))
            )
          }))
        }
      )
    )
    prev_banner_block
  })
  
  output$today_tasks <- renderUI({
    req(rv$current_device, rv$data)
    rv$tasks_refresh
    today <- as.integer(format(Sys.Date(), "%d"))
    today_day_name <- format(Sys.Date(), "%A")
    today_day_german <- switch(today_day_name,
                               "Monday" = "Montag", "Tuesday" = "Dienstag", "Wednesday" = "Mittwoch",
                               "Thursday" = "Donnerstag", "Friday" = "Freitag",
                               "Saturday" = "Samstag", "Sunday" = "Sonntag", today_day_name
    )
    
    current_date     <- Sys.Date()
    is_first_tuesday <- is_due_28day_cycle(current_date, TUESDAY_CYCLE_ANCHOR)
    is_first_friday  <- is_due_28day_cycle(current_date, FRIDAY_CYCLE_ANCHOR)
    
    nrows <- nrow(rv$data)
    if (is.null(nrows) || nrows == 0)
      return(div(class="alert alert-info", icon("info-circle"), " Keine Aufgaben definiert."))
    
    cs <- isolate(rv$table_status) %||% data.frame()
    existing_for_today <- list()
    if (nrow(cs)) {
      ssub <- cs[cs$day == today, , drop = FALSE]
      for (i in seq_len(nrow(ssub)))
        existing_for_today[[ as.character(ssub$row_index[i]) ]] <- ssub$value_text[i]
    }
    
    today_remarks     <- list()
    recent_remarks_df <- data.frame()
    task_meta_dates   <- list()
    last_remarks      <- list()   # row -> most recent earlier Bemerkung
    tryCatch({
      con_r <- pg_con(); on.exit(dbDisconnect(con_r), add = TRUE)
      # Rueckmeldung aus dem Labor: "Kann man einen Kommentar schreiben, wenn
      # die Aufgabe erledigt ist? Beim woechentlichen Drogentest wechseln wir
      # zwischen positiv und negativ." -- Die Bemerkung ist auch bei
      # erledigten Aufgaben speicherbar; damit man sie beim naechsten Mal
      # sieht, wird unter jeder Aufgabe die letzte fruehere Bemerkung
      # angezeigt (gilt fuer alle Geraete). One query for the whole device.
      lr <- DBI::dbGetQuery(con_r, "
        SELECT DISTINCT ON (row_index)
               row_index, remark_date, remark_text, option_code, updated_by
          FROM device_task_remark
         WHERE device_id = $1 AND remark_date < $2
           AND COALESCE(TRIM(remark_text), '') <> ''
           AND remark_date >= $3
         ORDER BY row_index, remark_date DESC
      ", params = list(rv$current_device, as.character(Sys.Date()),
                       as.character(Sys.Date() - 120L)))
      if (nrow(lr))
        for (i in seq_len(nrow(lr)))
          last_remarks[[ as.character(lr$row_index[i]) ]] <- list(
            date = as.Date(lr$remark_date[i]), text = lr$remark_text[i] %||% "",
            opt = lr$option_code[i] %||% "", who = lr$updated_by[i] %||% "")
      tr <- load_remarks_for_date(con_r, rv$current_device, Sys.Date())
      if (nrow(tr))
        for (i in seq_len(nrow(tr)))
          today_remarks[[ as.character(tr$row_index[i]) ]] <-
        list(text = tr$remark_text[i] %||% "", opt = tr$option_code[i] %||% "")
      recent_remarks_df <- load_recent_remarks(con_r, rv$current_device,
                                               today = Sys.Date(), days_back = 14L)
      task_meta_dates <- load_task_meta_dates(con_r, rv$current_device)
    }, error = function(e) NULL)
    
    # ── Start ui_list with date header (compact, to leave more room for
    # the task list below) ───────────────────────────────────────────────
    ui_list <- list(
      tags$div(
        class = "date-header",
        style = "background: linear-gradient(135deg, #003B73 0%, #136377 100%);
                 padding:6px 12px; margin-bottom:10px; font-size:13px;",
        icon("calendar-check"), " ", format_de_date(Sys.Date())
      ),
      # Kurzanleitung zum Ablauf je Aufgabe (Rueckmeldung: Ablauf unklar).
      tags$div(
        style = "font-size:12px; color:#455a64; background:#f5f8fb;
                 border:1px solid #dbe4ee; border-radius:6px;
                 padding:6px 10px; margin-bottom:10px;",
        icon("circle-info"), " Je Aufgabe: ",
        tags$b("Erledigt"), " ankreuzen – optional Kommentar zur Erledigung. ",
        "Oder ", tags$b("Nicht erledigt"), " – Grund auswählen und ",
        "ggf. Bemerkung eintragen (bei NE Pflicht). ",
        "Bemerkungen erscheinen in der Monatsübersicht mit ",
        tags$span(style = "color:#1565c0; font-weight:700;", "✎"), "."
      )
    )
    
    # ── Hinweis auf den Tab "Ueberfaellige Aufgaben" ─────────────────────
    # Rueckmeldung aus dem Labor: die Tagesliste soll nur die heutigen
    # Aufgaben zeigen. Versaeumte Eintraege stehen im eigenen Tab
    # "Ueberfaellige Aufgaben (offen: N)"; hier nur ein kurzer Hinweis.
    n_overdue <- tryCatch(length(overdue_info()$items), error = function(e) 0L)
    if (n_overdue > 0L) {
      ui_list <- c(list(tags$div(
        style = "display:flex; align-items:center; gap:10px; flex-wrap:wrap;
                 background:#fff8f0; border:1px solid #ffd699;
                 border-left:4px solid #f57c00; border-radius:6px;
                 padding:8px 12px; margin-bottom:10px; font-size:13px; color:#7f4f00;",
        icon("triangle-exclamation"),
        tags$span(sprintf("%d überfällige %s aus den letzten 14 Tagen.",
                          n_overdue, if (n_overdue == 1L) "Aufgabe" else "Aufgaben")),
        actionButton("goto_overdue_tab",
                     label = tagList(icon("arrow-right"), " Zu den überfälligen Aufgaben"),
                     class = "btn btn-warning btn-sm")
      )), ui_list)
    }
    
    
    # ── "Due right now" red banner ────────────────────────────────────────────
    # Only consider tasks whose schedule is actually due TODAY. Without this
    # check, a task whose NAME contains a time (e.g. "... ab 6:00") was being
    # flagged "Jetzt fällig" every day of the week, even when its schedule
    # (e.g. Wöchentlich (Mittwoch)) means it isn't due today at all.
    due_now_tasks <- character(0)
    cur_hdr_dn <- ""
    for (r2 in seq_len(nrows)) {
      h2 <- rv$data$Header[r2]
      if (nzchar(h2)) { cur_hdr_dn <- h2; next }
      tt <- if ("Task" %in% names(rv$data)) rv$data$Task[r2] else ""
      if (!nzchar(tt)) next
      if (!is_task_relevant_on(cur_hdr_dn, Sys.Date())) next   # not due today -> skip
      already_done <- !is.null(existing_for_today[[ as.character(r2) ]]) &&
        nzchar(existing_for_today[[ as.character(r2) ]])
      if (already_done) next
      ttime <- extract_task_time(tt)
      urg   <- if (!is.na(ttime)) classify_task_urgency(ttime) else NA_character_
      if (!is.na(urg) && urg %in% c("due_now", "overdue"))
        due_now_tasks <- c(due_now_tasks, sprintf("%s (%s)", tt, ttime))
    }
    if (length(due_now_tasks)) {
      ui_list <- append(ui_list, list(
        tags$div(
          style = "background:#d32f2f; color:#fff; border-radius:8px;
                   padding:12px 16px; margin-bottom:12px;
                   box-shadow:0 3px 8px rgba(211,47,47,0.3);",
          tags$div(style = "font-weight:800; font-size:14px; margin-bottom:6px;",
                   icon("bell"), " Jetzt f\u00e4llige Aufgaben"),
          tags$ul(style = "margin:0; padding-left:20px; font-size:13px;",
                  lapply(due_now_tasks, tags$li))
        )
      ), after = 1L)
    }
    # ─────────────────────────────────────────────────────────────────────────
    
    # ── Elektrodenaustausch reminder banner (COBAS Pro I/II) ─────────────────
    # Surface "zuletzt getauscht am:" rows whose next due Wednesday is today
    # or already in the past, so the team sees a red call-out at the very top
    # of the daily list listing exactly what has to be replaced.
    if (rv$current_device %in% c("g4", "g5")) {
      replace_due <- list()
      for (r3 in seq_len(nrows)) {
        if (nzchar(rv$data$Header[r3])) next
        tt3 <- if ("Task" %in% names(rv$data)) as.character(rv$data$Task[r3]) else ""
        if (!nzchar(tt3) || !is_last_replaced_task(tt3)) next
        months <- extract_replacement_months(tt3)
        if (is.na(months) || months <= 0L) next
        last_d <- task_meta_dates[[ as.character(r3) ]]
        if (is.null(last_d) || is.na(last_d)) {
          replace_due[[length(replace_due) + 1L]] <- list(
            row = r3, task = tt3, due = NA, missing = TRUE
          )
          next
        }
        due_d <- replacement_next_due(last_d, months)
        if (is.na(due_d)) next
        if (Sys.Date() >= due_d) {
          replace_due[[length(replace_due) + 1L]] <- list(
            row = r3, task = tt3, due = due_d, missing = FALSE,
            last = as.Date(last_d), months = months
          )
        }
      }
      if (length(replace_due)) {
        line_for <- function(item) {
          short <- sub("\\s*\u2013\\s*zuletzt.*$", "", item$task)
          if (isTRUE(item$missing)) {
            txt <- sprintf("%s \u2013 Datum bisher nicht eingetragen.", short)
          } else {
            overdue_days <- as.integer(Sys.Date() - item$due)
            tag <- if (overdue_days <= 0L) "heute f\u00e4llig"
            else sprintf("\u00fcberf\u00e4llig seit %d Tag%s",
                         overdue_days, ifelse(overdue_days == 1L, "", "en"))
            txt <- sprintf("%s \u2013 %s am %s (zuletzt: %s)",
                           short, tag,
                           format_de_short(item$due),
                           format(item$last, "%d.%m.%Y"))
          }
          tags$li(
            tags$a(href = sprintf("#task_row_%d", item$row),
                   onclick = sprintf(
                     "var el=document.getElementById('task_row_%d');
                      if(el){el.scrollIntoView({behavior:'smooth',block:'center'});
                             el.style.outline='2px solid #fff';
                             setTimeout(function(){el.style.outline='';},2000);}
                      return false;", item$row),
                   style = "color:#fff; text-decoration:underline;",
                   txt)
          )
        }
        ui_list <- append(ui_list, list(
          tags$div(
            style = "background:#d32f2f; color:#fff; border-radius:8px;
                     padding:12px 16px; margin-bottom:12px;
                     box-shadow:0 3px 8px rgba(211,47,47,0.3);",
            tags$div(style = "font-weight:800; font-size:14px; margin-bottom:6px;",
                     icon("triangle-exclamation"),
                     " Elektrodenaustausch f\u00e4llig (immer mittwochs)"),
            tags$div(style = "font-size:12px; opacity:0.92; margin-bottom:6px;",
                     "Bitte am n\u00e4chsten Mittwoch durchf\u00fchren \u2013 ",
                     "bei Feiertag am n\u00e4chsten Werktag."),
            tags$ul(style = "margin:0; padding-left:20px; font-size:13px;",
                    lapply(replace_due, line_for))
          )
        ), after = 1L)
      }
    }
    # ─────────────────────────────────────────────────────────────────────────
    
    # ── Main loop ─────────────────────────────────────────────────────────────
    # Sections whose schedule is not due today are hidden completely here
    # (rather than just dimmed) -- any task from them that's still
    # unfinished from an earlier due date is already surfaced in the
    # "Überfällige Aufgaben" banner above, so nothing is silently lost.
    skip_section <- FALSE
    # Buffer for a click-to-expand section (currently only Cobas 8100's
    # "Bei Bedarf"): while active, the header and its task rows are held
    # here instead of ui_list, then flushed together as one <details>
    # element so the tasks stay hidden until the user clicks the header.
    collapse_active     <- FALSE
    collapse_header_tag <- NULL
    collapse_buffer     <- list()
    for (r in seq_len(nrows)) {
      hdr       <- as.character(rv$data$Header[r])
      task_text <- if ("Task" %in% names(rv$data)) as.character(rv$data$Task[r]) else ""
      
      if (nzchar(hdr)) {
        # ── HEADER ROW ───────────────────────────────────────────────────────
        is_device_header <- hdr %in% c(
          "Cobas u411 (SN 5637) /Schnellteste",
          "PFA (SN 00398)", "MC1", "Multiplate  (SN 310071)"
        )
        
        is_today_monday   <- today_day_german == "Montag"
        is_today_thursday <- today_day_german == "Donnerstag"
        is_today_workday1 <- is_first_workday_of_month()
        is_today_monthly_cycle <- is_due_28day_cycle(Sys.Date(), MONTHLY_CYCLE_ANCHOR)
        is_today_quarter1 <- is_first_workday_of_quarter()
        is_today_biwk_mon <- is_biweekly_monday()
        is_today_biwk_wed <- is_biweekly_wednesday()
        is_phadia_friday   <- is_due_28day_cycle(Sys.Date(), PHADIA_FRIDAY_ANCHOR)
        is_analyzer_tuesday <- is_due_28day_cycle(Sys.Date(), ANALYZER_TUESDAY_ANCHOR)
        is_cobas_wednesday  <- is_due_28day_cycle(Sys.Date(), COBAS_WEDNESDAY_ANCHOR)
        
        sched_styles <- list(
          "Täglich"                         = list("#28a745","#20c997","#218838", TRUE),
          "Täglich (ZL)"                    = list("#28a745","#20c997","#218838", TRUE),
          "Täglich (Ablesen zwischen 12:00 und 14:00 Uhr)" = list("#28a745","#20c997","#218838", TRUE),
          "Montag und Donnerstag"           = list("#1e88e5","#42a5f5","#1565c0",
                                                   is_today_monday || is_today_thursday),
          "Wöchentlich"                     = list("#0097a7","#26c6da","#006064", is_today_monday),
          "Wöchentlich (Montag)"            = list("#0097a7","#26c6da","#006064", is_today_monday),
          "Wöchentlich (Dienstag)"          = list("#0097a7","#26c6da","#006064",
                                                   today_day_german == "Dienstag"),
          "Wöchentlich (Mittwoch)"          = list("#0097a7","#26c6da","#006064",
                                                   today_day_german == "Mittwoch"),
          "Wöchentlich (Mittwoch, TD)"      = list("#0097a7","#26c6da","#006064",
                                                   today_day_german == "Mittwoch"),
          "Wöchentlich (Donnerstag)"        = list("#0097a7","#26c6da","#006064", is_today_thursday),
          "Wöchentlich (Freitag)"           = list("#0097a7","#26c6da","#006064",
                                                   today_day_german == "Freitag"),
          "14-tägig"                        = list("#5e35b1","#7e57c2","#311b92", is_today_biwk_mon),
          "14-tägig (Mittwoch)"             = list("#5e35b1","#7e57c2","#311b92", is_today_biwk_wed),
          "Monatlich"                       = list("#8e24aa","#ba68c8","#4a148c", is_today_monthly_cycle),
          "Monatlich (Freitag)"             = list("#8e24aa","#ba68c8","#4a148c",
                                                   today_day_german == "Freitag" && is_today_workday1),
          "Monatlich oder alle 2500 Proben" = list("#8e24aa","#ba68c8","#4a148c", is_today_monthly_cycle),
          "Quartalsweise"                   = list("#ad1457","#ec407a","#880e4f", is_today_quarter1),
          "Alle 3 Monate oder alle 7500 Proben" = list("#ad1457","#ec407a","#880e4f", is_today_quarter1),
          "Am ersten Dienstag im Monat"     = list("#7f0000","#b71c1c","#4a0000", is_first_tuesday),
          "Am ersten Freitag im Monat"      = list("#00695c","#26a69a","#004d40", is_first_friday),
          "Monatlich (Freitag, alle 4 Wochen)"  = list("#00695c","#26a69a","#004d40", is_phadia_friday),
          "Monatlich (Dienstag, alle 4 Wochen)" = list("#7f0000","#b71c1c","#4a0000", is_analyzer_tuesday),
          "Monatlich (Mittwoch, alle 4 Wochen)" = list("#8e24aa","#ba68c8","#4a148c", is_cobas_wednesday),
          "Bei Bedarf"                      = list("#546e7a","#78909c","#37474f", FALSE),
          "Wartung bei Bedarf"              = list("#546e7a","#78909c","#37474f", FALSE),
          "Nach jeder Migration:"           = list("#546e7a","#78909c","#37474f", TRUE)
        )
        if (hdr %in% c("Montag","Dienstag","Mittwoch","Donnerstag","Freitag","Samstag","Sonntag"))
          sched_styles[[hdr]] <- list("#bf360c","#e65100","#7f2200", hdr == today_day_german)
        
        sty              <- sched_styles[[hdr]]
        is_today_relevant <- !is.null(sty) && isTRUE(sty[[4]])
        
        # Headers with no schedule entry (device sub-grouping labels, e.g.
        # "PFA (SN 00398)") and "as needed" schedules always show; every
        # other schedule header is hidden today unless it's actually due.
        always_show_header <- is.null(sty) || hdr %in% SCHEDULE_HEADERS_NO_DUE_DATE
        skip_section <- !always_show_header && !is_today_relevant
        if (skip_section) next
        
        header_style     <- ""
        if (!is.null(sty))
          header_style <- sprintf(
            "background:linear-gradient(135deg,%s 0%%,%s 100%%);
             color:white; padding:12px; margin:-5px 0 10px 0; border-radius:5px;
             font-weight:700; font-size:14px;
             box-shadow:0 2px 4px rgba(0,0,0,0.1);
             border-left:4px solid %s;%s",
            sty[[1]], sty[[2]], sty[[3]],
            if (is_today_relevant) "" else " opacity:0.55;"
          )
        
        # Compute the next due date for this schedule (if any).
        next_due_date <- switch(hdr,
                                "Monatlich"                       = next_28day_due(Sys.Date(), MONTHLY_CYCLE_ANCHOR),
                                "Monatlich oder alle 2500 Proben" = next_28day_due(Sys.Date(), MONTHLY_CYCLE_ANCHOR),
                                "Am ersten Dienstag im Monat"     = next_28day_due(Sys.Date(), TUESDAY_CYCLE_ANCHOR),
                                "Am ersten Freitag im Monat"      = next_28day_due(Sys.Date(), FRIDAY_CYCLE_ANCHOR),
                                "Monatlich (Freitag, alle 4 Wochen)"  = next_28day_due(Sys.Date(), PHADIA_FRIDAY_ANCHOR),
                                "Monatlich (Dienstag, alle 4 Wochen)" = next_28day_due(Sys.Date(), ANALYZER_TUESDAY_ANCHOR),
                                "Monatlich (Mittwoch, alle 4 Wochen)" = next_28day_due(Sys.Date(), COBAS_WEDNESDAY_ANCHOR),
                                "14-tägig (Mittwoch)"             = next_biweekly_wednesday(Sys.Date()),
                                "Wöchentlich"                     = next_monday(Sys.Date()),
                                "Wöchentlich (Montag)"            = next_monday(Sys.Date()),
                                "Wöchentlich (Mittwoch)"          = next_wednesday(Sys.Date()),
                                "Wöchentlich (Mittwoch, TD)"      = next_wednesday(Sys.Date()),
                                "Wöchentlich (Donnerstag)"        = next_thursday(Sys.Date()),
                                NA
        )
        next_due_label <- if (!is.na(next_due_date) && inherits(next_due_date, "Date"))
          sprintf("(f\u00e4llig am %s)", format_de_short(next_due_date))
        else NULL
        
        header_tag <- if (nzchar(header_style)) {
          tags$div(class = "task-header", style = header_style,
                   icon(if (is_today_relevant) "bell" else "calendar"),
                   " ", hdr,
                   if (is_today_relevant)
                     tags$span(style = "margin-left:8px; font-size:11px; opacity:0.9;",
                               "(heute fällig)")
                   else if (!is.null(next_due_label))
                     tags$span(style = "margin-left:8px; font-size:11px;
                                        background:rgba(255,255,255,0.22);
                                        padding:2px 8px; border-radius:999px;
                                        font-weight:600;",
                               next_due_label))
        } else if (is_device_header) {
          tags$div(class = "task-header device-header", icon("microchip"), " ", hdr)
        } else {
          tags$div(class = "task-header", icon("cog"), " ", hdr)
        }
        
        # Flush any still-open collapsible section before starting a new header.
        if (collapse_active) {
          ui_list <- append(ui_list, list(
            tags$details(class = "task-collapse",
                         tags$summary(collapse_header_tag),
                         collapse_buffer)
          ))
          collapse_active     <- FALSE
          collapse_header_tag <- NULL
          collapse_buffer     <- list()
        }
        
        # Cobas 8100's "Bei Bedarf" tasks are rarely needed day-to-day, so
        # they're tucked behind a click-to-expand section instead of
        # always taking up space in the list.
        if (identical(rv$current_device, "g3") && identical(hdr, "Bei Bedarf")) {
          collapse_active     <- TRUE
          collapse_header_tag <- header_tag
        } else {
          ui_list <- append(ui_list, list(header_tag))
        }
        
      } else {
        # ── TASK ROW ─────────────────────────────────────────────────────────
        if (skip_section) next
        task_name <- if (nzchar(task_text)) task_text else paste0("Aufgabe ", r)
        done_id   <- paste0("task_done_",    r)
        opt_id    <- paste0("task_opt_",     r)
        rem_id    <- paste0("task_rem_",     r)
        nicht_id  <- paste0("task_nicht_",   r)
        
        existing_val <- existing_for_today[[ as.character(r) ]] %||% ""
        is_done      <- FALSE
        selected_opt <- ""
        if (nzchar(existing_val)) {
          v <- trimws(as.character(existing_val))
          if (startsWith(v, "\u2713")) {
            is_done <- TRUE
          } else {
            abbrev_map <- c(
              "WE"   = "WE (Wochenende)",
              "FT"   = "FT (Feiertag)",
              "Ø"    = "Ø (An diesem Tag wurden keine Analysen gestartet)",
              "W.e." = "W.e. (Wartungspunkt ist in einer größeren Wartung enthalten)",
              "ne"   = "ne (Nicht erforderlich (für die Rubrik \u201e bei Bedarf\"))",
              "D"    = "D (Gerät / Modul defekt)",
              "NE"   = "NE (Nicht erledigt \u2013 bitte Bemerkung eintragen)",
              "sB"   = "sB (Siehe Bemerkungen)",
              "sQ"   = "sQ (Siehe Quasi)"
            )
            selected_opt <- if (v %in% TASK_OPTIONS) v else abbrev_map[v] %||% ""
          }
        }
        
        rem_val      <- today_remarks[[ as.character(r) ]]$text %||% ""
        # Is this task currently marked "Nicht erledigt" (NE)?
        is_ne <- identical(selected_opt,
                           "NE (Nicht erledigt \u2013 bitte Bemerkung eintragen)")
        task_class   <- if (is_done) "task-item completed"
        else if (is_ne) "task-item nicht-erledigt"
        else "task-item"
        remark_class <- if (is_ne) "task-remark needs-remark" else "task-remark"
        state_cls    <- task_state_class(is_done, selected_opt)
        
        # ── Reminder logic ────────────────────────────────────────────────────
        t_time     <- extract_task_time(task_name)
        urgency    <- if (!is_done && !is.na(t_time))
          classify_task_urgency(t_time) else NA_character_
        time_badge <- urgency_badge(urgency, t_time)
        item_style <- if (is_ne) {
          "border-left-color:#d32f2f !important; background:#fdecea;"
        } else if (!is_done) {
          switch(urgency %||% "none",
                 due_now  = "border-left-color:#d32f2f !important; background:#fff5f5;",
                 upcoming = "border-left-color:#f57c00 !important; background:#fff8f0;",
                 overdue  = "border-left-color:#6d1a1a !important; background:#fdf0f0;",
                 ""
          )
        } else ""
        # ─────────────────────────────────────────────────────────────────────
        
        row_ui <- tags$div(
          class = task_class,
          id = paste0("task_row_", r),
          style = item_style,
          tags$div(class = "task-name",
                   icon(if (is_done) "clipboard-check" else "clipboard"),
                   " ", task_name, time_badge,
                   if (is_ne) tags$span(
                     style = "background:#d32f2f; color:#fff; padding:1px 8px;
                              border-radius:999px; font-size:11px; font-weight:700;
                              margin-left:8px; text-transform:uppercase;
                              letter-spacing:.3px;",
                     icon("xmark"), " Nicht erledigt") else NULL),
          tags$div(class = paste("task-controls", state_cls),
                   tags$div(class = "task-checkbox",
                            checkboxInput(done_id, label = "Erledigt", value = is_done)),
                   tags$div(class = "task-nicht",
                            actionButton(nicht_id,
                                         label = tagList(icon("xmark"), " Nicht erledigt"),
                                         class = paste("btn btn-sm btn-nicht-erledigt",
                                                       if (state_cls == "state-notdone") "is-ne" else ""))),
                   tags$div(class = "task-select",
                            selectInput(opt_id, label = NULL,
                                        choices  = task_reason_choices(),
                                        selected = if (is_done) "" else selected_opt,
                                        selectize = FALSE)),
                   tags$div(class = remark_class,
                            textInput(rem_id, label = NULL, value = rem_val,
                                      placeholder = task_remark_placeholder(state_cls)))
          ),
          # Letzte fruehere Bemerkung zu dieser Aufgabe (z. B. "positiv" beim
          # Drogentest), damit man weiss, was beim letzten Mal war.
          {
            lr_i <- last_remarks[[ as.character(r) ]]
            if (!is.null(lr_i)) {
              opt_txt <- if (!is.na(lr_i$opt) && nzchar(lr_i$opt)) paste0("[", lr_i$opt, "] ") else ""
              tags$div(
                style = "font-size:11px; color:#5f6b7a; margin:4px 0 0 2px;",
                icon("comment"), " Letzte Bemerkung (",
                format(lr_i$date, "%d.%m.%Y"),
                if (nzchar(lr_i$who)) paste0(", ", lr_i$who) else "", "): ",
                tags$span(style = "font-style:italic; color:#333;",
                          paste0(opt_txt, lr_i$text)))
            }
          },
          if (is_last_replaced_task(task_name)) {
            date_id  <- paste0("task_lastrepl_",      r)
            date_btn <- paste0("task_lastrepl_save_", r)
            cur_date <- task_meta_dates[[ as.character(r) ]]
            if (is.null(cur_date) || is.na(cur_date)) cur_date <- NA
            # For COBAS Pro I/II compute the next-due Wednesday so the user
            # can see when the next replacement should happen.
            next_due  <- NA
            due_label <- NULL
            if (rv$current_device %in% c("g4", "g5") &&
                !is.null(cur_date) && !is.na(cur_date)) {
              months <- extract_replacement_months(task_name)
              next_due <- replacement_next_due(cur_date, months)
              if (!is.na(next_due)) {
                overdue_n <- as.integer(Sys.Date() - next_due)
                badge_bg  <- if (overdue_n < 0L) "#e3f2fd"
                else if (overdue_n == 0L) "#fff3cd"
                else "#fdecea"
                badge_fg  <- if (overdue_n < 0L) "#0d47a1"
                else if (overdue_n == 0L) "#7a5d00"
                else "#a02218"
                txt <- if (overdue_n < 0L)
                  sprintf("N\u00e4chster Austausch: %s",
                          format_de_short(next_due))
                else if (overdue_n == 0L)
                  sprintf("Heute f\u00e4llig (%s)",
                          format_de_short(next_due))
                else
                  sprintf("\u00dcberf\u00e4llig seit %d Tag%s (%s)",
                          overdue_n,
                          ifelse(overdue_n == 1L, "", "en"),
                          format_de_short(next_due))
                due_label <- tags$span(
                  style = sprintf(
                    "background:%s; color:%s; padding:2px 8px; border-radius:999px;
                     font-size:12px; font-weight:700; margin-left:8px;",
                    badge_bg, badge_fg),
                  icon("calendar-week"), " ", txt
                )
              }
            }
            tags$div(class = "task-lastreplaced",
                     style = "display:flex; align-items:center; gap:8px; margin-top:6px;
                       padding:8px 10px; background:#f5faff; border-left:3px solid #1e88e5;
                       border-radius:6px; font-size:13px; flex-wrap:wrap;",
                     tags$span(style = "font-weight:600; color:#1e3a5c;",
                               icon("calendar-day"), " Zuletzt getauscht am:"),
                     dateInput(date_id, label = NULL,
                               value = if (is.na(cur_date)) NA else cur_date,
                               format = "dd.mm.yyyy", language = "de",
                               weekstart = 1, autoclose = TRUE),
                     actionButton(date_btn,
                                  label = tagList(icon("floppy-disk"), " Speichern"),
                                  class = "btn btn-sm btn-save-remark"),
                     if (rv$current_device %in% c("g4", "g5"))
                       tags$span(style = "font-size:11px; color:#1e3a5c; opacity:0.85;
                                    margin-left:4px;",
                                 icon("info-circle"),
                                 " Termin immer mittwochs (bei Feiertag n\u00e4chster Werktag)."),
                     due_label
            )
          }
        )
        
        if (collapse_active) {
          collapse_buffer <- append(collapse_buffer, list(row_ui))
        } else {
          ui_list <- append(ui_list, list(row_ui))
        }
      }   # end task row
    }     # end for loop
    
    # Flush a collapsible section that ran to the end of the table.
    if (collapse_active) {
      ui_list <- append(ui_list, list(
        tags$details(class = "task-collapse",
                     tags$summary(collapse_header_tag),
                     collapse_buffer)
      ))
    }
    
    do.call(tagList, ui_list)
  })
  
  # Refresh tasks button
  observeEvent(input$refresh_tasks, {
    req(rv$current_device)
    con <- pg_con(); on.exit(dbDisconnect(con), add = TRUE)
    rv$table_status <- load_cell_status_current(con, rv$current_device)
    rv$tasks_refresh <- isolate(rv$tasks_refresh) + 1L
    showNotification("Aufgaben aktualisiert", type = "message", duration = 2)
  })
  
  # ---- Admin: add a new task to any device ---------------------------------
  # Distinct schedule headers in a device's table (excludes device-section
  # headers which we treat as anything not matching a known schedule).
  # Reuses SCHEDULE_HEADER_NAMES (defined earlier) as the single source of
  # truth for recognized schedules, plus "Start-Up" which has no due-date
  # logic of its own but is still a valid admin-assignable section.
  KNOWN_SCHEDULES <- c(SCHEDULE_HEADER_NAMES, "Start-Up")
  schedule_headers_for_device <- function(con, device_id) {
    df <- tryCatch(load_device_table(con, device_id), error = function(e) NULL)
    if (is.null(df) || !nrow(df)) return(character(0))
    hdrs <- unique(df$Header[nzchar(df$Header)])
    intersect(hdrs, KNOWN_SCHEDULES)
  }
  # Insert a new empty task row (Header == "") under the LAST occurrence of
  # `header_text` in `df`, just before the next non-empty header (i.e. at the
  # end of that schedule's block).
  insert_task_under_header <- function(df, header_text, task_text) {
    if (is.null(df) || !nrow(df)) return(df)
    hdr_idx <- which(df$Header == header_text)
    if (!length(hdr_idx)) return(df)
    hi <- tail(hdr_idx, 1)
    later <- which(nzchar(df$Header))
    later <- later[later > hi]
    end_idx <- if (length(later)) min(later) - 1L else nrow(df)
    new_row <- df[hi, , drop = FALSE]
    new_row[1, ] <- ""
    new_row$Header <- ""
    new_row$Task   <- task_text
    before <- df[seq_len(end_idx), , drop = FALSE]
    after  <- if (end_idx < nrow(df)) df[(end_idx + 1L):nrow(df), , drop = FALSE] else df[0, , drop = FALSE]
    rbind(before, new_row, after)
  }
  
  output$admin_add_task_box <- renderUI({
    req(rv$authed)
    if (!identical(rv$role, "admin")) return(NULL)
    con <- pg_con(); on.exit(dbDisconnect(con), add = TRUE)
    devs <- tryCatch(
      DBI::dbGetQuery(con, "SELECT device_id, label FROM devices ORDER BY device_id"),
      error = function(e) data.frame(device_id = character(0), label = character(0))
    )
    devs <- devs[!(devs$device_id %in% RETIRED_DEVICE_IDS), , drop = FALSE]
    if (!nrow(devs)) {
      return(div(class = "alert alert-warning",
                 "Keine Geräte vorhanden \u2013 neue Aufgabe kann nicht hinzugefügt werden."))
    }
    dev_choices <- setNames(devs$device_id,
                            ifelse(nzchar(devs$label %||% ""),
                                   paste0(devs$label, " (", devs$device_id, ")"),
                                   devs$device_id))
    selected_dev <- rv$current_device %||% devs$device_id[1]
    box(width = 12, collapsible = TRUE, collapsed = FALSE,
        title = "Neue Aufgabe hinzufügen (Admin)",
        status = "warning", solidHeader = TRUE,
        fluidRow(
          column(4, selectInput("admin_new_task_device", "Gerät",
                                choices = dev_choices, selected = selected_dev)),
          column(4, uiOutput("admin_new_task_header_ui")),
          column(4, textInput("admin_new_task_text", "Aufgabe",
                              placeholder = "z. B. Filter wechseln"))
        ),
        actionButton("admin_add_task_btn", "Aufgabe hinzufügen",
                     class = "btn btn-warning", icon = icon("plus")),
        helpText("Die Aufgabe wird am Ende des gewählten Wartungsabschnitts eingefügt und sofort gespeichert.")
    )
  })
  
  output$admin_new_task_header_ui <- renderUI({
    req(rv$authed, identical(rv$role, "admin"))
    did <- input$admin_new_task_device %||% rv$current_device
    req(did)
    con <- pg_con(); on.exit(dbDisconnect(con), add = TRUE)
    hdrs <- schedule_headers_for_device(con, did)
    if (!length(hdrs)) {
      return(tagList(
        selectInput("admin_new_task_header", "Wartungsabschnitt", choices = character(0)),
        helpText("Keine Wartungsabschnitte für dieses Gerät gefunden.")
      ))
    }
    selectInput("admin_new_task_header", "Wartungsabschnitt", choices = hdrs)
  })
  
  observeEvent(input$admin_add_task_btn, {
    req(rv$authed)
    if (!identical(rv$role, "admin")) {
      showNotification("Nur Admins können Aufgaben hinzufügen.", type = "error"); return()
    }
    did   <- input$admin_new_task_device
    hdr   <- input$admin_new_task_header
    task  <- trimws(input$admin_new_task_text %||% "")
    if (is.null(did) || !nzchar(did)) {
      showNotification("Bitte ein Gerät wählen.", type = "error"); return()
    }
    if (is.null(hdr) || !nzchar(hdr)) {
      showNotification("Bitte einen Wartungsabschnitt wählen.", type = "error"); return()
    }
    if (!nzchar(task)) {
      showNotification("Bitte einen Aufgabennamen eingeben.", type = "error"); return()
    }
    con <- pg_con(); on.exit(dbDisconnect(con), add = TRUE)
    df  <- tryCatch(load_device_table(con, did) %||% create_initial_table(did),
                    error = function(e) NULL)
    if (is.null(df) || !nrow(df)) {
      showNotification("Gerätetabelle konnte nicht geladen werden.", type = "error"); return()
    }
    if (!any(df$Header == hdr)) {
      showNotification(sprintf("Abschnitt '%s' existiert für dieses Gerät nicht.", hdr),
                       type = "error"); return()
    }
    df_new <- insert_task_under_header(df, hdr, task)
    tryCatch({
      save_device_table(con, did, df_new, rv$user %||% "<admin>")
      showNotification(sprintf("Aufgabe in '%s' hinzugefügt.", hdr), type = "message")
      updateTextInput(session, "admin_new_task_text", value = "")
      # If admin edited the currently-open device, refresh in-memory data
      # so the new task appears immediately on the checklist.
      if (identical(did, rv$current_device)) {
        rv$data <- order_cols(df_new)
        rv$table_status <- load_cell_status_current(con, did)
        rv$tasks_refresh <- isolate(rv$tasks_refresh) + 1L
      }
    }, error = function(e) {
      showNotification(paste("Fehler beim Speichern:", conditionMessage(e)), type = "error")
    })
  })
  
  
  # ---- Admin: edit an existing task's text and/or schedule assignment -----
  # Renumbering row positions is the tricky part here: task_status,
  # device_cell_status, device_task_remark, and device_task_meta all key
  # their historical entries by plain positional row_index. Moving a task
  # into a different schedule section means physically relocating its row,
  # which shifts the position of every row between the old and new spot --
  # if we didn't also renumber THEIR history, checkmarks/remarks would end
  # up silently attached to the wrong task. remap_row_indices() keeps all
  # four tables in sync whenever a move happens.
  
  # Compute the full old-index -> new-index mapping for a move of row
  # `p_old` (out of `n` original rows) to new position `q` (1-based, in the
  # table that already had `p_old` removed). Returns a named list, old
  # index (as character) -> new index; only rows that actually move are
  # included.
  compute_move_remap <- function(n, p_old, q) {
    remap <- list()
    for (k in seq_len(n)) {
      if (k == p_old) next
      m  <- if (k < p_old) k else k - 1L
      fp <- if (m < q) m else m + 1L
      if (fp != k) remap[[as.character(k)]] <- fp
    }
    remap
  }
  
  # Apply a row_index remap to every history table for one device. Uses a
  # negative holding range in a first pass so no intermediate UPDATE can
  # collide with another row's primary key, regardless of the shift pattern.
  remap_row_indices <- function(con, device_id, remap) {
    if (!length(remap)) return(invisible(NULL))
    tables <- c("task_status", "device_cell_status", "device_task_remark", "device_task_meta")
    OFFSET <- 1000000L
    DBI::dbWithTransaction(con, {
      for (tbl in tables) {
        for (old_idx in as.integer(names(remap))) {
          DBI::dbExecute(con, sprintf(
            "UPDATE %s SET row_index = $1 WHERE device_id = $2 AND row_index = $3", tbl),
            params = list(-(old_idx + OFFSET), device_id, old_idx))
        }
        for (old_idx in as.integer(names(remap))) {
          new_idx <- remap[[as.character(old_idx)]]
          DBI::dbExecute(con, sprintf(
            "UPDATE %s SET row_index = $1 WHERE device_id = $2 AND row_index = $3", tbl),
            params = list(new_idx, device_id, -(old_idx + OFFSET)))
        }
      }
    })
  }
  
  # Move task row `row_idx` (Header == "") so it becomes the last task under
  # `new_header`'s block. If that schedule doesn't exist yet on this
  # device, a new header block for it is appended at the end first. Returns
  # list(df = <new table>, remap = <old->new row_index map, incl. the
  # moved row itself>) so the caller can keep history tables in sync.
  move_task_to_header <- function(df, row_idx, new_header) {
    n <- nrow(df)
    if (is.null(df) || row_idx < 1 || row_idx > n || nzchar(df$Header[row_idx]))
      return(list(df = df, remap = list()))
    row <- df[row_idx, , drop = FALSE]
    df_without <- df[-row_idx, , drop = FALSE]
    hdr_idx <- which(df_without$Header == new_header)
    if (!length(hdr_idx)) {
      new_hdr_row <- df_without[1, , drop = FALSE]
      new_hdr_row[1, ] <- ""
      new_hdr_row$Header <- new_header
      df_without <- rbind(df_without, new_hdr_row)
      hdr_idx <- nrow(df_without)
    }
    hi <- tail(hdr_idx, 1)
    later <- which(nzchar(df_without$Header))
    later <- later[later > hi]
    end_idx <- if (length(later)) min(later) - 1L else nrow(df_without)
    q <- end_idx + 1L
    before <- df_without[seq_len(end_idx), , drop = FALSE]
    after  <- if (end_idx < nrow(df_without)) df_without[(end_idx + 1L):nrow(df_without), , drop = FALSE] else df_without[0, , drop = FALSE]
    df_new <- rbind(before, row, after)
    
    remap <- compute_move_remap(n, row_idx, q)
    if (q != row_idx) remap[[as.character(row_idx)]] <- q
    
    list(df = df_new, remap = remap)
  }
  
  # ---- Admin diagnostic: inspect raw cell_status/remark history for a device ----
  # Built to investigate the "wrong initials in Monatsübersicht" report: shows
  # exactly what's stored (row_index, day, value_text, updated_by, updated_at)
  # so a mismatch between the task now at that row and who the DB says touched
  # it becomes visible directly, instead of guessing from code alone.
  # ---- Admin: rebuild ONE device's task structure from the template -------
  # Safe, explicit, on-demand replacement for the old blanket auto-DELETE.
  # Use this when a device's stored task table is structurally broken/stale
  # (e.g. tasks showing in wrong columns from an old malformed save). It
  # replaces ONLY the task structure (headers + task texts) from the current
  # template; it does NOT touch device_cell_status / remarks (the actual
  # checkmark & remark history). If the template's row layout differs from
  # the stored one, history alignment is the same concern as any structural
  # edit -- so this warns explicitly and is gated behind a confirmation.
  # ---- Admin: "Alle Aufgaben sehen" -- every task across all devices with
  # its schedule (when/day due) and next due date. Read-only overview. -------
  output$admin_all_tasks_box <- renderUI({
    req(rv$authed)
    if (!identical(rv$role, "admin")) return(tags$p("Nur für Administratoren."))
    rv$tasks_refresh  # refresh if structure changes in-session
    con <- pg_con(); on.exit(dbDisconnect(con), add = TRUE)
    devs <- tryCatch(
      DBI::dbGetQuery(con, "SELECT device_id, label FROM devices ORDER BY device_id"),
      error = function(e) data.frame(device_id = character(0), label = character(0)))
    devs <- devs[!(devs$device_id %in% RETIRED_DEVICE_IDS), , drop = FALSE]
    if (!nrow(devs)) return(tags$p("Keine Geräte gefunden."))
    label_of <- setNames(devs$label, devs$device_id)
    
    # Same Arbeitsplatz grouping as the Geräte-Übersicht hub page.
    arbeitsplaetze <- list(
      list(name = "Arbeitsplatz 1 \u2013 H\u00e4matologie",            devices = c("g7", "g15")),
      list(name = "Arbeitsplatz 2 \u2013 Gerinnung",              devices = c("g1", "g6", "g16", "g11")),
      list(name = "Arbeitsplatz 3 \u2013 Klinische Chemie",       devices = c("g4", "g5", "g9")),
      list(name = "Arbeitsplatz 4 \u2013 Immunologie / Allergie", devices = c("g2", "g8", "g10")),
      list(name = "Arbeitsplatz 5 \u2013 Cobas 8100",             devices = c("g3", "g17"))
    )
    
    # Detect devices whose STORED structure differs from the current
    # template (headers + task texts). This is the usual cause of "the
    # overview shows the wrong Fälligkeit": the code/template was updated but
    # the device's saved table in the database is still the old version.
    # Read-only check -- it only flags; the admin fixes it with the rebuild
    # tool. (Header+Task signature ignores the day columns / history.)
    struct_sig <- function(df) {
      if (is.null(df) || !nrow(df)) return("")
      paste(paste0(as.character(df$Header), "\u241f", as.character(df$Task)),
            collapse = "\u2016")
    }
    stale_devices <- character(0)
    for (dd in devs$device_id) {
      stored <- tryCatch(load_device_table(con, dd), error = function(e) NULL)
      if (is.null(stored)) next  # never saved yet -> builds from template on open
      tmpl <- tryCatch(create_initial_table(dd), error = function(e) NULL)
      if (is.null(tmpl)) next
      if (!identical(struct_sig(stored), struct_sig(tmpl))) {
        stale_devices <- c(stale_devices, dd)
      }
    }
    stale_banner <- if (length(stale_devices)) {
      lbls <- vapply(stale_devices, function(d)
        sprintf("%s (%s)", label_of[[d]] %||% d, d), character(1))
      tags$div(
        style = "background:#fff3cd; border:1px solid #ffe08a; color:#7a5c00;
                 border-radius:8px; padding:12px 16px; margin-bottom:16px;",
        tags$div(style = "font-weight:700; margin-bottom:4px;",
                 icon("triangle-exclamation"),
                 " Veraltete Aufgaben-Struktur erkannt"),
        tags$div(style = "font-size:13px;",
                 "Bei folgenden Geräten weicht die gespeicherte Struktur von der ",
                 "aktuellen Vorlage ab \u2013 die angezeigte Fälligkeit kann dadurch ",
                 "falsch sein: ",
                 tags$b(paste(lbls, collapse = ", ")), ". ",
                 "Bitte unter \u201eNeue Aufgabe hinzufügen\u201c \u2192 \u201eAufgaben-Struktur ",
                 "aus Vorlage neu aufbauen\u201c für diese Geräte aktualisieren.")
      )
    } else NULL
    
    # Build the task table for a single device (returns a box, or NULL).
    render_device_block <- function(did) {
      label <- label_of[[did]] %||% did
      df <- tryCatch(load_device_table(con, did) %||% create_initial_table(did),
                     error = function(e) NULL)
      if (is.null(df) || !nrow(df)) return(NULL)
      cur_hdr <- ""
      trows <- list()
      for (i in seq_len(nrow(df))) {
        h <- as.character(df$Header[i])
        if (nzchar(h)) { cur_hdr <- h; next }
        tk <- as.character(df$Task[i])
        if (!nzchar(tk)) next
        when_txt <- schedule_when_text(cur_hdr)
        if (!nzchar(when_txt)) when_txt <- cur_hdr
        nd <- tryCatch(schedule_next_due(cur_hdr), error = function(e) as.Date(NA))
        nd_txt <- if (!is.na(nd)) format_de_short(nd) else "\u2013"
        trows[[length(trows) + 1L]] <- tags$tr(
          tags$td(style = "padding:4px 10px 4px 0; vertical-align:top;", tk),
          tags$td(style = "padding:4px 10px 4px 0; vertical-align:top; white-space:nowrap;",
                  tags$span(style = "background:#eef2f7; color:#003B73; padding:1px 8px;
                                     border-radius:4px; font-size:12px; font-weight:600;",
                            when_txt)),
          tags$td(style = "padding:4px 0; vertical-align:top; white-space:nowrap; color:#555;",
                  nd_txt)
        )
      }
      if (!length(trows)) return(NULL)
      box(width = 12, collapsible = TRUE, collapsed = TRUE,
          title = sprintf("%s (%s) \u2013 %d Aufgaben",
                          if (nzchar(label)) label else did, did, length(trows)),
          status = "primary", solidHeader = FALSE,
          tags$table(
            style = "width:100%; border-collapse:collapse; font-size:13px;",
            tags$thead(tags$tr(
              tags$th(style = "text-align:left; padding:4px 10px 6px 0; border-bottom:2px solid #dee2e6;", "Aufgabe"),
              tags$th(style = "text-align:left; padding:4px 10px 6px 0; border-bottom:2px solid #dee2e6;", "Fälligkeit"),
              tags$th(style = "text-align:left; padding:4px 0 6px 0; border-bottom:2px solid #dee2e6;", "Nächster Termin")
            )),
            tags$tbody(trows)
          )
      )
    }
    
    assigned <- unlist(lapply(arbeitsplaetze, `[[`, "devices"))
    ap_sections <- lapply(arbeitsplaetze, function(ap) {
      dev_blocks <- Filter(Negate(is.null),
                           lapply(intersect(ap$devices, devs$device_id), render_device_block))
      if (!length(dev_blocks)) return(NULL)
      tagList(
        tags$h4(style = "margin:22px 0 10px; color:#003B73; border-bottom:2px solid #003B73;
                         padding-bottom:4px;",
                icon("layer-group"), " ", ap$name),
        dev_blocks
      )
    })
    ap_sections <- Filter(Negate(is.null), ap_sections)
    
    # Any device not assigned to an Arbeitsplatz -> "Sonstige Geräte".
    other_ids <- setdiff(devs$device_id, assigned)
    other_blocks <- Filter(Negate(is.null), lapply(other_ids, render_device_block))
    if (length(other_blocks)) {
      ap_sections <- c(ap_sections, list(tagList(
        tags$h4(style = "margin:22px 0 10px; color:#555; border-bottom:2px solid #999;
                         padding-bottom:4px;",
                icon("layer-group"), " Sonstige Geräte"),
        other_blocks
      )))
    }
    
    if (!length(ap_sections)) return(tags$p("Keine Aufgaben gefunden."))
    tagList(
      stale_banner,
      tags$p(style = "color:#555; margin-bottom:14px;",
             icon("circle-info"),
             " Überblick über alle definierten Aufgaben je Gerät \u2013 nach Arbeitsplatz ",
             "gruppiert wie auf der Geräte-Übersicht. Nur-Lese-Ansicht."),
      ap_sections
    )
  })
  
  output$admin_rebuild_box <- renderUI({
    req(rv$authed)
    if (!identical(rv$role, "admin")) return(NULL)
    con <- pg_con(); on.exit(dbDisconnect(con), add = TRUE)
    devs <- tryCatch(
      DBI::dbGetQuery(con, "SELECT device_id, label FROM devices ORDER BY device_id"),
      error = function(e) data.frame(device_id = character(0), label = character(0))
    )
    devs <- devs[!(devs$device_id %in% RETIRED_DEVICE_IDS), , drop = FALSE]
    if (!nrow(devs)) return(NULL)
    dev_choices <- setNames(devs$device_id,
                            ifelse(nzchar(devs$label %||% ""),
                                   paste0(devs$label, " (", devs$device_id, ")"),
                                   devs$device_id))
    box(width = 12, collapsible = TRUE, collapsed = TRUE,
        title = "Aufgaben-Struktur aus Vorlage neu aufbauen (Admin)",
        status = "danger", solidHeader = TRUE,
        fluidRow(
          column(6, selectInput("admin_rebuild_device", "Gerät",
                                choices = dev_choices,
                                selected = rv$current_device %||% devs$device_id[1]))
        ),
        actionButton("admin_rebuild_btn", "Struktur neu aufbauen",
                     class = "btn btn-danger", icon = icon("rotate")),
        helpText("Ersetzt NUR die Aufgaben-Struktur (Überschriften + Aufgabentexte) dieses ",
                 "Geräts durch die aktuelle Vorlage. Haken/Bemerkungen (Historie) werden NICHT ",
                 "gelöscht. Nur verwenden, wenn eine Tabelle beschädigt ist (z. B. Aufgaben in ",
                 "falschen Spalten). Bei geänderter Zeilenanzahl kann sich die Zuordnung alter ",
                 "Haken zu Aufgaben verschieben.")
    )
  })
  
  observeEvent(input$admin_rebuild_btn, {
    req(rv$authed)
    if (!identical(rv$role, "admin")) {
      showNotification("Nur Admins.", type = "error"); return()
    }
    did <- input$admin_rebuild_device
    req(did)
    showModal(modalDialog(
      title = "Struktur neu aufbauen?",
      paste0("Die Aufgaben-Struktur von '", did, "' wird durch die aktuelle Vorlage ersetzt. ",
             "Haken und Bemerkungen bleiben erhalten, können sich aber anderen Aufgaben zuordnen, ",
             "falls sich die Zeilenreihenfolge geändert hat. Fortfahren?"),
      footer = tagList(
        modalButton("Abbrechen"),
        actionButton("admin_rebuild_confirm", "Ja, neu aufbauen", class = "btn btn-danger")
      ),
      easyClose = TRUE
    ))
  })
  
  observeEvent(input$admin_rebuild_confirm, {
    req(rv$authed, identical(rv$role, "admin"))
    did <- input$admin_rebuild_device
    if (is.null(did) || !nzchar(did)) { removeModal(); return() }
    con <- pg_con(); on.exit(dbDisconnect(con), add = TRUE)
    tryCatch({
      fresh <- create_initial_table(did)
      save_device_table(con, did, fresh, rv$user %||% "<admin>")
      removeModal()
      showNotification(sprintf("Struktur von '%s' aus Vorlage neu aufgebaut.", did),
                       type = "message")
      if (identical(did, rv$current_device)) {
        rv$data <- order_cols(fresh)
        rv$table_status <- load_cell_status_current(con, did)
        rv$tasks_refresh <- isolate(rv$tasks_refresh) + 1L
      }
    }, error = function(e) {
      removeModal()
      showNotification(paste("Fehler:", conditionMessage(e)), type = "error")
    })
  })
  
  output$admin_diag_box <- renderUI({
    req(rv$authed)
    if (!identical(rv$role, "admin")) return(NULL)
    con <- pg_con(); on.exit(dbDisconnect(con), add = TRUE)
    devs <- tryCatch(
      DBI::dbGetQuery(con, "SELECT device_id, label FROM devices ORDER BY device_id"),
      error = function(e) data.frame(device_id = character(0), label = character(0))
    )
    devs <- devs[!(devs$device_id %in% RETIRED_DEVICE_IDS), , drop = FALSE]
    if (!nrow(devs)) return(NULL)
    dev_choices <- setNames(devs$device_id,
                            ifelse(nzchar(devs$label %||% ""),
                                   paste0(devs$label, " (", devs$device_id, ")"),
                                   devs$device_id))
    box(width = 12, collapsible = TRUE, collapsed = TRUE,
        title = "Diagnose: Rohdaten je Aufgabe (Admin)",
        status = "info", solidHeader = TRUE,
        fluidRow(
          column(4, selectInput("admin_diag_device", "Gerät",
                                choices = dev_choices,
                                selected = rv$current_device %||% devs$device_id[1])),
          column(4, numericInput("admin_diag_month", "Monat",
                                 value = as.integer(format(Sys.Date(), "%m")), min = 1, max = 12)),
          column(4, numericInput("admin_diag_year", "Jahr",
                                 value = as.integer(format(Sys.Date(), "%Y")), min = 2020, max = 2100))
        ),
        actionButton("admin_diag_run", "Rohdaten anzeigen", icon = icon("magnifying-glass")),
        tags$div(style = "margin-top:12px;", tableOutput("admin_diag_table")),
        helpText("Zeigt für jede gespeicherte Zelle: aktuelle Position (Zeile/Aufgabentext), Tag, ",
                 "gespeicherter Wert, wer ihn laut Datenbank gesetzt hat, und wann. Hilft zu erkennen, ",
                 "ob eine Zeile durch eine frühere Strukturänderung verschoben wurde.")
    )
  })
  
  observeEvent(input$admin_diag_run, {
    req(rv$authed, identical(rv$role, "admin"))
    did <- input$admin_diag_device
    mo  <- input$admin_diag_month
    yr  <- input$admin_diag_year
    req(did)
    con <- pg_con(); on.exit(dbDisconnect(con), add = TRUE)
    df <- tryCatch(load_device_table(con, did) %||% create_initial_table(did),
                   error = function(e) NULL)
    cs <- tryCatch(load_cell_status(con, did, mo, yr), error = function(e) NULL)
    if (is.null(df) || is.null(cs) || !nrow(cs)) {
      output$admin_diag_table <- renderTable({
        data.frame(Hinweis = "Keine Daten für diese Auswahl gefunden.")
      })
      return()
    }
    cur_hdr <- character(nrow(df))
    h <- ""
    for (i in seq_len(nrow(df))) {
      if (nzchar(df$Header[i])) h <- df$Header[i]
      cur_hdr[i] <- h
    }
    out <- data.frame(
      Zeile      = cs$row_index,
      Abschnitt  = ifelse(cs$row_index >= 1 & cs$row_index <= nrow(df),
                          cur_hdr[pmax(1, pmin(cs$row_index, nrow(df)))], NA),
      Aufgabe    = ifelse(cs$row_index >= 1 & cs$row_index <= nrow(df),
                          df$Task[pmax(1, pmin(cs$row_index, nrow(df)))], NA),
      Tag        = cs$day,
      Wert       = cs$value_text,
      Wer        = cs$updated_by,
      Zuletzt_am = format(as.POSIXct(cs$updated_at, tz = "Europe/Berlin"), "%Y-%m-%d %H:%M"),
      stringsAsFactors = FALSE
    )
    out <- out[order(out$Zeile, out$Tag), , drop = FALSE]
    output$admin_diag_table <- renderTable(out)
  })
  
  output$admin_edit_task_box <- renderUI({
    req(rv$authed)
    if (!identical(rv$role, "admin")) return(NULL)
    con <- pg_con(); on.exit(dbDisconnect(con), add = TRUE)
    devs <- tryCatch(
      DBI::dbGetQuery(con, "SELECT device_id, label FROM devices ORDER BY device_id"),
      error = function(e) data.frame(device_id = character(0), label = character(0))
    )
    devs <- devs[!(devs$device_id %in% RETIRED_DEVICE_IDS), , drop = FALSE]
    if (!nrow(devs)) return(NULL)
    dev_choices <- setNames(devs$device_id,
                            ifelse(nzchar(devs$label %||% ""),
                                   paste0(devs$label, " (", devs$device_id, ")"),
                                   devs$device_id))
    selected_dev <- rv$current_device %||% devs$device_id[1]
    box(width = 12, collapsible = TRUE, collapsed = FALSE,
        title = "Aufgabe bearbeiten (Admin)",
        status = "warning", solidHeader = TRUE,
        fluidRow(
          column(4, selectInput("admin_edit_task_device", "Gerät",
                                choices = dev_choices, selected = selected_dev)),
          column(8, uiOutput("admin_edit_task_picker_ui"))
        ),
        fluidRow(
          column(6, textInput("admin_edit_task_text", "Aufgabentext")),
          column(6, uiOutput("admin_edit_task_header_ui"))
        ),
        actionButton("admin_edit_task_save_btn", "Änderungen speichern",
                     class = "btn btn-warning", icon = icon("save")),
        actionButton("admin_delete_task_btn", "Aufgabe löschen",
                     class = "btn btn-danger", icon = icon("trash"),
                     style = "margin-left:8px;"),
        helpText("Ändert Text und/oder Wartungsabschnitt (Tag/Rhythmus) einer bestehenden Aufgabe. ",
                 "Wird der Abschnitt geändert, verschiebt sich die Aufgabe im Zeitplan; ihre bisherige ",
                 "Historie (Haken/Bemerkungen) wandert automatisch mit an die neue Stelle. ",
                 "Löschen entfernt die Aufgabe und ihre gesamte Historie unwiderruflich.")
    )
  })
  
  output$admin_edit_task_picker_ui <- renderUI({
    req(rv$authed, identical(rv$role, "admin"))
    did <- input$admin_edit_task_device %||% rv$current_device
    req(did)
    con <- pg_con(); on.exit(dbDisconnect(con), add = TRUE)
    df <- tryCatch(load_device_table(con, did) %||% create_initial_table(did),
                   error = function(e) NULL)
    if (is.null(df) || !nrow(df)) {
      return(selectInput("admin_edit_task_picker", "Aufgabe", choices = character(0)))
    }
    cur_hdr <- ""
    labels <- character(0)
    values <- character(0)
    for (i in seq_len(nrow(df))) {
      h <- df$Header[i]
      if (nzchar(h)) { cur_hdr <- h; next }
      t <- df$Task[i]
      if (!nzchar(t)) next
      labels <- c(labels, sprintf("[%s] %s", cur_hdr, t))
      values <- c(values, as.character(i))
    }
    if (!length(values)) {
      return(tagList(
        selectInput("admin_edit_task_picker", "Aufgabe", choices = character(0)),
        helpText("Keine Aufgaben für dieses Gerät gefunden.")
      ))
    }
    selectInput("admin_edit_task_picker", "Aufgabe", choices = setNames(values, labels))
  })
  
  output$admin_edit_task_header_ui <- renderUI({
    req(rv$authed, identical(rv$role, "admin"))
    selectInput("admin_edit_task_header", "Wartungsabschnitt (Tag/Rhythmus)",
                choices = KNOWN_SCHEDULES)
  })
  
  # Pre-fill text + header fields whenever the device or the picked task changes.
  observeEvent(list(input$admin_edit_task_device, input$admin_edit_task_picker), {
    did  <- input$admin_edit_task_device
    ridx <- suppressWarnings(as.integer(input$admin_edit_task_picker))
    req(did, !is.na(ridx))
    con <- pg_con(); on.exit(dbDisconnect(con), add = TRUE)
    df <- tryCatch(load_device_table(con, did) %||% create_initial_table(did),
                   error = function(e) NULL)
    if (is.null(df) || ridx < 1 || ridx > nrow(df)) return()
    updateTextInput(session, "admin_edit_task_text", value = df$Task[ridx] %||% "")
    cur_hdr <- ""
    for (i in seq_len(ridx)) if (nzchar(df$Header[i])) cur_hdr <- df$Header[i]
    updateSelectInput(session, "admin_edit_task_header", selected = cur_hdr)
  }, ignoreInit = TRUE)
  
  observeEvent(input$admin_edit_task_save_btn, {
    req(rv$authed)
    if (!identical(rv$role, "admin")) {
      showNotification("Nur Admins können Aufgaben bearbeiten.", type = "error"); return()
    }
    did      <- input$admin_edit_task_device
    ridx     <- suppressWarnings(as.integer(input$admin_edit_task_picker))
    new_text <- trimws(input$admin_edit_task_text %||% "")
    new_hdr  <- input$admin_edit_task_header
    if (is.null(did) || !nzchar(did) || is.na(ridx)) {
      showNotification("Bitte Gerät und Aufgabe wählen.", type = "error"); return()
    }
    if (!nzchar(new_text)) {
      showNotification("Aufgabentext darf nicht leer sein.", type = "error"); return()
    }
    if (is.null(new_hdr) || !nzchar(new_hdr)) {
      showNotification("Bitte einen Wartungsabschnitt wählen.", type = "error"); return()
    }
    con <- pg_con(); on.exit(dbDisconnect(con), add = TRUE)
    df <- tryCatch(load_device_table(con, did) %||% create_initial_table(did),
                   error = function(e) NULL)
    if (is.null(df) || ridx < 1 || ridx > nrow(df) || nzchar(df$Header[ridx])) {
      showNotification("Aufgabe konnte nicht gefunden werden (Tabelle hat sich evtl. geändert).",
                       type = "error"); return()
    }
    old_hdr <- ""
    for (i in seq_len(ridx)) if (nzchar(df$Header[i])) old_hdr <- df$Header[i]
    df$Task[ridx] <- new_text
    
    if (!identical(old_hdr, new_hdr)) {
      moved   <- move_task_to_header(df, ridx, new_hdr)
      df_new  <- moved$df
      tryCatch({
        remap_row_indices(con, did, moved$remap)
        save_device_table(con, did, df_new, rv$user %||% "<admin>")
        showNotification(sprintf("Aufgabe aktualisiert und nach '%s' verschoben.", new_hdr),
                         type = "message")
        if (identical(did, rv$current_device)) {
          rv$data <- order_cols(df_new)
          rv$table_status <- load_cell_status_current(con, did)
          rv$tasks_refresh <- isolate(rv$tasks_refresh) + 1L
        }
      }, error = function(e) {
        showNotification(paste("Fehler beim Speichern:", conditionMessage(e)), type = "error")
      })
    } else {
      tryCatch({
        save_device_table(con, did, df, rv$user %||% "<admin>")
        showNotification("Aufgabe aktualisiert.", type = "message")
        if (identical(did, rv$current_device)) {
          rv$data <- order_cols(df)
          rv$table_status <- load_cell_status_current(con, did)
          rv$tasks_refresh <- isolate(rv$tasks_refresh) + 1L
        }
      }, error = function(e) {
        showNotification(paste("Fehler beim Speichern:", conditionMessage(e)), type = "error")
      })
    }
  })
  
  # Remove task row `row_idx` entirely, along with its own history in all
  # four history tables, and shift every later row's row_index down by one
  # (via the same safe two-phase remap used for moves) so nothing after it
  # ends up misattributed to the wrong task.
  delete_task_row <- function(df, row_idx) {
    n <- nrow(df)
    if (row_idx < 1 || row_idx > n || nzchar(df$Header[row_idx]))
      return(list(df = df, remap = list()))
    remap <- list()
    if (row_idx < n) {
      for (k in (row_idx + 1L):n) remap[[as.character(k)]] <- k - 1L
    }
    df_new <- df[-row_idx, , drop = FALSE]
    list(df = df_new, remap = remap)
  }
  
  delete_task_history <- function(con, device_id, row_idx) {
    tables <- c("task_status", "device_cell_status", "device_task_remark", "device_task_meta")
    for (tbl in tables) {
      DBI::dbExecute(con, sprintf(
        "DELETE FROM %s WHERE device_id = $1 AND row_index = $2", tbl),
        params = list(device_id, row_idx))
    }
  }
  
  # "Aufgabe löschen": ask for confirmation before permanently removing a task.
  observeEvent(input$admin_delete_task_btn, {
    req(rv$authed)
    if (!identical(rv$role, "admin")) {
      showNotification("Nur Admins können Aufgaben löschen.", type = "error"); return()
    }
    ridx <- suppressWarnings(as.integer(input$admin_edit_task_picker))
    if (is.na(ridx)) {
      showNotification("Bitte eine Aufgabe wählen.", type = "error"); return()
    }
    task_label <- trimws(input$admin_edit_task_text %||% "")
    showModal(modalDialog(
      title = "Aufgabe löschen",
      paste0("Möchten Sie die Aufgabe '", task_label,
             "' wirklich endgültig löschen? Die gesamte Historie ",
             "(Haken/Bemerkungen) dieser Aufgabe geht dabei unwiderruflich verloren."),
      footer = tagList(
        modalButton("Abbrechen"),
        actionButton("admin_delete_task_confirm", "Löschen", class = "btn btn-danger")
      ),
      easyClose = TRUE
    ))
  })
  
  observeEvent(input$admin_delete_task_confirm, {
    req(rv$authed)
    if (!identical(rv$role, "admin")) { removeModal(); return() }
    did  <- input$admin_edit_task_device
    ridx <- suppressWarnings(as.integer(input$admin_edit_task_picker))
    if (is.null(did) || !nzchar(did) || is.na(ridx)) { removeModal(); return() }
    con <- pg_con(); on.exit(dbDisconnect(con), add = TRUE)
    df <- tryCatch(load_device_table(con, did) %||% create_initial_table(did),
                   error = function(e) NULL)
    if (is.null(df) || ridx < 1 || ridx > nrow(df) || nzchar(df$Header[ridx])) {
      removeModal()
      showNotification("Aufgabe konnte nicht gefunden werden (Tabelle hat sich evtl. geändert).",
                       type = "error"); return()
    }
    result <- delete_task_row(df, ridx)
    tryCatch({
      delete_task_history(con, did, ridx)
      remap_row_indices(con, did, result$remap)
      save_device_table(con, did, result$df, rv$user %||% "<admin>")
      removeModal()
      showNotification("Aufgabe gelöscht.", type = "message")
      if (identical(did, rv$current_device)) {
        rv$data <- order_cols(result$df)
        rv$table_status <- load_cell_status_current(con, did)
        rv$tasks_refresh <- isolate(rv$tasks_refresh) + 1L
      }
    }, error = function(e) {
      removeModal()
      showNotification(paste("Fehler beim Löschen:", conditionMessage(e)), type = "error")
    })
  })
  
  
  observeEvent(rv$data, {
    req(rv$data)
    data_rows <- which(rv$data$Header == "")
    # ensure_task_observers will create observers for new rows only
    ensure_task_observers(data_rows)
  }, ignoreNULL = TRUE)
  
  observeEvent(input$mark_today, {
    req(rv$current_device, rv$data)
    today <- as.integer(format(Sys.Date(), "%d"))
    if (today %in% rv$invalid_days) {
      showNotification("Heutiger Tag ist für den ausgewählten Monat ungültig.", type = "error")
      return()
    }
    con <- pg_con(); on.exit(dbDisconnect(con), add = TRUE)
    who <- rv$user_initials %||% rv$user
    data_rows <- which(rv$data$Header == "")
    for (r in data_rows) {
      upsert_cell(con, rv$current_device, r, today, paste0("\u2713 ", who), who)
    }
    rv$table_status <- load_cell_status_current(con, rv$current_device)
    # Sync visible checkboxes/selects so the UI matches the DB without
    # rebuilding the whole task list (which would re-fire every observer).
    for (r in data_rows) {
      updateCheckboxInput(session, paste0("task_done_", r), value = TRUE)
      updateSelectInput(session, paste0("task_opt_", r), selected = "")
    }
  })
  
  # Simple observer to update handsontable when table_status changes (like old code pattern)
  observe({
    req(rv$current_device, rv$data)
    # Re-render when any of these change
    rv$table_status
    rv$remarks_rev   # a Bemerkung was saved -> show it in the grid
    # Selected month/year drive which historical snapshot we show
    sel_m <- suppressWarnings(as.integer(input$month))
    sel_y <- suppressWarnings(as.integer(input$year))
    if (length(sel_m) != 1L || is.na(sel_m)) sel_m <- as.integer(format(Sys.Date(), "%m"))
    if (length(sel_y) != 1L || is.na(sel_y)) sel_y <- as.integer(format(Sys.Date(), "%Y"))
    
    cur_m <- as.integer(format(Sys.Date(), "%m"))
    cur_y <- as.integer(format(Sys.Date(), "%Y"))
    is_current <- (sel_m == cur_m && sel_y == cur_y)
    
    # Refresh invalid-day mask for the selected month/year
    rv$invalid_days <- calc_invalid_days(sel_y, sel_m)
    
    # For the current month use the live rv$table_status; for historical
    # months load a snapshot filtered by month/year so the user sees that
    # month's logs and the comments left by other users.
    view_status <- if (is_current) {
      rv$table_status
    } else {
      tryCatch({
        con <- pg_con(); on.exit(dbDisconnect(con), add = TRUE)
        load_cell_status(con, rv$current_device, sel_m, sel_y)
      }, error = function(e) data.frame())
    }
    
    # Remarks (other users' Bemerkungen) for the selected month
    view_remarks <- tryCatch({
      con2 <- pg_con(); on.exit(dbDisconnect(con2), add = TRUE)
      load_remarks_for_month(con2, rv$current_device, sel_m, sel_y)
    }, error = function(e) data.frame())
    
    output$tableRH <- renderRHandsontable({
      cells_readonly_for_headers(
        rv$data, rv$user_initials, view_status,
        rv$role, rv$invalid_days,
        month = sel_m, year = sel_y,
        read_only_all = !is_current,
        remarks_df = view_remarks
      )
    })
  })
  
  # ---- Nachtrag: Eintraege fuer einen frueheren Tag ergaenzen -------------
  # Rueckmeldung aus dem Labor: "Wie kann ich auf vorherige Tage zugreifen und
  # hier noch etwas nachtragen? Dies scheint nicht zu funktionieren."
  # Bisher liess sich nur ueber das Banner "Ueberfaellige Aufgaben" nachtragen,
  # und das zeigt ausschliesslich den zuletzt faelligen Termin. Die
  # Monatsuebersicht selbst ist reine Anzeige. Dieser Dialog erlaubt es, ein
  # beliebiges Datum der letzten 120 Tage zu waehlen und alle an diesem Tag
  # faelligen Aufgaben nachzutragen. Geschrieben wird mit explizitem
  # Monat/Jahr, damit der Eintrag im richtigen Monat landet; jede Aenderung
  # wird als Nachtrag im Audit-Trail protokolliert.
  NACHTRAG_MAX_DAYS <- 120L
  
  nachtrag_rows_for <- function(d) {
    if (is.null(rv$data) || !nrow(rv$data)) return(integer(0))
    hdrs <- as.character(rv$data$Header)
    tsks <- if ("Task" %in% names(rv$data)) as.character(rv$data$Task) else rep("", length(hdrs))
    cur <- ""
    out <- integer(0)
    for (i in seq_along(hdrs)) {
      if (nzchar(hdrs[i])) {
        if (hdrs[i] %in% SCHEDULE_HEADER_NAMES) cur <- hdrs[i]
        next
      }
      if (!nzchar(trimws(tsks[i]))) next
      if (isTRUE(is_task_relevant_on(cur, d))) out <- c(out, i)
    }
    out
  }
  
  observeEvent(input$open_nachtrag, {
    req(rv$authed, rv$current_device, rv$data)
    showModal(modalDialog(
      title = tagList(icon("clock-rotate-left"), " Nachtrag f\u00fcr einen fr\u00fcheren Tag"),
      size = "l", easyClose = FALSE,
      tags$p(style = "font-size:13px; color:#555;",
             "Datum w\u00e4hlen \u2013 es werden alle Aufgaben angezeigt, die an ",
             "diesem Tag f\u00e4llig waren. Jede \u00c4nderung wird als Nachtrag ",
             "im \u00c4nderungsprotokoll festgehalten."),
      dateInput("nachtrag_date", "Datum:",
                value    = Sys.Date() - 1L,
                min      = Sys.Date() - NACHTRAG_MAX_DAYS,
                max      = Sys.Date(),
                language = "de", weekstart = 1, format = "dd.mm.yyyy"),
      tags$hr(),
      uiOutput("nachtrag_tasks"),
      footer = tagList(
        modalButton("Abbrechen"),
        actionButton("nachtrag_save", "Nachtrag speichern", class = "btn btn-primary")
      )
    ))
  })
  
  output$nachtrag_tasks <- renderUI({
    req(rv$current_device, rv$data)
    d <- input$nachtrag_date
    if (is.null(d) || is.na(as.Date(d)))
      return(tags$p("Bitte ein Datum w\u00e4hlen."))
    d <- as.Date(d)
    if (d > Sys.Date())
      return(tags$p(style = "color:#c62828;", "Zuk\u00fcnftige Tage k\u00f6nnen nicht nachgetragen werden."))
    
    rows <- nachtrag_rows_for(d)
    if (!length(rows))
      return(tags$p(style = "color:#7f4f00;",
                    sprintf("F\u00fcr %s sind f\u00fcr dieses Ger\u00e4t keine Aufgaben f\u00e4llig.",
                            format_de_date(d))))
    
    mm <- as.integer(format(d, "%m")); yy <- as.integer(format(d, "%Y"))
    dd <- as.integer(format(d, "%d"))
    cs <- tryCatch({
      con <- pg_con(); on.exit(dbDisconnect(con), add = TRUE)
      load_cell_status(con, rv$current_device, mm, yy)
    }, error = function(e) data.frame())
    rk <- tryCatch({
      con <- pg_con(); on.exit(dbDisconnect(con), add = TRUE)
      load_remarks_for_date(con, rv$current_device, d)
    }, error = function(e) data.frame())
    
    val_of <- function(r) {
      if (!is.data.frame(cs) || !nrow(cs)) return("")
      s <- cs[cs$day == dd & cs$row_index == r, , drop = FALSE]
      if (nrow(s)) as.character(s$value_text[1]) else ""
    }
    rem_of <- function(r) {
      if (!is.data.frame(rk) || !nrow(rk)) return(list(text = "", opt = ""))
      s <- rk[rk$row_index == r, , drop = FALSE]
      if (!nrow(s)) return(list(text = "", opt = ""))
      list(text = s$remark_text[1] %||% "", opt = s$option_code[1] %||% "")
    }
    
    tagList(
      tags$div(style = "font-weight:700; margin-bottom:8px;",
               icon("calendar-day"), " ", format_de_date(d),
               tags$span(style = "font-weight:400; color:#777; margin-left:8px;",
                         sprintf("(%d Aufgabe(n))", length(rows)))),
      tags$div(
        style = "max-height:46vh; overflow-y:auto; padding-right:6px;",
        lapply(rows, function(r) {
          existing <- val_of(r)
          rmk      <- rem_of(r)
          task_txt <- as.character(rv$data$Task[r])
          done_now <- grepl("^\u2713", trimws(existing))
          tags$div(
            style = sprintf("border-left:4px solid %s; background:%s;
                             padding:8px 10px; margin-bottom:8px; border-radius:4px;",
                            if (nzchar(existing)) "#2e7d32" else "#f57c00",
                            if (nzchar(existing)) "#f1f8f2" else "#fff8f0"),
            tags$div(style = "font-size:13px; font-weight:600; margin-bottom:6px;", task_txt),
            if (nzchar(existing))
              tags$div(style = "font-size:11px; color:#2e7d32; margin-bottom:6px;",
                       icon("circle-check"), " Bereits eingetragen: ", existing),
            fluidRow(
              column(3, checkboxInput(paste0("nach_done_", r), "Erledigt", value = done_now)),
              column(4, selectInput(paste0("nach_opt_", r), label = NULL,
                                    choices = task_reason_choices(),
                                    selected = "", selectize = FALSE)),
              column(5, textInput(paste0("nach_rem_", r), label = NULL,
                                  value = rmk$text,
                                  placeholder = "Kommentar zur Erledigung bzw. Bemerkung zum Grund ..."))
            )
          )
        })
      )
    )
  })
  
  observeEvent(input$nachtrag_save, {
    req(rv$authed, rv$current_device, rv$data)
    d <- input$nachtrag_date
    if (is.null(d) || is.na(as.Date(d))) {
      showNotification("Bitte ein Datum w\u00e4hlen.", type = "error"); return()
    }
    d <- as.Date(d)
    if (d > Sys.Date() || d < Sys.Date() - NACHTRAG_MAX_DAYS) {
      showNotification("Datum liegt ausserhalb des zul\u00e4ssigen Nachtragszeitraums.",
                       type = "error"); return()
    }
    rows <- nachtrag_rows_for(d)
    if (!length(rows)) { removeModal(); return() }
    
    who      <- rv$user_initials %||% rv$user
    is_admin <- identical(rv$role, "admin")
    mm <- as.integer(format(d, "%m")); yy <- as.integer(format(d, "%Y"))
    dd <- as.integer(format(d, "%d"))
    n_changed <- 0L
    
    tryCatch({
      con <- pg_con(); on.exit(dbDisconnect(con), add = TRUE)
      for (r in rows) {
        done <- isTRUE(input[[paste0("nach_done_", r)]])
        opt  <- input[[paste0("nach_opt_", r)]] %||% ""
        rem  <- trimws(input[[paste0("nach_rem_", r)]] %||% "")
        prev <- tryCatch(read_cell_value(con, rv$current_device, r, dd, mm, yy),
                         error = function(e) "")
        prev <- prev %||% ""
        
        new_val <- if (nzchar(opt)) {
          sub("^([A-Za-z.\u00f8\u00d8]+).*", "\\1", opt)
        } else if (done) {
          paste0("\u2713 (", who, ")")
        } else NA_character_
        
        if (!is.na(new_val) && !identical(new_val, prev)) {
          upsert_cell(con, rv$current_device, r, dd, new_val, who,
                      month = mm, year = yy)
          log_event(con, "nachtrag.eintrag", entity_type = "zelle",
                    device_id = rv$current_device,
                    task_name = audit_task_name(con, rv$current_device, r),
                    ref_date = as.character(d), row_index = r,
                    old_value = prev, new_value = new_val,
                    details = sprintf("Nachtrag f\u00fcr %s (erfasst am %s)",
                                      format(d, "%d.%m.%Y"), format(Sys.Date(), "%d.%m.%Y")),
                    who = who, role = if (is_admin) "admin" else "user")
          n_changed <- n_changed + 1L
        }
        
        if (nzchar(rem) || nzchar(opt)) {
          upsert_remark(con, rv$current_device, r, d, rem,
                        if (nzchar(opt)) sub("^([A-Za-z.\u00f8\u00d8]+).*", "\\1", opt) else NA_character_,
                        who)
        }
      }
      rv$table_status  <- load_cell_status_current(con, rv$current_device)
      rv$tasks_refresh <- isolate(rv$tasks_refresh) + 1L
    }, error = function(e) {
      showNotification(paste("Nachtrag fehlgeschlagen:", conditionMessage(e)),
                       type = "error", duration = 8)
    })
    
    removeModal()
    showNotification(sprintf("Nachtrag f\u00fcr %s gespeichert (%d \u00c4nderung(en)).",
                             format(d, "%d.%m.%Y"), n_changed),
                     type = "message", duration = 5)
  })
  
  output$download_table_csv <- downloadHandler(
    filename = function() {
      dev <- rv$current_device %||% "geraet"
      paste0("Wartungsplan_", dev, "_", format(Sys.Date(), "%Y%m%d_%H%M%S"), ".csv")
    },
    content = function(file) {
      req(rv$data)
      con <- pg_con(); on.exit(dbDisconnect(con), add = TRUE)
      sel_m <- suppressWarnings(as.integer(input$month))
      sel_y <- suppressWarnings(as.integer(input$year))
      if (length(sel_m) != 1L || is.na(sel_m)) sel_m <- as.integer(format(Sys.Date(), "%m"))
      if (length(sel_y) != 1L || is.na(sel_y)) sel_y <- as.integer(format(Sys.Date(), "%Y"))
      over <- build_overlayed_table(rv$data,
                                    load_cell_status(con, rv$current_device, sel_m, sel_y))
      write.csv(over, file, row.names = FALSE, fileEncoding = "UTF-8")
    },
    contentType = "text/csv"
  )
  
  # ---- PDF-Export: Monatsplan ------------------------------------------------
  # Erzeugt das PDF direkt mit cairo_pdf (Base-R-Grafik). Es werden weder
  # LaTeX/xelatex noch eine .Rmd-Vorlage benoetigt -- genau daran ist der
  # bisherige Export auf dem Server gescheitert.
  output$download_table_pdf <- downloadHandler(
    filename = function() {
      dev <- rv$current_device %||% "geraet"
      paste0("Wartungsplan_", dev, "_", format(Sys.Date(), "%Y%m%d_%H%M%S"), ".pdf")
    },
    content = function(file) {
      req(rv$data, rv$current_device)
      showNotification("PDF wird erstellt \u2026", id = "pdf_generation",
                       duration = NULL, type = "message")
      on.exit(removeNotification(id = "pdf_generation"), add = TRUE)
      con <- pg_con(); on.exit(dbDisconnect(con), add = TRUE)

      sel_month <- suppressWarnings(as.integer(input$month))
      sel_year  <- suppressWarnings(as.integer(input$year))
      if (length(sel_month) != 1L || is.na(sel_month))
        sel_month <- as.integer(format(Sys.Date(), "%m"))
      if (length(sel_year) != 1L || is.na(sel_year))
        sel_year <- as.integer(format(Sys.Date(), "%Y"))

      cell_status <- load_cell_status(con, rv$current_device, sel_month, sel_year)
      pdf_df <- build_overlayed_table(rv$data, cell_status)

      device_info <- tryCatch(
        DBI::dbGetQuery(con, "SELECT label FROM devices WHERE device_id = $1",
                        params = list(rv$current_device)),
        error = function(e) data.frame(label = character(0)))
      device_label <- if (nrow(device_info) > 0) device_info$label[1] else rv$current_device

      serials <- tryCatch(load_device_serials(con, rv$current_device),
                          error = function(e) list())
      layout  <- tryCatch(load_device_layout(con, rv$current_device),
                          error = function(e) list(footer_text = NULL, version = NULL))
      footer_text <- if (!is.null(layout$footer_text) && nzchar(layout$footer_text))
        layout$footer_text else ""
      version_str <- if (!is.null(layout$version) && nzchar(layout$version))
        layout$version else ""

      month_str <- sprintf("%s %d", unname(.DE_MONTHS[sel_month]), sel_year)

      ok <- tryCatch({
        render_wartungsplan_pdf(
          file = file, table_data = pdf_df,
          device_label = device_label, month_str = month_str,
          report_date = format(Sys.Date(), "%d.%m.%Y"),
          serials = serials, footer_text = footer_text,
          version_str = version_str,
          sel_month = sel_month, sel_year = sel_year,
          option_meanings = OPTION_MEANINGS,
          initials_df = cell_status
        )
        file.exists(file) && file.info(file)$size > 0
      }, error = function(e) {
        message("PDF-Fehler: ", conditionMessage(e)); FALSE
      })

      if (isTRUE(ok)) {
        showNotification("PDF erfolgreich erstellt.", type = "message", duration = 5)
        tryCatch(log_event(con, "export.pdf", entity_type = "monatsplan",
                           entity_id = month_str, device_id = rv$current_device,
                           new_value = basename(file),
                           who = rv$user, role = rv$role),
                 error = function(e) NULL)
      } else {
        showNotification(
          paste0("PDF konnte nicht erstellt werden. Bitte einen Administrator ",
                 "informieren (Details stehen im Server-Log)."),
          type = "error", duration = 15)
      }
    },
    contentType = "application/pdf"
  )
  
  # ---- Admin: download the EMPTY maintenance plan (current structure) -------
  # Produces the same PDF as the normal export but with all day cells blank,
  # so it's a fresh printable checklist. Because the table comes from the
  # live device_tables structure (build_overlayed_table with NO cell status),
  # it automatically reflects any admin add/edit/delete of tasks -- no
  # separate template to keep in sync.
  output$download_empty_plan_pdf <- downloadHandler(
    filename = function() {
      dev <- input$empty_plan_device %||% rv$current_device %||% "geraet"
      paste0("Wartungsplan_LEER_", dev, "_", format(Sys.Date(), "%Y%m%d"), ".pdf")
    },
    content = function(file) {
      did <- input$empty_plan_device %||% rv$current_device
      if (is.null(did) || !nzchar(did)) {
        showNotification("Bitte zuerst ein Ger\u00e4t w\u00e4hlen.", type = "error")
        return()
      }
      showNotification("Leeres Wartungsplan-PDF wird erstellt \u2026",
                       id = "empty_pdf_generation", duration = NULL, type = "message")
      on.exit(removeNotification(id = "empty_pdf_generation"), add = TRUE)
      con <- pg_con(); on.exit(dbDisconnect(con), add = TRUE)

      # Aktuelle Struktur des Geraets (enthaelt Admin-Aenderungen), ohne
      # Eintraege -> alle Tagesfelder bleiben leer.
      df_struct <- tryCatch(load_device_table(con, did) %||% create_initial_table(did),
                            error = function(e) create_initial_table(did))
      empty_df <- build_overlayed_table(df_struct, data.frame(
        row_index = integer(0), day = integer(0), value_text = character(0)
      ))

      device_info <- tryCatch(
        DBI::dbGetQuery(con, "SELECT label FROM devices WHERE device_id = $1",
                        params = list(did)),
        error = function(e) data.frame(label = character(0)))
      device_label <- if (nrow(device_info) > 0) device_info$label[1] else did

      serials <- tryCatch(load_device_serials(con, did), error = function(e) list())
      layout  <- tryCatch(load_device_layout(con, did),
                          error = function(e) list(footer_text = NULL, version = NULL))
      footer_text <- if (!is.null(layout$footer_text) && nzchar(layout$footer_text))
        layout$footer_text else ""
      version_str <- if (!is.null(layout$version) && nzchar(layout$version))
        layout$version else ""

      sel_month <- suppressWarnings(as.integer(input$empty_plan_month))
      sel_year  <- suppressWarnings(as.integer(input$empty_plan_year))
      if (length(sel_month) != 1L || is.na(sel_month))
        sel_month <- as.integer(format(Sys.Date(), "%m"))
      if (length(sel_year) != 1L || is.na(sel_year))
        sel_year <- as.integer(format(Sys.Date(), "%Y"))
      month_str <- sprintf("%s %d", unname(.DE_MONTHS[sel_month]), sel_year)

      ok <- tryCatch({
        render_wartungsplan_pdf(
          file = file, table_data = empty_df,
          device_label = device_label, month_str = month_str,
          report_date = format(Sys.Date(), "%d.%m.%Y"),
          serials = serials, footer_text = footer_text,
          version_str = version_str,
          sel_month = sel_month, sel_year = sel_year,
          option_meanings = OPTION_MEANINGS
        )
        file.exists(file) && file.info(file)$size > 0
      }, error = function(e) {
        message("PDF-Fehler (leerer Plan): ", conditionMessage(e)); FALSE
      })

      if (isTRUE(ok))
        showNotification("Leeres Wartungsplan-PDF erstellt.",
                         type = "message", duration = 5)
      else
        showNotification("PDF konnte nicht erstellt werden.",
                         type = "error", duration = 15)
    },
    contentType = "application/pdf"
  )
  
  # UI for the empty-plan download (admin-only), shown on its own sidebar tab.
  output$empty_plan_box <- renderUI({
    req(rv$authed)
    if (!identical(rv$role, "admin")) {
      return(tags$p("Nur für Administratoren."))
    }
    con <- pg_con(); on.exit(dbDisconnect(con), add = TRUE)
    devs <- tryCatch(
      DBI::dbGetQuery(con, "SELECT device_id, label FROM devices ORDER BY device_id"),
      error = function(e) data.frame(device_id = character(0), label = character(0)))
    devs <- devs[!(devs$device_id %in% RETIRED_DEVICE_IDS), , drop = FALSE]
    if (!nrow(devs)) return(tags$p("Keine Geräte gefunden."))
    dev_choices <- setNames(devs$device_id,
                            ifelse(nzchar(devs$label %||% ""),
                                   paste0(devs$label, " (", devs$device_id, ")"),
                                   devs$device_id))
    box(width = 12, title = "Ungefülltes Wartungsplan herunterladen (Admin)",
        status = "primary", solidHeader = TRUE,
        selectInput("empty_plan_device", "Gerät",
                    choices = dev_choices,
                    selected = rv$current_device %||% devs$device_id[1]),
        fluidRow(
          column(6, selectInput("empty_plan_month", "Monat",
                                choices = setNames(1:12, unname(.DE_MONTHS)),
                                selected = as.integer(format(Sys.Date(), "%m")))),
          column(6, selectInput("empty_plan_year", "Jahr",
                                choices = seq(as.integer(format(Sys.Date(), "%Y")) - 1,
                                              as.integer(format(Sys.Date(), "%Y")) + 2),
                                selected = as.integer(format(Sys.Date(), "%Y"))))
        ),
        downloadButton("download_empty_plan_pdf", "Leeres Wartungsplan-PDF herunterladen",
                       class = "btn btn-primary"),
        helpText("Erzeugt einen leeren, druckbaren Wartungsplan mit der aktuellen ",
                 "Aufgabenstruktur des gewählten Geräts. Änderungen an Aufgaben ",
                 "(Hinzufügen/Bearbeiten/Löschen) werden automatisch berücksichtigt.")
    )
  })
  
  # ---- Feedback: submission form (all users) --------------------------------
  output$feedback_form_box <- renderUI({
    req(rv$authed)
    con <- pg_con(); on.exit(dbDisconnect(con), add = TRUE)
    devs <- tryCatch(
      DBI::dbGetQuery(con, "SELECT device_id, label FROM devices ORDER BY device_id"),
      error = function(e) data.frame(device_id = character(0), label = character(0)))
    devs <- devs[!(devs$device_id %in% RETIRED_DEVICE_IDS), , drop = FALSE]
    dev_choices <- c("(Allgemein / kein bestimmtes Gerät)" = "")
    if (nrow(devs)) dev_choices <- c(dev_choices,
                                     setNames(devs$device_id, ifelse(nzchar(devs$label), paste0(devs$label, " (", devs$device_id, ")"), devs$device_id)))
    box(width = 12, title = "Ihr Feedback oder Problem", status = "primary", solidHeader = TRUE,
        tags$p(style = "color:#555;",
               "Teilen Sie uns Fehler, Verbesserungsvorschläge oder Probleme im ",
               "Wartungsplan mit. Ihre Meldung wird an die Administratoren weitergeleitet."),
        selectInput("feedback_device", "Betrifft Gerät (optional)", choices = dev_choices),
        selectInput("feedback_category", "Art der Meldung",
                    choices = c("Problem / Fehler" = "Problem",
                                "Verbesserungsvorschlag" = "Vorschlag",
                                "Frage" = "Frage",
                                "Sonstiges" = "Sonstiges")),
        textAreaInput("feedback_message", "Ihre Nachricht", rows = 5,
                      placeholder = "Beschreiben Sie hier Ihr Anliegen..."),
        actionButton("feedback_submit", "Absenden", class = "btn btn-primary", icon = icon("paper-plane"))
    )
  })
  
  observeEvent(input$feedback_submit, {
    req(rv$authed)
    msg <- trimws(input$feedback_message %||% "")
    if (!nzchar(msg)) {
      showNotification("Bitte geben Sie eine Nachricht ein.", type = "error"); return()
    }
    con <- pg_con(); on.exit(dbDisconnect(con), add = TRUE)
    ok <- tryCatch({
      dbExecute(con, "
        INSERT INTO app_feedback (created_by, device_id, category, message)
        VALUES ($1, $2, $3, $4)
      ", params = list(rv$user %||% "unbekannt",
                       input$feedback_device %||% "",
                       input$feedback_category %||% "Sonstiges",
                       msg))
      TRUE
    }, error = function(e) FALSE)
    if (isTRUE(ok)) {
      updateTextAreaInput(session, "feedback_message", value = "")
      showNotification("Vielen Dank! Ihr Feedback wurde übermittelt.",
                       type = "message", duration = 6)
    } else {
      showNotification("Feedback konnte nicht gespeichert werden.", type = "error")
    }
  })
  
  # ---- Änderungsprotokoll / Audit-Trail (Admin) ------------------------------
  # Read-only viewer over app_audit_log with filters and CSV export. The table
  # is append-only -- there is deliberately no delete button, because an audit
  # trail that can be edited is worthless for an accreditation audit.
  audit_action_labels <- c(
    "anmeldung"                = "Anmeldung",
    "anmeldung.fehlgeschlagen" = "Anmeldung fehlgeschlagen",
    "abmeldung"                = "Abmeldung",
    "eintrag.neu"              = "Eintrag erstellt",
    "eintrag.geaendert"        = "Eintrag ge\u00e4ndert",
    "eintrag.geloescht"        = "Eintrag gel\u00f6scht",
    "eintrag.bestand"          = "Eintrag (Bestand)",
    "bemerkung.neu"            = "Bemerkung erstellt",
    "bemerkung.geaendert"      = "Bemerkung ge\u00e4ndert",
    "bemerkung.geloescht"      = "Bemerkung gel\u00f6scht",
    "bemerkung.bestand"        = "Bemerkung (Bestand)",
    "nachtrag.eintrag"         = "Nachtrag (fr\u00fcherer Tag)",
    "version.veroeffentlicht"  = "Neue Programmversion",
    "bestand.uebernommen"      = "Datenübernahme"
  )
  # Icon + colour per action, so the timeline is scannable at a glance.
  audit_style <- function(a) {
    switch(as.character(a),
      "anmeldung"                = list(i = "right-to-bracket", c = "sl-info"),
      "anmeldung.fehlgeschlagen" = list(i = "ban",              c = "sl-danger"),
      "abmeldung"                = list(i = "right-from-bracket", c = "sl-muted"),
      "eintrag.neu"              = list(i = "circle-plus",      c = "sl-ok"),
      "eintrag.geaendert"        = list(i = "pen",              c = "sl-warn"),
      "eintrag.geloescht"        = list(i = "trash",            c = "sl-danger"),
      "eintrag.bestand"          = list(i = "box-archive",      c = "sl-muted"),
      "bemerkung.neu"            = list(i = "comment-medical",  c = "sl-ok"),
      "bemerkung.geaendert"      = list(i = "comment-dots",     c = "sl-warn"),
      "bemerkung.geloescht"      = list(i = "comment-slash",    c = "sl-danger"),
      "bemerkung.bestand"        = list(i = "box-archive",      c = "sl-muted"),
      "nachtrag.eintrag"         = list(i = "clock-rotate-left", c = "sl-warn"),
      "version.veroeffentlicht"  = list(i = "tag",              c = "sl-version"),
      "bestand.uebernommen"      = list(i = "database",         c = "sl-muted"),
      list(i = "circle-dot", c = "sl-muted"))
  }
  audit_label <- function(a) {
    a <- as.character(a)
    lab <- unname(audit_action_labels[a])
    ifelse(is.na(lab), a, lab)
  }
  # "vor 3 Stunden" style relative time, like a commit list.
  audit_relative <- function(ts) {
    mins <- as.numeric(difftime(Sys.time(), ts, units = "mins"))
    vapply(mins, function(m) {
      if (is.na(m))     return("")
      if (m < 1)        return("gerade eben")
      if (m < 60)       return(sprintf("vor %d Min.", round(m)))
      if (m < 1440)     return(sprintf("vor %d Std.", round(m / 60)))
      if (m < 43200) {
        d <- round(m / 1440)
        return(if (d <= 1) "gestern" else sprintf("vor %d Tagen", d))
      }
      mo <- round(m / 43200)
      if (mo <= 1) "vor einem Monat" else sprintf("vor %d Monaten", mo)
    }, character(1))
  }
  
  # Filtered result set, shared by the table and the CSV download.
  audit_rows <- reactive({
    req(rv$authed, identical(rv$role, "admin"))
    rv$audit_refresh
    rng  <- input$audit_range
    from <- if (!is.null(rng)) as.Date(rng[1]) else Sys.Date() - 30
    to   <- if (!is.null(rng) && length(rng) > 1) as.Date(rng[2]) else Sys.Date()
    usr  <- trimws(input$audit_user %||% "")
    dev  <- input$audit_device %||% "__all__"
    act  <- input$audit_action %||% "__all__"
    txt  <- trimws(input$audit_search %||% "")
    lim  <- suppressWarnings(as.integer(input$audit_limit %||% 500))
    if (is.na(lim)) lim <- 500L
    con <- pg_con(); on.exit(dbDisconnect(con), add = TRUE)
    tryCatch(
      DBI::dbGetQuery(con, "
        SELECT a.ts, a.username, a.user_role, a.action, a.entity_type,
               a.entity_id, a.device_id, COALESCE(d.label, a.device_id) AS device_label,
               a.task_name, a.ref_date, a.row_index,
               a.old_value, a.new_value, a.details, a.app_version,
               a.session_id, a.client_ip
          FROM app_audit_log a
          LEFT JOIN devices d ON d.device_id = a.device_id
         WHERE a.ts >= $1::date AND a.ts < ($2::date + INTERVAL '1 day')
           AND ($3::text = '' OR a.username ILIKE '%' || $3::text || '%')
           AND ($4::text = '__all__' OR a.device_id = $4::text)
           AND ($5::text = '__all__' OR a.action   = $5::text)
           AND ($6::text = '' OR a.task_name ILIKE '%' || $6::text || '%'
                             OR a.entity_id ILIKE '%' || $6::text || '%'
                             OR a.new_value ILIKE '%' || $6::text || '%'
                             OR a.old_value ILIKE '%' || $6::text || '%'
                             OR a.details   ILIKE '%' || $6::text || '%')
         ORDER BY a.ts DESC
         LIMIT $7
      ", params = list(as.character(from), as.character(to), usr, dev, act,
                       txt, lim)),
      error = function(e) NULL)
  })
  
  output$audit_panel <- renderUI({
    req(rv$authed)
    if (!identical(rv$role, "admin"))
      return(tags$p("Nur f\u00fcr Administratoren."))
    con <- pg_con(); on.exit(dbDisconnect(con), add = TRUE)
    devs <- tryCatch(DBI::dbGetQuery(con,
                                     "SELECT device_id, label FROM devices ORDER BY device_id"),
                     error = function(e) NULL)
    dev_choices <- c("Alle Ger\u00e4te" = "__all__")
    if (!is.null(devs) && nrow(devs))
      dev_choices <- c(dev_choices,
                       setNames(devs$device_id,
                                paste0(devs$label, " (", devs$device_id, ")")))
    act_choices <- c("Alle Aktionen" = "__all__",
                     setNames(names(audit_action_labels),
                              unname(audit_action_labels)))
    tagList(
      tags$div(class = "audit-hint",
               icon("lock"),
               tags$b(" Revisionssicher: "),
               paste0("Dieses Protokoll wird nur erg\u00e4nzt. Eintr\u00e4ge k\u00f6nnen ",
                      "weder ge\u00e4ndert noch gel\u00f6scht werden. Eintr\u00e4ge mit der ",
                      "Kennzeichnung \u201eBestand\u201c stammen aus der Zeit vor ",
                      "Einf\u00fchrung des Protokolls und wurden einmalig \u00fcbernommen.")),
      uiOutput("audit_stats"),
      box(width = 12, status = "primary", solidHeader = FALSE,
          fluidRow(
            column(3, dateRangeInput("audit_range", "Zeitraum",
                                     start = Sys.Date() - 30, end = Sys.Date(),
                                     format = "dd.mm.yyyy", separator = " bis ",
                                     language = "de", weekstart = 1)),
            column(2, textInput("audit_user", "Benutzer", placeholder = "z. B. jure")),
            column(3, selectInput("audit_device", "Ger\u00e4t", choices = dev_choices)),
            column(2, selectInput("audit_action", "Aktion", choices = act_choices)),
            column(2, textInput("audit_search", "Volltextsuche",
                                placeholder = "Aufgabe, Wert \u2026"))
          ),
          fluidRow(
            column(12,
                   tags$div(style = "display:flex; align-items:center; gap:10px; flex-wrap:wrap;",
                            radioButtons("audit_view", NULL,
                                         choices = c("Verlauf" = "timeline",
                                                     "Tabelle" = "table"),
                                         selected = "timeline", inline = TRUE),
                            selectInput("audit_limit", NULL,
                                        choices = c("200 Eintr\u00e4ge" = 200,
                                                    "500 Eintr\u00e4ge" = 500,
                                                    "2.000 Eintr\u00e4ge" = 2000,
                                                    "10.000 Eintr\u00e4ge" = 10000),
                                        selected = 500, width = "160px"),
                            actionButton("audit_reload",
                                         label = tagList(icon("rotate"), " Aktualisieren"),
                                         class = "btn btn-default"),
                            downloadButton("audit_csv", "CSV-Export",
                                           class = "btn btn-primary"),
                            tags$span(style = "color:#6c757d; font-size:13px;",
                                      textOutput("audit_count", inline = TRUE))
                   )
            )
          )
      ),
      box(width = 12, title = "Protokolleintr\u00e4ge", status = "primary",
          solidHeader = TRUE,
          conditionalPanel("input.audit_view == 'timeline'",
                           uiOutput("audit_timeline")),
          conditionalPanel("input.audit_view == 'table'",
                           if (HAS_DT) DT::dataTableOutput("audit_table")
                           else uiOutput("audit_table_plain")))
    )
  })
  
  observeEvent(input$audit_reload, { rv$audit_refresh <- isolate(rv$audit_refresh) + 1L })
  
  # Small KPI strip above the filters: activity at a glance.
  output$audit_stats <- renderUI({
    d <- audit_rows()
    if (is.null(d) || !nrow(d)) return(NULL)
    is_change <- grepl("^(eintrag|bemerkung)\\.(neu|geaendert|geloescht)$", d$action)
    chip <- function(ico, num, lbl) tags$div(class = "audit-chip",
                                             icon(ico), tags$b(num), tags$span(lbl))
    tags$div(class = "audit-chips",
             chip("list", nrow(d), "Eintr\u00e4ge"),
             chip("pen-to-square", sum(is_change), "\u00c4nderungen"),
             chip("users", length(unique(na.omit(d$username[nzchar(d$username %||% "")]))),
                  "Personen"),
             chip("microchip", length(unique(na.omit(d$device_id[!is.na(d$device_id)]))),
                  "Ger\u00e4te"),
             chip("triangle-exclamation",
                  sum(d$action == "anmeldung.fehlgeschlagen", na.rm = TRUE),
                  "fehlgeschl. Anmeldungen"))
  })
  
  output$audit_count <- renderText({
    d <- audit_rows()
    if (is.null(d)) return("Protokoll nicht lesbar.")
    sprintf("%d Eintr\u00e4ge angezeigt.", nrow(d))
  })
  
  # ---- Verlaufsansicht (GitHub-artige Commit-Liste) --------------------------
  output$audit_timeline <- renderUI({
    d <- audit_rows()
    if (is.null(d)) return(tags$p("Protokoll nicht lesbar."))
    if (!nrow(d))  return(tags$p("Keine Eintr\u00e4ge f\u00fcr die gew\u00e4hlten Filter."))
    nn  <- function(x) { x <- as.character(x); ifelse(is.na(x), "", x) }
    ts  <- as.POSIXct(d$ts)
    rel <- audit_relative(ts)
    day <- format(ts, "%Y-%m-%d")
    
    rows <- lapply(seq_len(nrow(d)), function(i) {
      st  <- audit_style(d$action[i])
      who <- nn(d$username[i]); if (!nzchar(who)) who <- "\u2013"
      ini <- toupper(substr(who, 1, 2))
      ov  <- nn(d$old_value[i]); nv <- nn(d$new_value[i])
      
      # Headline: the task name if we have one, otherwise the generic object id.
      headline <- nn(d$task_name[i])
      if (!nzchar(headline)) headline <- nn(d$entity_id[i])
      if (!nzchar(headline)) headline <- audit_label(d$action[i])
      
      # Release entries render their bullet list instead of an old->new diff.
      is_release <- identical(as.character(d$action[i]), "version.veroeffentlicht")
      
      meta <- tagList(
        tags$span(class = "au-badge", audit_label(d$action[i])),
        if (nzchar(nn(d$device_label[i])))
          tags$span(class = "au-chip", icon("microchip"), " ", nn(d$device_label[i])),
        if (nzchar(nn(d$ref_date[i])))
          tags$span(class = "au-chip", icon("calendar-day"), " ",
                    format(as.Date(d$ref_date[i]), "%d.%m.%Y")),
        if (!is.na(d$row_index[i]))
          tags$span(class = "au-chip", "Zeile ", d$row_index[i]),
        if (nzchar(nn(d$app_version[i])) && !is_release)
          tags$span(class = "au-chip", "v", nn(d$app_version[i]))
      )
      
      body <- if (is_release) {
        tags$ul(class = "au-release",
                lapply(strsplit(nn(d$details[i]), "\n", fixed = TRUE)[[1]],
                       function(l) if (nzchar(trimws(l))) tags$li(l)))
      } else if (nzchar(ov) || nzchar(nv)) {
        tags$div(class = "au-diff",
                 tags$span(class = "au-old", if (nzchar(ov)) ov else "(leer)"),
                 tags$span(class = "au-arrow", HTML("&rarr;")),
                 tags$span(class = "au-new", if (nzchar(nv)) nv else "(leer)"))
      } else NULL
      
      tags$li(class = "au-item",
        tags$div(class = paste("au-dot", st$c), icon(st$i)),
        tags$div(class = "au-body",
          tags$div(class = "au-head",
                   tags$span(class = "au-avatar", ini),
                   tags$b(who),
                   tags$span(class = "au-title", headline),
                   tags$span(class = "au-time",
                             title = format(ts[i], "%d.%m.%Y %H:%M:%S"),
                             rel[i])),
          tags$div(class = "au-meta", meta),
          body,
          if (nzchar(nn(d$details[i])) && !is_release)
            tags$div(class = "au-details", icon("circle-info"), " ", nn(d$details[i]))
        )
      )
    })
    
    # Group by calendar day, like a commit history.
    out <- list()
    for (dd in unique(day)) {
      idx <- which(day == dd)
      out <- c(out, list(
        tags$div(class = "au-daysep",
                 icon("calendar"), " ",
                 to_german_date_str(format(as.Date(dd), "%A, %d.%m.%Y")),
                 tags$span(class = "au-daycount",
                           sprintf("%d Eintr\u00e4ge", length(idx)))),
        tags$ul(class = "au-list", rows[idx])))
    }
    tags$div(class = "audit-timeline", out)
  })
  
  # Shared display formatting for both the DT viewer and the CSV export.
  audit_display <- function(d) {
    nn <- function(x) { x <- as.character(x); ifelse(is.na(x), "", x) }
    out <- data.frame(
      Zeitpunkt = format(as.POSIXct(d$ts), "%d.%m.%Y %H:%M:%S"),
      Benutzer  = nn(d$username),
      Rolle     = nn(d$user_role),
      Aktion    = audit_label(d$action),
      Geraet    = nn(d$device_label),
      Aufgabe   = nn(d$task_name),
      Datum     = ifelse(is.na(d$ref_date), "",
                         format(as.Date(d$ref_date), "%d.%m.%Y")),
      Zeile     = nn(d$row_index),
      Vorher    = nn(d$old_value),
      Nachher   = nn(d$new_value),
      Details   = gsub("\n", " / ", nn(d$details), fixed = TRUE),
      Version   = nn(d$app_version),
      check.names = FALSE, stringsAsFactors = FALSE
    )
    names(out)[names(out) == "Geraet"] <- "Ger\u00e4t"
    out
  }
  
  if (HAS_DT) {
    output$audit_table <- DT::renderDataTable({
      d <- audit_rows()
      if (is.null(d) || !nrow(d))
        return(DT::datatable(
          data.frame(Hinweis = "Keine Eintr\u00e4ge im gew\u00e4hlten Zeitraum."),
          rownames = FALSE, options = list(dom = "t")))
      DT::datatable(audit_display(d), rownames = FALSE, filter = "top",
                    options = list(pageLength = 25, order = list(),
                                   scrollX = TRUE))
    })
  } else {
    output$audit_table_plain <- renderUI({
      d <- audit_rows()
      if (is.null(d) || !nrow(d))
        return(tags$p("Keine Eintr\u00e4ge im gew\u00e4hlten Zeitraum."))
      out <- utils::head(audit_display(d), 500)
      tags$div(style = "overflow-x:auto;",
               tags$table(class = "table table-striped table-condensed",
                          tags$thead(tags$tr(lapply(names(out), tags$th))),
                          tags$tbody(lapply(seq_len(nrow(out)), function(i)
                            tags$tr(lapply(out[i, ], function(v) tags$td(v)))))),
               tags$p(style = "color:#6c757d; font-size:12px;",
                      "Anzeige auf 500 Zeilen begrenzt \u2013 vollst\u00e4ndige ",
                      "Daten per CSV-Export."))
    })
  }
  
  output$audit_csv <- downloadHandler(
    filename = function()
      sprintf("Aenderungsprotokoll_%s.csv", format(Sys.time(), "%Y-%m-%d_%H%M")),
    content = function(file) {
      d <- audit_rows()
      out <- if (is.null(d) || !nrow(d)) data.frame() else audit_display(d)
      # UTF-8 BOM so Excel on Windows shows umlauts correctly; semicolon
      # separator matches the German Excel default.
      con_f <- file(file, open = "wb")
      on.exit(close(con_f), add = TRUE)
      writeBin(charToRaw("\ufeff"), con_f)
      writeBin(charToRaw(paste0(
        paste(utils::capture.output(
          utils::write.table(out, stdout(), sep = ";", row.names = FALSE,
                             qmethod = "double")),
          collapse = "\r\n"), "\r\n")), con_f)
    }
  )
  
  # ---- Feedback: admin inbox ------------------------------------------------
  output$feedback_admin_box <- renderUI({
    req(rv$authed)
    if (!identical(rv$role, "admin")) return(tags$p("Nur für Administratoren."))
    rv$feedback_refresh  # dependency so the list updates after resolve/delete
    con <- pg_con(); on.exit(dbDisconnect(con), add = TRUE)
    fb <- tryCatch(
      DBI::dbGetQuery(con, "SELECT id, created_at, created_by, device_id, category, message, resolved
                            FROM app_feedback ORDER BY resolved ASC, created_at DESC"),
      error = function(e) NULL)
    if (is.null(fb) || !nrow(fb)) return(tags$p("Noch keine Rückmeldungen eingegangen."))
    label_of <- tryCatch({
      d <- DBI::dbGetQuery(con, "SELECT device_id, label FROM devices")
      setNames(d$label, d$device_id)
    }, error = function(e) c())
    
    rows_ui <- lapply(seq_len(nrow(fb)), function(i) {
      r <- fb[i, ]
      dev_txt <- if (nzchar(r$device_id %||% ""))
        sprintf("%s (%s)", label_of[[r$device_id]] %||% r$device_id, r$device_id) else "Allgemein"
      when <- tryCatch(format(as.POSIXct(r$created_at, tz = "Europe/Berlin"), "%d.%m.%Y %H:%M"),
                       error = function(e) "")
      is_res <- isTRUE(r$resolved)
      tags$div(
        style = sprintf("border:1px solid %s; border-left:5px solid %s; border-radius:8px;
                         padding:12px 16px; margin-bottom:12px; background:%s;",
                        if (is_res) "#cfe8cf" else "#ffe08a",
                        if (is_res) "#28a745" else "#f0ad4e",
                        if (is_res) "#f4fbf4" else "#fffdf5"),
        tags$div(style = "font-size:12px; color:#666; margin-bottom:4px;",
                 tags$b(r$category %||% ""), " \u00b7 ", dev_txt, " \u00b7 ",
                 "von ", tags$b(r$created_by %||% "?"), " \u00b7 ", when,
                 if (is_res) tags$span(style = "color:#28a745; font-weight:700;",
                                       " \u00b7 erledigt") else NULL),
        tags$div(style = "white-space:pre-wrap; margin:6px 0 10px;", r$message %||% ""),
        tags$div(
          actionButton(paste0("fb_toggle_", r$id),
                       label = if (is_res) "Als offen markieren" else "Als erledigt markieren",
                       class = if (is_res) "btn btn-sm btn-default" else "btn btn-sm btn-success"),
          actionButton(paste0("fb_delete_", r$id), label = "Löschen",
                       class = "btn btn-sm btn-danger", style = "margin-left:6px;")
        )
      )
    })
    n_open <- sum(!fb$resolved)
    tagList(
      tags$p(style = "color:#555; margin-bottom:14px;",
             icon("inbox"), sprintf(" %d Meldung(en), davon %d offen.", nrow(fb), n_open)),
      rows_ui
    )
  })
  
  # Dynamic observers for the per-feedback toggle/delete buttons.
  observe({
    req(rv$authed, identical(rv$role, "admin"))
    con <- pg_con(); on.exit(dbDisconnect(con), add = TRUE)
    ids <- tryCatch(DBI::dbGetQuery(con, "SELECT id FROM app_feedback")$id, error = function(e) integer(0))
    for (fid in ids) {
      local({
        this_id <- fid
        tog <- paste0("fb_toggle_", this_id)
        del <- paste0("fb_delete_", this_id)
        if (is.null(rv$fb_obs) || !(tog %in% rv$fb_obs)) {
          observeEvent(input[[tog]], {
            con2 <- pg_con(); on.exit(dbDisconnect(con2), add = TRUE)
            tryCatch(dbExecute(con2,
                               "UPDATE app_feedback SET resolved = NOT resolved WHERE id = $1",
                               params = list(this_id)), error = function(e) NULL)
            rv$feedback_refresh <- isolate(rv$feedback_refresh %||% 0L) + 1L
          }, ignoreInit = TRUE)
          observeEvent(input[[del]], {
            con2 <- pg_con(); on.exit(dbDisconnect(con2), add = TRUE)
            tryCatch(dbExecute(con2, "DELETE FROM app_feedback WHERE id = $1",
                               params = list(this_id)), error = function(e) NULL)
            rv$feedback_refresh <- isolate(rv$feedback_refresh %||% 0L) + 1L
          }, ignoreInit = TRUE)
          rv$fb_obs <- c(isolate(rv$fb_obs), tog)
        }
      })
    }
  })
}

shinyApp(ui, server)
