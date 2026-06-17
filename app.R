options(encoding = "UTF-8")

# ---- packages ----
library(shiny)
library(shinydashboard)
library(shinydashboardPlus)
library(fresh)
library(shinyjs)
library(rhandsontable)
library(rmarkdown)
library(pdftools)
library(knitr)
library(kableExtra)
library(digest)
library(DBI)
library(RPostgres)
library(jsonlite)
library(bcrypt)
library(dplyr)
library(stringr)

options(encoding = "UTF-8")



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
pg_con <- function() {
  con <- DBI::dbConnect(
    RPostgres::Postgres(),
    dbname   = Sys.getenv("DB_NAME"),
    host     = Sys.getenv("DB_HOST"),
    port     = as.integer(Sys.getenv("PGPORT", "5433")),  
    user     = Sys.getenv("DB_USER"),
    password = Sys.getenv("DB_PASSWORD"),
    sslmode  = "disable"
  )
  
  
  DBI::dbExecute(con, "SET timezone = 'Europe/Berlin'")
  
  return(con)
}


has_column <- function(con, table_name, column_name) {
  q <- "SELECT 1 FROM information_schema.columns
        WHERE table_schema='public' AND table_name=$1 AND column_name=$2"
  nrow(DBI::dbGetQuery(con, q, params = list(table_name, column_name))) > 0
}
ensure_schema <- function() {
  con <- pg_con()
  on.exit(dbDisconnect(con), add = TRUE)
  
  # Clear old data to force rebuild from template
  try(dbExecute(con, "DELETE FROM device_tables WHERE device_id = 'g1'"), silent = TRUE) #PFA, Cobas 411, Multiplate, MC1  <- g1

  try(dbExecute(con, "DELETE FROM device_tables WHERE device_id = 'g2'"), silent = TRUE) #Euroimmun Analyser <- g2

  try(dbExecute(con, "DELETE FROM device_tables WHERE device_id = 'g3'"), silent = TRUE) #Cobas 8100 <- g3

  try(dbExecute(con, "DELETE FROM device_tables WHERE device_id = 'g4'"), silent = TRUE) #Cobas Pro I <- g4

  try(dbExecute(con, "DELETE FROM device_tables WHERE device_id = 'g5'"), silent = TRUE) #Cobas Pro II <- g5

  try(dbExecute(con, "DELETE FROM device_tables WHERE device_id = 'g6'"), silent = TRUE) #CS <- g6

  try(dbExecute(con, "DELETE FROM device_tables WHERE device_id = 'g7'"), silent = TRUE) #Hämatologie <- g7
  
  try(dbExecute(con, "DELETE FROM device_tables WHERE device_id = 'g8'"), silent = TRUE)  #Hydrasys <- g8
  
  try(dbExecute(con, "DELETE FROM device_tables WHERE device_id = 'g9'"), silent = TRUE) #Optilite <- g9

try(dbExecute(con, "DELETE FROM device_tables WHERE device_id = 'g10'"), silent = TRUE) #Phadia 250 <- g10

try(dbExecute(con, "DELETE FROM device_tables WHERE device_id = 'g11'"), silent = TRUE) #ROTEM Sigma, Übersichtstabelle Kontrollen <- g11

  try(dbExecute(con, "DELETE FROM device_tables WHERE device_id = 'g12'"), silent = TRUE) #Sysmex XP300 MVZ Onko Ambulanz <- g12
  
  try(dbExecute(con, "DELETE FROM device_tables WHERE device_id = 'g13'"), silent = TRUE) #Sysmex XP300 MVZ-DEL <- g13
  
  try(dbExecute(con, "DELETE FROM device_tables WHERE device_id = 'g14'"), silent = TRUE) #Sysmex XQ-320 Onko-Ambulanz, Kinderambulanz <- g14
  
  try(dbExecute(con, "DELETE FROM device_tables WHERE device_id = 'g15'"), silent = TRUE) #Sysmex XQ-320 ZL <- g15
  
  

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
  
  # Seed devices
  dbExecute(con, "
  INSERT INTO devices (device_id, label) VALUES
    ('g1',  'PFA, Cobas 411, Multiplate, MC1'),
    ('g2',  'Euroimmun Analyzer I'),
    ('g3',  'Cobas 8100'),
    ('g4',  'COBAS Pro I'),
    ('g5',  'COBAS Pro II'),
    ('g6',  'CS'),
    ('g7',  'Hämatologie'),
    ('g8',  'Hydrasys'),                                         
    ('g9',  'Optilite'),
    ('g10', 'Phadia 250'),
    ('g11', 'ROTEM Sigma, Übersichtstabelle Kontrollen'),         
    ('g12', 'Sysmex XP300 MVZ Onko Ambulanz'),                  
    ('g13', 'Sysmex XP300 MVZ-DEL'),                             
    ('g14', 'Sysmex XQ-320 Onko-Ambulanz, Kinderambulanz'),      
    ('g15', 'Sysmex XQ-320 ZL')                                     
  ON CONFLICT (device_id) DO UPDATE
    SET label = EXCLUDED.label;
")

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
        "CS" = list(label = "CS", sn = "", pattern = "^[0-9A-Z]{0,15}$")
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
      "g12" = list(
        "Sysmex XP300 MVZ Onko Ambulanz" = list(label = "Sysmex XP300 MVZ Onko Ambulanz", sn = "", pattern = "^[0-9A-Z]{0,15}$")
      ),
      "g13" = list(
        "Sysmex XP300 MVZ-DEL" = list(label = "Sysmex XP300 MVZ-DEL", sn = "", pattern = "^[0-9A-Z]{0,15}$")
      ),
      "g14" = list(
        "Sysmex XQ-320 Onko-Ambulanz, Kinderambulanz" = list(label = "Sysmex XQ-320 Onko-Ambulanz, Kinderambulanz", sn = "", pattern = "^[0-9A-Z]{0,15}$")
      ),
      "g15" = list(
        "Sysmex XQ-320 ZL" = list(label = "Sysmex XQ-320 ZL", sn = "", pattern = "^[0-9A-Z]{0,15}$")
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

create_initial_table <- function(device_id = NULL) {
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
    rows <- rbind(rows, mk_row(header = "", task = "Liquid COntrol Set Level 1 und 2 laufen lassen"))
    rows <- rbind(rows, mk_row(header = "", task = "Durchführung siehe Anleitung Multiplate"))
    
    # Ensure column order: Header, Task, 1..31
    rows <- rows[, c("Header", "Task", as.character(1:31)), drop = FALSE]
    return(rows)
  }
  
  
  
#---------------Hämatologie (g7)───────────────────────────────────────────────────
  
  if (identical(device_id, "g7")) {
    rows <- NULL
    
    # ── Täglich ──────────────────────────────────────────────────────────────
    rows <- rbind(rows, mk_row(header = "Täglich", task = ""))
    rows <- rbind(rows, mk_row(header = "", task = "Farblösungen und Reagenzien überprüfen"))
    
    rows <- rbind(rows, mk_row(header = "Nach jeder Migration:", task = ""))
    rows <- rbind(rows, mk_row(header = "", 
    task = "Reinigung der Migrationsfläche
            Reinigung des Elektrodenrahmens
            Reinigung der Antiserenmaske unter fließendem Wasser, anschließendes Spülen mit A.dest."))
    
    # ── Wöchentlich ───────────────────────────────────────────────────────────
    rows <- rbind(rows, mk_row(header = "Wöchentlich", task = ""))
    rows <- rbind(rows, mk_row(header = "", task = "Programm TANKREINIGUNG für den Färbetank starten"))
    rows <- rbind(rows, mk_row(header = "", task = "Schlauchsystem überprüfen"))
    rows <- rbind(rows, mk_row(header = "", task = "Gerät äußerlich reinigen"))

    # ── Monatlich ───────────────────────────────────────────────────────────
    rows <- rbind(rows, mk_row(header = "Wöchentlich", task = ""))
    rows <- rbind(rows, mk_row(header = "", task = "Einlesen des Scan Control Film
                                                    (Überprüfung der densitometrischen  Auswertung) "))
    rows <- rbind(rows, mk_row(header = "", task = "Auswahl Programm→ Test Pattern→
                                                    Scann-Icon "))
    rows <- rbind(rows, mk_row(header = "", task = "Technikereinsatz/ Wartung"))
    

    
    rows <- rows[, c("Header", "Task", as.character(1:31)), drop = FALSE]
    return(rows)
  }
  
  
  #-----------------------Hydrasys (g8)──────────────────────────────────────────────
  
  if (identical(device_id, "g8")) {
    rows <- NULL
    
    # ── Täglich ──────────────────────────────────────────────────────────────
    rows <- rbind(rows, mk_row(header = "Täglich", task = ""))
    rows <- rbind(rows, mk_row(header = "", task = "XN1 / XN2   Shutdown + Neustart (6:00)"))
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
    
    # ── Wöchentlich ───────────────────────────────────────────────────────────
    rows <- rbind(rows, mk_row(header = "Wöchentlich", task = ""))
    rows <- rbind(rows, mk_row(header = "", task = "Färbereihe erneuern (Mo + Do)"))
    rows <- rbind(rows, mk_row(header = "", task = "Bestellung Diff-Platz (Montags)"))
    rows <- rbind(rows, mk_row(header = "", task = "Wöchentliche Wartung SP-50 + Straße Neustart (Mittwoch ab 6:00)"))
    rows <- rbind(rows, mk_row(header = "", task = "Kontrollmaterial erneuern XN+XQ alle Level (Donnerstags)"))
    
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
    
    # ── Wöchentlich ──────────────────────────────────────────────────────────
    rows <- rbind(rows, mk_row(header = "Wöchentlich", task = ""))
    rows <- rbind(rows, mk_row(header = "", task = "Erweitertes Spülen"))
    rows <- rbind(rows, mk_row(header = "", task = "Reinigen der Wasch-, Spüllösungs- und Abfallflaschen"))
    rows <- rbind(rows, mk_row(header = "", task = "Außenfläche reinigen: siehe tägl. Wartung"))
    rows <- rbind(rows, mk_row(header = "", task = "Phadia-Prime-PC herunterfahren"))
    
    # ── Monatlich ────────────────────────────────────────────────────────────
    rows <- rbind(rows, mk_row(header = "Monatlich", task = ""))
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
    
    # ── Wöchentlich (Freitag, ZL) ────────────────────────────────────────────
    rows <- rbind(rows, mk_row(header = "Wöchentlich", task = ""))
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


  #----------CS (g6)────────────────────────────────────────────────

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


    # ── Wöchentlich ─────────────────────────────────────────────────────────
    rows <- rbind(rows, mk_row(header = "", task = "Mittwochs Abfallbeutel erneuern"))
    rows <- rbind(rows, mk_row(header = "", task = "Mittwochs Analysator reinigen"))
    rows <- rbind(rows, mk_row(header = "", task = "Mittwochs Filter reinigen"))
    rows <- rbind(rows, mk_row(header = "", task = "Montags Bestellung Verbrauchsmaterial"))

    # ── Monatlich ─────────────────────────────────────────────────────────
    rows <- rbind(rows, mk_row(header = "", task = "PC herunterfahren und Gerät ausschalten"))
    rows <- rbind(rows, mk_row(header = "", task = "Daten-Back-Up erstellen"))
    rows <- rbind(rows, mk_row(header = "", task = "Spülkanister spülen"))
    rows <- rbind(rows, mk_row(header = "", task = "Fotometerlampe kalibrieren/wechseln"))
    rows <- rbind(rows, mk_row(header = "", task = "Alle 1000 Std. (ca. 5 Wochen)	"))
    
    # ── Wartung bei Bedarf──────────────────────────────────────────────
    rows <- rbind(rows, mk_row(header = "Bei Bedarf", task = "Technikereinsatz/ Wartung, aktuelle Chargendaten laden"))
    
    
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

    # ── 14-tägig ─────────────────────────────────────────────────────────────
    rows <- rbind(rows, mk_row(header = "14-tägig", task = ""))
    rows <- rbind(rows, mk_row(header = "", task = "e801 Messzellreinigung (Dauer ca. 25 Min.)"))
    rows <- rbind(rows, mk_row(header = "", task = "Verfallsdaten von Ko/Kal/Reagenzien in der Kühlzelle überprüfen"))

    # ── Monatlich ────────────────────────────────────────────────────────────
    rows <- rbind(rows, mk_row(header = "Monatlich", task = ""))
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

    # ── Wöchentlich ──────────────────────────────────────────────────────────
    rows <- rbind(rows, mk_row(header = "Wöchentlich", task = ""))
    rows <- rbind(rows, mk_row(header = "", task = "Dekontaminieren der Waschstation + Selbsttes"))
    rows <- rbind(rows, mk_row(header = "", task = "Die 4 Vorratsbehälter reinigen, mit A.dest füllen. Maintenance weekly starten"))
    rows <- rbind(rows, mk_row(header = "", task = "Spitzenabwurframpe desinfizieren"))
    rows <- rbind(rows, mk_row(header = "", task = "Abfallkanister entleeren, desinfizieren und spülen"))
    rows <- rbind(rows, mk_row(header = "", task = "Reinigen und desinfizieren der Geräteoberflächen"))

    # ── Monatlich ────────────────────────────────────────────────────────────
    rows <- rbind(rows, mk_row(header = "Monatlich, zusätzlich zur täglichen u. wöchentlichen", task = ""))
    rows <- rbind(rows, mk_row(header = "", task = "Vorratsbehälter m. Reinigungslösung füllen + Maintenance monthly ausführen"))
    rows <- rbind(rows, mk_row(header = "", task = "Vorratsbehälter mit A.dest ausspülen und befüllen Rinse monthly"))
    rows <- rbind(rows, mk_row(header = "", task = "Systemflüssigkeitsbehälter desinfizieren, anschließend gründlich mit A.dest spülen + mit A.dest neu befüllen"))
    rows <- rbind(rows, mk_row(header = "", task = "Pipettorspitze m. fusselfreien Tuch und Ethanol reinigen"))

    rows <- rows[, c("Header", "Task", as.character(1:31)), drop = FALSE]
    return(rows)
  }

  default_builder(c("Täglich", "Wöchentlich", "14-tägig", "Monatlich", "Bei Bedarf"),
                  c(3, 13, 3, 5, 4))
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
  
  # Ensure the g1 table is up-to-date with the current template
  if (identical(device_id, "g1")) {
    df_template <- create_initial_table("g1")
    n_existing  <- nrow(df)
    n_template  <- nrow(df_template)
    needs_save  <- FALSE
    
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
    
    if (needs_save) {
      df <- order_cols(df)
      save_device_table(con, "g1", df, "<system>")
    }
  }

  # ---- COBAS Pro I (g4) / COBAS Pro II (g5) migration --------------------
  # Sync stored tables with the current template so newly added task texts
  # (Taeglich / Woechentlich / TD / 14-taegig / Monatlich / Elektrodenaustausch)
  # appear without dropping existing daily entries or comments. Mirrors the g1
  # logic above: inject missing task texts only into rows that don't already
  # have one, and append any extra template rows at the end.
  if (identical(device_id, "g4") || identical(device_id, "g5")) {
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

upsert_cell <- function(con, device_id, row_index, day, value_text, who,
                        month = NULL, year = NULL) {
  if (is.null(month) || is.na(month)) month <- as.integer(format(Sys.Date(), "%m"))
  if (is.null(year)  || is.na(year))  year  <- as.integer(format(Sys.Date(), "%Y"))
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
}

delete_cell_if_owner <- function(con, device_id, row_index, day, who,
                                 is_admin = FALSE,
                                 month = NULL, year = NULL) {
  if (is.null(month) || is.na(month)) month <- as.integer(format(Sys.Date(), "%m"))
  if (is.null(year)  || is.na(year))  year  <- as.integer(format(Sys.Date(), "%Y"))
  if (is_admin) {
    DBI::dbExecute(con, "
      DELETE FROM device_cell_status
       WHERE device_id=$1 AND row_index=$2 AND day=$3 AND month=$4 AND year=$5
    ", params = list(device_id, row_index, day,
                     as.integer(month), as.integer(year)))
  } else {
    DBI::dbExecute(con, "
      DELETE FROM device_cell_status
       WHERE device_id=$1 AND row_index=$2 AND day=$3 AND month=$4 AND year=$5
         AND updated_by=$6
    ", params = list(device_id, row_index, day,
                     as.integer(month), as.integer(year), who))
  }
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
  # If both fields are empty, treat as a deletion so we don't accumulate blanks.
  if (!nzchar(rt) && (is.na(oc) || !nzchar(oc))) {
    DBI::dbExecute(con, "
      DELETE FROM device_task_remark
       WHERE device_id=$1 AND row_index=$2 AND remark_date=$3
    ", params = list(device_id, row_index, as.character(remark_date)))
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
          updated_at  = NOW()
  ", params = list(device_id, row_index, as.character(remark_date),
                   rt, oc, who))
}

load_remarks_for_date <- function(con, device_id, remark_date) {
  DBI::dbGetQuery(con, "
    SELECT row_index, remark_text, option_code, updated_by, updated_at
      FROM device_task_remark
     WHERE device_id=$1 AND remark_date=$2
  ", params = list(device_id, as.character(remark_date)))
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

# Recent remarks (last `days_back` days, excluding today) where either a
# remark_text was written OR an option_code was selected -> used to build the
# "Bitte siehe Bemerkungen von letzten Tagen" notice.
load_recent_remarks <- function(con, device_id, today = Sys.Date(),
                                days_back = 14L, include_today = FALSE) {
  upper <- if (isTRUE(include_today)) today + 1L else today
  DBI::dbGetQuery(con, "
    SELECT row_index, remark_date, remark_text, option_code, updated_by
      FROM device_task_remark
     WHERE device_id = $1
       AND remark_date >= $2
       AND remark_date <  $3
       AND (
             (remark_text IS NOT NULL AND remark_text <> '')
          OR (option_code IS NOT NULL AND option_code <> '')
           )
     ORDER BY remark_date DESC, row_index ASC
  ", params = list(device_id,
                   as.character(today - as.integer(days_back)),
                   as.character(upper)))
}

# Delete a single remark identified by (device_id, row_index, remark_date).
# Used by the "Erledigt" button in the recent-remarks modal so colleagues
# can clear away resolved items.
delete_remark <- function(con, device_id, row_index, remark_date) {
  DBI::dbExecute(con, "
    DELETE FROM device_task_remark
     WHERE device_id = $1 AND row_index = $2 AND remark_date = $3
  ", params = list(device_id, as.integer(row_index), as.character(remark_date)))
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

# ---- Handsontable renderer (per-cell locking) ----
cells_readonly_for_headers <- function(df, current_user_initials, cell_status_df,
                                       role = "user", invalid_days = integer(),
                                       month = as.integer(format(Sys.Date(), "%m")),
                                       year  = as.integer(format(Sys.Date(), "%Y")),
                                       read_only_all = FALSE,
                                       remarks_df = NULL) {
  df_show <- overlay_for_render(df, cell_status_df)

  # German-friendly column headers:
  #   "Header" column -> blank label (only the cell's content matters)
  #   "Task"   column -> "Aufgabe"
  #   day columns 1..31 -> "Mo 1", "Di 2", ... using the selected month/year
  if (length(month) != 1L || is.na(month)) month <- as.integer(format(Sys.Date(), "%m"))
  if (length(year)  != 1L || is.na(year))  year  <- as.integer(format(Sys.Date(), "%Y"))
  display_names <- names(df_show)
  display_names[display_names == "Header"] <- " "
  display_names[display_names == "Task"]   <- "Aufgabe"
  de_wd <- c("So", "Mo", "Di", "Mi", "Do", "Fr", "Sa")
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
      display_names[idx] <- paste0(de_wd[wd], " ", d)
    }
  }

  rh <- rhandsontable(df_show, stretchH = "all") %>%
    # Color the Header column per schedule category (matches Tägliche Aufgaben tab)
    hot_col(
      "Header",
      readOnly = TRUE,
      renderer = htmlwidgets::JS("
        function (instance, td, row, col, prop, value, cellProperties) {
          Handsontable.renderers.TextRenderer.apply(this, arguments);
          var v = value ? String(value).trim() : '';
          if (v.length === 0) {
            td.style.background = '#ffffff';
            return;
          }
          // Schedule-category -> color (must mirror sched_styles in server)
          var map = {
            'Täglich':                       '#28a745',
            'Montag und Donnerstag':         '#1e88e5',
            'Wöchentlich':                   '#0097a7',
            '14-tägig':                      '#5e35b1',
            'Monatlich':                     '#8e24aa',
            'Quartalsweise':                 '#ad1457',
            'Am ersten Dienstag im Monat':   '#7f0000',
            'Montag':                        '#bf360c',
            'Dienstag':                      '#bf360c',
            'Mittwoch':                      '#bf360c',
            'Donnerstag':                    '#bf360c',
            'Freitag':                       '#bf360c',
            'Samstag':                       '#bf360c',
            'Sonntag':                       '#bf360c'
          };
          var bg = map[v];
          if (bg) {
            td.style.background = bg;
            td.style.color = '#ffffff';
            td.style.fontWeight = 'bold';
          } else {
            td.style.background = '#fff7b2';
            td.style.color = '#000000';
            td.style.fontWeight = 'bold';
          }
        }
      ")
    ) %>%
    hot_cols(columnSorting = TRUE, manualColumnResize = TRUE, colHeaders = display_names) %>%
    hot_table(highlightCol = TRUE, highlightRow = TRUE, rowHeaders = FALSE, comments = TRUE,
              readOnly = isTRUE(read_only_all))


  # Keep the rest: lock header ROWS for Task/day cells if you want them uneditable
  header_rows <- which(df_show$Header != "")
  if (length(header_rows)) {
    for (r in header_rows) {
      # Task column (keep readOnly but no yellow)
      if ("Task" %in% names(df_show)) rh <- rh %>% hot_cell(r - 1, "Task", readOnly = TRUE)
      # Day columns 1..31 (keep readOnly but no yellow)
      for (d in as.character(1:31)) {
        if (d %in% names(df_show)) rh <- rh %>% hot_cell(r - 1, d, readOnly = TRUE)
      }
    }
  }

  # lock invalid days (unchanged)
  if (length(invalid_days)) {
    for (d in as.character(invalid_days)) {
      if (d %in% names(df_show)) {
        for (r in seq_len(nrow(df_show))) {
          rh <- rh %>% hot_cell(r - 1, d, readOnly = TRUE, comment = "Ungültig für diesen Monat")
        }
      }
    }
  }

  # per-cell locks + comments (unchanged)
  # Build a quick lookup of remark text/option by (row_index, day) for this month
  remark_lookup <- new.env(hash = TRUE, parent = emptyenv())
  if (!is.null(remarks_df) && nrow(remarks_df)) {
    for (i in seq_len(nrow(remarks_df))) {
      rr <- as.integer(remarks_df$row_index[i])
      dd <- as.integer(remarks_df$day[i])
      key <- paste0(rr, "_", dd)
      txt <- if (!is.null(remarks_df$remark_text[i]) && !is.na(remarks_df$remark_text[i])) remarks_df$remark_text[i] else ""
      opt <- if (!is.null(remarks_df$option_code[i]) && !is.na(remarks_df$option_code[i])) remarks_df$option_code[i] else ""
      who <- remarks_df$updated_by[i] %||% ""
      parts <- character(0)
      if (nzchar(opt)) parts <- c(parts, sprintf("[%s]", opt))
      if (nzchar(txt)) parts <- c(parts, txt)
      label <- if (length(parts)) paste(parts, collapse = " ") else ""
      if (nzchar(label) || nzchar(who)) {
        assign(key, list(label = label, who = who), envir = remark_lookup)
      }
    }
  }

  build_comment <- function(rr, dd_chr, base = NULL) {
    key <- paste0(rr, "_", as.integer(dd_chr))
    if (exists(key, envir = remark_lookup, inherits = FALSE)) {
      r <- get(key, envir = remark_lookup, inherits = FALSE)
      extra <- if (nzchar(r$label)) sprintf("Bemerkung von %s: %s", r$who, r$label)
               else sprintf("Bemerkung von %s", r$who)
      if (is.null(base) || !nzchar(base)) extra else paste(base, extra, sep = "\n")
    } else base
  }

  seen_cells <- new.env(hash = TRUE, parent = emptyenv())
  if (!missing(cell_status_df) && nrow(cell_status_df)) {
    for (i in seq_len(nrow(cell_status_df))) {
      rr  <- cell_status_df$row_index[i]
      dd  <- as.character(cell_status_df$day[i])
      who <- cell_status_df$updated_by[i]
      if (!(dd %in% names(df_show))) next
      if (rr < 1 || rr > nrow(df_show)) next
      if (df_show$Header[rr] != "") next
      if (!identical(who, current_user_initials) && role != "admin") {
        rh <- rh %>% hot_cell(rr - 1, dd, readOnly = TRUE)
      }
      when <- format(as.POSIXct(cell_status_df$updated_at[i], tz = "Europe/Berlin"), "%Y-%m-%d %H:%M %Z")
      base_comment <- sprintf("von %s · %s", who, when)
      full <- build_comment(rr, dd, base_comment)
      rh <- rh %>% hot_cell(rr - 1, dd, comment = full)
      assign(paste0(rr, "_", as.integer(dd)), TRUE, envir = seen_cells)
    }
  }

  # Remarks for cells that have no cell_status entry -> still show as comment
  if (!is.null(remarks_df) && nrow(remarks_df)) {
    for (i in seq_len(nrow(remarks_df))) {
      rr  <- as.integer(remarks_df$row_index[i])
      dd  <- as.integer(remarks_df$day[i])
      key <- paste0(rr, "_", dd)
      if (exists(key, envir = seen_cells, inherits = FALSE)) next
      dd_chr <- as.character(dd)
      if (!(dd_chr %in% names(df_show))) next
      if (rr < 1 || rr > nrow(df_show)) next
      if (df_show$Header[rr] != "") next
      cmt <- build_comment(rr, dd_chr, NULL)
      if (!is.null(cmt) && nzchar(cmt)) {
        rh <- rh %>% hot_cell(rr - 1, dd_chr, comment = cmt)
      }
    }
  }

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

    /* Compact dropdown so it doesn't dominate the row or visually
       clash with the rows below when the menu pops open. */
    .task-checkbox { flex: 0 0 auto; padding-left: 12px; padding-right: 8px; }
    .task-checkbox .form-group { margin-bottom: 0; }
    .task-nicht    { flex: 0 0 auto; }
    .task-select   { flex: 0 1 160px; min-width: 90px; }
    .task-select .form-group { margin-bottom: 0; }
    .task-remark   { flex: 1 1 160px; min-width: 0; display: flex; gap: 4px; align-items: center; }
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
      overflow: auto;
      max-height: calc(100vh - 150px);
      border: 1px solid #ddd;
      border-radius: 5px;
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
      .task-select   { flex: 0 1 140px; min-width: 90px; }
      .task-remark   { flex: 1 1 140px; min-width: 0; }

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
    .login-wrapper {
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
    .login-left h2 {
      color: #fff;
      margin: 0 0 6px 0;
      font-weight: 700;
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
      gap: 10px;
    }
    .login-left ul.feature-list li i.fa,
    .login-left ul.feature-list li svg {
      color: #ffd166;
      margin-top: 3px;
      flex-shrink: 0;
    }
    .login-left .login-foot {
      font-size: 12px;
      opacity: 0.75;
      margin-top: 24px;
      border-top: 1px solid rgba(255,255,255,0.18);
      padding-top: 14px;
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
      grid-template-columns: repeat(auto-fit, minmax(190px, 1fr));
      gap: 14px;
      margin-bottom: 22px;
    }
    .hub-summary .sum-card {
      background: #fff; border-radius: 14px;
      padding: 18px 20px;
      box-shadow: 0 4px 12px rgba(0,0,0,0.06);
      border-left: 5px solid #003B73;
      display: flex; align-items: center; gap: 14px;
    }
    .hub-summary .sum-card.warn { border-left-color: #ff6a3d; }
    .hub-summary .sum-card.ok   { border-left-color: #00b894; }
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
    .hub-summary .sum-num {
      font-size: 26px; font-weight: 800; color: #2c3e50; line-height: 1;
    }
    .hub-summary .sum-lbl {
      font-size: 12px; color: #6c757d; text-transform: uppercase;
      letter-spacing: 1px; font-weight: 600; margin-top: 4px;
    }

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
    $(document).on('change', '.task-select select', function() {
      var label = $(this).find('option:selected').text() || '';
      var remark = $(this).closest('.task-controls').find('.task-remark');
      if (label.indexOf('Nicht erledigt') !== -1) {
        remark.addClass('needs-remark');
        setTimeout(function(){ remark.find('input[type=text]').focus(); }, 50);
      } else {
        remark.removeClass('needs-remark');
      }
    });
    // Apply on initial render too (e.g. when reopening a device).
    $(document).on('shiny:value', function(e) {
      if (e.name === 'today_tasks') {
        setTimeout(function() {
          $('.task-select select').each(function() {
            var label = $(this).find('option:selected').text() || '';
            var remark = $(this).closest('.task-controls').find('.task-remark');
            if (label.indexOf('Nicht erledigt') !== -1) remark.addClass('needs-remark');
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
            tags$div(style = "display:flex; align-items:center; gap:12px; margin-bottom:18px;",
              tags$div(style = "background:rgba(255,255,255,0.15); width:48px; height:48px; border-radius:12px; display:flex; align-items:center; justify-content:center;",
                tags$i(class = "fa fa-tools", style = "font-size:22px; color:#ffd166;")
              ),
              tags$div(
                tags$div(style = "font-size:11px; letter-spacing:2px; opacity:0.8; text-transform:uppercase;",
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
            tags$b("Yadwinder"), ", ", tags$b("Frank"), " oder ", tags$b("Martina"), "."
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

        # Compact device header (always visible, slim)
        uiOutput("device_info_header_slim"),

        # Tabbed interface for tasks and visualization (THIS is what the user
        # should see first when opening a device)
        tabsetPanel(
          tabPanel(
            "Tägliche Aufgaben",
            tags$div(
              class = "left-panel",
              style = "flex: none; width: auto; height: calc(100vh - 200px); margin-top: 20px;",
              
              # Date header
              tags$div(class = "date-header", 
                       icon("calendar-day"),
                       " Tägliche Aufgaben"
              ),
              
              # Stats bar
              uiOutput("task_stats"),
              
              # Quick actions
              # tags$div(
              #   class = "quick-actions",
              #   actionButton("mark_today", "✓ Alle erledigen", 
              #                class = "btn btn-success btn-sm",
              #                style = "width: 100%;"),
              #   actionButton("refresh_tasks", "🔄 Aktualisieren", 
              #                class = "btn btn-info btn-sm",
              #                style = "width: 100%;")
              # ),
              # 
              tags$hr(),
              
              # Admin-only: add a new task to any device
              uiOutput("admin_add_task_box"),
              
              # Task list
              uiOutput("today_tasks")
            )
          ),
          tabPanel(
            "Monatsübersicht - Wartungsplan",
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
                                        style = "margin-right: 5px;")
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
                           helpText("Sie können ein Bild manuell in www/uploads/ ablegen und den Pfad in der DB unter app_images (id='hub_header') setzen, falls gewünscht.")
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
      tabItem(tabName = "admin",
              uiOutput("admin_panel")
      )
    )
  )
)

ui <- dashboardPage(header, sidebar, body, title = "Wartungsplan")

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
           "Wöchentlich"                   = (today_wd == "Monday"),
           "Wöchentlich (Montag)"          = (today_wd == "Monday"),
           "Wöchentlich (Freitag)"         = (today_wd == "Friday"),
           "Wöchentlich (Freitag, ZL)"     = (today_wd == "Friday"),
           "14-tägig"                      = is_biweekly_monday(),
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
           "Alle 3 Monate oder alle 7500 Proben" = is_first_workday_of_quarter(),
           "Nach jeder Migration:"         = TRUE,
           FALSE   # Bei Bedarf / Wartung bei Bedarf / unknown -> never auto-flagged
    )
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
  
  ensure_schema()
  
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
    tasks_refresh = 0L,
    pending_remarks = NULL
  )
  
  # Ensure upload directory exists (www/uploads) so you can place images there manually
  if (!dir.exists(file.path("www", "uploads"))) dir.create(file.path("www", "uploads"), recursive = TRUE, showWarnings = FALSE)
  
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
    if (!ok) { showNotification("Falsches Passwort.", type = "error"); return() }
    
    rv$authed <- TRUE; rv$must_reset <- FALSE
    dbExecute(con, "UPDATE app_users SET last_login = NOW() WHERE username=$1", params = list(rv$user))
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
      menuItem("Checkliste",           tabName = "checklist", icon = icon("tasks"))
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

  # ---- Add user
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
  
  # ---- Übersicht (hub) ----
  count_open_today <- function(con, device_id, df) {
    if (is.null(df) || !nrow(df)) return(0L)
    data_rows <- which(df$Header == "")
    if (!length(data_rows)) return(0L)
    cur_m <- as.integer(format(Sys.Date(), "%m"))
    cur_y <- as.integer(format(Sys.Date(), "%Y"))
    cs <- load_cell_status(con, device_id, cur_m, cur_y)
    today <- as.integer(format(Sys.Date(), "%d"))
    done_rows <- unique(cs$row_index[cs$day == today])
    sum(!(data_rows %in% done_rows))
  }

  # Build a summary of devices with open tasks for today.
  # Returns a data.frame with columns: device_id, label, open_count
  compute_due_today <- function() {
    con <- pg_con(); on.exit(dbDisconnect(con), add = TRUE)
    devs <- DBI::dbGetQuery(con, "SELECT device_id, label FROM devices ORDER BY device_id")
    if (!nrow(devs)) return(devs)
    devs$open_count <- vapply(seq_len(nrow(devs)), function(i) {
      did <- devs$device_id[i]
      df_tmp <- tryCatch(load_device_table(con, did) %||% create_initial_table(did),
                         error = function(e) NULL)
      tryCatch(as.integer(count_open_today(con, did, df_tmp)),
               error = function(e) 0L)
    }, integer(1))
    devs
  }

  # Show a friendly modal listing devices with open tasks today.
  # Each device with open tasks gets a button that opens its checklist.
  show_due_tasks_dialog <- function() {
    devs <- tryCatch(compute_due_today(), error = function(e) NULL)
    if (is.null(devs) || !nrow(devs)) return(invisible(NULL))

    open_devs <- devs[devs$open_count > 0, , drop = FALSE]
    today_str <- to_german_date_str(format(Sys.Date(), "%A, %d.%m.%Y"))
    total_open <- sum(open_devs$open_count)

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
           devices = c("g7", "g12", "g13", "g14", "g15")),
      list(name = "Arbeitsplatz 2 \u2013 Gerinnung",
           icon = "wave-square",
           devices = c("g1", "g6", "g11")),
      list(name = "Arbeitsplatz 3 \u2013 Klinische Chemie",
           icon = "flask",
           devices = c("g3", "g4", "g5", "g9")),
      list(name = "Arbeitsplatz 4 \u2013 Immunologie / Allergie",
           icon = "shield-alt",
           devices = c("g2", "g8", "g10"))
    )

    make_dev_card <- function(did, lbl, cnt) {
      tags$div(class = "due-card",
        tags$div(class = "due-info",
          tags$div(class = "due-count", cnt),
          tags$div(
            tags$div(class = "due-name", lbl),
            tags$div(class = "due-meta",
                     sprintf("%d offene Aufgabe%s", cnt, ifelse(cnt == 1, "", "n")))
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
      cards <- lapply(seq_len(nrow(sub)), function(i)
        make_dev_card(sub$device_id[i], sub$label[i], sub$open_count[i]))
      tags$div(class = "due-ap-group",
        tags$div(class = "due-ap-title",
          tags$span(class = "due-ap-icon", icon(ap$icon)),
          tags$span(ap$name),
          tags$span(class = "due-ap-count",
                    sprintf("%d offen", ap_total))
        ),
        cards
      )
    })

    # Devices with open tasks that aren't mapped to any Arbeitsplatz
    leftover <- open_devs[!(open_devs$device_id %in% assigned_ids), , drop = FALSE]
    if (nrow(leftover)) {
      cards <- lapply(seq_len(nrow(leftover)), function(i)
        make_dev_card(leftover$device_id[i], leftover$label[i], leftover$open_count[i]))
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
                   sprintf(" Aufgabe%s offen", ifelse(total_open == 1, "", "n"))),
          tags$div(class = "hero-pill",
                   tags$b(nrow(open_devs)),
                   sprintf(" Gerät%s betroffen", ifelse(nrow(open_devs) == 1, "", "e")))
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
        modalButton("Später") |> tagAppendAttributes(class = "btn-later")
      )
    ))
  }
  

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

  output$hub_summary <- renderUI({
    req(rv$authed)
    devs <- tryCatch(compute_due_today(), error = function(e) NULL)
    if (is.null(devs) || !nrow(devs)) return(NULL)
    n_total   <- nrow(devs)
    n_open    <- sum(devs$open_count > 0)
    n_done    <- n_total - n_open
    sum_open  <- sum(devs$open_count)
    tags$div(class = "hub-summary",
      tags$div(class = "sum-card",
        tags$div(class = "sum-icon", icon("microchip")),
        tags$div(
          tags$div(class = "sum-num", n_total),
          tags$div(class = "sum-lbl", "Geräte gesamt")
        )
      ),
      tags$div(class = paste("sum-card clickable", if (sum_open > 0) "warn" else "ok"),
        title = "Klicken, um die offenen Aufgaben pro Ger\u00e4t anzuzeigen",
        onclick = "Shiny.setInputValue('hub_open_due_modal', Math.random(), {priority:'event'});",
        style = "cursor:pointer;",
        tags$div(class = "sum-icon",
                 icon(if (sum_open > 0) "triangle-exclamation" else "check")),
        tags$div(
          tags$div(class = "sum-num", sum_open),
          tags$div(class = "sum-lbl", "Offene Aufgaben heute")
        )
      ),
      tags$div(class = "sum-card ok",
        tags$div(class = "sum-icon", icon("circle-check")),
        tags$div(
          tags$div(class = "sum-num", n_done),
          tags$div(class = "sum-lbl", "Geräte erledigt")
        )
      )
    )
  })

  output$hub_buttons <- renderUI({
    req(rv$authed)
    con <- pg_con(); on.exit(DBI::dbDisconnect(con), add = TRUE)
    devices <- DBI::dbGetQuery(con, "SELECT device_id, label FROM devices ORDER BY device_id")
    
    # ── Arbeitsplatz definitions ──────────────────────────────────────────────
    arbeitsplaetze <- list(
      list(
        name    = "Arbeitsplatz 1 \u2013 H\u00e4matologie",
        icon    = "tint",
        color   = "#B23A48",
        bg      = "#FFF6F7",
        border  = "#B23A48",
        devices = c("g7", "g12", "g13", "g14", "g15")
      ),
      list(
        name    = "Arbeitsplatz 2 \u2013 Gerinnung",
        icon    = "wave-square",
        color   = "#8e24aa",
        bg      = "#fdf4ff",
        border  = "#8e24aa",
        devices = c("g1", "g6", "g11")
      ),
      list(
        name    = "Arbeitsplatz 3 \u2013 Klinische Chemie",
        icon    = "flask",
        color   = "#1e88e5",
        bg      = "#f0f7ff",
        border  = "#1e88e5",
        devices = c("g3", "g4", "g5", "g9")
      ),
      list(
        name    = "Arbeitsplatz 4 \u2013 Immunologie / Allergie",
        icon    = "shield-alt",
        color   = "#00897b",
        bg      = "#f0faf9",
        border  = "#00897b",
        devices = c("g2", "g8", "g10")
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
        df_tmp     <- tryCatch(
          load_device_table(con, did) %||% create_initial_table(did),
          error = function(e) NULL
        )
        open_count <- tryCatch(
          as.integer(count_open_today(con, did, df_tmp)),
          error = function(e) 0L
        )
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
  lapply(1:15, function(i) {
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
    show_due_tasks_dialog()
  })
  
  # ---- Recent remarks modal --------------------------------------------------
  # Persistent dialog that replaces the old toast notification. Lists each
  # open Bemerkung from the last 14 days with two actions:
  #   * "Zur Aufgabe" – switches to the Tägliche-Aufgaben tab and scrolls to
  #     the relevant row so the user can address the remark in context.
  #   * "Erledigt" – removes the remark from the database (the issue was
  #     resolved) and refreshes the modal.
  show_recent_remarks_modal <- function() {
    rr <- rv$pending_remarks
    if (is.null(rr) || !nrow(rr) || is.null(rv$current_device)) {
      removeModal()
      return(invisible(NULL))
    }
    task_lookup <- if (!is.null(rv$data) && "Task" %in% names(rv$data))
                     as.character(rv$data$Task) else character(0)

    line_items <- lapply(seq_len(nrow(rr)), function(i) {
      ri   <- rr$row_index[i]
      d    <- as.Date(rr$remark_date[i])
      opt  <- rr$option_code[i] %||% ""
      txt  <- rr$remark_text[i] %||% ""
      usr  <- rr$updated_by[i]  %||% ""
      tname <- if (length(task_lookup) >= ri && ri >= 1 && nzchar(task_lookup[ri]))
                 task_lookup[ri] else paste0("Aufgabe ", ri)
      key <- sprintf("%d|%s", as.integer(ri), format(d, "%Y-%m-%d"))
      tags$li(
        style = "margin-bottom:10px; padding:10px 12px; border-radius:8px;
                 background:#fff5f5; border-left:4px solid #d32f2f;
                 list-style:none;",
        tags$div(
          style = "display:flex; align-items:flex-start; gap:10px; flex-wrap:wrap;",
          tags$div(style = "flex:1 1 240px; min-width:0;",
            tags$div(style = "font-weight:700; color:#003B73; font-size:13px;",
                     format(d, "%d.%m.%Y"), " \u2013 \u00BB", tname, "\u00AB"),
            tags$div(style = "font-size:12px; color:#555; margin-top:3px;",
              if (nzchar(opt)) tags$span(
                style = "background:#003B73; color:#fff; padding:1px 6px;
                         border-radius:4px; font-weight:700; margin-right:6px;",
                opt) else NULL,
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
                  " Klicken Sie auf \u201eErledigt\u201c, sobald die Bemerkung bearbeitet wurde."),
        modalButton("Sp\u00e4ter")
      ),
      tags$div(
        style = "background: linear-gradient(135deg,#d32f2f 0%,#b71c1c 100%);
                 color:#fff; padding:14px 18px; margin:-15px -15px 12px -15px;
                 border-radius:6px 6px 0 0;",
        tags$div(style = "font-size:18px; font-weight:800;",
                 icon("triangle-exclamation"),
                 " Bitte siehe Bemerkungen von letzten Tagen"),
        tags$div(style = "font-size:12px; opacity:.92; margin-top:2px;",
                 sprintf("%d offene Bemerkung%s f\u00fcr dieses Ger\u00e4t",
                         nrow(rr), if (nrow(rr) == 1) "" else "en"))
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
  })

  # User clicked "Erledigt" – delete the remark and refresh the modal.
  observeEvent(input$resolve_remark, {
    req(rv$current_device, input$resolve_remark)
    parts <- strsplit(input$resolve_remark, "|", fixed = TRUE)[[1]]
    if (length(parts) < 2) return()
    ri <- suppressWarnings(as.integer(parts[1]))
    rd <- parts[2]
    if (is.na(ri) || !nzchar(rd)) return()
    ok <- tryCatch({
      con_d <- pg_con(); on.exit(dbDisconnect(con_d), add = TRUE)
      delete_remark(con_d, rv$current_device, ri, rd)
      TRUE
    }, error = function(e) FALSE)
    if (!ok) {
      showNotification("Bemerkung konnte nicht entfernt werden.",
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
    rv$task_obs_ids <- character(0)

    # Notify the user if there are recent remarks from previous days for this
    # device (typed reasons or dropdown options like D / sB / etc.). The
    # remarks are shown in a persistent modal so the user can act on each
    # one (jump to the task or mark it as resolved/Erledigt).
    tryCatch({
      rr <- load_recent_remarks(con, did, today = Sys.Date(),
                                days_back = 14L, include_today = TRUE)
      rv$pending_remarks <- if (nrow(rr) > 0) rr else NULL
      if (nrow(rr) > 0) show_recent_remarks_modal()
    }, error = function(e) NULL)
    
    # load device title (if set) for display
    dl <- load_device_layout(con, did)
    rv$current_device_title <- dl$title
    
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
        sid  <- paste0("task_save_rem_", rr)
        
        # when checkbox toggled
        observeEvent(input[[cid]], {
          req(rv$current_device, rv$data)
          val <- isTRUE(input[[cid]])
          con <- pg_con(); on.exit(dbDisconnect(con), add = TRUE)
          today <- as.integer(format(Sys.Date(), "%d"))
          who <- rv$user_initials %||% rv$user
          is_admin <- identical(rv$role, "admin")
          
          if (val) {
            # If checkbox is checked, put checkmark with username and clear dropdown
            updateSelectInput(session, oid, selected = "")
            cell_val <- paste0("\u2713 (", who, ")")
            upsert_cell(con, rv$current_device, rr, today, cell_val, who)

          } else {
            # delete only if present
            delete_cell_if_owner(con, rv$current_device, rr, today, who, is_admin)
          }
          # Simple update without complex reactive dependencies
          rv$table_status <- load_cell_status_current(con, rv$current_device)
        }, ignoreInit = TRUE)
        
        # when select input changed -> uncheck checkbox and put abbreviation only
        observeEvent(input[[oid]], {
          req(rv$current_device, rv$data)
          sel <- input[[oid]]
          if (is.null(sel) || sel == "") return()  # Ignore empty selection
          
          # Uncheck the checkbox when dropdown is selected
          updateCheckboxInput(session, cid, value = FALSE)
          
          con <- pg_con(); on.exit(dbDisconnect(con), add = TRUE)
          today <- as.integer(format(Sys.Date(), "%d"))
          who <- rv$user_initials %||% rv$user
          
          # Extract abbreviation from full text and put only that in the cell
          abbrev <- sub("^([A-Za-z.øØ]+).*", "\\1", sel)  # Extract the part before the first space/parenthesis
          upsert_cell(con, rv$current_device, rr, today, abbrev, who)

          # Persist the option code alongside any current remark text so
          # tomorrow's banner can show "[abbrev] reason".
          cur_text <- isolate(input[[rmid]]) %||% ""
          tryCatch(
            upsert_remark(con, rv$current_device, rr, Sys.Date(),
                          cur_text, abbrev, who),
            error = function(e) NULL
          )

          # Simple update without complex reactive dependencies
          rv$table_status <- load_cell_status_current(con, rv$current_device)
        }, ignoreInit = TRUE)

        # when remark text changed -> save (debounced by Shiny's textInput)
        observeEvent(input[[rmid]], {
          req(rv$current_device, rv$data)
          who <- rv$user_initials %||% rv$user
          txt <- input[[rmid]] %||% ""
          # Pair with the currently selected dropdown option (abbrev), if any.
          sel <- isolate(input[[oid]]) %||% ""
          opt <- if (nzchar(sel)) sub("^([A-Za-z.øØ]+).*", "\\1", sel) else ""
          tryCatch({
            con <- pg_con(); on.exit(dbDisconnect(con), add = TRUE)
            upsert_remark(con, rv$current_device, rr, Sys.Date(),
                          txt, opt, who)
          }, error = function(e) NULL)
        }, ignoreInit = TRUE)

        # "Nicht erledigt" button: marks the cell as NE, unchecks Erledigt,
        # and focuses the Bemerkung textbox so the user can type the reason.
        observeEvent(input[[nid]], {
          req(rv$current_device, rv$data)
          updateCheckboxInput(session, cid, value = FALSE)
          updateSelectInput(session, oid,
            selected = "NE (Nicht erledigt \u2013 bitte Bemerkung eintragen)")
          who <- rv$user_initials %||% rv$user
          today <- as.integer(format(Sys.Date(), "%d"))
          tryCatch({
            con <- pg_con(); on.exit(dbDisconnect(con), add = TRUE)
            upsert_cell(con, rv$current_device, rr, today, "NE", who)
            cur_text <- isolate(input[[rmid]]) %||% ""
            upsert_remark(con, rv$current_device, rr, Sys.Date(),
                          cur_text, "NE", who)
            rv$table_status <- load_cell_status_current(con, rv$current_device)
          }, error = function(e) NULL)
          # focus the textbox via JS
          session$sendCustomMessage("focusTaskRemark", list(id = rmid))
        }, ignoreInit = TRUE)

        # Explicit Save button for the Bemerkung: persists text + current
        # option and shows a confirmation toast.
        observeEvent(input[[sid]], {
          req(rv$current_device, rv$data)
          who <- rv$user_initials %||% rv$user
          txt <- input[[rmid]] %||% ""
          sel <- isolate(input[[oid]]) %||% ""
          opt <- if (nzchar(sel)) sub("^([A-Za-z.øØ]+).*", "\\1", sel) else ""
          ok <- tryCatch({
            con <- pg_con(); on.exit(dbDisconnect(con), add = TRUE)
            upsert_remark(con, rv$current_device, rr, Sys.Date(),
                          txt, opt, who)
            TRUE
          }, error = function(e) FALSE)
          if (isTRUE(ok)) {
            showNotification(
              if (nzchar(txt))
                sprintf("Bemerkung gespeichert: %s", txt)
              else "Bemerkung gel\u00f6scht.",
              type = "message", duration = 4
            )
          } else {
            showNotification("Bemerkung konnte nicht gespeichert werden.",
                             type = "error", duration = 6)
          }
        }, ignoreInit = TRUE)

      })
      rv$task_obs_ids <- unique(c(rv$task_obs_ids, rid))
    }
  }

  # ---- Monatsübersicht: top-right "open remarks" dropdown ----------------
  # Shows recent / not-yet-closed Bemerkungen grouped into three buckets:
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
      if (h == "Täglich") return("daily")
      if (h %in% c("Bei Bedarf", "Start-Up")) return("ondemand")
      if (h %in% c("Wöchentlich", "Monatlich", "14-tägig", "Quartalsweise",
                   "Montag und Donnerstag", "Am ersten Dienstag im Monat",
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
    tryCatch({
      con_r <- pg_con(); on.exit(dbDisconnect(con_r), add = TRUE)
      tr <- load_remarks_for_date(con_r, rv$current_device, Sys.Date())
      if (nrow(tr))
        for (i in seq_len(nrow(tr)))
          today_remarks[[ as.character(tr$row_index[i]) ]] <-
        list(text = tr$remark_text[i] %||% "", opt = tr$option_code[i] %||% "")
      recent_remarks_df <- load_recent_remarks(con_r, rv$current_device,
                                               today = Sys.Date(), days_back = 14L)
    }, error = function(e) NULL)
    
    # ── Start ui_list with date header ───────────────────────────────────────
    ui_list <- list(
      tags$div(
        class = "date-header",
        style = "background: linear-gradient(135deg, #003B73 0%, #136377 100%);",
        icon("calendar-check"), " ", format_de_date(Sys.Date())
      )
    )
    
    # ── "Due right now" red banner ────────────────────────────────────────────
    due_now_tasks <- character(0)
    for (r2 in seq_len(nrows)) {
      if (nzchar(rv$data$Header[r2])) next
      tt <- if ("Task" %in% names(rv$data)) rv$data$Task[r2] else ""
      if (!nzchar(tt)) next
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
    
    # ── Main loop ─────────────────────────────────────────────────────────────
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
        
        sched_styles <- list(
          "Täglich"                         = list("#28a745","#20c997","#218838", TRUE),
          "Täglich (ZL)"                    = list("#28a745","#20c997","#218838", TRUE),
          "Montag und Donnerstag"           = list("#1e88e5","#42a5f5","#1565c0",
                                                   is_today_monday || is_today_thursday),
          "Wöchentlich"                     = list("#0097a7","#26c6da","#006064", is_today_monday),
          "Wöchentlich (Montag)"            = list("#0097a7","#26c6da","#006064", is_today_monday),
          "Wöchentlich (Freitag)"           = list("#0097a7","#26c6da","#006064",
                                                   today_day_german == "Freitag"),
          "14-tägig"                        = list("#5e35b1","#7e57c2","#311b92", is_today_biwk_mon),
          "Monatlich"                       = list("#8e24aa","#ba68c8","#4a148c", is_today_monthly_cycle),
          "Monatlich (Freitag)"             = list("#8e24aa","#ba68c8","#4a148c",
                                                   today_day_german == "Freitag" && is_today_workday1),
          "Monatlich oder alle 2500 Proben" = list("#8e24aa","#ba68c8","#4a148c", is_today_monthly_cycle),
          "Quartalsweise"                   = list("#ad1457","#ec407a","#880e4f", is_today_quarter1),
          "Alle 3 Monate oder alle 7500 Proben" = list("#ad1457","#ec407a","#880e4f", is_today_quarter1),
          "Am ersten Dienstag im Monat"     = list("#7f0000","#b71c1c","#4a0000", is_first_tuesday),
          "Bei Bedarf"                      = list("#546e7a","#78909c","#37474f", FALSE),
          "Wartung bei Bedarf"              = list("#546e7a","#78909c","#37474f", FALSE),
          "Nach jeder Migration:"           = list("#546e7a","#78909c","#37474f", TRUE)
        )
        if (hdr %in% c("Montag","Dienstag","Mittwoch","Donnerstag","Freitag","Samstag","Sonntag"))
          sched_styles[[hdr]] <- list("#bf360c","#e65100","#7f2200", hdr == today_day_german)
        
        sty              <- sched_styles[[hdr]]
        is_today_relevant <- !is.null(sty) && isTRUE(sty[[4]])
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
          "Wöchentlich"                     = next_monday(Sys.Date()),
          "Wöchentlich (Montag)"            = next_monday(Sys.Date()),
          NA
        )
        next_due_label <- if (!is.na(next_due_date) && inherits(next_due_date, "Date"))
          sprintf("(f\u00e4llig am %s)", format_de_short(next_due_date))
        else NULL
        
        ui_list <- append(ui_list, list(
          if (nzchar(header_style)) {
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
        ))
        
      } else {
        # ── TASK ROW ─────────────────────────────────────────────────────────
        task_name <- if (nzchar(task_text)) task_text else paste0("Aufgabe ", r)
        done_id   <- paste0("task_done_",    r)
        opt_id    <- paste0("task_opt_",     r)
        rem_id    <- paste0("task_rem_",     r)
        nicht_id  <- paste0("task_nicht_",   r)
        save_id   <- paste0("task_save_rem_",r)
        
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
        task_class   <- if (is_done) "task-item completed" else "task-item"
        remark_class <- if (identical(selected_opt,
                                      "NE (Nicht erledigt \u2013 bitte Bemerkung eintragen)"))
          "task-remark needs-remark" else "task-remark"
        
        # ── Reminder logic ────────────────────────────────────────────────────
        t_time     <- extract_task_time(task_name)
        urgency    <- if (!is_done && !is.na(t_time))
          classify_task_urgency(t_time) else NA_character_
        time_badge <- urgency_badge(urgency, t_time)
        item_style <- if (!is_done) {
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
                   icon(if (is_done) "check-circle" else "circle"),
                   " ", task_name, time_badge),
          tags$div(class = "task-controls",
                   tags$div(class = "task-checkbox",
                            checkboxInput(done_id, label = "Erledigt", value = is_done)),
                   tags$div(class = "task-nicht",
                            actionButton(nicht_id,
                                         label = tagList(icon("xmark"), " Nicht erledigt"),
                                         class = "btn btn-sm btn-nicht-erledigt")),
                   tags$div(class = "task-select",
                            selectInput(opt_id, label = NULL,
                                        choices  = c("Andere Option w\u00e4hlen..." = "", TASK_OPTIONS),
                                        selected = if (is_done) "" else selected_opt,
                                        selectize = FALSE)),
                   tags$div(class = remark_class,
                            textInput(rem_id, label = NULL, value = rem_val,
                                      placeholder = "ggf. Bemerkung eingeben..."),
                            actionButton(save_id,
                                         label = tagList(icon("floppy-disk"), " Speichern"),
                                         class = "btn btn-sm btn-save-remark"))
          )
        )
        
        ui_list <- append(ui_list, list(row_ui))
      }   # end task row
    }     # end for loop
    
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
  KNOWN_SCHEDULES <- c(
    "Täglich", "Wöchentlich", "14-tägig", "Monatlich", "Quartalsweise",
    "Bei Bedarf", "Start-Up",
    "Montag und Donnerstag", "Am ersten Dienstag im Monat",
    "Montag", "Dienstag", "Mittwoch", "Donnerstag", "Freitag", "Samstag", "Sonntag"
  )
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
    if (!nrow(devs)) {
      return(div(class = "alert alert-warning",
                 "Keine Geräte vorhanden \u2013 neue Aufgabe kann nicht hinzugefügt werden."))
    }
    dev_choices <- setNames(devs$device_id,
                            ifelse(nzchar(devs$label %||% ""),
                                   paste0(devs$label, " (", devs$device_id, ")"),
                                   devs$device_id))
    selected_dev <- rv$current_device %||% devs$device_id[1]
    box(width = 12, collapsible = TRUE, collapsed = TRUE,
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
  
  output$download_table_csv <- downloadHandler(
    filename = function() {
      paste0("Wartungsplan_", format(Sys.Date(), "%Y%m%d_%H%M%S"), ".pdf")
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
  
  output$download_table_pdf <- downloadHandler(
    filename = function() {
      paste0("Wartungsplan_", format(Sys.Date(), "%Y%m%d_%H%M%S"), ".pdf")
    },
    content = function(file) {
      # Now we're inside a reactive context - we can access rv$data
      req(rv$data)
      req(rv$current_device)
      
      # Check if required packages are available
      required_pkgs <- c("rmarkdown", "knitr", "kableExtra")
      missing_pkgs <- required_pkgs[!sapply(required_pkgs, requireNamespace, quietly = TRUE)]
      if (length(missing_pkgs) > 0) {
        showNotification(
          paste("Fehlende Pakete für PDF-Generierung:", paste(missing_pkgs, collapse = ", ")),
          type = "error",
          duration = 10
        )
        return()
      }
      
      # Check if template file exists
      template_path <- "wartungsplan_template.Rmd"
      if (!file.exists(template_path)) {
        showNotification(
          "Template-Datei 'wartungsplan_template.Rmd' nicht gefunden!",
          type = "error",
          duration = 10
        )
        return()
      }
      
      # Show progress notification
      showNotification("PDF wird generiert...", id = "pdf_generation", duration = NULL, type = "message")
      
      # Get data from reactive values (we're inside content function, so this is OK)
      con <- pg_con()
      on.exit(dbDisconnect(con), add = TRUE)
      
      # Selected month/year from the UI (fall back to today's month/year)
      sel_month <- suppressWarnings(as.integer(input$month))
      sel_year  <- suppressWarnings(as.integer(input$year))
      if (length(sel_month) != 1L || is.na(sel_month)) sel_month <- as.integer(format(Sys.Date(), "%m"))
      if (length(sel_year)  != 1L || is.na(sel_year))  sel_year  <- as.integer(format(Sys.Date(), "%Y"))

      pdf_df <- build_overlayed_table(rv$data,
                                      load_cell_status(con, rv$current_device,
                                                       sel_month, sel_year))
      
      # Get device label
      device_info <- tryCatch({
        DBI::dbGetQuery(con, "SELECT label FROM devices WHERE device_id = $1", 
                        params = list(rv$current_device))
      }, error = function(e) {
        data.frame(label = character(0))
      })
      device_label <- if (nrow(device_info) > 0) device_info$label[1] else rv$current_device
      
      # Get serial numbers
      serials <- tryCatch({
        load_device_serials(con, rv$current_device)
      }, error = function(e) {
        list()
      })

      # Get device layout (footer text + version)
      layout <- tryCatch(load_device_layout(con, rv$current_device),
                        error = function(e) list(footer_text = NULL, version = NULL))
      footer_text <- if (!is.null(layout$footer_text) && nzchar(layout$footer_text)) layout$footer_text else ""
      version_str <- if (!is.null(layout$version) && nzchar(layout$version)) layout$version else ""

      month_de  <- unname(.DE_MONTHS[sel_month])
      month_str <- sprintf("%s %d", month_de, sel_year)
      
      # Generate PDF
      tryCatch({
        rmarkdown::render(
          input = template_path,
          output_file = basename(file),
          output_dir = dirname(file),
          params = list(
            table_data = pdf_df,
            device_label = device_label,
            report_date = format(Sys.Date(), "%d.%m.%Y"),
            serial_numbers = serials,
            month = sprintf("%02d", sel_month),
            year = as.character(sel_year),
            month_str = month_str,
            footer_text = footer_text,
            version_str = version_str
          ),
          envir = new.env(parent = globalenv()),
          quiet = TRUE
        )
        
        removeNotification(id = "pdf_generation")
        showNotification("PDF erfolgreich erstellt!", type = "message", duration = 5)
        
      }, error = function(e) {
        # Remove progress notification
        removeNotification(id = "pdf_generation")
        
        # Fallback: Create HTML version
        warning("LaTeX PDF generation failed: ", e$message)
        
        # Create HTML fallback
        serials_html <- ""
        if (length(serials) > 0) {
          serial_items <- sapply(names(serials), function(name) {
            sn <- serials[[name]]$sn %||% ""
            if (nzchar(sn)) {
              sprintf("<strong>%s:</strong> SN %s", htmltools::htmlEscape(name), htmltools::htmlEscape(sn))
            } else {
              ""
            }
          })
          serial_items <- serial_items[nzchar(serial_items)]
          if (length(serial_items) > 0) {
            serials_html <- paste0("<div class='serials'>", paste(serial_items, collapse = " &nbsp;|&nbsp; "), "</div>")
          }
        }
        
              html_content <- paste0("
      <!DOCTYPE html>
      <html>
      <head>
        <meta charset='UTF-8'>
        <title>Wartungsplan - ", htmltools::htmlEscape(device_label), "</title>
        <style>
          @page { size: landscape; margin: 1cm; }
          @media print {
            body { -webkit-print-color-adjust: exact; print-color-adjust: exact; }
            .no-print { display: none; }
          }
          body { font-family: Arial, sans-serif; font-size: 8pt; margin: 0; padding: 20px; }
          h1 { text-align: center; color: #003B73; margin-bottom: 5px; }
          .date { text-align: center; margin-bottom: 10px; color: #666; font-size: 10pt; }
          .serials { text-align: center; margin-bottom: 15px; padding: 10px; background-color: #e3f2fd; border-radius: 5px; border: 1px solid #2196F3; }
          .print-button { text-align: center; margin: 20px 0; }
          .print-button button { background-color: #003B73; color: white; border: none; padding: 10px 30px; font-size: 14pt; border-radius: 5px; cursor: pointer; }
          table { border-collapse: collapse; width: 100%; margin: 20px 0; font-size: 7pt; }
          th, td { border: 1px solid #ccc; padding: 4px 2px; text-align: center; }
          th { background-color: #003B73; color: white; font-weight: bold; }
          .header-row { background-color: #fff7b2; font-weight: bold; text-align: left; }
          tr:nth-child(even):not(.header-row) { background-color: #f9f9f9; }
        </style>
      </head>
      <body>
        <div class='no-print print-button'><button onclick='window.print()'>🖨️ Als PDF drucken</button></div>
        <h1>Wartungsplan - ", htmltools::htmlEscape(device_label), "</h1>
        <div class='date'>", month_str, "</div>
        ", serials_html, "
        <table><thead><tr><th>Abschnitt</th><th>Aufgabe</th>")
        
        for (day in 1:31) {
          html_content <- paste0(html_content, "<th>", day, "</th>")
        }
        html_content <- paste0(html_content, "</tr></thead><tbody>")
        
        for (i in seq_len(nrow(pdf_df))) {
          is_header <- nzchar(pdf_df$Header[i])
          row_class <- if (is_header) " class='header-row'" else ""
          html_content <- paste0(html_content, "<tr", row_class, ">")
          html_content <- paste0(html_content, "<td style='text-align:left;'>", htmltools::htmlEscape(as.character(pdf_df$Header[i])), "</td>")
          html_content <- paste0(html_content, "<td style='text-align:left;'>", htmltools::htmlEscape(as.character(pdf_df$Task[i])), "</td>")
          
          for (day in 1:31) {
            col_name <- as.character(day)
            cell_value <- if (col_name %in% names(pdf_df)) htmltools::htmlEscape(as.character(pdf_df[i, col_name])) else ""
            html_content <- paste0(html_content, "<td>", cell_value, "</td>")
          }
          html_content <- paste0(html_content, "</tr>")
        }
        
        html_content <- paste0(html_content, "</tbody></table></body></html>")
        
        html_file <- sub("\\.pdf$", ".html", file)
        writeLines(html_content, html_file, useBytes = TRUE)
        
        showNotification("PDF-Generierung fehlgeschlagen. HTML-Version wurde erstellt.", type = "warning", duration = 10)
      })
    },
    contentType = "application/pdf"
  )
}

shinyApp(ui, server)
