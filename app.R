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

# ----------------------------- DB CONFIG / HELPERS ----------------------------
pg_con <- function() {
  DBI::dbConnect(
    RPostgres::Postgres(),
    host     = "ep-lucky-bread-agh1e4ax-pooler.c-2.eu-central-1.aws.neon.tech",
    port     = 5432,
    dbname   = "neondb",
    user     = "neondb_owner",
    password = "npg_uiX5Mf7nGrOm",
    sslmode  = "require"
  )
}

has_column <- function(con, table_name, column_name) {
  q <- "SELECT 1 FROM information_schema.columns
        WHERE table_schema='public' AND table_name=$1 AND column_name=$2"
  nrow(DBI::dbGetQuery(con, q, params = list(table_name, column_name))) > 0
}

ensure_schema <- function() {
  con <- pg_con(); on.exit(dbDisconnect(con), add = TRUE)
  
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

dbExecute(con, "ALTER TABLE device_layout ADD COLUMN IF NOT EXISTS version TEXT;")
dbExecute(con, "ALTER TABLE device_layout ADD COLUMN IF NOT EXISTS valid_from DATE;")

  
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
  
  # Per-device layout table: footer and title for each device (store file path relative to www/)
  dbExecute(con, "
    CREATE TABLE IF NOT EXISTS device_layout (
      device_id   TEXT PRIMARY KEY REFERENCES devices(device_id),
      title       TEXT,
      footer_text TEXT,
      footer_path TEXT,   -- relative path under www/, e.g. 'uploads/g1-footer-20251029.png'
      footer_mime TEXT,
      updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_by  TEXT
    );
  ")
  
  # App-level images (for hub/übersicht header) - optional; you upload manually to www/uploads and set path if needed
  dbExecute(con, "
    CREATE TABLE IF NOT EXISTS app_images (
      id         TEXT PRIMARY KEY,
      img_path   TEXT,
      img_mime   TEXT,
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_by TEXT
    );
  ")
  # ensure default row for hub header exists (can be NULL path)
  dbExecute(con, "
    INSERT INTO app_images (id, img_path)
    VALUES ('hub_header', NULL)
    ON CONFLICT (id) DO NOTHING;
  ")
  
  # Seed devices (upsert so labels are applied/updated)
  dbExecute(con, "
    INSERT INTO devices (device_id, label) VALUES
      ('g1', 'PFA,Cobas 411, Multiplate, MC1'),
      ('g2', 'Euroimmun Analyzer I'),
      ('g3', 'Cobas 8100'),
      ('g4', 'COBAS Pro I'),
      ('g5', 'COBAS Pro II'),
      ('g6', 'CS'),
      ('g7', 'Hämatologie 1'),
      ('g8', 'Hämatologie 2'),
      ('g9', 'Optilite'),
      ('g10', 'Phadia 250 1'),
      ('g11', 'Phadia 250 2'),
      ('g12', 'Sysmex XP300 1'),
      ('g13', 'Sysmex XP300 2'),
      ('g14', 'Sysmex XQ-320 Onko-Ambulanz, Kinderambulanz 1'),
      ('g15', 'Sysmex XQ-320 Onko-Ambulanz, Kinderambulanz 2')
    ON CONFLICT (device_id) DO UPDATE
      SET label = EXCLUDED.label;
  ")
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
    
    # Lower headers with empty task rows so they are visible in the checklist
    rows <- rbind(rows, mk_row(header = "PFA (SN 00398)", task = ""))
    rows <- rbind(rows, mk_row(header = "", task = ""))
    
    rows <- rbind(rows, mk_row(header = "MC1", task = ""))
    rows <- rbind(rows, mk_row(header = "", task = ""))
    
    rows <- rbind(rows, mk_row(header = "Multiplate  (SN 310071)", task = ""))
    rows <- rbind(rows, mk_row(header = "", task = ""))
    
    # Ensure column order: Header, Task, 1..31
    rows <- rows[, c("Header", "Task", as.character(1:31)), drop = FALSE]
    return(rows)
  }
  
  # Fallback for other devices (same behavior as before)
  if (identical(device_id, "g2")) {
    default_builder(c("Täglich", "Wöchentlich", "Monatlich", "Quartalsweise", "Bei Bedarf"),
                    c(5, 10, 6, 3, 2))
  } else if (identical(device_id, "g3")) {
    default_builder(c("Start-Up", "Täglich", "Wöchentlich", "Monatlich", "Bei Bedarf"),
                    c(4, 8, 8, 4, 3))
  } else {
    default_builder(c("Täglich", "Wöchentlich", "14-tägig", "Monatlich", "Bei Bedarf"),
                    c(3, 13, 3, 5, 4))
  }
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
                  "D (Gerät / Modul defekt)", "sB (Siehe Bemerkungen)", "sQ (Siehe Quasi)")

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
  # ensure Task column exists for compatibility
  if (!"Task" %in% names(df)) df$Task <- ""
  
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
load_cell_status <- function(con, device_id) {
  DBI::dbGetQuery(con, "
    SELECT device_id, row_index, day, value_text, updated_by, updated_at
      FROM device_cell_status
     WHERE device_id = $1
  ", params = list(device_id))
}

upsert_cell <- function(con, device_id, row_index, day, value_text, who) {
  DBI::dbExecute(con, "
    INSERT INTO device_cell_status (device_id, row_index, day, value_text, updated_by, updated_at)
    VALUES ($1,$2,$3,$4,$5,NOW())
    ON CONFLICT (device_id, row_index, day) DO UPDATE
      SET value_text = EXCLUDED.value_text,
          updated_by = EXCLUDED.updated_by,
          updated_at = NOW()
  ", params = list(device_id, row_index, day, value_text, who))
}

delete_cell_if_owner <- function(con, device_id, row_index, day, who, is_admin = FALSE) {
  if (is_admin) {
    DBI::dbExecute(con, "
      DELETE FROM device_cell_status WHERE device_id=$1 AND row_index=$2 AND day=$3
    ", params = list(device_id, row_index, day))
  } else {
    DBI::dbExecute(con, "
      DELETE FROM device_cell_status
       WHERE device_id=$1 AND row_index=$2 AND day=$3 AND updated_by=$4
    ", params = list(device_id, row_index, day, who))
  }
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
                                       role = "user", invalid_days = integer()) {
  df_show <- overlay_for_render(df, cell_status_df)

  rh <- rhandsontable(df_show, stretchH = "all") %>%
    # Make Header column yellow if not empty
    hot_col(
      "Header",
      readOnly = TRUE,
      renderer = htmlwidgets::JS("
        function (instance, td, row, col, prop, value, cellProperties) {
          Handsontable.renderers.TextRenderer.apply(this, arguments);
          if (value && String(value).trim().length > 0) {
            td.style.background = '#fff7b2';  // nice yellow
            td.style.fontWeight = 'bold';
          } else {
            td.style.background = '#ffffff';  // plain white for blank rows
          }
        }
      ")
    ) %>%
    hot_cols(columnSorting = TRUE, manualColumnResize = TRUE) %>%
    hot_table(highlightCol = TRUE, highlightRow = TRUE, rowHeaders = FALSE, comments = TRUE)


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
      rh <- rh %>% hot_cell(rr - 1, dd, comment = sprintf("von %s · %s", who, when))
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
header <- dashboardHeader(title = "Wartungsplan")

sidebar <- dashboardSidebar(
  sidebarMenu(id = "tabs",
              menuItem("Übersicht", tabName = "hub", icon = icon("th-large")),
              menuItem("Checkliste", tabName = "checklist", icon = icon("tasks")),
              menuItem("Kopf-Fuß Zeile ändern", tabName = "layout", icon = icon("images"))
  )
)

body <- dashboardBody(
  useShinyjs(),
  use_theme(apptheme),
    tags$style(HTML(sprintf("
      .btn.btn-primary { color: #fff !important; }
      .btn.btn-primary{ background:%1$s; border-color:%1$s; }
      .btn.btn-primary:hover{ filter:brightness(0.9); }
      .badge{ background:%1$s; }
      h1,h2,h3,h4{ color:%1$s; }

      /* yellow highlight for header rows in the handsontable */
      .header-yellow {
        background-color: #fff7b2 !important;  /* soft yellow */
        font-weight: 600;
      }

      .handsontable td.htYellowHeader {
  background-color: #fff7b2 !important;  /* soft yellow */
  font-weight: 700 !important;
}


      /* small responsive tweak for header/footer images */
      .app-header, .app-footer { text-align:center; padding:8px 0; background: #ffffff; }
      .app-header img, .app-footer img { max-height:100px; display:inline-block; margin:4px; }
    ", DARK_BLUE))),

  conditionalPanel(
    condition = "!output.is_authed",

    fluidRow(column(6, offset = 3,
                    box(width = 12, title = "Anmeldung", status = "primary", solidHeader = TRUE,
                        textInput("login_user", "Benutzername"),
                        passwordInput("login_pass", "Passwort"),
                        actionLink("forgot_pw_link", "Passwort vergessen?"),
                        br(), actionButton("login_btn", "Einloggen"),
                        br(), br(),
                        helpText("Hinweis: Bei der ersten Anmeldung lassen Sie das Passwort frei und klicken Sie auf 'Einloggen' – dann können Sie eines festlegen.")
                    ),
                    uiOutput("password_setup_panel")
    )),
    
  ),
  conditionalPanel(
    condition = "output.is_authed",
    tabItems(
      tabItem(tabName = "hub",
              uiOutput("hub_header_image"),
              h3("Diagnostikzentrum"),
              h4("Universitätsinstitut für Klinische Chemie und Laboratoriumsmedizin"),
              br(),
              uiOutput("hub_buttons"),
              br(),br(),
              uiOutput("hub_table_area")


      ),
      tabItem(tabName = "checklist",
              uiOutput("device_title"),
              br(),
              fluidRow(
                # LEFT column made wider (was 4) -> now 5 to give more space for tasks
                column(
                  width = 5,
                  box(width = 12, title = "Heutige Aufgaben (Heute)", status = "primary", solidHeader = TRUE,
                      uiOutput("today_tasks")
                  ),
                  box(width = 12, title = "Aktionen", status = "primary", solidHeader = TRUE,
                      selectInput("section", "Reihe unter Abschnitt hinzufügen:",
                                  choices = c("Täglich", "Wöchentlich", "Monatlich", "Quartalsweise", "Bei Bedarf")),
                      actionButton("add_row", "Reihe hinzufügen", class = "btn btn-primary"),
                      br(), br(),
                      fluidRow(
                        column(2, selectInput("day", "Tag", choices = 1:31, selected = as.integer(format(Sys.Date(), "%d")))),
                        column(3, selectInput("month", "Monat", choices = 1:12, selected = as.integer(format(Sys.Date(), "%m")))),
                        column(3, selectInput(
                          "year", "Jahr",
                          choices = (as.integer(format(Sys.Date(), "%Y")) - 1):(as.integer(format(Sys.Date(), "%Y")) + 1),
                          selected = as.integer(format(Sys.Date(), "%Y"))
                        ))
                      ),
                      actionButton("mark_today", "Heute abhaken ✓", class = "btn btn-primary")

                  )
                ),
                column(
                  width = 7,
                  div(style = "text-align: right; margin-bottom: 15px;",
                      downloadButton("download_table_pdf", "📄 PDF herunterladen", 
                                   class = "btn btn-success", 
                                   style = "background: linear-gradient(45deg, #28a745, #20c997); border: none; color: white; font-weight: bold; padding: 10px 20px; border-radius: 8px; box-shadow: 0 4px 8px rgba(0,0,0,0.2);")),
                  box(width = 12, title = "Checkliste", status = "primary", solidHeader = TRUE,
                      uiOutput("pending_alert"),
                      rHandsontableOutput("tableRH"),
                      br(),
                      # Editable per-device information box: placed below the table and above the legend
                      uiOutput("device_info_area"),
                      # device-specific footer area will be rendered below the info area if present
                      uiOutput("device_footer_ui"),
                  box(
                    width = 12, title = "Legende", status = "primary", solidHeader = TRUE,
                    div(style = "display:flex; justify-content:flex-end;",
                        tags$table(class = "table table-condensed", style = "width:auto; max-width:320px; margin:0;",
                                  tags$tbody(
                                    tags$tr(tags$td(strong("Legende"), colspan = 2)),
                                    tags$tr(tags$td(strong("WE")), tags$td("Wochenende")),
                                    tags$tr(tags$td(strong("FT")), tags$td("Feiertag")),
                                    tags$tr(tags$td(strong("Ø")), tags$td("An diesem Tag wurden keine Analysen gestartet")),
                                    tags$tr(tags$td(strong("W.e.")), tags$td("Wartungspunkt ist in einer größeren Wartung enthalten")),
                                    tags$tr(tags$td(strong("ne")), tags$td("Nicht erforderlich (für die Rubrik „bei Bedarf“)")),
                                    tags$tr(tags$td(strong("D")), tags$td("Gerät / Modul defekt")),
                                    tags$tr(tags$td(strong("sB")), tags$td("Siehe Bemerkungen")),
                                    tags$tr(tags$td(strong("sQ")), tags$td("Siehe Quasi"))
                                  )
                        )
                    )
                  )
                  )
                )
              )
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
    task_obs_ids = character(0)
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
  })
  
  output$password_setup_panel <- renderUI({
    if (!isTRUE(rv$must_reset) || is.null(rv$user)) return(NULL)
    box(width = 12, title = "Passwort festlegen", status = "warning", solidHeader = TRUE,
        p("Erstmalige Anmeldung erkannt. Bitte Passwort setzen."),
        passwordInput("new_pass", "Neues Passwort"),
        passwordInput("new_pass2", "Passwort bestätigen"),
        actionButton("set_pass_btn", "Passwort speichern", class = "btn btn-primary")
    )
  })
  
  observeEvent(input$set_pass_btn, {
    req(input$new_pass, input$new_pass2, rv$user)
    if (nchar(input$new_pass) < 8) { showNotification("Passwort zu kurz (min. 8 Zeichen).", type = "error"); return() }
    if (input$new_pass != input$new_pass2) { showNotification("Passwörter stimmen nicht überein.", type = "error"); return() }
    con <- pg_con(); on.exit(dbDisconnect(con), add = TRUE)
    ph <- bcrypt::hashpw(input$new_pass)
    dbExecute(con,
              "UPDATE app_users SET password_hash=$1, must_reset=FALSE, last_login=NOW() WHERE username=$2",
              params = list(ph, rv$user)
    )
    rv$authed <- TRUE; rv$must_reset <- FALSE
    showNotification("Passwort gespeichert. Willkommen!", type = "message")
    updateTabItems(session, "tabs", "hub")
  })
  
  observeEvent(input$forgot_pw_link, {
    showModal(modalDialog(
      title = "Passwort zurücksetzen",
      textInput("forgot_user", "Benutzername"),
      passwordInput("forgot_secret", "Reset-Secret"),
      helpText("Setzen Sie RESET_SECRET als Umgebungsvariable auf dem Server."),
      footer = tagList(modalButton("Abbrechen"), actionButton("forgot_confirm", "Zurücksetzen")),
      easyClose = TRUE
    ))
  })
  observeEvent(input$forgot_confirm, {
    req(input$forgot_user, input$forgot_secret)
    expected <- Sys.getenv("RESET_SECRET", unset = NA)
    if (!safe_equal(input$forgot_secret, expected)) {
      showNotification("Ungültiges Reset-Secret.", type = "error"); return()
    }
    con <- pg_con(); on.exit(dbDisconnect(con), add = TRUE)
    u <- dbGetQuery(con, "SELECT 1 FROM app_users WHERE username=$1", params = list(input$forgot_user))
    if (!nrow(u)) { showNotification("Benutzer nicht gefunden.", type = "error"); return() }
    force_reset_db(con, input$forgot_user)
    removeModal()
    showNotification("Zurückgesetzt. Beim nächsten Login wird ein neues Passwort verlangt.", type = "message")
  })
  
  # Add a bypass mode for testing purposes
  bypass_auth <- FALSE  # Set to TRUE to skip login, FALSE for normal behavior
  
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
    cs <- load_cell_status(con, device_id)
    today <- as.integer(format(Sys.Date(), "%d"))
    done_rows <- unique(cs$row_index[cs$day == today])
    sum(!(data_rows %in% done_rows))
  }
  

  output$hub_buttons <- renderUI({
    req(rv$authed)
    con <- pg_con(); on.exit(DBI::dbDisconnect(con), add = TRUE)
    devices <- DBI::dbGetQuery(con, "SELECT device_id, label FROM devices ORDER BY device_id")
    
    # Build a vertical list of buttons (one per device)
    tagList(
      lapply(seq_len(nrow(devices)), function(i) {
        did <- devices$device_id[i]
        lbl <- devices$label[i]
        df_tmp <- load_device_table(con, did) %||% create_initial_table(did)
        open_count <- tryCatch(count_open_today(con, did, df_tmp), error = function(e) 0L)

        # Note: version and valid_from display removed as they're not currently used
        
        # Wrap button in a div to get spacing; make button full-width
        div(style = "margin-bottom:8px; width:50%;",
            actionButton(
              inputId = paste0("open_", did),
              label = tagList(
                div(style = "display:flex; justify-content:space-between; align-items:center; width:100%;",
                    span(lbl, style = "flex:1; text-align:left;"),
                    span(class = "badge", style = "margin-left:10px; border-radius:10px; padding:4px 8px;", open_count)
                )
              ),
              class = "btn btn-primary",
              style = "width:100%; text-align:left;"
            )
        )
      })
    )
  })
  
  # observers for each device button
  lapply(1:15, function(i) {
    obs_id <- paste0("open_g", i)
    observeEvent(input[[obs_id]], {
      load_device(paste0("g", i))
    })
  })
  
  observeEvent(list(input$month, input$year), {
    req(rv$authed, !is.null(rv$data))
    month <- as.integer(input$month %||% as.integer(format(Sys.Date(), "%m")))
    year  <- as.integer(input$year  %||% as.integer(format(Sys.Date(), "%Y")))
    rv$invalid_days <- calc_invalid_days(year, month)
    
    output$tableRH <- renderRHandsontable({
      cells_readonly_for_headers(rv$data) %>% 
        { cells_readonly_for_headers(rv$data, rv$user_initials, rv$table_status, rv$role, rv$invalid_days) }
    })
  }, ignoreInit = FALSE)
  
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
    
    status <- load_cell_status(con, did)
    rv$current_device <- did
    rv$data <- order_cols(df)
    rv$table_status <- status
    rv$task_obs_ids <- character(0)
    
    # load device title (if set) for display
    dl <- load_device_layout(con, did)
    rv$current_device_title <- dl$title
    
    updateTabItems(session, "tabs", "checklist")
    output$tableRH <- renderRHandsontable({
      cells_readonly_for_headers(rv$data, rv$user_initials, rv$table_status, rv$role, rv$invalid_days)
    })
    
    # Render the device info area (editable by any authenticated user)
    output$device_info_area <- renderUI({
      req(rv$current_device)
      con2 <- pg_con(); on.exit(dbDisconnect(con2), add = TRUE)
      dl2 <- load_device_layout(con2, rv$current_device)
      # show current footer text (if any), allow editing by all authenticated users
      box(
        width = 12, title = "Geräte-Informationen (editable)", status = "primary", solidHeader = TRUE,
        if (!is.null(dl2$footer_path) && nzchar(dl2$footer_path)) {
          tagList(
            tags$div("Hinweis: Ein Gerätebild ist hinterlegt und wird unten angezeigt (manuelles Upload in www/uploads/)."),
            tags$div(tags$img(src = dl2$footer_path, style = "max-height:120px;"))
          )
        } else NULL,
        textAreaInput("device_info_text", "Informationen zum Gerät (sichtbar für alle):", value = dl2$footer_text %||% "", rows = 4),
        actionButton("save_device_info", "Speichern", class = "btn btn-primary"),
        if (!is.null(dl2$updated_at)) tags$small(sprintf(" Letzte Änderung: %s von %s", format(as.POSIXct(dl2$updated_at, tz = "UTC"), "%Y-%m-%d %H:%M"), dl2$updated_by)) else NULL
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
    txt <- input$device_info_text %||% ""
    # Save footer_text for the device (this is the editable info box)
    save_device_layout(con, rv$current_device, title = NULL, footer_text = txt, footer_path = NULL, footer_mime = NULL, who = who)
    # Refresh UI: re-render device info area and footer display
    dl <- load_device_layout(con, rv$current_device)
    output$device_info_area <- renderUI({
      box(
        width = 12, title = "Geräte-Informationen (editable)", status = "primary", solidHeader = TRUE,
        if (!is.null(dl$footer_path) && nzchar(dl$footer_path)) {
          tagList(
            tags$div("Hinweis: Ein Gerätebild ist hinterlegt und wird unten angezeigt (manuelles Upload in www/uploads/)."),
            tags$div(tags$img(src = dl$footer_path, style = "max-height:120px;"))
          )
        } else NULL,
        textAreaInput("device_info_text", "Informationen zum Gerät (sichtbar für alle):", value = dl$footer_text %||% "", rows = 4),
        actionButton("save_device_info", "Speichern", class = "btn btn-primary"),
        if (!is.null(dl$updated_at)) tags$small(sprintf(" Letzte Änderung: %s von %s", format(as.POSIXct(dl$updated_at, tz = "UTC"), "%Y-%m-%d %H:%M"), dl$updated_by)) else NULL
      )
    })
    output$device_footer_ui <- renderUI({
      if (!is.null(dl$footer_path) && nzchar(dl$footer_path)) {
        tagList(tags$hr(), tags$div(class = "app-footer", tags$img(src = dl$footer_path, alt = "Geräte-Fuß")))
      } else if (!is.null(dl$footer_text) && nzchar(dl$footer_text)) {
        tagList(tags$hr(), tags$div(class = "app-footer", dl$footer_text))
      } else NULL
    })
    showNotification("Geräteinformationen gespeichert.", type = "message")
  })
  
  output$device_title <- renderUI({
    req(rv$current_device)
    con <- pg_con(); on.exit(dbDisconnect(con), add = TRUE)
    lab <- DBI::dbGetQuery(con, "SELECT label FROM devices WHERE device_id = $1", params = list(rv$current_device))$label
    if (length(lab) == 0 || is.na(lab)) lab <- rv$current_device
    display_title <- if (!is.null(rv$current_device_title) && nzchar(rv$current_device_title)) rv$current_device_title else lab
    tagList(
      h3(paste("Checkliste ·", display_title)),
      tags$small(sprintf("Angemeldet als %s%s (%s)",
                         rv$user, if (rv$role == "admin") " (Admin)" else "", rv$user_initials))
    )
  })
  
  output$pending_alert <- renderUI({
    req(rv$data, rv$current_device, rv$table_status)
    today <- as.integer(format(Sys.Date(), "%d"))
    data_rows <- which(rv$data$Header == "")
    cs <- rv$table_status
    done_rows <- unique(cs$row_index[cs$day == today])
    open_n <- sum(!(data_rows %in% done_rows))
    if (open_n <= 0) return(NULL)
    div(class = "alert alert-warning",
        strong("Tätigkeiten heute fällig: "),
        sprintf("%d Zeile(n) ohne Eintrag im Tag %d.", open_n, today))
  })
  
  observeEvent(input$tableRH$changes$changes, {
    req(rv$authed, rv$current_device, rv$data)
    ch <- input$tableRH$changes$changes
    if (is.null(ch)) return()
    
    con <- pg_con(); on.exit(dbDisconnect(con), add = TRUE)
    is_admin <- identical(rv$role, "admin")
    who <- rv$user_initials %||% rv$user %||% ""
    
    for (i in seq_len(length(ch))) {
      row0 <- ch[[i]][[1]]
      coln <- ch[[i]][[2]]
      newv <- ch[[i]][[4]]
      if (!(coln %in% as.character(1:31))) next
      if (as.integer(coln) %in% rv$invalid_days) next
      
      row_index <- row0 + 1
      if (rv$data$Header[row_index] != "") next
      day <- as.integer(coln)
      device_id <- rv$current_device
      
      if (isTRUE(is.na(newv)) || identical(newv, "")) {
        delete_cell_if_owner(con, device_id, row_index, day, who, is_admin)
      } else {
        upsert_cell(con, device_id, row_index, day, as.character(newv), who)
      }
    }
    rv$table_status <- load_cell_status(con, rv$current_device)
    output$tableRH <- renderRHandsontable({
      cells_readonly_for_headers(rv$data, rv$user_initials, rv$table_status, rv$role, rv$invalid_days)
    })
  }, ignoreInit = TRUE)
  
  observeEvent(input$add_row, {
    req(rv$data, input$section)
    section <- input$section
    header_idx <- which(rv$data$Header == section)
    next_header_idx <- suppressWarnings(min(which(seq_len(nrow(rv$data)) > max(header_idx) & rv$data$Header != "")))
    if (is.infinite(next_header_idx)) next_header_idx <- nrow(rv$data) + 1
    insert_idx <- next_header_idx - 1
    
    if (!"Task" %in% names(rv$data)) rv$data$Task <- ""
    
    cn <- colnames(rv$data)
    new_row <- as.data.frame(t(setNames(rep("", length(cn)), cn)),
                             stringsAsFactors = FALSE, check.names = FALSE)
    new_row$Header    <- ""
    new_row$Task      <- ""
    new_row$Done      <- FALSE
    new_row$Kommentar <- ""
    new_row$Benutzer  <- ""
    
    # Handle edge cases for rbind
    if (insert_idx <= 0) {
      rv$data <- rbind(new_row, rv$data)
    } else if (insert_idx >= nrow(rv$data)) {
      rv$data <- rbind(rv$data, new_row)
    } else {
      rv$data <- rbind(rv$data[seq_len(insert_idx), ], new_row, rv$data[seq(insert_idx+1, nrow(rv$data)), ])
    }
    rv$data <- order_cols(rv$data)
    
    output$tableRH <- renderRHandsontable({
      cells_readonly_for_headers(rv$data, rv$user_initials, rv$table_status, rv$role, rv$invalid_days)
    })
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
          rv$table_status <- load_cell_status(con, rv$current_device)
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
          # Simple update without complex reactive dependencies
          rv$table_status <- load_cell_status(con, rv$current_device)
        }, ignoreInit = TRUE)
        
      })
      rv$task_obs_ids <- unique(c(rv$task_obs_ids, rid))
    }
  }
  
  output$today_tasks <- renderUI({
    req(rv$current_device, rv$data)
    today <- as.integer(format(Sys.Date(), "%d"))
    # iterate all rows in order and render headers as group titles and data rows as tasks
    nrows <- nrow(rv$data)
    if (is.null(nrows) || nrows == 0) return(div(class="alert alert-info", "Keine Aufgaben definiert."))
    
    cs <- rv$table_status %||% data.frame()
    # map existing saved values for today
    existing_for_today <- list()
    if (nrow(cs)) {
      ssub <- cs[cs$day == today, , drop = FALSE]
      for (i in seq_len(nrow(ssub))) existing_for_today[[ as.character(ssub$row_index[i]) ]] <- ssub$value_text[i]
    }
    done_rows <- unique(cs$row_index[cs$day == today])
    
    # make sure observers exist for data rows (non-header)
    data_rows <- which(rv$data$Header == "")
    
    
    ui_list <- list(div(style = "margin-bottom:8px;", strong(format(Sys.Date(), "%A, %d.%m.%Y"))))
    
    for (r in seq_len(nrows)) {
      hdr <- as.character(rv$data$Header[r])
      task_text <- if ("Task" %in% names(rv$data)) as.character(rv$data$Task[r]) else ""
      is_header_row <- nzchar(hdr)
      
      if (is_header_row) {
        # Render header row (group label). Use a stronger visual style.
ui_list <- append(ui_list, list(
  div(
    style = "padding:6px 0; font-weight:600; background:#FFC000; margin-top:6px; padding-left:6px;",
    hdr
  )
))
      } else {
        # Render a task row (with checkbox/select)
        task_name <- if (nzchar(task_text)) task_text else paste0("Aufgabe ", r)
        done_id <- paste0("task_done_", r)
        opt_id  <- paste0("task_opt_", r)
        
        existing_val <- existing_for_today[[ as.character(r) ]] %||% ""
        is_done <- FALSE
        selected_opt <- ""

        # parsing existing_val (handle both old and new formats)
        if (nzchar(existing_val)) {
          v <- trimws(as.character(existing_val))
          if (startsWith(v, "\u2713")) {  # "\u2713" is the actual ✓ character
            is_done <- TRUE
            selected_opt <- ""
          } else if (v %in% TASK_OPTIONS) {
            # One of our full task options - show in dropdown
            is_done <- FALSE
            selected_opt <- v
          } else if (v %in% c("WE", "FT", "Ø", "W.e.", "ne", "D", "sB", "sQ")) {
            # Abbreviation format - map to full format
            abbrev_map <- c(
              "WE" = "WE (Wochenende)",
              "FT" = "FT (Feiertag)", 
              "Ø" = "Ø (An diesem Tag wurden keine Analysen gestartet)",
              "W.e." = "W.e. (Wartungspunkt ist in einer größeren Wartung enthalten)",
              "ne" = "ne (Nicht erforderlich)",
              "D" = "D (Gerät / Modul defekt)",
              "sB" = "sB (Siehe Bemerkungen)",
              "sQ" = "sQ (Siehe Quasi)"
            )
            is_done <- FALSE
            selected_opt <- abbrev_map[v] %||% ""
          } else {
            # Unknown format - default to not done
            is_done <- FALSE
            selected_opt <- ""
          }
        }

        row_ui <- div(style = "border-bottom:1px solid #eee; padding:8px 0;",
                      fluidRow(
                        column(5, div(strong(task_name))),
                        column(3, checkboxInput(done_id, label = "Erledigt", value = is_done, width = "100%")),
                        column(4, selectInput(opt_id, label = "Andere Option", 
                                            choices = c("", TASK_OPTIONS), 
                                            selected = if(is_done) "" else selected_opt, width = "100%"))
                      )
        )
        ui_list <- append(ui_list, list(row_ui))
      }
    }
    
    do.call(tagList, ui_list)
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
    rv$table_status <- load_cell_status(con, rv$current_device)
  })
  
  # Simple observer to update handsontable when table_status changes (like old code pattern)
  observe({
    req(rv$current_device, rv$data, rv$table_status)
    output$tableRH <- renderRHandsontable({
      cells_readonly_for_headers(rv$data, rv$user_initials, rv$table_status, rv$role, rv$invalid_days)
    })
  })
  
  output$download_table_csv <- downloadHandler(
    filename = function() paste0("Wartungsplan_", Sys.Date(), ".csv"),
    content = function(file) {
      req(rv$data)
      con <- pg_con(); on.exit(dbDisconnect(con), add = TRUE)
      over <- build_overlayed_table(rv$data, load_cell_status(con, rv$current_device))
      write.csv(over, file, row.names = FALSE, fileEncoding = "UTF-8")
    },
    contentType = "text/csv"
  )
  
  output$download_table_pdf <- downloadHandler(
    filename = function() paste0("Wartungsplan_", Sys.Date(), ".pdf"),
    content = function(file) {
      req(rv$data)
      
      # Check if required packages are available
      required_pkgs <- c("rmarkdown", "knitr", "kableExtra")
      missing_pkgs <- required_pkgs[!sapply(required_pkgs, requireNamespace, quietly = TRUE)]
      if (length(missing_pkgs) > 0) {
        stop("Missing required packages for PDF generation: ", paste(missing_pkgs, collapse = ", "))
      }
      
      con <- pg_con(); on.exit(dbDisconnect(con), add = TRUE)
      pdf_df <- build_overlayed_table(rv$data, load_cell_status(con, rv$current_device))
      
      # Create temporary directory for PDF generation
      temp_dir <- tempdir()
      owd <- setwd(temp_dir); on.exit(setwd(owd), add = TRUE)
      
      # Simplified R markdown template with better error handling
      rmd_content <- paste0("---
title: 'Wartungsplan'
date: '", format(Sys.Date(), "%d.%m.%Y"), "'
output:
  pdf_document:
    latex_engine: pdflatex
    keep_tex: no
    number_sections: false
header-includes:
  - \\usepackage{longtable}
  - \\usepackage{array}
  - \\usepackage[utf8]{inputenc}
  - \\usepackage[T1]{fontenc}
  - \\usepackage{lmodern}
  - \\setlength{\\tabcolsep}{2pt}
  - \\renewcommand{\\arraystretch}{1.0}
params:
  table_data: NA
---

\\tiny

```{r, echo=FALSE, results='asis'}
library(knitr)
library(kableExtra)

# Ensure table_data is available
if (is.null(params$table_data) || nrow(params$table_data) == 0) {
  cat('No data available for PDF generation.')
} else {
  # Create alignment specification
  nr_cols <- ncol(params$table_data)
  align_spec <- paste(rep('c', nr_cols), collapse='')
  
  # Generate table
  kable(params$table_data,
        format = 'latex',
        booktabs = FALSE,
        longtable = TRUE,
        align = align_spec,
        escape = FALSE) %>%
    kable_styling(latex_options = c('repeat_header'))
}
```
")
      
      # Write R markdown file
      temp_rmd <- file.path(temp_dir, "wartungsplan_report.Rmd")
      writeLines(rmd_content, temp_rmd, useBytes = TRUE)
      
      # Generate PDF
      tmp_pdf <- file.path(temp_dir, "wartungsplan_rendered.pdf")
      
      tryCatch({
        rmarkdown::render(
          temp_rmd,
          output_format = rmarkdown::pdf_document(latex_engine = "pdflatex"),
          output_file = basename(tmp_pdf),
          params = list(table_data = pdf_df),
          envir = new.env(parent = globalenv()),
          quiet = TRUE
        )
        
        # Check if PDF was created successfully
        if (!file.exists(tmp_pdf)) {
          stop("PDF generation failed - output file not created")
        }
        
        # Copy to final destination
        file.copy(tmp_pdf, file, overwrite = TRUE)
        
      }, error = function(e) {
        # Fallback: Try HTML to PDF conversion
        warning("LaTeX PDF generation failed, trying HTML method: ", e$message)
        
        tryCatch({
          # Create HTML version first
          html_content <- paste0("
<!DOCTYPE html>
<html>
<head>
  <meta charset='UTF-8'>
  <title>Wartungsplan</title>
  <style>
    body { font-family: Arial, sans-serif; font-size: 8pt; }
    table { border-collapse: collapse; width: 100%; }
    th, td { border: 1px solid black; padding: 2px; text-align: center; }
    .header-row { background-color: #fff7b2; font-weight: bold; }
  </style>
</head>
<body>
  <h1>Wartungsplan - ", format(Sys.Date(), "%d.%m.%Y"), "</h1>
  <table>
")
          
          # Add table headers
          html_content <- paste0(html_content, "<tr>")
          for (col_name in names(pdf_df)) {
            html_content <- paste0(html_content, "<th>", htmltools::htmlEscape(col_name), "</th>")
          }
          html_content <- paste0(html_content, "</tr>")
          
          # Add table rows
          for (i in seq_len(nrow(pdf_df))) {
            row_class <- if (nzchar(pdf_df$Header[i])) " class='header-row'" else ""
            html_content <- paste0(html_content, "<tr", row_class, ">")
            for (j in seq_len(ncol(pdf_df))) {
              cell_value <- htmltools::htmlEscape(as.character(pdf_df[i, j]))
              html_content <- paste0(html_content, "<td>", cell_value, "</td>")
            }
            html_content <- paste0(html_content, "</tr>")
          }
          
          html_content <- paste0(html_content, "
  </table>
</body>
</html>")
          
          # Write HTML file
          html_file <- file.path(temp_dir, "wartungsplan_report.html")
          writeLines(html_content, html_file, useBytes = TRUE)
          
          # Try to render HTML to PDF using rmarkdown
          rmarkdown::render(
            html_file,
            output_format = rmarkdown::html_document(),
            output_file = basename(sub("\\.html$", ".html", html_file)),
            quiet = TRUE
          )
          
          # Copy HTML file as final output (user can print to PDF)
          file.copy(html_file, sub("\\.pdf$", ".html", file), overwrite = TRUE)
          
        }, error = function(e2) {
          # Final fallback to CSV
          warning("HTML generation also failed, creating CSV instead: ", e2$message)
          csv_file <- sub("\\.pdf$", ".csv", file)
          write.csv(pdf_df, csv_file, row.names = FALSE, fileEncoding = "UTF-8")
          file.copy(csv_file, file, overwrite = TRUE)
        })
      })
    },
    contentType = "application/pdf"
  )
}

shinyApp(ui, server)
