
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
  
  # Seed devices (upsert so labels are applied/updated)
  dbExecute(con, "
    INSERT INTO devices (device_id, label) VALUES
      ('g1', 'PFA /Cobas 411/ Multiplate, MC1'),
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

# =================== Helpers ===================
# Helper: compute invalid days for selected month/year
calc_invalid_days <- function(year, month) {
  start <- as.Date(sprintf("%04d-%02d-01", year, month))
  nextm <- if (month == 12) as.Date(sprintf("%04d-01-01", year + 1))
  else as.Date(sprintf("%04d-%02d-01", year, month + 1))
  dim <- as.integer(format(nextm - 1, "%d"))
  setdiff(1:31, 1:dim)
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
  
  # Default builder for other devices (unchanged)
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
  
  # If this is g1 but Task column is empty for all data rows, replace with the custom g1 template
  if (identical(device_id, "g1")) {
    data_rows <- which(df$Header == "")
    has_task_text <- FALSE
    if (length(data_rows)) {
      has_task_text <- any(nzchar(df$Task[data_rows]))
    }
    if (!has_task_text) {
      # Merge template into existing table preserving day columns if possible
      df_template <- create_initial_table("g1")
      # If existing table has same or more rows we inject Task texts for matching rows,
      # otherwise append extra template rows.
      n_existing <- nrow(df)
      n_template <- nrow(df_template)
      n_min <- min(n_existing, n_template)
      for (i in seq_len(n_min)) {
        if (df$Header[i] == "" && nzchar(df_template$Task[i]) && !nzchar(df$Task[i])) {
          df$Task[i] <- df_template$Task[i]
        }
      }
      if (n_template > n_existing) {
        extra <- df_template[(n_existing+1):n_template, , drop = FALSE]
        # align columns
        missing_cols <- setdiff(names(df), names(extra))
        if (length(missing_cols)) for (c in missing_cols) extra[[c]] <- ""
        extra <- extra[, names(df), drop = FALSE]
        df <- rbind(df, extra)
      }
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
  order_cols(out)
}

# ---- Handsontable renderer (per-cell locking) ----
cells_readonly_for_headers <- function(df, current_user_initials, cell_status_df, role = "user", invalid_days = integer()) {
  df_show <- overlay_for_render(df, cell_status_df)
  
  rh <- rhandsontable(df_show) %>%
    hot_cols(columnSorting = TRUE, manualColumnResize = TRUE) %>%
    hot_table(highlightCol = TRUE, highlightRow = TRUE, rowHeaders = FALSE, comments = TRUE)
  
  # lock header rows entirely (also lock Task column for header rows)
  header_rows <- which(df_show$Header != "")
  if (length(header_rows)) {
    for (r in header_rows) {
      rh <- rh %>% hot_cell(r - 1, "Header", readOnly = TRUE)
      if ("Task" %in% names(df_show)) rh <- rh %>% hot_cell(r - 1, "Task", readOnly = TRUE)
      for (d in as.character(1:31)) {
        if (d %in% names(df_show)) rh <- rh %>% hot_cell(r - 1, d, readOnly = TRUE)
      }
    }
  }
  
  # lock invalid days (by month/year selection)
  if (length(invalid_days)) {
    for (d in as.character(invalid_days)) {
      if (d %in% names(df_show)) {
        for (r in seq_len(nrow(df_show))) {
          rh <- rh %>% hot_cell(r - 1, d, readOnly = TRUE, comment = "Ungültig für diesen Monat")
        }
      }
    }
  }
  
  # add per-cell locks + comments
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
              menuItem("Checkliste", tabName = "checklist", icon = icon("tasks"))
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
    ))
  ),
  conditionalPanel(
    condition = "output.is_authed",
    tabItems(
      tabItem(tabName = "hub",
              h3("Diagnostikzentrum"),
              h4("Universitätsinstitut für Klinische Chemie und Laboratoriumsmedizin"),
              br(),
              uiOutput("hub_buttons")
      ),
      tabItem(tabName = "checklist",
              uiOutput("device_title"),
              br(),
              fluidRow(
                column(
                  width = 4,
                  box(width = 12, title = "Heutige Aufgaben (Heute)", status = "primary", solidHeader = TRUE,
                      uiOutput("today_tasks")
                  ),
                  box(width = 12, title = "Aktionen", status = "primary", solidHeader = TRUE,
                      selectInput("section", "Reihe unter Abschnitt hinzufügen:",
                                  choices = c("Täglich", "Wöchentlich", "14-tägig", "Monatlich", "Bei Bedarf")),
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
                      actionButton("mark_today", "Heute abhaken ✓", class = "btn btn-primary"),
                      br(), br(),
                      actionButton("save_db", "In DB speichern", class = "btn btn-primary"),
                      br(), br(),
                      downloadButton("download_table_csv", "CSV herunterladen"),
                      downloadButton("download_table_pdf", "PDF herunterladen")
                  )
                ),
                column(
                  width = 8,
                  box(width = 12, title = "Checkliste", status = "primary", solidHeader = TRUE,
                      uiOutput("pending_alert"),
                      rHandsontableOutput("tableRH")
                  )
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
    data = NULL,
    table_status = data.frame(),
    invalid_days = integer(),
    task_obs_ids = character(0)
  )
  
  output$is_authed <- reactive({ rv$authed })
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
        
        # Wrap button in a div to get spacing; make button full-width
        div(style = "margin-bottom:8px;",
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
      cells_readonly_for_headers(rv$data, rv$user_initials, rv$table_status, rv$role, rv$invalid_days)
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
    
    updateTabItems(session, "tabs", "checklist")
    output$tableRH <- renderRHandsontable({
      cells_readonly_for_headers(rv$data, rv$user_initials, rv$table_status, rv$role, rv$invalid_days)
    })
  }
  
  output$device_title <- renderUI({
    req(rv$current_device)
    con <- pg_con(); on.exit(dbDisconnect(con), add = TRUE)
    lab <- DBI::dbGetQuery(con, "SELECT label FROM devices WHERE device_id = $1", params = list(rv$current_device))$label
    if (length(lab) == 0 || is.na(lab)) lab <- rv$current_device
    tagList(
      h3(paste("Checkliste ·", lab)),
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
    next_header_idx <- suppressWarnings(min(which((1:nrow(rv$data)) > max(header_idx) & rv$data$Header != "")))
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
    
    rv$data <- rbind(rv$data[1:insert_idx, ], new_row, rv$data[(insert_idx+1):nrow(rv$data), ])
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
        other_id <- paste0("task_other_", rr)
        
        # when checkbox toggled
        observeEvent(input[[cid]], {
          req(rv$current_device, rv$data)
          val <- isTRUE(input[[cid]])
          con <- pg_con(); on.exit(dbDisconnect(con), add = TRUE)
          today <- as.integer(format(Sys.Date(), "%d"))
          who <- rv$user_initials %||% rv$user
          is_admin <- identical(rv$role, "admin")
          
          sel <- isolate(input[[oid]] %||% "1")
          other_txt <- isolate(input[[other_id]] %||% "")
          
          if (val) {
            txt <- if (identical(sel, "Andere")) {
              if (nzchar(other_txt)) other_txt else "Andere"
            } else sel
            cell_val <- paste0("\u2713 ", txt, if (nzchar(who)) paste0(" (", who, ")") else "")
            # Only write if different
            cur <- ""
            ss <- rv$table_status
            if (nrow(ss)) {
              sel_row <- ss$row_index == rr & ss$day == today
              if (any(sel_row)) cur <- ss$value_text[which(sel_row)[1]]
            }
            if (!identical(cur, cell_val)) upsert_cell(con, rv$current_device, rr, today, cell_val, who)
          } else {
            # delete only if present
            cur <- ""
            ss <- rv$table_status
            if (nrow(ss)) {
              sel_row <- ss$row_index == rr & ss$day == today
              if (any(sel_row)) cur <- ss$value_text[which(sel_row)[1]]
            }
            if (nzchar(cur)) delete_cell_if_owner(con, rv$current_device, rr, today, who, is_admin)
          }
          rv$table_status <- load_cell_status(con, rv$current_device)
          output$tableRH <- renderRHandsontable({
            cells_readonly_for_headers(rv$data, rv$user_initials, rv$table_status, rv$role, rv$invalid_days)
          })
        }, ignoreInit = TRUE)
        
        # when select input changed -> if already checked, update the cell (use isolate)
        observeEvent(input[[oid]], {
          req(rv$current_device, rv$data)
          val_checked <- isTRUE(isolate(input[[cid]]))
          if (!val_checked) return()
          con <- pg_con(); on.exit(dbDisconnect(con), add = TRUE)
          today <- as.integer(format(Sys.Date(), "%d"))
          who <- rv$user_initials %||% rv$user
          sel <- isolate(input[[oid]] %||% "1")
          other_txt <- isolate(input[[other_id]] %||% "")
          
          txt <- if (identical(sel, "Andere")) {
            if (nzchar(other_txt)) other_txt else "Andere"
          } else sel
          cell_val <- paste0("\u2713 ", txt, if (nzchar(who)) paste0(" (", who, ")") else "")
          cur <- ""
          ss <- rv$table_status
          if (nrow(ss)) {
            sel_row <- ss$row_index == rr & ss$day == today
            if (any(sel_row)) cur <- ss$value_text[which(sel_row)[1]]
          }
          if (!identical(cur, cell_val)) {
            upsert_cell(con, rv$current_device, rr, today, cell_val, who)
            rv$table_status <- load_cell_status(con, rv$current_device)
            output$tableRH <- renderRHandsontable({
              cells_readonly_for_headers(rv$data, rv$user_initials, rv$table_status, rv$role, rv$invalid_days)
            })
          }
        }, ignoreInit = TRUE)
        
        # when "Andere" text changed -> update only when checked & selected opt == "Andere"
        observeEvent(input[[other_id]], {
          req(rv$current_device, rv$data)
          val_checked <- isTRUE(isolate(input[[cid]]))
          sel <- isolate(input[[oid]] %||% "1")
          if (!val_checked || !identical(sel, "Andere")) return()
          con <- pg_con(); on.exit(dbDisconnect(con), add = TRUE)
          today <- as.integer(format(Sys.Date(), "%d"))
          who <- rv$user_initials %||% rv$user
          other_txt <- isolate(input[[other_id]] %||% "")
          txt <- if (nzchar(other_txt)) other_txt else "Andere"
          cell_val <- paste0("\u2713 ", txt, if (nzchar(who)) paste0(" (", who, ")") else "")
          cur <- ""
          ss <- rv$table_status
          if (nrow(ss)) {
            sel_row <- ss$row_index == rr & ss$day == today
            if (any(sel_row)) cur <- ss$value_text[which(sel_row)[1]]
          }
          if (!identical(cur, cell_val)) {
            upsert_cell(con, rv$current_device, rr, today, cell_val, who)
            rv$table_status <- load_cell_status(con, rv$current_device)
            output$tableRH <- renderRHandsontable({
              cells_readonly_for_headers(rv$data, rv$user_initials, rv$table_status, rv$role, rv$invalid_days)
            })
          }
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
    ensure_task_observers(data_rows)
    
    ui_list <- list(div(style = "margin-bottom:8px;", strong(format(Sys.Date(), "%A, %d.%m.%Y"))))
    
    for (r in seq_len(nrows)) {
      hdr <- as.character(rv$data$Header[r])
      task_text <- if ("Task" %in% names(rv$data)) as.character(rv$data$Task[r]) else ""
      is_header_row <- nzchar(hdr)
      
      if (is_header_row) {
        # Render header row (group label). Use a stronger visual style.
        ui_list <- append(ui_list, list(div(style = "padding:6px 0; font-weight:600; background:#f5f5f5; margin-top:6px; padding-left:6px;", hdr)))
      } else {
        # Render a task row (with checkbox/select)
        task_name <- if (nzchar(task_text)) task_text else paste0("Aufgabe ", r)
        done_id <- paste0("task_done_", r)
        opt_id  <- paste0("task_opt_", r)
        other_id <- paste0("task_other_", r)
        
        existing_val <- existing_for_today[[ as.character(r) ]] %||% ""
        is_done <- r %in% done_rows
        selected_opt <- "1"
        other_txt <- ""
        if (nzchar(existing_val)) {
          v <- existing_val
          v <- sub("^\\s*\u2713\\s*", "", v)
          v <- sub("\\s*\\([^)]*\\)\\s*$", "", v)
          parts <- strsplit(v, " - ", fixed = TRUE)[[1]]
          if (length(parts) >= 2) {
            selected_opt <- "Andere"
            other_txt <- paste(parts[-1], collapse = " - ")
          } else {
            token <- trimws(parts[[1]])
            if (token %in% c("1","2","3","4")) selected_opt <- token else { selected_opt <- "Andere"; other_txt <- token }
          }
          is_done <- TRUE
        }
        
        row_ui <- div(style = "border-bottom:1px solid #eee; padding:8px 0;",
                      fluidRow(
                        column(7, div(strong(task_name))),
                        column(3, checkboxInput(done_id, label = "Erledigt", value = is_done, width = "100%")),
                        column(2, selectInput(opt_id, label = NULL, choices = c("1","2","3","4","Andere"), selected = selected_opt, width = "100%"))
                      ),
                      conditionalPanel(condition = sprintf("input.%s == 'Andere'", opt_id),
                                       textInput(other_id, label = "Weitere Angaben", value = other_txt, width = "100%"))
        )
        ui_list <- append(ui_list, list(row_ui))
      }
    }
    
    do.call(tagList, ui_list)
  })
  
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
      con <- pg_con(); on.exit(dbDisconnect(con), add = TRUE)
      pdf_df <- build_overlayed_table(rv$data, load_cell_status(con, rv$current_device))
      owd <- setwd(tempdir()); on.exit(setwd(owd), add = TRUE)
      rmd_content <- "
---
title: 'Wartungsplan'
date: '`r format(Sys.Date(), \"%d.%m.%Y\")`'
output:
  pdf_document:
    latex_engine: xelatex
    keep_tex: no
    number_sections: false
header-includes:
  - \\usepackage{longtable}
  - \\usepackage{pdflscape}
  - \\usepackage{array}
  - \\usepackage[T1]{fontenc}
  - \\usepackage{lmodern}
  - \\setlength{\\tabcolsep}{2pt}
  - \\renewcommand{\\arraystretch}{1.0}
params:
  table_data: NA
---

\\tiny

```{r, echo=FALSE, results='asis'}
nr_cols <- ncol(params$table_data)
align_spec <- paste(rep('c', nr_cols), collapse='')
knitr::kable(
  params$table_data,
  format    = 'latex',
  booktabs  = FALSE,
  longtable = TRUE,
  align     = align_spec,
  linesep   = '\\\\hline'
)
```"
  temp_rmd <- file.path(tempdir(), "wartungsplan_report.Rmd")
  writeLines(rmd_content, temp_rmd, useBytes = TRUE)
  tmp_pdf <- file.path(tempdir(), "wartungsplan_rendered.pdf")
  rmarkdown::render(
    temp_rmd,
    output_format = rmarkdown::pdf_document(latex_engine = "xelatex"),
    output_file = basename(tmp_pdf),
    params = list(table_data = pdf_df),
    envir = new.env(parent = globalenv())
  )
  file.copy(tmp_pdf, file, overwrite = TRUE)
    },
  contentType = "application/pdf"
  )
}

shinyApp(ui, server)