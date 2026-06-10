#============================================================================
# COMPREHENSIVE BACKUP SYSTEM
# Purpose: Backup all database tables and application files
# 
# IMPORTANT: This system preserves old project data!
# Tables are backed up selectively:
#   - App configuration (users, devices, etc.) - always backed up
#   - Old projects/data - always backed up (NEVER deleted)
#
# Usage:
#   source("backup_system.R")
#   complete_backup()  # Creates a full backup with timestamp
#============================================================================

library(DBI)
library(RPostgres)

# ============================================================================
# DATABASE CONNECTION
# ============================================================================

get_db_con <- function() {
  DBI::dbConnect(
    RPostgres::Postgres(),
    dbname   = Sys.getenv("DB_NAME"),
    host     = Sys.getenv("DB_HOST"),
    port     = as.integer(Sys.getenv("DB_PORT")),
    user     = Sys.getenv("DB_USER"),
    password = Sys.getenv("DB_PASSWORD"),
    sslmode  = "disable"
  )
}

# ============================================================================
# BACKUP TABLES (these should never be deleted)
# ============================================================================

# Tables containing old project/research data
OLD_DATA_TABLES <- c(
  "projects",
  "responses", 
  "submissions",
  "questionnaire_drafts",
  "lab_info",
  "invitation_codes"
)

# Application configuration tables (safe to restore)
APP_CONFIG_TABLES <- c(
  "users",
  "devices",
  "device_layout",
  "device_tables",
  "task_status",
  "device_cell_status",
  "device_task_remark",
  "app_images",
  "serial_number_history"
)

# All tables to backup
ALL_BACKUP_TABLES <- c(APP_CONFIG_TABLES, OLD_DATA_TABLES)

# ============================================================================
# BACKUP FUNCTIONS
# ============================================================================

#' Create a timestamped backup directory
#' 
#' @param backup_base_dir Base directory for backups (default: "backups")
#' 
#' @return Path to the new backup directory
create_backup_dir <- function(backup_base_dir = "backups") {
  if (!dir.exists(backup_base_dir)) {
    dir.create(backup_base_dir, recursive = TRUE)
  }
  
  timestamp <- format(Sys.time(), "%Y%m%d_%H%M%S")
  backup_dir <- file.path(backup_base_dir, paste0("backup_", timestamp))
  
  dir.create(backup_dir, recursive = TRUE)
  
  backup_dir
}

#' Backup a single table from the database
#' 
#' @param con Database connection
#' @param table_name Name of the table to backup
#' @param output_dir Directory to save the backup file
#' 
#' @return TRUE if successful, FALSE otherwise
backup_table <- function(con, table_name, output_dir) {
  tryCatch({
    message("  Backing up table: ", table_name)
    
    # Read entire table
    data <- DBI::dbReadTable(con, table_name)
    
    if (nrow(data) == 0) {
      message("    (empty table)")
    } else {
      message("    (", nrow(data), " rows)")
    }
    
    # Save as RDS (preserves data types)
    output_file <- file.path(output_dir, paste0(table_name, ".rds"))
    saveRDS(data, output_file)
    
    return(TRUE)
  }, error = function(e) {
    message("    ✗ ERROR: ", e$message)
    return(FALSE)
  })
}

#' Create backup information file
#' 
#' @param backup_dir Directory where backup is stored
#' @param tables_backed_up Vector of table names that were backed up
#' @param success Whether the backup was successful
create_backup_info <- function(backup_dir, tables_backed_up, success) {
  info_file <- file.path(backup_dir, "backup_info.txt")
  
  info_lines <- c(
    "========================================",
    "BACKUP INFORMATION",
    "========================================",
    paste0("Created: ", format(Sys.time(), "%Y-%m-%d %H:%M:%S")),
    paste0("Status: ", if (success) "✓ SUCCESS" else "✗ FAILED"),
    paste0("Total tables: ", length(tables_backed_up)),
    paste0("Directory: ", backup_dir),
    "",
    "TABLES BACKED UP:",
    "="
  )
  
  for (table_name in sort(tables_backed_up)) {
    info_lines <- c(info_lines, paste0("  - ", table_name))
  }
  
  info_lines <- c(
    info_lines,
    "",
    "IMPORTANT NOTES:",
    "===============",
    "✓ Old project data (projects, responses, etc.) is PRESERVED",
    "✓ Use this backup to restore app configuration",
    "✗ Do NOT restore if you want to keep recent changes",
    "",
    "TO RESTORE THIS BACKUP:",
    "======================",
    'source("restore_system.R")',
    paste0('restore_from_backup("', backup_dir, '")'),
    "",
    "========================================",
    ""
  )
  
  writeLines(info_lines, info_file)
}

# ============================================================================
# MAIN BACKUP FUNCTION
# ============================================================================

#' Create a complete backup of the database
#' 
#' This function:
#' 1. Creates a timestamped backup directory
#' 2. Backs up ALL tables (app config + old project data)
#' 3. Creates a backup info file
#' 4. Returns the backup directory path
#' 
#' SAFETY: This preserves old project data - it never deletes anything
#' 
#' @param backup_base_dir Base directory for backups (default: "backups")
#' 
#' @return Path to the created backup directory
#' 
#' @examples
#' # Create a backup
#' backup_path <- complete_backup()
#' message("Backup created at: ", backup_path)
#' 
complete_backup <- function(backup_base_dir = "backups") {
  message("\n========================================")
  message("CREATING COMPLETE DATABASE BACKUP")
  message("========================================\n")
  
  # Create backup directory
  backup_dir <- create_backup_dir(backup_base_dir)
  message("Backup directory: ", backup_dir, "\n")
  
  # Connect to database
  con <- get_db_con()
  on.exit(DBI::dbDisconnect(con), add = TRUE)
  
  # Get list of all tables in database
  all_tables <- DBI::dbListTables(con)
  
  message("Found ", length(all_tables), " tables in database\n")
  
  # Backup each table
  backed_up <- c()
  failed <- c()
  
  for (table_name in all_tables) {
    success <- backup_table(con, table_name, backup_dir)
    if (success) {
      backed_up <- c(backed_up, table_name)
    } else {
      failed <- c(failed, table_name)
    }
  }
  
  # Create info file
  create_backup_info(backup_dir, backed_up, length(failed) == 0)
  
  # Summary
  message("\n========================================")
  message("BACKUP COMPLETE")
  message("========================================\n")
  message("✓ Backed up: ", length(backed_up), " tables")
  
  if (length(failed) > 0) {
    message("✗ Failed: ", length(failed), " tables")
    message("  Failed tables: ", paste(failed, collapse = ", "))
  }
  
  message("\nBackup location:")
  message("  ", backup_dir)
  
  message("\n📦 IMPORTANT:")
  message("   Old project data is PRESERVED in this backup")
  message("   Use restore_system.R to selectively restore tables")
  
  message("\n========================================\n")
  
  invisible(backup_dir)
}

# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

#' List all available backups
#' 
#' @param backup_dir Directory where backups are stored (default: "backups")
#' 
list_all_backups <- function(backup_dir = "backups") {
  if (!dir.exists(backup_dir)) {
    message("No backup directory found")
    return(invisible(NULL))
  }
  
  backup_folders <- list.dirs(backup_dir, full.names = FALSE, recursive = FALSE)
  backup_folders <- backup_folders[grepl("^backup_\\d{8}_\\d{6}$", backup_folders)]
  
  if (length(backup_folders) == 0) {
    message("No backups found")
    return(invisible(NULL))
  }
  
  message("\n========================================")
  message("AVAILABLE BACKUPS")
  message("========================================\n")
  
  for (folder in sort(backup_folders, decreasing = TRUE)) {
    full_path <- file.path(backup_dir, folder)
    
    # Count tables
    rds_files <- list.files(full_path, pattern = "\\.rds$")
    
    # Size
    all_files <- list.files(full_path, recursive = TRUE, full.names = TRUE)
    total_size <- sum(file.size(all_files), na.rm = TRUE) / 1024^2
    
    message("📦 ", folder)
    message("   Tables: ", length(rds_files), " | Size: ", round(total_size, 2), " MB")
  }
  
  message("\n========================================\n")
  
  invisible(backup_folders)
}

#' Get size of a specific backup
#' 
#' @param backup_dir Path to backup directory
#' 
#' @return Size in MB
get_backup_size <- function(backup_dir) {
  if (!dir.exists(backup_dir)) return(0)
  
  all_files <- list.files(backup_dir, recursive = TRUE, full.names = TRUE)
  sum(file.size(all_files), na.rm = TRUE) / 1024^2
}

message("✓ Backup system loaded")
message("  Run complete_backup() to create a backup")
message("  Run list_all_backups() to see available backups")
