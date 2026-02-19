#!/bin/bash

# PostgreSQL Database Migration Script
# Usage:
#   Export: ./pg-migrate.sh export
#   Import: ./pg-migrate.sh import <dump_file>

set -e

DB_NAME="sootio"
DUMP_DIR="./db_dumps"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
DUMP_FILE="${DUMP_DIR}/${DB_NAME}_${TIMESTAMP}.sql.gz"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Export function
export_db() {
    log_info "Starting export of database: ${DB_NAME}"

    # Create dump directory if it doesn't exist
    mkdir -p "${DUMP_DIR}"

    # Prompt for connection details if not set via environment
    if [ -z "$PGHOST" ]; then
        read -p "Source host (default: localhost): " PGHOST
        PGHOST=${PGHOST:-localhost}
    fi

    if [ -z "$PGPORT" ]; then
        read -p "Source port (default: 5432): " PGPORT
        PGPORT=${PGPORT:-5432}
    fi

    if [ -z "$PGUSER" ]; then
        read -p "Source username (default: postgres): " PGUSER
        PGUSER=${PGUSER:-postgres}
    fi

    log_info "Connecting to ${PGHOST}:${PGPORT} as ${PGUSER}"
    log_info "Exporting to: ${DUMP_FILE}"

    # Create the dump with pg_dump
    # -Fc = custom format (compressed, allows parallel restore)
    # Or use plain SQL with gzip for more compatibility
    PGPASSWORD="${PGPASSWORD}" pg_dump \
        -h "${PGHOST}" \
        -p "${PGPORT}" \
        -U "${PGUSER}" \
        -d "${DB_NAME}" \
        --no-owner \
        --no-acl \
        --clean \
        --if-exists \
        2>&1 | gzip > "${DUMP_FILE}"

    if [ $? -eq 0 ]; then
        FILESIZE=$(du -h "${DUMP_FILE}" | cut -f1)
        log_info "Export completed successfully!"
        log_info "Dump file: ${DUMP_FILE} (${FILESIZE})"
        echo ""
        log_info "To transfer to another server, use:"
        echo "  scp ${DUMP_FILE} user@remote-server:/path/to/destination/"
    else
        log_error "Export failed!"
        exit 1
    fi
}

# Import function
import_db() {
    local IMPORT_FILE="$1"

    if [ -z "${IMPORT_FILE}" ]; then
        log_error "Please provide a dump file to import"
        echo "Usage: $0 import <dump_file.sql.gz>"
        exit 1
    fi

    if [ ! -f "${IMPORT_FILE}" ]; then
        log_error "File not found: ${IMPORT_FILE}"
        exit 1
    fi

    log_info "Starting import from: ${IMPORT_FILE}"

    # Prompt for connection details if not set via environment
    if [ -z "$PGHOST" ]; then
        read -p "Target host (default: localhost): " PGHOST
        PGHOST=${PGHOST:-localhost}
    fi

    if [ -z "$PGPORT" ]; then
        read -p "Target port (default: 5432): " PGPORT
        PGPORT=${PGPORT:-5432}
    fi

    if [ -z "$PGUSER" ]; then
        read -p "Target username (default: postgres): " PGUSER
        PGUSER=${PGUSER:-postgres}
    fi

    log_info "Connecting to ${PGHOST}:${PGPORT} as ${PGUSER}"

    # Check if database exists, create if not
    log_info "Checking if database exists..."

    if psql -h "${PGHOST}" -p "${PGPORT}" -U "${PGUSER}" -lqt | cut -d \| -f 1 | grep -qw "${DB_NAME}"; then
        log_warn "Database ${DB_NAME} already exists"
        read -p "Drop and recreate? (y/N): " CONFIRM
        if [ "$CONFIRM" = "y" ] || [ "$CONFIRM" = "Y" ]; then
            log_info "Dropping existing database..."
            psql -h "${PGHOST}" -p "${PGPORT}" -U "${PGUSER}" -d postgres -c "DROP DATABASE IF EXISTS \"${DB_NAME}\";"
        else
            log_info "Proceeding with import (will overwrite existing data)..."
        fi
    fi

    # Create database if it doesn't exist
    log_info "Creating database if not exists..."
    psql -h "${PGHOST}" -p "${PGPORT}" -U "${PGUSER}" -d postgres -c "CREATE DATABASE \"${DB_NAME}\";" 2>/dev/null || true

    # Import the dump
    log_info "Importing data (this may take a while)..."

    if [[ "${IMPORT_FILE}" == *.gz ]]; then
        gunzip -c "${IMPORT_FILE}" | psql -h "${PGHOST}" -p "${PGPORT}" -U "${PGUSER}" -d "${DB_NAME}"
    else
        psql -h "${PGHOST}" -p "${PGPORT}" -U "${PGUSER}" -d "${DB_NAME}" < "${IMPORT_FILE}"
    fi

    if [ $? -eq 0 ]; then
        log_info "Import completed successfully!"
    else
        log_error "Import completed with errors (some errors may be expected if objects already exist)"
    fi
}

# Show usage
usage() {
    echo "PostgreSQL Database Migration Script"
    echo ""
    echo "Usage:"
    echo "  $0 export              - Export database to a compressed SQL file"
    echo "  $0 import <file>       - Import database from a SQL file"
    echo ""
    echo "Environment variables (optional):"
    echo "  PGHOST    - Database host"
    echo "  PGPORT    - Database port"
    echo "  PGUSER    - Database username"
    echo "  PGPASSWORD - Database password (or use .pgpass file)"
    echo ""
    echo "Examples:"
    echo "  # Export from local database"
    echo "  $0 export"
    echo ""
    echo "  # Export with custom connection"
    echo "  PGHOST=db.example.com PGUSER=admin $0 export"
    echo ""
    echo "  # Import to new server"
    echo "  PGHOST=newserver.com $0 import ./db_dumps/sootio-postgres_20240101_120000.sql.gz"
}

# Main
case "${1}" in
    export)
        export_db
        ;;
    import)
        import_db "$2"
        ;;
    -h|--help|help)
        usage
        ;;
    *)
        usage
        exit 1
        ;;
esac
