{ pkgs, port ? 5443 }:

let
  postgresql = pkgs.postgresql_16;
in
{
  buildInputs = [ postgresql ];

  shellHook = ''
    export PGDATA="$PWD/.nix-postgres"
    export PGHOST="$PGDATA"
    export PGPORT="${toString port}"
    export DATABASE_URL="postgresql://localhost:${toString port}/pg_bus_test?host=$PGDATA"

    if [ ! -d "$PGDATA" ]; then
      echo "Initializing PostgreSQL data dir..."
      initdb --locale=C.UTF-8 --encoding=UTF8 -U postgres
    fi

    db_start() {
      if pg_ctl status > /dev/null 2>&1; then
        echo "PostgreSQL is already running on port ${toString port}"
        return
      fi
      # Prepared transactions on, for the stall test (a forgotten one holds
      # delivery back).
      pg_ctl start -w -l "$PGDATA/logfile" -o "-k $PGDATA -p ${toString port} -c max_prepared_transactions=10"
      psql -U postgres -d postgres -tc "SELECT 1 FROM pg_roles WHERE rolname = '$USER'" | grep -q 1 || \
        psql -U postgres -d postgres -c "CREATE ROLE \"$USER\" WITH LOGIN SUPERUSER CREATEDB"
      psql -U postgres -d postgres -tc "SELECT 1 FROM pg_database WHERE datname = 'pg_bus_test'" | grep -q 1 || \
        createdb -U postgres -O "$USER" pg_bus_test
    }

    db_stop() {
      pg_ctl stop
    }

    db_status() {
      pg_ctl status
    }
  '';
}
