//! The bus's tables, created on start.

use sqlx::PgPool;

use crate::Error;

/// A schema name the bus can interpolate into SQL: a lowercase identifier.
pub(crate) fn validate(schema: &str) -> Result<(), Error> {
    let mut chars = schema.chars();
    let ok = chars
        .next()
        .is_some_and(|c| c.is_ascii_lowercase() || c == '_')
        && chars.all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
        && schema.len() <= 63;
    if ok {
        Ok(())
    } else {
        Err(Error::InvalidSchema(schema.to_string()))
    }
}

/// The oldest PostgreSQL with `xid8` and `pg_current_xact_id`.
const MINIMUM_SERVER: i64 = 130_000;

/// Refuses servers without `xid8`, which every query here needs.
pub(crate) fn supported(server_version_num: i64) -> Result<(), Error> {
    if server_version_num < MINIMUM_SERVER {
        return Err(Error::UnsupportedServer(server_version_num));
    }
    Ok(())
}

pub(crate) async fn check_server(pool: &PgPool) -> Result<(), Error> {
    let version: String = sqlx::query_scalar("SHOW server_version_num")
        .fetch_one(pool)
        .await?;
    supported(version.trim().parse().unwrap_or(0))
}

/// Creates the schema, the backlog and the state row if missing.
pub(crate) async fn migrate(pool: &PgPool, schema: &str) -> Result<(), Error> {
    let mut tx = pool.begin().await?;
    // Two processes starting together must not race on CREATE.
    sqlx::query("SELECT pg_advisory_xact_lock(hashtext($1))")
        .bind(format!("pg-bus:{schema}"))
        .execute(&mut *tx)
        .await?;
    for statement in [
        format!("CREATE SCHEMA IF NOT EXISTS {schema}"),
        format!(
            "CREATE TABLE IF NOT EXISTS {schema}.messages ( \
               id bigserial PRIMARY KEY, \
               xid xid8 NOT NULL DEFAULT pg_current_xact_id(), \
               channel text NOT NULL, \
               data jsonb NOT NULL, \
               audience text[], \
               created_at timestamptz NOT NULL DEFAULT now())"
        ),
        format!("CREATE INDEX IF NOT EXISTS messages_position ON {schema}.messages (xid, id)"),
        format!(
            "CREATE INDEX IF NOT EXISTS messages_channel ON {schema}.messages (channel, xid, id)"
        ),
        // Trimming by age.
        format!("CREATE INDEX IF NOT EXISTS messages_created_at ON {schema}.messages (created_at)"),
        // The newest position trim has deleted: cursors below it have gaps.
        format!(
            "CREATE TABLE IF NOT EXISTS {schema}.state ( \
               one boolean PRIMARY KEY DEFAULT true CHECK (one), \
               trimmed_xid xid8 NOT NULL DEFAULT '0', \
               trimmed_id bigint NOT NULL DEFAULT 0)"
        ),
        format!("INSERT INTO {schema}.state DEFAULT VALUES ON CONFLICT DO NOTHING"),
    ] {
        sqlx::query(&statement).execute(&mut *tx).await?;
    }
    tx.commit().await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::validate;

    #[test]
    fn needs_postgresql_13() {
        assert!(super::supported(130_000).is_ok());
        assert!(super::supported(160_010).is_ok());
        assert!(matches!(
            super::supported(120_017),
            Err(crate::Error::UnsupportedServer(120_017))
        ));
    }

    #[test]
    fn schema_names_are_plain_identifiers() {
        assert!(validate("pg_bus").is_ok());
        assert!(validate("bus2").is_ok());
        for bad in ["", "2bus", "Bus", "pg-bus", "a;drop", "x y"] {
            assert!(validate(bad).is_err(), "{bad}");
        }
    }
}
