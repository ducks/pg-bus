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
    fn schema_names_are_plain_identifiers() {
        assert!(validate("pg_bus").is_ok());
        assert!(validate("bus2").is_ok());
        for bad in ["", "2bus", "Bus", "pg-bus", "a;drop", "x y"] {
            assert!(validate(bad).is_err(), "{bad}");
        }
    }
}
