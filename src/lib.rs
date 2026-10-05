//! pg-bus: a message bus on PostgreSQL.
//!
//! Messages are published inside the caller's transaction, kept in a
//! backlog table, and delivered to subscribers that resume from a cursor.
