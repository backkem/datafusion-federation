use std::sync::Arc;

use datafusion::{
    catalog::schema::SchemaProvider,
    error::Result,
    execution::context::{SessionContext, SessionState},
};
use datafusion_federation::FederationAnalyzerRule;
use datafusion_federation_sql::{executor::CXExecutor, SQLFederationProvider, SQLSchemaProvider};

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let dsn = "sqlite://./chinook.sqlite".to_string();
    let known_tables: Vec<String> = ["Track", "Album", "Artist"]
        .iter()
        .map(|&x| x.into())
        .collect();

    let state = SessionContext::new().state();

    // Register FederationAnalyzer
    // TODO: Interaction with other analyzers & optimizers.
    let state = state.with_analyzer_rules(vec![Arc::new(FederationAnalyzerRule::new())]);

    // Register schema
    // TODO: table inference
    let executor = Arc::new(CXExecutor::new(dsn)?);
    let provider = Arc::new(SQLFederationProvider::new(executor));
    let schema_provider = Arc::new(SQLSchemaProvider::new(provider, known_tables).await?);
    overwrite_default_schema(&state, schema_provider)?;

    // Run query
    let ctx = SessionContext::new_with_state(state);
    let query = r#"SELECT * FROM `Track` limit 10"#;
    // let query = r#"SELECT
    //         t.TrackId,
    //         t.Name AS TrackName,
    //         a.Title AS AlbumTitle,
    //         ar.Name AS ArtistName
    //     FROM Track t
    //     JOIN Album a ON t.AlbumId = a.AlbumId
    //     JOIN Artist ar ON a.ArtistId = ar.ArtistId"#;
    let df = ctx.sql(query).await?;

    df.show().await
}

fn overwrite_default_schema(state: &SessionState, schema: Arc<dyn SchemaProvider>) -> Result<()> {
    let options = &state.config().options().catalog;
    let catalog = state
        .catalog_list()
        .catalog(options.default_catalog.as_str())
        .unwrap();

    catalog.register_schema(options.default_schema.as_str(), schema)?;

    Ok(())
}
