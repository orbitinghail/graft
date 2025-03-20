#[tracing::instrument(name = "health")]
pub async fn handler() -> &'static str {
    "OK\n"
}
