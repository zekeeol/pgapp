use pgapp_sdk::PgAppClient;
use std::time::Duration;

#[test]
fn exposes_expected_client_type() {
    let _ = std::any::type_name::<PgAppClient>();
    let _timeout = Duration::from_secs(1);
}
