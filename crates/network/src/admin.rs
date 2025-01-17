use axum::{extract::Extension, http::StatusCode, routing::get, Json, Router};
use consensus_metrics::monitored_future;
use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr, TcpListener},
    time::Duration,
};
use tn_types::{Noticer, TaskManager};
use tokio::time::sleep;
use tracing::{error, info};

pub fn start_admin_server(
    port: u16,
    network: anemo::Network,
    rx_shutdown: Noticer,
    task_manager: &TaskManager,
) {
    let mut router =
        Router::new().route("/peers", get(get_peers)).route("/known_peers", get(get_known_peers));

    router = router.layer(Extension(network));

    let socket_address = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port);
    info!(
        address =% socket_address,
        "starting admin server"
    );

    let handle = axum_server::Handle::new();
    let shutdown_handle = handle.clone();

    // Spawn a task to shutdown server.
    task_manager.spawn_task(
        "admin server shutdown",
        monitored_future!(async move {
            _ = rx_shutdown.await;
            handle.clone().shutdown();
        }),
    );

    task_manager.spawn_task(
        "admin server",
        monitored_future!(
            async move {
                // retry a few times before quitting
                let mut total_retries = 10;

                loop {
                    total_retries -= 1;

                    match TcpListener::bind(socket_address) {
                        Ok(listener) => {
                            axum_server::from_tcp(listener)
                                .handle(shutdown_handle)
                                .serve(router.into_make_service())
                                .await
                                .unwrap_or_else(|err| {
                                    panic!("Failed to boot admin {}: {err}", socket_address)
                                });

                            return;
                        }
                        Err(err) => {
                            if total_retries == 0 {
                                error!("{}", err);
                                panic!("Failed to boot admin {}: {}", socket_address, err);
                            }

                            error!("{}", err);

                            // just sleep for a bit before retrying in case the port
                            // has not been de-allocated
                            sleep(Duration::from_secs(1)).await;

                            continue;
                        }
                    }
                }
            },
            "AdminServerTask"
        ),
    );
}

async fn get_peers(
    Extension(network): Extension<anemo::Network>,
) -> (StatusCode, Json<Vec<String>>) {
    (StatusCode::OK, Json(network.peers().iter().map(|x| x.to_string()).collect()))
}

async fn get_known_peers(
    Extension(network): Extension<anemo::Network>,
) -> (StatusCode, Json<Vec<String>>) {
    (
        StatusCode::OK,
        Json(
            network
                .known_peers()
                .get_all()
                .iter()
                .map(|x| format!("{}: {:?}", x.peer_id, x.address))
                .collect(),
        ),
    )
}
