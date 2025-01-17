//! Helpers for starting a node

use std::net::TcpListener;

/// Return an ephemeral, available port.
///
/// On unix systems, the port returned will be in the
/// TIME_WAIT state ensuring that the OS won't hand out this port for some grace period.
/// Callers should be able to bind to this port given they use SO_REUSEADDR.
/// TODO: Track ports we have used as an extra check?  Had to remove the old connect code- it was
/// broken on OSX at least.
pub fn get_available_tcp_port(host: &str) -> Option<u16> {
    const MAX_PORT_RETRIES: u32 = 1000;

    for _ in 0..MAX_PORT_RETRIES {
        if let Ok(port) = get_ephemeral_port(host) {
            return Some(port);
        }
    }

    panic!("Error: could not find an available port on host: {}\n", host);
}

fn get_ephemeral_port(host: &str) -> std::io::Result<u16> {
    // Request a random available port from the OS
    let listener = TcpListener::bind((host, 0))?;
    let addr = listener.local_addr()?;
    Ok(addr.port())
}
