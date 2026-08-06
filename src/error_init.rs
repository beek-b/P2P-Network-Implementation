use crate::{network::error_network::ErrorThreadNetwork, protocol::error_protocol::ErrorThreadProtocol};

#[derive(Debug)]
pub enum ErrorInit {
    ErrorThreadProtocol(std::thread::Result<ErrorThreadProtocol>),
    ErrorThreadNetwork(std::thread::Result<ErrorThreadNetwork>),

    ErrorThreadNetworkThreadProtocol{network: std::thread::Result<ErrorThreadNetwork>, protocol: std::thread::Result<ErrorThreadProtocol>}
}