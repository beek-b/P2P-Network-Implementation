use crate::{network::error_network::ErrorCreateVersionMessage};

#[derive(Debug)]
pub enum ErrorThreadProtocol {
    ErrorInputOutput(std::io::Error),

    ErrorHandleMessage(ErrorHandleMessage),

    ErrorJoinHandleHandleMessage(tokio::task::JoinError),
}

#[derive(Debug)]
pub enum ErrorHandleMessage {
    ErrorChannelThreadProtocolClosed,

    ErrorChannelPeerManagerClosed,
}

#[derive(Debug)]
pub enum ErrorHandle {
    PeerManagerClosed,
    ErrorCreateVersionMessage(ErrorCreateVersionMessage),

    ErrorInvalidVersionMessage,
    ErrorInvalidStateVersion,
    ErrorInvalidStateVerack,

    ErrorInvalidStateHandshake,
}