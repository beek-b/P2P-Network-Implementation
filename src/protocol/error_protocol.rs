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