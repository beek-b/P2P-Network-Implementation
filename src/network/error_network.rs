use crate::network::success_network::{SuccessTaskRead, SuccessTaskWrite};

#[derive(Debug)]
pub enum ErrorThreadNetwork {
    ErrorInputOutput(std::io::Error),

    ErrorPeerManager(ErrorPeerManager),
    ErrorLoopListen(ErrorLoopListen),

    ErrorPeerManagerErrorLoopListen{error_peermanager: ErrorPeerManager, error_looplisten: ErrorLoopListen},

    ErrorPeerManagerJoinHandleLoopListen{error_peermanager: ErrorPeerManager, error_joinhandle_looplisten: tokio::task::JoinError},
    ErrorLoopListenJoinHandlePeerManager{error_looplisten: ErrorLoopListen, error_joinhandle_peermanager: tokio::task::JoinError},

    ErrorPeerManagerJoinHandle(tokio::task::JoinError),
    ErrorLoopListenJoinHandle(tokio::task::JoinError),

    ErrorPeerManagerErrorLoopListenJoinHandle{error_joinhandle_peermanager: tokio::task::JoinError, error_joinhandle_looplisten: tokio::task::JoinError},
}

#[derive(Debug)]
pub enum ErrorPeerManager {
    ErrorChannelThreadNetworkClosed,
    ErrorChannelLoopListenClosed,
    ErrorChannelHandleMessageClosed,

    ErrorChannelAllPeersClosed,
}

#[derive(Debug)]
pub enum ErrorLoopListen {
    ErrorInputOutput(std::io::Error),
    ErrorChannelPeerManagerClosed,
    ErrorChannelThreadNetworkClosed,
}

#[derive(Debug)]
pub enum ErrorSendEvents {
    ErrorChannelPeerClosed,
    MissingPeer,
}

#[derive(Debug)]
pub enum ErrorPeer {
    ErrorRemoteAddress(std::io::Error),
    ErrorLocalAddress(std::io::Error),

    ErrorSuccessTaskReadErrorTaskWrite{success_read: SuccessTaskRead, error_write: ErrorTaskWrite},
    ErrorSuccessTaskWriteErrorTaskRead{error_read: ErrorTaskRead, success_write: SuccessTaskWrite},

    ErrorTaskReadTaskWrite{error_read: ErrorTaskRead, error_write: ErrorTaskWrite},

    ErrorSuccessTaskReadErrorJoinHandleTaskWrite{success_read: SuccessTaskRead, error_write: tokio::task::JoinError},
    ErrorSuccessTaskWriteErrorJoinHandleTaskRead{error_read: tokio::task::JoinError, success_write: SuccessTaskWrite},

    ErrorTaskReadErrorJoinHandleTaskWrite{error_read: ErrorTaskRead, error_write: tokio::task::JoinError},
    ErrorTaskWriteErrorJoinHandleTaskRead{error_write: ErrorTaskWrite, error_read: tokio::task::JoinError},

    ErrorJoinHandleTaskReadTaskWrite{error_read: tokio::task::JoinError, error_write: tokio::task::JoinError},

    ErrorSuccessTaskRead(SuccessTaskRead),
    ErrorSuccessTaskWrite(SuccessTaskWrite),
    
    ErrorTaskRead(ErrorTaskRead),
    ErrorTaskWrite(ErrorTaskWrite),

    ErrorTaskReadJoinHandle(tokio::task::JoinError),
    ErrorTaskWriteJoinHandle(tokio::task::JoinError),
}

#[derive(Debug)]
pub enum ErrorTaskRead {
    ErrorInputOutput(std::io::Error),

    ErrorChannelPeerClosed,
    ErrorParsing(ErrorParsing),
}

#[derive(Debug)]
pub enum ErrorParsing {
    ConversionError(std::array::TryFromSliceError),

    InvalidMagicBytes,
    InvalidCommand,
    LargePayloadSize,
    InvalidCheckSum,

    ErrorLowLevelParsing(ErrorLowLevelParsing),
}

#[derive(Debug)]
pub enum ErrorLowLevelParsing {
    ConversionError(std::array::TryFromSliceError),

    ErrorParseUtf8(std::string::FromUtf8Error),
    NotEnoughBytes,
}

#[derive(Debug)]
pub enum ErrorTaskWrite {
    ErrorInputOutput(std::io::Error),
    ErrorChannelPeerClosed
}

#[derive(Debug)]
pub enum ErrorCreateVersionMessage {
    ErrorTimestamp(std::time::SystemTimeError)
}