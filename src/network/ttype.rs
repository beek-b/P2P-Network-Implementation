use std::net::SocketAddr;

use tokio::{net::TcpStream, task::JoinError};

use crate::network::{error_network::{ErrorPeerManager, ErrorLoopListen, ErrorPeer, ErrorTaskRead, ErrorTaskWrite}, peermanager::PeerId, success_network::{SuccessPeer, SuccessTaskRead, SuccessTaskWrite}, wire::{HeaderMessage, VerackMessage, VersionMessage}};

pub enum TypeCommandMessage {
    VersionMessage,
    VerackMessage,
    UncertainMessage,
}

#[derive(Debug)]
pub enum MessagePayload {
    Version(VersionMessage),
    Verack(VerackMessage),
}

pub struct FullMessage {
    pub header: HeaderMessage,
    pub payload: MessagePayload,
}

pub enum TypeConnection {
    IncomingConnection,
    OutgoingConnection,
}

pub struct StructContextConnection {
    pub peerid: PeerId,

    pub type_connection: TypeConnection,
    pub nonce: u64,

    pub local_address: SocketAddr,
    pub remove_address: SocketAddr,
}

impl StructContextConnection {
    pub fn new(peerid: PeerId, type_connection: TypeConnection, nonce: u64, local_address: SocketAddr, remove_address: SocketAddr) -> Self {
        Self { peerid, type_connection, nonce, local_address, remove_address }
    }
}

// ThreadNetwork
pub enum EventsWorkCycleThreadNetwork {

}

pub enum EventsCriticalCompletionThreadNetwork {
    IncidentConfirmationShutdown,

    IncidentChannelInitClosed,

    IncidentComplitedPeerManager(Result<(), ErrorPeerManager>),
    IncidentComplitedLoopListen(Result<(), ErrorLoopListen>),

    IncidentJoinHandlePeerManager(tokio::task::JoinError),
    IncidentJoinHandleLoopListen(tokio::task::JoinError),
}

// PeerManager
pub enum EventsWorkCyclePeerManager {
    IncidentThreadNetworkOutgoingConnection(SocketAddr),
    IncidentLoopListenIncomingConnection{socket: TcpStream, address: SocketAddr},

    IncidentProtocolReadyMessagePeer{peerid: PeerId, message: MessagePayload},

    IncidentPeerLowLevelMessage{peerid: PeerId, message: MessagePayload},

    IncidentComplitedPeerResult(Result<(PeerId, Result<SuccessPeer, ErrorPeer>), JoinError>),

    IncidentComplitedPeer,

    IncidentNone,
}

pub enum EventsCriticalCompletionPeerManager {
    IncidentConfirmationShutdown,

    IncidentChannelThreadNetworkClosed,
    IncidentChannelLoopListenClosed,
    IncidentChannelProtocolClosed,

    IncidentZeroPeers,
}

// Peer

pub enum EventsWorkCycle {
    IncidentPeerManagerReadyMessageTaskWrite(MessagePayload),

    IncidentTaskReadRawMessagePeerManager{peerid: PeerId, message: MessagePayload},
    IncidentNone,
}

pub enum EventsCriticalCompletion {
    IncidentConfirmationShutdown,

    IncidentChannelPeerManagerClosed,
    IncidentChannelTaskReadClosed,
    IncidentChannelTaskWriteClosed,

    IncidentComplitedTaskRead(Result<SuccessTaskRead, ErrorTaskRead>),
    IncidentComplitedTaskWrite(Result<SuccessTaskWrite, ErrorTaskWrite>),

    IncidentJoinHandleTaskRead(tokio::task::JoinError),
    IncidentJoinHandleTaskWrite(tokio::task::JoinError),
}