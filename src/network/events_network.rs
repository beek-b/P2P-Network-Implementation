use std::net::SocketAddr;
use tokio::net::TcpStream;

use crate::network::peermanager::{PeerId};
use crate::network::ttype::{MessagePayload, TypeConnection, StructContextConnection};

pub enum EventsThreadNetwork {
    OutgoingConnection(SocketAddr),
    Shutdown,
}

pub enum EventsPeerManager {
    ReadyMessage{peerid: PeerId, message: MessagePayload},
    ReadyLowMessage{message: MessagePayload},

    Shutdown,



    IncidentNewConnection(StructContextConnection),
}

pub enum EventsLoopListen {
    IncomingConnection{socket: TcpStream, address: SocketAddr},
}

pub enum EventsPeer {
    Shutdown,

    ReadyLowMessage{peerid: PeerId, message: MessagePayload},
    LowLevelMessage(MessagePayload),
}

pub enum EventsTaskRead {
    LowRawMessage(MessagePayload)
}

pub enum EventsThreadNetworkProtocol {
    ExitCompletionThreadNetwork,
    ExitCompletionThreadProtocol,
}