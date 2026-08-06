use crate::{network::{peermanager::PeerId, ttype::{MessagePayload, StructContextConnection}}, protocol::error_protocol::ErrorHandleMessage};

pub enum EventsWorkCycleHandleMessage {
    IncidentHandleReadyMessage{peerid: PeerId, message: MessagePayload},
    IncidentNone,

    IncidentNewConnection(StructContextConnection),
}

pub enum EventsCriticalCompletionHandleMessage {
    IncidentConfirmationShutdown,

    IncidentChannelThreadProtocolClosed,
    IncidentChannelPeerManagerClosed

}


pub enum EventsCriticalCompletionThreadProtocol {
    IncidentConfirmationShutdown,

    IncidentChannelInitClosed,

    IncidentComplitedHandleMessage(Result<(), ErrorHandleMessage>),

    IncidentJoinHandleHandleMessage(tokio::task::JoinError),
}