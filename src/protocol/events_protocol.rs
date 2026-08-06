use crate::network::peermanager::{PeerId};
use crate::network::ttype::{MessagePayload};

pub enum EventsThreadProtocol {
    Shutdown,
}

pub enum EventsHandleMessage {
    ReadyMessage{peerid: PeerId, message: MessagePayload},
}