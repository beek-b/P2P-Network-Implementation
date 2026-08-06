use std::{collections::HashMap, net::SocketAddr};
use tokio::sync::mpsc;

use crate::{network::{events_network::EventsPeerManager, peermanager::PeerId, ttype::{MessagePayload, TypeConnection}, wire::VersionMessage}, protocol::{error_protocol::ErrorHandleMessage, events_protocol::{EventsHandleMessage, EventsThreadProtocol}, ttype_protocol::{EventsCriticalCompletionHandleMessage, EventsWorkCycleHandleMessage}}};

pub struct HandleMessage {
    set_context_peers: HashMap<PeerId, PeerContextMetadata>,
    set_state_peers: HashMap<PeerId, PeerStateMetadata>,

    rx_channel_from_protocol: mpsc::Receiver<EventsThreadProtocol>,

    tx_channel_to_peermanager: mpsc::UnboundedSender<EventsHandleMessage>,
    rx_channel_from_peermanager: mpsc::UnboundedReceiver<EventsPeerManager>,
}

impl HandleMessage {
    pub fn create(rx_channel_from_protocol: mpsc::Receiver<EventsThreadProtocol>, tx_channel_to_peermanager: mpsc::UnboundedSender<EventsHandleMessage>, rx_channel_from_peermanager: mpsc::UnboundedReceiver<EventsPeerManager>) -> Self {
        Self { set_context_peers: HashMap::<PeerId, PeerContextMetadata>::new(), set_state_peers: HashMap::<PeerId, PeerStateMetadata>::new(), rx_channel_from_protocol, tx_channel_to_peermanager, rx_channel_from_peermanager }
    }

    pub async fn run(mut self) -> Result<(), ErrorHandleMessage> {
        let critical_state_loop = loop {
            let work_state = tokio::select! {
                incident_thread_protocol = self.rx_channel_from_protocol.recv() => {
                    match incident_thread_protocol {
                        Some(EventsThreadProtocol::Shutdown) => {
                            break EventsCriticalCompletionHandleMessage::IncidentConfirmationShutdown;
                        },
                        None => {
                            break EventsCriticalCompletionHandleMessage::IncidentChannelThreadProtocolClosed;
                        }
                    }
                },
                incident_peermanager = self.rx_channel_from_peermanager.recv() => {
                    match incident_peermanager {
                        Some(EventsPeerManager::ReadyMessage{peerid, message}) => {
                            EventsWorkCycleHandleMessage::IncidentHandleReadyMessage{peerid, message}
                        },
                        Some(EventsPeerManager::IncidentNewConnection(context)) => {
                            EventsWorkCycleHandleMessage::IncidentNewConnection(context)
                        },
                        Some(_) => {
                            EventsWorkCycleHandleMessage::IncidentNone
                        },
                        None => {
                            break EventsCriticalCompletionHandleMessage::IncidentChannelPeerManagerClosed;
                        }
                    }
                },
            };

            match work_state {
                EventsWorkCycleHandleMessage::IncidentNewConnection(context_new_connection) => {
                    if !self.set_context_peers.contains_key(&context_new_connection.peerid) {
                        let struct_context_peer = PeerContextMetadata::create(
                            context_new_connection.type_connection, 
                            context_new_connection.local_address, 
                            context_new_connection.remove_address
                        );

                        self.set_context_peers.insert(context_new_connection.peerid, struct_context_peer);
                    }

                    if !self.set_state_peers.contains_key(&context_new_connection.peerid) {
                        let struct_state_peer = PeerStateMetadata::create();

                        self.set_state_peers.insert(context_new_connection.peerid, struct_state_peer);

                    }
                    continue;
                }
                EventsWorkCycleHandleMessage::IncidentHandleReadyMessage{peerid, message} => {
                    if self.set_context_peers.contains_key(&peerid) {
                        if self.set_state_peers.contains_key(&peerid) {
                            log::info!("HandleMessage: получено новое сообщение на обработку: {:?}", message);
                        }
                    }
                    continue;
                },
                EventsWorkCycleHandleMessage::IncidentNone => {
                    continue;
                }
            }
        };

        match critical_state_loop {
            EventsCriticalCompletionHandleMessage::IncidentConfirmationShutdown => {
                log::info!("Задача HandleMessage успешно было завершено");

                Ok(())
            },
            EventsCriticalCompletionHandleMessage::IncidentChannelThreadProtocolClosed => {
                log::info!("Задача HandleMessage было завершено по ошибке Error: ThreadProtocolClosed");

                Err(ErrorHandleMessage::ErrorChannelThreadProtocolClosed)
            },
            EventsCriticalCompletionHandleMessage::IncidentChannelPeerManagerClosed => {
                log::info!("Задача HandleMessage было завершено по ошибке Error: PeerManagerClosed");

                Err(ErrorHandleMessage::ErrorChannelPeerManagerClosed)
            }
        }
    }
}

// pub fn handle_msg(peerid: PeerId, message: MessagePayload, context: &PeerContextMetadata, state: &mut PeerStateMetadata) {
//     match message {
//         MessagePayload::Version(version_message) => {
//             match context.type_connect {
//                 TypeConnection::IncomingConnection => {
//                     if !state.recv_version && !state.send_version && !state.recv_verack && !state.send_verack {
//                         state.recv_version = true;

//                         if version_message.validation_version_message() {
//                             // create message version

//                             // create message verack
//                         }
//                     }
//                 },
//                 TypeConnection::OutgoingConnection => {
                    
//                 }
//             }
//         }
//     }
// }

pub struct PeerContextMetadata {
    type_connect: TypeConnection,

    local_address: SocketAddr,
    remove_address: SocketAddr,
}

impl PeerContextMetadata {
    pub fn create(type_connect: TypeConnection, local_address: SocketAddr, remove_address: SocketAddr) -> Self {
        Self { type_connect, local_address, remove_address }
    }
}

pub struct PeerStateMetadata {
    send_version: bool,
    recv_verack: bool,
    send_verack: bool,
    recv_version: bool,

    completed_handshake: bool,
}

impl PeerStateMetadata {
    pub fn create() -> Self {
        Self { send_version: false, recv_verack: false, send_verack: false, recv_version: false, completed_handshake: false }
    }
}