use std::{collections::HashMap, net::SocketAddr};
use tokio::{sync::mpsc, time::{sleep, Duration}};

use crate::{network::{events_network::EventsPeerManager, peermanager::PeerId, ttype::{MessagePayload, TypeConnection}, wire::{VersionMessage, VerackMessage}}, protocol::{error_protocol::{ErrorHandleMessage, ErrorHandle}, events_protocol::{EventsHandleMessage, EventsThreadProtocol}, ttype_protocol::{EventsCriticalCompletionHandleMessage, EventsWorkCycleHandleMessage}}};

pub struct HandleMessage {
    set_context_peers: HashMap<PeerId, PeerContextMetadata>,
    set_state_peers: HashMap<PeerId, PeerStateMetadata>,

    rx_channel_from_protocol: mpsc::Receiver<EventsThreadProtocol>,

    tx_channel_to_peermanager: mpsc::Sender<EventsHandleMessage>,
    rx_channel_from_peermanager: mpsc::Receiver<EventsPeerManager>,
}

impl HandleMessage {
    pub fn create(rx_channel_from_protocol: mpsc::Receiver<EventsThreadProtocol>, tx_channel_to_peermanager: mpsc::Sender<EventsHandleMessage>, rx_channel_from_peermanager: mpsc::Receiver<EventsPeerManager>) -> Self {
        Self { set_context_peers: HashMap::<PeerId, PeerContextMetadata>::new(), set_state_peers: HashMap::<PeerId, PeerStateMetadata>::new(), rx_channel_from_protocol, tx_channel_to_peermanager, rx_channel_from_peermanager }
    }

    pub async fn run(mut self) -> Result<(), ErrorHandleMessage> {
        log::info!("Цикл событие handlemessage был успешно запущено");
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
                        Some(EventsPeerManager::IncidentCompletedAllPeer) => {
                            EventsWorkCycleHandleMessage::IncidentCompletedAllPeer
                        },
                        Some(EventsPeerManager::IncidentCompletedPeer(peerid)) => {
                            EventsWorkCycleHandleMessage::IncidentCompletedPeer(peerid)
                        }
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
                            context_new_connection.nonce,
                            context_new_connection.local_address, 
                            context_new_connection.remove_address
                        );

                        self.set_context_peers.insert(context_new_connection.peerid, struct_context_peer);

                        log::info!("HandleMessage успешно создало PeerContextMetadata и добавило по id: {}", context_new_connection.peerid);
                    }

                    if !self.set_state_peers.contains_key(&context_new_connection.peerid) {
                        let struct_state_peer = PeerStateMetadata::create();

                        self.set_state_peers.insert(context_new_connection.peerid, struct_state_peer);

                        log::info!("HandleMessage успешно создало PeerStateMetadata и добавило по id: {}", context_new_connection.peerid);

                    }

                    if self.set_context_peers.contains_key(&context_new_connection.peerid) && self.set_state_peers.contains_key(&context_new_connection.peerid) {
                        let context_peers = &self.set_context_peers[&context_new_connection.peerid];
                        match context_peers.type_connect {
                            TypeConnection::IncomingConnection => {
                                ()
                            },
                            TypeConnection::OutgoingConnection => {
                                //sleep(Duration::from_secs(5)).await;
                                log::info!("HandleMessage определил что соединение является исходящий и создал сообщение Version");

                                match VersionMessage::build_version_message(context_peers.local_address.clone(), context_peers.remove_address.clone(), context_peers.nonce) {
                                    Ok(new_message_version) => {
                                        log::info!("HandleMessage успешно создало сообщение VersionMessage: {:?}, bytes: {:?}", new_message_version, new_message_version.serialize_version_message());

                                        match self.tx_channel_to_peermanager.send(EventsHandleMessage::ReadyMessage {peerid: context_new_connection.peerid, message: MessagePayload::Version(new_message_version)}).await {
                                            Ok(_) => {
                                                log::info!("HandleMessage успешно отправил сообщение ReadyMessage PeerManager");

                                                if let Some(state_peer) = self.set_state_peers.get_mut(&context_new_connection.peerid) {
                                                    if !state_peer.send_version && !state_peer.recv_version && !state_peer.send_verack && !state_peer.recv_verack {
                                                        state_peer.send_version = true;
                                                    }
                                                }
                                            },
                                            Err(_) => {
                                                break EventsCriticalCompletionHandleMessage::IncidentChannelPeerManagerClosed;
                                            }
                                        }
                                    },
                                    Err(e) => {
                                        log::error!("HandleMessage получил ошибку во время создание сообщение Version, ошибка: {:?}", e);
                                    }
                                }
                            }
                        }
                    }

                    continue;
                }
                EventsWorkCycleHandleMessage::IncidentHandleReadyMessage{peerid, message} => {
                    if self.set_context_peers.contains_key(&peerid) {
                        if self.set_state_peers.contains_key(&peerid) {
                            log::info!("HandleMessage: получено новое сообщение на обработку: {:?}", message);

                            let context_peer = &self.set_context_peers[&peerid];

                            match handle_msg(peerid, message, &context_peer, &mut self.set_state_peers.get_mut(&peerid).unwrap(), &mut self.tx_channel_to_peermanager).await {
                                Ok(_) => {
                                    log::info!("HandleMessage Успешно обработал входящию сообщение");
                                    log::info!("PeerStateMetadata: id: {}, state: {:?}", peerid, &self.set_state_peers[&peerid]);
                                },
                                Err(error) => {
                                    match error {
                                        ErrorHandle::ErrorInvalidStateVersion => {
                                            log::error!("HandleMessage во время обработки получил не правильную последовательность сообщение VersionMessage и перешел к завершение пира");
                                        },
                                        ErrorHandle::ErrorInvalidStateVerack => {
                                            log::error!("HandleMessage во время обработки получил не правильную последовательность сообщение VerackMessage и перешел к завершение пира");
                                        },
                                        _=> {
                                            log::error!("HandleMessage получил ошибку во время обработки входящего сообщение, ошибка: {:?}", error);
                                        }
                                    }
                                }
                            }
                        }
                    }
                    continue;
                },
                EventsWorkCycleHandleMessage::IncidentCompletedAllPeer => {
                    self.set_context_peers.clear();
                    self.set_state_peers.clear();
                    
                    log::info!("PeerContextMetadata и PeerStateMetadata полностью очищенно из за завершение всех активных пиров");
                },
                EventsWorkCycleHandleMessage::IncidentCompletedPeer(peerid) => {
                    let context_rm_peer = self.set_context_peers.remove(&peerid);
                    let state_rm_peer = self.set_state_peers.remove(&peerid);

                    match (context_rm_peer, state_rm_peer) {
                        (Some(_), Some(_)) => {
                            log::info!("HandleMessage успешно удалил PeerContextMetadata и PeerStateMetadata по id: {}", peerid);
                        },
                        (Some(_), None) => {
                            log::info!("HandleMessage удалил PeerContextMetadata, отсутствует PeerStateMetadata по id: {}",peerid);
                        },
                        (None, Some(_)) => {
                            log::info!("HandleMessage удалил PeerStateMetadata, отсутствует PeerContextMetadata по id: {}",peerid);
                        },
                        (None, None) => {
                            log::info!("HandleMessage удалить ничего, отсутствует PeerContextMetadata и PeerStateMetadata по id: {}",peerid);
                        }
                    }
                }
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

pub async fn handle_msg(peerid: PeerId, message: MessagePayload, context: &PeerContextMetadata, state: &mut PeerStateMetadata, tx_channel_peermanager: &mut mpsc::Sender<EventsHandleMessage>) -> Result<(), ErrorHandle> {
    match message {
        MessagePayload::Version(version_message) => {
            match context.type_connect {
                TypeConnection::IncomingConnection => {
                    if !state.recv_version && !state.send_version && !state.recv_verack && !state.send_verack {
                        if version_message.validation_version_message() {
                            state.recv_version = true;

                            // create message version
                            match VersionMessage::build_version_message(context.local_address.clone(), context.remove_address.clone(), context.nonce) {
                                Ok(new_version_message) => {
                                    match tx_channel_peermanager.send(EventsHandleMessage::ReadyMessage { peerid: peerid, message: MessagePayload::Version(new_version_message) }).await {
                                        Ok(_) => {
                                            state.send_version = true;
                                            // create message Verack
                                            let new_verack_message = VerackMessage::build_verack_message();
                                            match tx_channel_peermanager.send(EventsHandleMessage::ReadyMessage { peerid, message: MessagePayload::Verack(new_verack_message) }).await {
                                                Ok(_) => {
                                                    state.send_verack = true;

                                                    Ok(())
                                                },
                                                Err(_) => {
                                                    Err(ErrorHandle::PeerManagerClosed)
                                                }
                                            }
                                        },
                                        Err(_) => {
                                            Err(ErrorHandle::PeerManagerClosed)
                                        }
                                    }
                                },
                                Err(e) => {
                                    log::error!("HandleMessage получил ошибку во время создание сообщение Version, ошибка: {:?}", e);

                                    Err(ErrorHandle::ErrorCreateVersionMessage(e))
                                }
                            }
                        } else {
                            Err(ErrorHandle::ErrorInvalidVersionMessage)
                        }
                    } else {
                        if tx_channel_peermanager.send(EventsHandleMessage::IncidetViolentComplitedPeer(peerid)).await.is_err() {
                            Err(ErrorHandle::PeerManagerClosed)
                        } else {
                            Err(ErrorHandle::ErrorInvalidStateVersion)
                        }
                    }
                },
                TypeConnection::OutgoingConnection => {
                    if version_message.validation_version_message() {
                        if state.send_version && !state.recv_version && !state.send_verack && !state.recv_verack {
                            state.recv_version = true;

                            // create message Verack
                            let new_verack_message = VerackMessage::build_verack_message();

                            match tx_channel_peermanager.send(EventsHandleMessage::ReadyMessage { peerid: peerid, message: MessagePayload::Verack(new_verack_message) }).await {
                                Ok(_) => {
                                    state.send_verack = true;

                                    Ok(())
                                },
                                Err(_) => {
                                    Err(ErrorHandle::PeerManagerClosed)
                                }
                            }
                        } else {
                            if tx_channel_peermanager.send(EventsHandleMessage::IncidetViolentComplitedPeer(peerid)).await.is_err() {
                                Err(ErrorHandle::PeerManagerClosed)
                            } else {
                                Err(ErrorHandle::ErrorInvalidStateVersion)
                            }
                        }
                    } else {
                        Err(ErrorHandle::ErrorInvalidVersionMessage)
                    }
                }
            }
        },
        MessagePayload::Verack(_) => {
            if state.recv_version && state.send_version && state.send_verack && !state.recv_verack {
                state.recv_verack = true;

                if state.send_version && state.recv_version && state.send_verack && state.recv_verack {
                    state.completed_handshake = true;

                    log::info!("HandleMessage успешно завершил процесс рукопожатие");

                    Ok(())
                } else {
                    if tx_channel_peermanager.send(EventsHandleMessage::IncidetViolentComplitedPeer(peerid)).await.is_err() {
                        Err(ErrorHandle::PeerManagerClosed)
                    } else {
                        Err(ErrorHandle::ErrorInvalidStateHandshake)
                    }
                }
            } else {
                if tx_channel_peermanager.send(EventsHandleMessage::IncidetViolentComplitedPeer(peerid)).await.is_err() {
                    Err(ErrorHandle::PeerManagerClosed)
                } else {
                    Err(ErrorHandle::ErrorInvalidStateVerack)
                }
            }
        }
    }
}

pub struct PeerContextMetadata {
    type_connect: TypeConnection,
    nonce: u64,

    local_address: SocketAddr,
    remove_address: SocketAddr,
}

impl PeerContextMetadata {
    pub fn create(type_connect: TypeConnection, nonce: u64, local_address: SocketAddr, remove_address: SocketAddr) -> Self {
        Self { type_connect, nonce, local_address, remove_address }
    }
}

#[derive(Debug)]
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