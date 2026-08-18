use tokio::{sync::mpsc, net::TcpStream};
use tokio::task::{JoinHandle, JoinSet, Id};
use std::collections::HashMap;
use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr};

use rand::{Rng, rngs::OsRng};

use crate::network::error_network::{ErrorPeer, ErrorPeerManager, ErrorSendEvents};

pub type PeerId = u64;

use crate::network::events_network::{EventsLoopListen, EventsThreadNetwork, EventsPeerManager, EventsPeer};
use crate::network::peer::Peer;
use crate::network::success_network::SuccessPeer;
use crate::protocol::events_protocol::{EventsHandleMessage};
use crate::network::ttype::{EventsCriticalCompletionPeerManager, EventsWorkCyclePeerManager, MessagePayload, StructContextConnection, TypeConnection};

pub struct PeerManager {
    peerid: PeerId,

    set_all_peers: HashMap<PeerId, PeerSystemMetadata>,
    active_peers: HashMap<PeerId, PeerSystemMetadata>,

    set_task_peers: JoinSet<(PeerId, Result<SuccessPeer, ErrorPeer>)>,
    set_id_peers: HashMap<Id, PeerId>,

    listen_address: SocketAddr,
    nonce: u64,

    rx_channel_peermanager_from_threadnetwork: mpsc::Receiver<EventsThreadNetwork>,

    rx_channel_peermanager_from_looplisten: mpsc::Receiver<EventsLoopListen>,
    
    tx_channel_peermanager_to_protocol: mpsc::Sender<EventsPeerManager>,
    rx_channel_peermanager_from_protocol: mpsc::Receiver<EventsHandleMessage>,

    tx_channel_peer_to_peermanager: mpsc::Sender<EventsPeer>,
    rx_channel_peermanager_from_peer: mpsc::Receiver<EventsPeer>,
}

impl PeerManager {
    pub fn create(rx_channel_peermanager_from_threadnetwork: mpsc::Receiver<EventsThreadNetwork>, rx_channel_peermanager_from_looplisten: mpsc::Receiver<EventsLoopListen>, tx_channel_peermanager_to_protocol: mpsc::Sender<EventsPeerManager>, rx_channel_peermanager_from_protocol: mpsc::Receiver<EventsHandleMessage>) -> Self {
        let listen_address = {
            SocketAddr::new(std::net::IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8080)
        };

        let nonce = {
            let rang: u64 = OsRng.r#gen();
            rang
        };

        let (tx_channel_peermanager, rx_channel_peer) = mpsc::channel::<EventsPeer>(1024);

        Self { peerid: 1, set_all_peers: HashMap::<PeerId, PeerSystemMetadata>::new(), active_peers: HashMap::<PeerId, PeerSystemMetadata>::new(), set_task_peers: JoinSet::new(), set_id_peers: HashMap::<Id, PeerId>::new(), listen_address: listen_address, nonce: nonce, rx_channel_peermanager_from_threadnetwork: rx_channel_peermanager_from_threadnetwork, rx_channel_peermanager_from_looplisten: rx_channel_peermanager_from_looplisten, tx_channel_peermanager_to_protocol: tx_channel_peermanager_to_protocol, rx_channel_peermanager_from_protocol: rx_channel_peermanager_from_protocol, tx_channel_peer_to_peermanager: tx_channel_peermanager, rx_channel_peermanager_from_peer: rx_channel_peer }
    }

    pub async fn run(mut self) -> Result<(), ErrorPeerManager> {
        log::info!("Цикл событие peermanager был успешно запущено");

        let critical_result_loop = loop {
            let work_cycle_event = if self.set_task_peers.is_empty() {
                tokio::select! {
                    incident_thread_network = self.rx_channel_peermanager_from_threadnetwork.recv() => {
                        match incident_thread_network {
                            Some(EventsThreadNetwork::OutgoingConnection(address)) => {
                                EventsWorkCyclePeerManager::IncidentThreadNetworkOutgoingConnection(address)
                            }
                            Some(EventsThreadNetwork::Shutdown) => {
                                break EventsCriticalCompletionPeerManager::IncidentConfirmationShutdown;
                            }
                            None => {
                                break EventsCriticalCompletionPeerManager::IncidentChannelThreadNetworkClosed;
                            }
                        }
                    },

                    incident_looplisten = self.rx_channel_peermanager_from_looplisten.recv() => {
                        match incident_looplisten {
                            Some(EventsLoopListen::IncomingConnection { socket, address }) => {
                                EventsWorkCyclePeerManager::IncidentLoopListenIncomingConnection { socket, address }
                            }
                            None => {
                                break EventsCriticalCompletionPeerManager::IncidentChannelLoopListenClosed;
                            }
                        }
                    },

                    incident_protocol = self.rx_channel_peermanager_from_protocol.recv() => {
                        match incident_protocol {
                            Some(EventsHandleMessage::ReadyMessage { peerid, message }) => {
                                EventsWorkCyclePeerManager::IncidentProtocolReadyMessagePeer { peerid, message }
                            },
                            Some(EventsHandleMessage::IncidetViolentComplitedPeer(peerid)) => {
                                EventsWorkCyclePeerManager::IncidetViolentComplitedPeer(peerid)
                            }
                            None => {
                                break EventsCriticalCompletionPeerManager::IncidentChannelProtocolClosed;
                            }
                        }
                    },

                    incident_peer = self.rx_channel_peermanager_from_peer.recv() => {
                        match incident_peer {
                            Some(EventsPeer::ReadyLowMessage { peerid, message }) => {
                                EventsWorkCyclePeerManager::IncidentPeerLowLevelMessage { peerid, message }
                            }
                            Some(_) => {
                                EventsWorkCyclePeerManager::IncidentNone
                            }
                            None => {
                                EventsWorkCyclePeerManager::IncidentComplitedPeer
                            }
                        }
                    },
                }
            } else {
                tokio::select! {
                    incident_thread_network = self.rx_channel_peermanager_from_threadnetwork.recv() => {
                        match incident_thread_network {
                            Some(EventsThreadNetwork::OutgoingConnection(address)) => {
                                EventsWorkCyclePeerManager::IncidentThreadNetworkOutgoingConnection(address)
                            }
                            Some(EventsThreadNetwork::Shutdown) => {
                                break EventsCriticalCompletionPeerManager::IncidentConfirmationShutdown;
                            }
                            None => {
                                break EventsCriticalCompletionPeerManager::IncidentChannelThreadNetworkClosed;
                            }
                        }
                    },

                    incident_looplisten = self.rx_channel_peermanager_from_looplisten.recv() => {
                        match incident_looplisten {
                            Some(EventsLoopListen::IncomingConnection { socket, address }) => {
                                EventsWorkCyclePeerManager::IncidentLoopListenIncomingConnection { socket, address }
                            }
                            None => {
                                break EventsCriticalCompletionPeerManager::IncidentChannelLoopListenClosed;
                            }
                        }
                    },

                    incident_protocol = self.rx_channel_peermanager_from_protocol.recv() => {
                        match incident_protocol {
                            Some(EventsHandleMessage::ReadyMessage { peerid, message }) => {
                                EventsWorkCyclePeerManager::IncidentProtocolReadyMessagePeer { peerid, message }
                            },
                            Some(EventsHandleMessage::IncidetViolentComplitedPeer(peerid)) => {
                                EventsWorkCyclePeerManager::IncidetViolentComplitedPeer(peerid)
                            }
                            None => {
                                break EventsCriticalCompletionPeerManager::IncidentChannelProtocolClosed;
                            }
                        }
                    },

                    incident_peer = self.rx_channel_peermanager_from_peer.recv() => {
                        match incident_peer {
                            Some(EventsPeer::ReadyLowMessage { peerid, message }) => {
                                EventsWorkCyclePeerManager::IncidentPeerLowLevelMessage { peerid, message }
                            }
                            Some(_) => {
                                EventsWorkCyclePeerManager::IncidentNone
                            }
                            None => {
                                EventsWorkCyclePeerManager::IncidentComplitedPeer
                            }
                        }
                    },

                    result_peer = self.set_task_peers.join_next() => {
                        EventsWorkCyclePeerManager::IncidentComplitedPeerResult(
                            result_peer.expect("JoinSet пуст, хотя перед select! был проверен")
                        )
                    }
                }
            };

            match work_cycle_event {
                EventsWorkCyclePeerManager::IncidentThreadNetworkOutgoingConnection(address) => {
                    match TcpStream::connect(address.clone()).await {
                        Ok(outgoing_socket) => {
                            log::info!("Исходящее соединение успешно установлено по адресу: {}", address);
                            // Peer::spawn();
                            let new_peerid = self.peerid;
                            self.peerid += 1;

                            let tx_channel_peer_to_peermanager = self.tx_channel_peer_to_peermanager.clone(); // channel send events peer --> PeerManager
                            let (tx_channel_peermanager_to_peer, rx_channel_peer_from_peermanager) = mpsc::channel(1024); // channel send events PeerManager --> Peer

                            // self.set_task_peers.spawn(
                            //     match Peer::create(peerid, outgoing_socket, TypeConnection::OutgoingConnection, tx_channel_peer_to_peermanager, rx_channel_peer_from_peermanager) {
                            //         Ok(peer_actor) => {
                            //             log::info!("[Success: CreatePeer]: Задача Peer успешно удалось создать для исходящего соединение с ID: {} с кодом успеха: Success", peerid);
                            //             peer_actor.spawn().await
                            //         },
                            //         Err(error_peer) => {
                            //             self.peerid -= 1;
                            //             log::error!("[Error: CreatePeer]: Задачу Peer не удалось создать для исходящего соединение с кодом ошибки: Error: {:?}", error_peer);
                            //         }
                            //     }
                            // );

                            match Peer::create(new_peerid, outgoing_socket, TypeConnection::OutgoingConnection, tx_channel_peer_to_peermanager, rx_channel_peer_from_peermanager) {
                                Ok((peeractor, local_address)) => {
                                    log::info!("[Success: CreatePeer]: Задача Peer успешно удалось создать для исходящего соединение с ID: {} с кодом успеха: Success", new_peerid);

                                    let task_joinset_peer = self.set_task_peers.spawn(async move {
                                        peeractor.spawn().await
                                    });

                                    self.set_all_peers.insert(new_peerid, PeerSystemMetadata::create_metadate(address.clone(), TypeConnection::OutgoingConnection, tx_channel_peermanager_to_peer));
                                    log::info!("Структура служебный данные PeerSystemMetadata по ID: {} для исходящего соединение успешно создано и добавлено", new_peerid);

                                    self.set_id_peers.insert(task_joinset_peer.id(), new_peerid);

                                    let context_outgoing_peer = StructContextConnection::new(new_peerid, TypeConnection::OutgoingConnection, self.nonce, local_address, address);
                                    if self.tx_channel_peermanager_to_protocol.send(EventsPeerManager::IncidentNewConnection(context_outgoing_peer)).await.is_err() {
                                        break EventsCriticalCompletionPeerManager::IncidentChannelProtocolClosed;
                                    } else {
                                        log::info!("PeerManager успешно отправляет факт нового исходящего соединение HandleMessage");
                                    }
                                },
                                Err(error_peer) => {
                                    self.peerid -= 1;
                                    log::error!("[Error: CreatePeer]: Задачу Peer не удалось создать для исходящего соединение с кодом ошибки: Error: {:?}", error_peer);
                                }
                            }
                        },
                        Err(e) => {
                            log::error!("Произошла ошибка во время исходящего соединения по адресу: {}, ошибка: {}", address, e);
                        }
                    }
                    continue;
                },
                EventsWorkCyclePeerManager::IncidentLoopListenIncomingConnection{socket, address} => {
                    // Peer::spawn();
                    let new_peerid = self.peerid;
                    self.peerid += 1;

                    let tx_channel_peer_to_peermanager = self.tx_channel_peer_to_peermanager.clone(); // channel send events peer --> PeerManager
                    let (tx_channel_peermanager_to_peer, rx_channel_peer_from_peermanager) = mpsc::channel(1024); // channel send events PeerManager --> Peer

                    // self.set_task_peers.spawn(tokio::spawn(async move {
                    //     match Peer::create(peerid, socket, TypeConnection::IncomingConnection, tx_channel_peer_to_peermanager, rx_channel_peer_from_peermanager) {
                    //         Ok(peer_actor) => {
                    //             log::info!("[Success: CreatePeer]: Задача Peer успешно удалось создать для входящего соединение с ID: {} с кодом успеха: Success", peerid);
                    //             peer_actor.spawn().await
                    //         },
                    //         Err(error_peer) => {
                    //             self.peerid -= 1;
                    //             log::error!("[Error: CreatePeer]: Задачу Peer не удалось создать для входящего соединение с кодом ошибки: Error: {:?}", error_peer);
                    //         }
                    //     }
                    // }));

                    match Peer::create(new_peerid, socket, TypeConnection::IncomingConnection, tx_channel_peer_to_peermanager, rx_channel_peer_from_peermanager) {
                        Ok((peeractor, local_address)) => {
                            log::info!("[Success: CreatePeer]: Задача Peer успешно удалось создать для входящего соединение с ID: {} с кодом успеха: Success", new_peerid);

                            let task_joinset_peer = self.set_task_peers.spawn(async move {
                                peeractor.spawn().await
                            });

                            self.set_all_peers.insert(new_peerid, PeerSystemMetadata::create_metadate(address.clone(), TypeConnection::IncomingConnection, tx_channel_peermanager_to_peer));
                            log::info!("Структура служебный данные PeerSystemMetadata по ID: {} для входящего соединение успешно создано и добавлено", new_peerid);

                            self.set_id_peers.insert(task_joinset_peer.id(), new_peerid);

                            let context_incoming_peer = StructContextConnection::new(new_peerid, TypeConnection::IncomingConnection, self.nonce, local_address, address);
                            if self.tx_channel_peermanager_to_protocol.send(EventsPeerManager::IncidentNewConnection(context_incoming_peer)).await.is_err() {
                                break EventsCriticalCompletionPeerManager::IncidentChannelProtocolClosed;
                            } else {
                                log::info!("PeerManager успешно отправляет факт нового входящего соединение HandleMessage");
                            }
                        },
                        Err(error_peer) => {
                            self.peerid -= 1;
                            log::error!("[Error: CreatePeer]: Задачу Peer не удалось создать для входящего соединение с кодом ошибки: Error: {:?}", error_peer);
                        }
                    }
                },
                EventsWorkCyclePeerManager::IncidentProtocolReadyMessagePeer{peerid, message} => {
                    if let Err(error_send) = send_events_peer(peerid, message, &mut self.set_all_peers).await {
                        log::error!("Произошла ошибка во время отправки события к пиру по id: {}, ошибка: {:?}", peerid, error_send);

                        match error_send {
                            ErrorSendEvents::ErrorChannelPeerClosed => {
                                let _ = &self.set_all_peers.remove(&peerid);
                            },
                            ErrorSendEvents::MissingPeer => (),
                        }
                    }
                    continue;
                },
                EventsWorkCyclePeerManager::IncidentPeerLowLevelMessage{peerid, message} => {
                    if self.tx_channel_peermanager_to_protocol.send(EventsPeerManager::ReadyMessage{peerid, message}).await.is_err() {
                        break EventsCriticalCompletionPeerManager::IncidentChannelProtocolClosed;
                    }
                    continue;
                },
                EventsWorkCyclePeerManager::IncidentComplitedPeer => {
                    log::warn!("[Error: IncidentPeer]: Задача Peer завершена (канал соединение сбросано)");

                    if self.tx_channel_peermanager_to_protocol.send(EventsPeerManager::IncidentCompletedAllPeer).await.is_err() {
                        break EventsCriticalCompletionPeerManager::IncidentChannelProtocolClosed;
                    }
                    continue;
                }
                EventsWorkCyclePeerManager::IncidentComplitedPeerResult(result_peer) => {
                    match result_peer {
                        Ok(result) => {
                            match result {
                                (peerid, Ok(success_peer)) => {
                                    match success_peer {
                                        SuccessPeer::SuccessfulTaskReadTaskWrite => {
                                            log::info!("[Success: IncidentPeer]: Задача Peer по ID: {} успешно завершена с кодом состояния: Successful: {:?}", peerid, success_peer);

                                            let _ = &self.set_all_peers.remove(&peerid);
                                            self.peerid -= 1;

                                            if self.tx_channel_peermanager_to_protocol.send(EventsPeerManager::IncidentCompletedPeer(peerid)).await.is_err() {
                                                break EventsCriticalCompletionPeerManager::IncidentChannelProtocolClosed;
                                            }
                                        },
                                        SuccessPeer::SuccessfulTaskReadBrokenPipeTaskWriteSuccessful => {
                                            log::warn!("[Success: IncidentPeer]: Задача Peer по ID: {} завершена с кодом состояния: Error: {:?}", peerid, success_peer);

                                            let _ = &self.set_all_peers.remove(&peerid);
                                            self.peerid -= 1;

                                            if self.tx_channel_peermanager_to_protocol.send(EventsPeerManager::IncidentCompletedPeer(peerid)).await.is_err() {
                                                break EventsCriticalCompletionPeerManager::IncidentChannelProtocolClosed;
                                            }
                                        }
                                    }
                                },
                                (peerid, Err(result_peer_error)) => {
                                    log::error!("[Error: IncidentPeer]: Задача Peer по ID: {} завершена с кодом состояния: Error: {:?}", peerid, result_peer_error);

                                    let _ = &self.set_all_peers.remove(&peerid);
                                    self.peerid -= 1;

                                    if self.tx_channel_peermanager_to_protocol.send(EventsPeerManager::IncidentCompletedPeer(peerid)).await.is_err() {
                                        break EventsCriticalCompletionPeerManager::IncidentChannelProtocolClosed;
                                    }
                                }
                            }
                        },
                        Err(error_peer) => {
                            if let Some(peerid) = self.set_id_peers.get(&error_peer.id()).copied() {
                                log::error!("[Error: IncidentPeer]: Задача Peer по ID: {} завершена с кодом состояния: Error: {:?}", peerid, error_peer);

                                let _ = &self.set_all_peers.remove(&peerid);
                                self.peerid -= 1;

                                if self.tx_channel_peermanager_to_protocol.send(EventsPeerManager::IncidentCompletedPeer(peerid)).await.is_err() {
                                    break EventsCriticalCompletionPeerManager::IncidentChannelProtocolClosed;
                                }
                            }
                        }
                    }
                    continue;
                },
                EventsWorkCyclePeerManager::IncidetViolentComplitedPeer(peerid) => {
                    log::info!("PeerManager получил событие о завершение пира с id: {} по причине некорректное поведение", peerid);

                    if let Some(complited_peer) = self.set_all_peers.get_mut(&peerid) {
                        if complited_peer.tx_channel_peermanager_to_peer.send(EventsPeerManager::Shutdown).await.is_err() {
                            log::info!("PeerManager не удалось отправить событие завершение пиру ведущий некорректное поведение: PeerChannelClosed");
                        }
                    }
                }
                EventsWorkCyclePeerManager::IncidentNone => {
                    continue;
                }
            }
        };

        match critical_result_loop {
            EventsCriticalCompletionPeerManager::IncidentConfirmationShutdown => {
                bulk_peer_disconnection(&mut self.set_all_peers).await;

                while let Some(state_peer) = self.set_task_peers.join_next().await {
                    match state_peer {
                        Ok((peerid, Ok(success_result_peer))) => {
                            match success_result_peer {
                                SuccessPeer::SuccessfulTaskReadTaskWrite => {
                                    log::info!("[Success: Shutdown]: Задача Peer по ID: {} успешно завершена с кодом состояния: Successful: {:?}", peerid, success_result_peer);

                                    let _ = &self.set_all_peers.remove(&peerid);
                                    self.peerid -= 1;
                                },
                                SuccessPeer::SuccessfulTaskReadBrokenPipeTaskWriteSuccessful => {
                                    log::warn!("[Success: Shutdown]: Задача Peer по ID: {} завершена с кодом состояния: Error: {:?}", peerid, success_result_peer);

                                    let _ = &self.set_all_peers.remove(&peerid);
                                    self.peerid -= 1;
                                }
                            }
                        },
                        Ok((peerid, Err(error_result_peer))) => {
                            log::error!("[Error: Shutdown]: Задача Peer по ID: {} завершена с кодом состояния: Error: {:?}", peerid, error_result_peer);

                            let _= &self.set_all_peers.remove(&peerid);
                            self.peerid -= 1;
                        },
                        Err(error_joinerror) => {
                            if let Some(peer_id) = self.set_id_peers.get(&error_joinerror.id()) {
                                log::error!("[Error: Shutdown]: Задача Peer по ID: {} завершена с кодом состояния: Error: {:?}", peer_id, error_joinerror);
                                let _ = &self.set_all_peers.remove(&peer_id);
                                self.peerid -= 1;
                            }
                        }
                    }
                }

                Ok(())
            },
            EventsCriticalCompletionPeerManager::IncidentChannelThreadNetworkClosed => {
                bulk_peer_disconnection(&mut self.set_all_peers).await;

                while let Some(state_peer) = self.set_task_peers.join_next().await {
                    match state_peer {
                        Ok((peerid, Ok(success_result_peer))) => {
                            match success_result_peer {
                                SuccessPeer::SuccessfulTaskReadTaskWrite => {
                                    log::info!("[Success: ChannelThreadNetworkClosed]: Задача Peer по ID: {} успешно завершена с кодом состояния: Successful: {:?}", peerid, success_result_peer);

                                    let _ = &self.set_all_peers.remove(&peerid);
                                    self.peerid -= 1;
                                },
                                SuccessPeer::SuccessfulTaskReadBrokenPipeTaskWriteSuccessful => {
                                    log::warn!("[Success: ChannelThreadNetworkClosed]: Задача Peer по ID: {} завершена с кодом состояния: Error: {:?}", peerid, success_result_peer);

                                    let _ = &self.set_all_peers.remove(&peerid);
                                    self.peerid -= 1;
                                }
                            }
                        },
                        Ok((peerid, Err(error_result_peer))) => {
                            log::error!("[Error: ChannelThreadNetworkClosed]: Задача Peer по ID: {} завершена с кодом состояния: Error: {:?}", peerid, error_result_peer);

                            let _ = &self.set_all_peers.remove(&peerid);
                            self.peerid -= 1;
                        },
                        Err(error_joinerror) => {
                            if let Some(peer_id) = self.set_id_peers.get(&error_joinerror.id()) {
                                log::error!("[Error: ChannelThreadNetworkClosed]: Задача Peer по ID: {} завершена с кодом состояния: Error: {:?}", peer_id, error_joinerror);
                                let _ = &self.set_all_peers.remove(&peer_id);
                                self.peerid -= 1;
                            }
                        }
                    }
                }

                Err(ErrorPeerManager::ErrorChannelThreadNetworkClosed)
            },
            EventsCriticalCompletionPeerManager::IncidentChannelLoopListenClosed => {
                bulk_peer_disconnection(&mut self.set_all_peers).await;

                while let Some(state_peer) = self.set_task_peers.join_next().await {
                    match state_peer {
                        Ok((peerid, Ok(success_result_peer))) => {
                            match success_result_peer {
                                SuccessPeer::SuccessfulTaskReadTaskWrite => {
                                    log::info!("[Success: ChannelLoopListenClosed]: Задача Peer по ID: {} успешно завершена с кодом состояния: Successful: {:?}", peerid, success_result_peer);

                                    let _ = &self.set_all_peers.remove(&peerid);
                                    self.peerid -= 1;
                                },
                                SuccessPeer::SuccessfulTaskReadBrokenPipeTaskWriteSuccessful => {
                                    log::warn!("[Success: ChannelLoopListenClosed]: Задача Peer по ID: {} завершена с кодом состояния: Error: {:?}", peerid, success_result_peer);

                                    let _ = &self.set_all_peers.remove(&peerid);
                                    self.peerid -= 1;
                                }
                            }
                        },
                        Ok((peerid, Err(error_result_peer))) => {
                            log::error!("[Error: ChannelLoopListenClosed]: Задача Peer по ID: {} завершена с кодом состояния: Error: {:?}", peerid, error_result_peer);

                            let _ = &self.set_all_peers.remove(&peerid);
                            self.peerid -= 1;
                        },
                        Err(error_joinerror) => {
                            if let Some(peer_id) = self.set_id_peers.get(&error_joinerror.id()) {
                                log::error!("[Error: ChannelLoopListenClosed]: Задача Peer по ID: {} завершена с кодом состояния: Error: {:?}", peer_id, error_joinerror);
                                let _ = &self.set_all_peers.remove(&peer_id);
                                self.peerid -= 1;
                            }
                        }
                    }
                }

                Err(ErrorPeerManager::ErrorChannelLoopListenClosed)
            },
            EventsCriticalCompletionPeerManager::IncidentChannelProtocolClosed => {
                bulk_peer_disconnection(&mut self.set_all_peers).await;

                while let Some(state_peer) = self.set_task_peers.join_next().await {
                    match state_peer {
                        Ok((peerid, Ok(success_result_peer))) => {
                            match success_result_peer {
                                SuccessPeer::SuccessfulTaskReadTaskWrite => {
                                    log::info!("[Success: ChannelProtocolClosed]: Задача Peer по ID: {} успешно завершена с кодом состояния: Successful: {:?}", peerid, success_result_peer);

                                    let _ = &self.set_all_peers.remove(&peerid);
                                    self.peerid -= 1;
                                },
                                SuccessPeer::SuccessfulTaskReadBrokenPipeTaskWriteSuccessful => {
                                    log::warn!("[Success: ChannelProtocolClosed]: Задача Peer по ID: {} завершена с кодом состояния: Error: {:?}", peerid, success_result_peer);

                                    let _ = &self.set_all_peers.remove(&peerid);
                                    self.peerid -= 1;
                                }
                            }
                        },
                        Ok((peerid, Err(error_result_peer))) => {
                            log::error!("[Error: ChannelProtocolClosed]: Задача Peer по ID: {} завершена с кодом состояния: Error: {:?}", peerid, error_result_peer);

                            let _ = &self.set_all_peers.remove(&peerid);
                            self.peerid -= 1;
                        },
                        Err(error_joinerror) => {
                            if let Some(peer_id) = self.set_id_peers.get(&error_joinerror.id()) {
                                log::error!("[Error: ChannelProtocolClosed]: Задача Peer по ID: {} завершена с кодом состояния: Error: {:?}", peer_id, error_joinerror);
                                let _ = &self.set_all_peers.remove(&peer_id);
                                self.peerid -= 1;
                            }
                        }
                    }
                }

                Err(ErrorPeerManager::ErrorChannelHandleMessageClosed)
            },
            EventsCriticalCompletionPeerManager::IncidentZeroPeers => {
                log::error!("[Error: ZeroActivePeers]: Задача All Peer завершена с кодом состояния: Error: Zero");

                Err(ErrorPeerManager::ErrorChannelAllPeersClosed)
            }
        }
    }
}

async fn bulk_peer_disconnection(all_peers: &mut HashMap<PeerId, PeerSystemMetadata>) {
    for peer in all_peers.values_mut() {
        if let Err(_) = peer.tx_channel_peermanager_to_peer.send(EventsPeerManager::Shutdown).await { log::warn!("Соединение с Peer по адресу: {} разорвано до инициации процедуры массового отключения", peer.socket_address) };
    }
}

async fn send_events_peer(basic_peerid: PeerId, message: MessagePayload, all_peers: &mut HashMap<PeerId, PeerSystemMetadata>) -> Result<(), ErrorSendEvents> {
    if let Some(peer) = all_peers.get_mut(&basic_peerid) {
        if peer.tx_channel_peermanager_to_peer.send(EventsPeerManager::ReadyLowMessage { message }).await.is_err() {
            Err(ErrorSendEvents::ErrorChannelPeerClosed)
        } else {
            Ok(())
        }
    } else {
        Err(ErrorSendEvents::MissingPeer)
    }
}

pub struct PeerSystemMetadata {
    socket_address: SocketAddr,
    type_connect: TypeConnection,

    tx_channel_peermanager_to_peer: mpsc::Sender<EventsPeerManager>,
}

impl PeerSystemMetadata {
    fn create_metadate(socket_address: SocketAddr, type_connect: TypeConnection, tx_channel_peermanager_to_peer: mpsc::Sender<EventsPeerManager>) -> Self {
        Self { socket_address, type_connect, tx_channel_peermanager_to_peer }
    }
}