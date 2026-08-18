use std::net::SocketAddr;

use tokio::{net::TcpStream, sync::mpsc};

use crate::network::events_network::EventsPeerManager;
use crate::network::peermanager::{PeerId};
use crate::network::success_network::{SuccessTaskRead, SuccessTaskWrite, SuccessPeer};
use crate::network::{error_network::{ErrorPeer}, events_network::{EventsPeer, EventsTaskRead}, ttype::{EventsWorkCycle, EventsCriticalCompletion, TypeConnection}};

use crate::network::{task_read::{start_task_read}, task_write::{start_task_write}};

pub struct Peer {
    peerid: PeerId,
    socket: TcpStream,

    type_connection: TypeConnection,

    remote_address: SocketAddr,
    local_address: SocketAddr,

    tx_channel_peer_to_peermanager: mpsc::Sender<EventsPeer>,
    rx_channel_peer_from_peermanager: mpsc::Receiver<EventsPeerManager>,
}

impl Peer {
    pub fn create(peerid: PeerId, socket: TcpStream, type_connection: TypeConnection, tx_channel_peer_to_peermanager: mpsc::Sender<EventsPeer>, rx_channel_peer_from_peermanager: mpsc::Receiver<EventsPeerManager>) -> Result<(Self, SocketAddr), ErrorPeer> {
        let remote_address = socket.peer_addr().map_err(|e_r| ErrorPeer::ErrorRemoteAddress(e_r))?;
        let local_address = socket.local_addr().map_err(|e_l| ErrorPeer::ErrorLocalAddress(e_l))?;
        
        Ok((
            Self { peerid: peerid, socket: socket, type_connection: type_connection, remote_address: remote_address, local_address: local_address, tx_channel_peer_to_peermanager: tx_channel_peer_to_peermanager, rx_channel_peer_from_peermanager: rx_channel_peer_from_peermanager },
            local_address
        ))
    }

    pub async fn spawn(mut self) -> (PeerId, Result<SuccessPeer, ErrorPeer>) {
        let (part_socket_read, part_socket_write) = self.socket.into_split();

        let (tx_channel_from_peer, rx_channel_taskread) = mpsc::channel::<EventsPeer>(1024);
        let (tx_channel_from_taskread, mut rx_channel_peer) = mpsc::channel::<EventsTaskRead>(1024);

        let mut state_task_read = tokio::spawn(async move {
            start_task_read(part_socket_read, rx_channel_taskread, tx_channel_from_taskread).await
        });

        let (tx_channel_from_peer_to_task_write, rx_channel_taskwrite) = mpsc::channel::<EventsPeer>(1024);

        let mut state_task_write = tokio::spawn(async move {
            start_task_write(part_socket_write, rx_channel_taskwrite).await
        });

        let critical_result_loop = loop {
            let work_cycle_event = tokio::select! {
                incident_peermanager = self.rx_channel_peer_from_peermanager.recv() => {
                    match incident_peermanager {
                        Some(EventsPeerManager::ReadyLowMessage{message}) => {
                            EventsWorkCycle::IncidentPeerManagerReadyMessageTaskWrite(message)
                        },
                        Some(EventsPeerManager::Shutdown) => {
                            break EventsCriticalCompletion::IncidentConfirmationShutdown;
                        },
                        Some(_) => {
                            EventsWorkCycle::IncidentNone
                        },
                        None => {
                            break EventsCriticalCompletion::IncidentConfirmationShutdown;
                        }
                    }
                },
                incident_task_read = rx_channel_peer.recv() => {
                    match incident_task_read {
                        Some(EventsTaskRead::LowRawMessage(message)) => {
                            EventsWorkCycle::IncidentTaskReadRawMessagePeerManager { peerid: self.peerid, message }
                        },
                        None => {
                            break EventsCriticalCompletion::IncidentChannelTaskReadClosed;
                        }
                    }
                },
                result_state_task_read = &mut state_task_read => {
                    break match result_state_task_read {
                        Ok(result_application_task_read) => {
                            EventsCriticalCompletion::IncidentComplitedTaskRead(result_application_task_read)
                        },
                        Err(error_joinhandle) => {
                            EventsCriticalCompletion::IncidentJoinHandleTaskRead(error_joinhandle)
                        }
                    }
                },
                result_state_task_write = &mut state_task_write => {
                    break match result_state_task_write {
                        Ok(result_application_task_write) => {
                            EventsCriticalCompletion::IncidentComplitedTaskWrite(result_application_task_write)
                        },
                        Err(error_joinhandle) => {
                            EventsCriticalCompletion::IncidentJoinHandleTaskWrite(error_joinhandle)
                        }
                    }
                }
            };

            match work_cycle_event {
                EventsWorkCycle::IncidentPeerManagerReadyMessageTaskWrite(ready_message) => {
                    if tx_channel_from_peer_to_task_write.send(EventsPeer::LowLevelMessage(ready_message)).await.is_err() {
                        break EventsCriticalCompletion::IncidentChannelTaskWriteClosed;
                    }
                    continue;
                },
                EventsWorkCycle::IncidentTaskReadRawMessagePeerManager { peerid, message } => {
                    if self.tx_channel_peer_to_peermanager.send(EventsPeer::ReadyLowMessage { peerid, message }).await.is_err() {
                        break EventsCriticalCompletion::IncidentChannelPeerManagerClosed;
                    }
                    continue;
                },
                EventsWorkCycle::IncidentNone => {
                    continue;
                }
            }
        };

        match critical_result_loop {
            EventsCriticalCompletion::IncidentConfirmationShutdown => {
                let _ = tx_channel_from_peer.send(EventsPeer::Shutdown).await;
                let _ = tx_channel_from_peer_to_task_write.send(EventsPeer::Shutdown).await;

                let (result_taskread, result_taskwrite) = tokio::join!(state_task_read, state_task_write);
                match (result_taskread, result_taskwrite) {
                    (Ok(task_read), Ok(task_write)) => {
                        match (task_read, task_write) {
                            (Ok(success_read), Ok(success_write)) => {
                                match success_read {
                                    SuccessTaskRead::Successful => {
                                        log::info!("[Success: Shutdown]: Подзадача TaskRead успешно завершена с кодом состояния: Successful");
                                        log::info!("[Success: Shutdown]: Подзадача TaskWrite успешно завершена с кодом состояния: Successful: {:?}", success_write);

                                        (self.peerid, Ok(SuccessPeer::SuccessfulTaskReadTaskWrite))
                                    },
                                    SuccessTaskRead::SuccessfulEOF => {
                                        log::warn!("[Success: Shutdown]: Подзадача TaskRead завершена с кодом состояния: Successful: SuccessfulEOF");
                                        log::info!("[Success: Shutdown]: Подзадача TaskWrite успешно завершена с кодом состояния: Successful: {:?}", success_write);

                                        (self.peerid, Ok(SuccessPeer::SuccessfulTaskReadBrokenPipeTaskWriteSuccessful))
                                    }
                                }
                            },
                            (Ok(success_read), Err(error_write)) => {
                                match success_read {
                                    SuccessTaskRead::Successful => {
                                        log::info!("[Success: Shutdown]: Подзадача TaskRead успешно завершена с кодом состояния: Successful: {:?}", success_read);
                                        log::error!("[Success: Shutdown]: Подзадача TaskWrite завершена с кодом состояния: Error: {:?}", error_write);

                                        (self.peerid, Err(ErrorPeer::ErrorSuccessTaskReadErrorTaskWrite{ success_read: success_read, error_write: error_write }))

                                    },
                                    SuccessTaskRead::SuccessfulEOF => {
                                        log::warn!("[Success: Shutdown]: Подзадача TaskRead завершена с кодом состояния: Successful: {:?}", success_read);
                                        log::error!("[Success: Shutdown]: Подзадача TaskWrite завершена с кодом состояния: Error: {:?}", error_write);

                                        (self.peerid, Err(ErrorPeer::ErrorSuccessTaskReadErrorTaskWrite{ success_read: success_read, error_write: error_write }))
                                    }
                                }
                            },
                            (Err(error_read), Ok(success_write)) => {
                                match success_write {
                                    SuccessTaskWrite::Successful => {
                                        log::info!("[Success: Shutdown]: Подзадача TaskWrite успешно завершена с кодом состояния: Successful: {:?}", success_write);
                                        log::error!("[Success: Shutdown]: Подзадача TaskRead завершена с кодом состояния: Error: {:?}", error_read);

                                        (self.peerid, Err(ErrorPeer::ErrorSuccessTaskWriteErrorTaskRead{error_read: error_read, success_write: success_write}))
                                    }
                                }
                            },
                            (Err(error_read), Err(error_write)) => {
                                log::error!("[Success: Shutdown]: Подзадача TaskRead завершена с кодом состояния: Error: {:?}", error_read);
                                log::error!("[Success: Shutdown]: Подзадача TaskWrite завершена с кодом состояния: Error: {:?}", error_write);

                                (self.peerid, Err(ErrorPeer::ErrorTaskReadTaskWrite{error_read: error_read, error_write: error_write}))
                            }
                        }
                    },
                    (Ok(task_read), Err(task_write)) => {
                        match task_read {
                            Ok(success_read) => {
                                match success_read {
                                    SuccessTaskRead::Successful => {
                                        log::info!("[Success: Shutdown]: Подзадача TaskRead успешно завершена с кодом состояния: Successful: {:?}", success_read);
                                        log::error!("[Success: Shutdown]: Подзадача TaskWrite завершена с кодом состояния: Error: {:?}", task_write);

                                        (self.peerid, Err(ErrorPeer::ErrorSuccessTaskReadErrorJoinHandleTaskWrite{success_read: success_read, error_write: task_write}))
                                    },
                                    SuccessTaskRead::SuccessfulEOF => {
                                        log::warn!("[Success: Shutdown]: Подзадача TaskRead завершена с кодом состояния: Successful: {:?}", success_read);
                                        log::error!("[Success: Shutdown]: Подзадача TaskWrite завершена с кодом состояния: Error: {:?}", task_write);

                                        (self.peerid, Err(ErrorPeer::ErrorSuccessTaskReadErrorJoinHandleTaskWrite{success_read: success_read, error_write: task_write}))
                                    }
                                }
                            },
                            Err(error_read) => {
                                log::error!("[Success: Shutdown]: Подзадача TaskRead завершена с кодом состояния: Error: {:?}", error_read);
                                log::error!("[Success: Shutdown]: Подзадача TaskWrite завершена с кодом состояния: Error: {:?}", task_write);
                                
                                (self.peerid, Err(ErrorPeer::ErrorTaskReadErrorJoinHandleTaskWrite{error_read: error_read, error_write: task_write}))
                            },
                        }
                    },
                    (Err(task_read), Ok(task_write)) => {
                        match task_write {
                            Ok(success_write) => {
                                match success_write {
                                    SuccessTaskWrite::Successful => {
                                        log::info!("[Success: Shutdown]: Подзадача TaskWrite успешно завершена с кодом состояния: Successful: {:?}", success_write);
                                        log::error!("[Success: Shutdown]: Подзадача TaskRead завершена с кодом состояния: Error: {:?}", task_read);

                                        (self.peerid, Err(ErrorPeer::ErrorSuccessTaskWriteErrorJoinHandleTaskRead{error_read: task_read, success_write: success_write}))
                                    }
                                }
                            },
                            Err(error_write) => {
                                log::error!("[Success: Shutdown]: Подзадача TaskWrite завершена с кодом состояния: Error: {:?}", error_write);
                                log::error!("[Success: Shutdown]: Подзадача TaskRead завершена с кодом состояния: Error: {:?}", task_read);

                                (self.peerid, Err(ErrorPeer::ErrorTaskWriteErrorJoinHandleTaskRead{error_write: error_write, error_read: task_read}))
                            }
                        }
                    },
                    (Err(task_read), Err(task_write)) => {
                        log::error!("[Success: Shutdown]: Подзадача TaskRead завершена с кодом состояния: Error: {:?}", task_read);
                        log::error!("[Success: Shutdown]: Подзадача TaskWrite завершена с кодом состояния: Error: {:?}", task_write);

                        (self.peerid, Err(ErrorPeer::ErrorJoinHandleTaskReadTaskWrite{error_read: task_read, error_write: task_write}))
                    }
                }
            },
            EventsCriticalCompletion::IncidentChannelPeerManagerClosed => {
                let _ = tx_channel_from_peer.send(EventsPeer::Shutdown).await;
                let _ = tx_channel_from_peer_to_task_write.send(EventsPeer::Shutdown).await;

                let (result_taskread, result_taskwrite) = tokio::join!(state_task_read, state_task_write);
                match (result_taskread, result_taskwrite) {
                    (Ok(task_read), Ok(task_write)) => {
                        match (task_read, task_write) {
                            (Ok(success_read), Ok(success_write)) => {
                                match success_read {
                                    SuccessTaskRead::Successful => {
                                        log::info!("[Error: ChannelPeerManagerClosed]: Подзадача TaskRead успешно завершена с кодом состояния: Successful");
                                        log::info!("[Error: ChannelPeerManagerClosed]: Подзадача TaskWrite успешно завершена с кодом состояния: Successful: {:?}", success_write);

                                        (self.peerid, Ok(SuccessPeer::SuccessfulTaskReadTaskWrite))
                                    },
                                    SuccessTaskRead::SuccessfulEOF => {
                                        log::warn!("[Error: ChannelPeerManagerClosed]: Подзадача TaskRead завершена с кодом состояния: Successful: SuccessfulEOF");
                                        log::info!("[Error: ChannelPeerManagerClosed]: Подзадача TaskWrite успешно завершена с кодом состояния: Successful: {:?}", success_write);

                                        (self.peerid, Ok(SuccessPeer::SuccessfulTaskReadBrokenPipeTaskWriteSuccessful))
                                    }
                                }
                            },
                            (Ok(success_read), Err(error_write)) => {
                                match success_read {
                                    SuccessTaskRead::Successful => {
                                        log::info!("[Error: ChannelPeerManagerClosed]: Подзадача TaskRead успешно завершена с кодом состояния: Successful: {:?}", success_read);
                                        log::error!("[Error: ChannelPeerManagerClosed]: Подзадача TaskWrite завершена с кодом состояния: Error: {:?}", error_write);

                                        (self.peerid, Err(ErrorPeer::ErrorSuccessTaskReadErrorTaskWrite{ success_read: success_read, error_write: error_write }))

                                    },
                                    SuccessTaskRead::SuccessfulEOF => {
                                        log::warn!("[Error: ChannelPeerManagerClosed]: Подзадача TaskRead завершена с кодом состояния: Successful: {:?}", success_read);
                                        log::error!("[Error: ChannelPeerManagerClosed]: Подзадача TaskWrite завершена с кодом состояния: Error: {:?}", error_write);

                                        (self.peerid, Err(ErrorPeer::ErrorSuccessTaskReadErrorTaskWrite{ success_read: success_read, error_write: error_write }))
                                    }
                                }
                            },
                            (Err(error_read), Ok(success_write)) => {
                                match success_write {
                                    SuccessTaskWrite::Successful => {
                                        log::info!("[Error: ChannelPeerManagerClosed]: Подзадача TaskWrite успешно завершена с кодом состояния: Successful: {:?}", success_write);
                                        log::error!("[Error: ChannelPeerManagerClosed]: Подзадача TaskRead завершена с кодом состояния: Error: {:?}", error_read);

                                        (self.peerid, Err(ErrorPeer::ErrorSuccessTaskWriteErrorTaskRead{error_read: error_read, success_write: success_write}))
                                    }
                                }
                            },
                            (Err(error_read), Err(error_write)) => {
                                log::error!("[Error: ChannelPeerManagerClosed]: Подзадача TaskRead завершена с кодом состояния: Error: {:?}", error_read);
                                log::error!("[Error: ChannelPeerManagerClosed]: Подзадача TaskWrite завершена с кодом состояния: Error: {:?}", error_write);

                                (self.peerid, Err(ErrorPeer::ErrorTaskReadTaskWrite{error_read: error_read, error_write: error_write}))
                            }
                        }
                    },
                    (Ok(task_read), Err(task_write)) => {
                        match task_read {
                            Ok(success_read) => {
                                match success_read {
                                    SuccessTaskRead::Successful => {
                                        log::info!("[Error: ChannelPeerManagerClosed]: Подзадача TaskRead успешно завершена с кодом состояния: Successful: {:?}", success_read);
                                        log::error!("[Error: ChannelPeerManagerClosed]: Подзадача TaskWrite завершена с кодом состояния: Error: {:?}", task_write);

                                        (self.peerid, Err(ErrorPeer::ErrorSuccessTaskReadErrorJoinHandleTaskWrite{success_read: success_read, error_write: task_write}))
                                    },
                                    SuccessTaskRead::SuccessfulEOF => {
                                        log::warn!("[Error: ChannelPeerManagerClosed]: Подзадача TaskRead завершена с кодом состояния: Successful: {:?}", success_read);
                                        log::error!("[Error: ChannelPeerManagerClosed]: Подзадача TaskWrite завершена с кодом состояния: Error: {:?}", task_write);

                                        (self.peerid, Err(ErrorPeer::ErrorSuccessTaskReadErrorJoinHandleTaskWrite{success_read: success_read, error_write: task_write}))
                                    }
                                }
                            },
                            Err(error_read) => {
                                log::error!("[Error: ChannelPeerManagerClosed]: Подзадача TaskRead завершена с кодом состояния: Error: {:?}", error_read);
                                log::error!("[Error: ChannelPeerManagerClosed]: Подзадача TaskWrite завершена с кодом состояния: Error: {:?}", task_write);
                                
                                (self.peerid, Err(ErrorPeer::ErrorTaskReadErrorJoinHandleTaskWrite{error_read: error_read, error_write: task_write}))
                            },
                        }
                    },
                    (Err(task_read), Ok(task_write)) => {
                        match task_write {
                            Ok(success_write) => {
                                match success_write {
                                    SuccessTaskWrite::Successful => {
                                        log::info!("[Error: ChannelPeerManagerClosed]: Подзадача TaskWrite успешно завершена с кодом состояния: Successful: {:?}", success_write);
                                        log::error!("[Error: ChannelPeerManagerClosed]: Подзадача TaskRead завершена с кодом состояния: Error: {:?}", task_read);

                                        (self.peerid, Err(ErrorPeer::ErrorSuccessTaskWriteErrorJoinHandleTaskRead{error_read: task_read, success_write: success_write}))
                                    }
                                }
                            },
                            Err(error_write) => {
                                log::error!("[Error: ChannelPeerManagerClosed]: Подзадача TaskWrite завершена с кодом состояния: Error: {:?}", error_write);
                                log::error!("[Error: ChannelPeerManagerClosed]: Подзадача TaskRead завершена с кодом состояния: Error: {:?}", task_read);

                                (self.peerid, Err(ErrorPeer::ErrorTaskWriteErrorJoinHandleTaskRead{error_write: error_write, error_read: task_read}))
                            }
                        }
                    },
                    (Err(task_read), Err(task_write)) => {
                        log::error!("[Error: ChannelPeerManagerClosed]: Подзадача TaskRead завершена с кодом состояния: Error: {:?}", task_read);
                        log::error!("[Error: ChannelPeerManagerClosed]: Подзадача TaskWrite завершена с кодом состояния: Error: {:?}", task_write);

                        (self.peerid, Err(ErrorPeer::ErrorJoinHandleTaskReadTaskWrite{error_read: task_read, error_write: task_write}))
                    }
                }
            },
            EventsCriticalCompletion::IncidentChannelTaskReadClosed => {
                let _ = tx_channel_from_peer_to_task_write.send(EventsPeer::Shutdown).await;

                let result_taskwrite = state_task_write.await;
                match result_taskwrite {
                    Ok(task_write) => {
                        match task_write {
                            Ok(success_taskwrite) => {
                                match success_taskwrite {
                                    SuccessTaskWrite::Successful => {
                                        log::info!("[Success: Shutdown]: Подзадача TaskWrite успешно завершена с кодом состояния: Successful: {:?}", success_taskwrite);

                                        (self.peerid, Err(ErrorPeer::ErrorSuccessTaskWrite(success_taskwrite)))
                                    }
                                }
                            },
                            Err(error_task_write) => {
                                log::error!("[Error: ChannelTaskReadClosed]: Подзадача TaskWrite завершена с кодом состояния: Error: {:?}", error_task_write);

                                (self.peerid, Err(ErrorPeer::ErrorTaskWrite(error_task_write)))
                            }
                        }
                    },
                    Err(error_write) => {
                        log::error!("[Error: ChannelTaskReadClosed]: Подзадача TaskWrite завершена с кодом состояния: Error: {:?}", error_write);

                        (self.peerid, Err(ErrorPeer::ErrorTaskWriteJoinHandle(error_write)))
                    }
                }
            },
            EventsCriticalCompletion::IncidentChannelTaskWriteClosed => {
                let _ = tx_channel_from_peer.send(EventsPeer::Shutdown).await;

                let result_taskread = state_task_read.await;
                match result_taskread {
                    Ok(task_read) => {
                        match task_read {
                            Ok(success_taskread) => {
                                match success_taskread {
                                    SuccessTaskRead::Successful => {
                                        log::info!("[Success: Shutdown]: Подзадача TaskRead успешно завершена с кодом состояния: Successful: {:?}", success_taskread);

                                        (self.peerid, Err(ErrorPeer::ErrorSuccessTaskRead(success_taskread)))
                                    },
                                    SuccessTaskRead::SuccessfulEOF => {
                                        log::info!("[Success: Shutdown]: Подзадача TaskRead успешно завершена с кодом состояния: Successful: {:?}", success_taskread);

                                        (self.peerid, Err(ErrorPeer::ErrorSuccessTaskRead(success_taskread)))
                                    }
                                }
                            },
                            Err(error_taskread) => {
                                log::error!("[Error: ChannelTaskWriteClosed]: Подзадача TaskRead завершена с кодом состояния: Error: {:?}", error_taskread);

                                (self.peerid, Err(ErrorPeer::ErrorTaskRead(error_taskread)))
                            }
                        }
                    },
                    Err(error_read) => {
                        log::error!("[Error: ChannelTaskWriteClosed]: Подзадача TaskRead завершена с кодом состояния: Error: {:?}", error_read);

                        (self.peerid, Err(ErrorPeer::ErrorTaskReadJoinHandle(error_read)))
                    }
                }
            },
            EventsCriticalCompletion::IncidentComplitedTaskRead(result_taskread) => {
                let _ = tx_channel_from_peer_to_task_write.send(EventsPeer::Shutdown).await;

                let result_taskwrite = state_task_write.await;
                match result_taskwrite {
                    Ok(task_taskwrite) => {
                        match (result_taskread, task_taskwrite) {
                            (Ok(success_read), Ok(success_write)) => {
                                match success_read {
                                    SuccessTaskRead::Successful => {
                                        log::info!("[Success: ComplitedTaskRead]: Подзадача TaskRead успешно завершена с кодом состояния: Successful");
                                        log::info!("[Success: ComplitedTaskRead]: Подзадача TaskWrite успешно завершена с кодом состояния: Successful: {:?}", success_write);

                                        (self.peerid, Ok(SuccessPeer::SuccessfulTaskReadTaskWrite))
                                    },
                                    SuccessTaskRead::SuccessfulEOF => {
                                        log::warn!("[Success: ComplitedTaskRead]: Подзадача TaskRead завершена с кодом состояния: Successful: SuccessfulEOF");
                                        log::info!("[Success: ComplitedTaskRead]: Подзадача TaskWrite успешно завершена с кодом состояния: Successful: {:?}", success_write);

                                        (self.peerid, Ok(SuccessPeer::SuccessfulTaskReadBrokenPipeTaskWriteSuccessful))
                                    }
                                }
                            },
                            (Ok(success_read), Err(error_write)) => {
                                log::info!("[Success: ComplitedTaskRead]: Подзадача TaskRead успешно завершена с кодом состояния: Successful: {:?}", success_read);
                                log::error!("[Success: ComplitedTaskRead]: Подзадача TaskWrite завершена с кодом состояния: Error: {:?}", error_write);

                                (self.peerid, Err(ErrorPeer::ErrorSuccessTaskReadErrorTaskWrite{ success_read: success_read, error_write: error_write }))
                            },
                            (Err(error_read), Ok(success_write)) => {
                                match success_write {
                                    SuccessTaskWrite::Successful => {
                                        log::info!("[Success: ComplitedTaskRead]: Подзадача TaskWrite успешно завершена с кодом состояния: Successful: {:?}", success_write);
                                        log::error!("[Success: ComplitedTaskRead]: Подзадача TaskRead завершена с кодом состояния: Error: {:?}", error_read);

                                        (self.peerid, Err(ErrorPeer::ErrorSuccessTaskWriteErrorTaskRead{error_read: error_read, success_write: success_write}))
                                    }
                                }
                            },
                            (Err(error_read), Err(error_write)) => {
                                log::error!("[Success: ComplitedTaskRead]: Подзадача TaskRead завершена с кодом состояния: Error: {:?}", error_read);
                                log::error!("[Success: ComplitedTaskRead]: Подзадача TaskWrite завершена с кодом состояния: Error: {:?}", error_write);

                                (self.peerid, Err(ErrorPeer::ErrorTaskReadTaskWrite{error_read: error_read, error_write: error_write}))
                            }
                        }
                    },
                    Err(task_task_write_error) => {
                        match result_taskread {
                            Ok(success_taskread) => {
                                match success_taskread {
                                    SuccessTaskRead::Successful => {
                                        log::info!("[Success: ComplitedTaskRead]: Подзадача TaskRead успешно завершена с кодом состояния: Error: {:?}", success_taskread);
                                        log::error!("[Success: ComplitedTaskRead]: Подзадача TaskWrite завершена с кодом состояния: Error: {:?}", task_task_write_error);

                                        (self.peerid, Err(ErrorPeer::ErrorSuccessTaskReadErrorJoinHandleTaskWrite{success_read: success_taskread, error_write: task_task_write_error}))
                                    },
                                    SuccessTaskRead::SuccessfulEOF => {
                                        log::info!("[Success: ComplitedTaskRead]: Подзадача TaskRead завершена с кодом состояния: Error: {:?}", success_taskread);
                                        log::error!("[Success: ComplitedTaskRead]: Подзадача TaskWrite завершена с кодом состояния: Error: {:?}", task_task_write_error);

                                        (self.peerid, Err(ErrorPeer::ErrorSuccessTaskReadErrorJoinHandleTaskWrite{success_read: success_taskread, error_write: task_task_write_error}))
                                    }
                                }
                            },
                            Err(error_taskread) => {
                                log::error!("[Success: ComplitedTaskRead]: Подзадача TaskRead завершена с кодом состояния: Error: {:?}", error_taskread);
                                log::error!("[Success: ComplitedTaskRead]: Подзадача TaskWrite завершена с кодом состояния: Error: {:?}", task_task_write_error);

                                (self.peerid, Err(ErrorPeer::ErrorTaskReadErrorJoinHandleTaskWrite{error_read: error_taskread, error_write: task_task_write_error}))
                            }
                        }
                    }
                }
            },
            EventsCriticalCompletion::IncidentComplitedTaskWrite(result_taskwrite) => {
                let _ = tx_channel_from_peer.send(EventsPeer::Shutdown).await;

                let result_taskread = state_task_read.await;
                match result_taskread {
                    Ok(task_taskread) => {
                        match (task_taskread, result_taskwrite) {
                            (Ok(success_read), Ok(success_write)) => {
                                match success_read {
                                    SuccessTaskRead::Successful => {
                                        log::info!("[Success: ComplitedTaskWrite]: Подзадача TaskRead успешно завершена с кодом состояния: Successful");
                                        log::info!("[Success: ComplitedTaskWrite]: Подзадача TaskWrite успешно завершена с кодом состояния: Successful: {:?}", success_write);

                                        (self.peerid, Ok(SuccessPeer::SuccessfulTaskReadTaskWrite))
                                    },
                                    SuccessTaskRead::SuccessfulEOF => {
                                        log::warn!("[Success: ComplitedTaskWrite]: Подзадача TaskRead завершена с кодом состояния: Successful: SuccessfulEOF");
                                        log::info!("[Success: ComplitedTaskWrite]: Подзадача TaskWrite успешно завершена с кодом состояния: Successful: {:?}", success_write);

                                        (self.peerid, Ok(SuccessPeer::SuccessfulTaskReadBrokenPipeTaskWriteSuccessful))
                                    }
                                }
                            },
                            (Ok(success_read), Err(error_write)) => {
                                log::info!("[Success: ComplitedTaskWrite]: Подзадача TaskRead успешно завершена с кодом состояния: Successful: {:?}", success_read);
                                log::error!("[Success: ComplitedTaskWrite]: Подзадача TaskWrite завершена с кодом состояния: Error: {:?}", error_write);

                                (self.peerid, Err(ErrorPeer::ErrorSuccessTaskReadErrorTaskWrite{ success_read: success_read, error_write: error_write }))
                            },
                            (Err(error_read), Ok(success_write)) => {
                                match success_write {
                                    SuccessTaskWrite::Successful => {
                                        log::info!("[Success: ComplitedTaskWrite]: Подзадача TaskWrite успешно завершена с кодом состояния: Successful: {:?}", success_write);
                                        log::error!("[Success: ComplitedTaskWrite]: Подзадача TaskRead завершена с кодом состояния: Error: {:?}", error_read);

                                        (self.peerid, Err(ErrorPeer::ErrorSuccessTaskWriteErrorTaskRead{error_read: error_read, success_write: success_write}))
                                    }
                                }
                            },
                            (Err(error_read), Err(error_write)) => {
                                log::error!("[Success: ComplitedTaskWrite]: Подзадача TaskRead завершена с кодом состояния: Error: {:?}", error_read);
                                log::error!("[Success: ComplitedTaskWrite]: Подзадача TaskWrite завершена с кодом состояния: Error: {:?}", error_write);

                                (self.peerid, Err(ErrorPeer::ErrorTaskReadTaskWrite{error_read: error_read, error_write: error_write}))
                            }
                        }
                    },
                    Err(task_task_read_error) => {
                        match result_taskwrite {
                            Ok(success_taskwrite) => {
                                match success_taskwrite {
                                    SuccessTaskWrite::Successful => {
                                        log::error!("[Success: ComplitedTaskWrite]: Подзадача TaskRead завершена с кодом состояния: Error: {:?}", task_task_read_error);
                                        log::info!("[Success: ComplitedTaskWrite]: Подзадача TaskWrite успешно завершена с кодом состояния: Successful: {:?}", success_taskwrite);

                                        (self.peerid, Err(ErrorPeer::ErrorSuccessTaskWriteErrorJoinHandleTaskRead{error_read: task_task_read_error, success_write: success_taskwrite}))
                                    }
                                }
                            },
                            Err(error_taskwrite) => {
                                log::error!("[Success: ComplitedTaskWrite]: Подзадача TaskRead завершена с кодом состояния: Error: {:?}", task_task_read_error);
                                log::error!("[Success: ComplitedTaskWrite]: Подзадача TaskWrite завершена с кодом состояния: Error: {:?}", error_taskwrite);

                                (self.peerid, Err(ErrorPeer::ErrorTaskWriteErrorJoinHandleTaskRead{error_write: error_taskwrite, error_read: task_task_read_error}))
                            }
                        }
                    }
                }
            },
            EventsCriticalCompletion::IncidentJoinHandleTaskRead(result_joinhandle_task_read) => {
                let _ = tx_channel_from_peer_to_task_write.send(EventsPeer::Shutdown).await;

                let result_taskwrite = state_task_write.await;
                match result_taskwrite {
                    Ok(success_taskwrite) => {
                        match success_taskwrite {
                            Ok(task_taskwrite) => {
                                match task_taskwrite {
                                    SuccessTaskWrite::Successful => {
                                        log::error!("[Success: ErrorJoinHandleTaskRead]: Подзадача TaskRead завершена с кодом состояния: Error: {:?}", result_joinhandle_task_read);
                                        log::info!("[Success: ErrorJoinHandleTaskRead]: Подзадача TaskWrite успешно завершена с кодом состояния: Successful: {:?}", task_taskwrite);

                                        (self.peerid, Err(ErrorPeer::ErrorSuccessTaskWriteErrorJoinHandleTaskRead{error_read: result_joinhandle_task_read, success_write: task_taskwrite}))
                                    }
                                }
                            },
                            Err(error_task_taskwrite) => {
                                log::error!("[Success: ErrorJoinHandleTaskRead]: Подзадача TaskRead завершена с кодом состояния: Error: {:?}", result_joinhandle_task_read);
                                log::error!("[Success: ErrorJoinHandleTaskRead]: Подзадача TaskWrite завершена с кодом состояния: Successful: {:?}", error_task_taskwrite);

                                (self.peerid, Err(ErrorPeer::ErrorTaskWriteErrorJoinHandleTaskRead{error_write: error_task_taskwrite, error_read: result_joinhandle_task_read}))
                            }
                        }
                    },
                    Err(error_joinhandle_taskwrite) => {
                        log::error!("[Success: ErrorJoinHandleTaskRead]: Подзадача TaskRead завершена с кодом состояния: Error: {:?}", result_joinhandle_task_read);
                        log::error!("[Success: ErrorJoinHandleTaskRead]: Подзадача TaskWrite завершена с кодом состояния: Successful: {:?}", error_joinhandle_taskwrite);

                        (self.peerid, Err(ErrorPeer::ErrorJoinHandleTaskReadTaskWrite{error_read: result_joinhandle_task_read, error_write: error_joinhandle_taskwrite}))
                    }
                }
            },
            EventsCriticalCompletion::IncidentJoinHandleTaskWrite(result_joinhandle_task_write) => {
                let _ = tx_channel_from_peer.send(EventsPeer::Shutdown).await;

                let result_taskread = state_task_read.await;
                match result_taskread {
                    Ok(result_task_taskread) => {
                        match result_task_taskread {
                            Ok(task_read) => {
                                match task_read {
                                    SuccessTaskRead::Successful => {
                                        log::info!("[Success: ErrorJoinHandleTaskWrite]: Подзадача TaskRead успешно завершена с кодом состояния: Successful: {:?}", task_read);
                                        log::error!("[Success: ErrorJoinHandleTaskWrite]: Подзадача TaskWrite завершена с кодом состояния: Error: {:?}", result_joinhandle_task_write);

                                        (self.peerid, Err(ErrorPeer::ErrorSuccessTaskReadErrorJoinHandleTaskWrite{success_read: task_read, error_write: result_joinhandle_task_write}))
                                    },
                                    SuccessTaskRead::SuccessfulEOF => {
                                        log::info!("[Success: ErrorJoinHandleTaskWrite]: Подзадача TaskRead успешно завершена с кодом состояния: Successful: {:?}", task_read);
                                        log::error!("[Success: ErrorJoinHandleTaskWrite]: Подзадача TaskWrite завершена с кодом состояния: Error: {:?}", result_joinhandle_task_write);

                                        (self.peerid, Err(ErrorPeer::ErrorSuccessTaskReadErrorJoinHandleTaskWrite{success_read: task_read, error_write: result_joinhandle_task_write}))
                                    }
                                }
                            },
                            Err(error_task_read) => {
                                log::error!("[Success: ErrorJoinHandleTaskWrite]: Подзадача TaskRead завершена с кодом состояния: Error: {:?}", error_task_read);
                                log::error!("[Success: ErrorJoinHandleTaskWrite]: Подзадача TaskWrite завершена с кодом состояния: Error: {:?}", result_joinhandle_task_write);

                                (self.peerid, Err(ErrorPeer::ErrorTaskReadErrorJoinHandleTaskWrite{error_read: error_task_read, error_write: result_joinhandle_task_write}))
                            }
                        }
                    },
                    Err(error_joinhandle_taskread) => {
                        log::error!("[Success: ErrorJoinHandleTaskWrite]: Подзадача TaskRead завершена с кодом состояния: Error: {:?}", error_joinhandle_taskread);
                        log::error!("[Success: ErrorJoinHandleTaskWrite]: Подзадача TaskWrite завершена с кодом состояния: Error: {:?}", result_joinhandle_task_write);

                        (self.peerid, Err(ErrorPeer::ErrorJoinHandleTaskReadTaskWrite{error_read: error_joinhandle_taskread, error_write: result_joinhandle_task_write}))
                    }
                }
            }
        }
    }
}