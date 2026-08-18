use std::net::{Ipv4Addr, SocketAddrV4};
use std::thread::{self, JoinHandle};
use tokio::time::{sleep, Duration};
use tokio::{sync::mpsc, runtime::Builder};

use crate::{events_init::EventsInit};
use crate::network::{error_network::ErrorThreadNetwork, events_network::{EventsLoopListen, EventsPeerManager, EventsThreadNetwork, EventsThreadNetworkProtocol}, looplisten::run, peermanager::PeerManager, ttype::EventsCriticalCompletionThreadNetwork};
use crate::protocol::events_protocol::{EventsHandleMessage};

pub fn spawn_thread_network(mut rx_channel_network: mpsc::UnboundedReceiver<EventsInit>, tx_channel_network_to_protocol: mpsc::Sender<EventsPeerManager>, rx_channel_network_from_protocol: mpsc::Receiver<EventsHandleMessage>, tx_threadnetwork_and_protocol_to_init_clone: mpsc::UnboundedSender<EventsThreadNetworkProtocol>) -> JoinHandle<Result<(), ErrorThreadNetwork>> {
    let state_thread_network = thread::spawn(move || {
        log::info!("Системный поток bcnetwork-thread поднять успешно");
        let runtime_network = Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| ErrorThreadNetwork::ErrorInputOutput(e))?;

        runtime_network.block_on(async move {
            let (tx_channel_network_to_peermanager, rx_channel_peermanager) = mpsc::channel::<EventsThreadNetwork>(1024); // events from thread_network to peermanager
            let (tx_channel_looplisten, rx_channel_peermanager_from_looplisten) = mpsc::channel::<EventsLoopListen>(1024); // events from peermanager to looplisten
            
            let mut state_task_peermanager = tokio::spawn(async move {
                let peermanager = PeerManager::create(rx_channel_peermanager, rx_channel_peermanager_from_looplisten, tx_channel_network_to_protocol, rx_channel_network_from_protocol);
                peermanager.run().await
            });

            let (tx_channel_network_to_looplisten, rx_channel_looplisten) = mpsc::channel::<EventsThreadNetwork>(1024); // events from thread_network to looplisten

            let mut state_task_looplisten = tokio::spawn(async move {
                run(rx_channel_looplisten, tx_channel_looplisten).await
            });

            // sleep(Duration::from_secs(30)).await;
            // let _ = tx_channel_network_to_peermanager.send(EventsThreadNetwork::OutgoingConnection(std::net::SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(172, 20, 10, 2), 8333)))).await;

            let critical_result_networkloop = loop {
                tokio::select! {
                    incident_init = rx_channel_network.recv() => {
                        match incident_init {
                            Some(EventsInit::Shutdown) => {
                                break EventsCriticalCompletionThreadNetwork::IncidentConfirmationShutdown;
                            },
                            None => {
                                break EventsCriticalCompletionThreadNetwork::IncidentChannelInitClosed;
                            }
                        }
                    },
                    result_state_peermanager = &mut state_task_peermanager => {
                        break match result_state_peermanager {
                            Ok(ok_peermanager) => {
                                EventsCriticalCompletionThreadNetwork::IncidentComplitedPeerManager(ok_peermanager)
                            },
                            Err(error_peermanager) => {
                                EventsCriticalCompletionThreadNetwork::IncidentJoinHandlePeerManager(error_peermanager)
                            }
                        }
                    },
                    result_state_looplisten = &mut state_task_looplisten => {
                        break match result_state_looplisten {
                            Ok(ok_looplisten) => {
                                EventsCriticalCompletionThreadNetwork::IncidentComplitedLoopListen(ok_looplisten)
                            },
                            Err(error_looplisten) => {
                                EventsCriticalCompletionThreadNetwork::IncidentJoinHandleLoopListen(error_looplisten)
                            }
                        }
                    }
                }
            };
            
            match critical_result_networkloop {
                EventsCriticalCompletionThreadNetwork::IncidentConfirmationShutdown => {
                    let _ = tx_channel_network_to_peermanager.send(EventsThreadNetwork::Shutdown).await;
                    let _ = tx_channel_network_to_looplisten.send(EventsThreadNetwork::Shutdown).await;

                    let result_state_tasks = tokio::join!(state_task_peermanager, state_task_looplisten);
                    match result_state_tasks {
                        (Ok(state_peermanager), Ok(state_looplisten)) => {
                            match (state_peermanager, state_looplisten) {
                                (Ok(_), Ok(_)) => {
                                    log::info!("[Success: Shutdown]: ЦиклЗадача PeerManager успешно завершена с кодом состояния: Successful");
                                    log::info!("[Success: Shutdown]: ЦиклЗадача LoopListen успешно завершена с кодом состояния: Successful");

                                    let _ = tx_threadnetwork_and_protocol_to_init_clone.send(EventsThreadNetworkProtocol::ExitCompletionThreadNetwork);

                                    Ok(())
                                },
                                (Ok(_), Err(error_looplisten)) => {
                                    log::info!("[Success: Shutdown]: ЦиклЗадача PeerManager успешно завершена с кодом состояния: Successful");
                                    log::error!("[Success: Shutdown]: ЦиклЗадача LoopListen завершена с кодом состояния: Error: {:?}", error_looplisten);

                                    let _ = tx_threadnetwork_and_protocol_to_init_clone.send(EventsThreadNetworkProtocol::ExitCompletionThreadNetwork);

                                    Err(ErrorThreadNetwork::ErrorLoopListen(error_looplisten))
                                },
                                (Err(error_peermanager), Ok(_)) => {
                                    log::error!("[Success: Shutdown]: ЦиклЗадача PeerManager завершена с кодом состояния: Error: {:?}", error_peermanager);
                                    log::info!("[Success: Shutdown]: ЦиклЗадача LoopListen успешно завершена с кодом состояния: Successful");

                                    let _ = tx_threadnetwork_and_protocol_to_init_clone.send(EventsThreadNetworkProtocol::ExitCompletionThreadNetwork);

                                    Err(ErrorThreadNetwork::ErrorPeerManager(error_peermanager))
                                },
                                (Err(error_peermanager), Err(error_looplisten)) => {
                                    log::error!("[Success: Shutdown]: ЦиклЗадача PeerManager завершена с кодом состояния: Error: {:?}", error_peermanager);
                                    log::error!("[Success: Shutdown]: ЦиклЗадача LoopListen завершена с кодом состояния: Error: {:?}", error_looplisten);

                                    let _ = tx_threadnetwork_and_protocol_to_init_clone.send(EventsThreadNetworkProtocol::ExitCompletionThreadNetwork);

                                    Err(ErrorThreadNetwork::ErrorPeerManagerErrorLoopListen{error_peermanager: error_peermanager, error_looplisten: error_looplisten})
                                }
                            }
                        },
                        (Ok(state_peermanager), Err(error_joinhandle_looplisten)) => {
                            match state_peermanager {
                                Ok(_) => {
                                    log::info!("[Success: Shutdown]: ЦиклЗадача PeerManager успешно завершена с кодом состояния: Successful");
                                    log::error!("[Success: Shutdown]: ЦиклЗадача LoopListen завершена с кодом состояния: Error: {:?}", error_joinhandle_looplisten);

                                    let _ = tx_threadnetwork_and_protocol_to_init_clone.send(EventsThreadNetworkProtocol::ExitCompletionThreadNetwork);

                                    Err(ErrorThreadNetwork::ErrorLoopListenJoinHandle(error_joinhandle_looplisten))
                                },
                                Err(error_pm) => {
                                    log::error!("[Success: Shutdown]: ЦиклЗадача PeerManager завершена с кодом состояния: Error: {:?}", error_pm);
                                    log::error!("[Success: Shutdown]: ЦиклЗадача LoopListen завершена с кодом состояния: Error: {:?}", error_joinhandle_looplisten);

                                    let _ = tx_threadnetwork_and_protocol_to_init_clone.send(EventsThreadNetworkProtocol::ExitCompletionThreadNetwork);

                                    Err(ErrorThreadNetwork::ErrorPeerManagerJoinHandleLoopListen{error_peermanager: error_pm, error_joinhandle_looplisten: error_joinhandle_looplisten})
                                }
                            }
                        },
                        (Err(error_joinhandle_peermanager), Ok(state_looplisten)) => {
                            match state_looplisten {
                                Ok(_) => {
                                    log::info!("[Success: Shutdown]: ЦиклЗадача LoopListen успешно завершена с кодом состояния: Successful");
                                    log::error!("[Success: Shutdown]: ЦиклЗадача PeerManager завершена с кодом состояния: Error: {:?}", error_joinhandle_peermanager);

                                    let _ = tx_threadnetwork_and_protocol_to_init_clone.send(EventsThreadNetworkProtocol::ExitCompletionThreadNetwork);

                                    Err(ErrorThreadNetwork::ErrorPeerManagerJoinHandle(error_joinhandle_peermanager))
                                },
                                Err(error_ll) => {
                                    log::error!("[Success: Shutdown]: ЦиклЗадача LoopListen завершена с кодом состояния: Error: {:?}", error_ll);
                                    log::error!("[Success: Shutdown]: ЦиклЗадача PeerManager завершена с кодом состояния: Error: {:?}", error_joinhandle_peermanager);

                                    let _ = tx_threadnetwork_and_protocol_to_init_clone.send(EventsThreadNetworkProtocol::ExitCompletionThreadNetwork);

                                    Err(ErrorThreadNetwork::ErrorLoopListenJoinHandlePeerManager{error_looplisten: error_ll, error_joinhandle_peermanager: error_joinhandle_peermanager})
                                }
                            }
                        },
                        (Err(error_joinhandle_peermanager), Err(error_joinhandle_looplisten)) => {
                            log::error!("[Success: Shutdown]: ЦиклЗадача PeerManager завершена с кодом состояния: Error: {:?}", error_joinhandle_peermanager);
                            log::error!("[Success: Shutdown]: ЦиклЗадача LoopListen завершена с кодом состояния: Error: {:?}", error_joinhandle_looplisten);

                            let _ = tx_threadnetwork_and_protocol_to_init_clone.send(EventsThreadNetworkProtocol::ExitCompletionThreadNetwork);

                            Err(ErrorThreadNetwork::ErrorPeerManagerErrorLoopListenJoinHandle{error_joinhandle_peermanager: error_joinhandle_peermanager, error_joinhandle_looplisten: error_joinhandle_looplisten})
                        }
                    }
                },
                EventsCriticalCompletionThreadNetwork::IncidentChannelInitClosed => {
                    let _ = tx_channel_network_to_peermanager.send(EventsThreadNetwork::Shutdown).await;
                    let _ = tx_channel_network_to_looplisten.send(EventsThreadNetwork::Shutdown).await;

                    let result_state_tasks = tokio::join!(state_task_peermanager, state_task_looplisten);
                    match result_state_tasks {
                        (Ok(state_peermanager), Ok(state_looplisten)) => {
                            match (state_peermanager, state_looplisten) {
                                (Ok(_), Ok(_)) => {
                                    log::info!("[Success: ChannelInitClosed]: ЦиклЗадача PeerManager успешно завершена с кодом состояния: Successful");
                                    log::info!("[Success: ChannelInitClosed]: ЦиклЗадача LoopListen успешно завершена с кодом состояния: Successful");

                                    let _ = tx_threadnetwork_and_protocol_to_init_clone.send(EventsThreadNetworkProtocol::ExitCompletionThreadNetwork);

                                    Ok(())
                                },
                                (Ok(_), Err(error_looplisten)) => {
                                    log::info!("[Success: ChannelInitClosed]: ЦиклЗадача PeerManager успешно завершена с кодом состояния: Successful");
                                    log::error!("[Success: ChannelInitClosed]: ЦиклЗадача LoopListen завершена с кодом состояния: Error: {:?}", error_looplisten);

                                    let _ = tx_threadnetwork_and_protocol_to_init_clone.send(EventsThreadNetworkProtocol::ExitCompletionThreadNetwork);

                                    Err(ErrorThreadNetwork::ErrorLoopListen(error_looplisten))
                                },
                                (Err(error_peermanager), Ok(_)) => {
                                    log::error!("[Success: ChannelInitClosed]: ЦиклЗадача PeerManager завершена с кодом состояния: Error: {:?}", error_peermanager);
                                    log::info!("[Success: ChannelInitClosed]: ЦиклЗадача LoopListen успешно завершена с кодом состояния: Successful");

                                    let _ = tx_threadnetwork_and_protocol_to_init_clone.send(EventsThreadNetworkProtocol::ExitCompletionThreadNetwork);

                                    Err(ErrorThreadNetwork::ErrorPeerManager(error_peermanager))
                                },
                                (Err(error_peermanager), Err(error_looplisten)) => {
                                    log::error!("[Success: ChannelInitClosed]: ЦиклЗадача PeerManager завершена с кодом состояния: Error: {:?}", error_peermanager);
                                    log::error!("[Success: ChannelInitClosed]: ЦиклЗадача LoopListen завершена с кодом состояния: Error: {:?}", error_looplisten);

                                    let _ = tx_threadnetwork_and_protocol_to_init_clone.send(EventsThreadNetworkProtocol::ExitCompletionThreadNetwork);

                                    Err(ErrorThreadNetwork::ErrorPeerManagerErrorLoopListen{error_peermanager: error_peermanager, error_looplisten: error_looplisten})
                                }
                            }
                        },
                        (Ok(state_peermanager), Err(error_joinhandle_looplisten)) => {
                            match state_peermanager {
                                Ok(_) => {
                                    log::info!("[Success: ChannelInitClosed]: ЦиклЗадача PeerManager успешно завершена с кодом состояния: Successful");
                                    log::error!("[Success: ChannelInitClosed]: ЦиклЗадача LoopListen завершена с кодом состояния: Error: {:?}", error_joinhandle_looplisten);

                                    let _ = tx_threadnetwork_and_protocol_to_init_clone.send(EventsThreadNetworkProtocol::ExitCompletionThreadNetwork);

                                    Err(ErrorThreadNetwork::ErrorLoopListenJoinHandle(error_joinhandle_looplisten))
                                },
                                Err(error_pm) => {
                                    log::error!("[Success: ChannelInitClosed]: ЦиклЗадача PeerManager завершена с кодом состояния: Error: {:?}", error_pm);
                                    log::error!("[Success: ChannelInitClosed]: ЦиклЗадача LoopListen завершена с кодом состояния: Error: {:?}", error_joinhandle_looplisten);

                                    let _ = tx_threadnetwork_and_protocol_to_init_clone.send(EventsThreadNetworkProtocol::ExitCompletionThreadNetwork);

                                    Err(ErrorThreadNetwork::ErrorPeerManagerJoinHandleLoopListen{error_peermanager: error_pm, error_joinhandle_looplisten: error_joinhandle_looplisten})
                                }
                            }
                        },
                        (Err(error_joinhandle_peermanager), Ok(state_looplisten)) => {
                            match state_looplisten {
                                Ok(_) => {
                                    log::info!("[Success: ChannelInitClosed]: ЦиклЗадача LoopListen успешно завершена с кодом состояния: Successful");
                                    log::error!("[Success: ChannelInitClosed]: ЦиклЗадача PeerManager завершена с кодом состояния: Error: {:?}", error_joinhandle_peermanager);

                                    let _ = tx_threadnetwork_and_protocol_to_init_clone.send(EventsThreadNetworkProtocol::ExitCompletionThreadNetwork);

                                    Err(ErrorThreadNetwork::ErrorPeerManagerJoinHandle(error_joinhandle_peermanager))
                                },
                                Err(error_ll) => {
                                    log::error!("[Success: ChannelInitClosed]: ЦиклЗадача LoopListen завершена с кодом состояния: Error: {:?}", error_ll);
                                    log::error!("[Success: ChannelInitClosed]: ЦиклЗадача PeerManager завершена с кодом состояния: Error: {:?}", error_joinhandle_peermanager);

                                    let _ = tx_threadnetwork_and_protocol_to_init_clone.send(EventsThreadNetworkProtocol::ExitCompletionThreadNetwork);

                                    Err(ErrorThreadNetwork::ErrorLoopListenJoinHandlePeerManager{error_looplisten: error_ll, error_joinhandle_peermanager: error_joinhandle_peermanager})
                                }
                            }
                        },
                        (Err(error_joinhandle_peermanager), Err(error_joinhandle_looplisten)) => {
                            log::error!("[Success: ChannelInitClosed]: ЦиклЗадача PeerManager завершена с кодом состояния: Error: {:?}", error_joinhandle_peermanager);
                            log::error!("[Success: ChannelInitClosed]: ЦиклЗадача LoopListen завершена с кодом состояния: Error: {:?}", error_joinhandle_looplisten);

                            let _ = tx_threadnetwork_and_protocol_to_init_clone.send(EventsThreadNetworkProtocol::ExitCompletionThreadNetwork);

                            Err(ErrorThreadNetwork::ErrorPeerManagerErrorLoopListenJoinHandle{error_joinhandle_peermanager: error_joinhandle_peermanager, error_joinhandle_looplisten: error_joinhandle_looplisten})
                        }
                    }
                },
                EventsCriticalCompletionThreadNetwork::IncidentComplitedPeerManager(result_peermanager) => {
                    let _ = tx_channel_network_to_looplisten.send(EventsThreadNetwork::Shutdown).await;

                    let result_state_looplisten = state_task_looplisten.await;
                    match result_state_looplisten {
                        Ok(ok_looplisten) => {
                            match (result_peermanager, ok_looplisten) {
                                (Ok(_), Ok(_)) => {
                                    log::info!("[Success: ComplitedPeerManager]: ЦиклЗадача PeerManager успешно завершена с кодом состояния: Successful");
                                    log::info!("[Success: ComplitedPeerManager]: ЦиклЗадача LoopListen успешно завершена с кодом состояния: Successful");

                                    let _ = tx_threadnetwork_and_protocol_to_init_clone.send(EventsThreadNetworkProtocol::ExitCompletionThreadNetwork);

                                    Ok(())
                                },
                                (Ok(_), Err(error_ll_joinerror)) => {
                                    log::info!("[Success: ComplitedPeerManager]: ЦиклЗадача PeerManager успешно завершена с кодом состояния: Successful");
                                    log::error!("[Success: ComplitedPeerManager]: ЦиклЗадача LoopListen завершена с кодом состояния: Error: {:?}", error_ll_joinerror);

                                    let _ = tx_threadnetwork_and_protocol_to_init_clone.send(EventsThreadNetworkProtocol::ExitCompletionThreadNetwork);

                                    Err(ErrorThreadNetwork::ErrorLoopListen(error_ll_joinerror))
                                },
                                (Err(error_pm_joinerror), Ok(_)) => {
                                    log::error!("[Success: ComplitedPeerManager]: ЦиклЗадача PeerManager завершена с кодом состояния: Error: {:?}", error_pm_joinerror);
                                    log::info!("[Success: ComplitedPeerManager]: ЦиклЗадача LoopListen успешно завершена с кодом состояния: Successful");

                                    let _ = tx_threadnetwork_and_protocol_to_init_clone.send(EventsThreadNetworkProtocol::ExitCompletionThreadNetwork);

                                    Err(ErrorThreadNetwork::ErrorPeerManager(error_pm_joinerror))
                                },
                                (Err(error_pm_joinerror), Err(error_ll_joinerror)) => {
                                    log::error!("[Success: ComplitedPeerManager]: ЦиклЗадача PeerManager завершена с кодом состояния: Error: {:?}", error_pm_joinerror);
                                    log::error!("[Success: ComplitedPeerManager]: ЦиклЗадача LoopListen завершена с кодом состояния: Error: {:?}", error_ll_joinerror);

                                    let _ = tx_threadnetwork_and_protocol_to_init_clone.send(EventsThreadNetworkProtocol::ExitCompletionThreadNetwork);

                                    Err(ErrorThreadNetwork::ErrorPeerManagerErrorLoopListen{error_peermanager: error_pm_joinerror, error_looplisten: error_ll_joinerror})
                                }
                            }
                        },
                        Err(error_looplisten) => {
                            log::error!("[Success: ComplitedPeerManager]: ЦиклЗадача LoopListen завершена с кодом состояния: Error: {:?}", error_looplisten);

                            let _ = tx_threadnetwork_and_protocol_to_init_clone.send(EventsThreadNetworkProtocol::ExitCompletionThreadNetwork);

                            Err(ErrorThreadNetwork::ErrorLoopListenJoinHandle(error_looplisten))
                        }
                    }
                },
                EventsCriticalCompletionThreadNetwork::IncidentComplitedLoopListen(result_looplisten) => {
                    let _ = tx_channel_network_to_peermanager.send(EventsThreadNetwork::Shutdown).await;

                    let result_state_peermanager = state_task_peermanager.await;
                    match result_state_peermanager {
                        Ok(ok_peermanager) => {
                            match (ok_peermanager, result_looplisten) {
                                (Ok(_), Ok(_)) => {
                                    log::info!("[Success: ComplitedLoopListen]: ЦиклЗадача PeerManager успешно завершена с кодом состояния: Successful");
                                    log::info!("[Success: ComplitedLoopListen]: ЦиклЗадача LoopListen успешно завершена с кодом состояния: Successful");

                                    let _ = tx_threadnetwork_and_protocol_to_init_clone.send(EventsThreadNetworkProtocol::ExitCompletionThreadNetwork);

                                    Ok(())
                                },
                                (Ok(_), Err(error_looplisten)) => {
                                    log::info!("[Success: ComplitedLoopListen]: ЦиклЗадача PeerManager успешно завершена с кодом состояния: Successful");
                                    log::error!("[Success: ComplitedLoopListen]: ЦиклЗадача LoopListen завершена с кодом состояния: Error: {:?}", error_looplisten);

                                    let _ = tx_threadnetwork_and_protocol_to_init_clone.send(EventsThreadNetworkProtocol::ExitCompletionThreadNetwork);

                                    Err(ErrorThreadNetwork::ErrorLoopListen(error_looplisten))
                                },
                                (Err(error_peermanager), Ok(_)) => {
                                    log::error!("[Success: ComplitedLoopListen]: ЦиклЗадача PeerManager завершена с кодом состояния: Error: {:?}", error_peermanager);
                                    log::info!("[Success: ComplitedLoopListen]: ЦиклЗадача LoopListen успешно завершена с кодом состояния: Successful");

                                    let _ = tx_threadnetwork_and_protocol_to_init_clone.send(EventsThreadNetworkProtocol::ExitCompletionThreadNetwork);

                                    Err(ErrorThreadNetwork::ErrorPeerManager(error_peermanager))
                                },
                                (Err(error_peermanager), Err(error_looplisten)) => {
                                    log::error!("[Success: ComplitedLoopListen]: ЦиклЗадача PeerManager завершена с кодом состояния: Error: {:?}", error_peermanager);
                                    log::error!("[Success: ComplitedLoopListen]: ЦиклЗадача LoopListen завершена с кодом состояния: Error: {:?}", error_looplisten);

                                    let _ = tx_threadnetwork_and_protocol_to_init_clone.send(EventsThreadNetworkProtocol::ExitCompletionThreadNetwork);

                                    Err(ErrorThreadNetwork::ErrorPeerManagerErrorLoopListen{error_peermanager: error_peermanager, error_looplisten: error_looplisten})
                                }
                            }
                        },
                        Err(error_peermanager) => {
                            log::error!("[Success: ComplitedLoopListen]: ЦиклЗадача PeerManager завершена с кодом состояния: Error: {:?}", error_peermanager);

                            let _ = tx_threadnetwork_and_protocol_to_init_clone.send(EventsThreadNetworkProtocol::ExitCompletionThreadNetwork);

                            Err(ErrorThreadNetwork::ErrorPeerManagerJoinHandle(error_peermanager))
                        }
                    }
                },
                EventsCriticalCompletionThreadNetwork::IncidentJoinHandlePeerManager(result_joinhandle_peermanager) => {
                    log::error!("[Success: ComplitedJoinHandlePeerManager]: ЦиклЗадача PeerManager завершена с кодом состояния: Error: {:?}", result_joinhandle_peermanager);

                    let _ = tx_threadnetwork_and_protocol_to_init_clone.send(EventsThreadNetworkProtocol::ExitCompletionThreadNetwork);

                    Err(ErrorThreadNetwork::ErrorPeerManagerJoinHandle(result_joinhandle_peermanager))
                },
                EventsCriticalCompletionThreadNetwork::IncidentJoinHandleLoopListen(result_joinhandle_looplisten) => {
                    log::error!("[Success: ComplitedJoinHandleLoopListen]: ЦиклЗадача LoopListen завершена с кодом состояния: Error: {:?}", result_joinhandle_looplisten);

                    let _ = tx_threadnetwork_and_protocol_to_init_clone.send(EventsThreadNetworkProtocol::ExitCompletionThreadNetwork);

                    Err(ErrorThreadNetwork::ErrorLoopListenJoinHandle(result_joinhandle_looplisten))
                }
            }
        })
    });
    state_thread_network
}