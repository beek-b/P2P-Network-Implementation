use std::thread;
use std::thread::{JoinHandle};
use tokio::{runtime::Builder, sync::mpsc};
use crate::{events_init::EventsInit, network::events_network::{EventsPeerManager, EventsThreadNetworkProtocol}, protocol::{error_protocol::ErrorThreadProtocol, events_protocol::{EventsHandleMessage, EventsThreadProtocol}, ttype_protocol::EventsCriticalCompletionThreadProtocol}};
use crate::protocol::handlemessage::{HandleMessage};

pub fn spawn_thread_protocol(mut rx_channel_protocol: mpsc::UnboundedReceiver<EventsInit>, tx_threadnetwork_and_protocol_to_init: mpsc::UnboundedSender<EventsThreadNetworkProtocol>, rx_channel_protocol_from_network: mpsc::Receiver<EventsPeerManager>, tx_channel_protocol_to_network: mpsc::Sender<EventsHandleMessage>) -> JoinHandle<Result<(), ErrorThreadProtocol>> {
    let state_thread_protocol = thread::spawn(move || {
        log::info!("Системный поток bcprotocol-thread поднять успешно");
        let runtime_protocol = Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| ErrorThreadProtocol::ErrorInputOutput(e))?;

        runtime_protocol.block_on(async move {
            let (tx_channel_from_protocol_to_handlemessage, rx_channel_from_protocol_handlemessage) = mpsc::channel::<EventsThreadProtocol>(1024); // events from threadprotocol --> HandleMessage

            let mut state_handlemessage = tokio::spawn(async move {
                let handlemessage = HandleMessage::create(rx_channel_from_protocol_handlemessage, tx_channel_protocol_to_network, rx_channel_protocol_from_network);
                handlemessage.run().await
            });

            let critical_result_protocolloop = loop {
                tokio::select! {
                    incident_init = rx_channel_protocol.recv() => {
                        match incident_init {
                            Some(EventsInit::Shutdown) => {
                                break EventsCriticalCompletionThreadProtocol::IncidentConfirmationShutdown;
                            },
                            None => {
                                break EventsCriticalCompletionThreadProtocol::IncidentChannelInitClosed;
                            }
                        }
                    },
                    result_state_handlemessage = &mut state_handlemessage => {
                        break match result_state_handlemessage {
                            Ok(ok_handlemessage) => {
                                EventsCriticalCompletionThreadProtocol::IncidentComplitedHandleMessage(ok_handlemessage)
                            },
                            Err(error_handlemessage) => {
                                EventsCriticalCompletionThreadProtocol::IncidentJoinHandleHandleMessage(error_handlemessage)
                            }
                        }
                    }
                }
            };

            match critical_result_protocolloop {
                EventsCriticalCompletionThreadProtocol::IncidentConfirmationShutdown => {
                    let _ = tx_channel_from_protocol_to_handlemessage.send(EventsThreadProtocol::Shutdown).await;

                    let result_state_handlemessage = state_handlemessage.await;
                    match result_state_handlemessage {
                        Ok(result_handlemessage) => {
                            match result_handlemessage {
                                Ok(_) => {
                                    let _ = tx_threadnetwork_and_protocol_to_init.send(EventsThreadNetworkProtocol::ExitCompletionThreadProtocol);

                                    Ok(())
                                },
                                Err(error) => {
                                    let _ = tx_threadnetwork_and_protocol_to_init.send(EventsThreadNetworkProtocol::ExitCompletionThreadProtocol);

                                    Err(ErrorThreadProtocol::ErrorHandleMessage(error))
                                }
                            }
                        },
                        Err(error_joinhandle) => {
                            let _ = tx_threadnetwork_and_protocol_to_init.send(EventsThreadNetworkProtocol::ExitCompletionThreadProtocol);

                            Err(ErrorThreadProtocol::ErrorJoinHandleHandleMessage(error_joinhandle))
                        }
                    }
                },
                EventsCriticalCompletionThreadProtocol::IncidentChannelInitClosed => {
                    let _ = tx_channel_from_protocol_to_handlemessage.send(EventsThreadProtocol::Shutdown).await;

                    let result_state_handlemessage = state_handlemessage.await;
                    match result_state_handlemessage {
                        Ok(result_handlemessage) => {
                            match result_handlemessage {
                                Ok(_) => {
                                    let _ = tx_threadnetwork_and_protocol_to_init.send(EventsThreadNetworkProtocol::ExitCompletionThreadProtocol);

                                    Ok(())
                                },
                                Err(error) => {
                                    let _ = tx_threadnetwork_and_protocol_to_init.send(EventsThreadNetworkProtocol::ExitCompletionThreadProtocol);

                                    Err(ErrorThreadProtocol::ErrorHandleMessage(error))
                                }
                            }
                        },
                        Err(error_joinhandle) => {
                            let _ = tx_threadnetwork_and_protocol_to_init.send(EventsThreadNetworkProtocol::ExitCompletionThreadProtocol);

                            Err(ErrorThreadProtocol::ErrorJoinHandleHandleMessage(error_joinhandle))
                        }
                    }
                },
                EventsCriticalCompletionThreadProtocol::IncidentComplitedHandleMessage(result_handlemessage) => {
                    match result_handlemessage {
                        Ok(_) => {
                            let _ = tx_threadnetwork_and_protocol_to_init.send(EventsThreadNetworkProtocol::ExitCompletionThreadProtocol);

                            Ok(())
                        },
                        Err(error_handlemessage) => {
                            let _ = tx_threadnetwork_and_protocol_to_init.send(EventsThreadNetworkProtocol::ExitCompletionThreadProtocol);
                            
                            Err(ErrorThreadProtocol::ErrorHandleMessage(error_handlemessage))
                        }
                    }
                },
                EventsCriticalCompletionThreadProtocol::IncidentJoinHandleHandleMessage(error_joinerror) => {
                    Err(ErrorThreadProtocol::ErrorJoinHandleHandleMessage(error_joinerror))
                }
            }
        })
    });

    state_thread_protocol
}