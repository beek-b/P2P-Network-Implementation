use tokio::{net::TcpListener, sync::mpsc};

use crate::network::error_network::{ErrorLoopListen};
use crate::network::events_network::{EventsLoopListen};

use crate::network::events_network::EventsThreadNetwork;

pub async fn run(mut rx_channel_looplisten_from_thread_network: mpsc::Receiver<EventsThreadNetwork>, tx_channel_looplisten_to_peermanager: mpsc::Sender<EventsLoopListen>) -> Result<(), ErrorLoopListen> {
    log::info!("Цикл событие looplisten был успешно запущено");

    let bind_address = TcpListener::bind("0.0.0.0:8080").await.map_err(|e| ErrorLoopListen::ErrorInputOutput(e))?;
    log::info!("Узел инициализировал порт 8080 и ожидает входящие соединения");

    loop {
        tokio::select! {
            result_accept = bind_address.accept() => {
                match result_accept {
                    Ok((socket_incoming, address)) => {
                        log::info!("Входящее соединение успешно принято по адресу: {}", address);

                        if tx_channel_looplisten_to_peermanager.send(EventsLoopListen::IncomingConnection { socket: socket_incoming, address: address }).await.is_err() {
                            return Err(ErrorLoopListen::ErrorChannelPeerManagerClosed);
                        }
                    },
                    Err(e) => {
                        log::info!("Произошла ошибка во время принятие входящего соединение ошибка: {}", e);
                    }
                }
            },
            incident_thread_network = rx_channel_looplisten_from_thread_network.recv() => {
                match incident_thread_network {
                    Some(EventsThreadNetwork::Shutdown) => {
                        break;
                    },
                    Some(_) => {},
                    None => {
                        return Err(ErrorLoopListen::ErrorChannelThreadNetworkClosed);
                    }
                }
            }
        }
    }
    Ok(())
}