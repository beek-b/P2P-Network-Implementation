use std::{thread::sleep, time::Duration};

use demonstration_of_the_peer_to_peer_network::{
    error_init::ErrorInit,
    events_init::EventsInit,
    network::{
        events_network::EventsThreadNetworkProtocol,
        thread_network::spawn_thread_network,
    },
    protocol::thread_protocol::spawn_thread_protocol,
};

use tokio::sync::mpsc;

fn main() -> Result<(), ErrorInit> {
    env_logger::init();

    log::info!("Bitcoin Center Node 1 версия v0.0.1 успешно начал процесс инициализаций");

    let (tx_channel_init_to_network, rx_channel_network) = mpsc::unbounded_channel::<EventsInit>(); // events from init to network
    let (tx_channel_init_to_protocol, rx_channel_protocol) = mpsc::unbounded_channel::<EventsInit>(); // events from init to protocol

    let (tx_threadnetwork_and_protocol_to_init, mut rx_init_from_threadnetwork_and_protocol) = mpsc::unbounded_channel::<EventsThreadNetworkProtocol>(); // events from network || protocol --> init

    let (tx_channel_network_to_protocol, rx_channel_protocol_from_network) = mpsc::channel(1024); // events from network --> protocol
    let (tx_channel_protocol_to_network, rx_channel_network_from_protocol) = mpsc::channel(1024); // events from protocol --> network

    let tx_threadnetwork_and_protocol_to_init_clone = tx_threadnetwork_and_protocol_to_init.clone(); // clone events from network --> init
    let state_thread_network = spawn_thread_network(
        rx_channel_network, 
        tx_channel_network_to_protocol, 
        rx_channel_network_from_protocol, 
        tx_threadnetwork_and_protocol_to_init_clone);
    
    let state_thread_protocol = spawn_thread_protocol(
        rx_channel_protocol, 
        tx_threadnetwork_and_protocol_to_init, 
        rx_channel_protocol_from_network, 
        tx_channel_protocol_to_network);

        // 1. Оборачиваем JoinHandle в Option, чтобы их можно было забрать из цикла один раз
    let mut state_thread_network = Some(state_thread_network);
    let mut state_thread_protocol = Some(state_thread_protocol);

    // sleep(Duration::from_secs(30));
    // log::info!("Init отправил событие Shutdown");

    // let _ = tx_channel_init_to_network.send(EventsInit::Shutdown);
    // let _ = tx_channel_init_to_protocol.send(EventsInit::Shutdown);

    while let Some(state_thread) = rx_init_from_threadnetwork_and_protocol.blocking_recv() {
        match state_thread {
            EventsThreadNetworkProtocol::ExitCompletionThreadNetwork => {
                // Сигнализируем второму потоку завершиться и выходим из цикла
                let _ = tx_channel_init_to_protocol.send(EventsInit::Shutdown);
                break;
            }
            EventsThreadNetworkProtocol::ExitCompletionThreadProtocol => {
                // Сигнализируем первому потоку завершиться и выходим из цикла
                let _ = tx_channel_init_to_network.send(EventsInit::Shutdown);
                break;
            }
        }
    }

    // 2. Гарантированно забираем handle обоих потоков (даже если Shutdown не отправился)
    let handle_network = state_thread_network
        .take()
        .expect("Network handle already taken");
    let handle_protocol = state_thread_protocol
        .take()
        .expect("Protocol handle already taken");

    // 3. Выполняем join строго 1 раз для каждого потока
    let result_network = handle_network.join();
    let result_protocol = handle_protocol.join();

    // 4. Единый сопоставитель (match) для вычисления ErrorInit
    match (result_network, result_protocol) {
        (Ok(Ok(_)), Ok(Ok(_))) => Ok(()),

        // Логические ошибки
        (Ok(Err(net_err)), Ok(Ok(_))) => Err(ErrorInit::ErrorThreadNetwork(Ok(net_err))),
        (Ok(Ok(_)), Ok(Err(pro_err))) => Err(ErrorInit::ErrorThreadProtocol(Ok(pro_err))),
        (Ok(Err(net_err)), Ok(Err(pro_err))) => Err(ErrorInit::ErrorThreadNetworkThreadProtocol {
            network: Ok(net_err),
            protocol: Ok(pro_err),
        }),

        // Паники в потоках
        (Err(net_panic), Ok(Ok(_))) => Err(ErrorInit::ErrorThreadNetwork(Err(net_panic))),
        (Ok(Ok(_)), Err(pro_panic)) => Err(ErrorInit::ErrorThreadProtocol(Err(pro_panic))),
        (Err(net_panic), Err(pro_panic)) => Err(ErrorInit::ErrorThreadNetworkThreadProtocol {
            network: Err(net_panic),
            protocol: Err(pro_panic),
        }),

        // Смешанные случаи (один паниковал, у второго логическая ошибка)
        (Err(net_panic), Ok(Err(pro_err))) => Err(ErrorInit::ErrorThreadNetworkThreadProtocol {
            network: Err(net_panic),
            protocol: Ok(pro_err),
        }),
        (Ok(Err(net_err)), Err(pro_panic)) => Err(ErrorInit::ErrorThreadNetworkThreadProtocol {
            network: Ok(net_err),
            protocol: Err(pro_panic),
        }),
    }
}