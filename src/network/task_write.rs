use tokio::{io::AsyncWriteExt, net::tcp::OwnedWriteHalf, sync::mpsc};

use crate::network::{error_network::{ErrorTaskWrite}, events_network::EventsPeer, success_network::SuccessTaskWrite, ttype::MessagePayload, wire::{BYTES_VERACK, BYTES_VERSION, HeaderMessage, checksum_calculate}};

pub async fn start_task_write(mut part_socket_write: OwnedWriteHalf, mut rx_channel_peer_to_taskwrite: mpsc::Receiver<EventsPeer>) -> Result<SuccessTaskWrite, ErrorTaskWrite> {
    loop {
        match rx_channel_peer_to_taskwrite.recv().await {
            Some(EventsPeer::LowLevelMessage(message)) => {
                let ready_header_message = create_header_message(&message);

                match message {
                    MessagePayload::Version(version) => {
                        part_socket_write.write_all(&ready_header_message.serialize_header_message()).await.map_err(|e| ErrorTaskWrite::ErrorInputOutput(e))?;
                        part_socket_write.write_all(&version.serialize_version_message()).await.map_err(|e| ErrorTaskWrite::ErrorInputOutput(e))?;
                    },
                    MessagePayload::Verack(verack) => {
                        part_socket_write.write_all(&ready_header_message.serialize_header_message()).await.map_err(|e| ErrorTaskWrite::ErrorInputOutput(e))?;
                        part_socket_write.write_all(&[verack.serialize_verack_message()]).await.map_err(|e| ErrorTaskWrite::ErrorInputOutput(e))?;
                    }
                }
            },
            Some(EventsPeer::Shutdown) => {
                return Ok(SuccessTaskWrite::Successful);
            }
            Some(_) => (),
            None => {
                return Err(ErrorTaskWrite::ErrorChannelPeerClosed);
            }
        }
    }
}

pub fn create_header_message(payload: &MessagePayload) -> HeaderMessage {
    let header_message = {
        match &payload {
            MessagePayload::Version(version) => {
                let length = version.length_version_message();
                let checksum = checksum_calculate(&version.serialize_version_message());

                HeaderMessage::create_header_message(BYTES_VERSION, length, checksum)
            },
            MessagePayload::Verack(verack) => {
                let length = verack.length_verack_message();
                let checksum = checksum_calculate(&[verack.serialize_verack_message()]);

                HeaderMessage::create_header_message(BYTES_VERACK, length, checksum)
            }
        }
    };

    header_message
}