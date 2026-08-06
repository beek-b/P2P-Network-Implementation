use tokio::{net::tcp::OwnedReadHalf, sync::mpsc, io::{AsyncBufRead, AsyncReadExt}};

use crate::network::events_network::{EventsPeer, EventsTaskRead};
use crate::network::success_network::{SuccessTaskRead};
use crate::network::error_network::{ErrorTaskRead, ErrorParsing};
use crate::network::wire::{MAGIC_BYTES, BYTES_VERSION, BYTES_VERACK};

use crate::network::ttype::{TypeCommandMessage,  MessagePayload};

use crate::network::wire::{VersionMessage, VerackMessage, checksum_calculate};

pub const LEN_HEADER_MESSAGE: usize = 24;
pub const MAX_SIZE_PAYLOAD: usize = 4 << 20;

pub async fn start_task_read(mut part_socket_read: OwnedReadHalf, mut rx_channel_from_peer: mpsc::Receiver<EventsPeer>, tx_channel_to_peer: mpsc::Sender<EventsTaskRead>) -> Result<SuccessTaskRead, ErrorTaskRead> {
    let mut storage_buffer = Vec::<u8>::new();

    let mut temporary_buffer: [u8; 4096] = [0u8; 4096];

    loop {
        tokio::select! {
            result_read = part_socket_read.read(&mut temporary_buffer) => {
                if let Err(e) = result_read { return Err(ErrorTaskRead::ErrorInputOutput(e)); }
                if let Ok(0) = result_read { return Ok(SuccessTaskRead::SuccessfulEOF); }

                if let Ok(n) = result_read {
                    storage_buffer.extend_from_slice(&temporary_buffer[..n]);

                    loop {
                        match raw_byte_processing(&storage_buffer) {
                            Ok(Some(message)) => {
                                if tx_channel_to_peer.send(EventsTaskRead::LowRawMessage(message)).await.is_err() {
                                    return Err(ErrorTaskRead::ErrorChannelPeerClosed);
                                }
                            },
                            Ok(None) => {
                                break;
                            },
                            Err(error_parse) => {
                                return Err(ErrorTaskRead::ErrorParsing(error_parse));
                            }
                        }
                    }
                }
            },
            incident_peer = rx_channel_from_peer.recv() => {
                match incident_peer {
                    Some(EventsPeer::Shutdown) => {
                        return Ok(SuccessTaskRead::Successful);
                    },
                    Some(_) => (),
                    None => {
                        return Err(ErrorTaskRead::ErrorChannelPeerClosed);
                    }
                }
            }
        }
    }
}

fn raw_byte_processing(storage_buffer: &Vec<u8>) -> Result<Option<MessagePayload>, ErrorParsing> {
    if storage_buffer.len() < 0 { return Ok(None); }
    let mut offsit = 0;

    let bytes_magic_bytes: [u8; 4] = storage_buffer[offsit..offsit + 4].try_into().map_err(|e| ErrorParsing::ConversionError(e))?;
    let magic_bytes = u32::from_le_bytes(bytes_magic_bytes);
    offsit += 4;

    if magic_bytes != MAGIC_BYTES { return Err(ErrorParsing::InvalidMagicBytes); }

    let command: [u8; 12] = storage_buffer[offsit..offsit + 12].try_into().map_err(|e| ErrorParsing::ConversionError(e))?;
    offsit += 12;

    match message_type_definition(command) {
        TypeCommandMessage::VersionMessage => {
            let bytes_length: [u8; 4] = storage_buffer[offsit..offsit + 4].try_into().map_err(|e| ErrorParsing::ConversionError(e))?;
            let length = u32::from_le_bytes(bytes_length);
            offsit += 4;

            let checksum: [u8; 4] = storage_buffer[offsit..offsit + 4].try_into().map_err(|e| ErrorParsing::ConversionError(e))?;
            offsit += 4;

            if storage_buffer.len() < LEN_HEADER_MESSAGE + length as usize { return Ok(None); }

            let (version_message, length_message) = VersionMessage::unserialize_version_message(&storage_buffer[offsit..]).map_err(|e| ErrorParsing::ErrorLowLevelParsing(e))?;
            if length_message > MAX_SIZE_PAYLOAD { return Err(ErrorParsing::LargePayloadSize); }

            if checksum != checksum_calculate(&version_message.serialize_version_message()) { return Err(ErrorParsing::InvalidCheckSum); }

            Ok(Some(MessagePayload::Version(version_message)))
        },
        TypeCommandMessage::VerackMessage => {
            let bytes_length: [u8; 4] = storage_buffer[offsit..offsit + 4].try_into().map_err(|e| ErrorParsing::ConversionError(e))?;
            let length = u32::from_le_bytes(bytes_length);
            offsit += 4;

            let checksum: [u8; 4] = storage_buffer[offsit..offsit + 4].try_into().map_err(|e| ErrorParsing::ConversionError(e))?;
            offsit += 4;

            if storage_buffer.len() < LEN_HEADER_MESSAGE + 0 { return Ok(None); }

            let (verack_message, length_message) = VerackMessage::unserialize_verack_message();
            if length_message > MAX_SIZE_PAYLOAD { return Err(ErrorParsing::LargePayloadSize); }

            if checksum != checksum_calculate(&[verack_message.serialize_verack_message()]) { return Err(ErrorParsing::InvalidCheckSum); }

            Ok(Some(MessagePayload::Verack(verack_message)))
        },
        TypeCommandMessage::UncertainMessage => {
            Err(ErrorParsing::InvalidCommand)
        }
    }
}

fn message_type_definition(command: [u8; 12]) -> TypeCommandMessage {
    if command == BYTES_VERSION {
        TypeCommandMessage::VersionMessage
    } else if command == BYTES_VERACK {
        TypeCommandMessage::VerackMessage
    } else {
        TypeCommandMessage::UncertainMessage
    }
}