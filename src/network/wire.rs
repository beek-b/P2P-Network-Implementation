use sha2::{Sha256, Digest};

pub const MAGIC_BYTES: u32 = 0xf9beb4d9;

pub const BYTES_VERSION: [u8; 12] = *b"version\0\0\0\0\0";
pub const BYTES_VERACK: [u8; 12] = *b"verack\0\0\0\0\0\0";

use crate::network::error_network::{ErrorLowLevelParsing};

pub struct HeaderMessage {
    pub magic_bytes: u32, // 4 bytes
    pub command: [u8; 12], // 12 bytes
    pub length: u32, // 4 bytes.   } 24 bytes
    pub checksum: [u8; 4], // 4 bytes
}

impl HeaderMessage {
    pub fn serialize_header_message(&self) -> Vec<u8> {
        let mut vec_bytes = Vec::<u8>::new();

        vec_bytes.extend_from_slice(&self.magic_bytes.to_le_bytes());
        vec_bytes.extend_from_slice(&self.command);
        vec_bytes.extend_from_slice(&self.length.to_le_bytes());
        vec_bytes.extend_from_slice(&self.checksum);

        vec_bytes
    }

    pub fn create_header_message(command: [u8; 12], length: u32, checksum: [u8; 4]) -> Self {
        HeaderMessage { magic_bytes: MAGIC_BYTES, command: command, length: length, checksum: checksum }
    }
}

#[derive(Debug)]
pub struct VersionMessage {
    version: u32,
    services: u64,
    timestamp: u64,
    net_addr_to: NetAddr,
    net_addr_from: NetAddr,
    nonce: u64,
    user_agent: UserAgent,
    start_height: i32,
    relay: bool,
}

impl VersionMessage {
    pub fn serialize_version_message(&self) -> Vec<u8> {
        let mut vec_bytes: Vec<u8> = Vec::<u8>::new();

        vec_bytes.extend_from_slice(&self.version.to_le_bytes());
        vec_bytes.extend_from_slice(&self.services.to_le_bytes());
        vec_bytes.extend_from_slice(&self.timestamp.to_le_bytes());
        vec_bytes.extend_from_slice(&self.net_addr_to.serialize_netaddr());
        vec_bytes.extend_from_slice(&self.net_addr_from.serialize_netaddr());
        vec_bytes.extend_from_slice(&self.nonce.to_le_bytes());
        vec_bytes.extend_from_slice(&self.user_agent.serialize_useragent());
        vec_bytes.extend_from_slice(&self.start_height.to_le_bytes());
        vec_bytes.extend_from_slice(&[self.relay as u8]);

        vec_bytes
    }

    pub fn unserialize_version_message(bytes: &[u8]) -> Result<(Self, usize), ErrorLowLevelParsing> {
        let mut offsit = 0;

        let bytes_version: [u8; 4] = bytes[offsit..offsit + 4].try_into().map_err(|e| ErrorLowLevelParsing::ConversionError(e))?;
        let version = u32::from_le_bytes(bytes_version);
        offsit += 4;

        let bytes_services: [u8; 8] = bytes[offsit..offsit + 8].try_into().map_err(|e| ErrorLowLevelParsing::ConversionError(e))?;
        let services = u64::from_le_bytes(bytes_services);
        offsit += 8;

        let bytes_timestamp: [u8; 8] = bytes[offsit..offsit + 8].try_into().map_err(|e| ErrorLowLevelParsing::ConversionError(e))?;
        let timestamp = u64::from_le_bytes(bytes_timestamp);
        offsit += 8;

        let net_addr_to = NetAddr::unserialize_netaddr(&bytes[offsit..offsit + 26])?;
        offsit += 26; 

        let net_addr_from = NetAddr::unserialize_netaddr(&bytes[offsit..offsit + 26])?;
        offsit += 26;

        let bytes_nonce: [u8; 8] = bytes[offsit..offsit + 8].try_into().map_err(|e| ErrorLowLevelParsing::ConversionError(e))?;
        let nonce = u64::from_le_bytes(bytes_nonce);
        offsit += 8;

        let (user_agent, length_useragent) = UserAgent::unserialize_useragent(&bytes[offsit..])?;
        offsit += length_useragent;

        let bytes_start_height: [u8; 4] = bytes[offsit..offsit + 4].try_into().map_err(|e| ErrorLowLevelParsing::ConversionError(e))?;
        let start_height = i32::from_le_bytes(bytes_start_height);
        offsit += 4;

        let relay = match bytes.get(offsit).ok_or(ErrorLowLevelParsing::NotEnoughBytes)? {
            0x00 => false,
            _=> true,
        };
        offsit += 1;

        Ok((
            Self { version: version, services: services, timestamp: timestamp, net_addr_to: net_addr_to, net_addr_from: net_addr_from, nonce: nonce, user_agent: user_agent, start_height: start_height, relay: relay },
            offsit
        ))
    }

    pub fn length_version_message(&self) -> u32 {
        self.serialize_version_message().len() as u32
    }

    //pub fn build_version_message()

    pub fn validation_version_message(&self) -> bool {
        if self.version < 70000 { return false; }
        if self.relay != true { return false; }

        true
    }
}

#[derive(Debug)]
pub struct NetAddr {
    pub services: u64, // 8 bytes
    pub ip: [u8; 16], // 16 bytes
    pub port: u16, // 2 bytes
}

impl NetAddr {
    pub fn serialize_netaddr(&self) -> Vec<u8> {
        let mut vec_bytes: Vec<u8> = Vec::<u8>::new();

        vec_bytes.extend_from_slice(&self.services.to_le_bytes());
        vec_bytes.extend_from_slice(&self.ip);
        vec_bytes.extend_from_slice(&self.port.to_be_bytes());

        vec_bytes
    }

    pub fn unserialize_netaddr(bytes: &[u8]) -> Result<Self, ErrorLowLevelParsing> {
        let bytes_services: [u8; 8] = bytes[0..8].try_into().map_err(|e| ErrorLowLevelParsing::ConversionError(e))?;
        let services = u64::from_le_bytes(bytes_services);

        let ip: [u8; 16] = bytes[8..24].try_into().map_err(|e| ErrorLowLevelParsing::ConversionError(e))?;

        let bytes_port: [u8; 2] = bytes[24..26].try_into().map_err(|e| ErrorLowLevelParsing::ConversionError(e))?;
        let port = u16::from_be_bytes(bytes_port);
        
        Ok(Self { services: services, ip: ip, port: port })
    }
}

#[derive(Debug)]
pub struct UserAgent {
    pub data: String,
}

impl UserAgent {
    pub fn serialize_useragent(&self) -> Vec<u8> {
        let mut vec_bytes: Vec<u8> = Vec::<u8>::new();

        let lenght = self.data.len() as u64;
        
        vec_bytes.extend_from_slice(&lenght.to_le_bytes());
        vec_bytes.extend_from_slice(&self.data.as_bytes());

        vec_bytes
    }

    pub fn unserialize_useragent(bytes: &[u8]) -> Result<(Self, usize), ErrorLowLevelParsing> {
        let mut offsit = 0;

        let bytes_length: [u8; 8] = bytes[offsit..offsit + 8].try_into().map_err(|e| ErrorLowLevelParsing::ConversionError(e))?;
        let length = u64::from_le_bytes(bytes_length) as usize;
        offsit += 8;

        let bytes_data: &[u8] = &bytes[offsit..offsit + length];
        let string_useragent = String::from_utf8(bytes_data.to_vec()).map_err(|e| ErrorLowLevelParsing::ErrorParseUtf8(e))?;
        offsit += length;
        
        Ok((
            Self { data: string_useragent },
            offsit
        ))
    }
}

pub fn checksum_calculate(raw_bytes: &[u8]) -> [u8; 4] {
    let mut bytes_checksum = [0u8; 4];

    let single_hash256 = Sha256::digest(raw_bytes);
    let double_hash256 = Sha256::digest(single_hash256);

    bytes_checksum.copy_from_slice(&double_hash256[0..4]);
    bytes_checksum
}

#[derive(Debug)]
pub struct VerackMessage;

impl VerackMessage {
    pub fn serialize_verack_message(&self) -> u8 {
        0
    }

    pub fn unserialize_verack_message() -> (Self, usize) {
        (Self, 0)
    }

    pub fn length_verack_message(&self) -> u32 {
        0
    }
}