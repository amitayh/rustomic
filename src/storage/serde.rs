#![allow(dead_code)]

use thiserror::Error;

pub struct Writer {
    buffer: Vec<u8>,
}

impl Writer {
    pub fn new(capacity: usize) -> Self {
        Writer {
            buffer: Vec::with_capacity(capacity),
        }
    }

    pub fn write_u8(&mut self, value: u8) {
        self.buffer.push(value);
    }

    pub fn write_u16(&mut self, value: u16) {
        self.buffer.extend_from_slice(&value.to_be_bytes());
    }

    pub fn write_u64(&mut self, value: u64) {
        self.buffer.extend_from_slice(&value.to_be_bytes());
    }

    pub fn write_i64(&mut self, value: i64) {
        self.buffer.extend_from_slice(&value.to_be_bytes());
    }

    pub fn write_str(&mut self, value: &str) {
        match u16::try_from(value.len()) {
            Ok(length) => {
                self.write_u16(length);
                self.buffer.extend_from_slice(value.as_bytes());
            }
            _ => (),
        }
    }

    pub fn result(self) -> Buffer {
        Buffer(self.buffer)
    }
}

pub struct Buffer(Vec<u8>);

impl AsRef<[u8]> for Buffer {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

// -------------------------------------------------------------------------------------------------

pub struct Reader<'a> {
    buffer: &'a [u8],
    index: usize,
}

impl<'a> Reader<'a> {
    pub fn new(buffer: &'a [u8]) -> Self {
        Reader { buffer, index: 0 }
    }

    pub fn read_u8(&mut self) -> Result<u8, ReadError> {
        let buffer = self.read_next(1)?;
        Ok(buffer[0])
    }

    pub fn read_u16(&mut self) -> Result<u16, ReadError> {
        let buffer = self.read_next(2)?;
        Ok(u16::from_be_bytes([buffer[0], buffer[1]]))
    }

    pub fn read_u64(&mut self) -> Result<u64, ReadError> {
        let buffer = self.read_next(8)?;
        Ok(u64::from_be_bytes([
            buffer[0], buffer[1], buffer[2], buffer[3], buffer[4], buffer[5], buffer[6], buffer[7],
        ]))
    }

    pub fn read_i64(&mut self) -> Result<i64, ReadError> {
        let buffer = self.read_next(8)?;
        Ok(i64::from_be_bytes([
            buffer[0], buffer[1], buffer[2], buffer[3], buffer[4], buffer[5], buffer[6], buffer[7],
        ]))
    }

    pub fn read_str(&mut self) -> Result<String, ReadError> {
        let length = self.read_u16()?;
        let buffer = self.read_next(length.into())?;
        let str = String::from_utf8_lossy(buffer);
        Ok(str.into_owned())
    }

    fn read_next(&mut self, num_bytes: usize) -> Result<&[u8], ReadError> {
        if self.index + num_bytes > self.buffer.len() {
            return Err(ReadError::EndOfInput);
        }
        let prev_index = self.index;
        self.index += num_bytes;
        Ok(&self.buffer[prev_index..])
    }
}

#[derive(Debug, Error)]
pub enum ReadError {
    #[error("end of input")]
    EndOfInput,
}
