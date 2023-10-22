use std::io::{Cursor, self};

use bytes::{BytesMut, Buf};
use mini_redis::{Result, Frame, frame::Error};
use tokio::{net::TcpStream, io::{AsyncReadExt, BufWriter, AsyncWriteExt}};

// enum Frame {
//     Simple(String),
//     Error(String),
//     Integer(u64),
//     Bulk(Bytes),
//     Null,
//     Array(Vec<Frame>),
// }

struct Connection {
    stream: BufWriter<TcpStream>, // write data into buffer, and flush the target writer when the buffer is full
    buffer: BytesMut, // includes automatic buffer cursor management (automatically resize the buffer if it has full of data)
}

impl Connection {
    pub fn new(stream: TcpStream) -> Connection {
        Connection {
            // create in BufWriter
            stream: BufWriter::new(stream),
            // allocate 4KB capacity buffer
            buffer: BytesMut::with_capacity(4096),
        }
    }

    pub async fn read_frame(&mut self) -> Result<Option<Frame>> {
        loop {
            // parse buffered data as frame
            if let Some(frame) = self.parse_frame()? {
                return Ok(Some(frame));
            }
            // read data from stream to buffer & check the data size is enough or not
            if 0 == self.stream.read_buf(&mut self.buffer).await? {
                if self.buffer.is_empty() {
                    return Ok(None); // EOF
                }
                // if partial data remains in buffer
                return Err("connection reset by peer".into());
            }
        }
    }

    fn parse_frame(&mut self) -> Result<Option<Frame>> {
        let mut buf = Cursor::new(&self.buffer[..]); // buffer[(present_index)::(end_of_index)]

        match Frame::check(&mut buf) {
            Ok(_) => {
                let len = buf.position() as usize; // end of the frame size
                buf.set_position(0);
                // parse the first data to frame
                let frame = Frame::parse(&mut buf)?;
                self.buffer.advance(len); // the position of the next data
                Ok(Some(frame))
            }
            Err(Error::Incomplete) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    pub async fn write_frame(&mut self, frame: &Frame) -> Result<()> {
        match frame {
            Frame::Simple(val) => {
                self.stream.write_u8(b'+').await?;
                self.stream.write_all(val.as_bytes()).await?;
                self.stream.write_all(b"\r\n").await?;
            }
            Frame::Error(val) => {
                self.stream.write_u8(b'-').await?;
                self.stream.write_all(val.as_bytes()).await?;
                self.stream.write_all(b"\r\n").await?;
            }
            Frame::Integer(val) => {
                self.stream.write_u8(b':').await?;
                self.write_decimal(*val).await?;
            }
            Frame::Null => {
                self.stream.write_all(b"$-1\r\n").await?;
            }
            Frame::Bulk(val) => {
                let len = val.len();

                self.stream.write_u8(b'$').await?;
                self.write_decimal(len as u64).await?;
                self.stream.write_all(val).await?;
                self.stream.write_all(b"\r\n").await?;
            }
            Frame::Array(_val) => unimplemented!(),
        }

        // send all of the data which was written in buffer
        self.stream.flush().await?;

        Ok(())
    }

    /// write a decimal frame to the stream
    async fn write_decimal(&mut self, val: u64) -> io::Result<()> {
        use std::io::Write;

        // convert the value to a string
        let mut buf = [0u8; 12];
        let mut buf = Cursor::new(&mut buf[..]);
        write!(&mut buf, "{}", val)?;

        let pos = buf.position() as usize;
        self.stream.write_all(&buf.get_ref()[..pos]).await?;
        self.stream.write_all(b"\r\n").await?;

        Ok(())
    }
}
