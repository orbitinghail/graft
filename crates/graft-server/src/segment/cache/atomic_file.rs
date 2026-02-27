use std::{
    io::{IoSlice, Write},
    path::Path,
};

use bytes::Buf;
use tokio::{
    io::{self},
    task::spawn_blocking,
};

pub async fn write_file_atomic<P, T: Buf + Send + 'static>(path: P, mut data: T) -> io::Result<()>
where
    P: AsRef<Path>,
{
    let path = path.as_ref().to_path_buf();
    assert!(path.is_absolute(), "path must be absolute");

    spawn_blocking(move || {
        // open a named temporary file
        let mut file = tempfile::NamedTempFile::new()?;

        while data.has_remaining() {
            let mut buf = [IoSlice::new(&[]); 64];
            let chunks = data.chunks_vectored(&mut buf);
            let written = file.write_vectored(&buf[..chunks])?;
            data.advance(written);
        }

        // flush the file to disk
        file.flush()?;

        // persist the file to disk
        file.persist_noclobber(path)?;

        Ok(())
    })
    .await
    .unwrap()
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use crate::bytes_vec::BytesVec;

    use super::*;
    use std::fs;

    #[graft_test::test]
    async fn test_write_file_atomic() {
        let tempdir = tempfile::tempdir().unwrap();
        let path = tempdir.path().join("test");
        let mut data = BytesVec::with_capacity(128);
        for i in 0..128 {
            data.put(Bytes::from(vec![i as u8; 1024]));
        }

        write_file_atomic(&path, data.clone()).await.unwrap();

        let read_data = fs::read(path).unwrap();
        assert_eq!(data.into_bytes(), read_data.as_slice());
    }
}
