use std::{io::Write, path::Path};

use bytes::Bytes;
use tokio::{
    io::{self},
    task::spawn_blocking,
};

pub async fn write_file_atomic<P>(path: P, data: &Bytes) -> io::Result<()>
where
    P: AsRef<Path>,
{
    let path = path.as_ref().to_path_buf();
    assert!(path.is_absolute(), "path must be absolute");

    let data = data.clone();

    spawn_blocking(move || {
        // open a named temporary file
        let mut file = tempfile::NamedTempFile::new()?;

        // write and flush the file to disk
        file.write_all(data.as_ref())?;
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
    use super::*;
    use std::fs;

    #[tokio::test]
    async fn test_write_file_atomic() {
        let tempdir = tempfile::tempdir().unwrap();
        let path = tempdir.path().join("test");
        let data = Bytes::from_static(b"hello world!");

        write_file_atomic(&path, &data).await.unwrap();

        let read_data = fs::read(path).unwrap();
        assert_eq!(data, read_data.as_slice());
    }
}
