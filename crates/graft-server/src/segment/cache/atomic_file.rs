use std::{io::Write, os::fd::AsRawFd, path::Path};

use nix::fcntl::{AtFlags, OFlag};
use tokio::{
    fs::OpenOptions,
    io::{self, AsyncWriteExt},
    task::block_in_place,
};

#[cfg(target_os = "linux")]
pub async fn write_file_atomic<P, B>(path: P, data: B) -> io::Result<()>
where
    P: AsRef<Path>,
    B: AsRef<[u8]>,
{
    let path = path.as_ref().to_path_buf();
    assert!(path.is_absolute(), "path must be absolute");

    // resolve the path to its directory
    let dir = path
        .parent()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "path has no parent"))?;

    // open a temporary file in the target directory
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .custom_flags(OFlag::O_TMPFILE.bits())
        .open(dir)
        .await?;

    // write and flush the file to disk
    file.write_all(data.as_ref()).await?;
    file.sync_all().await?;

    // use linkat to map the file to its final location
    let fd = file.as_raw_fd();
    nix::unistd::linkat(Some(fd), Path::new(""), None, &path, AtFlags::AT_EMPTY_PATH)?;

    Ok(())
}

#[cfg(not(target_os = "linux"))]
pub async fn write_file_atomic<P, B>(path: P, data: B) -> io::Result<()>
where
    P: AsRef<Path>,
    B: AsRef<[u8]>,
{
    write_file_atomic_generic(path, data).await
}

pub async fn write_file_atomic_generic<P, B>(path: P, data: B) -> io::Result<()>
where
    P: AsRef<Path>,
    B: AsRef<[u8]>,
{
    let path = path.as_ref().to_path_buf();
    assert!(path.is_absolute(), "path must be absolute");

    block_in_place(|| {
        // open a named temporary file
        let mut file = tempfile::NamedTempFile::new()?;

        // write and flush the file to disk
        file.write_all(data.as_ref())?;
        file.flush()?;

        // persist the file to disk
        file.persist_noclobber(path)?;

        Ok(())
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[tokio::test]
    async fn test_write_file_atomic() {
        let tempdir = tempfile::tempdir().unwrap();
        let path = tempdir.path().join("test");
        let data = b"hello, world!";

        write_file_atomic(&path, data).await.unwrap();

        let read_data = fs::read(path).unwrap();
        assert_eq!(data, read_data.as_slice());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_write_file_atomic_generic() {
        let tempdir = tempfile::tempdir().unwrap();
        let path = tempdir.path().join("test");
        let data = b"hello, world!";

        write_file_atomic_generic(&path, data).await.unwrap();

        let read_data = fs::read(path).unwrap();
        assert_eq!(data, read_data.as_slice());
    }
}
