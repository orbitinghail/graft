use std::{
    os::fd::{AsFd, AsRawFd},
    path::Path,
};

use nix::fcntl::{AtFlags, OFlag};
use tokio::{
    fs::OpenOptions,
    io::{self, AsyncWriteExt},
};

pub async fn write_file_atomic<P, B>(path: P, data: B) -> io::Result<()>
where
    P: AsRef<Path>,
    B: AsRef<[u8]>,
{
    let path = path.as_ref();
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

    // write the data to the temporary file
    file.write_all(data.as_ref()).await?;

    // flush the file
    file.sync_all().await?;

    // use linkat to map the file to its final location
    let fd = file.as_fd().as_raw_fd();
    nix::unistd::linkat(Some(fd), Path::new(""), None, path, AtFlags::AT_EMPTY_PATH)?;

    Ok(())
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
}
