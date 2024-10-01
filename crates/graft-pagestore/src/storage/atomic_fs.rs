use std::{
    os::{
        fd::{AsFd, AsRawFd},
        unix::fs::MetadataExt,
    },
    path::{Path, PathBuf},
    pin::Pin,
    task::{Context, Poll},
};

use graft_core::byte_unit::ByteUnit;
use nix::fcntl::{AtFlags, OFlag};
use pin_project::pin_project;
use tokio::{
    fs::OpenOptions,
    io::{self, AsyncSeek, AsyncWrite, AsyncWriteExt},
};

pub async fn write_file_atomic<P, B>(path: P, data: B) -> io::Result<ByteUnit>
where
    P: AsRef<Path>,
    B: AsRef<[u8]>,
{
    let mut writer = AtomicFileWriter::open(path).await?;
    writer.write_all(data.as_ref()).await?;
    writer.commit().await
}

#[pin_project]
pub struct AtomicFileWriter {
    path: PathBuf,
    #[pin]
    file: tokio::fs::File,
}

impl AtomicFileWriter {
    pub async fn open<P: AsRef<Path>>(path: P) -> io::Result<Self> {
        let path = path.as_ref().to_path_buf();
        assert!(path.is_absolute(), "path must be absolute");

        // resolve the path to its directory
        let dir = path
            .parent()
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "path has no parent"))?;

        // open a temporary file in the target directory
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .custom_flags(OFlag::O_TMPFILE.bits())
            .open(dir)
            .await?;

        Ok(Self { path, file })
    }

    pub async fn commit(self) -> io::Result<ByteUnit> {
        let Self { path, file } = self;

        // flush the file
        file.sync_all().await?;

        let size = file.metadata().await?.size();

        // use linkat to map the file to its final location
        let fd = file.as_fd().as_raw_fd();
        nix::unistd::linkat(Some(fd), Path::new(""), None, &path, AtFlags::AT_EMPTY_PATH)?;

        Ok(size.into())
    }
}

impl AsyncSeek for AtomicFileWriter {
    fn start_seek(self: Pin<&mut Self>, position: std::io::SeekFrom) -> std::io::Result<()> {
        self.project().file.start_seek(position)
    }

    fn poll_complete(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<u64>> {
        self.project().file.poll_complete(cx)
    }
}

impl AsyncWrite for AtomicFileWriter {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.project().file.poll_write(cx, buf)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.project().file.poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.project().file.poll_shutdown(cx)
    }
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
