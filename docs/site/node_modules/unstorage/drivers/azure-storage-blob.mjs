import { createError, defineDriver } from "./utils/index.mjs";
import {
  BlobServiceClient,
  StorageSharedKeyCredential
} from "@azure/storage-blob";
import { DefaultAzureCredential } from "@azure/identity";
const DRIVER_NAME = "azure-storage-blob";
export default defineDriver((opts) => {
  let containerClient;
  const getContainerClient = () => {
    if (containerClient) {
      return containerClient;
    }
    if (!opts.accountName) {
      throw createError(DRIVER_NAME, "accountName");
    }
    let serviceClient;
    if (opts.accountKey) {
      const credential = new StorageSharedKeyCredential(
        opts.accountName,
        opts.accountKey
      );
      serviceClient = new BlobServiceClient(
        `https://${opts.accountName}.blob.core.windows.net`,
        credential
      );
    } else if (opts.sasKey) {
      serviceClient = new BlobServiceClient(
        `https://${opts.accountName}.blob.core.windows.net${opts.sasKey}`
      );
    } else if (opts.connectionString) {
      serviceClient = BlobServiceClient.fromConnectionString(
        opts.connectionString
      );
    } else {
      const credential = new DefaultAzureCredential();
      serviceClient = new BlobServiceClient(
        `https://${opts.accountName}.blob.core.windows.net`,
        credential
      );
    }
    containerClient = serviceClient.getContainerClient(
      opts.containerName || "unstorage"
    );
    return containerClient;
  };
  return {
    name: DRIVER_NAME,
    options: opts,
    getInstance: getContainerClient,
    async hasItem(key) {
      return await getContainerClient().getBlockBlobClient(key).exists();
    },
    async getItem(key) {
      try {
        const blob = await getContainerClient().getBlockBlobClient(key).download();
        if (isBrowser) {
          return blob.blobBody ? await blobToString(await blob.blobBody) : null;
        }
        return blob.readableStreamBody ? (await streamToBuffer(blob.readableStreamBody)).toString() : null;
      } catch {
        return null;
      }
    },
    async setItem(key, value) {
      await getContainerClient().getBlockBlobClient(key).upload(value, Buffer.byteLength(value));
    },
    async removeItem(key) {
      await getContainerClient().getBlockBlobClient(key).delete();
    },
    async getKeys() {
      const iterator = getContainerClient().listBlobsFlat().byPage({ maxPageSize: 1e3 });
      const keys = [];
      for await (const page of iterator) {
        const pageKeys = page.segment.blobItems.map((blob) => blob.name);
        keys.push(...pageKeys);
      }
      return keys;
    },
    async getMeta(key) {
      const blobProperties = await getContainerClient().getBlockBlobClient(key).getProperties();
      return {
        mtime: blobProperties.lastModified,
        atime: blobProperties.lastAccessed,
        cr: blobProperties.createdOn,
        ...blobProperties.metadata
      };
    },
    async clear() {
      const iterator = getContainerClient().listBlobsFlat().byPage({ maxPageSize: 1e3 });
      for await (const page of iterator) {
        await Promise.all(
          page.segment.blobItems.map(
            async (blob) => await getContainerClient().deleteBlob(blob.name, {
              deleteSnapshots: "include"
            })
          )
        );
      }
    }
  };
});
const isBrowser = typeof window !== "undefined";
async function streamToBuffer(readableStream) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    readableStream.on("data", (data) => {
      chunks.push(data instanceof Buffer ? data : Buffer.from(data));
    });
    readableStream.on("end", () => {
      resolve(Buffer.concat(chunks));
    });
    readableStream.on("error", reject);
  });
}
async function blobToString(blob) {
  const fileReader = new FileReader();
  return new Promise((resolve, reject) => {
    fileReader.onloadend = (ev) => {
      resolve(ev.target?.result);
    };
    fileReader.onerror = reject;
    fileReader.readAsText(blob);
  });
}
