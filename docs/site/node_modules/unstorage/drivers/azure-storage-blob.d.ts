import { ContainerClient } from "@azure/storage-blob";
export interface AzureStorageBlobOptions {
    /**
     * The name of the Azure Storage account.
     */
    accountName: string;
    /**
     * The name of the storage container. All entities will be stored in the same container.
     * @default "unstorage"
     */
    containerName?: string;
    /**
     * The account key. If provided, the SAS key will be ignored. Only available in Node.js runtime.
     */
    accountKey?: string;
    /**
     * The SAS key. If provided, the account key will be ignored.
     */
    sasKey?: string;
    /**
     * The connection string. If provided, the account key and SAS key will be ignored. Only available in Node.js runtime.
     */
    connectionString?: string;
}
declare const _default: (opts: AzureStorageBlobOptions) => import("..").Driver<AzureStorageBlobOptions, ContainerClient>;
export default _default;
