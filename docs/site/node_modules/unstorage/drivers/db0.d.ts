import type { Database } from "db0";
export interface DB0DriverOptions {
    database: Database;
    tableName?: string;
}
declare const _default: (opts: DB0DriverOptions) => import("..").Driver<DB0DriverOptions, Database<import("db0").Connector<any>>>;
export default _default;
