import type { DocSearchClientOptions } from "@astrojs/starlight-docsearch";

export default {
  appId: "GS869RQCPN",
  apiKey: "d6afaeb4da018efde82718b6bf1abda7",
  indexName: "graft",
  insights: true,
  getMissingResultsUrl({ query }) {
    return `https://github.com/orbitinghail/graft/issues/new?title=${query}&labels=documentation`;
  },
} satisfies DocSearchClientOptions;
