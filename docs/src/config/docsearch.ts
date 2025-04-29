import type { DocSearchClientOptions } from "@astrojs/starlight-docsearch";

const isProd = window.location.hostname === "graft.rs";

export default {
  appId: "GS869RQCPN",
  apiKey: "d6afaeb4da018efde82718b6bf1abda7",
  indexName: "graft",
  insights: true,
  getMissingResultsUrl({ query }) {
    return `https://github.com/orbitinghail/graft/issues/new?title=${query}&labels=documentation`;
  },
  ...(isProd
    ? {}
    : {
        transformItems(items) {
          const previewOrigin = window.location.origin;
          return items.map((item) => ({
            ...item,
            url: item.url.replace(/^https:\/\/graft\.rs/, previewOrigin),
          }));
        },
      }),
} satisfies DocSearchClientOptions;
