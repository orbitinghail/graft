import { defineCollection, z } from "astro:content";
import { docsLoader } from "@astrojs/starlight/loaders";
import { docsSchema } from "@astrojs/starlight/schema";

export const collections = {
  docs: defineCollection({
    loader: docsLoader(),
    schema: docsSchema({
      extend: z.object({
        rfc: z
          .object({
            id: z.number(),
            slug: z.string(),
            startDate: z.coerce.date(),
            issue: z.number().nullable(),
            pr: z.number().nullable(),
          })
          .optional(),
      }),
    }),
  }),
};
