import { defineConfig } from 'astro/config';
import starlight from '@astrojs/starlight';

export default defineConfig({
  site: 'https://graft.rs',
  integrations: [
    starlight({
      title: 'Graft',
      social: [
        { label: 'GitHub', icon: 'github', href: 'https://github.com/orbitinghail/graft' },
      ],
      sidebar: [
        {
          label: 'Getting Started',
          items: [
            { label: 'Introduction', link: '/' },
            { label: 'SQLite Extension', link: '/sqlite/' },
          ],
        },
        {
          label: 'Design',
          items: [
            { label: 'Design Document', link: '/design/' },
          ],
        },
      ],
    }),
  ],
});