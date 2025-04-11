# Graft Documentation Site

This is the source code for the Graft documentation site, built with [Starlight](https://starlight.astro.build/), a documentation theme for [Astro](https://astro.build/).

## Running Locally

To run the documentation site locally:

1. Make sure you have [Node.js](https://nodejs.org/) installed (version 18 or later)
2. Install dependencies:
   ```bash
   npm install
   ```
3. Start the development server:
   ```bash
   npm run dev
   ```
4. Open your browser and navigate to http://localhost:4321

## Building for Production

To build the site for production:

```bash
npm run build
```

The built site will be in the `dist` directory.

## Project Structure

- `src/content/docs/`: Documentation content in Markdown format
- `public/`: Static assets like images
- `astro.config.mjs`: Astro configuration file
- `package.json`: Project dependencies and scripts

## Adding New Pages

To add a new documentation page, create a new Markdown file in the appropriate directory under `src/content/docs/`. Update the sidebar configuration in `astro.config.mjs` to include the new page.