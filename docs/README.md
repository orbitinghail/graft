# Graft Documentation

This directory contains documentation for the Graft project.

## Documentation Structure

- `design.md`: Detailed design document for Graft
- `sqlite.md`: Documentation for the Graft SQLite extension (libgraft)
- `site/`: Source code for the Graft documentation website (graft.rs)

## Documentation Website

The documentation website is built using [Starlight](https://starlight.astro.build/), a documentation theme for [Astro](https://astro.build/).

### Running the Documentation Website Locally

To run the documentation website locally:

1. Navigate to the `site` directory:
   ```bash
   cd site
   ```

2. Install dependencies:
   ```bash
   npm install
   ```

3. Start the development server:
   ```bash
   npm run dev
   ```

4. Open your browser and navigate to http://localhost:4321

### Building for Production

To build the site for production:

```bash
cd site
npm run build
```

The built site will be in the `site/dist` directory.