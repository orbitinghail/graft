let cachedRenderer: ReturnType<typeof createRenderer> | undefined = undefined

export async function getRenderer() {
	if (!cachedRenderer) {
		cachedRenderer = createRenderer()
	}
	return await cachedRenderer
}

async function createRenderer() {
	const { astroConfig, ecConfigFileOptions, ecIntegrationOptions = {} } = await import('virtual:astro-expressive-code/config')
	const { createAstroRenderer, mergeEcConfigOptions } = await import('virtual:astro-expressive-code/api')

	const strIntegrationOptions = JSON.stringify(ecIntegrationOptions)
	if (strIntegrationOptions.includes('"[Function]"') || strIntegrationOptions.includes("'[Circular]'")) {
		throw new Error(
			`Your Astro config file contains Expressive Code options that are not serializable to JSON.
			To use the \`<Code>\` component, please create a separate config file called \`ec.config.mjs\`
			in your project root, move your Expressive Code options object into the config file,
			and export it as the default export.`.replace(/\s+/g, ' ')
		)
	}

	let mergedEcConfig = mergeEcConfigOptions(ecIntegrationOptions, ecConfigFileOptions)
	try {
		const { default: preprocessEcConfig } = await import('virtual:astro-expressive-code/preprocess-config')
		mergedEcConfig = (await preprocessEcConfig({ ecConfig: mergedEcConfig, astroConfig })) || mergedEcConfig
	} catch (error) {
		const msg = error instanceof Error ? error.message : (error as string)
		throw new Error(`Failed to preprocess Expressive Code config for the Code component: ${msg}`, { cause: error })
	}

	const astroRenderer = await createAstroRenderer({
		astroConfig,
		ecConfig: mergedEcConfig,
	})
	return {
		...astroRenderer,
		// Also return any config options that are normally processed by `rehype-expressive-code`
		// and need to be processed by `astro-expressive-code` when using the `Code` component
		tabWidth: mergedEcConfig.tabWidth,
		getBlockLocale: mergedEcConfig.getBlockLocale,
	}
}
