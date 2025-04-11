/*
  @license
	Rollup.js v4.39.0
	Wed, 02 Apr 2025 04:49:00 GMT - commit 5c001245779063abac3899aa9d25294ab003581b

	https://github.com/rollup/rollup

	Released under the MIT License.
*/
export { version as VERSION, defineConfig, rollup, watch } from './shared/node-entry.js';
import './shared/parseAst.js';
import '../native.js';
import 'node:path';
import 'path';
import 'node:process';
import 'node:perf_hooks';
import 'node:fs/promises';
