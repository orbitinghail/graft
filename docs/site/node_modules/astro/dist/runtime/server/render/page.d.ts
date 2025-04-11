import { type NonAstroPageComponent } from './component.js';
import type { AstroComponentFactory } from './index.js';
import type { RouteData, SSRResult } from '../../../types/public/internal.js';
export declare function renderPage(result: SSRResult, componentFactory: AstroComponentFactory | NonAstroPageComponent, props: any, children: any, streaming: boolean, route?: RouteData): Promise<Response>;
