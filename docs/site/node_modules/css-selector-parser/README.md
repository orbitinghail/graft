css-selector-parser
===================

* Fast and low memory CSS selector parser.
* Parses CSS selector into object-model (AST).
* Compliant with all historical and modern CSS specs.
* Covered with tests.
* Documented.
* Supported CSS selector standards:
    * `css1`: https://www.w3.org/TR/CSS1/
    * `css2`: https://www.w3.org/TR/CSS2/
    * `css3`/`selectors-3`: https://www.w3.org/TR/selectors-3/
    * `selectors-4`: https://www.w3.org/TR/selectors-4/
    * `latest`: refers to `selectors-4`
    * `progressive`: `latest` + accepts unknown psudo-classes, psudo-elements and attribute case sensitivity modifiers

**Important:**
 * [Migrating from 1.x to 3.x](CHANGELOG.md#migrating-from-1x-to-3x).
 * [Migrating from 2.x to 3.x](CHANGELOG.md#migrating-from-2x-to-3x).
 * [Migrating from 1.x to 2.x](CHANGELOG.md#220).

Latest releases: [Changelog](CHANGELOG.md).

Installation
------------

```
npm install css-selector-parser
```

Usage
-----

### Parsing

```javascript
import {createParser} from 'css-selector-parser';

const parse = createParser();
const selector = parse('a[href^="/"], .container:has(nav) > a[href]:nth-child(2)::before');

console.log(selector);
```

Produces:

```javascript
({
    type: 'Selector',
    rules: [
        {
            type: 'Rule',
            items: [
                { type: 'TagName', name: 'a' },
                {
                    type: 'Attribute',
                    name: 'href',
                    operator: '^=',
                    value: { type: 'String', value: '/' }
                }
            ]
        },
        {
            type: 'Rule',
            items: [
                { type: 'ClassName', name: 'container' },
                {
                    type: 'PseudoClass',
                    name: 'has',
                    argument: {
                        type: 'Selector',
                        rules: [
                            {
                                type: 'Rule',
                                items: [ { type: 'TagName', name: 'nav' } ]
                            }
                        ]
                    }
                }
            ],
            nestedRule: {
                type: 'Rule',
                items: [
                    { type: 'TagName', name: 'a' },
                    { type: 'Attribute', name: 'href' },
                    {
                        type: 'PseudoClass',
                        name: 'nth-child',
                        argument: { type: 'Formula', a: 0, b: 2 }
                    },
                    {
                        type: 'PseudoElement',
                        name: 'before'
                    }
                ],
                combinator: '>'
            }
        }
    ]
})
```

### Constructing AST and rendering

```javascript
import {ast, render} from 'css-selector-parser';

const selector = ast.selector({
    rules: [
        ast.rule({
            items: [
                ast.tagName({name: 'a'}),
                ast.attribute({name: 'href', operator: '^=', value: ast.string({value: '/'})})
            ]
        }),
        ast.rule({
            items: [
                ast.className({name: 'container'}),
                ast.pseudoClass({
                    name: 'has',
                    argument: ast.selector({
                        rules: [ast.rule({items: [ast.tagName({name: 'nav'})]})]
                    })
                })
            ],
            nestedRule: ast.rule({
                combinator: '>',
                items: [
                    ast.tagName({name: 'a'}),
                    ast.attribute({name: 'href'}),
                    ast.pseudoClass({
                        name: 'nth-child',
                        argument: ast.formula({a: 0, b: 2})
                    }),
                    ast.pseudoElement({name: 'before'})
                ]
            })
        })
    ]
});

console.log(render(selector)); // a[href^="/"], .container:has(nav) > a[href]:nth-child(2)::before
```

CSS Modules
-----------

CSS Modules are specific CSS specifications that add new selectors or modify existing ones.
The parser supports various CSS modules that can be included in your syntax definition:

```javascript
import {createParser} from 'css-selector-parser';

// Create a parser with specific CSS modules enabled
const parse = createParser({
    syntax: 'selectors-4',
    modules: ['css-position-3', 'css-scoping-1']
});
```

Supported CSS modules:

* `css-position-1`, `css-position-2`, `css-position-3`, `css-position-4`: Adds position-related pseudo-classes
* `css-scoping-1`: Adds Shadow DOM selectors like `:host`, `:host-context()`, and `::slotted()`
* `css-pseudo-4`: Adds modern pseudo-elements like `::selection`, `::backdrop`, etc.
* `css-shadow-parts-1`: Adds `::part()` for styling shadow DOM components

The `latest` syntax automatically includes all modules marked as current specifications.

API
---

* [Full API Documentation](docs/modules.md)
* [Parsing CSS selectors](docs/modules.md#createParser)
* [Constructing CSS AST](docs/modules.md#ast)
* [Rendering CSS AST](docs/modules.md#render)

LICENSE
-------

MIT

## Security contact information

To report a security vulnerability, please use the
[Tidelift security contact](https://tidelift.com/security).
Tidelift will coordinate the fix and disclosure.
