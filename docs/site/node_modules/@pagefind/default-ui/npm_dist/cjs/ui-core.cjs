var __defProp = Object.defineProperty;
var __getOwnPropDesc = Object.getOwnPropertyDescriptor;
var __getOwnPropNames = Object.getOwnPropertyNames;
var __hasOwnProp = Object.prototype.hasOwnProperty;
var __export = (target, all) => {
  for (var name in all)
    __defProp(target, name, { get: all[name], enumerable: true });
};
var __copyProps = (to, from, except, desc) => {
  if (from && typeof from === "object" || typeof from === "function") {
    for (let key of __getOwnPropNames(from))
      if (!__hasOwnProp.call(to, key) && key !== except)
        __defProp(to, key, { get: () => from[key], enumerable: !(desc = __getOwnPropDesc(from, key)) || desc.enumerable });
  }
  return to;
};
var __toCommonJS = (mod) => __copyProps(__defProp({}, "__esModule", { value: true }), mod);

// ui-core.js
var ui_core_exports = {};
__export(ui_core_exports, {
  PagefindUI: () => PagefindUI
});
module.exports = __toCommonJS(ui_core_exports);

// node_modules/svelte/internal/index.mjs
function noop() {
}
function run(fn) {
  return fn();
}
function blank_object() {
  return /* @__PURE__ */ Object.create(null);
}
function run_all(fns) {
  fns.forEach(run);
}
function is_function(thing) {
  return typeof thing === "function";
}
function safe_not_equal(a, b) {
  return a != a ? b == b : a !== b || (a && typeof a === "object" || typeof a === "function");
}
var src_url_equal_anchor;
function src_url_equal(element_src, url) {
  if (!src_url_equal_anchor) {
    src_url_equal_anchor = document.createElement("a");
  }
  src_url_equal_anchor.href = url;
  return element_src === src_url_equal_anchor.href;
}
function is_empty(obj) {
  return Object.keys(obj).length === 0;
}
var globals = typeof window !== "undefined" ? window : typeof globalThis !== "undefined" ? globalThis : global;
var ResizeObserverSingleton = class {
  constructor(options) {
    this.options = options;
    this._listeners = "WeakMap" in globals ? /* @__PURE__ */ new WeakMap() : void 0;
  }
  observe(element2, listener) {
    this._listeners.set(element2, listener);
    this._getObserver().observe(element2, this.options);
    return () => {
      this._listeners.delete(element2);
      this._observer.unobserve(element2);
    };
  }
  _getObserver() {
    var _a;
    return (_a = this._observer) !== null && _a !== void 0 ? _a : this._observer = new ResizeObserver((entries) => {
      var _a2;
      for (const entry of entries) {
        ResizeObserverSingleton.entries.set(entry.target, entry);
        (_a2 = this._listeners.get(entry.target)) === null || _a2 === void 0 ? void 0 : _a2(entry);
      }
    });
  }
};
ResizeObserverSingleton.entries = "WeakMap" in globals ? /* @__PURE__ */ new WeakMap() : void 0;
var is_hydrating = false;
function start_hydrating() {
  is_hydrating = true;
}
function end_hydrating() {
  is_hydrating = false;
}
function append(target, node) {
  target.appendChild(node);
}
function insert(target, node, anchor) {
  target.insertBefore(node, anchor || null);
}
function detach(node) {
  if (node.parentNode) {
    node.parentNode.removeChild(node);
  }
}
function destroy_each(iterations, detaching) {
  for (let i = 0; i < iterations.length; i += 1) {
    if (iterations[i])
      iterations[i].d(detaching);
  }
}
function element(name) {
  return document.createElement(name);
}
function svg_element(name) {
  return document.createElementNS("http://www.w3.org/2000/svg", name);
}
function text(data) {
  return document.createTextNode(data);
}
function space() {
  return text(" ");
}
function empty() {
  return text("");
}
function listen(node, event, handler, options) {
  node.addEventListener(event, handler, options);
  return () => node.removeEventListener(event, handler, options);
}
function attr(node, attribute, value) {
  if (value == null)
    node.removeAttribute(attribute);
  else if (node.getAttribute(attribute) !== value)
    node.setAttribute(attribute, value);
}
function children(element2) {
  return Array.from(element2.childNodes);
}
function set_data(text2, data) {
  data = "" + data;
  if (text2.data === data)
    return;
  text2.data = data;
}
function set_input_value(input, value) {
  input.value = value == null ? "" : value;
}
function toggle_class(element2, name, toggle) {
  element2.classList[toggle ? "add" : "remove"](name);
}
var HtmlTag = class {
  constructor(is_svg = false) {
    this.is_svg = false;
    this.is_svg = is_svg;
    this.e = this.n = null;
  }
  c(html) {
    this.h(html);
  }
  m(html, target, anchor = null) {
    if (!this.e) {
      if (this.is_svg)
        this.e = svg_element(target.nodeName);
      else
        this.e = element(target.nodeType === 11 ? "TEMPLATE" : target.nodeName);
      this.t = target.tagName !== "TEMPLATE" ? target : target.content;
      this.c(html);
    }
    this.i(anchor);
  }
  h(html) {
    this.e.innerHTML = html;
    this.n = Array.from(this.e.nodeName === "TEMPLATE" ? this.e.content.childNodes : this.e.childNodes);
  }
  i(anchor) {
    for (let i = 0; i < this.n.length; i += 1) {
      insert(this.t, this.n[i], anchor);
    }
  }
  p(html) {
    this.d();
    this.h(html);
    this.i(this.a);
  }
  d() {
    this.n.forEach(detach);
  }
};
var current_component;
function set_current_component(component) {
  current_component = component;
}
function get_current_component() {
  if (!current_component)
    throw new Error("Function called outside component initialization");
  return current_component;
}
function onMount(fn) {
  get_current_component().$$.on_mount.push(fn);
}
function onDestroy(fn) {
  get_current_component().$$.on_destroy.push(fn);
}
var dirty_components = [];
var binding_callbacks = [];
var render_callbacks = [];
var flush_callbacks = [];
var resolved_promise = /* @__PURE__ */ Promise.resolve();
var update_scheduled = false;
function schedule_update() {
  if (!update_scheduled) {
    update_scheduled = true;
    resolved_promise.then(flush);
  }
}
function add_render_callback(fn) {
  render_callbacks.push(fn);
}
function add_flush_callback(fn) {
  flush_callbacks.push(fn);
}
var seen_callbacks = /* @__PURE__ */ new Set();
var flushidx = 0;
function flush() {
  if (flushidx !== 0) {
    return;
  }
  const saved_component = current_component;
  do {
    try {
      while (flushidx < dirty_components.length) {
        const component = dirty_components[flushidx];
        flushidx++;
        set_current_component(component);
        update(component.$$);
      }
    } catch (e) {
      dirty_components.length = 0;
      flushidx = 0;
      throw e;
    }
    set_current_component(null);
    dirty_components.length = 0;
    flushidx = 0;
    while (binding_callbacks.length)
      binding_callbacks.pop()();
    for (let i = 0; i < render_callbacks.length; i += 1) {
      const callback = render_callbacks[i];
      if (!seen_callbacks.has(callback)) {
        seen_callbacks.add(callback);
        callback();
      }
    }
    render_callbacks.length = 0;
  } while (dirty_components.length);
  while (flush_callbacks.length) {
    flush_callbacks.pop()();
  }
  update_scheduled = false;
  seen_callbacks.clear();
  set_current_component(saved_component);
}
function update($$) {
  if ($$.fragment !== null) {
    $$.update();
    run_all($$.before_update);
    const dirty = $$.dirty;
    $$.dirty = [-1];
    $$.fragment && $$.fragment.p($$.ctx, dirty);
    $$.after_update.forEach(add_render_callback);
  }
}
function flush_render_callbacks(fns) {
  const filtered = [];
  const targets = [];
  render_callbacks.forEach((c) => fns.indexOf(c) === -1 ? filtered.push(c) : targets.push(c));
  targets.forEach((c) => c());
  render_callbacks = filtered;
}
var outroing = /* @__PURE__ */ new Set();
var outros;
function group_outros() {
  outros = {
    r: 0,
    c: [],
    p: outros
    // parent group
  };
}
function check_outros() {
  if (!outros.r) {
    run_all(outros.c);
  }
  outros = outros.p;
}
function transition_in(block, local) {
  if (block && block.i) {
    outroing.delete(block);
    block.i(local);
  }
}
function transition_out(block, local, detach2, callback) {
  if (block && block.o) {
    if (outroing.has(block))
      return;
    outroing.add(block);
    outros.c.push(() => {
      outroing.delete(block);
      if (callback) {
        if (detach2)
          block.d(1);
        callback();
      }
    });
    block.o(local);
  } else if (callback) {
    callback();
  }
}
function outro_and_destroy_block(block, lookup) {
  transition_out(block, 1, 1, () => {
    lookup.delete(block.key);
  });
}
function update_keyed_each(old_blocks, dirty, get_key, dynamic, ctx, list, lookup, node, destroy, create_each_block5, next, get_context) {
  let o = old_blocks.length;
  let n = list.length;
  let i = o;
  const old_indexes = {};
  while (i--)
    old_indexes[old_blocks[i].key] = i;
  const new_blocks = [];
  const new_lookup = /* @__PURE__ */ new Map();
  const deltas = /* @__PURE__ */ new Map();
  const updates = [];
  i = n;
  while (i--) {
    const child_ctx = get_context(ctx, list, i);
    const key = get_key(child_ctx);
    let block = lookup.get(key);
    if (!block) {
      block = create_each_block5(key, child_ctx);
      block.c();
    } else if (dynamic) {
      updates.push(() => block.p(child_ctx, dirty));
    }
    new_lookup.set(key, new_blocks[i] = block);
    if (key in old_indexes)
      deltas.set(key, Math.abs(i - old_indexes[key]));
  }
  const will_move = /* @__PURE__ */ new Set();
  const did_move = /* @__PURE__ */ new Set();
  function insert2(block) {
    transition_in(block, 1);
    block.m(node, next);
    lookup.set(block.key, block);
    next = block.first;
    n--;
  }
  while (o && n) {
    const new_block = new_blocks[n - 1];
    const old_block = old_blocks[o - 1];
    const new_key = new_block.key;
    const old_key = old_block.key;
    if (new_block === old_block) {
      next = new_block.first;
      o--;
      n--;
    } else if (!new_lookup.has(old_key)) {
      destroy(old_block, lookup);
      o--;
    } else if (!lookup.has(new_key) || will_move.has(new_key)) {
      insert2(new_block);
    } else if (did_move.has(old_key)) {
      o--;
    } else if (deltas.get(new_key) > deltas.get(old_key)) {
      did_move.add(new_key);
      insert2(new_block);
    } else {
      will_move.add(old_key);
      o--;
    }
  }
  while (o--) {
    const old_block = old_blocks[o];
    if (!new_lookup.has(old_block.key))
      destroy(old_block, lookup);
  }
  while (n)
    insert2(new_blocks[n - 1]);
  run_all(updates);
  return new_blocks;
}
var _boolean_attributes = [
  "allowfullscreen",
  "allowpaymentrequest",
  "async",
  "autofocus",
  "autoplay",
  "checked",
  "controls",
  "default",
  "defer",
  "disabled",
  "formnovalidate",
  "hidden",
  "inert",
  "ismap",
  "loop",
  "multiple",
  "muted",
  "nomodule",
  "novalidate",
  "open",
  "playsinline",
  "readonly",
  "required",
  "reversed",
  "selected"
];
var boolean_attributes = /* @__PURE__ */ new Set([..._boolean_attributes]);
function bind(component, name, callback) {
  const index = component.$$.props[name];
  if (index !== void 0) {
    component.$$.bound[index] = callback;
    callback(component.$$.ctx[index]);
  }
}
function create_component(block) {
  block && block.c();
}
function mount_component(component, target, anchor, customElement) {
  const { fragment, after_update } = component.$$;
  fragment && fragment.m(target, anchor);
  if (!customElement) {
    add_render_callback(() => {
      const new_on_destroy = component.$$.on_mount.map(run).filter(is_function);
      if (component.$$.on_destroy) {
        component.$$.on_destroy.push(...new_on_destroy);
      } else {
        run_all(new_on_destroy);
      }
      component.$$.on_mount = [];
    });
  }
  after_update.forEach(add_render_callback);
}
function destroy_component(component, detaching) {
  const $$ = component.$$;
  if ($$.fragment !== null) {
    flush_render_callbacks($$.after_update);
    run_all($$.on_destroy);
    $$.fragment && $$.fragment.d(detaching);
    $$.on_destroy = $$.fragment = null;
    $$.ctx = [];
  }
}
function make_dirty(component, i) {
  if (component.$$.dirty[0] === -1) {
    dirty_components.push(component);
    schedule_update();
    component.$$.dirty.fill(0);
  }
  component.$$.dirty[i / 31 | 0] |= 1 << i % 31;
}
function init(component, options, instance5, create_fragment5, not_equal, props, append_styles, dirty = [-1]) {
  const parent_component = current_component;
  set_current_component(component);
  const $$ = component.$$ = {
    fragment: null,
    ctx: [],
    // state
    props,
    update: noop,
    not_equal,
    bound: blank_object(),
    // lifecycle
    on_mount: [],
    on_destroy: [],
    on_disconnect: [],
    before_update: [],
    after_update: [],
    context: new Map(options.context || (parent_component ? parent_component.$$.context : [])),
    // everything else
    callbacks: blank_object(),
    dirty,
    skip_bound: false,
    root: options.target || parent_component.$$.root
  };
  append_styles && append_styles($$.root);
  let ready = false;
  $$.ctx = instance5 ? instance5(component, options.props || {}, (i, ret, ...rest) => {
    const value = rest.length ? rest[0] : ret;
    if ($$.ctx && not_equal($$.ctx[i], $$.ctx[i] = value)) {
      if (!$$.skip_bound && $$.bound[i])
        $$.bound[i](value);
      if (ready)
        make_dirty(component, i);
    }
    return ret;
  }) : [];
  $$.update();
  ready = true;
  run_all($$.before_update);
  $$.fragment = create_fragment5 ? create_fragment5($$.ctx) : false;
  if (options.target) {
    if (options.hydrate) {
      start_hydrating();
      const nodes = children(options.target);
      $$.fragment && $$.fragment.l(nodes);
      nodes.forEach(detach);
    } else {
      $$.fragment && $$.fragment.c();
    }
    if (options.intro)
      transition_in(component.$$.fragment);
    mount_component(component, options.target, options.anchor, options.customElement);
    end_hydrating();
    flush();
  }
  set_current_component(parent_component);
}
var SvelteElement;
if (typeof HTMLElement === "function") {
  SvelteElement = class extends HTMLElement {
    constructor() {
      super();
      this.attachShadow({ mode: "open" });
    }
    connectedCallback() {
      const { on_mount } = this.$$;
      this.$$.on_disconnect = on_mount.map(run).filter(is_function);
      for (const key in this.$$.slotted) {
        this.appendChild(this.$$.slotted[key]);
      }
    }
    attributeChangedCallback(attr2, _oldValue, newValue) {
      this[attr2] = newValue;
    }
    disconnectedCallback() {
      run_all(this.$$.on_disconnect);
    }
    $destroy() {
      destroy_component(this, 1);
      this.$destroy = noop;
    }
    $on(type, callback) {
      if (!is_function(callback)) {
        return noop;
      }
      const callbacks = this.$$.callbacks[type] || (this.$$.callbacks[type] = []);
      callbacks.push(callback);
      return () => {
        const index = callbacks.indexOf(callback);
        if (index !== -1)
          callbacks.splice(index, 1);
      };
    }
    $set($$props) {
      if (this.$$set && !is_empty($$props)) {
        this.$$.skip_bound = true;
        this.$$set($$props);
        this.$$.skip_bound = false;
      }
    }
  };
}
var SvelteComponent = class {
  $destroy() {
    destroy_component(this, 1);
    this.$destroy = noop;
  }
  $on(type, callback) {
    if (!is_function(callback)) {
      return noop;
    }
    const callbacks = this.$$.callbacks[type] || (this.$$.callbacks[type] = []);
    callbacks.push(callback);
    return () => {
      const index = callbacks.indexOf(callback);
      if (index !== -1)
        callbacks.splice(index, 1);
    };
  }
  $set($$props) {
    if (this.$$set && !is_empty($$props)) {
      this.$$.skip_bound = true;
      this.$$set($$props);
      this.$$.skip_bound = false;
    }
  }
};

// node_modules/is-alphabetical/index.js
function isAlphabetical(character) {
  const code = typeof character === "string" ? character.charCodeAt(0) : character;
  return code >= 97 && code <= 122 || code >= 65 && code <= 90;
}

// node_modules/is-decimal/index.js
function isDecimal(character) {
  const code = typeof character === "string" ? character.charCodeAt(0) : character;
  return code >= 48 && code <= 57;
}

// node_modules/is-alphanumerical/index.js
function isAlphanumerical(character) {
  return isAlphabetical(character) || isDecimal(character);
}

// node_modules/bcp-47/lib/regular.js
var regular = [
  "art-lojban",
  "cel-gaulish",
  "no-bok",
  "no-nyn",
  "zh-guoyu",
  "zh-hakka",
  "zh-min",
  "zh-min-nan",
  "zh-xiang"
];

// node_modules/bcp-47/lib/normal.js
var normal = {
  "en-gb-oed": "en-GB-oxendict",
  "i-ami": "ami",
  "i-bnn": "bnn",
  "i-default": null,
  "i-enochian": null,
  "i-hak": "hak",
  "i-klingon": "tlh",
  "i-lux": "lb",
  "i-mingo": null,
  "i-navajo": "nv",
  "i-pwn": "pwn",
  "i-tao": "tao",
  "i-tay": "tay",
  "i-tsu": "tsu",
  "sgn-be-fr": "sfb",
  "sgn-be-nl": "vgt",
  "sgn-ch-de": "sgg",
  "art-lojban": "jbo",
  "cel-gaulish": null,
  "no-bok": "nb",
  "no-nyn": "nn",
  "zh-guoyu": "cmn",
  "zh-hakka": "hak",
  "zh-min": null,
  "zh-min-nan": "nan",
  "zh-xiang": "hsn"
};

// node_modules/bcp-47/lib/parse.js
var own = {}.hasOwnProperty;
function parse(tag, options = {}) {
  const result = empty2();
  const source = String(tag);
  const value = source.toLowerCase();
  let index = 0;
  if (tag === null || tag === void 0) {
    throw new Error("Expected string, got `" + tag + "`");
  }
  if (own.call(normal, value)) {
    const replacement = normal[value];
    if ((options.normalize === void 0 || options.normalize === null || options.normalize) && typeof replacement === "string") {
      return parse(replacement);
    }
    result[regular.includes(value) ? "regular" : "irregular"] = source;
    return result;
  }
  while (isAlphabetical(value.charCodeAt(index)) && index < 9)
    index++;
  if (index > 1 && index < 9) {
    result.language = source.slice(0, index);
    if (index < 4) {
      let groups = 0;
      while (value.charCodeAt(index) === 45 && isAlphabetical(value.charCodeAt(index + 1)) && isAlphabetical(value.charCodeAt(index + 2)) && isAlphabetical(value.charCodeAt(index + 3)) && !isAlphabetical(value.charCodeAt(index + 4))) {
        if (groups > 2) {
          return fail(
            index,
            3,
            "Too many extended language subtags, expected at most 3 subtags"
          );
        }
        result.extendedLanguageSubtags.push(source.slice(index + 1, index + 4));
        index += 4;
        groups++;
      }
    }
    if (value.charCodeAt(index) === 45 && isAlphabetical(value.charCodeAt(index + 1)) && isAlphabetical(value.charCodeAt(index + 2)) && isAlphabetical(value.charCodeAt(index + 3)) && isAlphabetical(value.charCodeAt(index + 4)) && !isAlphabetical(value.charCodeAt(index + 5))) {
      result.script = source.slice(index + 1, index + 5);
      index += 5;
    }
    if (value.charCodeAt(index) === 45) {
      if (isAlphabetical(value.charCodeAt(index + 1)) && isAlphabetical(value.charCodeAt(index + 2)) && !isAlphabetical(value.charCodeAt(index + 3))) {
        result.region = source.slice(index + 1, index + 3);
        index += 3;
      } else if (isDecimal(value.charCodeAt(index + 1)) && isDecimal(value.charCodeAt(index + 2)) && isDecimal(value.charCodeAt(index + 3)) && !isDecimal(value.charCodeAt(index + 4))) {
        result.region = source.slice(index + 1, index + 4);
        index += 4;
      }
    }
    while (value.charCodeAt(index) === 45) {
      const start = index + 1;
      let offset = start;
      while (isAlphanumerical(value.charCodeAt(offset))) {
        if (offset - start > 7) {
          return fail(
            offset,
            1,
            "Too long variant, expected at most 8 characters"
          );
        }
        offset++;
      }
      if (
        // Long variant.
        offset - start > 4 || // Short variant.
        offset - start > 3 && isDecimal(value.charCodeAt(start))
      ) {
        result.variants.push(source.slice(start, offset));
        index = offset;
      } else {
        break;
      }
    }
    while (value.charCodeAt(index) === 45) {
      if (value.charCodeAt(index + 1) === 120 || !isAlphanumerical(value.charCodeAt(index + 1)) || value.charCodeAt(index + 2) !== 45 || !isAlphanumerical(value.charCodeAt(index + 3))) {
        break;
      }
      let offset = index + 2;
      let groups = 0;
      while (value.charCodeAt(offset) === 45 && isAlphanumerical(value.charCodeAt(offset + 1)) && isAlphanumerical(value.charCodeAt(offset + 2))) {
        const start = offset + 1;
        offset = start + 2;
        groups++;
        while (isAlphanumerical(value.charCodeAt(offset))) {
          if (offset - start > 7) {
            return fail(
              offset,
              2,
              "Too long extension, expected at most 8 characters"
            );
          }
          offset++;
        }
      }
      if (!groups) {
        return fail(
          offset,
          4,
          "Empty extension, extensions must have at least 2 characters of content"
        );
      }
      result.extensions.push({
        singleton: source.charAt(index + 1),
        extensions: source.slice(index + 3, offset).split("-")
      });
      index = offset;
    }
  } else {
    index = 0;
  }
  if (index === 0 && value.charCodeAt(index) === 120 || value.charCodeAt(index) === 45 && value.charCodeAt(index + 1) === 120) {
    index = index ? index + 2 : 1;
    let offset = index;
    while (value.charCodeAt(offset) === 45 && isAlphanumerical(value.charCodeAt(offset + 1))) {
      const start = index + 1;
      offset = start;
      while (isAlphanumerical(value.charCodeAt(offset))) {
        if (offset - start > 7) {
          return fail(
            offset,
            5,
            "Too long private-use area, expected at most 8 characters"
          );
        }
        offset++;
      }
      result.privateuse.push(source.slice(index + 1, offset));
      index = offset;
    }
  }
  if (index !== source.length) {
    return fail(index, 6, "Found superfluous content after tag");
  }
  return result;
  function fail(offset, code, reason) {
    if (options.warning)
      options.warning(reason, code, offset);
    return options.forgiving ? result : empty2();
  }
}
function empty2() {
  return {
    language: null,
    extendedLanguageSubtags: [],
    script: null,
    region: null,
    variants: [],
    extensions: [],
    privateuse: [],
    irregular: null,
    regular: null
  };
}

// svelte/result.svelte
function get_each_context(ctx, list, i) {
  const child_ctx = ctx.slice();
  child_ctx[8] = list[i][0];
  child_ctx[9] = list[i][1];
  return child_ctx;
}
function create_else_block(ctx) {
  let t0;
  let div;
  let p0;
  let t2;
  let p1;
  let if_block = (
    /*show_images*/
    ctx[0] && create_if_block_4(ctx)
  );
  return {
    c() {
      if (if_block)
        if_block.c();
      t0 = space();
      div = element("div");
      p0 = element("p");
      p0.textContent = `${/*placeholder*/
      ctx[3](30)}`;
      t2 = space();
      p1 = element("p");
      p1.textContent = `${/*placeholder*/
      ctx[3](40)}`;
      attr(p0, "class", "pagefind-ui__result-title pagefind-ui__loading svelte-j9e30");
      attr(p1, "class", "pagefind-ui__result-excerpt pagefind-ui__loading svelte-j9e30");
      attr(div, "class", "pagefind-ui__result-inner svelte-j9e30");
    },
    m(target, anchor) {
      if (if_block)
        if_block.m(target, anchor);
      insert(target, t0, anchor);
      insert(target, div, anchor);
      append(div, p0);
      append(div, t2);
      append(div, p1);
    },
    p(ctx2, dirty) {
      if (
        /*show_images*/
        ctx2[0]
      ) {
        if (if_block) {
        } else {
          if_block = create_if_block_4(ctx2);
          if_block.c();
          if_block.m(t0.parentNode, t0);
        }
      } else if (if_block) {
        if_block.d(1);
        if_block = null;
      }
    },
    d(detaching) {
      if (if_block)
        if_block.d(detaching);
      if (detaching)
        detach(t0);
      if (detaching)
        detach(div);
    }
  };
}
function create_if_block(ctx) {
  let t0;
  let div;
  let p0;
  let a;
  let t1_value = (
    /*data*/
    ctx[1].meta?.title + ""
  );
  let t1;
  let a_href_value;
  let t2;
  let p1;
  let raw_value = (
    /*data*/
    ctx[1].excerpt + ""
  );
  let t3;
  let if_block0 = (
    /*show_images*/
    ctx[0] && create_if_block_2(ctx)
  );
  let if_block1 = (
    /*meta*/
    ctx[2].length && create_if_block_1(ctx)
  );
  return {
    c() {
      if (if_block0)
        if_block0.c();
      t0 = space();
      div = element("div");
      p0 = element("p");
      a = element("a");
      t1 = text(t1_value);
      t2 = space();
      p1 = element("p");
      t3 = space();
      if (if_block1)
        if_block1.c();
      attr(a, "class", "pagefind-ui__result-link svelte-j9e30");
      attr(a, "href", a_href_value = /*data*/
      ctx[1].meta?.url || /*data*/
      ctx[1].url);
      attr(p0, "class", "pagefind-ui__result-title svelte-j9e30");
      attr(p1, "class", "pagefind-ui__result-excerpt svelte-j9e30");
      attr(div, "class", "pagefind-ui__result-inner svelte-j9e30");
    },
    m(target, anchor) {
      if (if_block0)
        if_block0.m(target, anchor);
      insert(target, t0, anchor);
      insert(target, div, anchor);
      append(div, p0);
      append(p0, a);
      append(a, t1);
      append(div, t2);
      append(div, p1);
      p1.innerHTML = raw_value;
      append(div, t3);
      if (if_block1)
        if_block1.m(div, null);
    },
    p(ctx2, dirty) {
      if (
        /*show_images*/
        ctx2[0]
      ) {
        if (if_block0) {
          if_block0.p(ctx2, dirty);
        } else {
          if_block0 = create_if_block_2(ctx2);
          if_block0.c();
          if_block0.m(t0.parentNode, t0);
        }
      } else if (if_block0) {
        if_block0.d(1);
        if_block0 = null;
      }
      if (dirty & /*data*/
      2 && t1_value !== (t1_value = /*data*/
      ctx2[1].meta?.title + ""))
        set_data(t1, t1_value);
      if (dirty & /*data*/
      2 && a_href_value !== (a_href_value = /*data*/
      ctx2[1].meta?.url || /*data*/
      ctx2[1].url)) {
        attr(a, "href", a_href_value);
      }
      if (dirty & /*data*/
      2 && raw_value !== (raw_value = /*data*/
      ctx2[1].excerpt + ""))
        p1.innerHTML = raw_value;
      ;
      if (
        /*meta*/
        ctx2[2].length
      ) {
        if (if_block1) {
          if_block1.p(ctx2, dirty);
        } else {
          if_block1 = create_if_block_1(ctx2);
          if_block1.c();
          if_block1.m(div, null);
        }
      } else if (if_block1) {
        if_block1.d(1);
        if_block1 = null;
      }
    },
    d(detaching) {
      if (if_block0)
        if_block0.d(detaching);
      if (detaching)
        detach(t0);
      if (detaching)
        detach(div);
      if (if_block1)
        if_block1.d();
    }
  };
}
function create_if_block_4(ctx) {
  let div;
  return {
    c() {
      div = element("div");
      attr(div, "class", "pagefind-ui__result-thumb pagefind-ui__loading svelte-j9e30");
    },
    m(target, anchor) {
      insert(target, div, anchor);
    },
    d(detaching) {
      if (detaching)
        detach(div);
    }
  };
}
function create_if_block_2(ctx) {
  let div;
  let if_block = (
    /*data*/
    ctx[1].meta.image && create_if_block_3(ctx)
  );
  return {
    c() {
      div = element("div");
      if (if_block)
        if_block.c();
      attr(div, "class", "pagefind-ui__result-thumb svelte-j9e30");
    },
    m(target, anchor) {
      insert(target, div, anchor);
      if (if_block)
        if_block.m(div, null);
    },
    p(ctx2, dirty) {
      if (
        /*data*/
        ctx2[1].meta.image
      ) {
        if (if_block) {
          if_block.p(ctx2, dirty);
        } else {
          if_block = create_if_block_3(ctx2);
          if_block.c();
          if_block.m(div, null);
        }
      } else if (if_block) {
        if_block.d(1);
        if_block = null;
      }
    },
    d(detaching) {
      if (detaching)
        detach(div);
      if (if_block)
        if_block.d();
    }
  };
}
function create_if_block_3(ctx) {
  let img;
  let img_src_value;
  let img_alt_value;
  return {
    c() {
      img = element("img");
      attr(img, "class", "pagefind-ui__result-image svelte-j9e30");
      if (!src_url_equal(img.src, img_src_value = /*data*/
      ctx[1].meta?.image))
        attr(img, "src", img_src_value);
      attr(img, "alt", img_alt_value = /*data*/
      ctx[1].meta?.image_alt || /*data*/
      ctx[1].meta?.title);
    },
    m(target, anchor) {
      insert(target, img, anchor);
    },
    p(ctx2, dirty) {
      if (dirty & /*data*/
      2 && !src_url_equal(img.src, img_src_value = /*data*/
      ctx2[1].meta?.image)) {
        attr(img, "src", img_src_value);
      }
      if (dirty & /*data*/
      2 && img_alt_value !== (img_alt_value = /*data*/
      ctx2[1].meta?.image_alt || /*data*/
      ctx2[1].meta?.title)) {
        attr(img, "alt", img_alt_value);
      }
    },
    d(detaching) {
      if (detaching)
        detach(img);
    }
  };
}
function create_if_block_1(ctx) {
  let ul;
  let each_value = (
    /*meta*/
    ctx[2]
  );
  let each_blocks = [];
  for (let i = 0; i < each_value.length; i += 1) {
    each_blocks[i] = create_each_block(get_each_context(ctx, each_value, i));
  }
  return {
    c() {
      ul = element("ul");
      for (let i = 0; i < each_blocks.length; i += 1) {
        each_blocks[i].c();
      }
      attr(ul, "class", "pagefind-ui__result-tags svelte-j9e30");
    },
    m(target, anchor) {
      insert(target, ul, anchor);
      for (let i = 0; i < each_blocks.length; i += 1) {
        if (each_blocks[i]) {
          each_blocks[i].m(ul, null);
        }
      }
    },
    p(ctx2, dirty) {
      if (dirty & /*meta*/
      4) {
        each_value = /*meta*/
        ctx2[2];
        let i;
        for (i = 0; i < each_value.length; i += 1) {
          const child_ctx = get_each_context(ctx2, each_value, i);
          if (each_blocks[i]) {
            each_blocks[i].p(child_ctx, dirty);
          } else {
            each_blocks[i] = create_each_block(child_ctx);
            each_blocks[i].c();
            each_blocks[i].m(ul, null);
          }
        }
        for (; i < each_blocks.length; i += 1) {
          each_blocks[i].d(1);
        }
        each_blocks.length = each_value.length;
      }
    },
    d(detaching) {
      if (detaching)
        detach(ul);
      destroy_each(each_blocks, detaching);
    }
  };
}
function create_each_block(ctx) {
  let li;
  let t0_value = (
    /*metaTitle*/
    ctx[8].replace(/^(\w)/, func) + ""
  );
  let t0;
  let t1;
  let t2_value = (
    /*metaValue*/
    ctx[9] + ""
  );
  let t2;
  let t3;
  let li_data_pagefind_ui_meta_value;
  return {
    c() {
      li = element("li");
      t0 = text(t0_value);
      t1 = text(": ");
      t2 = text(t2_value);
      t3 = space();
      attr(li, "class", "pagefind-ui__result-tag svelte-j9e30");
      attr(li, "data-pagefind-ui-meta", li_data_pagefind_ui_meta_value = /*metaTitle*/
      ctx[8]);
    },
    m(target, anchor) {
      insert(target, li, anchor);
      append(li, t0);
      append(li, t1);
      append(li, t2);
      append(li, t3);
    },
    p(ctx2, dirty) {
      if (dirty & /*meta*/
      4 && t0_value !== (t0_value = /*metaTitle*/
      ctx2[8].replace(/^(\w)/, func) + ""))
        set_data(t0, t0_value);
      if (dirty & /*meta*/
      4 && t2_value !== (t2_value = /*metaValue*/
      ctx2[9] + ""))
        set_data(t2, t2_value);
      if (dirty & /*meta*/
      4 && li_data_pagefind_ui_meta_value !== (li_data_pagefind_ui_meta_value = /*metaTitle*/
      ctx2[8])) {
        attr(li, "data-pagefind-ui-meta", li_data_pagefind_ui_meta_value);
      }
    },
    d(detaching) {
      if (detaching)
        detach(li);
    }
  };
}
function create_fragment(ctx) {
  let li;
  function select_block_type(ctx2, dirty) {
    if (
      /*data*/
      ctx2[1]
    )
      return create_if_block;
    return create_else_block;
  }
  let current_block_type = select_block_type(ctx, -1);
  let if_block = current_block_type(ctx);
  return {
    c() {
      li = element("li");
      if_block.c();
      attr(li, "class", "pagefind-ui__result svelte-j9e30");
    },
    m(target, anchor) {
      insert(target, li, anchor);
      if_block.m(li, null);
    },
    p(ctx2, [dirty]) {
      if (current_block_type === (current_block_type = select_block_type(ctx2, dirty)) && if_block) {
        if_block.p(ctx2, dirty);
      } else {
        if_block.d(1);
        if_block = current_block_type(ctx2);
        if (if_block) {
          if_block.c();
          if_block.m(li, null);
        }
      }
    },
    i: noop,
    o: noop,
    d(detaching) {
      if (detaching)
        detach(li);
      if_block.d();
    }
  };
}
var func = (c) => c.toLocaleUpperCase();
function instance($$self, $$props, $$invalidate) {
  let { show_images = true } = $$props;
  let { process_result = null } = $$props;
  let { result = {
    data: async () => {
    }
  } } = $$props;
  const skipMeta = ["title", "image", "image_alt", "url"];
  let data;
  let meta = [];
  const load = async (r) => {
    $$invalidate(1, data = await r.data());
    $$invalidate(1, data = process_result?.(data) ?? data);
    $$invalidate(2, meta = Object.entries(data.meta).filter(([key]) => !skipMeta.includes(key)));
  };
  const placeholder = (max = 30) => {
    return ". ".repeat(Math.floor(10 + Math.random() * max));
  };
  $$self.$$set = ($$props2) => {
    if ("show_images" in $$props2)
      $$invalidate(0, show_images = $$props2.show_images);
    if ("process_result" in $$props2)
      $$invalidate(4, process_result = $$props2.process_result);
    if ("result" in $$props2)
      $$invalidate(5, result = $$props2.result);
  };
  $$self.$$.update = () => {
    if ($$self.$$.dirty & /*result*/
    32) {
      $:
        load(result);
    }
  };
  return [show_images, data, meta, placeholder, process_result, result];
}
var Result = class extends SvelteComponent {
  constructor(options) {
    super();
    init(this, options, instance, create_fragment, safe_not_equal, {
      show_images: 0,
      process_result: 4,
      result: 5
    });
  }
};
var result_default = Result;

// svelte/result_with_subs.svelte
function get_each_context2(ctx, list, i) {
  const child_ctx = ctx.slice();
  child_ctx[11] = list[i][0];
  child_ctx[12] = list[i][1];
  return child_ctx;
}
function get_each_context_1(ctx, list, i) {
  const child_ctx = ctx.slice();
  child_ctx[15] = list[i];
  return child_ctx;
}
function create_else_block2(ctx) {
  let t0;
  let div;
  let p0;
  let t2;
  let p1;
  let if_block = (
    /*show_images*/
    ctx[0] && create_if_block_5(ctx)
  );
  return {
    c() {
      if (if_block)
        if_block.c();
      t0 = space();
      div = element("div");
      p0 = element("p");
      p0.textContent = `${/*placeholder*/
      ctx[5](30)}`;
      t2 = space();
      p1 = element("p");
      p1.textContent = `${/*placeholder*/
      ctx[5](40)}`;
      attr(p0, "class", "pagefind-ui__result-title pagefind-ui__loading svelte-4xnkmf");
      attr(p1, "class", "pagefind-ui__result-excerpt pagefind-ui__loading svelte-4xnkmf");
      attr(div, "class", "pagefind-ui__result-inner svelte-4xnkmf");
    },
    m(target, anchor) {
      if (if_block)
        if_block.m(target, anchor);
      insert(target, t0, anchor);
      insert(target, div, anchor);
      append(div, p0);
      append(div, t2);
      append(div, p1);
    },
    p(ctx2, dirty) {
      if (
        /*show_images*/
        ctx2[0]
      ) {
        if (if_block) {
        } else {
          if_block = create_if_block_5(ctx2);
          if_block.c();
          if_block.m(t0.parentNode, t0);
        }
      } else if (if_block) {
        if_block.d(1);
        if_block = null;
      }
    },
    d(detaching) {
      if (if_block)
        if_block.d(detaching);
      if (detaching)
        detach(t0);
      if (detaching)
        detach(div);
    }
  };
}
function create_if_block2(ctx) {
  let t0;
  let div;
  let p;
  let a;
  let t1_value = (
    /*data*/
    ctx[1].meta?.title + ""
  );
  let t1;
  let a_href_value;
  let t2;
  let t3;
  let t4;
  let if_block0 = (
    /*show_images*/
    ctx[0] && create_if_block_32(ctx)
  );
  let if_block1 = (
    /*has_root_sub_result*/
    ctx[4] && create_if_block_22(ctx)
  );
  let each_value_1 = (
    /*non_root_sub_results*/
    ctx[3]
  );
  let each_blocks = [];
  for (let i = 0; i < each_value_1.length; i += 1) {
    each_blocks[i] = create_each_block_1(get_each_context_1(ctx, each_value_1, i));
  }
  let if_block2 = (
    /*meta*/
    ctx[2].length && create_if_block_12(ctx)
  );
  return {
    c() {
      if (if_block0)
        if_block0.c();
      t0 = space();
      div = element("div");
      p = element("p");
      a = element("a");
      t1 = text(t1_value);
      t2 = space();
      if (if_block1)
        if_block1.c();
      t3 = space();
      for (let i = 0; i < each_blocks.length; i += 1) {
        each_blocks[i].c();
      }
      t4 = space();
      if (if_block2)
        if_block2.c();
      attr(a, "class", "pagefind-ui__result-link svelte-4xnkmf");
      attr(a, "href", a_href_value = /*data*/
      ctx[1].meta?.url || /*data*/
      ctx[1].url);
      attr(p, "class", "pagefind-ui__result-title svelte-4xnkmf");
      attr(div, "class", "pagefind-ui__result-inner svelte-4xnkmf");
    },
    m(target, anchor) {
      if (if_block0)
        if_block0.m(target, anchor);
      insert(target, t0, anchor);
      insert(target, div, anchor);
      append(div, p);
      append(p, a);
      append(a, t1);
      append(div, t2);
      if (if_block1)
        if_block1.m(div, null);
      append(div, t3);
      for (let i = 0; i < each_blocks.length; i += 1) {
        if (each_blocks[i]) {
          each_blocks[i].m(div, null);
        }
      }
      append(div, t4);
      if (if_block2)
        if_block2.m(div, null);
    },
    p(ctx2, dirty) {
      if (
        /*show_images*/
        ctx2[0]
      ) {
        if (if_block0) {
          if_block0.p(ctx2, dirty);
        } else {
          if_block0 = create_if_block_32(ctx2);
          if_block0.c();
          if_block0.m(t0.parentNode, t0);
        }
      } else if (if_block0) {
        if_block0.d(1);
        if_block0 = null;
      }
      if (dirty & /*data*/
      2 && t1_value !== (t1_value = /*data*/
      ctx2[1].meta?.title + ""))
        set_data(t1, t1_value);
      if (dirty & /*data*/
      2 && a_href_value !== (a_href_value = /*data*/
      ctx2[1].meta?.url || /*data*/
      ctx2[1].url)) {
        attr(a, "href", a_href_value);
      }
      if (
        /*has_root_sub_result*/
        ctx2[4]
      ) {
        if (if_block1) {
          if_block1.p(ctx2, dirty);
        } else {
          if_block1 = create_if_block_22(ctx2);
          if_block1.c();
          if_block1.m(div, t3);
        }
      } else if (if_block1) {
        if_block1.d(1);
        if_block1 = null;
      }
      if (dirty & /*non_root_sub_results*/
      8) {
        each_value_1 = /*non_root_sub_results*/
        ctx2[3];
        let i;
        for (i = 0; i < each_value_1.length; i += 1) {
          const child_ctx = get_each_context_1(ctx2, each_value_1, i);
          if (each_blocks[i]) {
            each_blocks[i].p(child_ctx, dirty);
          } else {
            each_blocks[i] = create_each_block_1(child_ctx);
            each_blocks[i].c();
            each_blocks[i].m(div, t4);
          }
        }
        for (; i < each_blocks.length; i += 1) {
          each_blocks[i].d(1);
        }
        each_blocks.length = each_value_1.length;
      }
      if (
        /*meta*/
        ctx2[2].length
      ) {
        if (if_block2) {
          if_block2.p(ctx2, dirty);
        } else {
          if_block2 = create_if_block_12(ctx2);
          if_block2.c();
          if_block2.m(div, null);
        }
      } else if (if_block2) {
        if_block2.d(1);
        if_block2 = null;
      }
    },
    d(detaching) {
      if (if_block0)
        if_block0.d(detaching);
      if (detaching)
        detach(t0);
      if (detaching)
        detach(div);
      if (if_block1)
        if_block1.d();
      destroy_each(each_blocks, detaching);
      if (if_block2)
        if_block2.d();
    }
  };
}
function create_if_block_5(ctx) {
  let div;
  return {
    c() {
      div = element("div");
      attr(div, "class", "pagefind-ui__result-thumb pagefind-ui__loading svelte-4xnkmf");
    },
    m(target, anchor) {
      insert(target, div, anchor);
    },
    d(detaching) {
      if (detaching)
        detach(div);
    }
  };
}
function create_if_block_32(ctx) {
  let div;
  let if_block = (
    /*data*/
    ctx[1].meta.image && create_if_block_42(ctx)
  );
  return {
    c() {
      div = element("div");
      if (if_block)
        if_block.c();
      attr(div, "class", "pagefind-ui__result-thumb svelte-4xnkmf");
    },
    m(target, anchor) {
      insert(target, div, anchor);
      if (if_block)
        if_block.m(div, null);
    },
    p(ctx2, dirty) {
      if (
        /*data*/
        ctx2[1].meta.image
      ) {
        if (if_block) {
          if_block.p(ctx2, dirty);
        } else {
          if_block = create_if_block_42(ctx2);
          if_block.c();
          if_block.m(div, null);
        }
      } else if (if_block) {
        if_block.d(1);
        if_block = null;
      }
    },
    d(detaching) {
      if (detaching)
        detach(div);
      if (if_block)
        if_block.d();
    }
  };
}
function create_if_block_42(ctx) {
  let img;
  let img_src_value;
  let img_alt_value;
  return {
    c() {
      img = element("img");
      attr(img, "class", "pagefind-ui__result-image svelte-4xnkmf");
      if (!src_url_equal(img.src, img_src_value = /*data*/
      ctx[1].meta?.image))
        attr(img, "src", img_src_value);
      attr(img, "alt", img_alt_value = /*data*/
      ctx[1].meta?.image_alt || /*data*/
      ctx[1].meta?.title);
    },
    m(target, anchor) {
      insert(target, img, anchor);
    },
    p(ctx2, dirty) {
      if (dirty & /*data*/
      2 && !src_url_equal(img.src, img_src_value = /*data*/
      ctx2[1].meta?.image)) {
        attr(img, "src", img_src_value);
      }
      if (dirty & /*data*/
      2 && img_alt_value !== (img_alt_value = /*data*/
      ctx2[1].meta?.image_alt || /*data*/
      ctx2[1].meta?.title)) {
        attr(img, "alt", img_alt_value);
      }
    },
    d(detaching) {
      if (detaching)
        detach(img);
    }
  };
}
function create_if_block_22(ctx) {
  let p;
  let raw_value = (
    /*data*/
    ctx[1].excerpt + ""
  );
  return {
    c() {
      p = element("p");
      attr(p, "class", "pagefind-ui__result-excerpt svelte-4xnkmf");
    },
    m(target, anchor) {
      insert(target, p, anchor);
      p.innerHTML = raw_value;
    },
    p(ctx2, dirty) {
      if (dirty & /*data*/
      2 && raw_value !== (raw_value = /*data*/
      ctx2[1].excerpt + ""))
        p.innerHTML = raw_value;
      ;
    },
    d(detaching) {
      if (detaching)
        detach(p);
    }
  };
}
function create_each_block_1(ctx) {
  let div;
  let p0;
  let a;
  let t0_value = (
    /*subres*/
    ctx[15].title + ""
  );
  let t0;
  let a_href_value;
  let t1;
  let p1;
  let raw_value = (
    /*subres*/
    ctx[15].excerpt + ""
  );
  return {
    c() {
      div = element("div");
      p0 = element("p");
      a = element("a");
      t0 = text(t0_value);
      t1 = space();
      p1 = element("p");
      attr(a, "class", "pagefind-ui__result-link svelte-4xnkmf");
      attr(a, "href", a_href_value = /*subres*/
      ctx[15].url);
      attr(p0, "class", "pagefind-ui__result-title svelte-4xnkmf");
      attr(p1, "class", "pagefind-ui__result-excerpt svelte-4xnkmf");
      attr(div, "class", "pagefind-ui__result-nested svelte-4xnkmf");
    },
    m(target, anchor) {
      insert(target, div, anchor);
      append(div, p0);
      append(p0, a);
      append(a, t0);
      append(div, t1);
      append(div, p1);
      p1.innerHTML = raw_value;
    },
    p(ctx2, dirty) {
      if (dirty & /*non_root_sub_results*/
      8 && t0_value !== (t0_value = /*subres*/
      ctx2[15].title + ""))
        set_data(t0, t0_value);
      if (dirty & /*non_root_sub_results*/
      8 && a_href_value !== (a_href_value = /*subres*/
      ctx2[15].url)) {
        attr(a, "href", a_href_value);
      }
      if (dirty & /*non_root_sub_results*/
      8 && raw_value !== (raw_value = /*subres*/
      ctx2[15].excerpt + ""))
        p1.innerHTML = raw_value;
      ;
    },
    d(detaching) {
      if (detaching)
        detach(div);
    }
  };
}
function create_if_block_12(ctx) {
  let ul;
  let each_value = (
    /*meta*/
    ctx[2]
  );
  let each_blocks = [];
  for (let i = 0; i < each_value.length; i += 1) {
    each_blocks[i] = create_each_block2(get_each_context2(ctx, each_value, i));
  }
  return {
    c() {
      ul = element("ul");
      for (let i = 0; i < each_blocks.length; i += 1) {
        each_blocks[i].c();
      }
      attr(ul, "class", "pagefind-ui__result-tags svelte-4xnkmf");
    },
    m(target, anchor) {
      insert(target, ul, anchor);
      for (let i = 0; i < each_blocks.length; i += 1) {
        if (each_blocks[i]) {
          each_blocks[i].m(ul, null);
        }
      }
    },
    p(ctx2, dirty) {
      if (dirty & /*meta*/
      4) {
        each_value = /*meta*/
        ctx2[2];
        let i;
        for (i = 0; i < each_value.length; i += 1) {
          const child_ctx = get_each_context2(ctx2, each_value, i);
          if (each_blocks[i]) {
            each_blocks[i].p(child_ctx, dirty);
          } else {
            each_blocks[i] = create_each_block2(child_ctx);
            each_blocks[i].c();
            each_blocks[i].m(ul, null);
          }
        }
        for (; i < each_blocks.length; i += 1) {
          each_blocks[i].d(1);
        }
        each_blocks.length = each_value.length;
      }
    },
    d(detaching) {
      if (detaching)
        detach(ul);
      destroy_each(each_blocks, detaching);
    }
  };
}
function create_each_block2(ctx) {
  let li;
  let t0_value = (
    /*metaTitle*/
    ctx[11].replace(/^(\w)/, func2) + ""
  );
  let t0;
  let t1;
  let t2_value = (
    /*metaValue*/
    ctx[12] + ""
  );
  let t2;
  let t3;
  let li_data_pagefind_ui_meta_value;
  return {
    c() {
      li = element("li");
      t0 = text(t0_value);
      t1 = text(": ");
      t2 = text(t2_value);
      t3 = space();
      attr(li, "class", "pagefind-ui__result-tag svelte-4xnkmf");
      attr(li, "data-pagefind-ui-meta", li_data_pagefind_ui_meta_value = /*metaTitle*/
      ctx[11]);
    },
    m(target, anchor) {
      insert(target, li, anchor);
      append(li, t0);
      append(li, t1);
      append(li, t2);
      append(li, t3);
    },
    p(ctx2, dirty) {
      if (dirty & /*meta*/
      4 && t0_value !== (t0_value = /*metaTitle*/
      ctx2[11].replace(/^(\w)/, func2) + ""))
        set_data(t0, t0_value);
      if (dirty & /*meta*/
      4 && t2_value !== (t2_value = /*metaValue*/
      ctx2[12] + ""))
        set_data(t2, t2_value);
      if (dirty & /*meta*/
      4 && li_data_pagefind_ui_meta_value !== (li_data_pagefind_ui_meta_value = /*metaTitle*/
      ctx2[11])) {
        attr(li, "data-pagefind-ui-meta", li_data_pagefind_ui_meta_value);
      }
    },
    d(detaching) {
      if (detaching)
        detach(li);
    }
  };
}
function create_fragment2(ctx) {
  let li;
  function select_block_type(ctx2, dirty) {
    if (
      /*data*/
      ctx2[1]
    )
      return create_if_block2;
    return create_else_block2;
  }
  let current_block_type = select_block_type(ctx, -1);
  let if_block = current_block_type(ctx);
  return {
    c() {
      li = element("li");
      if_block.c();
      attr(li, "class", "pagefind-ui__result svelte-4xnkmf");
    },
    m(target, anchor) {
      insert(target, li, anchor);
      if_block.m(li, null);
    },
    p(ctx2, [dirty]) {
      if (current_block_type === (current_block_type = select_block_type(ctx2, dirty)) && if_block) {
        if_block.p(ctx2, dirty);
      } else {
        if_block.d(1);
        if_block = current_block_type(ctx2);
        if (if_block) {
          if_block.c();
          if_block.m(li, null);
        }
      }
    },
    i: noop,
    o: noop,
    d(detaching) {
      if (detaching)
        detach(li);
      if_block.d();
    }
  };
}
var func2 = (c) => c.toLocaleUpperCase();
function instance2($$self, $$props, $$invalidate) {
  let { show_images = true } = $$props;
  let { process_result = null } = $$props;
  let { result = {
    data: async () => {
    }
  } } = $$props;
  const skipMeta = ["title", "image", "image_alt", "url"];
  let data;
  let meta = [];
  let non_root_sub_results = [];
  let has_root_sub_result = false;
  const thin_sub_results = (results, limit) => {
    if (results.length <= limit) {
      return results;
    }
    const top_results = [...results].sort((a, b) => b.locations.length - a.locations.length).slice(0, 3).map((r) => r.url);
    return results.filter((r) => top_results.includes(r.url));
  };
  const load = async (r) => {
    $$invalidate(1, data = await r.data());
    $$invalidate(1, data = process_result?.(data) ?? data);
    $$invalidate(2, meta = Object.entries(data.meta).filter(([key]) => !skipMeta.includes(key)));
    if (Array.isArray(data.sub_results)) {
      $$invalidate(4, has_root_sub_result = data.sub_results?.[0]?.url === (data.meta?.url || data.url));
      if (has_root_sub_result) {
        $$invalidate(3, non_root_sub_results = thin_sub_results(data.sub_results.slice(1), 3));
      } else {
        $$invalidate(3, non_root_sub_results = thin_sub_results([...data.sub_results], 3));
      }
    }
  };
  const placeholder = (max = 30) => {
    return ". ".repeat(Math.floor(10 + Math.random() * max));
  };
  $$self.$$set = ($$props2) => {
    if ("show_images" in $$props2)
      $$invalidate(0, show_images = $$props2.show_images);
    if ("process_result" in $$props2)
      $$invalidate(6, process_result = $$props2.process_result);
    if ("result" in $$props2)
      $$invalidate(7, result = $$props2.result);
  };
  $$self.$$.update = () => {
    if ($$self.$$.dirty & /*result*/
    128) {
      $:
        load(result);
    }
  };
  return [
    show_images,
    data,
    meta,
    non_root_sub_results,
    has_root_sub_result,
    placeholder,
    process_result,
    result
  ];
}
var Result_with_subs = class extends SvelteComponent {
  constructor(options) {
    super();
    init(this, options, instance2, create_fragment2, safe_not_equal, {
      show_images: 0,
      process_result: 6,
      result: 7
    });
  }
};
var result_with_subs_default = Result_with_subs;

// svelte/filters.svelte
function get_each_context3(ctx, list, i) {
  const child_ctx = ctx.slice();
  child_ctx[10] = list[i][0];
  child_ctx[11] = list[i][1];
  child_ctx[12] = list;
  child_ctx[13] = i;
  return child_ctx;
}
function get_each_context_12(ctx, list, i) {
  const child_ctx = ctx.slice();
  child_ctx[14] = list[i][0];
  child_ctx[15] = list[i][1];
  child_ctx[16] = list;
  child_ctx[17] = i;
  return child_ctx;
}
function create_if_block3(ctx) {
  let fieldset;
  let legend;
  let t0_value = (
    /*translate*/
    ctx[4](
      "filters_label",
      /*automatic_translations*/
      ctx[5],
      /*translations*/
      ctx[6]
    ) + ""
  );
  let t0;
  let t1;
  let each_value = Object.entries(
    /*available_filters*/
    ctx[1]
  );
  let each_blocks = [];
  for (let i = 0; i < each_value.length; i += 1) {
    each_blocks[i] = create_each_block3(get_each_context3(ctx, each_value, i));
  }
  return {
    c() {
      fieldset = element("fieldset");
      legend = element("legend");
      t0 = text(t0_value);
      t1 = space();
      for (let i = 0; i < each_blocks.length; i += 1) {
        each_blocks[i].c();
      }
      attr(legend, "class", "pagefind-ui__filter-panel-label svelte-1v2r7ls");
      attr(fieldset, "class", "pagefind-ui__filter-panel svelte-1v2r7ls");
    },
    m(target, anchor) {
      insert(target, fieldset, anchor);
      append(fieldset, legend);
      append(legend, t0);
      append(fieldset, t1);
      for (let i = 0; i < each_blocks.length; i += 1) {
        if (each_blocks[i]) {
          each_blocks[i].m(fieldset, null);
        }
      }
    },
    p(ctx2, dirty) {
      if (dirty & /*translate, automatic_translations, translations*/
      112 && t0_value !== (t0_value = /*translate*/
      ctx2[4](
        "filters_label",
        /*automatic_translations*/
        ctx2[5],
        /*translations*/
        ctx2[6]
      ) + ""))
        set_data(t0, t0_value);
      if (dirty & /*default_open, open_filters, Object, available_filters, selected_filters, show_empty_filters*/
      143) {
        each_value = Object.entries(
          /*available_filters*/
          ctx2[1]
        );
        let i;
        for (i = 0; i < each_value.length; i += 1) {
          const child_ctx = get_each_context3(ctx2, each_value, i);
          if (each_blocks[i]) {
            each_blocks[i].p(child_ctx, dirty);
          } else {
            each_blocks[i] = create_each_block3(child_ctx);
            each_blocks[i].c();
            each_blocks[i].m(fieldset, null);
          }
        }
        for (; i < each_blocks.length; i += 1) {
          each_blocks[i].d(1);
        }
        each_blocks.length = each_value.length;
      }
    },
    d(detaching) {
      if (detaching)
        detach(fieldset);
      destroy_each(each_blocks, detaching);
    }
  };
}
function create_if_block_13(ctx) {
  let div;
  let input;
  let input_id_value;
  let input_name_value;
  let input_value_value;
  let t0;
  let label;
  let html_tag;
  let raw_value = (
    /*value*/
    ctx[14] + ""
  );
  let t1;
  let t2_value = (
    /*count*/
    ctx[15] + ""
  );
  let t2;
  let t3;
  let label_for_value;
  let t4;
  let mounted;
  let dispose;
  function input_change_handler() {
    ctx[9].call(
      input,
      /*filter*/
      ctx[10],
      /*value*/
      ctx[14]
    );
  }
  return {
    c() {
      div = element("div");
      input = element("input");
      t0 = space();
      label = element("label");
      html_tag = new HtmlTag(false);
      t1 = text(" (");
      t2 = text(t2_value);
      t3 = text(")");
      t4 = space();
      attr(input, "class", "pagefind-ui__filter-checkbox svelte-1v2r7ls");
      attr(input, "type", "checkbox");
      attr(input, "id", input_id_value = /*filter*/
      ctx[10] + "-" + /*value*/
      ctx[14]);
      attr(input, "name", input_name_value = /*filter*/
      ctx[10]);
      input.__value = input_value_value = /*value*/
      ctx[14];
      input.value = input.__value;
      html_tag.a = t1;
      attr(label, "class", "pagefind-ui__filter-label svelte-1v2r7ls");
      attr(label, "for", label_for_value = /*filter*/
      ctx[10] + "-" + /*value*/
      ctx[14]);
      attr(div, "class", "pagefind-ui__filter-value svelte-1v2r7ls");
      toggle_class(
        div,
        "pagefind-ui__filter-value--checked",
        /*selected_filters*/
        ctx[0][`${/*filter*/
        ctx[10]}:${/*value*/
        ctx[14]}`]
      );
    },
    m(target, anchor) {
      insert(target, div, anchor);
      append(div, input);
      input.checked = /*selected_filters*/
      ctx[0][`${/*filter*/
      ctx[10]}:${/*value*/
      ctx[14]}`];
      append(div, t0);
      append(div, label);
      html_tag.m(raw_value, label);
      append(label, t1);
      append(label, t2);
      append(label, t3);
      append(div, t4);
      if (!mounted) {
        dispose = listen(input, "change", input_change_handler);
        mounted = true;
      }
    },
    p(new_ctx, dirty) {
      ctx = new_ctx;
      if (dirty & /*available_filters*/
      2 && input_id_value !== (input_id_value = /*filter*/
      ctx[10] + "-" + /*value*/
      ctx[14])) {
        attr(input, "id", input_id_value);
      }
      if (dirty & /*available_filters*/
      2 && input_name_value !== (input_name_value = /*filter*/
      ctx[10])) {
        attr(input, "name", input_name_value);
      }
      if (dirty & /*available_filters*/
      2 && input_value_value !== (input_value_value = /*value*/
      ctx[14])) {
        input.__value = input_value_value;
        input.value = input.__value;
      }
      if (dirty & /*selected_filters, Object, available_filters*/
      3) {
        input.checked = /*selected_filters*/
        ctx[0][`${/*filter*/
        ctx[10]}:${/*value*/
        ctx[14]}`];
      }
      if (dirty & /*available_filters*/
      2 && raw_value !== (raw_value = /*value*/
      ctx[14] + ""))
        html_tag.p(raw_value);
      if (dirty & /*available_filters*/
      2 && t2_value !== (t2_value = /*count*/
      ctx[15] + ""))
        set_data(t2, t2_value);
      if (dirty & /*available_filters*/
      2 && label_for_value !== (label_for_value = /*filter*/
      ctx[10] + "-" + /*value*/
      ctx[14])) {
        attr(label, "for", label_for_value);
      }
      if (dirty & /*selected_filters, Object, available_filters*/
      3) {
        toggle_class(
          div,
          "pagefind-ui__filter-value--checked",
          /*selected_filters*/
          ctx[0][`${/*filter*/
          ctx[10]}:${/*value*/
          ctx[14]}`]
        );
      }
    },
    d(detaching) {
      if (detaching)
        detach(div);
      mounted = false;
      dispose();
    }
  };
}
function create_each_block_12(ctx) {
  let if_block_anchor;
  let if_block = (
    /*show_empty_filters*/
    (ctx[2] || /*count*/
    ctx[15] || /*selected_filters*/
    ctx[0][`${/*filter*/
    ctx[10]}:${/*value*/
    ctx[14]}`]) && create_if_block_13(ctx)
  );
  return {
    c() {
      if (if_block)
        if_block.c();
      if_block_anchor = empty();
    },
    m(target, anchor) {
      if (if_block)
        if_block.m(target, anchor);
      insert(target, if_block_anchor, anchor);
    },
    p(ctx2, dirty) {
      if (
        /*show_empty_filters*/
        ctx2[2] || /*count*/
        ctx2[15] || /*selected_filters*/
        ctx2[0][`${/*filter*/
        ctx2[10]}:${/*value*/
        ctx2[14]}`]
      ) {
        if (if_block) {
          if_block.p(ctx2, dirty);
        } else {
          if_block = create_if_block_13(ctx2);
          if_block.c();
          if_block.m(if_block_anchor.parentNode, if_block_anchor);
        }
      } else if (if_block) {
        if_block.d(1);
        if_block = null;
      }
    },
    d(detaching) {
      if (if_block)
        if_block.d(detaching);
      if (detaching)
        detach(if_block_anchor);
    }
  };
}
function create_each_block3(ctx) {
  let details;
  let summary;
  let raw0_value = (
    /*filter*/
    ctx[10].replace(/^(\w)/, func3) + ""
  );
  let t0;
  let fieldset;
  let legend;
  let raw1_value = (
    /*filter*/
    ctx[10] + ""
  );
  let t1;
  let t2;
  let details_open_value;
  let each_value_1 = Object.entries(
    /*values*/
    ctx[11] || {}
  );
  let each_blocks = [];
  for (let i = 0; i < each_value_1.length; i += 1) {
    each_blocks[i] = create_each_block_12(get_each_context_12(ctx, each_value_1, i));
  }
  return {
    c() {
      details = element("details");
      summary = element("summary");
      t0 = space();
      fieldset = element("fieldset");
      legend = element("legend");
      t1 = space();
      for (let i = 0; i < each_blocks.length; i += 1) {
        each_blocks[i].c();
      }
      t2 = space();
      attr(summary, "class", "pagefind-ui__filter-name svelte-1v2r7ls");
      attr(legend, "class", "pagefind-ui__filter-group-label svelte-1v2r7ls");
      attr(fieldset, "class", "pagefind-ui__filter-group svelte-1v2r7ls");
      attr(details, "class", "pagefind-ui__filter-block svelte-1v2r7ls");
      details.open = details_open_value = /*default_open*/
      ctx[7] || /*open_filters*/
      ctx[3].map(func_1).includes(
        /*filter*/
        ctx[10].toLowerCase()
      );
    },
    m(target, anchor) {
      insert(target, details, anchor);
      append(details, summary);
      summary.innerHTML = raw0_value;
      append(details, t0);
      append(details, fieldset);
      append(fieldset, legend);
      legend.innerHTML = raw1_value;
      append(fieldset, t1);
      for (let i = 0; i < each_blocks.length; i += 1) {
        if (each_blocks[i]) {
          each_blocks[i].m(fieldset, null);
        }
      }
      append(details, t2);
    },
    p(ctx2, dirty) {
      if (dirty & /*available_filters*/
      2 && raw0_value !== (raw0_value = /*filter*/
      ctx2[10].replace(/^(\w)/, func3) + ""))
        summary.innerHTML = raw0_value;
      ;
      if (dirty & /*available_filters*/
      2 && raw1_value !== (raw1_value = /*filter*/
      ctx2[10] + ""))
        legend.innerHTML = raw1_value;
      ;
      if (dirty & /*selected_filters, Object, available_filters, show_empty_filters*/
      7) {
        each_value_1 = Object.entries(
          /*values*/
          ctx2[11] || {}
        );
        let i;
        for (i = 0; i < each_value_1.length; i += 1) {
          const child_ctx = get_each_context_12(ctx2, each_value_1, i);
          if (each_blocks[i]) {
            each_blocks[i].p(child_ctx, dirty);
          } else {
            each_blocks[i] = create_each_block_12(child_ctx);
            each_blocks[i].c();
            each_blocks[i].m(fieldset, null);
          }
        }
        for (; i < each_blocks.length; i += 1) {
          each_blocks[i].d(1);
        }
        each_blocks.length = each_value_1.length;
      }
      if (dirty & /*default_open, open_filters, available_filters*/
      138 && details_open_value !== (details_open_value = /*default_open*/
      ctx2[7] || /*open_filters*/
      ctx2[3].map(func_1).includes(
        /*filter*/
        ctx2[10].toLowerCase()
      ))) {
        details.open = details_open_value;
      }
    },
    d(detaching) {
      if (detaching)
        detach(details);
      destroy_each(each_blocks, detaching);
    }
  };
}
function create_fragment3(ctx) {
  let show_if = (
    /*available_filters*/
    ctx[1] && Object.entries(
      /*available_filters*/
      ctx[1]
    ).length
  );
  let if_block_anchor;
  let if_block = show_if && create_if_block3(ctx);
  return {
    c() {
      if (if_block)
        if_block.c();
      if_block_anchor = empty();
    },
    m(target, anchor) {
      if (if_block)
        if_block.m(target, anchor);
      insert(target, if_block_anchor, anchor);
    },
    p(ctx2, [dirty]) {
      if (dirty & /*available_filters*/
      2)
        show_if = /*available_filters*/
        ctx2[1] && Object.entries(
          /*available_filters*/
          ctx2[1]
        ).length;
      if (show_if) {
        if (if_block) {
          if_block.p(ctx2, dirty);
        } else {
          if_block = create_if_block3(ctx2);
          if_block.c();
          if_block.m(if_block_anchor.parentNode, if_block_anchor);
        }
      } else if (if_block) {
        if_block.d(1);
        if_block = null;
      }
    },
    i: noop,
    o: noop,
    d(detaching) {
      if (if_block)
        if_block.d(detaching);
      if (detaching)
        detach(if_block_anchor);
    }
  };
}
var func3 = (c) => c.toLocaleUpperCase();
var func_1 = (f) => f.toLowerCase();
function instance3($$self, $$props, $$invalidate) {
  let { available_filters = null } = $$props;
  let { show_empty_filters = true } = $$props;
  let { open_filters = [] } = $$props;
  let { translate = () => "" } = $$props;
  let { automatic_translations = {} } = $$props;
  let { translations = {} } = $$props;
  let { selected_filters = {} } = $$props;
  let initialized = false;
  let default_open = false;
  function input_change_handler(filter, value) {
    selected_filters[`${filter}:${value}`] = this.checked;
    $$invalidate(0, selected_filters);
  }
  $$self.$$set = ($$props2) => {
    if ("available_filters" in $$props2)
      $$invalidate(1, available_filters = $$props2.available_filters);
    if ("show_empty_filters" in $$props2)
      $$invalidate(2, show_empty_filters = $$props2.show_empty_filters);
    if ("open_filters" in $$props2)
      $$invalidate(3, open_filters = $$props2.open_filters);
    if ("translate" in $$props2)
      $$invalidate(4, translate = $$props2.translate);
    if ("automatic_translations" in $$props2)
      $$invalidate(5, automatic_translations = $$props2.automatic_translations);
    if ("translations" in $$props2)
      $$invalidate(6, translations = $$props2.translations);
    if ("selected_filters" in $$props2)
      $$invalidate(0, selected_filters = $$props2.selected_filters);
  };
  $$self.$$.update = () => {
    if ($$self.$$.dirty & /*available_filters, initialized*/
    258) {
      $:
        if (available_filters && !initialized) {
          $$invalidate(8, initialized = true);
          let filters = Object.entries(available_filters || {});
          if (filters.length === 1) {
            let values = Object.entries(filters[0][1]);
            if (values?.length <= 6) {
              $$invalidate(7, default_open = true);
            }
          }
        }
    }
  };
  return [
    selected_filters,
    available_filters,
    show_empty_filters,
    open_filters,
    translate,
    automatic_translations,
    translations,
    default_open,
    initialized,
    input_change_handler
  ];
}
var Filters = class extends SvelteComponent {
  constructor(options) {
    super();
    init(this, options, instance3, create_fragment3, safe_not_equal, {
      available_filters: 1,
      show_empty_filters: 2,
      open_filters: 3,
      translate: 4,
      automatic_translations: 5,
      translations: 6,
      selected_filters: 0
    });
  }
};
var filters_default = Filters;

// ../translations/af.json
var af_exports = {};
__export(af_exports, {
  comments: () => comments,
  default: () => af_default,
  direction: () => direction,
  strings: () => strings,
  thanks_to: () => thanks_to
});
var thanks_to = "Jan Claasen <jan@cloudcannon.com>";
var comments = "";
var direction = "ltr";
var strings = {
  placeholder: "Soek",
  clear_search: "Opruim",
  load_more: "Laai nog resultate",
  search_label: "Soek hierdie webwerf",
  filters_label: "Filters",
  zero_results: "Geen resultate vir [SEARCH_TERM]",
  many_results: "[COUNT] resultate vir [SEARCH_TERM]",
  one_result: "[COUNT] resultate vir [SEARCH_TERM]",
  alt_search: "Geen resultate vir [SEARCH_TERM]. Toon resultate vir [DIFFERENT_TERM] in plaas daarvan",
  search_suggestion: "Geen resultate vir [SEARCH_TERM]. Probeer eerder een van die volgende terme:",
  searching: "Soek vir [SEARCH_TERM]"
};
var af_default = {
  thanks_to,
  comments,
  direction,
  strings
};

// ../translations/ar.json
var ar_exports = {};
__export(ar_exports, {
  comments: () => comments2,
  default: () => ar_default,
  direction: () => direction2,
  strings: () => strings2,
  thanks_to: () => thanks_to2
});
var thanks_to2 = "Jermanuts";
var comments2 = "";
var direction2 = "rtl";
var strings2 = {
  placeholder: "\u0628\u062D\u062B",
  clear_search: "\u0627\u0645\u0633\u062D",
  load_more: "\u062D\u0645\u0651\u0650\u0644 \u0627\u0644\u0645\u0632\u064A\u062F \u0645\u0646 \u0627\u0644\u0646\u062A\u0627\u0626\u062C",
  search_label: "\u0627\u0628\u062D\u062B \u0641\u064A \u0647\u0630\u0627 \u0627\u0644\u0645\u0648\u0642\u0639",
  filters_label: "\u062A\u0635\u0641\u064A\u0627\u062A",
  zero_results: "\u0644\u0627 \u062A\u0648\u062C\u062F \u0646\u062A\u0627\u0626\u062C \u0644 [SEARCH_TERM]",
  many_results: "[COUNT] \u0646\u062A\u0627\u0626\u062C \u0644 [SEARCH_TERM]",
  one_result: "[COUNT] \u0646\u062A\u064A\u062C\u0629 \u0644 [SEARCH_TERM]",
  alt_search: "\u0644\u0627 \u062A\u0648\u062C\u062F \u0646\u062A\u0627\u0626\u062C \u0644 [SEARCH_TERM]. \u064A\u0639\u0631\u0636 \u0627\u0644\u0646\u062A\u0627\u0626\u062C \u0644 [DIFFERENT_TERM] \u0628\u062F\u0644\u0627\u064B \u0645\u0646 \u0630\u0644\u0643",
  search_suggestion: "\u0644\u0627 \u062A\u0648\u062C\u062F \u0646\u062A\u0627\u0626\u062C \u0644 [SEARCH_TERM]. \u062C\u0631\u0628 \u0623\u062D\u062F \u0639\u0645\u0644\u064A\u0627\u062A \u0627\u0644\u0628\u062D\u062B \u0627\u0644\u062A\u0627\u0644\u064A\u0629:",
  searching: "\u064A\u0628\u062D\u062B \u0639\u0646 [SEARCH_TERM]..."
};
var ar_default = {
  thanks_to: thanks_to2,
  comments: comments2,
  direction: direction2,
  strings: strings2
};

// ../translations/bn.json
var bn_exports = {};
__export(bn_exports, {
  comments: () => comments3,
  default: () => bn_default,
  direction: () => direction3,
  strings: () => strings3,
  thanks_to: () => thanks_to3
});
var thanks_to3 = "Maruf Alom <mail@marufalom.com>";
var comments3 = "";
var direction3 = "ltr";
var strings3 = {
  placeholder: "\u0985\u09A8\u09C1\u09B8\u09A8\u09CD\u09A7\u09BE\u09A8 \u0995\u09B0\u09C1\u09A8",
  clear_search: "\u09AE\u09C1\u099B\u09C7 \u09AB\u09C7\u09B2\u09C1\u09A8",
  load_more: "\u0986\u09B0\u09CB \u09AB\u09B2\u09BE\u09AB\u09B2 \u09A6\u09C7\u0996\u09C1\u09A8",
  search_label: "\u098F\u0987 \u0993\u09DF\u09C7\u09AC\u09B8\u09BE\u0987\u099F\u09C7 \u0985\u09A8\u09C1\u09B8\u09A8\u09CD\u09A7\u09BE\u09A8 \u0995\u09B0\u09C1\u09A8",
  filters_label: "\u09AB\u09BF\u09B2\u09CD\u099F\u09BE\u09B0",
  zero_results: "[SEARCH_TERM] \u098F\u09B0 \u099C\u09A8\u09CD\u09AF \u0995\u09BF\u099B\u09C1 \u0996\u09C1\u0981\u099C\u09C7 \u09AA\u09BE\u0993\u09DF\u09BE \u09AF\u09BE\u09DF\u09A8\u09BF",
  many_results: "[COUNT]-\u099F\u09BF \u09AB\u09B2\u09BE\u09AB\u09B2 \u09AA\u09BE\u0993\u09DF\u09BE \u0997\u09BF\u09DF\u09C7\u099B\u09C7 [SEARCH_TERM] \u098F\u09B0 \u099C\u09A8\u09CD\u09AF",
  one_result: "[COUNT]-\u099F\u09BF \u09AB\u09B2\u09BE\u09AB\u09B2 \u09AA\u09BE\u0993\u09DF\u09BE \u0997\u09BF\u09DF\u09C7\u099B\u09C7 [SEARCH_TERM] \u098F\u09B0 \u099C\u09A8\u09CD\u09AF",
  alt_search: "\u0995\u09CB\u09A8 \u0995\u09BF\u099B\u09C1 \u0996\u09C1\u0981\u099C\u09C7 \u09AA\u09BE\u0993\u09DF\u09BE \u09AF\u09BE\u09DF\u09A8\u09BF [SEARCH_TERM] \u098F\u09B0 \u099C\u09A8\u09CD\u09AF. \u09AA\u09B0\u09BF\u09AC\u09B0\u09CD\u09A4\u09C7 [DIFFERENT_TERM] \u098F\u09B0 \u099C\u09A8\u09CD\u09AF \u09A6\u09C7\u0996\u09BE\u09A8\u09CB \u09B9\u099A\u09CD\u099B\u09C7",
  search_suggestion: "\u0995\u09CB\u09A8 \u0995\u09BF\u099B\u09C1 \u0996\u09C1\u0981\u099C\u09C7 \u09AA\u09BE\u0993\u09DF\u09BE \u09AF\u09BE\u09DF\u09A8\u09BF [SEARCH_TERM] \u098F\u09B0 \u09AC\u09BF\u09B7\u09DF\u09C7. \u09A8\u09BF\u09A8\u09CD\u09AE\u09C7\u09B0 \u09AC\u09BF\u09B7\u09DF\u09AC\u09B8\u09CD\u09A4\u09C1 \u0996\u09C1\u0981\u099C\u09C7 \u09A6\u09C7\u0996\u09C1\u09A8:",
  searching: "\u0985\u09A8\u09C1\u09B8\u09A8\u09CD\u09A7\u09BE\u09A8 \u099A\u09B2\u099B\u09C7 [SEARCH_TERM]..."
};
var bn_default = {
  thanks_to: thanks_to3,
  comments: comments3,
  direction: direction3,
  strings: strings3
};

// ../translations/ca.json
var ca_exports = {};
__export(ca_exports, {
  comments: () => comments4,
  default: () => ca_default,
  direction: () => direction4,
  strings: () => strings4,
  thanks_to: () => thanks_to4
});
var thanks_to4 = "Pablo Villaverde <https://github.com/pvillaverde>";
var comments4 = "";
var direction4 = "ltr";
var strings4 = {
  placeholder: "Cerca",
  clear_search: "Netejar",
  load_more: "Veure m\xE9s resultats",
  search_label: "Cerca en aquest lloc",
  filters_label: "Filtres",
  zero_results: "No es van trobar resultats per [SEARCH_TERM]",
  many_results: "[COUNT] resultats trobats per [SEARCH_TERM]",
  one_result: "[COUNT] resultat trobat per [SEARCH_TERM]",
  alt_search: "No es van trobar resultats per [SEARCH_TERM]. Mostrant al seu lloc resultats per [DIFFERENT_TERM]",
  search_suggestion: "No es van trobar resultats per [SEARCH_TERM]. Proveu una de les cerques seg\xFCents:",
  searching: "Cercant [SEARCH_TERM]..."
};
var ca_default = {
  thanks_to: thanks_to4,
  comments: comments4,
  direction: direction4,
  strings: strings4
};

// ../translations/cs.json
var cs_exports = {};
__export(cs_exports, {
  comments: () => comments5,
  default: () => cs_default,
  direction: () => direction5,
  strings: () => strings5,
  thanks_to: () => thanks_to5
});
var thanks_to5 = "Dalibor Hon <https://github.com/dallyh>";
var comments5 = "";
var direction5 = "ltr";
var strings5 = {
  placeholder: "Hledat",
  clear_search: "Smazat",
  load_more: "Na\u010D\xEDst dal\u0161\xED v\xFDsledky",
  search_label: "Prohledat tuto str\xE1nku",
  filters_label: "Filtry",
  zero_results: "\u017D\xE1dn\xE9 v\xFDsledky pro [SEARCH_TERM]",
  many_results: "[COUNT] v\xFDsledk\u016F pro [SEARCH_TERM]",
  one_result: "[COUNT] v\xFDsledek pro [SEARCH_TERM]",
  alt_search: "\u017D\xE1dn\xE9 v\xFDsledky pro [SEARCH_TERM]. Zobrazuj\xED se v\xFDsledky pro [DIFFERENT_TERM]",
  search_suggestion: "\u017D\xE1dn\xE9 v\xFDsledky pro [SEARCH_TERM]. Souvisej\xEDc\xED v\xFDsledky hled\xE1n\xED:",
  searching: "Hled\xE1m [SEARCH_TERM]..."
};
var cs_default = {
  thanks_to: thanks_to5,
  comments: comments5,
  direction: direction5,
  strings: strings5
};

// ../translations/da.json
var da_exports = {};
__export(da_exports, {
  comments: () => comments6,
  default: () => da_default,
  direction: () => direction6,
  strings: () => strings6,
  thanks_to: () => thanks_to6
});
var thanks_to6 = "Jonas Smedegaard <dr@jones.dk>";
var comments6 = "";
var direction6 = "ltr";
var strings6 = {
  placeholder: "S\xF8g",
  clear_search: "Nulstil",
  load_more: "Indl\xE6s flere resultater",
  search_label: "S\xF8g p\xE5 dette website",
  filters_label: "Filtre",
  zero_results: "Ingen resultater for [SEARCH_TERM]",
  many_results: "[COUNT] resultater for [SEARCH_TERM]",
  one_result: "[COUNT] resultat for [SEARCH_TERM]",
  alt_search: "Ingen resultater for [SEARCH_TERM]. Viser resultater for [DIFFERENT_TERM] i stedet",
  search_suggestion: "Ingen resultater for [SEARCH_TERM]. Pr\xF8v et af disse s\xF8geord i stedet:",
  searching: "S\xF8ger efter [SEARCH_TERM]..."
};
var da_default = {
  thanks_to: thanks_to6,
  comments: comments6,
  direction: direction6,
  strings: strings6
};

// ../translations/de.json
var de_exports = {};
__export(de_exports, {
  comments: () => comments7,
  default: () => de_default,
  direction: () => direction7,
  strings: () => strings7,
  thanks_to: () => thanks_to7
});
var thanks_to7 = "Jan Claasen <jan@cloudcannon.com>";
var comments7 = "";
var direction7 = "ltr";
var strings7 = {
  placeholder: "Suche",
  clear_search: "L\xF6schen",
  load_more: "Mehr Ergebnisse laden",
  search_label: "Suche diese Seite",
  filters_label: "Filter",
  zero_results: "Keine Ergebnisse f\xFCr [SEARCH_TERM]",
  many_results: "[COUNT] Ergebnisse f\xFCr [SEARCH_TERM]",
  one_result: "[COUNT] Ergebnis f\xFCr [SEARCH_TERM]",
  alt_search: "Keine Ergebnisse f\xFCr [SEARCH_TERM]. Stattdessen werden Ergebnisse f\xFCr [DIFFERENT_TERM] angezeigt",
  search_suggestion: "Keine Ergebnisse f\xFCr [SEARCH_TERM]. Versuchen Sie eine der folgenden Suchen:",
  searching: "Suche f\xFCr [SEARCH_TERM]"
};
var de_default = {
  thanks_to: thanks_to7,
  comments: comments7,
  direction: direction7,
  strings: strings7
};

// ../translations/en.json
var en_exports = {};
__export(en_exports, {
  comments: () => comments8,
  default: () => en_default,
  direction: () => direction8,
  strings: () => strings8,
  thanks_to: () => thanks_to8
});
var thanks_to8 = "Liam Bigelow <liam@cloudcannon.com>";
var comments8 = "";
var direction8 = "ltr";
var strings8 = {
  placeholder: "Search",
  clear_search: "Clear",
  load_more: "Load more results",
  search_label: "Search this site",
  filters_label: "Filters",
  zero_results: "No results for [SEARCH_TERM]",
  many_results: "[COUNT] results for [SEARCH_TERM]",
  one_result: "[COUNT] result for [SEARCH_TERM]",
  alt_search: "No results for [SEARCH_TERM]. Showing results for [DIFFERENT_TERM] instead",
  search_suggestion: "No results for [SEARCH_TERM]. Try one of the following searches:",
  searching: "Searching for [SEARCH_TERM]..."
};
var en_default = {
  thanks_to: thanks_to8,
  comments: comments8,
  direction: direction8,
  strings: strings8
};

// ../translations/es.json
var es_exports = {};
__export(es_exports, {
  comments: () => comments9,
  default: () => es_default,
  direction: () => direction9,
  strings: () => strings9,
  thanks_to: () => thanks_to9
});
var thanks_to9 = "Pablo Villaverde <https://github.com/pvillaverde>";
var comments9 = "";
var direction9 = "ltr";
var strings9 = {
  placeholder: "Buscar",
  clear_search: "Limpiar",
  load_more: "Ver m\xE1s resultados",
  search_label: "Buscar en este sitio",
  filters_label: "Filtros",
  zero_results: "No se encontraron resultados para [SEARCH_TERM]",
  many_results: "[COUNT] resultados encontrados para [SEARCH_TERM]",
  one_result: "[COUNT] resultado encontrado para [SEARCH_TERM]",
  alt_search: "No se encontraron resultados para [SEARCH_TERM]. Mostrando en su lugar resultados para [DIFFERENT_TERM]",
  search_suggestion: "No se encontraron resultados para [SEARCH_TERM]. Prueba una de las siguientes b\xFAsquedas:",
  searching: "Buscando [SEARCH_TERM]..."
};
var es_default = {
  thanks_to: thanks_to9,
  comments: comments9,
  direction: direction9,
  strings: strings9
};

// ../translations/fa.json
var fa_exports = {};
__export(fa_exports, {
  comments: () => comments10,
  default: () => fa_default,
  direction: () => direction10,
  strings: () => strings10,
  thanks_to: () => thanks_to10
});
var thanks_to10 = "Ali Khaleqi Yekta <https://yekta.dev>";
var comments10 = "";
var direction10 = "rtl";
var strings10 = {
  placeholder: "\u062C\u0633\u062A\u062C\u0648",
  clear_search: "\u067E\u0627\u06A9\u0633\u0627\u0632\u06CC",
  load_more: "\u0628\u0627\u0631\u06AF\u0630\u0627\u0631\u06CC \u0646\u062A\u0627\u06CC\u062C \u0628\u06CC\u0634\u062A\u0631",
  search_label: "\u062C\u0633\u062A\u062C\u0648 \u062F\u0631 \u0633\u0627\u06CC\u062A",
  filters_label: "\u0641\u06CC\u0644\u062A\u0631\u0647\u0627",
  zero_results: "\u0646\u062A\u06CC\u062C\u0647\u200C\u0627\u06CC \u0628\u0631\u0627\u06CC [SEARCH_TERM] \u06CC\u0627\u0641\u062A \u0646\u0634\u062F",
  many_results: "[COUNT] \u0646\u062A\u06CC\u062C\u0647 \u0628\u0631\u0627\u06CC [SEARCH_TERM] \u06CC\u0627\u0641\u062A \u0634\u062F",
  one_result: "[COUNT] \u0646\u062A\u06CC\u062C\u0647 \u0628\u0631\u0627\u06CC [SEARCH_TERM] \u06CC\u0627\u0641\u062A \u0634\u062F",
  alt_search: "\u0646\u062A\u06CC\u062C\u0647\u200C\u0627\u06CC \u0628\u0631\u0627\u06CC [SEARCH_TERM] \u06CC\u0627\u0641\u062A \u0646\u0634\u062F. \u062F\u0631 \u0639\u0648\u0636 \u0646\u062A\u0627\u06CC\u062C \u0628\u0631\u0627\u06CC [DIFFERENT_TERM] \u0646\u0645\u0627\u06CC\u0634 \u062F\u0627\u062F\u0647 \u0645\u06CC\u200C\u0634\u0648\u062F",
  search_suggestion: "\u0646\u062A\u06CC\u062C\u0647\u200C\u0627\u06CC \u0628\u0631\u0627\u06CC [SEARCH_TERM] \u06CC\u0627\u0641\u062A \u0646\u0634\u062F. \u06CC\u06A9\u06CC \u0627\u0632 \u062C\u0633\u062A\u062C\u0648\u0647\u0627\u06CC \u0632\u06CC\u0631 \u0631\u0627 \u0627\u0645\u062A\u062D\u0627\u0646 \u06A9\u0646\u06CC\u062F:",
  searching: "\u062F\u0631 \u062D\u0627\u0644 \u062C\u0633\u062A\u062C\u0648\u06CC [SEARCH_TERM]..."
};
var fa_default = {
  thanks_to: thanks_to10,
  comments: comments10,
  direction: direction10,
  strings: strings10
};

// ../translations/fi.json
var fi_exports = {};
__export(fi_exports, {
  comments: () => comments11,
  default: () => fi_default,
  direction: () => direction11,
  strings: () => strings11,
  thanks_to: () => thanks_to11
});
var thanks_to11 = "Valtteri Laitinen <dev@valtlai.fi>";
var comments11 = "";
var direction11 = "ltr";
var strings11 = {
  placeholder: "Haku",
  clear_search: "Tyhjenn\xE4",
  load_more: "Lataa lis\xE4\xE4 tuloksia",
  search_label: "Hae t\xE4lt\xE4 sivustolta",
  filters_label: "Suodattimet",
  zero_results: "Ei tuloksia haulle [SEARCH_TERM]",
  many_results: "[COUNT] tulosta haulle [SEARCH_TERM]",
  one_result: "[COUNT] tulos haulle [SEARCH_TERM]",
  alt_search: "Ei tuloksia haulle [SEARCH_TERM]. N\xE4ytet\xE4\xE4n tulokset sen sijaan haulle [DIFFERENT_TERM]",
  search_suggestion: "Ei tuloksia haulle [SEARCH_TERM]. Kokeile jotain seuraavista:",
  searching: "Haetaan [SEARCH_TERM]..."
};
var fi_default = {
  thanks_to: thanks_to11,
  comments: comments11,
  direction: direction11,
  strings: strings11
};

// ../translations/fr.json
var fr_exports = {};
__export(fr_exports, {
  comments: () => comments12,
  default: () => fr_default,
  direction: () => direction12,
  strings: () => strings12,
  thanks_to: () => thanks_to12
});
var thanks_to12 = "Nicolas Friedli <nicolas@theologique.ch>";
var comments12 = "";
var direction12 = "ltr";
var strings12 = {
  placeholder: "Rechercher",
  clear_search: "Nettoyer",
  load_more: "Charger plus de r\xE9sultats",
  search_label: "Recherche sur ce site",
  filters_label: "Filtres",
  zero_results: "Pas de r\xE9sultat pour [SEARCH_TERM]",
  many_results: "[COUNT] r\xE9sultats pour [SEARCH_TERM]",
  one_result: "[COUNT] r\xE9sultat pour [SEARCH_TERM]",
  alt_search: "Pas de r\xE9sultat pour [SEARCH_TERM]. Montre les r\xE9sultats pour [DIFFERENT_TERM] \xE0 la place",
  search_suggestion: "Pas de r\xE9sultat pour [SEARCH_TERM]. Essayer une des recherches suivantes:",
  searching: "Recherche [SEARCH_TERM]..."
};
var fr_default = {
  thanks_to: thanks_to12,
  comments: comments12,
  direction: direction12,
  strings: strings12
};

// ../translations/gl.json
var gl_exports = {};
__export(gl_exports, {
  comments: () => comments13,
  default: () => gl_default,
  direction: () => direction13,
  strings: () => strings13,
  thanks_to: () => thanks_to13
});
var thanks_to13 = "Pablo Villaverde <https://github.com/pvillaverde>";
var comments13 = "";
var direction13 = "ltr";
var strings13 = {
  placeholder: "Buscar",
  clear_search: "Limpar",
  load_more: "Ver m\xE1is resultados",
  search_label: "Buscar neste sitio",
  filters_label: "Filtros",
  zero_results: "Non se atoparon resultados para [SEARCH_TERM]",
  many_results: "[COUNT] resultados atopados para [SEARCH_TERM]",
  one_result: "[COUNT] resultado atopado para [SEARCH_TERM]",
  alt_search: "Non se atoparon resultados para [SEARCH_TERM]. Amosando no seu lugar resultados para [DIFFERENT_TERM]",
  search_suggestion: "Non se atoparon resultados para [SEARCH_TERM]. Probe unha das seguintes pesquisas:",
  searching: "Buscando [SEARCH_TERM]..."
};
var gl_default = {
  thanks_to: thanks_to13,
  comments: comments13,
  direction: direction13,
  strings: strings13
};

// ../translations/he.json
var he_exports = {};
__export(he_exports, {
  comments: () => comments14,
  default: () => he_default,
  direction: () => direction14,
  strings: () => strings14,
  thanks_to: () => thanks_to14
});
var thanks_to14 = "Nir Tamir <nirtamir2@gmail.com>";
var comments14 = "";
var direction14 = "rtl";
var strings14 = {
  placeholder: "\u05D7\u05D9\u05E4\u05D5\u05E9",
  clear_search: "\u05E0\u05D9\u05E7\u05D5\u05D9",
  load_more: "\u05E2\u05D5\u05D3 \u05EA\u05D5\u05E6\u05D0\u05D5\u05EA",
  search_label: "\u05D7\u05D9\u05E4\u05D5\u05E9 \u05D1\u05D0\u05EA\u05E8 \u05D6\u05D4",
  filters_label: "\u05DE\u05E1\u05E0\u05E0\u05D9\u05DD",
  zero_results: "\u05DC\u05D0 \u05E0\u05DE\u05E6\u05D0\u05D5 \u05EA\u05D5\u05E6\u05D0\u05D5\u05EA \u05E2\u05D1\u05D5\u05E8 [SEARCH_TERM]",
  many_results: "\u05E0\u05DE\u05E6\u05D0\u05D5 [COUNT] \u05EA\u05D5\u05E6\u05D0\u05D5\u05EA \u05E2\u05D1\u05D5\u05E8 [SEARCH_TERM]",
  one_result: "\u05E0\u05DE\u05E6\u05D0\u05D4 \u05EA\u05D5\u05E6\u05D0\u05D4 \u05D0\u05D7\u05EA \u05E2\u05D1\u05D5\u05E8 [SEARCH_TERM]",
  alt_search: "\u05DC\u05D0 \u05E0\u05DE\u05E6\u05D0\u05D5 \u05EA\u05D5\u05E6\u05D0\u05D5\u05EA \u05E2\u05D1\u05D5\u05E8 [SEARCH_TERM]. \u05DE\u05D5\u05E6\u05D2\u05D5\u05EA \u05EA\u05D5\u05E6\u05D0\u05D5\u05EA \u05E2\u05D1\u05D5\u05E8 [DIFFERENT_TERM]",
  search_suggestion: "\u05DC\u05D0 \u05E0\u05DE\u05E6\u05D0\u05D5 \u05EA\u05D5\u05E6\u05D0\u05D5\u05EA \u05E2\u05D1\u05D5\u05E8 [SEARCH_TERM]. \u05E0\u05E1\u05D5 \u05D0\u05D7\u05D3 \u05DE\u05D4\u05D7\u05D9\u05E4\u05D5\u05E9\u05D9\u05DD \u05D4\u05D1\u05D0\u05D9\u05DD:",
  searching: "\u05DE\u05D7\u05E4\u05E9 \u05D0\u05EA [SEARCH_TERM]..."
};
var he_default = {
  thanks_to: thanks_to14,
  comments: comments14,
  direction: direction14,
  strings: strings14
};

// ../translations/hi.json
var hi_exports = {};
__export(hi_exports, {
  comments: () => comments15,
  default: () => hi_default,
  direction: () => direction15,
  strings: () => strings15,
  thanks_to: () => thanks_to15
});
var thanks_to15 = "Amit Yadav <amit@thetechbasket.com>";
var comments15 = "";
var direction15 = "ltr";
var strings15 = {
  placeholder: "\u0916\u094B\u091C\u0947\u0902",
  clear_search: "\u0938\u093E\u092B \u0915\u0930\u0947\u0902",
  load_more: "\u0914\u0930 \u0905\u0927\u093F\u0915 \u092A\u0930\u093F\u0923\u093E\u092E \u0932\u094B\u0921 \u0915\u0930\u0947\u0902",
  search_label: "\u0907\u0938 \u0938\u093E\u0907\u091F \u092E\u0947\u0902 \u0916\u094B\u091C\u0947\u0902",
  filters_label: "\u092B\u093C\u093F\u0932\u094D\u091F\u0930",
  zero_results: "\u0915\u094B\u0908 \u092A\u0930\u093F\u0923\u093E\u092E [SEARCH_TERM] \u0915\u0947 \u0932\u093F\u090F \u0928\u0939\u0940\u0902 \u092E\u093F\u0932\u093E",
  many_results: "[COUNT] \u092A\u0930\u093F\u0923\u093E\u092E [SEARCH_TERM] \u0915\u0947 \u0932\u093F\u090F \u092E\u093F\u0932\u0947",
  one_result: "[COUNT] \u092A\u0930\u093F\u0923\u093E\u092E [SEARCH_TERM] \u0915\u0947 \u0932\u093F\u090F \u092E\u093F\u0932\u093E",
  alt_search: "[SEARCH_TERM] \u0915\u0947 \u0932\u093F\u090F \u0915\u094B\u0908 \u092A\u0930\u093F\u0923\u093E\u092E \u0928\u0939\u0940\u0902 \u092E\u093F\u0932\u093E\u0964 \u0907\u0938\u0915\u0947 \u092C\u091C\u093E\u092F [DIFFERENT_TERM] \u0915\u0947 \u0932\u093F\u090F \u092A\u0930\u093F\u0923\u093E\u092E \u0926\u093F\u0916\u093E \u0930\u0939\u093E \u0939\u0948",
  search_suggestion: "[SEARCH_TERM] \u0915\u0947 \u0932\u093F\u090F \u0915\u094B\u0908 \u092A\u0930\u093F\u0923\u093E\u092E \u0928\u0939\u0940\u0902 \u092E\u093F\u0932\u093E\u0964 \u0928\u093F\u092E\u094D\u0928\u0932\u093F\u0916\u093F\u0924 \u0916\u094B\u091C\u094B\u0902 \u092E\u0947\u0902 \u0938\u0947 \u0915\u094B\u0908 \u090F\u0915 \u0906\u091C\u093C\u092E\u093E\u090F\u0902:",
  searching: "[SEARCH_TERM] \u0915\u0940 \u0916\u094B\u091C \u0915\u0940 \u091C\u093E \u0930\u0939\u0940 \u0939\u0948..."
};
var hi_default = {
  thanks_to: thanks_to15,
  comments: comments15,
  direction: direction15,
  strings: strings15
};

// ../translations/hr.json
var hr_exports = {};
__export(hr_exports, {
  comments: () => comments16,
  default: () => hr_default,
  direction: () => direction16,
  strings: () => strings16,
  thanks_to: () => thanks_to16
});
var thanks_to16 = "Diomed <https://github.com/diomed>";
var comments16 = "";
var direction16 = "ltr";
var strings16 = {
  placeholder: "Tra\u017Ei",
  clear_search: "O\u010Disti",
  load_more: "U\u010Ditaj vi\u0161e rezultata",
  search_label: "Pretra\u017Ei ovu stranicu",
  filters_label: "Filteri",
  zero_results: "Nema rezultata za [SEARCH_TERM]",
  many_results: "[COUNT] rezultata za [SEARCH_TERM]",
  one_result: "[COUNT] rezultat za [SEARCH_TERM]",
  alt_search: "Nema rezultata za [SEARCH_TERM]. Prikazujem rezultate za [DIFFERENT_TERM]",
  search_suggestion: "Nema rezultata za [SEARCH_TERM]. Poku\u0161aj s jednom od ovih pretraga:",
  searching: "Pretra\u017Eujem [SEARCH_TERM]..."
};
var hr_default = {
  thanks_to: thanks_to16,
  comments: comments16,
  direction: direction16,
  strings: strings16
};

// ../translations/hu.json
var hu_exports = {};
__export(hu_exports, {
  comments: () => comments17,
  default: () => hu_default,
  direction: () => direction17,
  strings: () => strings17,
  thanks_to: () => thanks_to17
});
var thanks_to17 = "Adam Laki <info@adamlaki.com>";
var comments17 = "";
var direction17 = "ltr";
var strings17 = {
  placeholder: "Keres\xE9s",
  clear_search: "T\xF6rl\xE9s",
  load_more: "Tov\xE1bbi tal\xE1latok bet\xF6lt\xE9se",
  search_label: "Keres\xE9s az oldalon",
  filters_label: "Sz\u0171r\xE9s",
  zero_results: "Nincs tal\xE1lat a(z) [SEARCH_TERM] kifejez\xE9sre",
  many_results: "[COUNT] db tal\xE1lat a(z) [SEARCH_TERM] kifejez\xE9sre",
  one_result: "[COUNT] db tal\xE1lat a(z) [SEARCH_TERM] kifejez\xE9sre",
  alt_search: "Nincs tal\xE1lat a(z) [SEARCH_TERM] kifejez\xE9sre. Tal\xE1latok mutat\xE1sa ink\xE1bb a(z) [DIFFERENT_TERM] kifejez\xE9sre",
  search_suggestion: "Nincs tal\xE1lat a(z) [SEARCH_TERM] kifejez\xE9sre. Pr\xF3b\xE1ld meg a k\xF6vetkez\u0151 keres\xE9sek egyik\xE9t:",
  searching: "Keres\xE9s a(z) [SEARCH_TERM] kifejez\xE9sre..."
};
var hu_default = {
  thanks_to: thanks_to17,
  comments: comments17,
  direction: direction17,
  strings: strings17
};

// ../translations/id.json
var id_exports = {};
__export(id_exports, {
  comments: () => comments18,
  default: () => id_default,
  direction: () => direction18,
  strings: () => strings18,
  thanks_to: () => thanks_to18
});
var thanks_to18 = "Nixentric";
var comments18 = "";
var direction18 = "ltr";
var strings18 = {
  placeholder: "Cari",
  clear_search: "Bersihkan",
  load_more: "Muat lebih banyak hasil",
  search_label: "Telusuri situs ini",
  filters_label: "Filter",
  zero_results: "[SEARCH_TERM] tidak ditemukan",
  many_results: "Ditemukan [COUNT] hasil untuk [SEARCH_TERM]",
  one_result: "Ditemukan [COUNT] hasil untuk [SEARCH_TERM]",
  alt_search: "[SEARCH_TERM] tidak ditemukan. Menampilkan hasil [DIFFERENT_TERM] sebagai gantinya",
  search_suggestion: "[SEARCH_TERM] tidak ditemukan. Coba salah satu pencarian berikut ini:",
  searching: "Mencari [SEARCH_TERM]..."
};
var id_default = {
  thanks_to: thanks_to18,
  comments: comments18,
  direction: direction18,
  strings: strings18
};

// ../translations/it.json
var it_exports = {};
__export(it_exports, {
  comments: () => comments19,
  default: () => it_default,
  direction: () => direction19,
  strings: () => strings19,
  thanks_to: () => thanks_to19
});
var thanks_to19 = "Cosette Bruhns Alonso, Andrew Janco <apjanco@upenn.edu>";
var comments19 = "";
var direction19 = "ltr";
var strings19 = {
  placeholder: "Cerca",
  clear_search: "Cancella la cronologia",
  load_more: "Mostra pi\xF9 risultati",
  search_label: "Cerca nel sito",
  filters_label: "Filtri di ricerca",
  zero_results: "Nessun risultato per [SEARCH_TERM]",
  many_results: "[COUNT] risultati per [SEARCH_TERM]",
  one_result: "[COUNT] risultato per [SEARCH_TERM]",
  alt_search: "Nessun risultato per [SEARCH_TERM]. Mostrando risultati per [DIFFERENT_TERM] come alternativa.",
  search_suggestion: "Nessun risultato per [SEARCH_TERM]. Prova una delle seguenti ricerche:",
  searching: "Cercando [SEARCH_TERM]..."
};
var it_default = {
  thanks_to: thanks_to19,
  comments: comments19,
  direction: direction19,
  strings: strings19
};

// ../translations/ja.json
var ja_exports = {};
__export(ja_exports, {
  comments: () => comments20,
  default: () => ja_default,
  direction: () => direction20,
  strings: () => strings20,
  thanks_to: () => thanks_to20
});
var thanks_to20 = "Tate";
var comments20 = "";
var direction20 = "ltr";
var strings20 = {
  placeholder: "\u691C\u7D22",
  clear_search: "\u30AF\u30EA\u30A2",
  load_more: "\u6B21\u3092\u8AAD\u307F\u8FBC\u3080",
  search_label: "\u3053\u306E\u30B5\u30A4\u30C8\u3092\u691C\u7D22",
  filters_label: "\u30D5\u30A3\u30EB\u30BF",
  zero_results: "[SEARCH_TERM]\u306E\u691C\u7D22\u306B\u4E00\u81F4\u3059\u308B\u60C5\u5831\u306F\u3042\u308A\u307E\u305B\u3093\u3067\u3057\u305F",
  many_results: "[SEARCH_TERM]\u306E[COUNT]\u4EF6\u306E\u691C\u7D22\u7D50\u679C",
  one_result: "[SEARCH_TERM]\u306E[COUNT]\u4EF6\u306E\u691C\u7D22\u7D50\u679C",
  alt_search: "[SEARCH_TERM]\u306E\u691C\u7D22\u306B\u4E00\u81F4\u3059\u308B\u60C5\u5831\u306F\u3042\u308A\u307E\u305B\u3093\u3067\u3057\u305F\u3002[DIFFERENT_TERM]\u306E\u691C\u7D22\u7D50\u679C\u3092\u8868\u793A\u3057\u3066\u3044\u307E\u3059",
  search_suggestion: "[SEARCH_TERM]\u306E\u691C\u7D22\u306B\u4E00\u81F4\u3059\u308B\u60C5\u5831\u306F\u3042\u308A\u307E\u305B\u3093\u3067\u3057\u305F\u3002\u6B21\u306E\u3044\u305A\u308C\u304B\u306E\u691C\u7D22\u3092\u8A66\u3057\u3066\u304F\u3060\u3055\u3044",
  searching: "[SEARCH_TERM]\u3092\u691C\u7D22\u3057\u3066\u3044\u307E\u3059"
};
var ja_default = {
  thanks_to: thanks_to20,
  comments: comments20,
  direction: direction20,
  strings: strings20
};

// ../translations/ko.json
var ko_exports = {};
__export(ko_exports, {
  comments: () => comments21,
  default: () => ko_default,
  direction: () => direction21,
  strings: () => strings21,
  thanks_to: () => thanks_to21
});
var thanks_to21 = "Seokho Son <https://github.com/seokho-son>";
var comments21 = "";
var direction21 = "ltr";
var strings21 = {
  placeholder: "\uAC80\uC0C9\uC5B4",
  clear_search: "\uBE44\uC6B0\uAE30",
  load_more: "\uAC80\uC0C9 \uACB0\uACFC \uB354 \uBCF4\uAE30",
  search_label: "\uC0AC\uC774\uD2B8 \uAC80\uC0C9",
  filters_label: "\uD544\uD130",
  zero_results: "[SEARCH_TERM]\uC5D0 \uB300\uD55C \uACB0\uACFC \uC5C6\uC74C",
  many_results: "[SEARCH_TERM]\uC5D0 \uB300\uD55C \uACB0\uACFC [COUNT]\uAC74",
  one_result: "[SEARCH_TERM]\uC5D0 \uB300\uD55C \uACB0\uACFC [COUNT]\uAC74",
  alt_search: "[SEARCH_TERM]\uC5D0 \uB300\uD55C \uACB0\uACFC \uC5C6\uC74C. [DIFFERENT_TERM]\uC5D0 \uB300\uD55C \uACB0\uACFC",
  search_suggestion: "[SEARCH_TERM]\uC5D0 \uB300\uD55C \uACB0\uACFC \uC5C6\uC74C. \uCD94\uCC9C \uAC80\uC0C9\uC5B4: ",
  searching: "[SEARCH_TERM] \uAC80\uC0C9 \uC911..."
};
var ko_default = {
  thanks_to: thanks_to21,
  comments: comments21,
  direction: direction21,
  strings: strings21
};

// ../translations/mi.json
var mi_exports = {};
__export(mi_exports, {
  comments: () => comments22,
  default: () => mi_default,
  direction: () => direction22,
  strings: () => strings22,
  thanks_to: () => thanks_to22
});
var thanks_to22 = "";
var comments22 = "";
var direction22 = "ltr";
var strings22 = {
  placeholder: "Rapu",
  clear_search: "Whakakore",
  load_more: "Whakauta \u0113tahi otinga k\u0113",
  search_label: "Rapu",
  filters_label: "T\u0101tari",
  zero_results: "Otinga kore ki [SEARCH_TERM]",
  many_results: "[COUNT] otinga ki [SEARCH_TERM]",
  one_result: "[COUNT] otinga ki [SEARCH_TERM]",
  alt_search: "Otinga kore ki [SEARCH_TERM]. Otinga k\u0113 ki [DIFFERENT_TERM]",
  search_suggestion: "Otinga kore ki [SEARCH_TERM]. whakam\u0101tau ki ng\u0101 mea atu:",
  searching: "Rapu ki [SEARCH_TERM]..."
};
var mi_default = {
  thanks_to: thanks_to22,
  comments: comments22,
  direction: direction22,
  strings: strings22
};

// ../translations/nl.json
var nl_exports = {};
__export(nl_exports, {
  comments: () => comments23,
  default: () => nl_default,
  direction: () => direction23,
  strings: () => strings23,
  thanks_to: () => thanks_to23
});
var thanks_to23 = "Paul van Brouwershaven";
var comments23 = "";
var direction23 = "ltr";
var strings23 = {
  placeholder: "Zoeken",
  clear_search: "Reset",
  load_more: "Meer resultaten laden",
  search_label: "Doorzoek deze site",
  filters_label: "Filters",
  zero_results: "Geen resultaten voor [SEARCH_TERM]",
  many_results: "[COUNT] resultaten voor [SEARCH_TERM]",
  one_result: "[COUNT] resultaat voor [SEARCH_TERM]",
  alt_search: "Geen resultaten voor [SEARCH_TERM]. In plaats daarvan worden resultaten voor [DIFFERENT_TERM] weergegeven",
  search_suggestion: "Geen resultaten voor [SEARCH_TERM]. Probeer een van de volgende zoekopdrachten:",
  searching: "Zoeken naar [SEARCH_TERM]..."
};
var nl_default = {
  thanks_to: thanks_to23,
  comments: comments23,
  direction: direction23,
  strings: strings23
};

// ../translations/no.json
var no_exports = {};
__export(no_exports, {
  comments: () => comments24,
  default: () => no_default,
  direction: () => direction24,
  strings: () => strings24,
  thanks_to: () => thanks_to24
});
var thanks_to24 = "Christopher Wingate";
var comments24 = "";
var direction24 = "ltr";
var strings24 = {
  placeholder: "S\xF8k",
  clear_search: "Fjern",
  load_more: "Last flere resultater",
  search_label: "S\xF8k p\xE5 denne siden",
  filters_label: "Filtre",
  zero_results: "Ingen resultater for [SEARCH_TERM]",
  many_results: "[COUNT] resultater for [SEARCH_TERM]",
  one_result: "[COUNT] resultat for [SEARCH_TERM]",
  alt_search: "Ingen resultater for [SEARCH_TERM]. Viser resultater for [DIFFERENT_TERM] i stedet",
  search_suggestion: "Ingen resultater for [SEARCH_TERM]. Pr\xF8v en av disse s\xF8keordene i stedet:",
  searching: "S\xF8ker etter [SEARCH_TERM]"
};
var no_default = {
  thanks_to: thanks_to24,
  comments: comments24,
  direction: direction24,
  strings: strings24
};

// ../translations/pl.json
var pl_exports = {};
__export(pl_exports, {
  comments: () => comments25,
  default: () => pl_default,
  direction: () => direction25,
  strings: () => strings25,
  thanks_to: () => thanks_to25
});
var thanks_to25 = "";
var comments25 = "";
var direction25 = "ltr";
var strings25 = {
  placeholder: "Szukaj",
  clear_search: "Wyczy\u015B\u0107",
  load_more: "Za\u0142aduj wi\u0119cej",
  search_label: "Przeszukaj t\u0119 stron\u0119",
  filters_label: "Filtry",
  zero_results: "Brak wynik\xF3w dla [SEARCH_TERM]",
  many_results: "[COUNT] wynik\xF3w dla [SEARCH_TERM]",
  one_result: "[COUNT] wynik dla [SEARCH_TERM]",
  alt_search: "Brak wynik\xF3w dla [SEARCH_TERM]. Wy\u015Bwietlam wyniki dla [DIFFERENT_TERM]",
  search_suggestion: "Brak wynik\xF3w dla [SEARCH_TERM]. Pokrewne wyniki wyszukiwania:",
  searching: "Szukam [SEARCH_TERM]..."
};
var pl_default = {
  thanks_to: thanks_to25,
  comments: comments25,
  direction: direction25,
  strings: strings25
};

// ../translations/pt.json
var pt_exports = {};
__export(pt_exports, {
  comments: () => comments26,
  default: () => pt_default,
  direction: () => direction26,
  strings: () => strings26,
  thanks_to: () => thanks_to26
});
var thanks_to26 = "Jonatah";
var comments26 = "";
var direction26 = "ltr";
var strings26 = {
  placeholder: "Pesquisar",
  clear_search: "Limpar",
  load_more: "Ver mais resultados",
  search_label: "Pesquisar",
  filters_label: "Filtros",
  zero_results: "Nenhum resultado encontrado para [SEARCH_TERM]",
  many_results: "[COUNT] resultados encontrados para [SEARCH_TERM]",
  one_result: "[COUNT] resultado encontrado para [SEARCH_TERM]",
  alt_search: "Nenhum resultado encontrado para [SEARCH_TERM]. Exibindo resultados para [DIFFERENT_TERM]",
  search_suggestion: "Nenhum resultado encontrado para [SEARCH_TERM]. Tente uma das seguintes pesquisas:",
  searching: "Pesquisando por [SEARCH_TERM]..."
};
var pt_default = {
  thanks_to: thanks_to26,
  comments: comments26,
  direction: direction26,
  strings: strings26
};

// ../translations/ro.json
var ro_exports = {};
__export(ro_exports, {
  comments: () => comments27,
  default: () => ro_default,
  direction: () => direction27,
  strings: () => strings27,
  thanks_to: () => thanks_to27
});
var thanks_to27 = "Bogdan Mateescu <bogdan@surfverse.com>";
var comments27 = "";
var direction27 = "ltr";
var strings27 = {
  placeholder: "C\u0103utare",
  clear_search: "\u015Eterge\u0163i",
  load_more: "\xCEnc\u0103rca\u021Bi mai multe rezultate",
  search_label: "C\u0103uta\u021Bi \xEEn acest site",
  filters_label: "Filtre",
  zero_results: "Niciun rezultat pentru [SEARCH_TERM]",
  many_results: "[COUNT] rezultate pentru [SEARCH_TERM]",
  one_result: "[COUNT] rezultat pentru [SEARCH_TERM]",
  alt_search: "Niciun rezultat pentru [SEARCH_TERM]. Se afi\u0219eaz\u0103 \xEEn schimb rezultatele pentru [DIFFERENT_TERM]",
  search_suggestion: "Niciun rezultat pentru [SEARCH_TERM]. \xCEncerca\u021Bi una dintre urm\u0103toarele c\u0103ut\u0103ri:",
  searching: "Se caut\u0103 dup\u0103: [SEARCH_TERM]..."
};
var ro_default = {
  thanks_to: thanks_to27,
  comments: comments27,
  direction: direction27,
  strings: strings27
};

// ../translations/ru.json
var ru_exports = {};
__export(ru_exports, {
  comments: () => comments28,
  default: () => ru_default,
  direction: () => direction28,
  strings: () => strings28,
  thanks_to: () => thanks_to28
});
var thanks_to28 = "Aleksandr Gordeev";
var comments28 = "";
var direction28 = "ltr";
var strings28 = {
  placeholder: "\u041F\u043E\u0438\u0441\u043A",
  clear_search: "\u041E\u0447\u0438\u0441\u0442\u0438\u0442\u044C \u043F\u043E\u043B\u0435",
  load_more: "\u0417\u0430\u0433\u0440\u0443\u0437\u0438\u0442\u044C \u0435\u0449\u0435",
  search_label: "\u041F\u043E\u0438\u0441\u043A \u043F\u043E \u0441\u0430\u0439\u0442\u0443",
  filters_label: "\u0424\u0438\u043B\u044C\u0442\u0440\u044B",
  zero_results: "\u041D\u0438\u0447\u0435\u0433\u043E \u043D\u0435 \u043D\u0430\u0439\u0434\u0435\u043D\u043E \u043F\u043E \u0437\u0430\u043F\u0440\u043E\u0441\u0443: [SEARCH_TERM]",
  many_results: "[COUNT] \u0440\u0435\u0437\u0443\u043B\u044C\u0442\u0430\u0442\u043E\u0432 \u043F\u043E \u0437\u0430\u043F\u0440\u043E\u0441\u0443: [SEARCH_TERM]",
  one_result: "[COUNT] \u0440\u0435\u0437\u0443\u043B\u044C\u0442\u0430\u0442 \u043F\u043E \u0437\u0430\u043F\u0440\u043E\u0441\u0443: [SEARCH_TERM]",
  alt_search: "\u041D\u0438\u0447\u0435\u0433\u043E \u043D\u0435 \u043D\u0430\u0439\u0434\u0435\u043D\u043E \u043F\u043E \u0437\u0430\u043F\u0440\u043E\u0441\u0443: [SEARCH_TERM]. \u041F\u043E\u043A\u0430\u0437\u0430\u043D\u044B \u0440\u0435\u0437\u0443\u043B\u044C\u0442\u0430\u0442\u044B \u043F\u043E \u0437\u0430\u043F\u0440\u043E\u0441\u0443: [DIFFERENT_TERM]",
  search_suggestion: "\u041D\u0438\u0447\u0435\u0433\u043E \u043D\u0435 \u043D\u0430\u0439\u0434\u0435\u043D\u043E \u043F\u043E \u0437\u0430\u043F\u0440\u043E\u0441\u0443: [SEARCH_TERM]. \u041F\u043E\u043F\u0440\u043E\u0431\u0443\u0439\u0442\u0435 \u043E\u0434\u0438\u043D \u0438\u0437 \u0441\u043B\u0435\u0434\u0443\u044E\u0449\u0438\u0445 \u0432\u0430\u0440\u0438\u0430\u043D\u0442\u043E\u0432",
  searching: "\u041F\u043E\u0438\u0441\u043A \u043F\u043E \u0437\u0430\u043F\u0440\u043E\u0441\u0443: [SEARCH_TERM]"
};
var ru_default = {
  thanks_to: thanks_to28,
  comments: comments28,
  direction: direction28,
  strings: strings28
};

// ../translations/sr.json
var sr_exports = {};
__export(sr_exports, {
  comments: () => comments29,
  default: () => sr_default,
  direction: () => direction29,
  strings: () => strings29,
  thanks_to: () => thanks_to29
});
var thanks_to29 = "Andrija Sagicc";
var comments29 = "";
var direction29 = "ltr";
var strings29 = {
  placeholder: "\u041F\u0440\u0435\u0442\u0440\u0430\u0433\u0430",
  clear_search: "\u0411\u0440\u0438\u0441\u0430\u045A\u0435",
  load_more: "\u041F\u0440\u0438\u043A\u0430\u0437 \u0432\u0438\u0448\u0435 \u0440\u0435\u0437\u0443\u043B\u0442\u0430\u0442\u0430",
  search_label: "\u041F\u0440\u0435\u0442\u0440\u0430\u0433\u0430 \u0441\u0430\u0458\u0442\u0430",
  filters_label: "\u0424\u0438\u043B\u0442\u0435\u0440\u0438",
  zero_results: "\u041D\u0435\u043C\u0430 \u0440\u0435\u0437\u0443\u043B\u0442\u0430\u0442\u0430 \u0437\u0430 [SEARCH_TERM]",
  many_results: "[COUNT] \u0440\u0435\u0437\u0443\u043B\u0442\u0430\u0442\u0430 \u0437\u0430 [SEARCH_TERM]",
  one_result: "[COUNT] \u0440\u0435\u0437\u0443\u043B\u0442\u0430\u0442\u0430 \u0437\u0430 [SEARCH_TERM]",
  alt_search: "\u041D\u0435\u043C\u0430 \u0440\u0435\u0437\u0443\u043B\u0442\u0430\u0442\u0430 \u0437\u0430 [SEARCH_TERM]. \u041F\u0440\u0438\u043A\u0430\u0437 \u0434\u043E\u0434\u0430\u0442\u043D\u0438\u043A \u0440\u0435\u0437\u0443\u043B\u0442\u0430\u0442\u0430 \u0437\u0430 [DIFFERENT_TERM]",
  search_suggestion: "\u041D\u0435\u043C\u0430 \u0440\u0435\u0437\u0443\u043B\u0442\u0430\u0442\u0430 \u0437\u0430 [SEARCH_TERM]. \u041F\u043E\u043A\u0443\u0448\u0430\u0458\u0442\u0435 \u0441\u0430 \u043D\u0435\u043A\u043E\u043C \u043E\u0434 \u0441\u043B\u0435\u0434\u0435\u045B\u0438\u0445 \u043F\u0440\u0435\u0442\u0440\u0430\u0433\u0430:",
  searching: "\u041F\u0440\u0435\u0442\u0440\u0430\u0433\u0430 \u0442\u0435\u0440\u043C\u0438\u043D\u0430 [SEARCH_TERM]..."
};
var sr_default = {
  thanks_to: thanks_to29,
  comments: comments29,
  direction: direction29,
  strings: strings29
};

// ../translations/sv.json
var sv_exports = {};
__export(sv_exports, {
  comments: () => comments30,
  default: () => sv_default,
  direction: () => direction30,
  strings: () => strings30,
  thanks_to: () => thanks_to30
});
var thanks_to30 = "Montazar Al-Jaber <montazar@nanawee.tech>";
var comments30 = "";
var direction30 = "ltr";
var strings30 = {
  placeholder: "S\xF6k",
  clear_search: "Rensa",
  load_more: "Visa fler tr\xE4ffar",
  search_label: "S\xF6k p\xE5 denna sida",
  filters_label: "Filter",
  zero_results: "[SEARCH_TERM] gav inga tr\xE4ffar",
  many_results: "[SEARCH_TERM] gav [COUNT] tr\xE4ffar",
  one_result: "[SEARCH_TERM] gav [COUNT] tr\xE4ff",
  alt_search: "[SEARCH_TERM] gav inga tr\xE4ffar. Visar resultat f\xF6r [DIFFERENT_TERM] ist\xE4llet",
  search_suggestion: "[SEARCH_TERM] gav inga tr\xE4ffar. F\xF6rs\xF6k igen med en av f\xF6ljande s\xF6kord:",
  searching: "S\xF6ker efter [SEARCH_TERM]..."
};
var sv_default = {
  thanks_to: thanks_to30,
  comments: comments30,
  direction: direction30,
  strings: strings30
};

// ../translations/sw.json
var sw_exports = {};
__export(sw_exports, {
  comments: () => comments31,
  default: () => sw_default,
  direction: () => direction31,
  strings: () => strings31,
  thanks_to: () => thanks_to31
});
var thanks_to31 = "Anonymous";
var comments31 = "";
var direction31 = "ltr";
var strings31 = {
  placeholder: "Tafuta",
  clear_search: "Futa",
  load_more: "Pakia matokeo zaidi",
  search_label: "Tafuta tovuti hii",
  filters_label: "Vichujio",
  zero_results: "Hakuna matokeo ya [SEARCH_TERM]",
  many_results: "Matokeo [COUNT] ya [SEARCH_TERM]",
  one_result: "Tokeo [COUNT] la [SEARCH_TERM]",
  alt_search: "Hakuna mayokeo ya [SEARCH_TERM]. Badala yake, inaonyesha matokeo ya [DIFFERENT_TERM]",
  search_suggestion: "Hakuna matokeo ya [SEARCH_TERM]. Jaribu mojawapo ya utafutaji ufuatao:",
  searching: "Kutafuta [SEARCH_TERM]..."
};
var sw_default = {
  thanks_to: thanks_to31,
  comments: comments31,
  direction: direction31,
  strings: strings31
};

// ../translations/ta.json
var ta_exports = {};
__export(ta_exports, {
  comments: () => comments32,
  default: () => ta_default,
  direction: () => direction32,
  strings: () => strings32,
  thanks_to: () => thanks_to32
});
var thanks_to32 = "";
var comments32 = "";
var direction32 = "ltr";
var strings32 = {
  placeholder: "\u0BA4\u0BC7\u0B9F\u0BC1\u0B95",
  clear_search: "\u0B85\u0BB4\u0BBF\u0B95\u0BCD\u0B95\u0BC1\u0B95",
  load_more: "\u0BAE\u0BC7\u0BB2\u0BC1\u0BAE\u0BCD \u0BAE\u0BC1\u0B9F\u0BBF\u0BB5\u0BC1\u0B95\u0BB3\u0BC8\u0B95\u0BCD \u0B95\u0BBE\u0B9F\u0BCD\u0B9F\u0BC1\u0B95",
  search_label: "\u0B87\u0BA8\u0BCD\u0BA4 \u0BA4\u0BB3\u0BA4\u0BCD\u0BA4\u0BBF\u0BB2\u0BCD \u0BA4\u0BC7\u0B9F\u0BC1\u0B95",
  filters_label: "\u0BB5\u0B9F\u0BBF\u0B95\u0B9F\u0BCD\u0B9F\u0BB2\u0BCD\u0B95\u0BB3\u0BCD",
  zero_results: "[SEARCH_TERM] \u0B95\u0BCD\u0B95\u0BBE\u0BA9 \u0BAE\u0BC1\u0B9F\u0BBF\u0BB5\u0BC1\u0B95\u0BB3\u0BCD \u0B87\u0BB2\u0BCD\u0BB2\u0BC8",
  many_results: "[SEARCH_TERM] \u0B95\u0BCD\u0B95\u0BBE\u0BA9 [COUNT] \u0BAE\u0BC1\u0B9F\u0BBF\u0BB5\u0BC1\u0B95\u0BB3\u0BCD",
  one_result: "[SEARCH_TERM] \u0B95\u0BCD\u0B95\u0BBE\u0BA9 \u0BAE\u0BC1\u0B9F\u0BBF\u0BB5\u0BC1",
  alt_search: "[SEARCH_TERM] \u0B87\u0BA4\u0BCD\u0BA4\u0BC7\u0B9F\u0BB2\u0BC1\u0B95\u0BCD\u0B95\u0BBE\u0BA9 \u0BAE\u0BC1\u0B9F\u0BBF\u0BB5\u0BC1\u0B95\u0BB3\u0BCD \u0B87\u0BB2\u0BCD\u0BB2\u0BC8, \u0B87\u0BA8\u0BCD\u0BA4 \u0BA4\u0BC7\u0B9F\u0BB2\u0BCD\u0B95\u0BB3\u0BC1\u0B95\u0BCD\u0B95\u0BBE\u0BA9 \u0B92\u0BA4\u0BCD\u0BA4 \u0BAE\u0BC1\u0B9F\u0BBF\u0BB5\u0BC1\u0B95\u0BB3\u0BCD [DIFFERENT_TERM]",
  search_suggestion: "[SEARCH_TERM] \u0B87\u0BA4\u0BCD \u0BA4\u0BC7\u0B9F\u0BB2\u0BC1\u0B95\u0BCD\u0B95\u0BBE\u0BA9 \u0BAE\u0BC1\u0B9F\u0BBF\u0BB5\u0BC1\u0B95\u0BB3\u0BCD \u0B87\u0BB2\u0BCD\u0BB2\u0BC8.\u0B87\u0BA4\u0BB1\u0BCD\u0B95\u0BC1 \u0BAA\u0BA4\u0BBF\u0BB2\u0BC0\u0B9F\u0BBE\u0BA9 \u0BA4\u0BC7\u0B9F\u0BB2\u0BCD\u0B95\u0BB3\u0BC8 \u0BA4\u0BC7\u0B9F\u0BC1\u0B95:",
  searching: "[SEARCH_TERM] \u0BA4\u0BC7\u0B9F\u0BAA\u0BCD\u0BAA\u0B9F\u0BC1\u0B95\u0BBF\u0BA9\u0BCD\u0BB1\u0BA4\u0BC1"
};
var ta_default = {
  thanks_to: thanks_to32,
  comments: comments32,
  direction: direction32,
  strings: strings32
};

// ../translations/tr.json
var tr_exports = {};
__export(tr_exports, {
  comments: () => comments33,
  default: () => tr_default,
  direction: () => direction33,
  strings: () => strings33,
  thanks_to: () => thanks_to33
});
var thanks_to33 = "Taylan \xD6zg\xFCr Bildik";
var comments33 = "";
var direction33 = "ltr";
var strings33 = {
  placeholder: "Ara\u015Ft\u0131r",
  clear_search: "Temizle",
  load_more: "Daha fazla sonu\xE7",
  search_label: "Site genelinde arama",
  filters_label: "Filtreler",
  zero_results: "[SEARCH_TERM] i\xE7in sonu\xE7 yok",
  many_results: "[SEARCH_TERM] i\xE7in [COUNT] sonu\xE7 bulundu",
  one_result: "[SEARCH_TERM] i\xE7in [COUNT] sonu\xE7 bulundu",
  alt_search: "[SEARCH_TERM] i\xE7in sonu\xE7 yok. Bunun yerine [DIFFERENT_TERM] i\xE7in sonu\xE7lar g\xF6steriliyor",
  search_suggestion: "[SEARCH_TERM] i\xE7in sonu\xE7 yok. Alternatif olarak a\u015Fa\u011F\u0131daki kelimelerden birini deneyebilirsiniz:",
  searching: "[SEARCH_TERM] ara\u015Ft\u0131r\u0131l\u0131yor..."
};
var tr_default = {
  thanks_to: thanks_to33,
  comments: comments33,
  direction: direction33,
  strings: strings33
};

// ../translations/uk.json
var uk_exports = {};
__export(uk_exports, {
  comments: () => comments34,
  default: () => uk_default,
  direction: () => direction34,
  strings: () => strings34,
  thanks_to: () => thanks_to34
});
var thanks_to34 = "Vladyslav Lyshenko <vladdnepr1989@gmail.com>";
var comments34 = "";
var direction34 = "ltr";
var strings34 = {
  placeholder: "\u041F\u043E\u0448\u0443\u043A",
  clear_search: "\u041E\u0447\u0438\u0441\u0442\u0438\u0442\u0438 \u043F\u043E\u043B\u0435",
  load_more: "\u0417\u0430\u0432\u0430\u043D\u0442\u0430\u0436\u0438\u0442\u0438 \u0449\u0435",
  search_label: "\u041F\u043E\u0448\u0443\u043A \u043F\u043E \u0441\u0430\u0439\u0442\u0443",
  filters_label: "\u0424\u0456\u043B\u044C\u0442\u0440\u0438",
  zero_results: "\u041D\u0456\u0447\u043E\u0433\u043E \u043D\u0435 \u0437\u043D\u0430\u0439\u0434\u0435\u043D\u043E \u0437\u0430 \u0437\u0430\u043F\u0438\u0442\u043E\u043C: [SEARCH_TERM]",
  many_results: "[COUNT] \u0440\u0435\u0437\u0443\u043B\u044C\u0442\u0430\u0442\u0456\u0432 \u043D\u0430 \u0437\u0430\u043F\u0438\u0442: [SEARCH_TERM]",
  one_result: "[COUNT] \u0440\u0435\u0437\u0443\u043B\u044C\u0442\u0430\u0442 \u0437\u0430 \u0437\u0430\u043F\u0438\u0442\u043E\u043C: [SEARCH_TERM]",
  alt_search: "\u041D\u0456\u0447\u043E\u0433\u043E \u043D\u0435 \u0437\u043D\u0430\u0439\u0434\u0435\u043D\u043E \u043D\u0430 \u0437\u0430\u043F\u0438\u0442: [SEARCH_TERM]. \u041F\u043E\u043A\u0430\u0437\u0430\u043D\u043E \u0440\u0435\u0437\u0443\u043B\u044C\u0442\u0430\u0442\u0438 \u043D\u0430 \u0437\u0430\u043F\u0438\u0442: [DIFFERENT_TERM]",
  search_suggestion: "\u041D\u0456\u0447\u043E\u0433\u043E \u043D\u0435 \u0437\u043D\u0430\u0439\u0434\u0435\u043D\u043E \u043D\u0430 \u0437\u0430\u043F\u0438\u0442: [SEARCH_TERM]. \u0421\u043F\u0440\u043E\u0431\u0443\u0439\u0442\u0435 \u043E\u0434\u0438\u043D \u0456\u0437 \u0442\u0430\u043A\u0438\u0445 \u0432\u0430\u0440\u0456\u0430\u043D\u0442\u0456\u0432",
  searching: "\u041F\u043E\u0448\u0443\u043A \u0437\u0430 \u0437\u0430\u043F\u0438\u0442\u043E\u043C: [SEARCH_TERM]"
};
var uk_default = {
  thanks_to: thanks_to34,
  comments: comments34,
  direction: direction34,
  strings: strings34
};

// ../translations/vi.json
var vi_exports = {};
__export(vi_exports, {
  comments: () => comments35,
  default: () => vi_default,
  direction: () => direction35,
  strings: () => strings35,
  thanks_to: () => thanks_to35
});
var thanks_to35 = "Long Nhat Nguyen";
var comments35 = "";
var direction35 = "ltr";
var strings35 = {
  placeholder: "T\xECm ki\u1EBFm",
  clear_search: "X\xF3a",
  load_more: "Nhi\u1EC1u k\u1EBFt qu\u1EA3 h\u01A1n",
  search_label: "T\xECm ki\u1EBFm trong trang n\xE0y",
  filters_label: "B\u1ED9 l\u1ECDc",
  zero_results: "Kh\xF4ng t\xECm th\u1EA5y k\u1EBFt qu\u1EA3 cho [SEARCH_TERM]",
  many_results: "[COUNT] k\u1EBFt qu\u1EA3 cho [SEARCH_TERM]",
  one_result: "[COUNT] k\u1EBFt qu\u1EA3 cho [SEARCH_TERM]",
  alt_search: "Kh\xF4ng t\xECm th\u1EA5y k\u1EBFt qu\u1EA3 cho [SEARCH_TERM]. Ki\u1EC3m th\u1ECB k\u1EBFt qu\u1EA3 thay th\u1EBF v\u1EDBi [DIFFERENT_TERM]",
  search_suggestion: "Kh\xF4ng t\xECm th\u1EA5y k\u1EBFt qu\u1EA3 cho [SEARCH_TERM]. Th\u1EED m\u1ED9t trong c\xE1c t\xECm ki\u1EBFm:",
  searching: "\u0110ang t\xECm ki\u1EBFm cho [SEARCH_TERM]..."
};
var vi_default = {
  thanks_to: thanks_to35,
  comments: comments35,
  direction: direction35,
  strings: strings35
};

// ../translations/zh-cn.json
var zh_cn_exports = {};
__export(zh_cn_exports, {
  comments: () => comments36,
  default: () => zh_cn_default,
  direction: () => direction36,
  strings: () => strings36,
  thanks_to: () => thanks_to36
});
var thanks_to36 = "Amber Song";
var comments36 = "";
var direction36 = "ltr";
var strings36 = {
  placeholder: "\u641C\u7D22",
  clear_search: "\u6E05\u9664",
  load_more: "\u52A0\u8F7D\u66F4\u591A\u7ED3\u679C",
  search_label: "\u7AD9\u5185\u641C\u7D22",
  filters_label: "\u7B5B\u9009",
  zero_results: "\u672A\u627E\u5230 [SEARCH_TERM] \u7684\u76F8\u5173\u7ED3\u679C",
  many_results: "\u627E\u5230 [COUNT] \u4E2A [SEARCH_TERM] \u7684\u76F8\u5173\u7ED3\u679C",
  one_result: "\u627E\u5230 [COUNT] \u4E2A [SEARCH_TERM] \u7684\u76F8\u5173\u7ED3\u679C",
  alt_search: "\u672A\u627E\u5230 [SEARCH_TERM] \u7684\u76F8\u5173\u7ED3\u679C\u3002\u6539\u4E3A\u663E\u793A [DIFFERENT_TERM] \u7684\u76F8\u5173\u7ED3\u679C",
  search_suggestion: "\u672A\u627E\u5230 [SEARCH_TERM] \u7684\u76F8\u5173\u7ED3\u679C\u3002\u8BF7\u5C1D\u8BD5\u4EE5\u4E0B\u641C\u7D22\u3002",
  searching: "\u6B63\u5728\u641C\u7D22 [SEARCH_TERM]..."
};
var zh_cn_default = {
  thanks_to: thanks_to36,
  comments: comments36,
  direction: direction36,
  strings: strings36
};

// ../translations/zh-tw.json
var zh_tw_exports = {};
__export(zh_tw_exports, {
  comments: () => comments37,
  default: () => zh_tw_default,
  direction: () => direction37,
  strings: () => strings37,
  thanks_to: () => thanks_to37
});
var thanks_to37 = "Amber Song";
var comments37 = "";
var direction37 = "ltr";
var strings37 = {
  placeholder: "\u641C\u7D22",
  clear_search: "\u6E05\u9664",
  load_more: "\u52A0\u8F09\u66F4\u591A\u7D50\u679C",
  search_label: "\u7AD9\u5167\u641C\u7D22",
  filters_label: "\u7BE9\u9078",
  zero_results: "\u672A\u627E\u5230 [SEARCH_TERM] \u7684\u76F8\u95DC\u7D50\u679C",
  many_results: "\u627E\u5230 [COUNT] \u500B [SEARCH_TERM] \u7684\u76F8\u95DC\u7D50\u679C",
  one_result: "\u627E\u5230 [COUNT] \u500B [SEARCH_TERM] \u7684\u76F8\u95DC\u7D50\u679C",
  alt_search: "\u672A\u627E\u5230 [SEARCH_TERM] \u7684\u76F8\u95DC\u7D50\u679C\u3002\u6539\u70BA\u986F\u793A [DIFFERENT_TERM] \u7684\u76F8\u95DC\u7D50\u679C",
  search_suggestion: "\u672A\u627E\u5230 [SEARCH_TERM] \u7684\u76F8\u95DC\u7D50\u679C\u3002\u8ACB\u5617\u8A66\u4EE5\u4E0B\u641C\u7D22\u3002",
  searching: "\u6B63\u5728\u641C\u7D22 [SEARCH_TERM]..."
};
var zh_tw_default = {
  thanks_to: thanks_to37,
  comments: comments37,
  direction: direction37,
  strings: strings37
};

// ../translations/zh.json
var zh_exports = {};
__export(zh_exports, {
  comments: () => comments38,
  default: () => zh_default,
  direction: () => direction38,
  strings: () => strings38,
  thanks_to: () => thanks_to38
});
var thanks_to38 = "Amber Song";
var comments38 = "";
var direction38 = "ltr";
var strings38 = {
  placeholder: "\u641C\u7D22",
  clear_search: "\u6E05\u9664",
  load_more: "\u52A0\u8F7D\u66F4\u591A\u7ED3\u679C",
  search_label: "\u7AD9\u5185\u641C\u7D22",
  filters_label: "\u7B5B\u9009",
  zero_results: "\u672A\u627E\u5230 [SEARCH_TERM] \u7684\u76F8\u5173\u7ED3\u679C",
  many_results: "\u627E\u5230 [COUNT] \u4E2A [SEARCH_TERM] \u7684\u76F8\u5173\u7ED3\u679C",
  one_result: "\u627E\u5230 [COUNT] \u4E2A [SEARCH_TERM] \u7684\u76F8\u5173\u7ED3\u679C",
  alt_search: "\u672A\u627E\u5230 [SEARCH_TERM] \u7684\u76F8\u5173\u7ED3\u679C\u3002\u6539\u4E3A\u663E\u793A [DIFFERENT_TERM] \u7684\u76F8\u5173\u7ED3\u679C",
  search_suggestion: "\u672A\u627E\u5230 [SEARCH_TERM] \u7684\u76F8\u5173\u7ED3\u679C\u3002\u8BF7\u5C1D\u8BD5\u4EE5\u4E0B\u641C\u7D22\u3002",
  searching: "\u6B63\u5728\u641C\u7D22 [SEARCH_TERM]..."
};
var zh_default = {
  thanks_to: thanks_to38,
  comments: comments38,
  direction: direction38,
  strings: strings38
};

// import-glob:../../translations/*.json
var modules = [af_exports, ar_exports, bn_exports, ca_exports, cs_exports, da_exports, de_exports, en_exports, es_exports, fa_exports, fi_exports, fr_exports, gl_exports, he_exports, hi_exports, hr_exports, hu_exports, id_exports, it_exports, ja_exports, ko_exports, mi_exports, nl_exports, no_exports, pl_exports, pt_exports, ro_exports, ru_exports, sr_exports, sv_exports, sw_exports, ta_exports, tr_exports, uk_exports, vi_exports, zh_cn_exports, zh_tw_exports, zh_exports];
var __default = modules;
var filenames = ["../../translations/af.json", "../../translations/ar.json", "../../translations/bn.json", "../../translations/ca.json", "../../translations/cs.json", "../../translations/da.json", "../../translations/de.json", "../../translations/en.json", "../../translations/es.json", "../../translations/fa.json", "../../translations/fi.json", "../../translations/fr.json", "../../translations/gl.json", "../../translations/he.json", "../../translations/hi.json", "../../translations/hr.json", "../../translations/hu.json", "../../translations/id.json", "../../translations/it.json", "../../translations/ja.json", "../../translations/ko.json", "../../translations/mi.json", "../../translations/nl.json", "../../translations/no.json", "../../translations/pl.json", "../../translations/pt.json", "../../translations/ro.json", "../../translations/ru.json", "../../translations/sr.json", "../../translations/sv.json", "../../translations/sw.json", "../../translations/ta.json", "../../translations/tr.json", "../../translations/uk.json", "../../translations/vi.json", "../../translations/zh-cn.json", "../../translations/zh-tw.json", "../../translations/zh.json"];

// svelte/ui.svelte
function get_each_context4(ctx, list, i) {
  const child_ctx = ctx.slice();
  child_ctx[51] = list[i];
  return child_ctx;
}
function create_if_block_7(ctx) {
  let filters;
  let updating_selected_filters;
  let current;
  function filters_selected_filters_binding(value) {
    ctx[37](value);
  }
  let filters_props = {
    show_empty_filters: (
      /*show_empty_filters*/
      ctx[5]
    ),
    open_filters: (
      /*open_filters*/
      ctx[6]
    ),
    available_filters: (
      /*available_filters*/
      ctx[18]
    ),
    translate: (
      /*translate*/
      ctx[20]
    ),
    automatic_translations: (
      /*automatic_translations*/
      ctx[19]
    ),
    translations: (
      /*translations*/
      ctx[7]
    )
  };
  if (
    /*selected_filters*/
    ctx[0] !== void 0
  ) {
    filters_props.selected_filters = /*selected_filters*/
    ctx[0];
  }
  filters = new filters_default({ props: filters_props });
  binding_callbacks.push(() => bind(filters, "selected_filters", filters_selected_filters_binding));
  return {
    c() {
      create_component(filters.$$.fragment);
    },
    m(target, anchor) {
      mount_component(filters, target, anchor);
      current = true;
    },
    p(ctx2, dirty) {
      const filters_changes = {};
      if (dirty[0] & /*show_empty_filters*/
      32)
        filters_changes.show_empty_filters = /*show_empty_filters*/
        ctx2[5];
      if (dirty[0] & /*open_filters*/
      64)
        filters_changes.open_filters = /*open_filters*/
        ctx2[6];
      if (dirty[0] & /*available_filters*/
      262144)
        filters_changes.available_filters = /*available_filters*/
        ctx2[18];
      if (dirty[0] & /*automatic_translations*/
      524288)
        filters_changes.automatic_translations = /*automatic_translations*/
        ctx2[19];
      if (dirty[0] & /*translations*/
      128)
        filters_changes.translations = /*translations*/
        ctx2[7];
      if (!updating_selected_filters && dirty[0] & /*selected_filters*/
      1) {
        updating_selected_filters = true;
        filters_changes.selected_filters = /*selected_filters*/
        ctx2[0];
        add_flush_callback(() => updating_selected_filters = false);
      }
      filters.$set(filters_changes);
    },
    i(local) {
      if (current)
        return;
      transition_in(filters.$$.fragment, local);
      current = true;
    },
    o(local) {
      transition_out(filters.$$.fragment, local);
      current = false;
    },
    d(detaching) {
      destroy_component(filters, detaching);
    }
  };
}
function create_if_block4(ctx) {
  let div;
  let current_block_type_index;
  let if_block;
  let current;
  const if_block_creators = [create_if_block_14, create_else_block3];
  const if_blocks = [];
  function select_block_type(ctx2, dirty) {
    if (
      /*loading*/
      ctx2[14]
    )
      return 0;
    return 1;
  }
  current_block_type_index = select_block_type(ctx, [-1, -1]);
  if_block = if_blocks[current_block_type_index] = if_block_creators[current_block_type_index](ctx);
  return {
    c() {
      div = element("div");
      if_block.c();
      attr(div, "class", "pagefind-ui__results-area svelte-e9gkc3");
    },
    m(target, anchor) {
      insert(target, div, anchor);
      if_blocks[current_block_type_index].m(div, null);
      current = true;
    },
    p(ctx2, dirty) {
      let previous_block_index = current_block_type_index;
      current_block_type_index = select_block_type(ctx2, dirty);
      if (current_block_type_index === previous_block_index) {
        if_blocks[current_block_type_index].p(ctx2, dirty);
      } else {
        group_outros();
        transition_out(if_blocks[previous_block_index], 1, 1, () => {
          if_blocks[previous_block_index] = null;
        });
        check_outros();
        if_block = if_blocks[current_block_type_index];
        if (!if_block) {
          if_block = if_blocks[current_block_type_index] = if_block_creators[current_block_type_index](ctx2);
          if_block.c();
        } else {
          if_block.p(ctx2, dirty);
        }
        transition_in(if_block, 1);
        if_block.m(div, null);
      }
    },
    i(local) {
      if (current)
        return;
      transition_in(if_block);
      current = true;
    },
    o(local) {
      transition_out(if_block);
      current = false;
    },
    d(detaching) {
      if (detaching)
        detach(div);
      if_blocks[current_block_type_index].d();
    }
  };
}
function create_else_block3(ctx) {
  let p;
  let t0;
  let ol;
  let each_blocks = [];
  let each_1_lookup = /* @__PURE__ */ new Map();
  let t1;
  let if_block1_anchor;
  let current;
  function select_block_type_1(ctx2, dirty) {
    if (
      /*searchResult*/
      ctx2[13].results.length === 0
    )
      return create_if_block_52;
    if (
      /*searchResult*/
      ctx2[13].results.length === 1
    )
      return create_if_block_6;
    return create_else_block_2;
  }
  let current_block_type = select_block_type_1(ctx, [-1, -1]);
  let if_block0 = current_block_type(ctx);
  let each_value = (
    /*searchResult*/
    ctx[13].results.slice(
      0,
      /*show*/
      ctx[17]
    )
  );
  const get_key = (ctx2) => (
    /*result*/
    ctx2[51].id
  );
  for (let i = 0; i < each_value.length; i += 1) {
    let child_ctx = get_each_context4(ctx, each_value, i);
    let key = get_key(child_ctx);
    each_1_lookup.set(key, each_blocks[i] = create_each_block4(key, child_ctx));
  }
  let if_block1 = (
    /*searchResult*/
    ctx[13].results.length > /*show*/
    ctx[17] && create_if_block_33(ctx)
  );
  return {
    c() {
      p = element("p");
      if_block0.c();
      t0 = space();
      ol = element("ol");
      for (let i = 0; i < each_blocks.length; i += 1) {
        each_blocks[i].c();
      }
      t1 = space();
      if (if_block1)
        if_block1.c();
      if_block1_anchor = empty();
      attr(p, "class", "pagefind-ui__message svelte-e9gkc3");
      attr(ol, "class", "pagefind-ui__results svelte-e9gkc3");
    },
    m(target, anchor) {
      insert(target, p, anchor);
      if_block0.m(p, null);
      insert(target, t0, anchor);
      insert(target, ol, anchor);
      for (let i = 0; i < each_blocks.length; i += 1) {
        if (each_blocks[i]) {
          each_blocks[i].m(ol, null);
        }
      }
      insert(target, t1, anchor);
      if (if_block1)
        if_block1.m(target, anchor);
      insert(target, if_block1_anchor, anchor);
      current = true;
    },
    p(ctx2, dirty) {
      if (current_block_type === (current_block_type = select_block_type_1(ctx2, dirty)) && if_block0) {
        if_block0.p(ctx2, dirty);
      } else {
        if_block0.d(1);
        if_block0 = current_block_type(ctx2);
        if (if_block0) {
          if_block0.c();
          if_block0.m(p, null);
        }
      }
      if (dirty[0] & /*show_images, process_result, searchResult, show, show_sub_results*/
      139292) {
        each_value = /*searchResult*/
        ctx2[13].results.slice(
          0,
          /*show*/
          ctx2[17]
        );
        group_outros();
        each_blocks = update_keyed_each(each_blocks, dirty, get_key, 1, ctx2, each_value, each_1_lookup, ol, outro_and_destroy_block, create_each_block4, null, get_each_context4);
        check_outros();
      }
      if (
        /*searchResult*/
        ctx2[13].results.length > /*show*/
        ctx2[17]
      ) {
        if (if_block1) {
          if_block1.p(ctx2, dirty);
        } else {
          if_block1 = create_if_block_33(ctx2);
          if_block1.c();
          if_block1.m(if_block1_anchor.parentNode, if_block1_anchor);
        }
      } else if (if_block1) {
        if_block1.d(1);
        if_block1 = null;
      }
    },
    i(local) {
      if (current)
        return;
      for (let i = 0; i < each_value.length; i += 1) {
        transition_in(each_blocks[i]);
      }
      current = true;
    },
    o(local) {
      for (let i = 0; i < each_blocks.length; i += 1) {
        transition_out(each_blocks[i]);
      }
      current = false;
    },
    d(detaching) {
      if (detaching)
        detach(p);
      if_block0.d();
      if (detaching)
        detach(t0);
      if (detaching)
        detach(ol);
      for (let i = 0; i < each_blocks.length; i += 1) {
        each_blocks[i].d();
      }
      if (detaching)
        detach(t1);
      if (if_block1)
        if_block1.d(detaching);
      if (detaching)
        detach(if_block1_anchor);
    }
  };
}
function create_if_block_14(ctx) {
  let if_block_anchor;
  let if_block = (
    /*search_term*/
    ctx[16] && create_if_block_23(ctx)
  );
  return {
    c() {
      if (if_block)
        if_block.c();
      if_block_anchor = empty();
    },
    m(target, anchor) {
      if (if_block)
        if_block.m(target, anchor);
      insert(target, if_block_anchor, anchor);
    },
    p(ctx2, dirty) {
      if (
        /*search_term*/
        ctx2[16]
      ) {
        if (if_block) {
          if_block.p(ctx2, dirty);
        } else {
          if_block = create_if_block_23(ctx2);
          if_block.c();
          if_block.m(if_block_anchor.parentNode, if_block_anchor);
        }
      } else if (if_block) {
        if_block.d(1);
        if_block = null;
      }
    },
    i: noop,
    o: noop,
    d(detaching) {
      if (if_block)
        if_block.d(detaching);
      if (detaching)
        detach(if_block_anchor);
    }
  };
}
function create_else_block_2(ctx) {
  let t_value = (
    /*translate*/
    ctx[20](
      "many_results",
      /*automatic_translations*/
      ctx[19],
      /*translations*/
      ctx[7]
    ).replace(
      /\[SEARCH_TERM\]/,
      /*search_term*/
      ctx[16]
    ).replace(/\[COUNT\]/, new Intl.NumberFormat(
      /*translations*/
      ctx[7].language
    ).format(
      /*searchResult*/
      ctx[13].results.length
    )) + ""
  );
  let t;
  return {
    c() {
      t = text(t_value);
    },
    m(target, anchor) {
      insert(target, t, anchor);
    },
    p(ctx2, dirty) {
      if (dirty[0] & /*automatic_translations, translations, search_term, searchResult*/
      598144 && t_value !== (t_value = /*translate*/
      ctx2[20](
        "many_results",
        /*automatic_translations*/
        ctx2[19],
        /*translations*/
        ctx2[7]
      ).replace(
        /\[SEARCH_TERM\]/,
        /*search_term*/
        ctx2[16]
      ).replace(/\[COUNT\]/, new Intl.NumberFormat(
        /*translations*/
        ctx2[7].language
      ).format(
        /*searchResult*/
        ctx2[13].results.length
      )) + ""))
        set_data(t, t_value);
    },
    d(detaching) {
      if (detaching)
        detach(t);
    }
  };
}
function create_if_block_6(ctx) {
  let t_value = (
    /*translate*/
    ctx[20](
      "one_result",
      /*automatic_translations*/
      ctx[19],
      /*translations*/
      ctx[7]
    ).replace(
      /\[SEARCH_TERM\]/,
      /*search_term*/
      ctx[16]
    ).replace(/\[COUNT\]/, new Intl.NumberFormat(
      /*translations*/
      ctx[7].language
    ).format(1)) + ""
  );
  let t;
  return {
    c() {
      t = text(t_value);
    },
    m(target, anchor) {
      insert(target, t, anchor);
    },
    p(ctx2, dirty) {
      if (dirty[0] & /*automatic_translations, translations, search_term*/
      589952 && t_value !== (t_value = /*translate*/
      ctx2[20](
        "one_result",
        /*automatic_translations*/
        ctx2[19],
        /*translations*/
        ctx2[7]
      ).replace(
        /\[SEARCH_TERM\]/,
        /*search_term*/
        ctx2[16]
      ).replace(/\[COUNT\]/, new Intl.NumberFormat(
        /*translations*/
        ctx2[7].language
      ).format(1)) + ""))
        set_data(t, t_value);
    },
    d(detaching) {
      if (detaching)
        detach(t);
    }
  };
}
function create_if_block_52(ctx) {
  let t_value = (
    /*translate*/
    ctx[20](
      "zero_results",
      /*automatic_translations*/
      ctx[19],
      /*translations*/
      ctx[7]
    ).replace(
      /\[SEARCH_TERM\]/,
      /*search_term*/
      ctx[16]
    ) + ""
  );
  let t;
  return {
    c() {
      t = text(t_value);
    },
    m(target, anchor) {
      insert(target, t, anchor);
    },
    p(ctx2, dirty) {
      if (dirty[0] & /*automatic_translations, translations, search_term*/
      589952 && t_value !== (t_value = /*translate*/
      ctx2[20](
        "zero_results",
        /*automatic_translations*/
        ctx2[19],
        /*translations*/
        ctx2[7]
      ).replace(
        /\[SEARCH_TERM\]/,
        /*search_term*/
        ctx2[16]
      ) + ""))
        set_data(t, t_value);
    },
    d(detaching) {
      if (detaching)
        detach(t);
    }
  };
}
function create_else_block_1(ctx) {
  let result;
  let current;
  result = new result_default({
    props: {
      show_images: (
        /*show_images*/
        ctx[2]
      ),
      process_result: (
        /*process_result*/
        ctx[4]
      ),
      result: (
        /*result*/
        ctx[51]
      )
    }
  });
  return {
    c() {
      create_component(result.$$.fragment);
    },
    m(target, anchor) {
      mount_component(result, target, anchor);
      current = true;
    },
    p(ctx2, dirty) {
      const result_changes = {};
      if (dirty[0] & /*show_images*/
      4)
        result_changes.show_images = /*show_images*/
        ctx2[2];
      if (dirty[0] & /*process_result*/
      16)
        result_changes.process_result = /*process_result*/
        ctx2[4];
      if (dirty[0] & /*searchResult, show*/
      139264)
        result_changes.result = /*result*/
        ctx2[51];
      result.$set(result_changes);
    },
    i(local) {
      if (current)
        return;
      transition_in(result.$$.fragment, local);
      current = true;
    },
    o(local) {
      transition_out(result.$$.fragment, local);
      current = false;
    },
    d(detaching) {
      destroy_component(result, detaching);
    }
  };
}
function create_if_block_43(ctx) {
  let resultwithsubs;
  let current;
  resultwithsubs = new result_with_subs_default({
    props: {
      show_images: (
        /*show_images*/
        ctx[2]
      ),
      process_result: (
        /*process_result*/
        ctx[4]
      ),
      result: (
        /*result*/
        ctx[51]
      )
    }
  });
  return {
    c() {
      create_component(resultwithsubs.$$.fragment);
    },
    m(target, anchor) {
      mount_component(resultwithsubs, target, anchor);
      current = true;
    },
    p(ctx2, dirty) {
      const resultwithsubs_changes = {};
      if (dirty[0] & /*show_images*/
      4)
        resultwithsubs_changes.show_images = /*show_images*/
        ctx2[2];
      if (dirty[0] & /*process_result*/
      16)
        resultwithsubs_changes.process_result = /*process_result*/
        ctx2[4];
      if (dirty[0] & /*searchResult, show*/
      139264)
        resultwithsubs_changes.result = /*result*/
        ctx2[51];
      resultwithsubs.$set(resultwithsubs_changes);
    },
    i(local) {
      if (current)
        return;
      transition_in(resultwithsubs.$$.fragment, local);
      current = true;
    },
    o(local) {
      transition_out(resultwithsubs.$$.fragment, local);
      current = false;
    },
    d(detaching) {
      destroy_component(resultwithsubs, detaching);
    }
  };
}
function create_each_block4(key_1, ctx) {
  let first;
  let current_block_type_index;
  let if_block;
  let if_block_anchor;
  let current;
  const if_block_creators = [create_if_block_43, create_else_block_1];
  const if_blocks = [];
  function select_block_type_2(ctx2, dirty) {
    if (
      /*show_sub_results*/
      ctx2[3]
    )
      return 0;
    return 1;
  }
  current_block_type_index = select_block_type_2(ctx, [-1, -1]);
  if_block = if_blocks[current_block_type_index] = if_block_creators[current_block_type_index](ctx);
  return {
    key: key_1,
    first: null,
    c() {
      first = empty();
      if_block.c();
      if_block_anchor = empty();
      this.first = first;
    },
    m(target, anchor) {
      insert(target, first, anchor);
      if_blocks[current_block_type_index].m(target, anchor);
      insert(target, if_block_anchor, anchor);
      current = true;
    },
    p(new_ctx, dirty) {
      ctx = new_ctx;
      let previous_block_index = current_block_type_index;
      current_block_type_index = select_block_type_2(ctx, dirty);
      if (current_block_type_index === previous_block_index) {
        if_blocks[current_block_type_index].p(ctx, dirty);
      } else {
        group_outros();
        transition_out(if_blocks[previous_block_index], 1, 1, () => {
          if_blocks[previous_block_index] = null;
        });
        check_outros();
        if_block = if_blocks[current_block_type_index];
        if (!if_block) {
          if_block = if_blocks[current_block_type_index] = if_block_creators[current_block_type_index](ctx);
          if_block.c();
        } else {
          if_block.p(ctx, dirty);
        }
        transition_in(if_block, 1);
        if_block.m(if_block_anchor.parentNode, if_block_anchor);
      }
    },
    i(local) {
      if (current)
        return;
      transition_in(if_block);
      current = true;
    },
    o(local) {
      transition_out(if_block);
      current = false;
    },
    d(detaching) {
      if (detaching)
        detach(first);
      if_blocks[current_block_type_index].d(detaching);
      if (detaching)
        detach(if_block_anchor);
    }
  };
}
function create_if_block_33(ctx) {
  let button;
  let t_value = (
    /*translate*/
    ctx[20](
      "load_more",
      /*automatic_translations*/
      ctx[19],
      /*translations*/
      ctx[7]
    ) + ""
  );
  let t;
  let mounted;
  let dispose;
  return {
    c() {
      button = element("button");
      t = text(t_value);
      attr(button, "type", "button");
      attr(button, "class", "pagefind-ui__button svelte-e9gkc3");
    },
    m(target, anchor) {
      insert(target, button, anchor);
      append(button, t);
      if (!mounted) {
        dispose = listen(
          button,
          "click",
          /*showMore*/
          ctx[22]
        );
        mounted = true;
      }
    },
    p(ctx2, dirty) {
      if (dirty[0] & /*automatic_translations, translations*/
      524416 && t_value !== (t_value = /*translate*/
      ctx2[20](
        "load_more",
        /*automatic_translations*/
        ctx2[19],
        /*translations*/
        ctx2[7]
      ) + ""))
        set_data(t, t_value);
    },
    d(detaching) {
      if (detaching)
        detach(button);
      mounted = false;
      dispose();
    }
  };
}
function create_if_block_23(ctx) {
  let p;
  let t_value = (
    /*translate*/
    ctx[20](
      "searching",
      /*automatic_translations*/
      ctx[19],
      /*translations*/
      ctx[7]
    ).replace(
      /\[SEARCH_TERM\]/,
      /*search_term*/
      ctx[16]
    ) + ""
  );
  let t;
  return {
    c() {
      p = element("p");
      t = text(t_value);
      attr(p, "class", "pagefind-ui__message svelte-e9gkc3");
    },
    m(target, anchor) {
      insert(target, p, anchor);
      append(p, t);
    },
    p(ctx2, dirty) {
      if (dirty[0] & /*automatic_translations, translations, search_term*/
      589952 && t_value !== (t_value = /*translate*/
      ctx2[20](
        "searching",
        /*automatic_translations*/
        ctx2[19],
        /*translations*/
        ctx2[7]
      ).replace(
        /\[SEARCH_TERM\]/,
        /*search_term*/
        ctx2[16]
      ) + ""))
        set_data(t, t_value);
    },
    d(detaching) {
      if (detaching)
        detach(p);
    }
  };
}
function create_fragment4(ctx) {
  let div1;
  let form;
  let input;
  let input_placeholder_value;
  let t0;
  let button;
  let t1_value = (
    /*translate*/
    ctx[20](
      "clear_search",
      /*automatic_translations*/
      ctx[19],
      /*translations*/
      ctx[7]
    ) + ""
  );
  let t1;
  let t2;
  let div0;
  let t3;
  let form_aria_label_value;
  let current;
  let mounted;
  let dispose;
  let if_block0 = (
    /*initializing*/
    ctx[12] && create_if_block_7(ctx)
  );
  let if_block1 = (
    /*searched*/
    ctx[15] && create_if_block4(ctx)
  );
  return {
    c() {
      div1 = element("div");
      form = element("form");
      input = element("input");
      t0 = space();
      button = element("button");
      t1 = text(t1_value);
      t2 = space();
      div0 = element("div");
      if (if_block0)
        if_block0.c();
      t3 = space();
      if (if_block1)
        if_block1.c();
      attr(input, "class", "pagefind-ui__search-input svelte-e9gkc3");
      attr(input, "type", "text");
      attr(input, "placeholder", input_placeholder_value = /*translate*/
      ctx[20](
        "placeholder",
        /*automatic_translations*/
        ctx[19],
        /*translations*/
        ctx[7]
      ));
      attr(input, "autocapitalize", "none");
      attr(input, "enterkeyhint", "search");
      input.autofocus = /*autofocus*/
      ctx[8];
      attr(button, "class", "pagefind-ui__search-clear svelte-e9gkc3");
      toggle_class(button, "pagefind-ui__suppressed", !/*val*/
      ctx[9]);
      attr(div0, "class", "pagefind-ui__drawer svelte-e9gkc3");
      toggle_class(div0, "pagefind-ui__hidden", !/*searched*/
      ctx[15]);
      attr(form, "class", "pagefind-ui__form svelte-e9gkc3");
      attr(form, "role", "search");
      attr(form, "aria-label", form_aria_label_value = /*translate*/
      ctx[20](
        "search_label",
        /*automatic_translations*/
        ctx[19],
        /*translations*/
        ctx[7]
      ));
      attr(form, "action", "javascript:void(0);");
      attr(div1, "class", "pagefind-ui svelte-e9gkc3");
      toggle_class(
        div1,
        "pagefind-ui--reset",
        /*reset_styles*/
        ctx[1]
      );
    },
    m(target, anchor) {
      insert(target, div1, anchor);
      append(div1, form);
      append(form, input);
      set_input_value(
        input,
        /*val*/
        ctx[9]
      );
      ctx[34](input);
      append(form, t0);
      append(form, button);
      append(button, t1);
      ctx[35](button);
      append(form, t2);
      append(form, div0);
      if (if_block0)
        if_block0.m(div0, null);
      append(div0, t3);
      if (if_block1)
        if_block1.m(div0, null);
      current = true;
      if (
        /*autofocus*/
        ctx[8]
      )
        input.focus();
      if (!mounted) {
        dispose = [
          listen(
            input,
            "focus",
            /*init*/
            ctx[21]
          ),
          listen(
            input,
            "keydown",
            /*keydown_handler*/
            ctx[32]
          ),
          listen(
            input,
            "input",
            /*input_input_handler*/
            ctx[33]
          ),
          listen(
            button,
            "click",
            /*click_handler*/
            ctx[36]
          ),
          listen(form, "submit", submit_handler)
        ];
        mounted = true;
      }
    },
    p(ctx2, dirty) {
      if (!current || dirty[0] & /*automatic_translations, translations*/
      524416 && input_placeholder_value !== (input_placeholder_value = /*translate*/
      ctx2[20](
        "placeholder",
        /*automatic_translations*/
        ctx2[19],
        /*translations*/
        ctx2[7]
      ))) {
        attr(input, "placeholder", input_placeholder_value);
      }
      if (!current || dirty[0] & /*autofocus*/
      256) {
        input.autofocus = /*autofocus*/
        ctx2[8];
      }
      if (dirty[0] & /*val*/
      512 && input.value !== /*val*/
      ctx2[9]) {
        set_input_value(
          input,
          /*val*/
          ctx2[9]
        );
      }
      if ((!current || dirty[0] & /*automatic_translations, translations*/
      524416) && t1_value !== (t1_value = /*translate*/
      ctx2[20](
        "clear_search",
        /*automatic_translations*/
        ctx2[19],
        /*translations*/
        ctx2[7]
      ) + ""))
        set_data(t1, t1_value);
      if (!current || dirty[0] & /*val*/
      512) {
        toggle_class(button, "pagefind-ui__suppressed", !/*val*/
        ctx2[9]);
      }
      if (
        /*initializing*/
        ctx2[12]
      ) {
        if (if_block0) {
          if_block0.p(ctx2, dirty);
          if (dirty[0] & /*initializing*/
          4096) {
            transition_in(if_block0, 1);
          }
        } else {
          if_block0 = create_if_block_7(ctx2);
          if_block0.c();
          transition_in(if_block0, 1);
          if_block0.m(div0, t3);
        }
      } else if (if_block0) {
        group_outros();
        transition_out(if_block0, 1, 1, () => {
          if_block0 = null;
        });
        check_outros();
      }
      if (
        /*searched*/
        ctx2[15]
      ) {
        if (if_block1) {
          if_block1.p(ctx2, dirty);
          if (dirty[0] & /*searched*/
          32768) {
            transition_in(if_block1, 1);
          }
        } else {
          if_block1 = create_if_block4(ctx2);
          if_block1.c();
          transition_in(if_block1, 1);
          if_block1.m(div0, null);
        }
      } else if (if_block1) {
        group_outros();
        transition_out(if_block1, 1, 1, () => {
          if_block1 = null;
        });
        check_outros();
      }
      if (!current || dirty[0] & /*searched*/
      32768) {
        toggle_class(div0, "pagefind-ui__hidden", !/*searched*/
        ctx2[15]);
      }
      if (!current || dirty[0] & /*automatic_translations, translations*/
      524416 && form_aria_label_value !== (form_aria_label_value = /*translate*/
      ctx2[20](
        "search_label",
        /*automatic_translations*/
        ctx2[19],
        /*translations*/
        ctx2[7]
      ))) {
        attr(form, "aria-label", form_aria_label_value);
      }
      if (!current || dirty[0] & /*reset_styles*/
      2) {
        toggle_class(
          div1,
          "pagefind-ui--reset",
          /*reset_styles*/
          ctx2[1]
        );
      }
    },
    i(local) {
      if (current)
        return;
      transition_in(if_block0);
      transition_in(if_block1);
      current = true;
    },
    o(local) {
      transition_out(if_block0);
      transition_out(if_block1);
      current = false;
    },
    d(detaching) {
      if (detaching)
        detach(div1);
      ctx[34](null);
      ctx[35](null);
      if (if_block0)
        if_block0.d();
      if (if_block1)
        if_block1.d();
      mounted = false;
      run_all(dispose);
    }
  };
}
var submit_handler = (e) => e.preventDefault();
function instance4($$self, $$props, $$invalidate) {
  const availableTranslations = {}, languages = filenames.map((file) => file.match(/([^\/]+)\.json$/)[1]);
  for (let i = 0; i < languages.length; i++) {
    availableTranslations[languages[i]] = {
      language: languages[i],
      ...__default[i].strings
    };
  }
  let { base_path = "/pagefind/" } = $$props;
  let { page_size = 5 } = $$props;
  let { reset_styles = true } = $$props;
  let { show_images = true } = $$props;
  let { show_sub_results = false } = $$props;
  let { excerpt_length } = $$props;
  let { process_result = null } = $$props;
  let { process_term = null } = $$props;
  let { show_empty_filters = true } = $$props;
  let { open_filters = [] } = $$props;
  let { debounce_timeout_ms = 300 } = $$props;
  let { pagefind_options = {} } = $$props;
  let { merge_index = [] } = $$props;
  let { trigger_search_term = "" } = $$props;
  let { translations = {} } = $$props;
  let { autofocus = false } = $$props;
  let { sort = null } = $$props;
  let { selected_filters = {} } = $$props;
  let val = "";
  let pagefind;
  let input_el, clear_el, clear_width = 40;
  let initializing = false;
  let searchResult = [];
  let loading = false;
  let searched = false;
  let search_id = 0;
  let search_term = "";
  let show = page_size;
  let initial_filters = null;
  let available_filters = null;
  let automatic_translations = availableTranslations["en"];
  const translate = (key, auto, overrides) => {
    return overrides[key] ?? auto[key] ?? "";
  };
  onMount(() => {
    let lang = document?.querySelector?.("html")?.getAttribute?.("lang") || "en";
    let parsedLang = parse(lang.toLocaleLowerCase());
    $$invalidate(19, automatic_translations = availableTranslations[`${parsedLang.language}-${parsedLang.script}-${parsedLang.region}`] || availableTranslations[`${parsedLang.language}-${parsedLang.region}`] || availableTranslations[`${parsedLang.language}`] || availableTranslations["en"]);
  });
  onDestroy(() => {
    pagefind?.destroy?.();
    pagefind = null;
  });
  const init2 = async () => {
    if (initializing)
      return;
    $$invalidate(12, initializing = true);
    if (!pagefind) {
      let imported_pagefind;
      try {
        imported_pagefind = await import(`${base_path}pagefind.js`);
      } catch (e) {
        console.error(e);
        console.error([
          `Pagefind couldn't be loaded from ${this.options.bundlePath}pagefind.js`,
          `You can configure this by passing a bundlePath option to PagefindUI`
        ].join("\n"));
        if (document?.currentScript && document.currentScript.tagName.toUpperCase() === "SCRIPT") {
          console.error(`[DEBUG: Loaded from ${document.currentScript.src ?? "bad script location"}]`);
        } else {
          console.error("no known script location");
        }
      }
      if (!excerpt_length) {
        $$invalidate(24, excerpt_length = show_sub_results ? 12 : 30);
      }
      let opts = {
        ...pagefind_options || {},
        excerptLength: excerpt_length
      };
      await imported_pagefind.options(opts);
      for (const index of merge_index) {
        if (!index.bundlePath) {
          throw new Error("mergeIndex requires a bundlePath parameter");
        }
        const url = index.bundlePath;
        delete index["bundlePath"];
        await imported_pagefind.mergeIndex(url, index);
      }
      pagefind = imported_pagefind;
      loadFilters();
    }
  };
  const loadFilters = async () => {
    if (pagefind) {
      initial_filters = await pagefind.filters();
      if (!available_filters || !Object.keys(available_filters).length) {
        $$invalidate(18, available_filters = initial_filters);
      }
    }
  };
  const parseSelectedFilters = (filters) => {
    let filter = {};
    Object.entries(filters).filter(([, selected]) => selected).forEach(([selection]) => {
      let [key, value] = selection.split(/:(.*)$/);
      filter[key] = filter[key] || [];
      filter[key].push(value);
    });
    return filter;
  };
  let timer;
  const debouncedSearch = async (term, raw_filters) => {
    if (!term) {
      $$invalidate(15, searched = false);
      if (timer)
        clearTimeout(timer);
      return;
    }
    const filters = parseSelectedFilters(raw_filters);
    const executeSearchFunc = () => search(term, filters);
    if (debounce_timeout_ms > 0 && term) {
      if (timer)
        clearTimeout(timer);
      timer = setTimeout(executeSearchFunc, debounce_timeout_ms);
      await waitForApiInit();
      pagefind.preload(term, { filters });
    } else {
      executeSearchFunc();
    }
    updateForButtonWidth();
  };
  const waitForApiInit = async () => {
    while (!pagefind) {
      init2();
      await new Promise((resolve) => setTimeout(resolve, 50));
    }
  };
  const search = async (term, filters) => {
    $$invalidate(16, search_term = term || "");
    if (typeof process_term === "function") {
      term = process_term(term);
    }
    $$invalidate(14, loading = true);
    $$invalidate(15, searched = true);
    await waitForApiInit();
    const local_search_id = ++search_id;
    const search_options = { filters };
    if (sort && typeof sort === "object") {
      search_options.sort = sort;
    }
    const results = await pagefind.search(term, search_options);
    if (search_id === local_search_id) {
      if (results.filters && Object.keys(results.filters)?.length) {
        $$invalidate(18, available_filters = results.filters);
      }
      $$invalidate(13, searchResult = results);
      $$invalidate(14, loading = false);
      $$invalidate(17, show = page_size);
    }
  };
  const updateForButtonWidth = () => {
    const width = clear_el.offsetWidth;
    if (width != clear_width) {
      $$invalidate(10, input_el.style.paddingRight = `${width + 2}px`, input_el);
    }
  };
  const showMore = (e) => {
    e?.preventDefault();
    $$invalidate(17, show += page_size);
  };
  const keydown_handler = (e) => {
    if (e.key === "Escape") {
      $$invalidate(9, val = "");
      input_el.blur();
    }
    if (e.key === "Enter") {
      e.preventDefault();
    }
  };
  function input_input_handler() {
    val = this.value;
    $$invalidate(9, val), $$invalidate(23, trigger_search_term);
  }
  function input_binding($$value) {
    binding_callbacks[$$value ? "unshift" : "push"](() => {
      input_el = $$value;
      $$invalidate(10, input_el);
    });
  }
  function button_binding($$value) {
    binding_callbacks[$$value ? "unshift" : "push"](() => {
      clear_el = $$value;
      $$invalidate(11, clear_el);
    });
  }
  const click_handler = () => {
    $$invalidate(9, val = "");
    input_el.blur();
  };
  function filters_selected_filters_binding(value) {
    selected_filters = value;
    $$invalidate(0, selected_filters);
  }
  $$self.$$set = ($$props2) => {
    if ("base_path" in $$props2)
      $$invalidate(25, base_path = $$props2.base_path);
    if ("page_size" in $$props2)
      $$invalidate(26, page_size = $$props2.page_size);
    if ("reset_styles" in $$props2)
      $$invalidate(1, reset_styles = $$props2.reset_styles);
    if ("show_images" in $$props2)
      $$invalidate(2, show_images = $$props2.show_images);
    if ("show_sub_results" in $$props2)
      $$invalidate(3, show_sub_results = $$props2.show_sub_results);
    if ("excerpt_length" in $$props2)
      $$invalidate(24, excerpt_length = $$props2.excerpt_length);
    if ("process_result" in $$props2)
      $$invalidate(4, process_result = $$props2.process_result);
    if ("process_term" in $$props2)
      $$invalidate(27, process_term = $$props2.process_term);
    if ("show_empty_filters" in $$props2)
      $$invalidate(5, show_empty_filters = $$props2.show_empty_filters);
    if ("open_filters" in $$props2)
      $$invalidate(6, open_filters = $$props2.open_filters);
    if ("debounce_timeout_ms" in $$props2)
      $$invalidate(28, debounce_timeout_ms = $$props2.debounce_timeout_ms);
    if ("pagefind_options" in $$props2)
      $$invalidate(29, pagefind_options = $$props2.pagefind_options);
    if ("merge_index" in $$props2)
      $$invalidate(30, merge_index = $$props2.merge_index);
    if ("trigger_search_term" in $$props2)
      $$invalidate(23, trigger_search_term = $$props2.trigger_search_term);
    if ("translations" in $$props2)
      $$invalidate(7, translations = $$props2.translations);
    if ("autofocus" in $$props2)
      $$invalidate(8, autofocus = $$props2.autofocus);
    if ("sort" in $$props2)
      $$invalidate(31, sort = $$props2.sort);
    if ("selected_filters" in $$props2)
      $$invalidate(0, selected_filters = $$props2.selected_filters);
  };
  $$self.$$.update = () => {
    if ($$self.$$.dirty[0] & /*trigger_search_term*/
    8388608) {
      $:
        if (trigger_search_term) {
          $$invalidate(9, val = trigger_search_term);
          $$invalidate(23, trigger_search_term = "");
        }
    }
    if ($$self.$$.dirty[0] & /*val, selected_filters*/
    513) {
      $:
        debouncedSearch(val, selected_filters);
    }
  };
  return [
    selected_filters,
    reset_styles,
    show_images,
    show_sub_results,
    process_result,
    show_empty_filters,
    open_filters,
    translations,
    autofocus,
    val,
    input_el,
    clear_el,
    initializing,
    searchResult,
    loading,
    searched,
    search_term,
    show,
    available_filters,
    automatic_translations,
    translate,
    init2,
    showMore,
    trigger_search_term,
    excerpt_length,
    base_path,
    page_size,
    process_term,
    debounce_timeout_ms,
    pagefind_options,
    merge_index,
    sort,
    keydown_handler,
    input_input_handler,
    input_binding,
    button_binding,
    click_handler,
    filters_selected_filters_binding
  ];
}
var Ui = class extends SvelteComponent {
  constructor(options) {
    super();
    init(
      this,
      options,
      instance4,
      create_fragment4,
      safe_not_equal,
      {
        base_path: 25,
        page_size: 26,
        reset_styles: 1,
        show_images: 2,
        show_sub_results: 3,
        excerpt_length: 24,
        process_result: 4,
        process_term: 27,
        show_empty_filters: 5,
        open_filters: 6,
        debounce_timeout_ms: 28,
        pagefind_options: 29,
        merge_index: 30,
        trigger_search_term: 23,
        translations: 7,
        autofocus: 8,
        sort: 31,
        selected_filters: 0
      },
      null,
      [-1, -1]
    );
  }
};
var ui_default = Ui;

// ui-core.js
var scriptBundlePath;
try {
  if (document?.currentScript && document.currentScript.tagName.toUpperCase() === "SCRIPT") {
    scriptBundlePath = new URL(document.currentScript.src).pathname.match(
      /^(.*\/)(?:pagefind-)?ui.js.*$/
    )[1];
  }
} catch (e) {
  scriptBundlePath = "/pagefind/";
}
var PagefindUI = class {
  constructor(opts) {
    this._pfs = null;
    let selector = opts.element ?? "[data-pagefind-ui]";
    let bundlePath = opts.bundlePath ?? scriptBundlePath;
    let pageSize = opts.pageSize ?? 5;
    let resetStyles = opts.resetStyles ?? true;
    let showImages = opts.showImages ?? true;
    let showSubResults = opts.showSubResults ?? false;
    let excerptLength = opts.excerptLength ?? 0;
    let processResult = opts.processResult ?? null;
    let processTerm = opts.processTerm ?? null;
    let showEmptyFilters = opts.showEmptyFilters ?? true;
    let openFilters = opts.openFilters ?? [];
    let debounceTimeoutMs = opts.debounceTimeoutMs ?? 300;
    let mergeIndex = opts.mergeIndex ?? [];
    let translations = opts.translations ?? [];
    let autofocus = opts.autofocus ?? false;
    let sort = opts.sort ?? null;
    delete opts["element"];
    delete opts["bundlePath"];
    delete opts["pageSize"];
    delete opts["resetStyles"];
    delete opts["showImages"];
    delete opts["showSubResults"];
    delete opts["excerptLength"];
    delete opts["processResult"];
    delete opts["processTerm"];
    delete opts["showEmptyFilters"];
    delete opts["openFilters"];
    delete opts["debounceTimeoutMs"];
    delete opts["mergeIndex"];
    delete opts["translations"];
    delete opts["autofocus"];
    delete opts["sort"];
    const dom = selector instanceof HTMLElement ? selector : document.querySelector(selector);
    if (dom) {
      this._pfs = new ui_default({
        target: dom,
        props: {
          base_path: bundlePath,
          page_size: pageSize,
          reset_styles: resetStyles,
          show_images: showImages,
          show_sub_results: showSubResults,
          excerpt_length: excerptLength,
          process_result: processResult,
          process_term: processTerm,
          show_empty_filters: showEmptyFilters,
          open_filters: openFilters,
          debounce_timeout_ms: debounceTimeoutMs,
          merge_index: mergeIndex,
          translations,
          autofocus,
          sort,
          pagefind_options: opts
        }
      });
    } else {
      console.error(`Pagefind UI couldn't find the selector ${selector}`);
    }
  }
  triggerSearch(term) {
    this._pfs.$$set({ trigger_search_term: term });
  }
  triggerFilters(filters) {
    let selected_filters = {};
    for (let [filter, key] of Object.entries(filters)) {
      if (Array.isArray(key)) {
        for (let val of key) {
          selected_filters[`${filter}:${val}`] = true;
        }
      } else {
        selected_filters[`${filter}:${key}`] = true;
      }
    }
    this._pfs.$$set({ selected_filters });
  }
  destroy() {
    this._pfs.$destroy();
  }
};
// Annotate the CommonJS export names for ESM import in node:
0 && (module.exports = {
  PagefindUI
});
