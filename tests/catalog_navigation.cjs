const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const source = fs.readFileSync(`${__dirname}/../frontend/index.html`, 'utf8');
const startupStart = source.indexOf('function sectionFromPath()');
const startup = source.slice(startupStart, source.indexOf('</script>', startupStart));
for (const pathname of ['/', '/rnp', '/RNP']) {
  let opened;
  const context = {
    location: {pathname, href: `https://example.test${pathname}`},
    history: {state: null, replaceState(state) { this.state = state; }},
    document: {querySelector: () => ({})},
    window: {addEventListener() {}},
    showSection: section => { opened = section; },
    // Model a slow or hanging unrelated dashboard-data request.
    loadData: () => new Promise(() => {}),
  };
  vm.runInNewContext(startup, context);
  assert.equal(opened, pathname === '/' ? 'catalog' : 'growth', 'Page must open before dashboard-data finishes');
}
assert(source.includes("if(id === 'wb-products') renderWbProducts(false);"));
assert(!source.includes('/api/wb-products?refresh='), 'Polling must not restart catalog builds');
console.log('Startup and catalog navigation checks passed');
