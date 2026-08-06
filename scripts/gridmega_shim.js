// Node stub for the browser globals docs/backtest-engine.js touches at load time
// (SLICE_BASE reads location.*; localStorage is only hit by functions we never call).
globalThis.location = { hostname: 'dhruvan246.github.io', protocol: 'https:', href: 'https://dhruvan246.github.io/stocks-dashboard/' };
globalThis.localStorage = { getItem: () => null, setItem: () => {}, removeItem: () => {} };
