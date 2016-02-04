var tests = [];
for (var file in window.__karma__.files) {
  if (window.__karma__.files.hasOwnProperty(file)) {
    if (/Spec\.js$/.test(file)) {
      tests.push(file);
    }
  }
}

requirejs.config({
    // Karma serves files from '/base'
    baseUrl: '/base',

    paths: {
        thrift: 'src/thrift-core',
        thrift_echo_transport: 'src/thrift-transport-echo',
        thrift_xhr_transport: 'src/thrift-transport-xhr',
        thrift_binary_protocol: 'src/thrift-protocol-binary',
    },

    shim: {
        'underscore': {
            exports: '_'
        }
    },
    map: {
        '*': {
            'css': 'css',
            'promise': 'bluebird'
        }
    },

    // ask Require.js to load these files (all our tests)
    deps: tests,

    // start test run, once Require.js is done
    callback: window.__karma__.start
});