"use strict";
var assert = require('assert');

var helper = require('../../test-helper');
var Client = require('../../../lib/client');
var utils = require('../../../lib/utils');
var types = require('../../../lib/types');
var policies = require('../../../lib/policies');
var errors = require('../../../lib/errors');
var Lbp = require('../../../lib/policies/token-aware-with-power-of-choice-policy');

describe('pwoc', function () {
  this.timeout(60000);
  helper.setup(3, {
    ccmOptions: {
      vnodes: false
    },
    queries: [
      "CREATE KEYSPACE test_power_choice WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 3}" +
      "  and durable_writes = false",
      "CREATE TABLE test_power_choice.table1 (key text PRIMARY KEY, value text)",
      "INSERT INTO test_power_choice.table1 (key, value) VALUES ('key1', 'value1')"
    ]
  });
  describe('pwoc policy', function () {
    it('should use spec in-flight count', function (done) {
      const client = new Client({
        contactPoints: ['127.0.0.1'],
        keyspace: 'test_power_choice',
        policies: {
          loadBalancing: new Lbp(),
          speculativeExecution: new policies.speculativeExecution.ConstantSpeculativeExecutionPolicy(50, 1)
        },
        pooling: { warmup: true }
      });
      utils.series([
        function (next) {
          client.connect(next);
        },
        function (next) {
          utils.timesLimit(5000, 100, function (n, timesNext) {
            if (n % 256 === 0){
              //var state = client.getState();
              //console.log('state', state._inFlightQueries);
            }
            client.execute('SELECT value FROM table1 WHERE key = ?', ['key1'], { prepare: true, isIdempotent: true }, timesNext);
          }, next);
        }
      ], done);
    });
  });
});