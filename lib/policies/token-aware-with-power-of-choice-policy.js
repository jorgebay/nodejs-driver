'use strict';

const util = require('util');
const types = require('../types');
const loadBalancing = require('./load-balancing');
const distance = types.distance;
const LoadBalancingPolicy = loadBalancing.LoadBalancingPolicy;
const doneIteratorObject = Object.freeze({ done: true });

/**
 * A wrapper load balancing policy that add token awareness to a child policy.
 * @extends LoadBalancingPolicy
 * @constructor
 */
function TokenAwarePolicy () {
  this.childPolicy = new loadBalancing.DCAwareRoundRobinPolicy();
}

util.inherits(TokenAwarePolicy, LoadBalancingPolicy);

TokenAwarePolicy.prototype.init = function (client, hosts, callback) {
  this.client = client;
  this.hosts = hosts;
  this.childPolicy.init(client, hosts, callback);
};

TokenAwarePolicy.prototype.getDistance = function (host) {
  return this.childPolicy.getDistance(host);
};

/**
 * Returns the hosts to use for a new query.
 * The returned plan will return local replicas first, if replicas can be determined, followed by the plan of the
 * child policy.
 * @param {String} keyspace Name of currently logged keyspace at <code>Client</code> level.
 * @param {QueryOptions} queryOptions Options related to this query execution.
 * @param {Function} callback The function to be invoked with the error as first parameter and the host iterator as
 * second parameter.
 */
TokenAwarePolicy.prototype.newQueryPlan = function (keyspace, queryOptions, callback) {
  var routingKey;
  if (queryOptions) {
    routingKey = queryOptions.routingKey;
    if (queryOptions.keyspace) {
      keyspace = queryOptions.keyspace;
    }
  }
  var replicas;
  if (routingKey) {
    replicas = this.client.getReplicas(keyspace, routingKey);
  }
  if (!routingKey || !replicas) {
    return this.childPolicy.newQueryPlan(keyspace, queryOptions, callback);
  }
  var iterator = new TokenAwareIterator(keyspace, queryOptions, replicas, this.childPolicy);
  iterator.iterate(callback);
};

/**
 * An iterator that holds the context for the subsequent next() calls
 * @param {String} keyspace
 * @param queryOptions
 * @param {Array} replicas
 * @param childPolicy
 * @constructor
 * @ignore
 */
function TokenAwareIterator(keyspace, queryOptions, replicas, childPolicy) {
  this.keyspace = keyspace;
  this.childPolicy = childPolicy;
  this.queryOptions = queryOptions;
  this.localReplicas = [];
  this.replicaIndex = 0;
  this.replicaMap = {};
  this.childIterator = null;
  // Memoize the local replicas
  // The amount of local replicas should be defined before start iterating, in order to select an
  // appropriate (pseudo random) startIndex
  for (var i = 0; i < replicas.length; i++) {
    var host = replicas[i];
    if (this.childPolicy.getDistance(host) !== distance.local) {
      continue;
    }
    this.replicaMap[host.address] = true;
    this.localReplicas.push(host);
  }
  shuffleArray(this.localReplicas);
  if (this.localReplicas.length >= 3) {
    this.chooseLessBusy();
  }
}

TokenAwareIterator.prototype.iterate = function (callback) {
  //Load the child policy hosts
  var self = this;
  this.childPolicy.newQueryPlan(this.keyspace, this.queryOptions, function (err, iterator) {
    if (err) {
      return callback(err);
    }
    //get the iterator of the child policy in case is needed
    self.childIterator = iterator;
    callback(null, {
      next: function () { return self.computeNext(); }
    });
  });
};

TokenAwareIterator.prototype.computeNext = function () {
  var host;
  if (this.replicaIndex < this.localReplicas.length) {
    host = this.localReplicas[this.replicaIndex++];
    return { value: host, done: false };
  }
  // Return hosts from child policy
  var item;
  while ((item = this.childIterator.next()) && !item.done) {
    if (this.replicaMap[item.value.address]) {
      // Avoid yielding local replicas from the child load balancing policy query plan
      continue;
    }
    return item;
  }
  return doneIteratorObject;
};

TokenAwareIterator.prototype.chooseLessBusy = function () {
  if (getTotalInFlight(this.localReplicas[0]) <= getTotalInFlight(this.localReplicas[1])) {
    return;
  }
  var temp = this.localReplicas[1];
  this.localReplicas[1] = this.localReplicas[0];
  this.localReplicas[0] = temp;
};

function getTotalInFlight(host) {
  var result = 0;
  for (var i = 0; i < host.pool.connections.length; i++) {
    result += host.pool.connections[i].getInFlightAsFirst();
  }
  return result;
}

/**
 * Shuffles an Array in-place.
 * @param {Array} arr
 */
function shuffleArray(arr) {
  // Modern Fisher–Yates algorithm
  for (var i = arr.length - 1; i > 0; i--) {
    // Math.random() has an extremely short permutation cycle length but we don't care about collisions
    var j = Math.floor(Math.random() * (i + 1));
    var temp = arr[i];
    arr[i] = arr[j];
    arr[j] = temp;
  }
}

module.exports = TokenAwarePolicy;