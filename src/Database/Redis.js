'use strict';

var Control_Monad_Aff = require('../Control.Monad.Aff');
var Data_Maybe = require('../Data.Maybe');
var ioredis = require('ioredis');

exports.connect = function(connstr) {
  return function(onSuccess, onError) {
    onSuccess(new ioredis(connstr));
    return Control_Monad_Aff.nonCanceler;
  };
};

exports.disconnect = function(conn) {
  return function(onSuccess, onError) {
    conn.disconnect();
    onSuccess();
    return Control_Monad_Aff.nonCanceler;
  };
};

exports.del = function(conn) {
  return function(keys) {
    return function(onSuccess, onError) {
      if (keys.length === 0) {
        onSuccess();
      } else {
        conn.del.apply(conn, keys.concat([function(err) {
          if (err !== null) {
            onError(err);
            return;
          }
          onSuccess();
        }]));
      }
      return Control_Monad_Aff.nonCanceler;
    };
  };
};

exports.set = function(conn) {
  return function(key) {
    return function(value) {
      return function(onSuccess, onError) {
        conn.set(key, value, function(err) {
          if (err !== null) {
            onError(err);
            return;
          }
          onSuccess();
        });
        return Control_Monad_Aff.nonCanceler;
      };
    };
  };
};

exports.get = function(conn) {
  return function(key) {
    return function(onSuccess, onError) {
      conn.getBuffer(key, function(err, value) {
        if (err !== null) {
          onError(err);
          return;
        }
        var valueMaybe = value === null
          ? Data_Maybe.Nothing.value
          : new Data_Maybe.Just(value);
        onSuccess(valueMaybe);
      });
      return Control_Monad_Aff.nonCanceler;
    };
  };
};

exports.incr = function(conn) {
  return function(key) {
    return function(onSuccess, onError) {
      conn.incr(key, function(err, value) {
        if (err !== null) {
          onError(err);
          return;
        }
        onSuccess(value);
      });
      return Control_Monad_Aff.nonCanceler;
    };
  };
};

exports.keys = function(conn) {
  return function(pattern) {
    return function(onSuccess, onError) {
      conn.keysBuffer(pattern, function(err, value) {
        if (err !== null) {
          onError(err);
          return;
        }
        onSuccess(value);
      });
      return Control_Monad_Aff.nonCanceler;
    };
  };
};
