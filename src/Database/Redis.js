'use strict';

var Control_Monad_Aff = require('../Control.Monad.Aff');
var Data_Maybe = require('../Data.Maybe');
var ioredis = require('ioredis');

exports.connectImpl = function(connstr) {
  return function(onError, onSuccess) {
    var redis = new ioredis(connstr);
    redis.connect(function() {
      onSuccess(redis);
    });
    return function(cancelError, cancelerError, cancelerSuccess) {
      cancelError();
    };
  };
};


exports.disconnectImpl = function(conn) {
  return function(onError, onSuccess) {
    conn.disconnect();
    onSuccess();
    return function(cancelError, cancelerError, cancelerSuccess) {
      cancelError();
    };
  };
};

exports.flushdbImpl = function(conn) {
  return function(onError, onSuccess) {
    conn.flushdb();
    onSuccess();
    return function(cancelError, cancelerError, cancelerSuccess) {
      cancelError();
    };
  };
};

exports.delImpl = function(conn) {
  return function(keys) {
    return function(onError, onSuccess) {
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
      return function(cancelError, cancelerError, cancelerSuccess) {
        cancelError();
      };
    };
  };
};

exports.setImpl = function(conn) {
  return function(key) {
    return function(value) {
      return function(expire) {
        return function(write) {
          return function(onError, onSuccess) {
            var handler = function(err) {
              if (err !== null) {
                onError(err);
                return;
              }
              onSuccess();
            };
            var args = [key, value];
            if(expire !== null) {
              args.push(expire.unit);
              args.push(expire.value);
            }
            if(write !== null) {
              args.push(write);
            }
            args.push(handler);
            conn.set.apply(conn, args);
            return function(cancelError, cancelerError, cancelerSuccess) {
              cancelError();
            };
          };
        };
      };
    };
  };
};

exports.getImpl = function(conn) {
  return function(key) {
    return function(onError, onSuccess) {
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
      return function(cancelError, cancelerError, cancelerSuccess) {
        cancelError();
      };
    };
  };
};

exports.incrImpl = function(conn) {
  return function(key) {
    return function(onError, onSuccess) {
      conn.incr(key, function(err, value) {
        if (err !== null) {
          onError(err);
          return;
        }
        onSuccess(value);
      });
      return function(cancelError, cancelerError, cancelerSuccess) {
        cancelError();
      };
    };
  };
};

exports.keysImpl = function(conn) {
  return function(pattern) {
    return function(onError, onSuccess) {
      conn.keysBuffer(pattern, function(err, value) {
        if (err !== null) {
          onError(err);
          return;
        }
        onSuccess(value);
      });
      return function(cancelError, cancelerError, cancelerSuccess) {
        cancelError();
      };
    };
  };
};

exports.mgetImpl = function(conn) {
  return function(keys) {
    return function(onError, onSuccess) {
      conn.mgetBuffer(keys, function(err, value) {
        if (err !== null) {
          onError(err);
          return;
        }
        onSuccess(value);
      });
      return function(cancelError, cancelerError, cancelerSuccess) {
        cancelError();
      };
    };
  };
};
