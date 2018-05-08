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

exports.zaddImpl = function(conn) {
  return function(key) {
    return function(writeMode) {
      return function(returnMode) {
        return function(members) {
          return function(onError, onSuccess) {
            var args = [key];
            if(writeMode !== null) {
              args.push(writeMode);
            }
            if(returnMode !== null) {
              args.push(returnMode);
            }
            members.forEach(function(i) {
              args.push(i.score);
              args.push(i.member);
            });
            var handler = function(err, val) {
              if (err !== null) {
                onError(err);
                return;
              }
              onSuccess(val);
            };
            args.push(handler);
            conn.zaddBuffer.apply(conn, args);
            return function(cancelError, cancelerError, cancelerSuccess) {
              cancelError();
            };
          };
        };
      };
    };
  };
};

exports.zrangeImpl = function(conn) {
  return function(key) {
    return function(start) {
      return function(stop) {
        return function(onError, onSuccess) {
          var args = [key, start, stop, 'WITHSCORES'];
          var handler = function(err, val) {
            var curr = {}, result = [];
            if (err !== null) {
              onError(err);
              return;
            }
            val.forEach(function(i) {
              if(curr.member === undefined) {
                curr.member = i;
              } else {
                // XXX: I'm not sure if this is safe parsing
                curr.score = parseFloat(i);
                result.push(curr);
                curr = {};
              }
            });
            onSuccess(result);
          };
          args.push(handler);
          conn.zrangeBuffer.apply(conn, args);
          return function(cancelError, cancelerError, cancelerSuccess) {
            cancelError();
          };
        };
      };
    };
  };
};

exports.zincrbyImpl = function(conn) {
  return function(key) {
    return function(increment) {
      return function(member) {
        return function(onError, onSuccess) {
          var args = [key, increment, member];
          var handler = function(err, val) {
            if (err !== null) {
              onError(err);
              return;
            }
            onSuccess(parseFloat(val));
          };
          args.push(handler);
          conn.zincrbyBuffer.apply(conn, args);
          return function(cancelError, cancelerError, cancelerSuccess) {
            cancelError();
          };
        };
      };
    };
  };
};

exports.zrankImpl = function(conn) {
  return function(key) {
    return function(member) {
      return function(onError, onSuccess) {
        var args = [key, member];
        var handler = function(err, val) {
          if (err !== null) {
            onError(err);
            return;
          }
          if (val !== null) {
            onSuccess(parseFloat(val));
            return;
          }
          onSuccess(null);
        };
        args.push(handler);
        conn.zrankBuffer.apply(conn, args);
        return function(cancelError, cancelerError, cancelerSuccess) {
          cancelError();
        };
      };
    };
  };
};
