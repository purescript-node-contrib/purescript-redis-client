'use strict';

var Data_Maybe = require('../Data.Maybe');
var ioredis = require('ioredis');

ioredis.Command.setReplyTransformer('hgetall', function (result) {
  var arr = [];
  for (var i = 0; i < result.length; i += 2) {
    arr.push({ key: result[i], value: result[i + 1] });
  }
  return arr;
});

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

exports.flushdbImpl = function(conn) {
  return function(onError, onSuccess) {
    conn.flushdb();
    onSuccess();
    return function(cancelError, cancelerError, cancelerSuccess) {
      cancelError();
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

exports.hgetallImpl = function(conn) {
  return function(key) {
    return function(onError, onSuccess) {
      conn.hgetallBuffer(key, function(err, value) {
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

exports.hgetImpl = function(conn) {
  return function(key) {
    return function(field) {
      return function(onError, onSuccess) {
        conn.hgetBuffer(key, field, function(err, value) {
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
};

exports.hsetImpl = function(conn) {
  return function(key) {
    return function(field) {
      return function(value) {
        return function(onError, onSuccess) {
          conn.hsetBuffer(key, field, value, function(err, value) {
            if (err !== null) {
              onError(err);
              return;
            }
            onSuccess(parseInt(value));
          });
          return function(cancelError, cancelerError, cancelerSuccess) {
            cancelError();
          };
        };
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

exports.lpopImpl = function(conn) {
  return function(key) {
    return function(onError, onSuccess) {
      var handler = function(err, val) {
        if (err !== null) {
          onError(err);
          return;
        }
        if (val !== null) {
          onSuccess(val);
          return;
        }
      };
      conn.lpopBuffer.apply(conn, [key, handler]);
      return function(cancelError, cancelerError, cancelerSuccess) {
        cancelError();
      };
    };
  };
};

exports.lpushImpl = function(conn) {
  return function(key) {
    return function(value) {
      return function(onError, onSuccess) {
        var handler = function(err, val) {
          if (err !== null) {
            onError(err);
            return;
          }
          onSuccess(parseInt(val));
        };
        conn.lpushBuffer.apply(conn, [key, value, handler]);
        return function(cancelError, cancelerError, cancelerSuccess) {
          cancelError();
        };
      };
    };
  };
};

exports.lrangeImpl = function(conn) {
  return function(key) {
    return function(start) {
      return function(end) {
        return function(onError, onSuccess) {
          var handler = function(err, val) {
            if (err !== null) {
              onError(err);
              return;
            }
            onSuccess(val);
          };
          conn.lrangeBuffer.apply(conn, [key, start, end, handler]);
          return function(cancelError, cancelerError, cancelerSuccess) {
            cancelError();
          };
        };
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

exports.zcardImpl = function(conn) {
  return function(key) {
    return function(onError, onSuccess) {
      var handler = function(err, val) {
        if (err !== null) {
          onError(err);
          return;
        }
        onSuccess(parseInt(val));
      };
      conn.zcardBuffer.apply(conn, [key, handler]);
      return function(cancelError, cancelerError, cancelerSuccess) {
        cancelError();
      };
    };
  };
};

exports.zrangeImpl = function(conn) {
  return function(key) {
    return function(start) {
      return function(stop) {
        return function(onError, onSuccess) {
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
          conn.zrangeBuffer.apply(conn, [key, start, stop, 'WITHSCORES', handler]);
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
          var handler = function(err, val) {
            if (err !== null) {
              onError(err);
              return;
            }
            onSuccess(parseFloat(val));
          };
          conn.zincrbyBuffer.apply(conn, [key, increment, member, handler]);
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
        var handler = function(err, val) {
          if (err !== null) {
            onError(err);
            return;
          }
          if (val !== null) {
            onSuccess(parseInt(val));
            return;
          }
          onSuccess(null);
        };
        conn.zrankBuffer.apply(conn, [key, member, handler]);
        return function(cancelError, cancelerError, cancelerSuccess) {
          cancelError();
        };
      };
    };
  };
};
