'use strict';

import ioredis from 'ioredis';

ioredis.Command.setReplyTransformer('hgetall', function (result) {
  var arr = [];
  for (var i = 0; i < result.length; i += 2) {
    arr.push({ key: result[i], value: result[i + 1] });
  }
  return arr;
});

export const _handleBlockingPopResult = function(onError, onSuccess) {
  return function(err, val) {
    if (err !== null) {
      onError(err);
      return;
    }
    if(val !== null) {
      var key = val[0], value = val[1];
      onSuccess({key: key, value: value});
      return;
    }
    onSuccess(null);
  };
};

export const blpopImpl = function(conn) {
  return function(keys) {
    return function(timeout) {
      return function(onError, onSuccess) {
        var handler = _handleBlockingPopResult(onError, onSuccess);
        conn.blpopBuffer.apply(conn, [keys, timeout, handler]);
        return function(cancelError, cancelerError, cancelerSuccess) {
          cancelError();
        };
      };
    };
  };
};

export const brpopImpl = function(conn) {
  return function(keys) {
    return function(timeout) {
      return function(onError, onSuccess) {
        var handler = _handleBlockingPopResult(onError, onSuccess);
        conn.brpopBuffer.apply(conn, [keys, timeout, handler]);
        return function(cancelError, cancelerError, cancelerSuccess) {
          cancelError();
        };
      };
    };
  };
};

export const connectImpl = function(connstr) {
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

export const disconnectImpl = function(conn) {
  return function(onError, onSuccess) {
    conn.disconnect();
    onSuccess();
    return function(cancelError, cancelerError, cancelerSuccess) {
      cancelError();
    };
  };
};

export const delImpl = function(conn) {
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

export const flushdbImpl = function(conn) {
  return function(onError, onSuccess) {
    conn.flushdb();
    onSuccess();
    return function(cancelError, cancelerError, cancelerSuccess) {
      cancelError();
    };
  };
};

export const getImpl = function(conn) {
  return function(key) {
    return function(onError, onSuccess) {
      conn.getBuffer(key, function(err, value) {
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

export const hgetallImpl = function(conn) {
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

export const hgetImpl = function(conn) {
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

export const hsetImpl = function(conn) {
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

export const incrImpl = function(conn) {
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

export const keysImpl = function(conn) {
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

export const lpopImpl = function(conn) {
  return function(key) {
    return function(onError, onSuccess) {
      var handler = _plainValueHandler(onError, onSuccess);
      conn.lpopBuffer.apply(conn, [key, handler]);
      return function(cancelError, cancelerError, cancelerSuccess) {
        cancelError();
      };
    };
  };
};

export const lpushImpl = function(conn) {
  return function(key) {
    return function(value) {
      return function(onError, onSuccess) {
        var handler = _intHandler(onError, onSuccess, false);
        conn.lpushBuffer.apply(conn, [key, value, handler]);
        return function(cancelError, cancelerError, cancelerSuccess) {
          cancelError();
        };
      };
    };
  };
};

export const rpopImpl = function(conn) {
  return function(key) {
    return function(onError, onSuccess) {
      var handler = _plainValueHandler(onError, onSuccess);
      conn.rpopBuffer.apply(conn, [key, handler]);
      return function(cancelError, cancelerError, cancelerSuccess) {
        cancelError();
      };
    };
  };
};

export const rpushImpl = function(conn) {
  return function(key) {
    return function(value) {
      return function(onError, onSuccess) {
        var handler = _intHandler(onError, onSuccess, false);
        conn.rpushBuffer.apply(conn, [key, value, handler]);
        return function(cancelError, cancelerError, cancelerSuccess) {
          cancelError();
        };
      };
    };
  };
};

export const lrangeImpl = function(conn) {
  return function(key) {
    return function(start) {
      return function(end) {
        return function(onError, onSuccess) {
          var handler = _plainValueHandler(onError, onSuccess);
          conn.lrangeBuffer.apply(conn, [key, start, end, handler]);
          return function(cancelError, cancelerError, cancelerSuccess) {
            cancelError();
          };
        };
      };
    };
  };
};

export const _plainValueHandler = function(onError, onSuccess) {
  return function(err, val) {
    if (err !== null) {
      onError(err);
      return;
    }
    onSuccess(val);
  };
};

export const _nullValueHandler = function(onError, onSuccess) {
  return function(err, val) {
    if (err !== null) {
      onError(err);
      return;
    }
    onSuccess(null);
  };
};

export const ltrimImpl = function(conn) {
  return function(key) {
    return function(start) {
      return function(end) {
        return function(onError, onSuccess) {
          var handler = _nullValueHandler(onError, onSuccess);
          conn.ltrimBuffer.apply(conn, [key, start, end, handler]);
          return function(cancelError, cancelerError, cancelerSuccess) {
            cancelError();
          };
        };
      };
    };
  };
};

export const mgetImpl = function(conn) {
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

export const setImpl = function(conn) {
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

export const zaddImpl = function(conn) {
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

export const zcardImpl = function(conn) {
  return function(key) {
    return function(onError, onSuccess) {
      conn.zcardBuffer.apply(conn, [key, _intHandler(onError, onSuccess, false)]);
      return function(cancelError, cancelerError, cancelerSuccess) {
        cancelError();
      };
    };
  };
};

export const zincrbyImpl = function(conn) {
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

export const _intHandler = function(onError, onSuccess, nullable) {
  return function(err, val) {
    if (err !== null) {
      onError(err);
      return;
    }
    if(nullable && val === null) {
      onSuccess(null);
    } else {
      onSuccess(parseInt(val));
    }
    return;
  };
};

export const _sortedSetItemsHanlder = function(onError, onSuccess) {
  return function(err, val) {
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
};

export const zrangeImpl = function(conn) {
  return function(key) {
    return function(start) {
      return function(stop) {
        return function(onError, onSuccess) {
          var handler = _sortedSetItemsHanlder(onError, onSuccess);
          conn.zrangeBuffer.apply(conn, [key, start, stop, 'WITHSCORES', handler]);
          return function(cancelError, cancelerError, cancelerSuccess) {
            cancelError();
          };
        };
      };
    };
  };
};

export const zrangebyscoreImpl = function(conn) {
  return function(key) {
    return function(min) {
      return function(max) {
        return function(limit) {
          return function(onError, onSuccess) {
            var handler = _sortedSetItemsHanlder(onError, onSuccess),
                args =[key, min, max, 'WITHSCORES'];
            if(limit !== null) {
              args.push('LIMIT');
              args.push(limit.offset);
              args.push(limit.count);
            }
            args.push(handler);
            conn.zrangebyscoreBuffer.apply(conn, args);
          };
        };
      };
    };
  };
};

export const zrankImpl = function(conn) {
  return function(key) {
    return function(member) {
      return function(onError, onSuccess) {
        conn.zrankBuffer.apply(conn, [key, member, _intHandler(onError, onSuccess, true)]);
        return function(cancelError, cancelerError, cancelerSuccess) {
          cancelError();
        };
      };
    };
  };
};

export const zremImpl = function(conn) {
  return function(key) {
    return function(members) {
      return function(onError, onSuccess) {
        conn.zremBuffer.apply(conn, [key, members, _intHandler(onError, onSuccess, false)]);
        return function(cancelError, cancelerError, cancelerSuccess) {
          cancelError();
        };
      };
    };
  };
};

export const zremrangebylexImpl = function(conn) {
  return function(key) {
    return function(min) {
      return function(max) {
        return function(onError, onSuccess) {
          conn.zremrangebylexBuffer.apply(conn, [key, min, max, _intHandler(onError, onSuccess, false)]);
          return function(cancelError, cancelerError, cancelerSuccess) {
            cancelError();
          };
        };
      };
    };
  };
};

export const zremrangebyrankImpl = function(conn) {
  return function(key) {
    return function(start) {
      return function(stop) {
        return function(onError, onSuccess) {
          conn.zremrangebyrankBuffer.apply(conn, [key, start, stop, _intHandler(onError, onSuccess, false)]);
          return function(cancelError, cancelerError, cancelerSuccess) {
            cancelError();
          };
        };
      };
    };
  };
};

export const zremrangebyscoreImpl = function(conn) {
  return function(key) {
    return function(min) {
      return function(max) {
        return function(onError, onSuccess) {
          conn.zremrangebyscoreBuffer.apply(conn, [key, min, max, _intHandler(onError, onSuccess, false)]);
          return function(cancelError, cancelerError, cancelerSuccess) {
            cancelError();
          };
        };
      };
    };
  };
};

export const zrevrangebyscoreImpl = function(conn) {
  return function(key) {
    return function(min) {
      return function(max) {
        return function(limit) {
          return function(onError, onSuccess) {
            var handler = _sortedSetItemsHanlder(onError, onSuccess),
                args =[key, min, max, 'WITHSCORES'];
            if(limit !== null) {
              args.push('LIMIT');
              args.push(limit.offset);
              args.push(limit.count);
            }
            args.push(handler);
            conn.zrevrangebyscoreBuffer.apply(conn, args);
          };
        };
      };
    };
  };
};

export const zscoreImpl = function(conn) {
  return function(key) {
    return function(member) {
      return function(onError, onSuccess) {
        var handler = function(err, val) {
          if (err !== null) {
            onError(err);
            return;
          }
          if(val !== null) {
            onSuccess(parseFloat(val));
          } else {
            onSuccess(val);
          }
        };
        conn.zscoreBuffer.apply(conn, [key, member, handler]);
      };
    };
  };
};
