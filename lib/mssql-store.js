/*jslint node: true*/
/*jslint asi: true */
/* Copyright (c) 2012-2013 Marian Radulescu */
"use strict";

var _ = require('underscore');
var sql = require('mssql');
var util = require('util')
var uuid = require('node-uuid');
var relationalstore = require('./relational-util')

var name = 'mssql-store';


module.exports = function (seneca, opts, cb) {

  var desc;

  var dbinst = null
  var specifications = null

  var upperCaseRegExp = /[A-Z]/g

  function camelToSnakeCase(field) {
    // replace "camelCase" with "camel_case"
    upperCaseRegExp.lastIndex = 0 // just to be sure. does not seem necessay. String.replace seems to reset the regexp each time.
    return field.replace(upperCaseRegExp, function(str, offset) {
      return('_'+str.toLowerCase());
    })
  }

  function snakeToCamelCase(column) {
    // replace "snake_case" with "snakeCase"
    var arr = column.split('_')
    var field = arr[0]
    for(var i = 1 ; i < arr.length ; i++) {
      field += arr[i][0].toUpperCase() + arr[i].slice(1, arr[i].length)
    }
    return field
  }

  function transformDBRowToJSObject(row) {
    var obj = {}
    for(var attr in row) {
      if(row.hasOwnProperty(attr)) {
        obj[snakeToCamelCase(attr)] = row[attr]
      }
    }
    return obj
  }


  function error(query, args, err, cb) {
    if (err) {
      var errorDetails = {
        message: err.message,
        err: err,
        stack: err.stack,
        query: query
      }
      seneca.log.error('Query Failed', JSON.stringify(errorDetails, null, 1))
      seneca.fail({code: 'entity/error', store: name}, cb)

      return true
    }

    return false
  }


  function configure(spec, cb) {
    specifications = spec

    var conf = 'string' === typeof(spec) ? null : spec

    if (!conf) {
      conf = {}

      var urlM = /^mssql:\/\/((.*?):(.*?)@)?(.*?)(:?(\d+))?\/(.*?)$/.exec(spec);
      conf.name = urlM[7]
      conf.port = urlM[6]
      conf.host = urlM[4]
      conf.username = urlM[2]
      conf.password = urlM[3]

      conf.port = conf.port ? parseInt(conf.port, 10) : null
    }

    conf.host = conf.host || conf.server
    conf.username = conf.username || conf.user
    conf.password = conf.password || conf.pass

    var sqlconf = {
      user: conf.username,
      password: conf.password,
      server: conf.host,
      port: conf.port,
      database: conf.name
    }

    dbinst = new sql.Connection(sqlconf, function(err) {
      if (err) {
        seneca.log.error('Connection error',err)
      }
      cb(err)
    });

  }


  var store = {

    name: name,

    close: function (cb) {
      if (dbinst) {
        dbinst.end(cb)
      }
    },


    save: function (args, cb) {
      var ent = args.ent
      var query;
      var update = !!ent.id;

      if (update) {
        query = updatestm(ent)

        var request = new sql.Request(dbinst)
        _.each(query.values, function(val, idx) {
          request.input('p'+(idx+1), val);
        })
        request.query(query.text, function (err, res) {
          if (!error(query, args, err, cb)) {
            seneca.log(args.tag$, 'update', ent)
            cb(null, ent)
          }
          else {
            seneca.fail({code: 'update', tag: args.tag$, store: store.name, query: query, error: err}, cb)
          }
        })
      }
      else {
        ent.id = ent.id$ || uuid()

        query = savestm(ent)

        var request = new sql.Request(dbinst)
        _.each(query.values, function(val, idx) {
          request.input('p'+(idx+1), val);
        })
        request.query(query.text, function (err, res) {
          if (!error(query, args, err, cb)) {
            seneca.log(args.tag$, 'save', ent)
            cb(null, ent)
          }
          else {
            seneca.log.error(query.text, query.values, err)
            seneca.fail({code: 'save', tag: args.tag$, store: store.name, query: query, error: err}, cb)
          }
        })
      }
    },


    load: function (args, cb) {
      var qent = args.qent
      var q = args.q

      var query = selectstm(qent, q)
      var trace = new Error()

      var request = new sql.Request(dbinst)
      _.each(query.values, function(val, idx) {
        request.input('p'+(idx+1), val);
      })
      request.query(query.text, function (err, recordset) {
        if (!error(query, args, err, cb)) {
          var ent
          if(recordset && recordset.length > 0) {
            var attrs = transformDBRowToJSObject(recordset[0])
            ent = relationalstore.makeent(qent, attrs)
          }
          seneca.log(args.tag$, 'load', ent)
          cb(null, ent)
        }
        else {
          seneca.log.error(query.text, query.values, trace.stack)
          seneca.fail({code: 'load', tag: args.tag$, store: store.name, query: query, error: err}, cb)
        }
      })
    },


    list: function (args, cb) {
      var qent = args.qent
      var q = args.q

      var list = []

      var query = selectstm(qent, q)

      var request = new sql.Request(dbinst)
      _.each(query.values, function(val, idx) {
        request.input('p'+(idx+1), val);
      })
      request.query(query.text, function (err, recordset) {
        if (!error(query, args, err, cb)) {
          recordset.forEach(function (row) {
            var attrs = transformDBRowToJSObject(row)
            var ent = relationalstore.makeent(qent, attrs)
            list.push(ent)
          })
          seneca.log(args.tag$, 'list', list.length, list[0])
          cb(null, list)
        }
        else {
          seneca.fail({code: 'list', tag: args.tag$, store: store.name, query: query, error: err}, cb)
        }
      })
    },


    remove: function (args, cb) {
      var qent = args.qent
      var q = args.q

      if (q.all$) {
        var query = deletestm(qent, q)

        var request = new sql.Request(dbinst)
        _.each(query.values, function(val, idx) {
          request.input('p'+(idx+1), val);
        })
        request.multiple = true;
        request.query(query.text, function (err, recordset) {
          if (!error(query, args, err, cb)) {
            var rowcount = (recordset && recordset.length) ? recordset[0].count : 0;
            seneca.log(args.tag$, 'remove', rowcount)
            cb(null, rowcount)
          }
          else {
            cb(err || new Error('no candidate for deletion'), undefined)
          }
        })
      }
      else {
        var selectQuery = selectstm(qent, q)

        var selectRequest = new sql.Request(dbinst)
        _.each(selectQuery.values, function(val, idx) {
          selectRequest.input('p'+(idx+1), val);
        })
        selectRequest.query(selectQuery.text, function (err, recordset) {
          if (!error(selectQuery, args, err, cb)) {

            var entp = recordset[0]

            if(!entp) {
              cb(new Error('no candidate for deletion'), undefined)
            } else {

              var query = deletestm(qent, {id: entp.id})

              var request = new sql.Request(dbinst)
              _.each(query.values, function(val, idx) {
                request.input('p'+(idx+1), val);
              })
              request.query(query.text, function (err, recordset) {
                if (!err) {
                  var rowcount = (recordset && recordset.length) ? recordset[0].count : 0;
                  seneca.log(args.tag$, 'remove', rowcount)
                  cb(null, rowcount)
                }
                else {
                  cb(err, undefined)
                }
              })
            }
          } else {

            var errorDetails = {
              message: err.message,
              err: err,
              stack: err.stack,
              query: query
            }
            seneca.log.error('Query Failed', JSON.stringify(errorDetails, null, 1))
            callback(err, undefined)
          }
        })
      }
    },


    native: function (args, done) {
//      dbinst.collection('seneca', function(err,coll){
//        if( !error(args,err,cb) ) {
//          coll.findOne({},{},function(err,entp){
//            if( !error(args,err,cb) ) {
//              done(null,dbinst)
//            }else{
//              done(err)
//            }
//          })
//        }else{
//          done(err)
//        }
//      })
    }

  }


  var savestm = function (ent) {
    var stm = {}

    var table = relationalstore.tablename(ent)
    var entp = relationalstore.makeentp(ent)
    var fields = _.keys(entp)

    var values = []
    var params = []

    var cnt = 0

    var escapedFields = []
    fields.forEach(function (field) {
      escapedFields.push('"' + escapeStr(camelToSnakeCase(field)) + '"')
      values.push(entp[field])
      params.push('@p' + (++cnt))
    })

    stm.text = 'INSERT INTO ' + escapeStr(table) + ' (' + escapedFields + ') values (' + escapeStr(params) + ')'
    stm.values = values

    return stm
  }


  var updatestm = function (ent) {
    var stm = {}

    var table = relationalstore.tablename(ent)
    var entp = relationalstore.makeentp(ent)
    var fields = _.keys(entp)

    var values = []
    var params = []
    var cnt = 0

    fields.forEach(function (field) {
      if (!(_.isUndefined(entp[field]) || _.isNull(entp[field]))) {
        values.push(entp[field])
        params.push('"' + escapeStr(camelToSnakeCase(field)) + '"=@p' + (++cnt))
      }
    })

    stm.text = "UPDATE " + escapeStr(table) + " SET " + params + " WHERE id='" + escapeStr(ent.id) + "'"
    stm.values = values

    return stm
  }


  var deletestm = function (qent, q) {
    var stm = {}

    var table = relationalstore.tablename(qent)
    var entp = relationalstore.makeentp(qent)

    var values = []
    var params = []

    var cnt = 0

    var w = whereargs(entp, q)

    var wherestr = ''

    if (!_.isEmpty(w) && w.params.length > 0) {
      w.params.forEach(function (param) {
        params.push('"' + escapeStr(camelToSnakeCase(param)) + '"=@p' + (++cnt))
      })

      if (!_.isEmpty(w.values)) {
        w.values.forEach(function (val) {
          values.push(escapeStr(val))
        })
      }

      wherestr = " WHERE " + params.join(' AND ')
    }

    stm.text = "DELETE FROM " + escapeStr(table) + wherestr + ';SELECT @@ROWCOUNT AS "count"'
    stm.values = values

    return stm
  }


  var selectstm = function (qent, q) {
    var stm = {}

    var table = relationalstore.tablename(qent)
    var entp = relationalstore.makeentp(qent)

    var values = []
    var params = []

    var cnt = 0

    var w = whereargs(entp, q)

    var wherestr = ''

    if (!_.isEmpty(w) && w.params.length > 0) {
      w.params.forEach(function (param) {
        params.push('"'+escapeStr(camelToSnakeCase(param)) + '"=@p' + (++cnt))
      })

      w.values.forEach(function (value) {
        values.push(value)
      })

      wherestr = " WHERE " + params.join(' AND ')
    }

    var mq = metaquery(qent, q)

    var metastr = ' ' + mq.params.join(' ')

    stm.text = "SELECT " + (mq.limit || '') +  " * FROM " + escapeStr(table) + wherestr + escapeStr(metastr)
    stm.values = values

    return stm
  }


  var whereargs = function (entp, q) {
    var w = {}

    w.params = []
    w.values = []

    var qok = relationalstore.fixquery(entp, q)

    for (var p in qok) {
      if (qok[p]) {
        w.params.push(camelToSnakeCase(p))
        w.values.push(qok[p])
      }
    }

    return w
  }


  var metaquery = function (qent, q) {
    var mq = {}

    mq.params = []
    mq.values = []

    if (q.sort$) {
      for (var sf in q.sort$) break;
      var sd = q.sort$[sf] < 0 ? 'ASC' : 'DESC'
      mq.params.push('ORDER BY ' + camelToSnakeCase(sf) + ' ' + sd)
    }

    var limit = q.limit$ || 20;
    if (limit) {
      mq.limit = 'TOP(' + limit + ')'
    }

    // TODO: implement
//    if( q.skip$ ) {
//      mq.params.push('OFFSET ' + q.skip$)
//    }

    return mq
  }


  seneca.store.init(seneca, opts, store, function (err, tag, description) {
    if (err) return cb(err);

    desc = description

    configure(opts, function (err) {
      if (err) {
        return seneca.fail({code: 'entity/configure', store: store.name, error: err}, cb)
      }
      else cb(null, {name: store.name, tag: tag});
    })
  })

}


var escapeStr = function(input) {
  if(input instanceof Date) {
    return input
  }
  var str = "" + input;
  return str.replace(/[\0\b\t\x08\x09\x1a\n\r"'\\\%]/g, function (char) {
    switch (char) {
      case "\0":
        return "\\0";
      case "\x08":
        return "\\b";
      case "\b":
        return "\\b";
      case "\x09":
        return "\\t";
      case "\t":
        return "\\t";
      case "\x1a":
        return "\\z";
      case "\n":
        return "\\n";
      case "\r":
        return "\\r";
      case "\"":
      case "'":
      case "\\":
      case "%":
        return "\\"+char;

    }
  });
};

