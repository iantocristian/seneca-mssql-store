var seneca = require('seneca');
var store = require('../lib/mssql-store.js');
var async = require('async');

var config = {};

var storeopts = {
  name: 'db',
  host: 'localhost',
  // port: 1433,
  username: 'user',
  password: 'password'
};

var si = seneca(config);
si.use(store, storeopts);

si.ready(function () {
  var ticket$ = si.make('', 'sys', 'ticket');

  var insertid;

  async.series([function(done) {
    ticket$.list$({sort$:{ticketNumber:1}}, function(err, tickets) {
      console.dir(tickets);
      done();
    });
  }, function(done) {
    ticket$.save$({ticketNumber:'12345'}, function(err, ticket) {
      console.dir(ticket);
      insertid = ticket.id;
      done();
    });
  }, function(done) {
    var ticket = ticket$.make$();
    ticket.load$(insertid, function(err, ticket) {
      if (err) {
        console.error(err);
      }
      else {
        console.log('loaded ok', JSON.stringify(ticket));
      }
      done();
    })
  }, function(done) {
    var ticket = ticket$.make$();
    ticket.load$(insertid, function(err, ticket) {
      if (err) {
        console.error(err);
        done();
      }
      else {
        console.log('loaded ok', JSON.stringify(ticket));
        ticket.ticketNumber='54321';
        ticket.save$(function(err, ticket) {
          if (err) {
            console.error(err);
          }
          else {
            console.log('saved ok', JSON.stringify(ticket));
          }
          done();
        });
      }
    })
  }, function(done) {
    ticket$.delete$({id: insertid}, function(err) {
      if (err) {
        console.error(err);
      }
      else {
        console.log('deleted ok');
      }

      done();
    });
  }])
});

