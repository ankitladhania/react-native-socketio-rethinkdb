var express = require('express');
var http = require('http')
var socketio = require('socket.io');
var bodyParser = require('body-parser');
var r = require('rethinkdb');
var uuidv1 = require('uuid/v1');
var config = require(__dirname+"/config.js");

var app = express();
var server = http.Server(app);
var websocket = socketio(server);

app.use(express.static(__dirname + '/public'));
app.use(bodyParser());

// Mapping objects to easily map sockets and users.
var clients = {};
var users = {};


var USERS_TABLE = 'users_5';
var MESSAGES_TABLE = 'messages_5';

// This represents a unique chatroom.
// For this example purpose, there is only one chatroom;
var chatId = 1;
var connection = null;

/*
 * Create a RethinkDB connection, and save it in req._rdbConn
 */
r.connect(config.rethinkdb).then(function(conn) {
    connection = conn;
}).catch(() => handleError('Fatta'));


websocket.on('connection', (socket) => {
    clients[socket.id] = socket;
    socket.on('userJoined', (userId) => onUserJoined(userId, socket));
    socket.on('message', (message) => onMessageReceived(message, socket));
});

// Event listeners.
// When a user joins the chatroom.
function onUserJoined(userId, socket) {
  try {
    // The userId is null for new users.
    if (!userId) {
     var user_id = uuidv1();
     var createdAt = r.now();
     r.table(USERS_TABLE).insert({ user_id: user_id }, {returnChanges: true}).run(connection).then((user) => {
        socket.emit('userJoined', user_id);
        users[socket.id] = user.user_id;
        _sendExistingMessages(socket);
      });
    } else {
      users[socket.id] = userId;
      _sendExistingMessages(socket);
    }
  } catch(err) {
    console.err(err);
  }
}

// When a user sends a message in the chatroom.
function onMessageReceived(message, senderSocket) {
  var userId = users[senderSocket.id];
  // Safety check.
  if (!userId) return;

  _sendAndSaveMessage(message, senderSocket);
}

// Helper functions.
// Send the pre-existing messages to the user that just joined.
function _sendExistingMessages(socket) {
  var messages = r.table(MESSAGES_TABLE).orderBy({ index: 'createdAt' }).run(connection).then(function(cursor) {
        return cursor.toArray();
    })
    .then((messages) => {
      // If there aren't any messages, then return.
      if (!messages.length) return;
      socket.emit('message', messages.reverse());
  });
}

// Save the message to the db and send all sockets but the sender.
function _sendAndSaveMessage(message, socket, fromServer) {
  var messageData = {
    text: message.text,
    user: message.user,
    createdAt: new Date(message.createdAt),
    chatId: chatId,
    _id: uuidv1(),
  };


  r.table(MESSAGES_TABLE).insert(messageData, {returnChanges: true}).run(connection).then((result) => {
    // If the message is from the server, then send to everyone.
    if (result.inserted !== 1) {
            console.err(new Error("Document was not inserted."));
    } else {
      var emitter = fromServer ? websocket : socket.broadcast;
      emitter.emit('message', [message]);
    }
  });
}

r.connect(config.rethinkdb, function(err, conn) {
    if (err) {
        console.log("Could not open a connection to initialize the database");
        console.log(err.message);
        process.exit(1);
    }

    r.table(MESSAGES_TABLE).indexWait('createdAt').run(conn).then(function(err, result) {
        console.log("Table and index are available, starting express...");
        startExpress();
    }).catch(function(err) {
        // The database/table/index was not available, create them
        r.dbCreate(config.rethinkdb.db).run(conn).finally(function() {
          return r.tableCreate(MESSAGES_TABLE).run(conn);
        }).finally(function(){
          return r.tableCreate(USERS_TABLE).run(conn);
        })
        .finally(() => {
         r.table(MESSAGES_TABLE).indexCreate('createdAt').run(conn);
        })
        .finally(() => {
         r.table(USERS_TABLE).indexCreate('createdAt').run(conn);
        })
        .finally(function(result) {
            r.table(MESSAGES_TABLE).indexWait('createdAt').run(conn)
        }).then(function(result) {
            console.log("Table and index are available, starting express...");
            startExpress();
            conn.close();
        }).catch(function(err) {
            if (err) {
                console.log("Could not wait for the completion of the index `todos`");
                console.log(err);
                process.exit(1);
            }
            console.log("Table and index are available, starting express...");
            startExpress();
            conn.close();
        });
    });
});

function startExpress() {
    server.listen(config.express.port, config.express.host);
    console.log('Listening on port '+config.express.port);
}
function handleError(res) {
        console.log({error: res});
}

// Allow the server to participate in the chatroom through stdin.
var stdin = process.openStdin();
stdin.addListener('data', function(d) {
  if (!d.toString().trim()) { return;}
  _sendAndSaveMessage({
    _id: uuidv1(),
    text: d.toString().trim(),
    createdAt: new Date(),
    user: { user_id: 'robot' }
  }, null /* no socket */, true /* send from server */);
});
