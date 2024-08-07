/*global require,module,process*/

var winston = require('winston');
const mysql = require('mysql2');

var MariaDBDocumentStore = function (options) {
  this.expireJS = parseInt(options.expire, 10);

  this.pool = mysql.createPool({
    host: options.host || 'localhost',
    user: options.user || 'root',
    password: options.password || '',
    database: options.database || 'documents',
    connectionLimit: 10 
  });

  this.createTable();
};

MariaDBDocumentStore.prototype.createTable = function () {
  const createTableQuery = `
    CREATE TABLE IF NOT EXISTS pastebin (
      id INT AUTO_INCREMENT PRIMARY KEY,
      \`key\` VARCHAR(255) NOT NULL UNIQUE,
      value TEXT NOT NULL,
      expiration INT
    )
  `;

  this.pool.query(createTableQuery, function (err) {
    if (err) {
      winston.error('error creating table in MariaDB', { error: err });
      process.exit(1); 
    } else {
      winston.info('MariaDB table "pastebin" created or already exists');
    }
  });
};

MariaDBDocumentStore.prototype.set = function (key, data, callback, skipExpire) {
  var now = Math.floor(new Date().getTime() / 1000);
  var expiration = this.expireJS && !skipExpire ? this.expireJS + now : null;
  
  this.pool.query(
    'INSERT INTO pastebin (`key`, value, expiration) VALUES (?, ?, ?) ON DUPLICATE KEY UPDATE value = VALUES(value), expiration = VALUES(expiration)',
    [key, data, expiration],
    function (err, results) {
      if (err) {
        winston.error('error persisting value to MariaDB', { error: err });
        return callback(false);
      }
      callback(true);
    }
  );
};

MariaDBDocumentStore.prototype.get = function (key, callback, skipExpire) {
  var now = Math.floor(new Date().getTime() / 1000);
  
  this.pool.query(
    'SELECT value, expiration FROM pastebin WHERE `key` = ? AND (expiration IS NULL OR expiration > ?)',
    [key, now],
    function (err, results) {
      if (err) {
        winston.error('error retrieving value from MariaDB', { error: err });
        return callback(false);
      }
      if (results.length) {
        var result = results[0];
        if (result.expiration && !skipExpire) {
          this.pool.query(
            'UPDATE pastebin SET expiration = ? WHERE `key` = ?',
            [this.expireJS + now, key],
            function (err) {
              if (err) {
                winston.error('error updating expiration in MariaDB', { error: err });
              }
            }
          );
        }
        callback(result.value);
      } else {
        callback(false);
      }
    }.bind(this)
  );
};

module.exports = MariaDBDocumentStore;