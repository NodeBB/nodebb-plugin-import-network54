
var async = require('async');
var mysql = require('mysql');
var _ = require('underscore');
var noop = function(){};
var logPrefix = '[nodebb-plugin-import-network54]';

(function(Exporter) {

    Exporter.setup = function(config, callback) {
        Exporter.log('setup');

        // mysql db only config
        // extract them from the configs passed by the nodebb-plugin-import adapter
        var _config = {
            host: config.dbhost || config.host || 'localhost',
            user: config.dbuser || config.user || 'user',
            password: config.dbpass || config.pass || config.password || 'password',
            port: config.dbport || config.port || 3306,
            database: config.dbname || config.name || config.database || ''
        };

        Exporter.config(_config);
        Exporter.config('prefix', config.prefix || config.tablePrefix || '');

        Exporter.connection = mysql.createConnection(_config);
        Exporter.connection.connect();

        Exporter.fixTables(function() {
            console.log('fixing posts done');
            callback(null, Exporter.config());
        });
    };

    Exporter.getUsers = function(callback) {
        return Exporter.getPaginatedUsers(0, -1, callback);
    };
    Exporter.getPaginatedUsers = function(start, limit, callback) {
        callback = !_.isFunction(callback) ? noop : callback;

        var err;
        var prefix = Exporter.config('prefix') || '';
        var startms = +new Date();

        var query = 'SELECT '
            + prefix + 'author.id as _uid, '
            + prefix + 'author.email as _email, '
            + prefix + 'author.handle as _username, '
            + prefix + 'author.name as _fullname, '
            + prefix + 'author.imageUrl as _picture '
            + 'FROM ' + prefix + 'author '
            + (start >= 0 && limit >= 0 ? 'LIMIT ' + start + ',' + limit : '');


        if (!Exporter.connection) {
            err = {error: 'MySQL connection is not setup. Run setup(config) first'};
            Exporter.error(err.error);
            return callback(err);
        }

        Exporter.connection.query(query,
            function(err, rows) {
                if (err) {
                    Exporter.error(err);
                    return callback(err);
                }
                //normalize here
                var map = {};
                rows.forEach(function(row) {
                    // lower case the email for consistency
                    row._email = (row._email || '').toLowerCase();
                    row._picture = Exporter.validateUrl(row._picture);
                    map[row._uid] = row;
                });
                callback(null, map);
            });
    };

    Exporter.getCategories = function(callback) {
        return Exporter.getPaginatedCategories(0, -1, callback);
    };
    Exporter.getPaginatedCategories = function(start, limit, callback) {
        callback = !_.isFunction(callback) ? noop : callback;

        var err;
        var prefix = Exporter.config('prefix');
        var startms = +new Date();
        var query = 'SELECT '
            + prefix + 'categories.cid as _cid, '
            + prefix + 'categories.name as _name, '
            + prefix + 'categories.description as _description '
            + 'FROM ' + prefix + 'categories '
            + (start >= 0 && limit >= 0 ? 'LIMIT ' + start + ',' + limit : '');


        if (!Exporter.connection) {
            err = {error: 'MySQL connection is not setup. Run setup(config) first'};
            Exporter.error(err.error);
            return callback(err);
        }

        Exporter.connection.query(query,
            function(err, rows) {
                if (err) {
                    Exporter.error(err);
                    return callback(err);
                }
                //normalize here
                var map = {};
                rows.forEach(function(row) {
                    row._name = row._name ? row._name[0].toUpperCase() + row._name.substr(1) : 'Untitled';
                    row._description = row._description || 'No description set';
                    row._timestamp = ((row._timestamp || 0) * 1000) || startms;
                    map[row._tid] = row;
                });
                callback(null, map);
            });
    };

    Exporter.getTopics = function(callback) {
        return Exporter.getPaginatedTopics(0, -1, callback);
    };
    Exporter.getPaginatedTopics = function(start, limit, callback) {
        callback = !_.isFunction(callback) ? noop : callback;

        var err;
        var prefix = Exporter.config('prefix');
        var startms = +new Date();
        var query = 'SELECT '
            + prefix + 'topics.tid as _tid, '
            + prefix + 'topics.uid as _uid, '
            + prefix + 'topics.title as _title, '
            + prefix + 'posts.content as _content, '
            + prefix + 'posts.url as _path, '
            + prefix + 'posts.fromIP as _ip, '
            + prefix + 'topics.timestamp as _timestamp '
            + 'FROM ' + prefix + 'topics '
            + 'JOIN ' + prefix + 'posts ON ' + prefix + 'topics.mainPid=' + prefix + 'posts.id '
            + (start >= 0 && limit >= 0 ? 'LIMIT ' + start + ',' + limit : '');


        if (!Exporter.connection) {
            err = {error: 'MySQL connection is not setup. Run setup(config) first'};
            Exporter.error(err.error);
            return callback(err);
        }

        Exporter.connection.query(query,
            function(err, rows) {
                if (err) {
                    Exporter.error(err);
                    return callback(err);
                }
                //normalize here
                var map = {};
                rows.forEach(function(row) {
                    row._title = row._title ? row._title[0].toUpperCase() + row._title.substr(1) : 'Untitled';
                    row._timestamp = ((row._timestamp || 0) * 1000) || startms;
                    row._cid = 1;
                    row._content = row._content || '...';

                    row._author_name = row._author_name || '';
                    if (! row._author_handle) {
                        var m = row._author_name.match(/(.*)\(no login\)/);
                        if (m && m.length) {
                            row._guest = m[1];
                        }
                    }

                    map[row._tid] = row;
                });
                callback(null, map);
            });
    };

    Exporter.getPosts = function(callback) {
        return Exporter.getPaginatedPosts(0, -1, callback);
    };
    Exporter.getPaginatedPosts = function(start, limit, callback) {
        callback = !_.isFunction(callback) ? noop : callback;
        var err;
        var prefix = Exporter.config('prefix');
        var startms = +new Date();
        var query = 'SELECT '
            + prefix + 'posts.id as _pid, '
            + prefix + 'posts.tid as _tid, '
            + prefix + 'posts.author_id as _uid, '
            + prefix + 'posts.url as _path, '
            + prefix + 'posts.fromIP as _ip, '
            + prefix + 'author.name as _author_name, '
            + prefix + 'author.handle as _author_handle, '
            + prefix + 'posts.parent_post_id as _toPid, '
            + 'CONCAT(' + prefix + 'posts.fullTitle' + ', \'\n\', ' + prefix + 'posts.content' + ')' + ' as _content, '
            + prefix + 'posts.timestamp as _timestamp '
            + 'FROM ' + prefix + 'posts '
            + 'JOIN ' + prefix + 'author ON ' + prefix + 'posts.author_id=' + prefix + 'author.id '
            + 'WHERE ' + prefix + 'posts.isTopPost IS NULL '
            + (start >= 0 && limit >= 0 ? 'LIMIT ' + start + ',' + limit : '');

        if (!Exporter.connection) {
            err = {error: 'MySQL connection is not setup. Run setup(config) first'};
            Exporter.error(err.error);
            return callback(err);
        }

        Exporter.connection.query(query,
            function(err, rows) {
                if (err) {
                    Exporter.error(err);
                    return callback(err);
                }

                //normalize here
                var map = {};
                rows.forEach(function(row) {
                    row._uid = row._uid || 0;
                    row._content = row._content || '...';
                    row._timestamp = row._timestamp || startms;
                    row._author_name = row._author_name || '';
                    if (! row._author_handle) {
                        var m = row._author_name.match(/(.*)\(no login\)/);
                        if (m && m.length) {
                            row._guest = m[1];
                        }
                    }
                    map[row._pid] = row;
                });

                callback(null, map);
            });
    };

    Exporter.teardown = function(callback) {
        Exporter.connection.end();
        callback();
    };

    Exporter.testrun = function(config, callback) {
        async.series([
            function(next) {
                Exporter.setup(config, next);
            },
            function(next) {
                Exporter.getUsers(next);
            },
            function(next) {
                Exporter.getCategories(next);
            },
            function(next) {
                Exporter.getTopics(next);
            },
            function(next) {
                Exporter.getPosts(next);
            },
            function(next) {
                Exporter.teardown(next);
            }
        ], callback);
    };

    Exporter.paginatedTestrun = function(config, callback) {
        async.series([
            function(next) {
                Exporter.setup(config, next);
            },
            function(next) {
                Exporter.getPaginatedUsers(0, 1000, next);
            },
            function(next) {
                Exporter.getPaginatedCategories(0, 1000, next);
            },
            function(next) {
                Exporter.getPaginatedTopics(0, 1000, next);
            },
            function(next) {
                Exporter.getPaginatedPosts(1001, 2000, next);
            },
            function(next) {
                Exporter.teardown(next);
            }
        ], callback);
    };

    Exporter.warn = function() {
        var args = _.toArray(arguments);
        args.unshift(logPrefix);
        console.warn.apply(console, args);
    };

    Exporter.log = function() {
        var args = _.toArray(arguments);
        args.unshift(logPrefix);
        console.log.apply(console, args);
    };

    Exporter.error = function() {
        var args = _.toArray(arguments);
        args.unshift(logPrefix);
        console.error.apply(console, args);
    };

    Exporter.config = function(config, val) {
        if (config != null) {
            if (typeof config === 'object') {
                Exporter._config = config;
            } else if (typeof config === 'string') {
                if (val != null) {
                    Exporter._config = Exporter._config || {};
                    Exporter._config[config] = val;
                }
                return Exporter._config[config];
            }
        }
        return Exporter._config;
    };

    Exporter.fixTables = function(callback) {
        /*
         This utility method goes through the "posts" table and:
         * extracts the topics to a "topics" table
         * adds a "topic_id" column to the "posts" table
         */
        callback = !_.isFunction(callback) ? noop : callback;

        var err;
        var prefix = Exporter.config('prefix');
        var // CREATE TABLE topics ( tid int(10) AUTO_INCREMENT PRIMARY KEY, mainPid int(10), uid int(10), title varchar(255), timestamp varchar(10) );
            query1 = 'CREATE TABLE ' + prefix + 'topics ( tid int(10) AUTO_INCREMENT PRIMARY KEY, mainPid int(10), uid int(10), title varchar(255), timestamp varchar(10) );',
        // INSERT INTO topics (mainPid, uid, title, timestamp) (SELECT id AS tid, author_id AS uid, fullTitle AS title, timestamp FROM posts WHERE isTopPost="1" ORDER BY timestamp ASC)
            query2 = 'INSERT INTO ' + prefix + 'topics (mainPid, uid, title, timestamp) (SELECT id AS tid, author_id AS uid, fullTitle AS title, timestamp FROM ' + prefix + 'posts WHERE isTopPost="1" ORDER BY timestamp ASC)',
        // SELECT mainPid, tid FROM `topics`
            query3 = 'SELECT mainPid, tid FROM `' + prefix + 'topics`',
            query4 = 'ALTER TABLE ' + prefix + 'posts ADD COLUMN tid int(10) AFTER id',
            query5 = 'SELECT id, parent_post_id AS toPid FROM ' + prefix + 'posts WHERE parent_post_id IS NOT NULL AND isTopPost IS NULL',
            query6 = 'CREATE TABLE ' + prefix + 'temp ( pid int(10), tid int(10) )',
            query7 = 'INSERT INTO ' + prefix + 'temp VALUES ',
            query8 = 'UPDATE ' + prefix + 'posts, ' + prefix + 'temp SET ' + prefix + 'posts.tid = ' + prefix + 'temp.tid WHERE ' + prefix + 'posts.id=' + prefix + 'temp.pid;',

            query9 = 'CREATE TABLE ' + prefix + 'categories ( cid int(10), name varchar(255), description varchar(255) )',
            query10 = 'INSERT INTO ' + prefix + 'categories VALUES ( 1, "Untitled Category", "No description set" ) ',

            mainPidToTid = {},
            pidToTid = {};

        if (!Exporter.connection) {
            err = {error: 'MySQL connection is not setup. Run setup(config) first'};
            Exporter.error(err.error);
            return callback(err);
        }

        async.waterfall([
            function(next) {
                console.log('dropping column tid');
                Exporter.connection.query('ALTER TABLE posts DROP COLUMN tid', function() {
                    next(); // dont care if errors
                });
            },
            function(next) {
                console.log('dropping topics');
                Exporter.connection.query('DROP TABLE IF EXISTS topics', function() {
                    next();
                });
            },
            function(next) {
                console.log('dropping temp');
                Exporter.connection.query('DROP TABLE IF EXISTS temp', function() {
                    next();
                });
            },
            function(next) {
                console.log('dropping fake categories');
                Exporter.connection.query('DROP TABLE IF EXISTS categories', function() {
                    next();
                });
            },
            function(next) {
                console.log('Creating categories table');
                Exporter.connection.query(query9, function() {
                    next();
                });
            },
            function(next) {
                console.log('Populating categories table');
                Exporter.connection.query(query10, function() {
                    next();
                });
            },
            function(next) {
                console.log('Creating topics table');
                Exporter.connection.query(query1, next);
            },
            function(rows, fields, next) {
                console.log('Copying parent topics into new table');
                Exporter.connection.query(query2, next);
            },
            function(rows, fields, next) {
                console.log('Grabbing those pids for assignment as mainPids');
                Exporter.connection.query(query3, next);
            },
            function(rows, fields, next) {
                console.log('Assigning...');
                for(var x=0,numRows=rows.length;x<numRows;x++) {
                    mainPidToTid[rows[x]['mainPid']] = rows[x]['tid'];
                }

                console.log('Done.');
                next();
            },
            function(next) {
                console.log('Adding tid column to posts table');
                Exporter.connection.query(query4, next);
            },
            function(rows, fields, next) {
                console.log('Retrieving pids with parent_ids for re-association');
                Exporter.connection.query(query5, next);
            },
            function(rows, fields, next) {
                var pass = 1,
                    matches = 0,
                    tid;

                while(rows.length > 0) {
                    rows = rows.map(function(row) {
                        tid = pidToTid[row.toPid] || mainPidToTid[row.toPid];
                        if (tid) {
                            pidToTid[row.id] = tid;
                            matches++;
                            row = null;
                        }
                    }).filter(Boolean);
                    console.log('[Pass ' + pass + '] ' + matches + ' matches found, ' + rows.length + ' records remaining');
                }

                next(null, pidToTid);
            },
            function(pidToTid, next) {
                console.log('Creating temp table');
                var values = '';
                for(var pid in pidToTid) {
                    values = values + ' (' + pid + ', ' + pidToTid[pid] + '),';
                }
                Exporter.connection.query(query6, function() {
                    Exporter.connection.query(query7 + values.slice(0, -1), next);
                });
            },
            function(rows, fields, next) {
                console.log('Adding tids into the posts table');
                Exporter.connection.query(query8, next);
            }
        ], callback);
    };

    // from Angular https://github.com/angular/angular.js/blob/master/src/ng/directive/input.js#L11
    Exporter.validateUrl = function(url) {
        var pattern = /^(ftp|http|https):\/\/(\w+:{0,1}\w*@)?(\S+)(:[0-9]+)?(\/|\/([\w#!:.?+=&%@!\-\/]))?$/;
        return url && url.length < 2083 && url.match(pattern) ? url : '';
    };

    Exporter.truncateStr = function(str, len) {
        if (typeof str != 'string') return str;
        len = _.isNumber(len) && len > 3 ? len : 20;
        return str.length <= len ? str : str.substr(0, len - 3) + '...';
    };

    Exporter.whichIsFalsy = function(arr) {
        for (var i = 0; i < arr.length; i++) {
            if (!arr[i])
                return i;
        }
        return null;
    };

})(module.exports);
