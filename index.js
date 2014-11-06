
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
            database: config.dbname || config.name || config.database || 'network54'
        };

        Exporter.log(_config);

        Exporter.config(_config);
        Exporter.config('prefix', config.prefix || config.tablePrefix || '');

        Exporter.connection = mysql.createConnection(_config);
        Exporter.connection.connect();

        callback(null, Exporter.config());
    };

    var getGroups = function(config, callback) {
        if (_.isFunction(config)) {
            callback = config;
            config = {};
        }
        callback = !_.isFunction(callback) ? noop : callback;
        if (!Exporter.connection) {
            Exporter.setup(config);
        }
        var prefix = Exporter.config('prefix');
        var query = 'SELECT '
            + prefix + 'usergroup.usergroupid as _gid, '
            + prefix + 'usergroup.title as _title, ' // not sure, just making an assumption
            + prefix + 'usergroup.pmpermissions as _pmpermissions, ' // not sure, just making an assumption
            + prefix + 'usergroup.adminpermissions as _adminpermissions ' // not sure, just making an assumption
            + ' from ' + prefix + 'usergroup ';
        Exporter.connection.query(query,
            function(err, rows) {
                if (err) {
                    Exporter.error(err);
                    return callback(err);
                }
                var map = {};

                //figure out the admin group
                var max = 0, admingid;
                rows.forEach(function(row) {
                    var adminpermission = parseInt(row._adminpermissions, 10);
                    if (adminpermission) {
                        if (adminpermission > max) {
                            max = adminpermission;
                            admingid = row._gid;
                        }
                    }
                });

                rows.forEach(function(row) {
                    if (! parseInt(row._pmpermissions, 10)) {
                        row._banned = 1;
                        row._level = 'member';
                    } else if (parseInt(row._adminpermissions, 10)) {
                        row._level = row._gid === admingid ? 'administrator' : 'moderator';
                        row._banned = 0;
                    } else {
                        row._level = 'member';
                        row._banned = 0;
                    }
                    map[row._gid] = row;
                });
                // keep a copy of the users in memory here
                Exporter._groups = map;
                callback(null, map);
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
            + prefix + 'user.userid as _uid, '
            + prefix + 'user.email as _email, '
            + prefix + 'user.username as _username, '
            + prefix + 'sigparsed.signatureparsed as _signature, '
            + prefix + 'user.joindate as _joindate, '
            + prefix + 'user.homepage as _website, '
            + prefix + 'user.reputation as _reputation, '
            + prefix + 'user.profilevisits as _profileviews, '
            + prefix + 'user.birthday as _birthday '
            + 'FROM ' + prefix + 'user '
            + 'LEFT JOIN ' + prefix + 'sigparsed ON ' + prefix + 'sigparsed.userid=' + prefix + 'user.userid '
            + (start >= 0 && limit >= 0 ? 'LIMIT ' + start + ',' + limit : '');


        if (!Exporter.connection) {
            err = {error: 'MySQL connection is not setup. Run setup(config) first'};
            Exporter.error(err.error);
            return callback(err);
        }

        getGroups(function(err, groups) {
            Exporter.connection.query(query,
                function(err, rows) {
                    if (err) {
                        Exporter.error(err);
                        return callback(err);
                    }

                    //normalize here
                    var map = {};
                    rows.forEach(function(row) {
                            // nbb forces signatures to be less than 150 chars
                            // keeping it HTML see https://github.com/akhoury/nodebb-plugin-import#markdown-note
                            row._signature = Exporter.truncateStr(row._signature || '', 150);

                            // from unix timestamp (s) to JS timestamp (ms)
                            row._joindate = ((row._joindate || 0) * 1000) || startms;

                            // lower case the email for consistency
                            row._email = (row._email || '').toLowerCase();

                            // I don't know about you about I noticed a lot my users have incomplete urls, urls like: http://
                            row._picture = Exporter.validateUrl(row._picture);
                            row._website = Exporter.validateUrl(row._website);

                            row._level = (groups[row._gid] || {})._level || '';
                            row._banned = (groups[row._gid] || {})._banned || 0;

                            map[row._uid] = row;
                    });

                    callback(null, map);
                });
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
            + prefix + 'forum.forumid as _cid, '
            + prefix + 'forum.title as _name, '
            + prefix + 'forum.description as _description, '
            + prefix + 'forum.displayorder as _order '
            + 'FROM ' + prefix + 'forum ' // filter added later
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
                    row._name = row._name || 'Untitled Category '
                    row._description = row._description || 'No decsciption available';
                    row._timestamp = ((row._timestamp || 0) * 1000) || startms;
                    map[row._cid] = row;
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
            + prefix + 'thread.threadid as _tid, '
            + prefix + 'post.userid as _uid, '
            + prefix + 'thread.forumid as _cid, '
            + prefix + 'post.title as _title, '
            + prefix + 'post.pagetext as _content, '
            + prefix + 'post.dateline as _timestamp, '
            + prefix + 'thread.views as _viewscount, '
            + prefix + 'thread.open as _open, '
            + prefix + 'thread.deletedcount as _deleted, '
            + prefix + 'thread.sticky as _pinned '
            + 'FROM ' + prefix + 'thread '
            + 'JOIN ' + prefix + 'post ON ' + prefix + 'thread.firstpostid=' + prefix + 'post.postid '
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
                    row._locked = row._open ? 0 : 1;
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

        /*
        network54 post schema
CREATE TABLE `posts` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `content` text,
  `fromIP` varchar(255) DEFAULT NULL,
  `fullTitle` varchar(255) DEFAULT NULL,
  `isTopPost` bit(1) DEFAULT NULL,        // IF ITS NULL NORMAL POST ELSE TOPIC
  `legacyId` bigint(20) DEFAULT NULL,
  `timestamp` bigint(20) DEFAULT NULL,
  `url` varchar(255) DEFAULT NULL,
  `author_id` int(11) DEFAULT NULL,
  `parent_post_id` int(11) DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `ix_url` (`url`),
  KEY `FK_5srofo2nnf15n2hj8fb4qity7` (`author_id`),
  KEY `FK_m4vnw2pq586dokmiu3tqemnqf` (`parent_post_id`),
  CONSTRAINT `FK_5srofo2nnf15n2hj8fb4qity7` FOREIGN KEY (`author_id`) REFERENCES `author` (`id`),
  CONSTRAINT `FK_m4vnw2pq586dokmiu3tqemnqf` FOREIGN KEY (`parent_post_id`) REFERENCES `posts` (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=44124 DEFAULT CHARSET=latin1;
        */


        var err;
        var prefix = Exporter.config('prefix');
        var startms = +new Date();
        var query = 'SELECT '
            + prefix + 'id as _pid, '
            + prefix + 'post.threadid as _tid, '   // WHERE DO WE GET THIS ?? -baris
            + prefix + 'author_id as _uid, '
            + prefix + 'content as _content, '
            + prefix + 'timestamp as _timestamp '
            + 'FROM ' + prefix + 'posts WHERE ' + prefix + 'isTopPost IS NULL '
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
                    row._content = row._content || '';
                    row._timestamp = ((row._timestamp || 0) * 1000) || startms;
                    map[row._pid] = row;
                });

                callback(null, map);
            });
    };

    Exporter.teardown = function(callback) {
        Exporter.log('teardown');
        Exporter.connection.end();

        Exporter.log('Done');
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

    Exporter.fixPostsTable = function(callback) {
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
            query8 = 'UPDATE ' + prefix + 'posts, ' + prefix + 'temp SET ' + prefix + 'posts.tid = ' + prefix + 'temp.tid WHERE ' + prefix + 'posts.id=' + prefix + 'temp.pid;';
        var mainPidToTid = {},
            pidToTid = {};

        if (!Exporter.connection) {
            err = {error: 'MySQL connection is not setup. Run setup(config) first'};
            Exporter.error(err.error);
            return callback(err);
        }

        async.waterfall([
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
