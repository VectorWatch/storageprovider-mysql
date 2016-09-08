var Promise = require('bluebird');
var mysql = require('mysql');
var util = require('util');
var StorageProviderAbstract = require('vectorwatch-storageprovider-abstract');

/**
 * @constructor
 * @augments StorageProviderAbstract
 */
function MySQLStorageProvider() {
    StorageProviderAbstract.call(this);

    this.connection = mysql.createPool({
        connectionLimit: process.env.VECTOR_DB_CONN_LIMIT,
        host: process.env.VECTOR_DB_HOST,
        user: process.env.VECTOR_DB_USER,
        password: process.env.VECTOR_DB_PASS,
        database: process.env.VECTOR_DB
    });
    this.connection.queryAsync = Promise.promisify(this.connection.query);
}
util.inherits(MySQLStorageProvider, StorageProviderAbstract);

/**
 * @inheritdoc
 */
MySQLStorageProvider.prototype.storeAuthTokensAsync = function(credentialsKey, authTokens) {
    return this.connection.queryAsync('INSERT INTO Auth (credentialsKey, authTokens) VALUES (?, ?) ON DUPLICATE KEY ' +
        'UPDATE authTokens = VALUES(authTokens)', [
        credentialsKey, JSON.stringify(authTokens)
    ]);
};

/**
 * @inheritdoc
 */
MySQLStorageProvider.prototype.getAuthTokensByCredentialsKeyAsync = function(credentialsKey) {
    return this.connection.queryAsync('SELECT authTokens FROM Auth WHERE credentialsKey = ?', [
        credentialsKey
    ]).then(function(records) {
        return records && records[0] && records[0].authTokens;
    }).then(function(authTokensString) {
        if (authTokensString) {
            return JSON.parse(authTokensString);
        }
    });
};

/**
 * @inheritdoc
 */
MySQLStorageProvider.prototype.getAuthTokensByChannelLabelAsync = function(channelLabel) {
    return this.connection.queryAsync(
        'SELECT Auth.authTokens FROM Auth LEFT JOIN UserSettings ON UserSettings.credentialsKey = Auth.credentialsKey' +
        ' WHERE UserSettings.channelLabel = ?',
        [channelLabel]
    ).then(function(records) {
        return records && records[0] && records[0].authTokens;
    }).then(function(authTokensString) {
        if (authTokensString) {
            return JSON.parse(authTokensString);
        }
    });
};

/**
 * @inheritdoc
 */
MySQLStorageProvider.prototype.storeUserSettingsAsync = function(channelLabel, userSettings, credentialsKey, contextualUniqueLabel) {
    if (contextualUniqueLabel) {
        userSettings.contextualUniqueLabel = channelLabel;
        return this.connection.queryAsync(
            'INSERT INTO UserSettings (channelLabel, userSettings, credentialsKey, count) VALUES (?, ?, ?, 1) ON DUPLICATE' +
            ' KEY UPDATE count = count + 1, credentialsKey = VALUES(credentialsKey), userSettings = ?',
            [contextualUniqueLabel, JSON.stringify(userSettings), credentialsKey, JSON.stringify(userSettings)]);
    } else {
        return this.connection.queryAsync(
            'INSERT INTO UserSettings (channelLabel, userSettings, credentialsKey, count) VALUES (?, ?, ?, 1) ON DUPLICATE' +
            ' KEY UPDATE count = count + 1, credentialsKey = VALUES(credentialsKey)',
            [channelLabel, JSON.stringify(userSettings), credentialsKey]);
    }
};

/**
 * @inheritdoc
 */
MySQLStorageProvider.prototype.removeUserSettingsAsync = function(channelLabel) {
    var _this = this;

    return this.connection.queryAsync('UPDATE UserSettings SET count = count - 1 WHERE channelLabel = ?', [
        channelLabel
    ]).then(function() {
        return _this.connection.queryAsync('DELETE FROM UserSettings WHERE count < 1');
    });
};

/**
 * @inheritdoc
 */
MySQLStorageProvider.prototype.getAllUserSettingsAsync = function() {
    return this.connection.queryAsync(
        'SELECT UserSettings.channelLabel, UserSettings.userSettings, Auth.authTokens, Auth.credentialsKey FROM UserSettings LEFT JOIN Auth' +
        ' ON Auth.credentialsKey = UserSettings.credentialsKey'
    ).then(function(records) {
        records = records || [];
        records.forEach(function(record) {
            record.userSettings = JSON.parse(record.userSettings);
            record.authTokens = JSON.parse(record.authTokens);
        });

        return records;
    });
};

/**
 * @inheritdoc
 */
MySQLStorageProvider.prototype.getUserSettingsAsync = function(channelLabel) {
    return this.connection.queryAsync(
        'SELECT UserSettings.channelLabel, UserSettings.userSettings, Auth.authTokens FROM UserSettings LEFT JOIN Auth' +
        ' ON Auth.credentialsKey = UserSettings.credentialsKey WHERE UserSettings.channelLabel = ?',
        [channelLabel]
    ).then(function(records) {
        var record = records && records[0];

        if (record) {
            record.userSettings = JSON.parse(record.userSettings);
            record.authTokens = JSON.parse(record.authTokens);
        }

        return record;
    });
};

/**
 * @inheritdoc
 */
MySQLStorageProvider.prototype.storeAppSettingsAsync = function(userKey, userSettings, credentialsKey, ttl) {
    return this.connection.queryAsync(
        'INSERT INTO AppSettings (userKey, userSettings, credentialsKey, expiresAt) VALUES (?, ?, ?, DATE_ADD(NOW(), INTERVAL ? SECOND)) ON DUPLICATE' +
        ' KEY UPDATE expiresAt = VALUES(expiresAt), credentialsKey = VALUES(credentialsKey)',
        [userKey, JSON.stringify(userSettings), credentialsKey, ttl]
    );
};

/**
 * @inheritdoc
 */
MySQLStorageProvider.prototype.removeExpiredAppSettingsAsync = function() {
    return this.connection.queryAsync(
        'START TRANSACTION;' +
        'SELECT AppSettings.userKey, AppSettings.userSettings, Auth.authTokens FROM AppSettings LEFT JOIN Auth' +
        ' ON Auth.credentialsKey = AppSettings.credentialsKey WHERE AppSettings.expiresAt <= NOW();' +
        'DELETE FROM AppSettings WHERE expiresAt <= NOW();' +
        'COMMIT;'
    ).then(function(records) {
        records = records || [];
        records.forEach(function(record) {
            record.userSettings = JSON.parse(record.userSettings);
            record.authTokens = JSON.parse(record.authTokens);
        });

        return records;
    });
};

/**
 * @inheritdoc
 */
MySQLStorageProvider.prototype.getAllAppSettingsAsync = function() {
    return this.connection.queryAsync(
        'SELECT AppSettings.userKey, AppSettings.userSettings, Auth.authTokens FROM AppSettings LEFT JOIN Auth' +
        ' ON Auth.credentialsKey = AppSettings.credentialsKey WHERE AppSettings.expiresAt > NOW()'
    ).then(function(records) {
        records = records || [];
        records.forEach(function(record) {
            record.userSettings = JSON.parse(record.userSettings);
            record.authTokens = JSON.parse(record.authTokens);
        });

        return records;
    });
};

/**
 * @inheritdoc
 */
MySQLStorageProvider.prototype.getAppSettingsAsync = function(userKey) {
    return this.connection.queryAsync(
        'SELECT AppSettings.userKey, AppSettings.userSettings, Auth.authTokens FROM AppSettings LEFT JOIN Auth' +
        ' ON Auth.credentialsKey = AppSettings.credentialsKey WHERE AppSettings.expiresAt > NOW() AND AppSettings.userKey = ?',
        [userKey]
    ).then(function(records) {
        var record = records && records[0];

        if (record) {
            record.userSettings = JSON.parse(record.userSettings);
            record.authTokens = JSON.parse(record.authTokens);
        }

        return record;
    });
};

/**
 * For applications that require push
 */

MySQLStorageProvider.prototype.storeUserKey = function(userKey, authTokens) {
    return this.connection.queryAsync(
        'UPDATE Auth SET userKey=? WHERE authTokens=?', [userKey, authTokens]);
}

MySQLStorageProvider.prototype.getAuthTokensForUserKey = function(userKey) {
    return this.connection.queryAsync(
        'SELECT authTokens FROM Auth WHERE userKey=?', userKey);
}

module.exports = MySQLStorageProvider;