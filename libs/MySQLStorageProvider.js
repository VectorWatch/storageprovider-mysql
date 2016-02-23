var Promise = require('bluebird');
var mysql = require('mysql');
var util = require('util');
var AbstractStorageProvider = require('vectorwatch-storageprovider-abstract');

function MySQLStorageProvider(options) {
    AbstractStorageProvider.call(this);

    this.connection = mysql.createPool(options);
    this.connection.queryAsync = Promise.promisify(this.connection.query);
}
util.inherits(MySQLStorageProvider, AbstractStorageProvider);

MySQLStorageProvider.prototype.storeAuthTokensAsync = function(credentialsKey, authTokens) {
    return this.connection.queryAsync('INSERT IGNORE INTO Auth (credentialsKey, authTokens) VALUES (?, ?)', [
        credentialsKey, JSON.stringify(authTokens)
    ]);
};

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

MySQLStorageProvider.prototype.storeUserSettingsAsync = function(channelLabel, userSettings, credentialsKey) {
    return this.connection.queryAsync(
        'INSERT INTO UserSettings (channelLabel, userSettings, credentialsKey, count) VALUES (?, ?, ?, 1) ON DUPLICATE' +
        ' KEY UPDATE count = count + 1, credentialsKey = VALUES(credentialsKey)',
        [channelLabel, JSON.stringify(userSettings), credentialsKey]
    );
};

MySQLStorageProvider.prototype.removeUserSettingAsync = function(channelLabel) {
    var _this = this;

    return this.connection.queryAsync('UPDATE UserSettings SET count = count - 1 WHERE channelLabel = ?', [
        channelLabel
    ]).then(function() {
        return _this.connection.queryAsync('DELETE FROM UserSettings WHERE count < 1');
    });
};

MySQLStorageProvider.prototype.getAllUserSettingsAsync = function() {
    return this.connection.queryAsync(
        'SELECT UserSettings.channelLabel, UserSettings.userSettings, Auth.authTokens FROM UserSettings LEFT JOIN Auth' +
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

module.exports = MySQLStorageProvider;