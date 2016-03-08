CREATE TABLE `Auth` (
  `credentialsKey` varchar(45) NOT NULL,
  `authTokens` text NOT NULL,
  PRIMARY KEY (`credentialsKey`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE `UserSettings` (
  `channelLabel` varchar(45) NOT NULL,
  `userSettings` text NOT NULL,
  `credentialsKey` varchar(45) DEFAULT NULL,
  `count` int(11) NOT NULL DEFAULT '1',
  PRIMARY KEY (`channelLabel`),
  KEY `auth_idx` (`credentialsKey`),
  CONSTRAINT `auth` FOREIGN KEY (`credentialsKey`) REFERENCES `Auth` (`credentialsKey`) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE `AppSettings` (
  `userKey` varchar(45) NOT NULL,
  `userSettings` text NOT NULL,
  `credentialsKey` varchar(45) DEFAULT NULL,
  `expiresAt` DATETIME NOT NULL,
  PRIMARY KEY (`userKey`),
  KEY `auth_idx2` (`credentialsKey`),
  CONSTRAINT `auth2` FOREIGN KEY (`credentialsKey`) REFERENCES `Auth` (`credentialsKey`) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
