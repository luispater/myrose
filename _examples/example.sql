CREATE TABLE `users` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT COMMENT 'ID',
  `name` varchar(255) NOT NULL DEFAULT '' COMMENT 'name',
  `password` varchar(32) NOT NULL DEFAULT '' COMMENT 'password',
  `status` tinyint(1) NOT NULL DEFAULT '0' COMMENT 'status',
  PRIMARY KEY (`id`),
  UNIQUE KEY `name` (`name`),
  KEY `status` (`status`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='users'

CREATE TABLE `users_info` (
  `user_id` int(11) unsigned NOT NULL AUTO_INCREMENT COMMENT 'ID',
  `user_sex` enum("male", "female") NOT NULL DEFAULT 'male',
  PRIMARY KEY (`user_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='users_info';