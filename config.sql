BEGIN;

USE PinkApricot;
DROP TABLE IF EXISTS ExporterConfig;

CREATE TABLE `ExporterConfig` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT '自增id',
  `config_name` varchar(63) NOT NULL COMMENT '配置名称',
  `version` int NOT NULL COMMENT '配置版本',
  `config` json NOT NULL COMMENT '配置内容',
  `createtime` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '记录创建时间戳',
  `updatetime` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '记录更新时间戳',
  PRIMARY KEY (`id`),
  UNIQUE KEY `config_name_version` (`config_name`, `version`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

COMMIT;