-- 日志字段有效统计
create table column_count_report(
  id bigint not null AUTO_INCREMENT comment '主键',
  total bigint default 0 comment '总记录数',
  `remote_addr` bigint default 0 comment '远程ip地址',
  `remote_user` bigint default 0 comment '用户',
  `time_local` bigint default 0 comment '本地时间',
  `request` bigint default 0 comment '请求',
  `status` bigint default 0 comment '状态',
  `body_bytes_sent` bigint default 0 comment '请求体大小',
  `http_referer` bigint default 0 comment '来源',
  `http_user_agent` bigint default 0 comment '用户请求代理',
  `http_x_forwarded_for` bigint default 0 comment '请求代理远程ip地址',
   primary key(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE external TABLE `nginx_log`(
  `remote_addr` string,
  `remote_user` string,
  `time_local` string,
  `request` string,
  `status` string,
  `body_bytes_sent` string,
  `http_referer` string,
  `http_user_agent` string,
  `http_x_forwarded_for` string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
LOCATION
  'file:/Users/xiaofengfu/Documents/ideaworkspace/etlreport/orc_output'