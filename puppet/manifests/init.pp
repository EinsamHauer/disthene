class disthene (
  $java_xmx = '16G',
  $java_xms = '2G',
  $java_string_table_size = '10000019',
  $java_extra_options = '',

  $disthene_package_version = 'present',

  $carbon_host = '127.0.0.1',
  $carbon_port = '2003',
  $carbon_rollups = ['60s:5356800s','900s:62208000s'],
  $carbon_aggregator_delay = '100',

  $store_cluster = [$::ipaddress],
  $store_port = '9042',
  $store_keyspace = 'metric',
  $store_column_family = 'metric',
  $store_max_connections = '2048',
  $store_read_timeout = '5',
  $store_connect_timeout = '5',
  $store_max_requests = '128',
  $store_batch = 'true',
  $store_batch_size = '200',
  $store_pool = '2',
  $store_load_balancing_policy = "TokenDcAwareRoundRobinPolicy",
  $store_protocol_version = "V2",
  $store_tenant_keyspace = "metric",
  $store_tenant_table_template = "metric_%s_%d",
  $store_tenant_table_create_template = "CREATE TABLE IF NOT EXISTS %s.%s (
  path text,
  time bigint,
  data list<double>,
  PRIMARY KEY ((path), time)
) WITH CLUSTERING ORDER BY (time ASC)
  AND bloom_filter_fp_chance = 0.01
  AND caching = 'KEYS_ONLY'
  AND compaction = {'min_threshold': '2', 'unchecked_tombstone_compaction': 'true', 'tombstone_compaction_interval': '86400', 'min_sstable_size': '104857600', 'tombstone_threshold': '0.1', 'bucket_low': '0.5', 'bucket_high': '1.5', 'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy'}
  AND compression = {'sstable_compression': 'org.apache.cassandra.io.compress.LZ4Compressor'}
  AND dclocal_read_repair_chance = 0.1
  AND default_time_to_live = 0
  AND gc_grace_seconds = 43200
  AND read_repair_chance = 0.1;",

  $index_name = 'disthene',
  $index_cluster = [$::ipaddress],
  $index_port = 9300,
  $index_index = 'disthene_paths',
  $index_type = 'path',
  $index_cache = 'true',
  $index_expire = '3600',
  $index_bulk_actions = '10000',
  $index_bulk_interval = '5',

  $stats_interval = 60,
  $stats_tenant = "NONE",
  $stats_hostname = "$::hostname",
  $stats_log = 'true',

  $custom_aggregator_config = false,
  $custom_blacklist_config = false,
  $custom_whitelist_config = false,
  $custom_log_config = false,
)
{
  if $custom_aggregator_config {
    $disthene_aggregator_config = 'puppet:///modules/config/aggregator.yaml'
  }
  else {
    $disthene_aggregator_config = 'puppet:///modules/disthene/aggregator.yaml'
  }

  if $custom_blacklist_config {
    $disthene_blacklist_config = 'puppet:///modules/config/blacklist.yaml'
  }
  else {
    $disthene_blacklist_config = 'puppet:///modules/disthene/blacklist.yaml'
  }

  if $custom_whitelist_config {
    $disthene_whitelist_config = 'puppet:///modules/config/whitelist.yaml'
  }
  else {
    $disthene_whitelist_config = 'puppet:///modules/disthene/whitelist.yaml'
  }

  if $custom_log_config {
    $disthene_log_config = 'puppet:///modules/config/disthene-log4j.xml'
  }
  else {
    $disthene_log_config = 'puppet:///modules/disthene/disthene-log4j.xml'
  }

  file { 'disthene_config':
    ensure  => present,
    path    => '/etc/disthene/disthene.yaml',
    content => template('disthene/disthene.yaml.erb'),
    require => Package['disthene'],
  }

  file { 'disthene_aggregator_config':
    ensure  => present,
    path    => '/etc/disthene/aggregator.yaml',
    source  => $disthene_aggregator_config,
    require => Package['disthene'],
    notify  => Service['disthene'],
  }

  file { 'disthene_blacklist_config':
    ensure  => present,
    path    => '/etc/disthene/blacklist.yaml',
    source  => $disthene_blacklist_config,
    require => Package['disthene'],
    notify  => Service['disthene'],
  }

  file { 'disthene_whitelist_config':
    ensure  => present,
    path    => '/etc/disthene/whitelist.yaml',
    source  => $disthene_whitelist_config,
    require => Package['disthene'],
    notify  => Service['disthene'],
  }

  file { 'disthene_log_config':
    ensure  => present,
    path    => '/etc/disthene/disthene-log4j.xml',
    source  => $disthene_log_config,
    require => Package['disthene'],
    notify  => Service['disthene'],
  }

  file { 'disthene_defaults':
    ensure  => present,
    path    => '/etc/default/disthene',
    content => template('disthene/disthene-default.erb'),
    require => File['disthene_config'],
  }

#sysctl { 'net.core.somaxconn': value => '2048' }

  service { 'disthene':
    ensure     => running,
    hasrestart => true,
    restart    => '/etc/init.d/disthene reload',
    require    => [Package['disthene'],
      File['disthene_config'],
    #Sysctl['net.core.somaxconn']
    ],
  }
}
