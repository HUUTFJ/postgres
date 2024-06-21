# Copyright (c) 2024, PostgreSQL Global Development Group

use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

# Construct a cascading replication system like:
#
# node_a --(logical replication)--> node_b --(streaming replication)--> node_c
#

# Set up node A as publisher
my $node_a = PostgreSQL::Test::Cluster->new('node_a');
my $aconnstr = $node_a->connstr;
$node_a->init(allows_streaming => 'logical');
$node_a->start;

# On node A
# - create databases
# - create test tables
# - insert a row
# - create publications
$node_a->safe_psql(
	'postgres', q(
	CREATE DATABASE pg1;
	CREATE DATABASE pg2;
));
$node_a->safe_psql('pg1', 'CREATE TABLE tbl1 (a text)');
$node_a->safe_psql('pg1', "INSERT INTO tbl1 VALUES('first row')");
$node_a->safe_psql('pg2', 'CREATE TABLE tbl2 (a text)');
$node_a->safe_psql('pg1', 'CREATE PUBLICATION pub1_pg1 FOR ALL TABLES');
$node_a->safe_psql('pg1', 'CREATE PUBLICATION pub2_pg1');
$node_a->safe_psql('pg2', 'CREATE PUBLICATION pub1_pg2 FOR ALL TABLES');
$node_a->safe_psql('pg2', 'CREATE PUBLICATION pub2_pg2');

# Set up node B as subscriber/primary
my $node_b = PostgreSQL::Test::Cluster->new('node_b');
my $bconnstr = $node_b->connstr;
$node_b->init(allows_streaming => 'logical');
$node_b->start;

# On node B
# - create databases
# - create subscriptions
$node_b->safe_psql(
	'postgres', q(
	CREATE DATABASE pg1;
	CREATE DATABASE pg2;
));
$node_b->safe_psql('pg1', 'CREATE TABLE tbl1 (a text)');
$node_b->safe_psql('pg2', 'CREATE TABLE tbl2 (a text)');
$node_b->safe_psql('pg1',
    "CREATE SUBSCRIPTION sub1_pg1 CONNECTION '$aconnstr dbname=pg1' PUBLICATION pub1_pg1");
$node_b->safe_psql('pg1',
    "CREATE SUBSCRIPTION sub2_pg1 CONNECTION '$aconnstr dbname=pg1' PUBLICATION pub2_pg1");
$node_b->safe_psql('pg2',
    "CREATE SUBSCRIPTION sub1_pg2 CONNECTION '$aconnstr dbname=pg2' PUBLICATION pub1_pg2");
$node_b->safe_psql('pg2',
    "CREATE SUBSCRIPTION sub2_pg2 CONNECTION '$aconnstr dbname=pg2' PUBLICATION pub2_pg2");

$node_b->wait_for_subscription_sync($node_a, 'sub1_pg1');
$node_b->wait_for_subscription_sync($node_a, 'sub1_pg2');

# Confirms logical replication works well
my $result = $node_b->safe_psql('pg1', 'SELECT * FROM tbl1;');
is($result, 'first row', 'check logical replication works well');

# Set up node C as standby
$node_b->backup('backup_1');
my $node_c = PostgreSQL::Test::Cluster->new('node_c');
$node_c->init_from_backup($node_b, 'backup_1', has_streaming => 1);
$node_c->append_conf(
	'postgresql.conf', qq[
primary_conninfo = '$bconnstr'
]);
$node_c->set_standby_mode();
$node_c->start;

$node_b->wait_for_replay_catchup($node_c);

# Confirms streaming replication works well
$result = $node_c->safe_psql('pg1', 'SELECT * FROM tbl1;');
is($result, 'first row', 'check streaming replication works well');

$node_c->stop;

# Run pg_createsubscriber
command_ok(
	[
		'pg_createsubscriber', '--verbose',
		'--recovery-timeout', "$PostgreSQL::Test::Utils::timeout_default",
		'--verbose',
        '--pgdata', $node_c->data_dir,
        '--publisher-server', $bconnstr,
        '--socket-directory', $node_c->host,
        '--subscriber-port', $node_c->port,
        '--database', 'pg1',
        '--database', 'pg2'
	],
	'run pg_createsubscriber on node S');

# Confirms pre-existing subscriptions are removed from the converted node
$node_c->start;
$result = $node_c->safe_psql('pg1',
    "SELECT subname FROM pg_subscription WHERE subname NOT LIKE 'pg_createsubscriber%';");
is($result, '', 'check subscriptions are removed');

# Confirms pre-existing subscriptions still exist on the primary node
$result = $node_b->safe_psql('pg1',
    "SELECT subname, subenabled, subslotname FROM pg_subscription WHERE subname NOT LIKE 'pg_createsubscriber%' ORDER BY subname;");
is($result, 'sub1_pg1|t|sub1_pg1
sub1_pg2|t|sub1_pg2
sub2_pg1|t|sub2_pg1
sub2_pg2|t|sub2_pg2', 'check subscriptions still exist');

done_testing();
