
# Restart a primary server with streaming replication in a loop
use strict;
use warnings FATAL => 'all';
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;
use File::Basename;

# Initialize primary node
my $node_primary = PostgreSQL::Test::Cluster->new('primary');
$node_primary->init(allows_streaming => 1);

$node_primary->start;
is( $node_primary->psql(
		'postgres',
		qq[SELECT pg_create_physical_replication_slot('standby_1');]),
	0,
	'physical slot created on primary');
my $backup_name = 'my_backup';

$node_primary->safe_psql('postgres', 'create database regression');

# Take backup
$node_primary->backup($backup_name);

# Create streaming standby linking to primary
my $node_standby_1 = PostgreSQL::Test::Cluster->new('standby_1');
$node_standby_1->init_from_backup($node_primary, $backup_name,
	has_streaming => 1);
$node_standby_1->append_conf('postgresql.conf',
	"primary_slot_name = standby_1");
$node_standby_1->append_conf('postgresql.conf',
	'max_standby_streaming_delay = 600s');
$node_standby_1->start;

for (my $i = 0; $i < 100; $i++)
{
$node_primary->restart;
$node_primary->wait_for_replay_catchup($node_standby_1);
ok(1);
$node_primary->safe_psql('regression', "create table t$i(i int);");
$node_primary->safe_psql('regression', "insert into t$i select generate_series(1, 10000);");
$node_primary->safe_psql('regression', "drop table t$i;");
}

$node_standby_1->stop;
$node_primary->stop;

done_testing();