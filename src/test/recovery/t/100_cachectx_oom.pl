use strict;
use warnings;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;
use Time::HiRes qw(usleep nanosleep);

my $node_primary;
my $node_subscriber;
my $pid;
my $kid;
my $publisher_connstr;
my $count = 1000;
my $process = 3;

$node_primary = PostgreSQL::Test::Cluster->new('primary');
$node_primary->init(allows_streaming => 'logical');
$node_primary->start;

$node_subscriber = PostgreSQL::Test::Cluster->new('subscriber');
$node_subscriber->init(allows_streaming => 'logical');
$node_subscriber->append_conf('postgresql.conf',	'wal_receiver_timeout = 0');
$node_subscriber->start;
$node_primary->safe_psql('postgres',
	"CREATE PUBLICATION my_pub FOR ALL TABLES;");

$publisher_connstr = $node_primary->connstr . ' dbname=postgres';

$node_subscriber->safe_psql('postgres',
	"CREATE SUBSCRIPTION my_sub CONNECTION '$publisher_connstr' PUBLICATION my_pub;"
);

usleep(1_000_000); #1s

$node_primary->safe_psql('postgres', qq(
	CREATE TABLE my_table(
		col01 int8,
		col02 int8,
		col03 int8,
		col04 int8,
		col05 int8,
		col06 int8,
		col07 int8,
		col08 int8,
		col09 int8,
		col10 int8,
		col11 char(256),
		col12 char(256),
		col13 char(256),
		col14 char(256),
		col15 char(256),
		col16 char(256),
		col17 char(256),
		col18 char(256),
		col19 char(256),
		col20 char(256),
		col21 jsonb,
		col22 jsonb,
		col23 jsonb,
		col24 jsonb,
		col25 jsonb,
		col26 jsonb,
		col27 jsonb,
		col28 jsonb,
		col29 jsonb,
		col30 jsonb,
		col31 boolean,
		col32 boolean,
		col33 boolean,
		col34 boolean,
		col35 boolean,
		col36 boolean,
		col37 boolean,
		col38 boolean,
		col39 boolean,
		col40 boolean,
		col41 timestamp,
		col42 timestamp,
		col43 timestamp,
		col44 timestamp,
		col45 timestamp,
		col46 timestamp,
		col47 timestamp,
		col48 timestamp,
		col49 timestamp,
		col50 timestamp);));

$node_subscriber->safe_psql('postgres', qq(
	CREATE TABLE my_table(
		col01 int8,
		col02 int8,
		col03 int8,
		col04 int8,
		col05 int8,
		col06 int8,
		col07 int8,
		col08 int8,
		col09 int8,
		col10 int8,
		col11 char(256),
		col12 char(256),
		col13 char(256),
		col14 char(256),
		col15 char(256),
		col16 char(256),
		col17 char(256),
		col18 char(256),
		col19 char(256),
		col20 char(256),
		col21 jsonb,
		col22 jsonb,
		col23 jsonb,
		col24 jsonb,
		col25 jsonb,
		col26 jsonb,
		col27 jsonb,
		col28 jsonb,
		col29 jsonb,
		col30 jsonb,
		col31 boolean,
		col32 boolean,
		col33 boolean,
		col34 boolean,
		col35 boolean,
		col36 boolean,
		col37 boolean,
		col38 boolean,
		col39 boolean,
		col40 boolean,
		col41 timestamp,
		col42 timestamp,
		col43 timestamp,
		col44 timestamp,
		col45 timestamp,
		col46 timestamp,
		col47 timestamp,
		col48 timestamp,
		col49 timestamp,
		col50 timestamp);));

# subscriber
for (my $i = 0; $i < $process; $i += 1) {
	$pid = fork();
	if ($pid == 0)
	{
		for (my $j = 0; $j < $count; $j += 1) {
			$node_subscriber->safe_psql('postgres', qq(
				CREATE TABLE my_table_$i\_$j AS TABLE my_table WITH NO DATA;));
			if ( $j % 100 == 0 ) {
				print "\nsubscriber:$j";
			}
		}
		exec "echo '\nchild:$i bye'";
	}
}
while (($kid = wait()) != -1 ) {
	print "\nReaped child $kid\n";
}

# primary
for (my $i = 0; $i < $process; $i += 1) {
	$pid = fork();
	if ($pid == 0)
	{
		for (my $j = 0; $j < $count; $j += 1) {
			$node_primary->safe_psql('postgres', qq(
				CREATE TABLE my_table_$i\_$j AS TABLE my_table WITH NO DATA;
				INSERT INTO my_table_$i\_$j (col01) VALUES($j);
				INSERT INTO my_table_$i\_$j (col01) VALUES($j);
				DROP TABLE my_table_$i\_$j;
				));
			if ( $j % 100 == 0 ) {
				print "\nnode_primary:$j";
			}
		}
		exec "echo '\nchild:$i bye'";
	}
}
while (($kid = wait()) != -1 ) {
	print "\nReaped child $kid\n";
}

usleep(3600_000_000); #3600s
# usleep(4_000_000);

is(1, 1, "test end");
$node_primary->stop;
done_testing();
