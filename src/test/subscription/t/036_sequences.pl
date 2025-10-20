
# Copyright (c) 2025, PostgreSQL Global Development Group

# This tests that sequences are synced correctly to the subscriber
use strict;
use warnings;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

# Initialize publisher node
my $node_publisher = PostgreSQL::Test::Cluster->new('publisher');

# Avoid checkpoint during the test, otherwise, extra values will be fetched for
# the sequences which will cause the test to fail randomly.
$node_publisher->init(allows_streaming => 'logical');
$node_publisher->append_conf('postgresql.conf', 'checkpoint_timeout = 1h');
$node_publisher->start;

# Initialize subscriber node
my $node_subscriber = PostgreSQL::Test::Cluster->new('subscriber');
$node_subscriber->init;
$node_subscriber->start;

# Setup structure on the publisher
my $ddl = qq(
	CREATE TABLE regress_seq_test (v BIGINT);
	CREATE SEQUENCE regress_s1;
);
$node_publisher->safe_psql('postgres', $ddl);

# Setup the same structure on the subscriber, plus some extra sequences that
# we'll create on the publisher later
$ddl = qq(
	CREATE TABLE regress_seq_test (v BIGINT);
	CREATE SEQUENCE regress_s1;
	CREATE SEQUENCE regress_s2;
	CREATE SEQUENCE regress_s3;
	CREATE SEQUENCE regress_s4
);
$node_subscriber->safe_psql('postgres', $ddl);

# Insert initial test data
$node_publisher->safe_psql(
	'postgres', qq(
	-- generate a number of values using the sequence
	INSERT INTO regress_seq_test SELECT nextval('regress_s1') FROM generate_series(1,100);
));

# Setup logical replication pub/sub
my $publisher_connstr = $node_publisher->connstr . ' dbname=postgres';
$node_publisher->safe_psql('postgres',
	"CREATE PUBLICATION regress_seq_pub FOR ALL SEQUENCES");
$node_subscriber->safe_psql('postgres',
	"CREATE SUBSCRIPTION regress_seq_sub CONNECTION '$publisher_connstr' PUBLICATION regress_seq_pub"
);

# Wait for initial sync to finish
my $synced_query =
  "SELECT count(1) = 0 FROM pg_subscription_rel WHERE srsubstate NOT IN ('r');";
$node_subscriber->poll_query_until('postgres', $synced_query)
  or die "Timed out while waiting for subscriber to synchronize data";

# Check the initial data on subscriber
my $result = $node_subscriber->safe_psql(
	'postgres', qq(
	SELECT last_value, log_cnt, is_called FROM regress_s1;
));
is($result, '100|0|t', 'initial test data replicated');

##########
## ALTER SUBSCRIPTION ... REFRESH PUBLICATION should cause sync of new
# sequences of the publisher, but changes to existing sequences should
# not be synced.
##########

# Create a new sequence 'regress_s2', and update existing sequence 'regress_s1'
$node_publisher->safe_psql(
	'postgres', qq(
	CREATE SEQUENCE regress_s2;
	INSERT INTO regress_seq_test SELECT nextval('regress_s2') FROM generate_series(1,100);

	-- Existing sequence
	INSERT INTO regress_seq_test SELECT nextval('regress_s1') FROM generate_series(1,100);
));

# Do ALTER SUBSCRIPTION ... REFRESH PUBLICATION
$result = $node_subscriber->safe_psql(
	'postgres', qq(
	ALTER SUBSCRIPTION regress_seq_sub REFRESH PUBLICATION
));
$node_subscriber->poll_query_until('postgres', $synced_query)
  or die "Timed out while waiting for subscriber to synchronize data";

$result = $node_publisher->safe_psql(
	'postgres', qq(
	SELECT last_value, log_cnt, is_called FROM regress_s1;
));
is($result, '200|31|t', 'Check sequence value in the publisher');

# Check - existing sequence is not synced
$result = $node_subscriber->safe_psql(
	'postgres', qq(
	SELECT last_value, log_cnt, is_called FROM regress_s1;
));
is($result, '100|0|t',
	'REFRESH PUBLICATION does not sync existing sequence');

# Check - newly published sequence is synced
$result = $node_subscriber->safe_psql(
	'postgres', qq(
	SELECT last_value, log_cnt, is_called FROM regress_s2;
));
is($result, '100|0|t',
	'REFRESH PUBLICATION will sync newly published sequence');

##########
## ALTER SUBSCRIPTION ... REFRESH SEQUENCES should cause sync of new sequences
# of the publisher, and changes to existing sequences should also be synced.
##########

# Create a new sequence 'regress_s3', and update the existing sequence
# 'regress_s2'.
$node_publisher->safe_psql(
	'postgres', qq(
	CREATE SEQUENCE regress_s3;
	INSERT INTO regress_seq_test SELECT nextval('regress_s3') FROM generate_series(1,100);

	-- Existing sequence
	INSERT INTO regress_seq_test SELECT nextval('regress_s2') FROM generate_series(1,100);
));

# Do ALTER SUBSCRIPTION ... REFRESH SEQUENCES
$result = $node_subscriber->safe_psql(
	'postgres', qq(
	ALTER SUBSCRIPTION regress_seq_sub REFRESH SEQUENCES
));
$node_subscriber->poll_query_until('postgres', $synced_query)
  or die "Timed out while waiting for subscriber to synchronize data";

# Check - existing sequences are synced
$result = $node_subscriber->safe_psql(
	'postgres', qq(
	SELECT last_value, log_cnt, is_called FROM regress_s1;
));
is($result, '200|0|t',
	'REFRESH SEQUENCES will sync existing sequences');
$result = $node_subscriber->safe_psql(
	'postgres', qq(
	SELECT last_value, log_cnt, is_called FROM regress_s2;
));
is($result, '200|0|t',
	'REFRESH SEQUENCES will sync existing sequences');

# Check - newly published sequence is not synced
$result = $node_subscriber->safe_psql(
	'postgres', qq(
	SELECT last_value, log_cnt, is_called FROM regress_s3;
));
is($result, '1|0|f',
	'REFRESH SEQUENCES will not sync newly published sequence');

##########
## ALTER SUBSCRIPTION ... REFRESH PUBLICATION with (copy_data = off) should
# not update the sequence values for the new sequence.
##########

# Create a new sequence 'regress_s4'
$node_publisher->safe_psql(
	'postgres', qq(
	CREATE SEQUENCE regress_s4;
	INSERT INTO regress_seq_test SELECT nextval('regress_s4') FROM generate_series(1,100);
));

# Do ALTER SUBSCRIPTION ... REFRESH PUBLICATION
$result = $node_subscriber->safe_psql(
	'postgres', qq(
	ALTER SUBSCRIPTION regress_seq_sub REFRESH PUBLICATION with (copy_data = false);
));
$node_subscriber->poll_query_until('postgres', $synced_query)
  or die "Timed out while waiting for subscriber to synchronize data";

$result = $node_publisher->safe_psql(
	'postgres', qq(
	SELECT last_value, log_cnt, is_called FROM regress_s4;
));
is($result, '100|32|t', 'Check sequence value in the publisher');

# Check - newly published sequence values are not updated
$result = $node_subscriber->safe_psql(
	'postgres', qq(
	SELECT last_value, log_cnt, is_called FROM regress_s4;
));
is($result, '1|0|f',
	'REFRESH PUBLICATION will not sync newly published sequence with copy_data as off'
);

##########
# ALTER SUBSCRIPTION ... REFRESH PUBLICATION should throw an error
# for sequence definition not matching between the publisher and the subscriber.
##########

# Create a new sequence 'regress_s5' whose START value is not the same in the
# publisher and subscriber.
$node_publisher->safe_psql(
	'postgres', qq(
	CREATE SEQUENCE regress_s5 START 1 INCREMENT 2;
));

$node_subscriber->safe_psql(
	'postgres', qq(
	CREATE SEQUENCE regress_s5 START 10 INCREMENT 2;
));

my $log_offset = -s $node_subscriber->logfile;

# Do ALTER SUBSCRIPTION ... REFRESH PUBLICATION
$node_subscriber->safe_psql(
	'postgres', "
	ALTER SUBSCRIPTION regress_seq_sub REFRESH PUBLICATION"
);

# Confirm that the warning for parameters differing is logged.
$node_subscriber->wait_for_log(
	qr/ERROR: ( [A-Z0-9]+:)? logical replication sequence synchronization failed for subscription "regress_seq_sub"\n.*DETAIL:.* Mismatched sequence\(s\) on subscriber: \("public.regress_s5"\)/,
	$log_offset);

$node_publisher->safe_psql(
	'postgres', qq(
	DROP SEQUENCE regress_s5;
));

# Confirm that the warning for missing sequence is logged.
$node_subscriber->wait_for_log(
	qr/LOG:  ? sequences not found on publisher removed from resynchronization: \("public.regress_s5"\)/,
	$log_offset);

done_testing();
