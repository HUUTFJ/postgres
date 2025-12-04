
# Copyright (c) 2025, PostgreSQL Global Development Group

# This tests that dependency tracking between transactions can work well

use strict;
use warnings;
use PostgreSQL::Test::Cluster;
use PostgreSQL::Test::Utils;
use Test::More;

if ($ENV{enable_injection_points} ne 'yes')
{
	plan skip_all => 'Injection points not supported by this build';
}

# Initialize publisher node
my $node_publisher = PostgreSQL::Test::Cluster->new('publisher');
$node_publisher->init(allows_streaming => 'logical');
$node_publisher->append_conf('postgresql.conf',
    "max_prepared_transactions = 10");
$node_publisher->start;

# Insert initial data
$node_publisher->safe_psql('postgres',
    "CREATE TABLE regress_tab (id int PRIMARY KEY, value text);");
$node_publisher->safe_psql('postgres',
    "INSERT INTO regress_tab VALUES (generate_series(1, 10), 'test');");

# Create a publication
$node_publisher->safe_psql('postgres',
    "CREATE PUBLICATION regress_pub FOR ALL TABLES;");

# Initialize subscriber node
my $node_subscriber = PostgreSQL::Test::Cluster->new('subscriber');
$node_subscriber->init;
$node_subscriber->append_conf('postgresql.conf', "log_min_messages = debug1");
$node_subscriber->append_conf('postgresql.conf',
	"max_logical_replication_workers = 10");
$node_subscriber->append_conf('postgresql.conf',
    "max_prepared_transactions = 10");
$node_subscriber->start;

# Check if the extension injection_points is available, as it may be
# possible that this script is run with installcheck, where the module
# would not be installed by default.
if (!$node_subscriber->check_extension('injection_points'))
{
	plan skip_all => 'Extension injection_points not installed';
}

$node_subscriber->safe_psql('postgres', 'CREATE EXTENSION injection_points;');

# Create a subscription
my $publisher_connstr = $node_publisher->connstr . ' dbname=postgres';

$node_subscriber->safe_psql('postgres',
    "CREATE TABLE regress_tab (id int PRIMARY KEY, value text);");
$node_subscriber->safe_psql('postgres',
    "CREATE SUBSCRIPTION regress_sub CONNECTION '$publisher_connstr' PUBLICATION regress_pub;");

# Wait for initial table sync to finish
$node_subscriber->wait_for_subscription_sync($node_publisher, 'regress_sub');

# Insert tuples on publisher
#
# XXX This may not enough to launch a parallel apply worker, because
# table_states_not_ready is not discarded yet.
$node_publisher->safe_psql('postgres',
    "INSERT INTO regress_tab VALUES (generate_series(11, 20), 'test');");
$node_publisher->wait_for_catchup('regress_sub');

# Insert tuples again
$node_publisher->safe_psql('postgres',
    "INSERT INTO regress_tab VALUES (generate_series(21, 30), 'test');");
$node_publisher->wait_for_catchup('regress_sub');

# Verify the parallel apply worker is launched
my $result = $node_subscriber->safe_psql('postgres',
    "SELECT count(1) FROM pg_stat_activity WHERE backend_type = 'logical replication parallel worker'");
is($result, '1', "parallel apply worker is laucnhed by a non-streamed transaction");

# Attach an injection_point. Parallel workers would wait before the commit
$node_subscriber->safe_psql('postgres',
	"SELECT injection_points_attach('parallel-worker-before-commit','wait');"
);

# Insert tuples on publisher
$node_publisher->safe_psql('postgres',
    "INSERT INTO regress_tab VALUES (generate_series(31, 40), 'test');");

# Wait until the parallel worker enters the injection point.
$node_subscriber->wait_for_event('logical replication parallel worker',
	'parallel-worker-before-commit');

my $offset = -s $node_subscriber->logfile;

# Insert tuples on publisher again. This transaction is independent from the
# previous one, but the parallel worker would wait till it finishes
$node_publisher->safe_psql('postgres',
    "INSERT INTO regress_tab VALUES (generate_series(41, 50), 'test');");

# Verify the parallel worker waits for the transaction
my $str = $node_subscriber->wait_for_log(qr/wait for depended xid ([1-9][0-9]+)/, $offset);
my $xid = $str =~ /wait for depended xid ([1-9][0-9]+)/;

# Update tuples which have not been applied yet on subscriber because the
# parallel worker stops at the injection point. Newly assigned worker also
# waits for the same transactions as above.
$node_publisher->safe_psql('postgres',
    "UPDATE regress_tab SET value = 'updated' WHERE id BETWEEN 31 AND 35;");

# Verify the parallel worker waits for the same transaction
$node_subscriber->wait_for_log(qr/wait for depended xid $xid/, $offset);

# Wakeup the parallel worker. We detach first no to stop other parallel workers
$node_subscriber->safe_psql('postgres', qq[
    SELECT injection_points_detach('parallel-worker-before-commit');
    SELECT injection_points_wakeup('parallel-worker-before-commit');
]);

# Verify the parallel worker wakes up
$node_subscriber->wait_for_log(qr/finish waiting for depended xid $xid/, $offset);

$result =
  $node_subscriber->safe_psql('postgres', "SELECT count(1) FROM regress_tab");
is ($result, 50, 'inserts are replicated to subscriber');

$result =
  $node_subscriber->safe_psql('postgres',
    "SELECT count(1) FROM regress_tab WHERE value = 'updated'");
is ($result, 5, 'updates are also replicated to subscriber');

# Ensure PREPAREd transaction also affects the parallel apply

$node_subscriber->safe_psql('postgres',
    "ALTER SUBSCRIPTION regress_sub DISABLE;");
$node_subscriber->poll_query_until('postgres',
	"SELECT count(*) = 0 FROM pg_stat_activity WHERE backend_type = 'logical replication apply worker'"
);
$node_subscriber->safe_psql(
	'postgres', "
    ALTER SUBSCRIPTION regress_sub SET (two_phase = on);
    ALTER SUBSCRIPTION regress_sub ENABLE;");

$result = $node_subscriber->safe_psql('postgres',
    "SELECT count(1) FROM pg_stat_activity WHERE backend_type = 'logical replication parallel worker'");
is($result, '0', "no parallel apply workers exist after restart");

# Attach an injection_point. Parallel workers would wait before the prepare
$node_subscriber->safe_psql('postgres',
	"SELECT injection_points_attach('parallel-worker-before-prepare','wait');"
);

# PREPARE a transaction on publisher. It would be handled by a parallel apply
# worker.
$node_publisher->safe_psql('postgres', qq[
    BEGIN;
    INSERT INTO regress_tab VALUES (generate_series(51, 60), 'prepare');
    PREPARE TRANSACTION 'regress_prepare';
]);

# Wait until the parallel worker enters the injection point.
$node_subscriber->wait_for_event('logical replication parallel worker',
	'parallel-worker-before-prepare');

$offset = -s $node_subscriber->logfile;

# Insert tuples on publisher again. This transaction waits for the prepared
# transaction
$node_publisher->safe_psql('postgres',
    "INSERT INTO regress_tab VALUES (generate_series(61, 70), 'test');");

# Verify the parallel worker waits for the transaction
$str = $node_subscriber->wait_for_log(qr/wait for depended xid ([1-9][0-9]+)/, $offset);
$xid = $str =~ /wait for depended xid ([1-9][0-9]+)/;

# Wakeup the parallel worker
$node_subscriber->safe_psql('postgres', qq[
    SELECT injection_points_detach('parallel-worker-before-prepare');
    SELECT injection_points_wakeup('parallel-worker-before-prepare');
]);

$node_subscriber->wait_for_log(qr/finish waiting for depended xid $xid/, $offset);

# COMMIT the prepared transaction. It is always handled by the leader
$node_publisher->safe_psql('postgres', "COMMIT PREPARED 'regress_prepare';");
$node_publisher->wait_for_catchup('regress_sub');

# Ensure streamed transactions waits the previous transaction

$node_publisher->append_conf('postgresql.conf',
   "logical_decoding_work_mem = 64kB");
$node_publisher->reload;
# Run a query to make sure that the reload has taken effect.
$node_publisher->safe_psql('postgres', "SELECT 1");

# Attach the injection_point again
$node_subscriber->safe_psql('postgres',
	"SELECT injection_points_attach('parallel-worker-before-commit','wait');"
);

$node_publisher->safe_psql('postgres',
    "INSERT INTO regress_tab VALUES (generate_series(71, 80), 'test');");

# Wait until the parallel worker enters the injection point.
$node_subscriber->wait_for_event('logical replication parallel worker',
	'parallel-worker-before-commit');

# Run a transaction which would be streamed
my $h = $node_publisher->background_psql('postgres', on_error_stop => 0);

$offset = -s $node_subscriber->logfile;

$h->query_safe(
	q{
BEGIN;
UPDATE regress_tab SET value = 'streamed-updated' WHERE id BETWEEN 71 AND 80;
INSERT INTO regress_tab VALUES (generate_series(100, 5100), 'streamed');
});

# Verify the parallel worker waits for the transaction
$str = $node_subscriber->wait_for_log(qr/wait for depended xid ([1-9][0-9]+)/, $offset);
$xid = $str =~ /wait for depended xid ([1-9][0-9]+)/;

# Wakeup the parallel worker
$node_subscriber->safe_psql('postgres', qq[
    SELECT injection_points_detach('parallel-worker-before-commit');
    SELECT injection_points_wakeup('parallel-worker-before-commit');
]);

# Verify the streamed transaction can be applied
$node_subscriber->wait_for_log(qr/finish waiting for depended xid $xid/, $offset);

$h->query_safe("COMMIT;");

done_testing();
