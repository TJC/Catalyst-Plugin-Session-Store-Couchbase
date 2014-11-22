#!/usr/bin/env perl

use strict;
use warnings;
use Test::More tests => 8;

use_ok('Catalyst::Plugin::Session::Store::Couchbase');

ok(
	my $s = Catalyst::Plugin::Session::Store::Couchbase->new(),
	'new'
);

test__build_couchbase_url();

sub test__build_couchbase_url {
	my $response = Catalyst::Plugin::Session::Store::Couchbase::_build_couchbase_url({
		server => 'localhost',
	});
	like(
		$response,
		qr/^couchbase:\/\/localhost\/default\?/,
		'_build_couchbase_url - host and defaults - pre-query url'
	);
	like(
		$response,
		qr/config_node_timeout=6/,
		'_build_couchbase_url - host and defaults - config_node_timeout'
	);
	$response = Catalyst::Plugin::Session::Store::Couchbase::_build_couchbase_url({
		server => 'localhost',
		timeout => 20,
	});
	like(
		$response,
		qr/config_node_timeout=20/,
		'_build_couchbase_url - host and timeout - config_node_timeout'
	);
	$response = Catalyst::Plugin::Session::Store::Couchbase::_build_couchbase_url({
		server => 'host1,host2,host3',
		bucket => 'different'
	});
	like(
		$response,
		qr/^couchbase:\/\/host1,host2,host3\/different\?/,
		'_build_couchbase_url - multihost and bucket - pre-query url'
	);
	$response = eval {
		Catalyst::Plugin::Session::Store::Couchbase::_build_couchbase_url({
			server => 'localhost',
			ssl => 1,
		});
	};
	like(
		$@,
		qr/SSL enabled, but certpath is missing or invalid/,
		'_build_couchbase_url - ssl without certpath - throws exception'
	);
	ok(
		(not $response),
		'_build_couchbase_url - ssl without certpath - empty response'
	);
}

done_testing();

1;
