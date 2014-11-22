package Catalyst::Plugin::Session::Store::Couchbase;
use Moose;
use MRO::Compat;
extends 'Catalyst::Plugin::Session::Store';
with 'Catalyst::ClassData';
use Catalyst::Exception;
use Couchbase::Bucket;
use Couchbase::Document;
use namespace::clean -except => 'meta'; # The last bit cargo culted.
use Storable qw(nfreeze thaw);
use URI::Escape qw(uri_escape);

our $VERSION = '0.94';

__PACKAGE__->mk_classdata('_session_cb_bucket_handle');
__PACKAGE__->mk_classdata('_session_couchbase_prefix');

=head1 NAME

Catalyst::Plugin::Session::Store::Couchbase

=head1 SYNOPSIS

  use Catalyst qw{Session Session::Store::Couchbase Session::State::Cookie};
  MyApp->config(
    'Plugin::Session' => {
      expires => 7200,
    },
    Couchbase => {
      server => 'couchbase01.domain',
      password => 'password',
      bucket => 'default',
      ssl => 1,
      certpath => '/example/certpath/cert.pem',
    }
  );

=head1 CONFIG OPTIONS

=over 4

=item server

The Couchbase server to connect to. If there are multiple nodes in a cluster,
multiple servers can be provided as a comma-delimited list (ex: host1,host2),
which can improve reliability if the primary connection node is down. If the
cluster is responding on a different port, it may be provided as host:port,
where port is the memcached listening port.

=item password

Password for the given bucket. This can be omitted if a password is not set on
the given bucket.

=item bucket

Bucket name to connect to. Defaults to "default" if it is not provided.

=item ssl

Set to 1 if the cluster is SSL-enabled and a SSL connection is desired. SSL
support requires Couchbase Server 2.5 or higher and a copy of the server's
SSL certificate. Defaults to off.

=item certpath

Path to the server's SSL pem-encoded certificate for validation. Not required if
SSL is disabled.

=item timeout

Timeout (in seconds) to allow for bootstrapping a client. Defaults to 6.

=cut

sub setup_session {
    my $c = shift;
    $c->maybe::next::method(@_);

    $c->log->debug("Setting up Couchbase session store") if $c->debug;

    my $cfg = $c->config->{'Couchbase'};

    my $appname = "$c";
    $c->_session_couchbase_prefix($appname . "sess:");

    my $connection_url = _build_couchbase_url($cfg);
    my $bucket = Couchbase::Bucket->new($connection_url);
    Catalyst::Exception->throw("Couchbase bucket object undefined!")
        unless defined $bucket;

    $c->_session_cb_bucket_handle($bucket);

    return 1;
}

sub get_session_data {
    my ($c, $key) = @_;
    croak("No cache key specified") unless length($key);
    my ( $type, $id ) = split( ':', $key );

    $key = $c->_session_couchbase_prefix . $id;
    my $doc = Couchbase::Document->new($key);
    $c->_session_cb_bucket_handle->get_and_touch($doc);
    if (defined $doc and $doc->is_ok and defined $doc->value) {
        return $doc->value->{$type};
    }
    elsif (defined $doc) {
        my $err = $doc->errstr;
        Catalyst::Exception->throw(
            "Failed to fetch Couchbase item: $err. Key was: $key"
        ) unless $err =~ /key does not exist/;
    }
    return;
}

sub store_session_data {
    my ($c, $key, $data) = @_;
    croak("No cache key specified") unless length($key);
    my ( $type, $id ) = split( ':', $key );

    $key = $c->_session_couchbase_prefix . $id;
    my $expiry = $c->session_expires ? $c->session_expires - time() : 0;
    if (not $expiry) {
        $c->log->warn("No expiry set for sessions! Defaulting to one hour..");
        $expiry = 3600;
    }
    my $doc = Couchbase::Document->new($key);
    $c->_session_cb_bucket_handle->get($doc);
    unless ($doc->is_ok) {
        $doc = Couchbase::Document->new( $key, {} );
    }
    $doc->value->{$type} = $data;
    $doc->expiry($expiry);
    $c->_session_cb_bucket_handle->upsert($doc);
    unless ($doc->is_ok) {
        Catalyst::Exception->throw(
            "Couldn't save $key / $data in couchbase storage: " . $doc->errstr
        );
    }
    return 1;
}

sub delete_session_data {
    my ($c, $key) = @_;
    $c->log->debug("Couchbase session store: delete_session_data($key)") if $c->debug;
    croak("No cache key specified") unless length($key);
    my ( $type, $id ) = split( ':', $key );

    $key = $c->_session_couchbase_prefix . $id;

    my $doc = Couchbase::Document->new($key);
    $c->_session_cb_bucket_handle->remove($doc);
    return 1;
}

# Not required as Couchbase expires things itself.
sub delete_expired_sessions { }

# Build a Couchbase connection string
sub _build_couchbase_url {
    my ($cfg) = @_;

    # Set timeout to 6 seconds
    my %options = (
        config_node_timeout => ( $cfg->{timeout} or 6 ) * 1_000_000,
    );

    # Connection URL is couchbases?://host1,host2/bucket?options
    my $connection_url = join('/',
        ':/',
        $cfg->{server},
        ( $cfg->{bucket} or 'default' ),
    );

    $options{password} = $cfg->{password} if ($cfg->{password});

    if ($cfg->{ssl}) {
        if (not $cfg->{certpath} or not -e $cfg->{certpath}) {
            Catalyst::Exception->throw(
                'SSL enabled, but certpath is missing or invalid'
            );
        }
        $connection_url = 'couchbases' . $connection_url;
        $options{certpath} = $cfg->{certpath};
    } else {
        $connection_url = 'couchbase' . $connection_url;
    }

    $connection_url .= '?' . join(
        '&', ( map { $_ . '=' . uri_escape($options{$_}) } keys %options )
    );

    return $connection_url;
}

=head1 AUTHOR

Toby Corkindale, C<< <tjc at wintrmute.net> >>

=head1 BUGS

Please report any bugs to the Github repo for this module:

https://github.com/TJC/Catalyst-Plugin-Session-Store-Couchbase

=head1 SUPPORT

You can find documentation for this module with the perldoc command.

    perldoc Catalyst::Plugin::Session::Store::Couchbase


You can also look for information at:

=over 4

=item * AnnoCPAN: Annotated CPAN documentation

L<http://annocpan.org/dist/Catalyst-Plugin-Session-Store-Couchbase>

=item * CPAN Ratings

L<http://cpanratings.perl.org/d/Catalyst-Plugin-Session-Store-Couchbase>

=item * Search CPAN

L<http://search.cpan.org/dist/Catalyst-Plugin-Session-Store-Couchbase/>

=back


=head1 ACKNOWLEDGEMENTS

This module was supported by Strategic Data. The module was originally
written for their internal use, and the company has allowed me to produce
an open-source version.

=head1 LICENSE AND COPYRIGHT

Copyright 2013 Toby Corkindale.

This program is free software; you can redistribute it and/or modify it
under the terms of either: the GNU General Public License as published
by the Free Software Foundation; or the Artistic License.

See http://dev.perl.org/licenses/ for more information.


=cut


__PACKAGE__->meta->make_immutable;
1;
