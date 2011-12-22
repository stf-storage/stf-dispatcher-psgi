package STF::Dispatcher::PSGI;
use strict;
our $VERSION = '1.05';
use Carp ();
use HTTP::Date ();
use Plack::Request;
use Plack::Middleware::HTTPExceptions;
use Scalar::Util ();
use STF::Dispatcher::PSGI::HTTPException;
use Class::Accessor::Lite
    rw => [ qw(impl) ]
;
use constant +{
    STF_DEBUG                         => $ENV{STF_DEBUG},
    STF_REPLICATION_HEADER            => 'X-STF-Replication-Count',
    STF_REPLICATION_HEADER_DEPRECATED => 'X-Replication-Count',
    STF_RECURSIVE_DELETE_HEADER       => 'X-STF-Recursive-Delete',
    STF_CONSISTENCY_HEADER            => "X-STF-Consistency",
    # XXX This below is should be deprecated. don't use
    STF_FORCE_MASTER_HEADER           => "X-STF-Force-MasterDB",
    STF_DELETED_OBJECTS_HEADER        => "X-STF-Deleted-Objects",
    STF_DEFAULT_REPLICATION_COUNT     => 2,
};

sub new {
    my ($class, %args) = @_;

    my $impl = $args{impl} or
        Carp::croak( "Required parameter 'impl' not specified" );
    foreach my $method (qw(create_bucket get_bucket delete_bucket create_object get_object delete_object delete_bucket modify_object is_valid_object)) {
        $impl->can( $method ) or
            Carp::croak("$impl does not implement $method");
    }

    bless { %args }, $class;
}

sub to_app {
    my $self = shift;
    my $app = sub { $self->handle_psgi(@_) };
    Plack::Middleware::HTTPExceptions->wrap( $app );
}

sub handle_psgi {
    my ($self, $env) = @_;

    my $guard = $self->impl->start_request( $env );

    my $req = Plack::Request->new($env);
    my $res;
    my $method = $env->{REQUEST_METHOD};
    if ($method =~ /^(?:GET|HEAD)$/) {
        $res = $self->get_object( $req );
    } elsif ($method eq 'PUT') {
        my $cl = $env->{CONTENT_LENGTH} || 0;
        if ( $cl == 0 ) {
            if (STF_DEBUG) {
                print STDERR "[Dispatcher] Content-Length = 0, creating bucket\n";
            }
            $res = $self->create_bucket( $req );
        } else {
            if (STF_DEBUG) {
                print STDERR "[Dispatcher] Content-Length > 0, creating object\n";
            }
            $res = $self->create_object( $req );
        }
    } elsif ($method eq 'DELETE') {
        $res = $self->delete_object( $req );
    } elsif ($method eq 'POST') {
        $res = $self->modify_object( $req );
    } else {
        $res = $req->new_response(400, [ "Content-Type" => "text/plain" ], [ "Bad Request" ]);
    }

    return $res->finalize();
}

sub parse_names {
    my ($self, $req) = @_;
    if ( $req->path !~ m{^/([^/]+)(?:/(.+)$)?} ) {
        if (STF_DEBUG) {
            print STDERR "[Dispatcher] Could not parse bucket/object name from " . $req->path . "\n";
        }
        return ();
    }
    return ($1, $2);
}

sub create_bucket {
    my ($self, $req) = @_;

    my ($bucket_name, $object_name) = $self->parse_names( $req );
    if ( $object_name ) {
        return $req->new_response( 400, [], [ "Bad bucket name $bucket_name/$object_name" ] );
    }

    my $bucket = $self->impl->get_bucket( {
        bucket_name => $bucket_name,
        request     => $req,
    } );
    if ($bucket) {
        return $req->new_response( 204, [], [] );
    }

    $bucket = $self->impl->create_bucket( {
        bucket_name => $bucket_name,
        request     => $req,
    } );
    if (! $bucket) {
        return $req->new_response( 500, [], [ "Failed to create bucket" ] );
    }

    return $req->new_response( 201, [], [ "Created $bucket_name" ] );
}

sub create_object {
    my ($self, $req) = @_;

    # find the appropriate bucket
    my ($bucket_name, $object_name) = $self->parse_names( $req );
    my $bucket = $self->impl->get_bucket( {
        bucket_name => $bucket_name,
        request     => $req
    } );
    if (! $bucket) {
        return $req->new_response(500, ["Content-Type" => "text/plain"], [ "Failed to find bucket for " . $req->path ] );
    }
    if (! $object_name) {
        return $req->new_response(400, [], ["Could not extract object name"]);
    }

    # try to find a suffix
    my ($suffix) = ( $req->path =~ /\.([a-zA-Z0-9]+)$/ );
    $suffix ||= 'dat';

    my $input = $req->input;
    my $code;
    if ( $code = $input->can('rewind') ) {
        $code->( $input );
    } elsif ( $code = $input->can('seek') ) {
        $code->( $input, 0, 0 );
    }

    my %ext_args;
    if ($req->content_type) {
        $ext_args{content_type} = $req->content_type;
    }

    my $object = $self->impl->create_object( {
        bucket      => $bucket,
        consistency => $req->header( STF_CONSISTENCY_HEADER ) || 0,
        object_name => $object_name,
        size        => $req->content_length || 0,
        suffix      => $suffix,
        input       => $input,
        replicas    => $req->header( STF_REPLICATION_HEADER ) ||
                       $req->header( STF_REPLICATION_HEADER_DEPRECATED ) ||
                       STF_DEFAULT_REPLICATION_COUNT || 0,
        request     => $req,
        %ext_args
    } );
    if (! $object) {
        return $req->new_response( 500, [], [ "Failed to create object" ] );
    }
    return $req->new_response( 201, [], [ "Created " . $req->path ] );
}

sub delete_object {
    my ($self, $req) = @_;

    # find the appropriate bucket
    my ($bucket_name, $object_name) = $self->parse_names( $req );
    my $bucket = $self->impl->get_bucket( {
        bucket_name => $bucket_name,
        request     => $req
    } );
    if (! $bucket) {
        if ( ! $object_name ) {
            return $req->new_response(404, ["Content-Type" => "text/plain"], [ "No such bucket $bucket_name" ] );
        } else {
            return $req->new_response(500, ["Content-Type" => "text/plain"], [ "Failed to find bucket for " . $req->path ] );
        }
    }

    # if there's no object_name, then this is a request to delete
    # the bucket, not an object
    if ( ! $object_name ) {
        my $bucket = $self->impl->get_bucket( {
            bucket_name => $bucket_name,
            request     => $req,
        } );

        my $ret = $self->impl->delete_bucket( {
            bucket    => $bucket,
            recursive => $req->header( STF_RECURSIVE_DELETE_HEADER ) || 0,
            request   => $req,
        } );
        if (! $ret) {
            return $req->new_response(500, ["Content-Type" => "text/plain"], ["Failed to delete bucket " . $bucket_name]);
        }

        return $req->new_response( 204, [], [] );
    }

    my $is_valid = $self->impl->is_valid_object( {
        bucket      => $bucket,
        object_name => $object_name,
        request     => $req,
    });
    if (! $is_valid) {
        return $req->new_response(404, [], [ "No such object " . $req->path ]);
    }

    if ($self->impl->delete_object( {
        bucket      => $bucket,
        object_name => $object_name,
        request     => $req,
    } )) {
        return $req->new_response( 204, [], [] );
    } else {
        return $req->new_response( 500, [ "Content-Type" ], [ "Failed to delete " . $req->path ] );
    }
}

sub get_object {
    my ($self, $req) = @_;
    # find the appropriate bucket
    my ($bucket_name, $object_name) = $self->parse_names( $req );
    my $bucket = $self->impl->get_bucket( {
        bucket_name => $bucket_name,
        request     => $req
    } );
    if (! $bucket) {
        return $req->new_response(500, ["Content-Type" => "text/plain"], [ "Failed to find bucket for " . $req->path ] );
    }

    my $object = $self->impl->get_object( {
        bucket       => $bucket,
        object_name  => $object_name,
        request      => $req,
        force_master => $req->header( STF_FORCE_MASTER_HEADER ) || 0,
    } );
    if (! $object) {
        return $req->new_response( 404, [], [ "Failed to get object " . $req->path ] );
    }

    my @headers;
    if ( my $ct = $object->can('content_type') ) {
        push @headers, "Content-Type", $object->content_type;
    }
    if ( my $lm = $object->can('modified_on') ) {
        push @headers, "Last-Modified", HTTP::Date::time2str($object->modified_on);
    }

    return $req->new_response( 200,
        \@headers,
        [ $req->method eq 'HEAD' ? '' : $object->content ]
    );
}

sub modify_object {
    my ($self, $req) = @_;

    my ($bucket_name, $object_name) = $self->parse_names( $req );
    my $bucket = $self->impl->get_bucket( {
        bucket_name => $bucket_name,
        request     => $req
    } );
    if (! $bucket) {
        return $req->new_response(500, ["Content-Type" => "text/plain"], [ "Failed to find bucket for " . $req->path ] );
    }

    my $ret = $self->impl->modify_object( {
        bucket      => $bucket,
        object_name => $object_name,
        replicas    => $req->header( STF_REPLICATION_HEADER ) ||
                       $req->header( STF_REPLICATION_HEADER_DEPRECATED ) ||
                       STF_DEFAULT_REPLICATION_COUNT || 0,
        request     => $req,
    } );

    return $req->new_response(204, [], []);
}

1;

__END__

=head1 NAME 

STF::Dispatcher::PSGI - Pluggable STF Dispatcher Interface

=head1 SYNOPSIS

    # in your stf.psgi
    use STF::Dispatcher::PSGI;

    my $object = ...;
    STF::Dispatcher::PSGI->new( impl => $object )->to_app;

=head1 DESCRIPTION

STF::Dispatcher::PSGI implements the basic STF Protocol (http://stf-storage.github.com) dispatcher component. It does not know how to actually store or retrieve data, so you must implement that portion yourself. 

The reason this exists is mainly to allow you to testing systems that interact with STF servers. For example, setting up the main STF implementation is quite a pain if all you want to do is to test your application, but with this module, you can easily create a dummy STF dispatcher.

For example, you can use STF::Dispatcher::Impl::Hash (which stores all data in a has in memory) for your tests:

    # in your stf.psgi
    use STF::Dispatcher::PSGI;
    use STF::Dispatcher::Impl::Hash;

    my $object = STF::Dispatcher::Impl::Hash->new();
    STF::Dispatcher::PSGI->new( impl => $object )->to_app;

And then you can do something like below in your application test to start a dummy STF server with Plack:

    use Plack::Runner;
    use Test::TCP;

    my $guard = Test::TCP->new(sub {
        my $port = shift;
        my $runner = Plack::Runner->new;
        $runner->parse_options('-p' => $port);
        $runner->run( do "stf.psgi" );
    });

    my $stf_uri = sprintf "http://127.0.0.1:%d", $guard->port;
    $ua->get( "$stf_uri/path/to/myobject.png" );

Of course, this is not only useful for testing, but it allows you to create a STF clone with a completely different backend without having to reimplement the entire STF protocol.

=head1 METHODS

=head2 $self = $class-E<gt>( impl =E<gt> $object )

Creates a new instance of STF::Dispatcher::PSGI. B<impl> must be the imeplementation object (L<see below|/THE "IMPLEMENTATION" OBJECT>).

=head2 $psgi_app = $self-E<gt>to_app()

Creates a PSGI app.

=head1 THE "IMPLEMENTATION" OBJECT

As described elsewhere, this module by itself DOES NOT work as a real STF server. This module will parse the request and extract the required data from that request, but has no idea how to actually use it. You must therefore provide it with an "implementation".

The simplest implementation is provided with this distribution: STF::Dispatcher::Impl::Hash. This implementation simply puts all the objects in an in-memory hash. See L<STF|STF> for a heavy duty example.

You can choose to create your own STF implementation. In that case, you need to implement list of methods described later.

In these methods, you may choose to throw an exception instead of returning a response. For example, in L<STF|STF>, we use X-Reproxy-URL to serve the objects. This means we cannot just return the fetched object. In that case, we throw an exception that L<Plack::Middleware::HTTPExceptions> can handle (our to_app method automatically enables Plack::Middleware::HTTPExceptions).

See the documentation for that module for details.

=head1 LIST OF REQUIRED METHODS IN THE IMPLEMENTATION

=head2 $object = $impl-E<gt>create_bucket(%args)

Used to create a bucket.

The implementation's get_bucket method will receive the following named parameters:

=over 4

=item B<request> =E<gt> $object

Plack::Request for this request

=item B<bucket_name> =E<gt> $string

The name of the bucket

=back

=head2 $object = $impl-E<gt>get_bucket(%args)

Used to retrieve a bucket. If there are no buckets that match the request, you should return undef.

The implementation's get_bucket method will receive the following named parameters:

=over 4

=item B<request> =E<gt> $object

Plack::Request for this request

=item B<bucket_name> =E<gt> $string

The name of the bucket

=back

=head2 $object = $impl-E<gt>get_object(%args)

Used to retrieve an object. If there are no object that matcht the request, you should return undef.

Note that this method will be called for both GET and HEAD requests.

The implementation's get_object method will receive the following named parameters:

=over 4

=item B<request> =E<gt> $object

Plack::Request for this request

=item B<bucket> =E<gt> $object

The bucket returned by get_bucket().

=item B<object_name> =E<gt> $string

The name of the object.

=item B<force_master> =E<gt> $bool

Set to true if X-STF-Force-MasterDB header was sent

=back

=head2 $impl-E<gt>delete_bucket(%args)

=over 4

=item B<request> =E<gt> $object

Plack::Request for this request

=item B<bucket> =E<gt> $object

The bucket returned by get_bucket().

=item B<recursive> =E<gt> $bool

Set to true if the X-STF-Recursive-Delete header was specified

=back

=head2 $impl-E<gt>create_object(%args)

=over 4

=item B<request> =E<gt> $object

Plack::Request for this request

=item B<bucket> =E<gt> $object

The bucket returned by get_bucket().

=item B<object_name> =E<gt> $string

The name of the object.

=item B<consistency> =E<gt> $int

The minimum consistency (number of replicas that must be created by the end of create_object call.

=item B<size> =E<gt> $int

The size of the object

=item B<suffix> =E<gt> $string

The suffix to be used for the object. defaults to ".dat"

=item B<input> =E<gt> $handle

The input handle to read the data from

=item B<replicas> =E<gt> $int

Number of replicas that the system should keep in the end.

=back

=head2 $impl-E<gt>modify_object(%args)

=over 4

=item B<replicas> =E<gt> $int

Number of replicas that the system should keep in the end.

=item B<request> =E<gt> $object

Plack::Request for this request

=item B<bucket> =E<gt> $object

The bucket returned by get_bucket().

=item B<object_name> =E<gt> $string

The name of the object.

=back

=head2 $impl-E<gt>delete_object(%args)

=over 4

=item B<request> =E<gt> $object

Plack::Request for this request

=item B<bucket> =E<gt> $object

The bucket returned by get_bucket().

=item B<object_name> =E<gt> $string

The name of the object.

=back

=head1 AUTHOR

Daisuke Maki C<< <daisuke@endeworks.jp> >>

=head1 COPYRIGHT AND LICENSE

Copyright (C) 2011 by Daisuke Maki

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself, either Perl version 5.8.0 or,
at your option, any later version of Perl 5 you may have available.

=cut
