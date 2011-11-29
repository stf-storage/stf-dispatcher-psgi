package STF::Dispatcher::PSGI::HTTPException;
use strict;
use Carp ();

sub throw {
    my $class = shift;
    Carp::croak( bless [@_], $class );
}

sub as_psgi {
    return [ @{$_[0]} ];
}

1;