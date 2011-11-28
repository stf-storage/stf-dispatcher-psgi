use inc::Module::Install;

name 'STF-Dispatcher-PSGI';
all_from 'lib/STF/Dispatcher/PSGI.pm';

requires 'Class::Accessor::Lite';
requires 'Plack';
requires 'Plack::Request';
requires 'Scalar::Util';
requires 'HTTP::Date';

test_requires 'HTTP::Request::Common';
test_requires 'Test::More';

auto_set_repository;
WriteAll;