use inc::Module::Install;

name 'STF-Dispatcher-PSGI';
all_from 'lib/STF/Dispatcher/PSGI.pm';

requires 'Class::Accessor::Lite';
requires 'Plack';
requires 'Plack::Middleware::Header';
requires 'Plack::Request';
requires 'Scalar::Util';
requires 'HTTP::Date';

recommends 'Net::STF::Client';

test_requires 'HTTP::Request::Common';
test_requires 'Test::More';

auto_set_repository;
WriteAll;