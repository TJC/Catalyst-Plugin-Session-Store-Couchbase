use inc::Module::Install;

name     'Catalyst-Plugin-Session-Store-Couchbase';
all_from 'lib/Catalyst/Plugin/Session/Store/Couchbase.pm';
author   q{Toby Corkindale <tjc@wintrmute.net>};
license  'perl';

build_requires 'Test::More';
build_requires 'Catalyst::Plugin::Session' => 0.27;

auto_install;

WriteAll;

