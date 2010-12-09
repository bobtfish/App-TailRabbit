use strict;
use warnings;
use inc::Module::Install;

name 'App-TailRabbit';
all_from 'lib/App/TailRabbit.pm';
requires 'JSON';
requires 'Try::Tiny';
requires 'Moose';
requires 'MooseX::Types::Moose';
requires 'namespace::autoclean';
requires 'MooseX::Getopt';
requires 'Net::RabbitFoot';
requires 'MooseX::Types::Common';
requires 'AnyEvent';
requires 'YAML';
requires 'File::HomeDir';
requires 'Path::Class';
requires 'MooseX::Types::LoadableClass';

install_script 'script/tail_rabbit';

WriteAll();

