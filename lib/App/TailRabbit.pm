package App::TailRabbit;
use Moose;
use MooseX::Getopt;
use AnyEvent::RabbitMQ;
use Data::Dumper;
use MooseX::Types::Moose qw/ ArrayRef Object Bool /;
use MooseX::Types::Common::String qw/ NonEmptySimpleStr /;
use AnyEvent;
use YAML qw/LoadFile/;
use File::HomeDir;
use Path::Class qw/ file /;
use MooseX::Types::LoadableClass qw/ LoadableClass /;
use namespace::autoclean;

our $VERSION = '0.003';

with qw/
    MooseX::Getopt
    MooseX::ConfigFromFile
/;

has exchange_name => (
    is => 'ro',
    isa => NonEmptySimpleStr,
    required => 1,
);

has routing_key => (
    is => 'ro',
    isa => ArrayRef[NonEmptySimpleStr],
    default => sub { [] },
);

has rabbitmq_host => (
    isa => NonEmptySimpleStr,
    is => 'ro',
    default => 'localhost',
);

has [qw/ rabbitmq_user rabbitmq_pass /] => (
    isa => NonEmptySimpleStr,
    is => 'ro',
    default => 'guest',
);

has convertor => (
    isa => LoadableClass,
    is => 'ro',
    coerce => 1,
    default => 'App::TailRabbit::Convertor::Null',
);

has _convertor => (
    is => 'ro',
    isa => Object,
    lazy => 1,
    default => sub {
        shift->convertor->new
    },
    handles => [qw/ convert /],
);

has exchange_type => (
    is => 'ro',
    isa => NonEmptySimpleStr,
    default => 'topic',
);

has durable => (
    is => 'ro',
    isa => Bool,
    default => 0,
);

sub get_config_from_file {
    my ($class, $file) = @_;
    return LoadFile($file) if (-r $file);
    return {};
}

has 'configfile' => (
    default => file(File::HomeDir->my_home(), ".tailrabbit.yml")->stringify,
    is => 'bare',
);

my $rf = AnyEvent::RabbitMQ->new(
#       verbose => 1,
)->load_xml_spec();

sub _get_mq {
    my $self = shift;
    my $cv = AnyEvent->condvar;
    $rf->connect(
        host => $self->rabbitmq_host,
        port => 5672,
        user => $self->rabbitmq_user,
        pass => $self->rabbitmq_pass,
        vhost => '/',
        on_close => sub {
            die("MQ connection closed");
        },
        on_read_failure => sub {
            die("READ FAILED");
        },
        on_failure => sub {
            die("Failed to connect to mq");
        },
        on_success => sub { $cv->send},
    );
    $cv->recv;
    return $rf;
}

sub _bind_anon_queue {
    my ($self, $ch) = @_;
    $ch->declare_queue(
        queue => $self->exchange_name,
#        auto_delete => 1,
#        exclusive => 1,
        durable => $self->durable,
        on_success => sub {
            my $queue_frame = shift;
            my @keys = @{ $self->routing_key };
            push(@keys, "#") unless scalar @keys;
            foreach my $key (@keys) {
                $ch->bind_queue(
                    queue => $queue_frame->method_frame->queue,
                    exchange => $self->exchange_name,
                    routing_key => $key,
                );
            }
        },
    );
}

sub _get_channel {
    my ($self, $rf) = @_;
    my $cv = AnyEvent->condvar;
    $rf->open_channel(
        on_close => sub { warn("Channel closed - wrong exchange options!\n"); exit; },
    
        on_success => sub {
            my $ch = shift;
            $ch->declare_exchange(
            type => $self->exchange_type,
            durable => $self->durable,
            exchange => $self->exchange_name,
            on_success => sub {
                my $reply = shift;
                my $exch_frame = $reply->method_frame;
                die Dumper($exch_frame) unless blessed $exch_frame and $exch_frame->isa('Net::AMQP::Protocol::Exchange::DeclareOk');
                $cv->send($ch);
            });
        },
    );
    my $ch = $cv->recv;
}

sub run {
    my $self = shift;
    my $done = AnyEvent->condvar;
    my $ch = $self->_get_channel($self->_get_mq);
    $self->_bind_anon_queue($ch);
    $ch->consume(
        on_consume => sub {
            my $message = shift;
            my $payload = $message->{body}->payload;
            my $routing_key = $message->{deliver}->method_frame->routing_key;
            $self->notify($payload, $routing_key, $message);
        },
    );
    $done->recv; # Go into the event loop forever.
}

sub notify {
    my ($self, $payload, $routing_key, $message) = @_;
    print $routing_key,
        ': ', $payload, "\n";
}

__PACKAGE__->meta->make_immutable;
1;

=head1 NAME

App::TailRabbit - Listen to a RabbitMQ exchange and emit the messages to console.

=head1 SYNOPSIS

    tail_reabbit --exchange_name firehose --routing_key # --rabbitmq_user guest --rabbitmq_user guest --rabbitmq_host localhost

=head1 DESCRIPTION

Simple module to consume messages from a RabitMQ message queue.

=head1 BUGS

=over

=item Virtually no docs

=item Creates all exchanges as durable.

=item Always creates exchange if it doesn't exist

=item Probably several more

=back

=head1 SEE ALSO

L<Net::RabbitFoot>

=head1 AUTHOR

Tomas (t0m) Doran C<< <bobtfish@bobtfish.net> >>.

=head1 COPYRIGHT & LICENSE

Copyright the above author(s).

Licensed under the same terms as perl itself.

=cut

