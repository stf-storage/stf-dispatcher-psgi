use strict;

sub mysystem(@) {
    system(@_) == 0 or die "Failed to execute @_: $!";
}

sub cpanm (@) {
    @_ = ("cpanm", "--notest", @_);
    goto &mysystem;
}

sub test_binding {
    cpanm("--installdeps", ".");
    mysystem("perl", "Makefile.PL");
    mysystem("make", "test");
}

cpanm(qw(
    inc::Module::Install
    Module::Install::Repository
));
run_tests();
