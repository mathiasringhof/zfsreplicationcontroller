package Capture::Tiny;

use strict;
use warnings;

use Exporter 'import';
use File::Temp qw(tempfile);

our @EXPORT_OK = qw(capture tee_stdout tee_stderr);
our %EXPORT_TAGS = (all => \@EXPORT_OK);

sub _read_file {
	my ($path) = @_;
	open my $handle, '<', $path or die "open $path: $!";
	local $/;
	my $contents = <$handle>;
	close $handle or die "close $path: $!";
	return defined $contents ? $contents : '';
}

sub _run {
	my ($capture_stdout, $capture_stderr, $tee, $code) = @_;
	my ($stdout_handle, $stdout_path) = tempfile();
	my ($stderr_handle, $stderr_path) = tempfile();
	close $stdout_handle or die "close stdout capture: $!";
	close $stderr_handle or die "close stderr capture: $!";

	my ($saved_stdout, $saved_stderr);
	if ($capture_stdout) {
		open $saved_stdout, '>&', \*STDOUT or die "save stdout: $!";
		open STDOUT, '>', $stdout_path or die "capture stdout: $!";
	}
	if ($capture_stderr) {
		open $saved_stderr, '>&', \*STDERR or die "save stderr: $!";
		open STDERR, '>', $stderr_path or die "capture stderr: $!";
	}

	my $result;
	{
		local $?;
		$code->();
		$result = $?;
	}

	if ($capture_stdout) {
		open STDOUT, '>&', $saved_stdout or die "restore stdout: $!";
	}
	if ($capture_stderr) {
		open STDERR, '>&', $saved_stderr or die "restore stderr: $!";
	}

	my $stdout = _read_file($stdout_path);
	my $stderr = _read_file($stderr_path);
	if ($tee && $capture_stdout) {
		print {$saved_stdout} $stdout or die "tee stdout: $!";
	}
	if ($tee && $capture_stderr) {
		print {$saved_stderr} $stderr or die "tee stderr: $!";
	}
	close $saved_stdout if $capture_stdout;
	close $saved_stderr if $capture_stderr;
	unlink $stdout_path or die "remove stdout capture: $!";
	unlink $stderr_path or die "remove stderr capture: $!";

	return ($stdout, $stderr, $result);
}

sub capture (&) {
	my ($code) = @_;
	return _run(1, 1, 0, $code);
}

sub tee_stdout (&) {
	my ($code) = @_;
	my ($stdout, undef, $result) = _run(1, 0, 1, $code);
	return ($stdout, $result);
}

sub tee_stderr (&) {
	my ($code) = @_;
	my (undef, $stderr, $result) = _run(0, 1, 1, $code);
	return ($stderr, $result);
}

1;
