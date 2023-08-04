# Run all precommit checks
all: format run_test xref dial grad

format:
	rebar3 as format fmt -w

check_format:
	rebar3 as format fmt --check

xref:
	rebar3 xref

dial:
	rebar3 dialyzer

grad:
	rebar3 as grad gradualizer

# Separate task to make output on CI more pretty
pre_test:
	rebar3 as test compile

run_test:
	rebar3 as test ct --sname=ct1

cover_test:
	rebar3 as test ct --sname=ct1 --cover

# Produce codecov.json report
cover_json:
	rebar3 as test codecov analyze
